use anyhow::{anyhow, Result};
use chrono::{DateTime, NaiveDateTime, Utc};
use indicatif::{ParallelProgressIterator, ProgressStyle};
use rayon::prelude::*;
use sqlx::types::time::OffsetDateTime;
use sqlx::{postgres::PgPoolOptions, Pool, Postgres};
use std::cell::RefCell;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;
use tokio::fs;
use tracing::{debug, error, info};

// html5ever imports
use html5ever::tendril::StrTendril;
use html5ever::tokenizer::{
    BufferQueue, Token, TokenSink, TokenSinkResult, Tokenizer, TokenizerOpts,
};

pub struct MatchScoreUpdater {
    pool: Pool<Postgres>,
}

impl MatchScoreUpdater {
    pub async fn new(database_url: &str) -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(32)
            .connect(database_url)
            .await?;
        Ok(Self { pool })
    }

    /// Process all HTML files, gather the necessary updates in memory,
    /// and then perform a single bulk write in one transaction.
    pub async fn update_match_scores(&self, html_dir: &str) -> Result<()> {
        let overall_start = Instant::now();

        // Fetch existing matches and their scores.
        let fetch_start = Instant::now();
        let existing_matches: HashMap<String, (Option<i32>, Option<i32>)> = sqlx::query!(
            r#"
            SELECT csv_reference, home_score, away_score
            FROM matches
            WHERE csv_reference IS NOT NULL
            "#
        )
        .fetch_all(&self.pool)
        .await?
        .into_iter()
        .filter_map(|row| {
            row.csv_reference
                .map(|csv_ref| (csv_ref, (row.home_score, row.away_score)))
        })
        .collect();
        let fetch_duration = fetch_start.elapsed();
        info!(
            "Fetched {} existing matches from database in {} ms",
            existing_matches.len(),
            fetch_duration.as_millis()
        );

        // Debug log for specific match
        if let Some((home_score, away_score)) = existing_matches.get("47_32_2025_1_20.html") {
            debug!(
                "Found existing match 47_32_2025_1_20.html in database with scores: home={:?}, away={:?}",
                home_score, away_score
            );
        } else {
            debug!("Match 47_32_2025_1_20.html not found in database");
        }

        // Read HTML file paths from the directory.
        let dir_read_start = Instant::now();
        let mut entries = fs::read_dir(html_dir).await?;
        let mut html_files = Vec::new();
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.extension().map_or(false, |ext| ext == "html") {
                debug!("Found HTML file: {:?}", path);
                html_files.push(path);
            }
        }
        let dir_read_duration = dir_read_start.elapsed();
        info!(
            "Found {} HTML files to process (dir read in {} ms)",
            html_files.len(),
            dir_read_duration.as_millis()
        );

        // Log a few example matches from the database
        for (csv_ref, (home_score, away_score)) in existing_matches.iter().take(5) {
            debug!(
                "Database entry: {} (scores: {:?}, {:?})",
                csv_ref, home_score, away_score
            );
        }

        // Counters for metrics.
        let total_read_time = AtomicU64::new(0);
        let total_parse_time = AtomicU64::new(0);

        // Set up progress bar.
        let style = ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} files processed ({eta})")
            .unwrap();

        let parallel_start = Instant::now();
        let existing_matches = Arc::new(existing_matches);
        let missing_matches = Arc::new(Mutex::new(Vec::new()));
        let updates: Vec<(String, i32, i32)> = html_files
            .par_iter()
            .progress_with_style(style)
            .filter_map(|path| {
                // Read file.
                let read_start = Instant::now();
                let html_content = match std::fs::read_to_string(path) {
                    Ok(content) => content,
                    Err(e) => {
                        error!("Failed to read file {:?}: {}", path, e);
                        return None;
                    }
                };
                let read_duration = read_start.elapsed();
                total_read_time.fetch_add(read_duration.as_nanos() as u64, Ordering::Relaxed);

                // Create CSV reference first to check if we should process this file
                let file_name = match path.strip_prefix(html_dir) {
                    Ok(name) => name.to_string_lossy().into_owned(),
                    Err(e) => {
                        error!("Failed to strip prefix from path {:?}: {}", path, e);
                        return None;
                    }
                };
                let csv_reference = format!("html_files/{}", file_name);

                // Check database before parsing
                if !existing_matches.contains_key(&csv_reference) {
                    let mut missing = missing_matches.lock().unwrap();
                    missing.push(csv_reference);
                    return None;
                }

                // Parse and extract score.
                let parse_start = Instant::now();
                let score_result = match self.extract_match_score(&html_content) {
                    Ok(Some(score)) => score,
                    Ok(None) => {
                        error!("No score found in file: {}", file_name);
                        return None;
                    }
                    Err(e) => {
                        error!("Failed to parse score from {}: {}", file_name, e);
                        return None;
                    }
                };
                let parse_duration = parse_start.elapsed();
                total_parse_time.fetch_add(parse_duration.as_nanos() as u64, Ordering::Relaxed);

                let (home_score, away_score) = score_result;

                // Only update if scores differ
                match existing_matches.get(&csv_reference) {
                    Some((Some(existing_home), Some(existing_away)))
                        if *existing_home == home_score && *existing_away == away_score =>
                    {
                        None
                    }
                    _ => Some((csv_reference, home_score, away_score)),
                }
            })
            .collect();
        let parallel_duration = parallel_start.elapsed();

        // Log missing matches.
        let missing_matches = Arc::try_unwrap(missing_matches)
            .unwrap()
            .into_inner()
            .unwrap();

        if !missing_matches.is_empty() {
            error!(
                "\nFound {} HTML files without corresponding database entries:",
                missing_matches.len()
            );
            error!(
                "Database has {} entries, found {} HTML files, difference of {}",
                existing_matches.len(),
                html_files.len(),
                html_files.len() as i64 - existing_matches.len() as i64
            );
            for missing in missing_matches.iter().take(5) {
                error!("  Missing database entry for: {}", missing);
            }
            if missing_matches.len() > 5 {
                error!("  ... and {} more", missing_matches.len() - 5);
            }
        }

        // Check for HTML files that should exist based on database entries
        let mut missing_html_files = Vec::new();
        for (csv_ref, _) in existing_matches.iter() {
            // Remove html_files/ prefix if it exists
            let file_name = csv_ref.strip_prefix("html_files/").unwrap_or(csv_ref);
            let html_path = std::path::Path::new(html_dir).join(file_name);
            if !html_path.exists() {
                missing_html_files.push(csv_ref.clone());
            }
        }
        if !missing_html_files.is_empty() {
            error!(
                "Found {} database entries without corresponding HTML files:",
                missing_html_files.len()
            );
            // Show up to 5 examples
            for missing in missing_html_files.iter().take(5) {
                error!("  Missing HTML file for: {}", missing);
            }
            if missing_html_files.len() > 5 {
                error!("  ... and {} more", missing_html_files.len() - 5);
            }
        }

        let files_processed = html_files.len() as u64;
        let updates_count = updates.len();
        let valid_files = files_processed as usize - missing_matches.len();

        // Handle missing matches
        if !missing_matches.is_empty() {
            error!(
                "\nFound {} HTML files without corresponding database entries:",
                missing_matches.len()
            );
            error!(
                "Database has {} entries, found {} HTML files, difference of {}",
                existing_matches.len(),
                html_files.len(),
                html_files.len() as i64 - existing_matches.len() as i64
            );

            let mut inserted_count = 0;
            for missing in &missing_matches {
                let html_path = std::path::Path::new(html_dir)
                    .join(missing.strip_prefix("html_files/").unwrap_or(missing));
                match std::fs::read_to_string(&html_path) {
                    Ok(content) => {
                        if let Err(e) = self.insert_new_match(missing, &content).await {
                            error!("Failed to insert match {}: {}", missing, e);
                        } else {
                            inserted_count += 1;
                        }
                    }
                    Err(e) => error!("Failed to read file {:?}: {}", html_path, e),
                }
            }
            info!("Inserted {} new matches", inserted_count);
        }

        // Perform bulk update if needed.
        if updates_count > 0 {
            let tx_start = Instant::now();
            let mut tx = self.pool.begin().await?;
            for (csv_reference, home_score, away_score) in updates {
                sqlx::query!(
                    r#"
                    UPDATE matches
                    SET home_score = $1, away_score = $2
                    WHERE csv_reference = $3
                    "#,
                    home_score,
                    away_score,
                    csv_reference
                )
                .execute(&mut *tx)
                .await?;
            }
            tx.commit().await?;
            let tx_duration = tx_start.elapsed();
            info!(
                "Updated {} matches in {} ms",
                updates_count,
                tx_duration.as_millis()
            );
        } else {
            info!(
                "No updates needed - all {} valid matches are up to date",
                valid_files
            );
        }

        let overall_duration = overall_start.elapsed();
        let total_read_ms = total_read_time.load(Ordering::Relaxed) as f64 / 1_000_000.0;
        let avg_read_ms = if files_processed > 0 {
            total_read_ms / files_processed as f64
        } else {
            0.0
        };
        let total_parse_ms = total_parse_time.load(Ordering::Relaxed) as f64 / 1_000_000.0;
        let avg_parse_ms = if files_processed > 0 {
            total_parse_ms / files_processed as f64
        } else {
            0.0
        };

        info!("Metrics Summary:");
        info!(
            "  DB fetch time         : {} ms",
            fetch_duration.as_millis()
        );
        info!(
            "  Dir read time         : {} ms",
            dir_read_duration.as_millis()
        );
        info!(
            "  Parallel processing   : {} ms (avg per file: read {:.2} ms, parse {:.2} ms)",
            parallel_duration.as_millis(),
            avg_read_ms,
            avg_parse_ms
        );
        info!(
            "  Overall time          : {} ms",
            overall_duration.as_millis()
        );
        info!(
            "  Files processed       : {} ({} valid, {} missing db entries, {} missing html files)",
            files_processed,
            valid_files,
            missing_matches.len(),
            missing_html_files.len()
        );

        Ok(())
    }

    /// Extracts the match score from the HTML by scanning for a meta tag with a description.
    ///
    /// This implementation uses html5ever's tokenizer. A local sink type (`MetaDescriptionSink`)
    /// stores the meta tag's content. A newtype wrapper (`SinkWrapper`) is used to pass a mutable
    /// reference to the tokenizer.
    fn extract_match_score(&self, html_content: &str) -> Result<Option<(i32, i32)>> {
        struct MetaDescriptionSink {
            content: RefCell<Option<String>>,
        }

        impl TokenSink for MetaDescriptionSink {
            type Handle = ();
            fn process_token(&self, token: Token, _line_number: u64) -> TokenSinkResult<()> {
                if let Token::TagToken(tag) = token {
                    if tag.name.as_ref().eq_ignore_ascii_case("meta") {
                        let mut is_description = false;
                        let mut content_val = None;
                        for attr in tag.attrs.iter() {
                            if attr.name.local.as_ref().eq_ignore_ascii_case("name")
                                && attr.value.eq_ignore_ascii_case("description")
                            {
                                is_description = true;
                            }
                            if attr.name.local.as_ref().eq_ignore_ascii_case("content") {
                                content_val = Some(attr.value.to_string());
                            }
                        }
                        if is_description {
                            if let Some(val) = content_val {
                                *self.content.borrow_mut() = Some(val);
                            }
                        }
                    }
                }
                TokenSinkResult::Continue
            }
        }

        struct SinkWrapper<'a>(&'a mut MetaDescriptionSink);

        impl<'a> TokenSink for SinkWrapper<'a> {
            type Handle = ();
            fn process_token(&self, token: Token, line_number: u64) -> TokenSinkResult<()> {
                self.0.process_token(token, line_number)
            }
        }

        let mut sink = MetaDescriptionSink {
            content: RefCell::new(None),
        };

        {
            let wrapper = SinkWrapper(&mut sink);
            let tokenizer = Tokenizer::new(wrapper, TokenizerOpts::default());
            let input = BufferQueue::default();
            input.push_back(StrTendril::from(html_content));
            let _ = tokenizer.feed(&input);
            tokenizer.end();
        }

        if let Some(desc) = sink.content.borrow().as_ref() {
            debug!("Found meta description: {}", desc);
            if let Some(start_idx) = desc.find("(final score:") {
                let after = &desc[start_idx + "(final score:".len()..];
                let after = after.trim_start();
                if let Some(end_idx) = after.find(')') {
                    let score_str = &after[..end_idx];
                    debug!("Extracted score string: {}", score_str);
                    if let Some((home, away)) = score_str.split_once('-') {
                        let home_score = home
                            .trim()
                            .parse::<i32>()
                            .map_err(|e: std::num::ParseIntError| anyhow!(e))?;
                        let away_score = away
                            .trim()
                            .parse::<i32>()
                            .map_err(|e: std::num::ParseIntError| anyhow!(e))?;
                        return Ok(Some((home_score, away_score)));
                    }
                }
            }
        }
        Ok(None)
    }

    fn extract_match_details(
        html_content: &str,
    ) -> Result<Option<(String, String, String, String, String)>> {
        struct MetaUrlSink {
            url: RefCell<Option<String>>,
        }

        impl TokenSink for MetaUrlSink {
            type Handle = ();
            fn process_token(&self, token: Token, _line_number: u64) -> TokenSinkResult<()> {
                if let Token::TagToken(tag) = token {
                    if tag.name.as_ref().eq_ignore_ascii_case("meta") {
                        let mut is_og_url = false;
                        let mut content_val = None;
                        for attr in tag.attrs.iter() {
                            if attr.name.local.as_ref().eq_ignore_ascii_case("property")
                                && attr.value.eq_ignore_ascii_case("og:url")
                            {
                                is_og_url = true;
                            }
                            if attr.name.local.as_ref().eq_ignore_ascii_case("content") {
                                content_val = Some(attr.value.to_string());
                            }
                        }
                        if is_og_url {
                            if let Some(val) = content_val {
                                debug!("Found og:url meta tag with content: {}", val);
                                *self.url.borrow_mut() = Some(val);
                            }
                        }
                    }
                }
                TokenSinkResult::Continue
            }
        }

        struct SinkWrapper<'a>(&'a MetaUrlSink);

        impl<'a> TokenSink for SinkWrapper<'a> {
            type Handle = ();
            fn process_token(&self, token: Token, line_number: u64) -> TokenSinkResult<()> {
                self.0.process_token(token, line_number)
            }
        }

        let sink = MetaUrlSink {
            url: RefCell::new(None),
        };

        {
            let wrapper = SinkWrapper(&sink);
            let tokenizer = Tokenizer::new(wrapper, TokenizerOpts::default());
            let input = BufferQueue::default();
            input.push_back(StrTendril::from(html_content));
            let _ = tokenizer.feed(&input);
            tokenizer.end();
        }

        if let Some(url) = sink.url.borrow().as_ref() {
            debug!("Found og:url: {}", url);
            if let Some(parts) = url.strip_prefix("https://www.mkttl.co.uk/matches/team/") {
                let parts: Vec<&str> = parts.split('/').collect();
                if parts.len() >= 7 {
                    let home_club = parts[0].to_string();
                    let home_team = parts[1].to_string();
                    let away_club = parts[2].to_string();
                    let away_team = parts[3].to_string();
                    let year = parts[4];
                    let month = parts[5];
                    let day = parts[6];

                    let match_date = format!("{}-{}-{}", year, month, day);
                    debug!(
                        "Extracted match details: {} {} vs {} {} ({})",
                        home_club, home_team, away_club, away_team, match_date
                    );
                    return Ok(Some((
                        home_club, home_team, away_club, away_team, match_date,
                    )));
                }
            }
        }
        debug!("No og:url meta tag found or invalid URL format");
        Ok(None)
    }

    async fn get_or_create_team(&self, club_name: &str, team_name: &str) -> Result<i64> {
        // First try to find the team
        if let Some(row) = sqlx::query!(
            r#"
            SELECT t.id
            FROM team t
            JOIN club c ON t.club_id = c.id
            WHERE c.name = $1 AND t.name = $2
            "#,
            club_name,
            team_name
        )
        .fetch_optional(&self.pool)
        .await?
        {
            return Ok(row.id);
        }

        // If not found, create club first if needed
        let club_id = if let Some(row) =
            sqlx::query!("SELECT id FROM club WHERE name = $1", club_name)
                .fetch_optional(&self.pool)
                .await?
        {
            row.id
        } else {
            sqlx::query!(
                r#"
                INSERT INTO club (league_id, name, venue_id)
                VALUES (1, $1, 1)
                RETURNING id
                "#,
                club_name
            )
            .fetch_one(&self.pool)
            .await?
            .id
        };

        // Create team
        let team_id = sqlx::query!(
            r#"
            INSERT INTO team (league_id, season_id, division_id, club_id, name)
            VALUES (1, 1, 1, $1, $2)
            RETURNING id
            "#,
            club_id,
            team_name
        )
        .fetch_one(&self.pool)
        .await?
        .id;

        Ok(team_id)
    }

    async fn insert_new_match(&self, csv_reference: &str, html_content: &str) -> Result<()> {
        // Extract match details from og:url
        let details = match Self::extract_match_details(html_content)? {
            Some(details) => details,
            None => {
                error!("Failed to extract match details from {}", csv_reference);
                return Ok(());
            }
        };
        let (home_club, home_team, away_club, away_team, match_date) = details;

        // Extract score
        let (home_score, away_score) = match self.extract_match_score(html_content)? {
            Some(score) => score,
            None => {
                error!("Failed to extract score from {}", csv_reference);
                return Ok(());
            }
        };

        // Get or create teams
        let home_team_id = self.get_or_create_team(&home_club, &home_team).await?;
        let away_team_id = self.get_or_create_team(&away_club, &away_team).await?;

        // Parse match date
        let match_date = NaiveDateTime::parse_from_str(
            &format!("{}T00:00:00", match_date),
            "%Y-%m-%dT%H:%M:%S",
        )?;
        let match_date: DateTime<Utc> = DateTime::from_naive_utc_and_offset(match_date, Utc);
        let match_date = OffsetDateTime::from_unix_timestamp(match_date.timestamp())?;

        // Insert new match
        sqlx::query!(
            r#"
            INSERT INTO matches (
                league_id, season_id, match_date, home_score, away_score,
                home_team_id, away_team_id, venue_id,
                competition_type, csv_reference, created_at, updated_at
            )
            VALUES (
                1, 1, $1, $2, $3, $4, $5, 1, 'MKTTL League', $6, NOW(), NOW()
            )
            ON CONFLICT (csv_reference) DO UPDATE
            SET home_score = EXCLUDED.home_score,
                away_score = EXCLUDED.away_score,
                updated_at = NOW()
            "#,
            match_date,
            home_score,
            away_score,
            home_team_id,
            away_team_id,
            csv_reference
        )
        .execute(&self.pool)
        .await?;

        info!(
            "Inserted new match: {} {} vs {} {} ({})",
            home_club,
            home_team,
            away_club,
            away_team,
            match_date.unix_timestamp()
        );

        Ok(())
    }

    /// Convenience function to run the updater standalone.
    pub async fn run_standalone(database_url: &str, html_dir: &str) -> Result<()> {
        let updater = Self::new(database_url).await?;
        updater.update_match_scores(html_dir).await
    }

   
}

#[cfg(test)]
mod tests {
    use super::*;
    use test_log::test;

    fn extract_match_details_from_html(html_content: &str) -> Result<Option<(String, String, String, String, String)>> {
        struct MetaUrlSink {
            url: RefCell<Option<String>>,
        }

        impl TokenSink for MetaUrlSink {
            type Handle = ();
            fn process_token(&self, token: Token, _line_number: u64) -> TokenSinkResult<()> {
                if let Token::TagToken(tag) = token {
                    if tag.name.as_ref().eq_ignore_ascii_case("meta") {
                        let mut is_og_url = false;
                        let mut content_val = None;
                        for attr in tag.attrs.iter() {
                            if attr.name.local.as_ref().eq_ignore_ascii_case("property")
                                && attr.value.eq_ignore_ascii_case("og:url")
                            {
                                is_og_url = true;
                            }
                            if attr.name.local.as_ref().eq_ignore_ascii_case("content") {
                                content_val = Some(attr.value.to_string());
                            }
                        }
                        if is_og_url {
                            if let Some(val) = content_val {
                                debug!("Found og:url meta tag with content: {}", val);
                                *self.url.borrow_mut() = Some(val);
                            }
                        }
                    }
                }
                TokenSinkResult::Continue
            }
        }

        struct SinkWrapper<'a>(&'a MetaUrlSink);

        impl<'a> TokenSink for SinkWrapper<'a> {
            type Handle = ();
            fn process_token(&self, token: Token, line_number: u64) -> TokenSinkResult<()> {
                self.0.process_token(token, line_number)
            }
        }

        let sink = MetaUrlSink {
            url: RefCell::new(None),
        };

        {
            let wrapper = SinkWrapper(&sink);
            let tokenizer = Tokenizer::new(wrapper, TokenizerOpts::default());
            let input = BufferQueue::default();
            input.push_back(StrTendril::from(html_content));
            let _ = tokenizer.feed(&input);
            tokenizer.end();
        }

        if let Some(url) = sink.url.borrow().as_ref() {
            debug!("Found og:url: {}", url);
            if let Some(parts) = url.strip_prefix("https://www.mkttl.co.uk/matches/team/") {
                let parts: Vec<&str> = parts.split('/').collect();
                if parts.len() >= 7 {
                    let home_club = parts[0].to_string();
                    let home_team = parts[1].to_string();
                    let away_club = parts[2].to_string();
                    let away_team = parts[3].to_string();
                    let year = parts[4];
                    let month = parts[5];
                    let day = parts[6];

                    let match_date = format!("{}-{}-{}", year, month, day);
                    debug!(
                        "Extracted match details: {} {} vs {} {} ({})",
                        home_club, home_team, away_club, away_team, match_date
                    );
                    return Ok(Some((
                        home_club, home_team, away_club, away_team, match_date,
                    )));
                }
            }
        }
        debug!("No og:url meta tag found or invalid URL format");
        Ok(None)
    }

    #[test]
    fn test_extract_match_details() -> Result<()> {
        // Initialize logging for the test
        let _ = env_logger::builder().is_test(true).try_init();

        let html_content = r#"<!DOCTYPE html>
<html>
<head>
<meta property="og:url" content="https://www.mkttl.co.uk/matches/team/open-university/explorers/great-brickhill/badgers/2025/02/27" />
</head>
<body>
</body>
</html>"#;

        debug!("Testing with first HTML content: {}", html_content);
        let result = extract_match_details_from_html(html_content)?;
        
        assert!(result.is_some(), "Failed to extract match details");
        let (home_club, home_team, away_club, away_team, match_date) = result.unwrap();
        
        debug!("Extracted details: {} {} vs {} {} ({})", 
            home_club, home_team, away_club, away_team, match_date);

        assert_eq!(home_club, "open-university");
        assert_eq!(home_team, "explorers");
        assert_eq!(away_club, "great-brickhill");
        assert_eq!(away_team, "badgers");
        assert_eq!(match_date, "2025-02-27");

        // Test with a different example
        let html_content_2 = r#"<!DOCTYPE html>
<html>
<head>
<meta property="og:url" content="https://www.mkttl.co.uk/matches/team/milton-keynes/dragons/woburn-sands/tigers/2025/03/15" />
</head>
<body>
</body>
</html>"#;

        debug!("Testing with second HTML content: {}", html_content_2);
        let result = extract_match_details_from_html(html_content_2)?;
        
        assert!(result.is_some(), "Failed to extract match details from second example");
        let (home_club, home_team, away_club, away_team, match_date) = result.unwrap();
        
        debug!("Extracted details: {} {} vs {} {} ({})", 
            home_club, home_team, away_club, away_team, match_date);

        assert_eq!(home_club, "milton-keynes");
        assert_eq!(home_team, "dragons");
        assert_eq!(away_club, "woburn-sands");
        assert_eq!(away_team, "tigers");
        assert_eq!(match_date, "2025-03-15");

        Ok(())
    }
}

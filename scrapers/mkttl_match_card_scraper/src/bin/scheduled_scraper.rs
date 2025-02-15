use anyhow::Result;
use chrono::{DateTime, Utc};
use indicatif::{ProgressBar, ProgressStyle};
use mkttl_match_card_scraper::match_html_scraper::MatchHtmlScraper;
use questdb::ingress::{Buffer, ColumnName, Sender, TableName, TimestampNanos};
use reqwest;
use serde::Serialize;
use serde_json;
use std::path::Path;
use tokio::fs;
use tracing_subscriber;
use csv;
use sqlx;
use dotenv::dotenv;
use sqlx::postgres::PgPoolOptions;
use mkttl_match_card_scraper::match_score_updater::MatchScoreUpdater;
use tracing::info;

const SCRAPER_NAME: &str = "mkttl";
const HTML_FILES_DIR: &str = "/Users/alexdavis/ghq/github.com/armincerf/ttrankings/scrapers/mkttl_match_card_scraper/html_files";
const CSV_OUTPUT_DIR: &str = "/Users/alexdavis/ghq/github.com/armincerf/ttrankings/scrapers/mkttl_match_card_scraper/parsed_html_output";
const SCRAPER_TABLE: &str = "scraper_runs";
const QUESTDB_ADDR: &str = "http::addr=localhost:9000;";

#[derive(Debug, Serialize)]
struct ScraperRun {
    scraper_name: String,
    run_start_time: DateTime<Utc>,
    run_end_time: DateTime<Utc>,
    step: String,
    status: String,
    new_files_count: i64,
    total_files_count: i64,
    error_message: Option<String>,
}

impl ScraperRun {
    fn new(step: &str) -> Self {
        Self {
            scraper_name: SCRAPER_NAME.to_string(),
            run_start_time: Utc::now(),
            run_end_time: Utc::now(),
            step: step.to_string(),
            status: "running".to_string(),
            new_files_count: 0,
            total_files_count: 0,
            error_message: None,
        }
    }

    fn complete(&mut self, new_files: i64, total_files: i64) {
        self.run_end_time = Utc::now();
        self.status = "success".to_string();
        self.new_files_count = new_files;
        self.total_files_count = total_files;
    }

    fn fail(&mut self, error: &str) {
        self.run_end_time = Utc::now();
        self.status = "error".to_string();
        self.error_message = Some(error.to_string());
    }

    async fn log_to_questdb(&self) -> Result<()> {
        println!("Attempting to log to QuestDB for step: {}", self.step);
        let mut buffer = Buffer::new();
        let table_name = TableName::new(SCRAPER_TABLE)?;
        let start_nanos =
            TimestampNanos::new(self.run_start_time.timestamp_nanos_opt().unwrap_or(0));
        let end_nanos = TimestampNanos::new(self.run_end_time.timestamp_nanos_opt().unwrap_or(0));

        buffer
            .table(table_name)?
            .symbol(ColumnName::new("scraper_name")?, &self.scraper_name)?
            .symbol(ColumnName::new("step")?, &self.step)?
            .symbol(ColumnName::new("status")?, &self.status)?
            .symbol(
                ColumnName::new("error_message")?,
                self.error_message.as_deref().unwrap_or(""),
            )?
            .column_i64(ColumnName::new("new_files_count")?, self.new_files_count)?
            .column_i64(
                ColumnName::new("total_files_count")?,
                self.total_files_count,
            )?
            .column_ts(ColumnName::new("run_end_time")?, end_nanos)?
            .at(start_nanos)?;

        let mut sender = match Sender::from_conf(QUESTDB_ADDR) {
            Ok(s) => s,
            Err(e) => {
                println!("Failed to create QuestDB sender: {}", e);
                return Err(anyhow::anyhow!("Failed to create QuestDB sender: {}", e));
            }
        };
        
        match sender.flush(&mut buffer) {
            Ok(_) => println!("Successfully logged to QuestDB for step: {}", self.step),
            Err(e) => println!("Failed to flush to QuestDB: {}", e),
        }
        Ok(())
    }
}

async fn get_record_count() -> Result<i64> {
    let client = reqwest::Client::new();
    let response = client
        .get("http://localhost:9000/exec")
        .query(&[("query", "SELECT count() FROM table_tennis_games")])
        .send()
        .await?;

    let json: serde_json::Value = response.json().await?;
    Ok(json
        .get("dataset")
        .and_then(|d| d.as_array())
        .and_then(|a| a.first())
        .and_then(|r| r.as_array())
        .and_then(|a| a.first())
        .and_then(|v| v.as_i64())
        .unwrap_or(0))
}

async fn count_files_in_dir(dir: &str, ext: &str) -> Result<i64> {
    let mut count = 0i64;
    let mut dir = fs::read_dir(dir).await?;
    while let Some(entry) = dir.next_entry().await? {
        if entry.path().extension().map_or(false, |e| e == ext) {
            count += 1;
        }
    }
    Ok(count)
}

async fn scrape_html() -> Result<(i64, i64)> {
    let mut run = ScraperRun::new("scrape-html");

    let result: Result<(i64, i64)> = async {
        let mut scraper = MatchHtmlScraper::new()?;
        let initial_files = count_files_in_dir(HTML_FILES_DIR, "html").await?;

        info!("Running scraper for {}", SCRAPER_NAME);
        scraper.run(None).await?;

        let final_files = count_files_in_dir(HTML_FILES_DIR, "html").await?;

        Ok((final_files - initial_files, final_files))
    }
    .await;

    match result {
        Ok((new_files, total_files)) => {
            run.complete(new_files, total_files);
            run.log_to_questdb().await?;
            Ok((new_files, total_files))
        }
        Err(e) => {
            run.fail(&e.to_string());
            run.log_to_questdb().await?;
            Err(e)
        }
    }
}

async fn process_html_to_csv() -> Result<(i64, i64)> {
    let mut run = ScraperRun::new("html-to-csv");

    let result: Result<(i64, i64)> = async {
        fs::create_dir_all(CSV_OUTPUT_DIR).await?;

        // Get all HTML files from 2025
        let mut html_files = Vec::new();
        let mut entries = fs::read_dir(HTML_FILES_DIR).await?;
        while let Some(entry) = entries.next_entry().await? {
            let path = entry.path();
            if path.extension().map_or(false, |ext| ext == "html") {
                // Only process files that contain "2025" in their name
                if path.to_string_lossy().contains("2025") {
                    html_files.push(path);
                }
            }
        }

        let initial_csvs = count_files_in_dir(CSV_OUTPUT_DIR, "csv").await?;

        let total_files = html_files.len();
        if total_files > 0 {
            let pb = ProgressBar::new(total_files as u64);
            pb.set_style(
                ProgressStyle::default_bar()
                    .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} files ({eta})")
                    .unwrap(),
            );

            let processor = mkttl_match_card_scraper::game_scraper::GameScraper::new();

            for path in html_files {
                let html = fs::read_to_string(&path).await?;
                let games = match processor.parse_html(&html, path.to_str().unwrap_or_default()) {
                    Ok(games) => games,
                    Err(e) => {
                        let error_msg = format!("Error processing HTML file {:?}: {}", path, e);
                        run.error_message = Some(error_msg.clone());
                        run.log_to_questdb().await?;
                        
                        // Send notification
                        let notification_cmd = format!(
                            "osascript -e 'display notification \"{}\" with title \"MKTTL Scraper Error\"'",
                            error_msg.replace("\"", "\\\"")
                        );
                        if let Err(notify_err) = std::process::Command::new("sh")
                            .arg("-c")
                            .arg(&notification_cmd)
                            .output()
                        {
                            eprintln!("Failed to send notification: {}", notify_err);
                        }
                        continue;
                    }
                };

                let csv_path = Path::new(CSV_OUTPUT_DIR)
                    .join(path.strip_prefix(HTML_FILES_DIR).unwrap_or(&path))
                    .with_extension("csv");

                if let Some(parent) = csv_path.parent() {
                    fs::create_dir_all(parent).await?;
                }

                let mut wtr = csv::Writer::from_path(&csv_path)?;
                wtr.write_record(&[
                    "event_start_time",
                    "match_id",
                    "set_number",
                    "game_number",
                    "competition_type",
                    "season",
                    "division",
                    "venue",
                    "home_team_name",
                    "home_team_club",
                    "away_team_name",
                    "away_team_club",
                    "home_player1",
                    "home_player2",
                    "away_player1",
                    "away_player2",
                    "home_score",
                    "away_score",
                    "handicap_home",
                    "handicap_away",
                    "report_html",
                    "tx_time",
                ])?;

                for game in games {
                    wtr.write_record(&[
                        &game.event_start_time.to_rfc3339(),
                        &game.match_id,
                        &game.set_number.to_string(),
                        &game.game_number.to_string(),
                        &game.competition_type,
                        &game.season,
                        &game.division,
                        &game.venue,
                        &game.home_team_name,
                        &game.home_team_club,
                        &game.away_team_name,
                        &game.away_team_club,
                        &game.home_player1,
                        &game.home_player2.unwrap_or_default(),
                        &game.away_player1,
                        &game.away_player2.unwrap_or_default(),
                        &game.home_score.to_string(),
                        &game.away_score.to_string(),
                        &game.handicap_home.to_string(),
                        &game.handicap_away.to_string(),
                        &game.report_html.unwrap_or_default(),
                        &game.tx_time.to_rfc3339(),
                    ])?;
                }
                wtr.flush()?;
                pb.inc(1);
            }
            pb.finish();
        }

        let final_csvs = count_files_in_dir(CSV_OUTPUT_DIR, "csv").await?;

        Ok((final_csvs - initial_csvs, final_csvs))
    }
    .await;

    match result {
        Ok((new_files, total_files)) => {
            run.complete(new_files, total_files);
            run.log_to_questdb().await?;
            Ok((new_files, total_files))
        }
        Err(e) => {
            let error_msg = e.to_string();
            run.fail(&error_msg);
            run.log_to_questdb().await?;
            
            // Send notification for any error
            let notification_cmd = format!(
                "osascript -e 'display notification \"{}\" with title \"MKTTL Scraper Error\"'",
                error_msg.replace("\"", "\\\"")
            );
            if let Err(notify_err) = std::process::Command::new("sh")
                .arg("-c")
                .arg(&notification_cmd)
                .output()
            {
                eprintln!("Failed to send notification: {}", notify_err);
            }
            Err(e)
        }
    }
}

async fn get_postgres_record_count(database_url: &str) -> Result<i64> {
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(5)
        .connect(database_url)
        .await?;

    let record = sqlx::query!("SELECT COUNT(*) as count FROM matches")
        .fetch_one(&pool)
        .await?;

    Ok(record.count.unwrap_or(0))
}

async fn import_to_postgres() -> Result<(i64, i64)> {
    let mut run = ScraperRun::new("import-to-postgres");

    let result: Result<(i64, i64)> = async {
        let database_url = std::env::var("DATABASE_URL")
            .map_err(|e| anyhow::anyhow!("DATABASE_URL not set: {}", e))?;

        let initial_count = get_postgres_record_count(&database_url).await?;
        
        // Use the existing import functionality
        mkttl_match_card_scraper::import_to_postgres::import_csv_files(CSV_OUTPUT_DIR, &database_url)
            .await
            .map_err(|e| anyhow::anyhow!("Import failed: {}", e))?;

        let final_count = get_postgres_record_count(&database_url).await?;
        Ok((final_count - initial_count, final_count))
    }
    .await;

    match result {
        Ok((new_records, total_records)) => {
            run.complete(new_records, total_records);
            run.log_to_questdb().await?;
            Ok((new_records, total_records))
        }
        Err(e) => {
            run.fail(&e.to_string());
            run.log_to_questdb().await?;
            Err(e)
        }
    }
}

async fn import_to_questdb() -> Result<(i64, i64)> {
    let mut run = ScraperRun::new("import-to-questdb");

    let result: Result<(i64, i64)> = async {
        let initial_count = get_record_count().await?;
        
        // Use the existing import functionality with the data
        mkttl_match_card_scraper::import_to_questdb::import_csv_files(CSV_OUTPUT_DIR)
            .await
            .map_err(|e| anyhow::anyhow!("Import failed: {}", e))?;

        let final_count = get_record_count().await?;
        Ok((final_count - initial_count, final_count))
    }
    .await;

    match result {
        Ok((new_records, total_records)) => {
            run.complete(new_records, total_records);
            run.log_to_questdb().await?;
            Ok((new_records, total_records))
        }
        Err(e) => {
            run.fail(&e.to_string());
            run.log_to_questdb().await?;
            Err(e)
        }
    }
}

async fn update_match_scores() -> Result<(i64, i64)> {
    let mut run = ScraperRun::new("update-match-scores");

    let result: Result<(i64, i64)> = async {
        let database_url = std::env::var("DATABASE_URL")
            .map_err(|e| anyhow::anyhow!("DATABASE_URL not set: {}", e))?;

        let initial_count = sqlx::query!(
            "SELECT COUNT(*) as count FROM matches WHERE home_score IS NOT NULL AND away_score IS NOT NULL"
        )
        .fetch_one(&PgPoolOptions::new()
            .max_connections(5)
            .connect(&database_url)
            .await?)
        .await?
        .count
        .unwrap_or(0);

        // Run the match score updater
        MatchScoreUpdater::run_standalone(&database_url, HTML_FILES_DIR).await?;

        let final_count = sqlx::query!(
            "SELECT COUNT(*) as count FROM matches WHERE home_score IS NOT NULL AND away_score IS NOT NULL"
        )
        .fetch_one(&PgPoolOptions::new()
            .max_connections(5)
            .connect(&database_url)
            .await?)
        .await?
        .count
        .unwrap_or(0);

        Ok((final_count - initial_count, final_count))
    }
    .await;

    match result {
        Ok((new_records, total_records)) => {
            run.complete(new_records, total_records);
            run.log_to_questdb().await?;
            Ok((new_records, total_records))
        }
        Err(e) => {
            run.fail(&e.to_string());
            run.log_to_questdb().await?;
            Err(e)
        }
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    // Load .env file
    dotenv().ok();
    
    // Initialize tracing subscriber
    tracing_subscriber::fmt::init();

    println!("Starting HTML scraping...");
    let result = async {
        let (new_html, total_html) = scrape_html().await?;
        println!(
            "HTML scraping complete: {} new files, {} total",
            new_html, total_html
        );

        println!("\nStarting HTML to CSV conversion...");
        let (new_csv, total_csv) = process_html_to_csv().await?;
        println!(
            "CSV conversion complete: {} new files, {} total",
            new_csv, total_csv
        );

        println!("\nStarting database imports...");
        
        let quest_result = import_to_questdb().await;
        match &quest_result {
            Ok((new_records, total_records)) => {
                println!(
                    "QuestDB import complete: {} new records, {} total",
                    new_records, total_records
                );
            }
            Err(e) => {
                println!("QuestDB import failed: {}", e);
            }
        }

        let postgres_result = import_to_postgres().await;
        match &postgres_result {
            Ok((new_records, total_records)) => {
                println!(
                    "PostgreSQL import complete: {} new records, {} total",
                    new_records, total_records
                );
            }
            Err(e) => {
                println!("PostgreSQL import failed: {}", e);
            }
        }

        println!("\nStarting match score updates...");
        let score_update_result = update_match_scores().await;
        match &score_update_result {
            Ok((new_scores, total_scores)) => {
                println!(
                    "Match score updates complete: {} new/updated scores, {} total matches with scores",
                    new_scores, total_scores
                );
            }
            Err(e) => {
                println!("Match score updates failed: {}", e);
            }
        }

        if quest_result.is_err() || postgres_result.is_err() || score_update_result.is_err() {
            return Err(anyhow::anyhow!(
                "Database operations failed. QuestDB: {:?}, PostgreSQL: {:?}, Score Updates: {:?}",
                quest_result.err().unwrap_or_else(|| anyhow::anyhow!("Unknown error")),
                postgres_result.err().unwrap_or_else(|| anyhow::anyhow!("Unknown error")),
                score_update_result.err().unwrap_or_else(|| anyhow::anyhow!("Unknown error"))
            ));
        }

        Ok::<(), anyhow::Error>(())
    }
    .await;

    if let Err(e) = result {
        // Send notification on error
        let notification_cmd = format!(
            "osascript -e 'display notification \"Error: {}\" with title \"MKTTL Scraper Error\"'",
            e.to_string().replace("\"", "\\\"")
        );
        if let Err(notify_err) = std::process::Command::new("sh")
            .arg("-c")
            .arg(&notification_cmd)
            .output()
        {
            eprintln!("Failed to send notification: {}", notify_err);
        }
        eprintln!("Error running scraper: {}", e);
        return Err(e);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use tokio::test;

    #[test]
    async fn test_count_files_in_dir() {
        // Create a temp directory with some test files
        let test_dir = "test_files";
        fs::create_dir_all(test_dir).unwrap();
        fs::write(format!("{}/test1.html", test_dir), "test").unwrap();
        fs::write(format!("{}/test2.html", test_dir), "test").unwrap();
        fs::write(format!("{}/test3.txt", test_dir), "test").unwrap();

        let count = count_files_in_dir(test_dir, "html").await.unwrap();
        assert_eq!(count, 2);

        // Cleanup
        fs::remove_dir_all(test_dir).unwrap();
    }

    #[test]
    async fn test_get_record_count() {
        match get_record_count().await {
            Ok(count) => {
                println!("Successfully got record count: {}", count);
                assert!(count >= 0);
            }
            Err(e) => {
                eprintln!("Failed to get record count: {}", e);
                panic!("Test failed: {}", e);
            }
        }
    }
}

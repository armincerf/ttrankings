use crate::parse_csv::{get_csv_files, TableTennisGame};
use anyhow::Result;
use chrono::{DateTime, Utc};
use indicatif::{ProgressBar, ProgressStyle};
use sqlx::{
    postgres::PgPoolOptions,
    types::time::{Date, OffsetDateTime},
    Postgres, Transaction,
};
use std::collections::HashMap;
use tokio::fs;
use sha2::{Digest, Sha256};
use std::sync::Arc;
use dotenv;

// Custom error type for import operations
#[derive(Debug, thiserror::Error)]
pub enum ImportError {
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("CSV parse error: {0}")]
    CsvParse(#[from] csv::Error),
    #[error("Other error: {0}")]
    Other(String),
}

// Enum to represent competition type in database
#[derive(Debug, sqlx::Type)]
#[sqlx(type_name = "competition_type_enum", rename_all = "snake_case")]
pub enum CompetitionType {
    #[sqlx(rename = "MKTTL League")]
    MkttlLeague,
    #[sqlx(rename = "MKTTL Challenge Cup")]
    MkttlChallengeCup,
}

// Struct to hold cached IDs to minimize database queries
#[derive(Default)]
struct CachedIds {
    leagues: HashMap<String, i64>,
    divisions: HashMap<(i64, String), i64>,
    seasons: HashMap<(i64, String), i64>,
    players: HashMap<String, i64>,
    venues: HashMap<String, i64>,
    clubs: HashMap<(i64, String), i64>,
    teams: HashMap<(i64, i64, i64, String), i64>,
}

impl CachedIds {
    fn new() -> Self {
        Self::default()
    }
}

// Function to get or create a league
async fn get_or_create_league<'c>(
    tx: &mut Transaction<'_, Postgres>,
    cache: &mut CachedIds,
    name: &str,
) -> Result<i64, ImportError> {
    if let Some(&id) = cache.leagues.get(name) {
        return Ok(id);
    }

    let league_id = sqlx::query!(
        r#"
        INSERT INTO league (name)
        VALUES ($1)
        ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
        RETURNING id
        "#,
        name
    )
    .fetch_one(&mut **tx)
    .await?
    .id;

    cache.leagues.insert(name.to_string(), league_id);
    Ok(league_id)
}

// Function to get or create a division
async fn get_or_create_division<'c>(
    tx: &mut Transaction<'_, Postgres>,
    cache: &mut CachedIds,
    league_id: i64,
    name: &str,
) -> Result<i64, ImportError> {
    // Clean up division name - remove "Group Round (Group X)" format
    let clean_name = if name.starts_with("Group Round (Group ") {
        name.replace("Group Round (Group ", "Group ").replace(")", "")
    } else {
        name.to_string()
    };

    let key = (league_id, clean_name.clone());
    if let Some(&id) = cache.divisions.get(&key) {
        return Ok(id);
    }

    let division_id = sqlx::query!(
        r#"
        INSERT INTO division (league_id, name)
        VALUES ($1, $2)
        ON CONFLICT (league_id, name) DO UPDATE SET name = EXCLUDED.name
        RETURNING id
        "#,
        league_id,
        clean_name
    )
    .fetch_one(&mut **tx)
    .await?
    .id;

    cache.divisions.insert(key, division_id);
    Ok(division_id)
}

// Function to get or create a season
async fn get_or_create_season<'c>(
    tx: &mut Transaction<'_, Postgres>,
    cache: &mut CachedIds,
    league_id: i64,
    name: &str,
) -> Result<i64, ImportError> {
    let key = (league_id, name.to_string());
    if let Some(&id) = cache.seasons.get(&key) {
        return Ok(id);
    }

    let season_id = sqlx::query!(
        r#"
        INSERT INTO season (league_id, name)
        VALUES ($1, $2)
        ON CONFLICT (league_id, name) DO UPDATE SET name = EXCLUDED.name
        RETURNING id
        "#,
        league_id,
        name
    )
    .fetch_one(&mut **tx)
    .await?
    .id;

    cache.seasons.insert(key, season_id);
    Ok(season_id)
}

// Function to get or create a player
async fn get_or_create_player<'c>(
    tx: &mut Transaction<'_, Postgres>,
    cache: &mut CachedIds,
    name: &str,
) -> Result<i64, ImportError> {
    if let Some(&id) = cache.players.get(name) {
        return Ok(id);
    }

    let player_id = sqlx::query!(
        r#"
        INSERT INTO player (name)
        VALUES ($1)
        ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
        RETURNING id
        "#,
        name
    )
    .fetch_one(&mut **tx)
    .await?
    .id;

    cache.players.insert(name.to_string(), player_id);
    Ok(player_id)
}

// Function to get or create a venue
async fn get_or_create_venue<'c>(
    tx: &mut Transaction<'_, Postgres>,
    cache: &mut CachedIds,
    name: &str,
) -> Result<i64, ImportError> {
    if let Some(&id) = cache.venues.get(name) {
        return Ok(id);
    }

    // Note: Since we don't have address information in the CSV, we'll use the name as the address
    let venue_id = sqlx::query!(
        r#"
        INSERT INTO venue (name, address)
        VALUES ($1, $1)
        ON CONFLICT (name, address) DO UPDATE SET name = EXCLUDED.name
        RETURNING id
        "#,
        name
    )
    .fetch_one(&mut **tx)
    .await?
    .id;

    cache.venues.insert(name.to_string(), venue_id);
    Ok(venue_id)
}

// Function to get or create a club
async fn get_or_create_club<'c>(
    tx: &mut Transaction<'_, Postgres>,
    cache: &mut CachedIds,
    league_id: i64,
    venue_id: i64,
    name: &str,
) -> Result<i64, ImportError> {
    let key = (league_id, name.to_string());
    if let Some(&id) = cache.clubs.get(&key) {
        return Ok(id);
    }

    let club_id = sqlx::query!(
        r#"
        INSERT INTO club (league_id, venue_id, name)
        VALUES ($1, $2, $3)
        ON CONFLICT (league_id, name) DO UPDATE SET venue_id = EXCLUDED.venue_id
        RETURNING id
        "#,
        league_id,
        venue_id,
        name
    )
    .fetch_one(&mut **tx)
    .await?
    .id;

    cache.clubs.insert(key, club_id);
    Ok(club_id)
}

// Function to get or create a team
async fn get_or_create_team<'c>(
    tx: &mut Transaction<'_, Postgres>,
    cache: &mut CachedIds,
    league_id: i64,
    season_id: i64,
    division_id: i64,
    club_id: i64,
    team_name: &str,
) -> Result<i64, ImportError> {
    // Get the club name from the database
    let club_name = sqlx::query!(
        "SELECT name FROM club WHERE id = $1",
        club_id
    )
    .fetch_one(&mut **tx)
    .await?
    .name;

    // Remove any duplicate club name prefix from the team name
    let clean_team_name = if team_name.starts_with(&format!("{} {}", club_name, club_name)) {
        team_name.replacen(&format!("{} ", club_name), "", 1)
    } else {
        team_name.to_string()
    };

    let key = (league_id, season_id, club_id, clean_team_name.clone());
    if let Some(&id) = cache.teams.get(&key) {
        return Ok(id);
    }

    let team_id = sqlx::query!(
        r#"
        INSERT INTO team (league_id, season_id, division_id, club_id, name)
        VALUES ($1, $2, $3, $4, $5)
        ON CONFLICT (league_id, season_id, club_id, name) 
        DO UPDATE SET division_id = EXCLUDED.division_id
        RETURNING id
        "#,
        league_id,
        season_id,
        division_id,
        club_id,
        clean_team_name
    )
    .fetch_one(&mut **tx)
    .await?
    .id;

    cache.teams.insert(key, team_id);
    Ok(team_id)
}

// Function to create a match
async fn create_match<'c>(
    tx: &mut Transaction<'_, Postgres>,
    league_id: i64,
    season_id: i64,
    division_id: i64,
    home_team_id: i64,
    away_team_id: i64,
    venue_id: i64,
    match_date: DateTime<Utc>,
    competition_type: CompetitionType,
    csv_reference: &str,
) -> Result<i64, ImportError> {
    // Convert DateTime<Utc> to OffsetDateTime
    let offset_dt = OffsetDateTime::from_unix_timestamp(match_date.timestamp())
        .map_err(|e| ImportError::Other(e.to_string()))?;

    // Normalize the csv_reference to use relative path
    let normalized_reference = if let Some(idx) = csv_reference.find("html_files/") {
        &csv_reference[idx..]
    } else {
        csv_reference
    };

    let match_id = sqlx::query!(
        r#"
        INSERT INTO matches (
            league_id, season_id, division_id, match_date,
            home_team_id, away_team_id, venue_id, competition_type,
            csv_reference
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        ON CONFLICT (csv_reference) DO UPDATE SET
            league_id = EXCLUDED.league_id,
            season_id = EXCLUDED.season_id,
            division_id = EXCLUDED.division_id,
            match_date = EXCLUDED.match_date,
            home_team_id = EXCLUDED.home_team_id,
            away_team_id = EXCLUDED.away_team_id,
            venue_id = EXCLUDED.venue_id,
            competition_type = EXCLUDED.competition_type
        RETURNING id
        "#,
        league_id,
        season_id,
        division_id,
        offset_dt,
        home_team_id,
        away_team_id,
        venue_id,
        competition_type as _,
        normalized_reference
    )
    .fetch_one(&mut **tx)
    .await?
    .id;

    Ok(match_id)
}

// Function to create a singles set from multiple games
async fn create_singles_set<'c>(
    tx: &mut Transaction<'_, Postgres>,
    match_id: i64,
    home_player_id: i64,
    away_player_id: i64,
    games: &[TableTennisGame],
    set_date: DateTime<Utc>,
) -> Result<(), ImportError> {
    // Build a scores object with all games in this set
    let mut scores = serde_json::Map::new();
    let mut home_games_won = 0;
    let mut away_games_won = 0;

    // Create an entry for each game in the set
    for (i, game) in games.iter().enumerate() {
        let game_key = format!("game{}", i + 1);
        let game_scores = serde_json::json!({
            "home": game.home_score,
            "away": game.away_score
        });
        scores.insert(game_key, game_scores);

        // Count who won this game
        if game.home_score > game.away_score {
            home_games_won += 1;
        } else if game.away_score > game.home_score {
            away_games_won += 1;
        }
    }

    // Determine the overall winner based on games won
    let winner_id = if home_games_won > away_games_won {
        home_player_id
    } else {
        away_player_id
    };

    // Convert DateTime<Utc> to Date for the database
    let date = Date::from_julian_day(set_date.timestamp() as i32 / 86400 + 2440588)
        .map_err(|e| ImportError::Other(e.to_string()))?;

    let scores_value = serde_json::Value::Object(scores);

    sqlx::query!(
        r#"
        INSERT INTO singles_set (
            set_date, match_id, home_player, away_player,
            scores, winner
        )
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (match_id, home_player, away_player, set_date) 
        DO UPDATE SET
            set_date = EXCLUDED.set_date,
            match_id = EXCLUDED.match_id,
            home_player = EXCLUDED.home_player,
            away_player = EXCLUDED.away_player,
            scores = EXCLUDED.scores,
            winner = EXCLUDED.winner
        "#,
        date,
        match_id,
        home_player_id,
        away_player_id,
        scores_value,
        winner_id
    )
    .execute(&mut **tx)
    .await?;

    Ok(())
}

// Function to create a doubles set from multiple games
async fn create_doubles_set<'c>(
    tx: &mut Transaction<'_, Postgres>,
    match_id: i64,
    home_player1_id: i64,
    home_player2_id: i64,
    away_player1_id: i64,
    away_player2_id: i64,
    games: &[TableTennisGame],
    set_date: DateTime<Utc>,
) -> Result<(), ImportError> {
    // Build a scores object with all games in this set
    let mut scores = serde_json::Map::new();
    let mut home_games_won = 0;
    let mut away_games_won = 0;

    // Create an entry for each game in the set
    for (i, game) in games.iter().enumerate() {
        let game_key = format!("game{}", i + 1);
        let game_scores = serde_json::json!({
            "home": game.home_score,
            "away": game.away_score
        });
        scores.insert(game_key, game_scores);

        // Count who won this game
        if game.home_score > game.away_score {
            home_games_won += 1;
        } else if game.away_score > game.home_score {
            away_games_won += 1;
        }
    }

    // Convert DateTime<Utc> to Date for the database
    let date = Date::from_julian_day(set_date.timestamp() as i32 / 86400 + 2440588)
        .map_err(|e| ImportError::Other(e.to_string()))?;

    let scores_value = serde_json::Value::Object(scores);
    let home_won = home_games_won > away_games_won;

    sqlx::query!(
        r#"
        INSERT INTO doubles_set (
            set_date, match_id, home_player1, home_player2,
            away_player1, away_player2, scores, home_won
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (match_id, home_player1, home_player2, away_player1, away_player2, set_date)
        DO UPDATE SET
            set_date = EXCLUDED.set_date,
            match_id = EXCLUDED.match_id,
            home_player1 = EXCLUDED.home_player1,
            home_player2 = EXCLUDED.home_player2,
            away_player1 = EXCLUDED.away_player1,
            away_player2 = EXCLUDED.away_player2,
            scores = EXCLUDED.scores,
            home_won = EXCLUDED.home_won
        "#,
        date,
        match_id,
        home_player1_id,
        home_player2_id,
        away_player1_id,
        away_player2_id,
        scores_value,
        home_won
    )
    .execute(tx.as_mut())
    .await?;

    Ok(())
}

pub async fn import_csv_files(csv_dir: &str, database_url: &str) -> Result<(), ImportError> {
    // Set up database connection pool
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(database_url)
        .await
        .map_err(ImportError::Database)?;

    // Get list of CSV files
    let csv_files = get_csv_files(csv_dir, None)
        .await
        .map_err(|e| ImportError::Other(e.to_string()))?;

    let mut files_to_process = Vec::new();
    let mut future_files = Vec::new();
    for file_path in &csv_files {
        // Read file into memory (or in streaming fashion)
        let mut file = fs::File::open(file_path).await?;
        let mut content = Vec::new();
        use tokio::io::AsyncReadExt;
        file.read_to_end(&mut content).await?;

        // Compute a SHA256 hash of the file contents
        let mut hasher = Sha256::new();
        hasher.update(&content);
        let file_hash = format!("{:x}", hasher.finalize());

        let metadata = fs::metadata(file_path).await?;
        let modified_time = metadata.modified().unwrap_or_else(|_| std::time::SystemTime::UNIX_EPOCH);
        let modified_time_utc: chrono::DateTime<chrono::Utc> = modified_time.into();

        // Extract date from filename (format: number_number_YYYY_MM_DD.csv)
        let file_name = file_path.file_name()
            .and_then(|n| n.to_str())
            .unwrap_or_default();
        
        // Extract date parts from filename
        let mut parts = file_name.split('_');
        // Skip the first two parts (numbers)
        parts.next();
        parts.next();
        // Get year, month, day
        if let (Some(year), Some(month), Some(day_with_ext)) = (parts.next(), parts.next(), parts.next()) {
            let day = day_with_ext.strip_suffix(".csv").unwrap_or(day_with_ext);
            let date_str = format!("{}-{}-{}", year, month, day);
            
            if let Ok(file_date) = chrono::NaiveDate::parse_from_str(&date_str, "%Y-%m-%d") {
                let today = chrono::Local::now().date_naive();
                if file_date > today {
                    future_files.push(file_path.clone());
                    continue; // Skip files with future dates
                }
            }
        }

        // Check existing record in csv_import_log
        let file_path_str = file_path.to_string_lossy().into_owned();
        let file_path_ref = file_path_str.as_str();

        if let Some(record) = sqlx::query!(
            r#"
            SELECT last_modified_time, file_hash
            FROM csv_import_log
            WHERE file_path = $1
            "#,
            file_path_ref
        )
        .fetch_optional(&pool)
        .await?
        {
            // If last_modified_time hasn't changed AND the hash is the same, skip it
            let already_imported_hash = record.file_hash.as_deref().unwrap_or("");
            if record.last_modified_time.unix_timestamp() >= modified_time_utc.timestamp() && already_imported_hash == file_hash {
                continue;
            }
        }

        // If new or changed, add it to files_to_process along with the computed SHA
        files_to_process.push((file_path.clone(), file_hash, modified_time_utc));
    }

    let total_files = files_to_process.len();
    println!("Found {} CSV files to process", total_files);

    let pb = ProgressBar::new(total_files as u64);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} files ({eta})")
            .unwrap(),
    );

    let mut cache = CachedIds::new();
    let errors = Arc::new(tokio::sync::Mutex::new(Vec::<String>::new()));

    for (file_path, file_hash, modified_time_utc) in files_to_process {
        let mut tx = pool.begin().await.map_err(ImportError::Database)?;

        // Delete any existing record for this file path before inserting
        let file_path_str = file_path.to_string_lossy().into_owned();
        sqlx::query!(
            r#"
            DELETE FROM csv_import_log
            WHERE file_path = $1
            "#,
            &file_path_str
        )
        .execute(&mut *tx)
        .await?;

        let file_content = fs::read_to_string(&file_path)
            .await
            .map_err(ImportError::Io)?;

        let mut rdr = csv::Reader::from_reader(file_content.as_bytes());

        // First, group games by match_id and set_number
        let mut matches: HashMap<String, HashMap<i64, Vec<TableTennisGame>>> = HashMap::new();
        
        for result in rdr.deserialize() {
            let game: TableTennisGame = result.map_err(ImportError::CsvParse)?;
            matches
                .entry(game.match_id.clone())
                .or_default()
                .entry(game.set_number)
                .or_default()
                .push(game);
        }

        if matches.is_empty() && !future_files.contains(&file_path) {
            errors.lock().await.push(format!(
                "Warning: No rows processed in {:?}",
                file_path
            ));
        }

        // Process each match
        for (match_id_str, sets) in matches {
            if sets.is_empty() {
                continue;
            }

            // Use the first game of the first set for common match data
            let first_set = sets.values().next().unwrap();
            let first_game = &first_set[0];

            // Convert competition type string to enum
            let competition_type = match first_game.competition_type.as_str() {
                "MKTTL League" => CompetitionType::MkttlLeague,
                "MKTTL Challenge Cup" => CompetitionType::MkttlChallengeCup,
                other => {
                    return Err(ImportError::Other(format!(
                        "Invalid competition type '{}' in file {:?}. Expected 'MKTTL League' or 'MKTTL Challenge Cup'",
                        other,
                        file_path
                    )))
                }
            };

            // Get or create all required entities
            let league_id = get_or_create_league(&mut tx, &mut cache, "MKTTL").await?;
            let division_id =
                get_or_create_division(&mut tx, &mut cache, league_id, &first_game.division).await?;
            let season_id =
                get_or_create_season(&mut tx, &mut cache, league_id, &first_game.season).await?;

            // Venue handling
            let venue_id = get_or_create_venue(&mut tx, &mut cache, &first_game.venue).await?;

            // Home team handling
            let home_club_id = get_or_create_club(&mut tx, &mut cache, league_id, venue_id, &first_game.home_team_club).await?;
            let home_team_id = get_or_create_team(
                &mut tx,
                &mut cache,
                league_id,
                season_id,
                division_id,
                home_club_id,
                &first_game.home_team_name,
            )
            .await?;

            // Away team handling
            let away_club_id = get_or_create_club(&mut tx, &mut cache, league_id, venue_id, &first_game.away_team_club).await?;
            let away_team_id = get_or_create_team(
                &mut tx,
                &mut cache,
                league_id,
                season_id,
                division_id,
                away_club_id,
                &first_game.away_team_name,
            )
            .await?;

            // Create match
            let match_id = create_match(
                &mut tx,
                league_id,
                season_id,
                division_id,
                home_team_id,
                away_team_id,
                venue_id,
                first_game.timestamp,
                competition_type,
                &match_id_str,
            )
            .await?;

            // Process each set in the match
            for (_, games) in sets {
                if games.is_empty() {
                    continue;
                }

                // Use the first game for player information
                let first_game = &games[0];
                let home_player1_id =
                    get_or_create_player(&mut tx, &mut cache, &first_game.home_player1).await?;
                let away_player1_id =
                    get_or_create_player(&mut tx, &mut cache, &first_game.away_player1).await?;

                // Check if this is a doubles match
                let is_doubles = !first_game.home_player2.is_empty() && !first_game.away_player2.is_empty();

                if is_doubles {
                    let home_player2_id =
                        get_or_create_player(&mut tx, &mut cache, &first_game.home_player2).await?;
                    let away_player2_id =
                        get_or_create_player(&mut tx, &mut cache, &first_game.away_player2).await?;

                    create_doubles_set(
                        &mut tx,
                        match_id,
                        home_player1_id,
                        home_player2_id,
                        away_player1_id,
                        away_player2_id,
                        &games,
                        first_game.timestamp,
                    )
                    .await?;

                    // Create player-league associations for all players
                    sqlx::query!(
                        r#"
                        INSERT INTO player_league (player_id, league_id)
                        VALUES ($1, $2), ($3, $2), ($4, $2), ($5, $2)
                        ON CONFLICT DO NOTHING
                        "#,
                        home_player1_id,
                        league_id,
                        home_player2_id,
                        away_player1_id,
                        away_player2_id
                    )
                    .execute(tx.as_mut())
                    .await?;
                } else {
                    create_singles_set(
                        &mut tx,
                        match_id,
                        home_player1_id,
                        away_player1_id,
                        &games,
                        first_game.timestamp,
                    )
                    .await?;

                    // Create player-league associations
                    sqlx::query!(
                        r#"
                        INSERT INTO player_league (player_id, league_id)
                        VALUES ($1, $2), ($3, $2)
                        ON CONFLICT DO NOTHING
                        "#,
                        home_player1_id,
                        league_id,
                        away_player1_id
                    )
                    .execute(tx.as_mut())
                    .await?;
                }
            }
        }

        // after successful import, upsert
        let modified_time_offset = OffsetDateTime::from_unix_timestamp(modified_time_utc.timestamp()).unwrap();
        sqlx::query!(
            r#"
            INSERT INTO csv_import_log (file_path, last_modified_time, file_hash, last_imported_at)
            VALUES ($1, $2, $3, NOW())
            "#,
            &file_path_str,
            modified_time_offset,
            file_hash
        )
        .execute(&mut *tx)
        .await?;

        tx.commit().await.map_err(ImportError::Database)?;
        pb.inc(1);
    }

    pb.finish_with_message("Import complete");

    // Display any errors that occurred
    let errors = errors.lock().await;
    if !errors.is_empty() {
        println!("\nErrors and Warnings:");
        for error in errors.iter() {
            eprintln!("{}", error);
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use sqlx::postgres::PgPoolOptions;
    use std::env;
    use dotenv;

    #[tokio::test]
    async fn test_database_connection() {
        // Load .env file
        dotenv::dotenv().ok();
        
        let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&database_url)
            .await
            .expect("Failed to connect to database");

        // Test a simple query
        let result = sqlx::query!("SELECT 1 as one")
            .fetch_one(&pool)
            .await
            .expect("Failed to execute query");

        assert_eq!(result.one.unwrap(), 1);
    }
}

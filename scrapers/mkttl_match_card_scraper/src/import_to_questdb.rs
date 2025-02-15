use chrono::DateTime;
use indicatif::{ProgressBar, ProgressStyle};
use questdb::{
    Result as QuestResult,
    ingress::{Buffer, Sender, TimestampNanos, TableName, ColumnName},
};
use reqwest;
use serde_json::Value;
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::error::Error;
use tokio::sync::{Mutex, Semaphore};
use tokio::task::JoinError;
use crate::parse_csv::{get_csv_files, parse_csv_file, CsvParseError, TableTennisGame};

// Define a custom error type for our import operations
#[derive(Debug)]
pub enum ImportError {
    Io(std::io::Error),
    Quest(questdb::Error),
    Reqwest(reqwest::Error),
    CsvParse(CsvParseError),
    ChronoParse(chrono::ParseError),
    Other(Box<dyn Error + Send + Sync>),
}

impl std::fmt::Display for ImportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ImportError::Io(e) => write!(f, "IO error: {}", e),
            ImportError::Quest(e) => write!(f, "QuestDB error: {}", e),
            ImportError::Reqwest(e) => write!(f, "Reqwest error: {}", e),
            ImportError::CsvParse(e) => write!(f, "CSV parse error: {}", e),
            ImportError::ChronoParse(e) => write!(f, "DateTime parse error: {}", e),
            ImportError::Other(e) => write!(f, "Other error: {}", e),
        }
    }
}

impl Error for ImportError {}

impl From<std::io::Error> for ImportError {
    fn from(err: std::io::Error) -> ImportError {
        ImportError::Io(err)
    }
}

impl From<questdb::Error> for ImportError {
    fn from(err: questdb::Error) -> ImportError {
        ImportError::Quest(err)
    }
}

impl From<reqwest::Error> for ImportError {
    fn from(err: reqwest::Error) -> ImportError {
        ImportError::Reqwest(err)
    }
}

impl From<CsvParseError> for ImportError {
    fn from(err: CsvParseError) -> ImportError {
        ImportError::CsvParse(err)
    }
}

impl From<chrono::ParseError> for ImportError {
    fn from(err: chrono::ParseError) -> ImportError {
        ImportError::ChronoParse(err)
    }
}

impl From<Box<dyn Error + Send + Sync>> for ImportError {
    fn from(err: Box<dyn Error + Send + Sync>) -> ImportError {
        ImportError::Other(err)
    }
}

impl From<JoinError> for ImportError {
    fn from(err: JoinError) -> ImportError {
        ImportError::Other(Box::new(err))
    }
}

const MAX_BUFFER_SIZE: usize = 100_000; // Adjust based on your needs
const FLUSH_INTERVAL: Duration = Duration::from_secs(5);

// Wrapper struct for thread-safe QuestDB components
struct QuestDBClient {
    sender: Arc<Mutex<Sender>>,
    buffer: Arc<Mutex<Buffer>>,
}

impl QuestDBClient {
    fn new(sender: Sender, buffer: Buffer) -> Self {
        Self {
            sender: Arc::new(Mutex::new(sender)),
            buffer: Arc::new(Mutex::new(buffer)),
        }
    }
}

async fn process_games(games: Vec<TableTennisGame>, client: Arc<QuestDBClient>, progress: ProgressBar) -> QuestResult<u64> {
    let mut rows_processed = 0;
    let mut last_flush = Instant::now();

    // Create table and column names
    let table_name = TableName::new("table_tennis_games")?;
    let competition_type = ColumnName::new("competition_type")?;
    let season = ColumnName::new("season")?;
    let division = ColumnName::new("division")?;
    let venue = ColumnName::new("venue")?;
    let home_team_name = ColumnName::new("home_team_name")?;
    let home_team_club = ColumnName::new("home_team_club")?;
    let away_team_name = ColumnName::new("away_team_name")?;
    let away_team_club = ColumnName::new("away_team_club")?;
    let home_player1 = ColumnName::new("home_player1")?;
    let home_player2 = ColumnName::new("home_player2")?;
    let away_player1 = ColumnName::new("away_player1")?;
    let away_player2 = ColumnName::new("away_player2")?;
    let match_id = ColumnName::new("match_id")?;
    let set_number = ColumnName::new("set_number")?;
    let game_number = ColumnName::new("game_number")?;
    let home_score = ColumnName::new("home_score")?;
    let away_score = ColumnName::new("away_score")?;
    let handicap_home = ColumnName::new("handicap_home")?;
    let handicap_away = ColumnName::new("handicap_away")?;
    let report_html = ColumnName::new("report_html")?;
    let tx_time = ColumnName::new("tx_time")?;

    for game in games {
        let ts_nanos = game.timestamp.timestamp_nanos_opt().unwrap();
        let tx_nanos = game.tx_time.timestamp_nanos_opt().unwrap();

        // Lock the buffer for modification
        let mut buffer = client.buffer.lock().await;
        
        // Build the row
        buffer
            .table(table_name)?
            .symbol(competition_type, &game.competition_type)?
            .symbol(season, &game.season)?
            .symbol(division, &game.division)?
            .symbol(venue, &game.venue)?
            .symbol(home_team_name, &game.home_team_name)?
            .symbol(home_team_club, &game.home_team_club)?
            .symbol(away_team_name, &game.away_team_name)?
            .symbol(away_team_club, &game.away_team_club)?
            .symbol(home_player1, &game.home_player1)?
            .symbol(home_player2, &game.home_player2)?
            .symbol(away_player1, &game.away_player1)?
            .symbol(away_player2, &game.away_player2)?
            .symbol(match_id, &game.match_id)?
            .column_i64(set_number, game.set_number)?
            .column_i64(game_number, game.game_number)?
            .column_i64(home_score, game.home_score)?
            .column_i64(away_score, game.away_score)?
            .column_i64(handicap_home, game.handicap_home)?
            .column_i64(handicap_away, game.handicap_away)?
            .column_str(report_html, &game.report_html)?
            .column_ts(tx_time, TimestampNanos::new(tx_nanos))?
            .at(TimestampNanos::new(ts_nanos))?;

        rows_processed += 1;
        progress.inc(1);

        // Check if we should flush based on buffer size or time
        if buffer.len() >= MAX_BUFFER_SIZE || last_flush.elapsed() >= FLUSH_INTERVAL {
            let mut sender = client.sender.lock().await;
            sender.flush(&mut buffer)?;
            last_flush = Instant::now();
        }
        
        // Drop the buffer lock
        drop(buffer);
    }

    // Final flush for any remaining data
    let mut buffer = client.buffer.lock().await;
    if buffer.len() > 0 {
        let mut sender = client.sender.lock().await;
        sender.flush(&mut buffer)?;
    }

    Ok(rows_processed)
}

async fn get_latest_successful_import() -> Result<Option<DateTime<chrono::Utc>>, ImportError> {
    let client = reqwest::Client::new();
    let query = "select run_end_time from scraper_runs where step = 'import-to-questdb' and status = 'success' and new_files_count > 0 order by run_end_time desc limit 1;";
    
    let response = client
        .get("http://localhost:9000/exec")
        .query(&[("query", query)])
        .send()
        .await?;

    let json: Value = response.json().await?;
    
    // Parse the response
    if let Some(dataset) = json.get("dataset") {
        if let Some(rows) = dataset.as_array() {
            if let Some(first_row) = rows.first() {
                if let Some(timestamp_str) = first_row[0].as_str() {
                    return Ok(Some(DateTime::parse_from_rfc3339(timestamp_str)?.into()));
                }
            }
        }
    }
    
    Ok(None)
}

pub async fn import_csv_files(csv_dir: &str) -> Result<(), ImportError> {
    // Get the latest successful import timestamp
    let latest_import = get_latest_successful_import().await?;
    
    // Get list of CSV files to process
    let csv_files = get_csv_files(csv_dir, latest_import).await?;
    let total_files = csv_files.len();

    if total_files > 0 {
        let pb = ProgressBar::new(total_files as u64);
        pb.set_style(
            ProgressStyle::default_bar()
                .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} files ({eta})")
                .unwrap(),
        );

        // Initialize QuestDB connection
        let connection_string = format!("http::addr={};", "localhost:9000");
        let sender = Sender::from_conf(&connection_string)?;
        let buffer = Buffer::new();
        let client = Arc::new(QuestDBClient::new(sender, buffer));

        // Create a vector to collect errors
        let errors = Arc::new(Mutex::new(Vec::new()));
        
        // Create a semaphore to limit concurrent file operations
        let semaphore = Arc::new(Semaphore::new(50));

        let mut handles = Vec::new();
        for file in csv_files {
            let client = Arc::clone(&client);
            let errors = Arc::clone(&errors);
            let semaphore = Arc::clone(&semaphore);

            let handle = tokio::spawn(async move {
                // Acquire a permit from the semaphore before processing the file
                let _permit = semaphore.acquire().await.unwrap();
                
                let file_path = file.clone();

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
                            return 0; // Skip files with future dates
                        }
                    }
                }
                
                // Create a hidden progress bar for the file
                let file_progress = ProgressBar::hidden();
                
                match parse_csv_file(file).await {
                    Ok(games) => {
                        match process_games(games, Arc::clone(&client), file_progress).await {
                            Ok(rows) => {
                                if rows == 0 {
                                    errors.lock().await.push(format!("Warning: No rows processed in {:?}", file_path));
                                }
                                rows
                            }
                            Err(e) => {
                                errors.lock().await.push(format!("Error processing games from {:?}: {}", file_path, e));
                                0
                            }
                        }
                    }
                    Err(e) => {
                        errors.lock().await.push(format!("Error parsing CSV file {:?}: {}", file_path, e));
                        0
                    }
                }
            });
            handles.push(handle);
        }

        let mut total_rows = 0;
        for handle in handles {
            total_rows += handle.await?;
            pb.inc(1);
        }

        pb.finish_with_message(format!("Processed {} total rows", total_rows));

        // Display any errors that occurred
        let errors = errors.lock().await;
        if !errors.is_empty() {
            println!("\nErrors and Warnings:");
            for error in errors.iter() {
                eprintln!("{}", error);
            }
        }
    } else {
        eprintln!("No CSV files found in {}", csv_dir);
    }

    Ok(())
}
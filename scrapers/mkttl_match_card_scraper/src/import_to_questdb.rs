use chrono::{DateTime, ParseError};
use clap::Parser;
use csv::Reader;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use questdb::{
    Result as QuestResult,
    ingress::{Buffer, Sender, TimestampNanos, TableName, ColumnName},
};
use reqwest;
use serde_json::Value;
use std::fs::{self, File};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime};
use std::error::Error;
use tokio::sync::{Mutex, Semaphore};
use tokio::task::{self, JoinError};

// Define a custom error type for our import operations
#[derive(Debug)]
pub enum ImportError {
    Io(std::io::Error),
    Quest(questdb::Error),
    Reqwest(reqwest::Error),
    ChronoParse(ParseError),
    Other(Box<dyn Error + Send + Sync>),
}

impl std::fmt::Display for ImportError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ImportError::Io(e) => write!(f, "IO error: {}", e),
            ImportError::Quest(e) => write!(f, "QuestDB error: {}", e),
            ImportError::Reqwest(e) => write!(f, "Reqwest error: {}", e),
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

impl From<ParseError> for ImportError {
    fn from(err: ParseError) -> ImportError {
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

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Run with a single test file
    #[arg(long)]
    test_run: bool,

    /// Process all CSV files in the directory
    #[arg(long)]
    run_all: bool,

    /// Directory containing CSV files
    #[arg(long, default_value = "parsed_html_output")]
    input_dir: String,

    /// QuestDB host address
    #[arg(long, default_value = "localhost:9000")]
    host: String,

    /// Number of parallel workers
    #[arg(long, default_value = "4")]
    workers: usize,
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

async fn process_csv_file(file_path: PathBuf, client: Arc<QuestDBClient>, progress: ProgressBar) -> QuestResult<u64> {
    let mut rows_processed = 0;
    let mut last_flush = Instant::now();
    let file = match File::open(&file_path) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("Failed to open file {:?}: {}", file_path, e);
            return Ok(0);
        }
    };
    
    let mut rdr = Reader::from_reader(file);
    progress.set_message(format!("Processing {:?}", file_path.file_name().unwrap_or_default()));

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
    let leg_number = ColumnName::new("leg_number")?;
    let home_score = ColumnName::new("home_score")?;
    let away_score = ColumnName::new("away_score")?;
    let handicap_home = ColumnName::new("handicap_home")?;
    let handicap_away = ColumnName::new("handicap_away")?;
    let report_html = ColumnName::new("report_html")?;
    let tx_time = ColumnName::new("tx_time")?;

    for result in rdr.records() {
        let record = match result {
            Ok(r) => r,
            Err(e) => {
                eprintln!("Error reading record in {:?}: {}", file_path, e);
                continue;
            }
        };

        let timestamp = match record.get(0) {
            Some(t) => t,
            None => {
                eprintln!("Missing timestamp in {:?}", file_path);
                continue;
            }
        };

        let tx_timestamp = match record.get(21) {
            Some(t) => t,
            None => {
                eprintln!("Missing tx_time in {:?}", file_path);
                continue;
            }
        };

        let timestamp = match DateTime::parse_from_rfc3339(timestamp) {
            Ok(t) => t,
            Err(e) => {
                eprintln!("Invalid timestamp in {:?}: {}", file_path, e);
                continue;
            }
        };

        let tx_timestamp = match DateTime::parse_from_rfc3339(tx_timestamp) {
            Ok(t) => t,
            Err(e) => {
                eprintln!("Invalid tx_time in {:?}: {}", file_path, e);
                continue;
            }
        };
        
        let ts_nanos = timestamp.timestamp_nanos_opt().unwrap_or(0);
        let tx_nanos = tx_timestamp.timestamp_nanos_opt().unwrap_or(0);

        // Lock the buffer for modification
        let mut buffer = client.buffer.lock().await;
        
        // Build the row
        buffer
            .table(table_name)?
            .symbol(competition_type, record.get(4).unwrap_or(""))?
            .symbol(season, record.get(5).unwrap_or(""))?
            .symbol(division, record.get(6).unwrap_or(""))?
            .symbol(venue, record.get(7).unwrap_or(""))?
            .symbol(home_team_name, record.get(8).unwrap_or(""))?
            .symbol(home_team_club, record.get(9).unwrap_or(""))?
            .symbol(away_team_name, record.get(10).unwrap_or(""))?
            .symbol(away_team_club, record.get(11).unwrap_or(""))?
            .symbol(home_player1, record.get(12).unwrap_or(""))?
            .symbol(home_player2, record.get(13).unwrap_or(""))?
            .symbol(away_player1, record.get(14).unwrap_or(""))?
            .symbol(away_player2, record.get(15).unwrap_or(""))?
            .symbol(match_id, record.get(1).unwrap_or(""))?
            .column_i64(
                set_number,
                record.get(2).and_then(|v| v.parse().ok()).unwrap_or(0),
            )?
            .column_i64(
                leg_number,
                record.get(3).and_then(|v| v.parse().ok()).unwrap_or(0),
            )?
            .column_i64(
                home_score,
                record.get(16).and_then(|v| v.parse().ok()).unwrap_or(0),
            )?
            .column_i64(
                away_score,
                record.get(17).and_then(|v| v.parse().ok()).unwrap_or(0),
            )?
            .column_i64(
                handicap_home,
                record.get(18).and_then(|v| v.parse().ok()).unwrap_or(0),
            )?
            .column_i64(
                handicap_away,
                record.get(19).and_then(|v| v.parse().ok()).unwrap_or(0),
            )?
            .column_str(report_html, record.get(20).unwrap_or(""))?
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

async fn run() -> Result<(), ImportError> {
    let args = Args::parse();

    // Initialize QuestDB connection
    let connection_string = format!("http::addr={};", args.host);
    let sender = Sender::from_conf(&connection_string)?;
    let buffer = Buffer::new();
    let client = Arc::new(QuestDBClient::new(sender, buffer));

    let input_dir = PathBuf::from(&args.input_dir);
    
    if args.test_run {
        // Process just one file for testing
        let test_files: Vec<_> = fs::read_dir(&input_dir)?
            .filter_map(Result::ok)
            .filter(|entry| {
                entry.path().extension().map_or(false, |ext| ext == "csv")
            })
            .collect();

        if let Some(test_file) = test_files.first() {
            println!("Processing test file: {:?}", test_file.path());
            let progress = ProgressBar::new_spinner();
            match process_csv_file(test_file.path(), Arc::clone(&client), progress).await {
                Ok(rows) => println!("Successfully processed {} rows from test file", rows),
                Err(e) => eprintln!("Error processing test file: {}", e),
            }
        } else {
            eprintln!("No CSV files found in {}", input_dir.display());
        }
    } else if args.run_all {
        // Process all CSV files in parallel
        let csv_files: Vec<_> = fs::read_dir(&input_dir)?
            .filter_map(Result::ok)
            .filter(|entry| {
                entry.path().extension().map_or(false, |ext| ext == "csv")
            })
            .map(|entry| entry.path())
            .collect();

        let progress_bar = ProgressBar::new(csv_files.len() as u64);
        progress_bar.set_style(
            ProgressStyle::default_bar()
                .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} files processed ({eta})")
                .unwrap()
        );

        // Create a vector to collect errors
        let errors = Arc::new(Mutex::new(Vec::new()));

        let mut handles = Vec::new();
        for chunk in csv_files.chunks(csv_files.len() / args.workers + 1) {
            let client = Arc::clone(&client);
            let chunk = chunk.to_vec();
            let progress_bar = progress_bar.clone();
            let errors = Arc::clone(&errors);

            let handle = task::spawn(async move {
                let mut chunk_rows = 0;
                for file in chunk {
                    let file_path = file.clone();
                    // Create a hidden progress bar for the file
                    let file_progress = ProgressBar::hidden();
                    match process_csv_file(file, Arc::clone(&client), file_progress).await {
                        Ok(rows) => {
                            chunk_rows += rows;
                            if rows == 0 {
                                errors.lock().await.push(format!("Warning: No rows processed in {:?}", file_path));
                            }
                        }
                        Err(e) => {
                            errors.lock().await.push(format!("Error processing {:?}: {}", file_path, e));
                        }
                    }
                    progress_bar.inc(1);
                }
                chunk_rows
            });
            handles.push(handle);
        }

        let mut total_rows = 0;
        for handle in handles {
            total_rows += handle.await?;
        }

        progress_bar.finish_with_message(format!("Processed {} total rows", total_rows));

        // Display any errors that occurred
        let errors = errors.lock().await;
        if !errors.is_empty() {
            println!("\nErrors and Warnings:");
            for error in errors.iter() {
                eprintln!("{}", error);
            }
        }
    } else {
        eprintln!("Please specify either --test-run or --run-all");
    }

    Ok(())
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
    let mut csv_files = Vec::new();
    let entries = fs::read_dir(csv_dir)?;
    
    // Get the latest successful import timestamp
    let latest_import = get_latest_successful_import().await?;
    
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        if path.extension().map_or(false, |ext| ext == "csv") {
            // Check if the file was modified after the latest successful import
            let should_process = if let Some(latest_import) = latest_import {
                let metadata = fs::metadata(&path)?;
                let modified = metadata.modified()?;
                let modified_datetime: DateTime<chrono::Utc> = modified.into();
                modified_datetime > latest_import
            } else {
                // If no previous successful import, process all files
                true
            };
            
            if should_process {
                csv_files.push(path);
            }
        }
    }

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
        // Using 50 as a reasonable default, adjust if needed
        let semaphore = Arc::new(Semaphore::new(50));

        let mut handles = Vec::new();
        for file in csv_files {
            let client = Arc::clone(&client);
            let errors = Arc::clone(&errors);
            let pb = pb.clone();
            let semaphore = Arc::clone(&semaphore);

            let handle = tokio::spawn(async move {
                // Acquire a permit from the semaphore before processing the file
                let _permit = semaphore.acquire().await.unwrap();
                
                let file_path = file.clone();
                // Create a hidden progress bar for the file
                let file_progress = ProgressBar::hidden();
                let result = process_csv_file(file, Arc::clone(&client), file_progress).await;
                match result {
                    Ok(rows) => {
                        if rows == 0 {
                            errors.lock().await.push(format!("Warning: No rows processed in {:?}", file_path));
                        }
                        rows
                    }
                    Err(e) => {
                        errors.lock().await.push(format!("Error processing {:?}: {}", file_path, e));
                        0
                    }
                }
                // The permit is automatically dropped here, releasing the semaphore slot
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

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}


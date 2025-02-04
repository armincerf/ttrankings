use chrono::DateTime;
use clap::Parser;
use csv::Reader;
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use questdb::{
    Result as QuestResult,
    ingress::{Buffer, Sender, TimestampNanos, TableName, ColumnName},
};
use std::fs::{self, File};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::error::Error;
use tokio::sync::Mutex;
use tokio::task;

type BoxError = Box<dyn Error + Send + Sync>;

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

        let timestamp = match DateTime::parse_from_rfc3339(timestamp) {
            Ok(t) => t,
            Err(e) => {
                eprintln!("Invalid timestamp in {:?}: {}", file_path, e);
                continue;
            }
        };
        
        let ts_nanos = timestamp.timestamp_nanos_opt().unwrap_or(0);

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

async fn run() -> Result<(), BoxError> {
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

pub async fn import_csv_files(csv_dir: &str) -> Result<(), Box<dyn std::error::Error>> {
    let mut csv_files = Vec::new();
    let entries = fs::read_dir(csv_dir)?;
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        if path.extension().map_or(false, |ext| ext == "csv") {
            // Only process files that contain "2025" in their name
            if path.to_string_lossy().contains("2025") {
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

        let mut handles = Vec::new();
        for file in csv_files {
            let client = Arc::clone(&client);
            let errors = Arc::clone(&errors);
            let pb = pb.clone();

            let handle = tokio::spawn(async move {
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
            });
            handles.push(handle);
        }

        let mut total_rows = 0;
        for handle in handles {
            total_rows += handle.await?;
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


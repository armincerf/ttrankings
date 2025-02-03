use chrono::DateTime;
use clap::Parser;
use csv::Reader;
use indicatif::{ProgressBar, ProgressStyle};
use questdb::{
    Result as QuestResult,
    ingress::{Buffer, Sender, TimestampNanos, TableName, ColumnName},
};
use std::fs::{self, File};
use std::path::PathBuf;
use std::time::{Duration, Instant};
use std::error::Error;

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
}

const MAX_BUFFER_SIZE: usize = 100_000; // Adjust based on your needs
const FLUSH_INTERVAL: Duration = Duration::from_secs(5);

fn process_csv_file(file_path: PathBuf, sender: &mut Sender, buffer: &mut Buffer) -> QuestResult<u64> {
    let mut rows_processed = 0;
    let mut last_flush = Instant::now();

    // Create table and column names first
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

    let file = match File::open(&file_path) {
        Ok(f) => f,
        Err(e) => {
            eprintln!("Failed to open file {:?}: {}", file_path, e);
            return Ok(0);
        }
    };
    
    let mut rdr = Reader::from_reader(file);

    for result in rdr.records() {
        let record = match result {
            Ok(r) => r,
            Err(e) => {
                eprintln!("Error reading record in {:?}: {}", file_path, e);
                continue;
            }
        };

        // Parse timestamp
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

        // Check if we should flush based on buffer size or time
        if buffer.len() >= MAX_BUFFER_SIZE || last_flush.elapsed() >= FLUSH_INTERVAL {
            sender.flush(buffer)?;
            last_flush = Instant::now();
        }
    }

    // Final flush for any remaining data
    if buffer.len() > 0 {
        sender.flush(buffer)?;
    }

    Ok(rows_processed)
}

fn run() -> Result<(), BoxError> {
    let args = Args::parse();

    // Initialize QuestDB connection
    let connection_string = format!("http::addr={};", args.host);
    let mut sender = Sender::from_conf(&connection_string)?;
    let mut buffer = Buffer::new();

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
            match process_csv_file(test_file.path(), &mut sender, &mut buffer) {
                Ok(rows) => println!("Successfully processed {} rows from test file", rows),
                Err(e) => eprintln!("Error processing test file: {}", e),
            }
        } else {
            eprintln!("No CSV files found in {}", input_dir.display());
        }
    } else if args.run_all {
        // Process all CSV files
        let csv_files: Vec<_> = fs::read_dir(&input_dir)?
            .filter_map(Result::ok)
            .filter(|entry| {
                entry.path().extension().map_or(false, |ext| ext == "csv")
            })
            .collect();

        let progress_bar = ProgressBar::new(csv_files.len() as u64);
        progress_bar.set_style(
            ProgressStyle::default_bar()
                .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} {msg}")
                .unwrap()
        );

        let mut total_rows = 0;
        for file in csv_files {
            progress_bar.set_message(format!("Processing {:?}", file.path().file_name().unwrap()));
            match process_csv_file(file.path(), &mut sender, &mut buffer) {
                Ok(rows) => total_rows += rows,
                Err(e) => eprintln!("Error processing {:?}: {}", file.path(), e),
            }
            progress_bar.inc(1);
        }

        progress_bar.finish_with_message(format!("Processed {} total rows", total_rows));
    } else {
        eprintln!("Please specify either --test-run or --run-all");
    }

    Ok(())
}

#[tokio::main]
async fn main() {
    if let Err(e) = run() {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}

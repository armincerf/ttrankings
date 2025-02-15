use chrono::{DateTime, ParseError};
use csv::Reader;
use std::fs::File;
use std::path::PathBuf;
use std::error::Error;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::io::Write;

#[derive(Debug, Deserialize, Serialize)]
pub struct TableTennisGame {
    #[serde(rename = "event_start_time")]
    pub timestamp: DateTime<chrono::Utc>,
    pub match_id: String,
    pub set_number: i64,
    pub game_number: i64,
    pub competition_type: String,
    pub season: String,
    pub division: String,
    pub venue: String,
    pub home_team_name: String,
    pub home_team_club: String,
    pub away_team_name: String,
    pub away_team_club: String,
    pub home_player1: String,
    pub home_player2: String,
    pub away_player1: String,
    pub away_player2: String,
    pub home_score: i64,
    pub away_score: i64,
    pub handicap_home: i64,
    pub handicap_away: i64,
    pub report_html: String,
    pub tx_time: DateTime<chrono::Utc>,
}

#[derive(Debug)]
pub enum CsvParseError {
    Io(std::io::Error),
    Csv(csv::Error),
    ChronoParse(ParseError),
    Validation(String),
    Other(Box<dyn Error + Send + Sync>),
}

impl std::fmt::Display for CsvParseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CsvParseError::Io(e) => write!(f, "IO error: {}", e),
            CsvParseError::Csv(e) => write!(f, "CSV error: {}", e),
            CsvParseError::ChronoParse(e) => write!(f, "DateTime parse error: {}", e),
            CsvParseError::Validation(e) => write!(f, "Validation error: {}", e),
            CsvParseError::Other(e) => write!(f, "Other error: {}", e),
        }
    }
}

impl Error for CsvParseError {}

impl From<std::io::Error> for CsvParseError {
    fn from(err: std::io::Error) -> CsvParseError {
        CsvParseError::Io(err)
    }
}

impl From<csv::Error> for CsvParseError {
    fn from(err: csv::Error) -> CsvParseError {
        CsvParseError::Csv(err)
    }
}

impl From<ParseError> for CsvParseError {
    fn from(err: ParseError) -> CsvParseError {
        CsvParseError::ChronoParse(err)
    }
}

impl From<Box<dyn Error + Send + Sync>> for CsvParseError {
    fn from(err: Box<dyn Error + Send + Sync>) -> CsvParseError {
        CsvParseError::Other(err)
    }
}

pub async fn parse_csv_file(file_path: PathBuf) -> Result<Vec<TableTennisGame>, CsvParseError> {
    let mut log_file = File::create("parse_warnings.log")?;
    writeln!(log_file, "Starting new parse run at {}", chrono::Utc::now())?;
    
    let mut games = Vec::new();
    let file = File::open(&file_path)?;
    let mut rdr = Reader::from_reader(file);
    
    // First, group games by match_id and set_number
    let mut matches: HashMap<String, HashMap<i64, Vec<TableTennisGame>>> = HashMap::new();

    for result in rdr.records() {
        let record = result?;

        let timestamp = match record.get(0) {
            Some(t) => t,
            None => {
                writeln!(log_file, "Warning: Skipping row with missing timestamp")?;
                continue;
            },
        };

        let tx_timestamp = match record.get(21) {
            Some(t) => t,
            None => {
                writeln!(log_file, "Warning: Skipping row with missing tx_timestamp")?;
                continue;
            },
        };

        let timestamp = DateTime::parse_from_rfc3339(timestamp)?.into();
        let tx_timestamp = DateTime::parse_from_rfc3339(tx_timestamp)?.into();

        // Normalize competition type
        let competition_type = match record.get(4).unwrap_or("").trim() {
            "League" | "MKTTL League" => "MKTTL League".to_string(),
            "Cup" | "MKTTL Challenge Cup" => "MKTTL Challenge Cup".to_string(),
            other => {
                writeln!(log_file, "Warning: Unknown competition type '{}' in file {:?}, defaulting to 'MKTTL League'", other, file_path)?;
                "MKTTL League".to_string()
            }
        };

        let game = TableTennisGame {
            timestamp,
            match_id: record.get(1).unwrap_or("").to_string(),
            set_number: record.get(2).and_then(|v| v.parse().ok()).unwrap_or(0),
            game_number: record.get(3).and_then(|v| v.parse().ok()).unwrap_or(0),
            competition_type,
            season: record.get(5).unwrap_or("").to_string(),
            division: record.get(6).unwrap_or("").to_string(),
            venue: record.get(7).unwrap_or("").to_string(),
            home_team_name: record.get(8).unwrap_or("").to_string(),
            home_team_club: record.get(9).unwrap_or("").to_string(),
            away_team_name: record.get(10).unwrap_or("").to_string(),
            away_team_club: record.get(11).unwrap_or("").to_string(),
            home_player1: record.get(12).unwrap_or("").to_string(),
            home_player2: record.get(13).unwrap_or("").to_string(),
            away_player1: record.get(14).unwrap_or("").to_string(),
            away_player2: record.get(15).unwrap_or("").to_string(),
            home_score: record.get(16).and_then(|v| v.parse().ok()).unwrap_or(0),
            away_score: record.get(17).and_then(|v| v.parse().ok()).unwrap_or(0),
            handicap_home: record.get(18).and_then(|v| v.parse().ok()).unwrap_or(0),
            handicap_away: record.get(19).and_then(|v| v.parse().ok()).unwrap_or(0),
            report_html: record.get(20).unwrap_or("").to_string(),
            tx_time: tx_timestamp,
        };

        // Group by match_id first, then by set_number
        matches
            .entry(game.match_id.clone())
            .or_default()
            .entry(game.set_number)
            .or_default()
            .push(game);
    }

    // Validate and process each match
    for (match_id, sets) in matches {
        if sets.is_empty() {
            writeln!(log_file, "Warning: Match {} has no sets, skipping", match_id)?;
            continue;
        }

        // Check if any sets have games
        let total_games: usize = sets.values().map(|games| games.len()).sum();
        if total_games == 0 {
            writeln!(log_file, "Warning: Match {} has no games, skipping", match_id)?;
            continue;
        }

        // Process each set's games
        for (set_number, set_games) in sets {
            if set_games.is_empty() {
                writeln!(log_file, "Warning: Match {} Set {} has no games, skipping", match_id, set_number)?;
                continue;
            }

            writeln!(log_file, "Processing match {} set {} with {} games", match_id, set_number, set_games.len())?;
            games.extend(set_games);
        }
    }

    if games.is_empty() {
        writeln!(log_file, "Warning: No valid games found in file {:?}", file_path)?;
    } else {
        writeln!(log_file, "Successfully parsed {} games from {:?}", games.len(), file_path)?;
    }

    Ok(games)
}

pub async fn get_csv_files(csv_dir: &str, latest_import: Option<DateTime<chrono::Utc>>) -> Result<Vec<PathBuf>, CsvParseError> {
    let mut csv_files = Vec::new();
    let entries = std::fs::read_dir(csv_dir)?;
    
    for entry in entries {
        let entry = entry?;
        let path = entry.path();
        if path.extension().map_or(false, |ext| ext == "csv") {
            // Check if the file was modified after the latest successful import
            let should_process = if let Some(latest_import) = latest_import {
                let metadata = std::fs::metadata(&path)?;
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

    Ok(csv_files)
} 
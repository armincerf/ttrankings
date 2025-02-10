mod config;
mod game_scraper;
mod types;
mod utils;
mod league_match;
mod cup_match;
mod match_html_scraper;

use anyhow::Result;
use std::{
    path::Path,
    fs,
};
use tracing::info;
use clap::{Parser, Subcommand};
use csv;

use config::ScraperConfig;
use game_scraper::GameScraper;
use match_html_scraper::MatchHtmlScraper;

const OUTPUT_DIR: &str = "parsed_html_output";

#[derive(Debug, Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Debug, Subcommand)]
enum Commands {
    /// Process a single file
    ProcessFile {
        /// Path to the HTML file to process
        #[arg(short, long)]
        file: String,
    },
    /// Scrape match HTML files
    ScrapeHtml {
        /// Optional limit on number of matches to scrape
        #[arg(short, long)]
        limit: Option<usize>,
    },
}

struct FileProcessor;

impl FileProcessor {
    fn new() -> Result<Self> {
        fs::create_dir_all(OUTPUT_DIR)?;
        Ok(Self)
    }

    async fn process_file(&self, path: &Path, config: &ScraperConfig) -> Result<()> {
        let html = fs::read_to_string(path)?;
        info!("Processing cup_match match: {:?}", path);

        // Process the match using the appropriate scraper
        let scraper = GameScraper::new(config);
        let games = scraper.parse_html(&html, path.to_str().unwrap_or_default())?;

        // Skip if no games data (unplayed match)
        if games.is_empty() {
            return Ok(());
        }

        // Generate output path that mirrors the input structure
        let relative_path = path.strip_prefix("html_files").unwrap_or(path);
        let output_path = Path::new(OUTPUT_DIR).join(relative_path);
        if let Some(parent) = output_path.parent() {
            fs::create_dir_all(parent)?;
        }
        let csv_path = output_path.with_extension("csv");
        info!("Writing CSV to {:?}", csv_path);

        // Write CSV
        let mut wtr = csv::Writer::from_path(&csv_path)?;

        // Write header
        wtr.write_record(&[
            "event_start_time",
            "match_id",
            "set_number",
            "leg_number",
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

        // Write game data
        for game in games {
            wtr.write_record(&[
                &game.event_start_time.to_rfc3339(),
                &game.match_id,
                &game.set_number.to_string(),
                &game.leg_number.to_string(),
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
        Ok(())
    }
}

fn main() -> Result<()> {
    // Initialize logging
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();
    let config = ScraperConfig::from_env();

    match cli.command {
        Commands::ProcessFile { file } => {
            let processor = FileProcessor::new()?;
            // Create a new tokio runtime just for this async operation
            let rt = tokio::runtime::Runtime::new()?;
            rt.block_on(async {
                processor.process_file(Path::new(&file), &config).await
            })?;
        }
        Commands::ScrapeHtml { limit } => {
            let mut scraper = MatchHtmlScraper::new(config)?;
            scraper.run(limit)?;
        }
    }

    Ok(())
}

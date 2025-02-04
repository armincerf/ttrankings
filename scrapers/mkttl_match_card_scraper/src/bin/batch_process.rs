use anyhow::Result;
use std::{
    path::Path,
    sync::{Arc, Mutex},
};
use mkttl_match_card_scraper::config::ScraperConfig;
use tokio::{fs, task};
use indicatif::{ProgressBar, ProgressStyle};
use clap::Parser;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    /// Number of files to process (for testing)
    #[arg(short, long)]
    limit: Option<usize>,

    /// Number of parallel tasks
    #[arg(short, long, default_value = "24")]
    parallel: usize,
}

struct FileProcessor;

impl FileProcessor {
    fn new() -> Result<Self> {
        std::fs::create_dir_all("parsed_html_output")?;
        Ok(Self)
    }

    async fn process_file(&self, path: &Path, config: &ScraperConfig) -> Result<()> {
        let html = fs::read_to_string(path).await?;
        let scraper = mkttl_match_card_scraper::game_scraper::GameScraper::new(config);
        let games = scraper.parse_html(&html, path.to_str().unwrap_or_default())?;

        // Skip if no games were found (e.g., future matches)
        if games.is_empty() {
            return Ok(());
        }

        // Only create output directories and CSV file if we have games to write
        let relative_path = path.strip_prefix("html_files").unwrap_or(path);
        let output_path = Path::new("parsed_html_output").join(relative_path);
        if let Some(parent) = output_path.parent() {
            fs::create_dir_all(parent).await?;
        }
        let csv_path = output_path.with_extension("csv");

        // Create CSV writer and write header
        let mut wtr = csv::Writer::from_path(&csv_path)?;
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
            ])?;
        }

        wtr.flush()?;
        Ok(())
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    let config = ScraperConfig::from_env();

    // Get all HTML files
    let mut html_files = Vec::new();
    let mut entries = fs::read_dir("html_files").await?;
    while let Some(entry) = entries.next_entry().await? {
        let path = entry.path();
        if path.extension().map_or(false, |ext| ext == "html") {
            let csv_path = Path::new("parsed_html_output")
                .join(path.strip_prefix("html_files").unwrap_or(&path))
                .with_extension("csv");
            if !csv_path.exists() {
                html_files.push(path);
            }
        }
    }

    // Apply limit if specified
    if let Some(limit) = cli.limit {
        html_files.truncate(limit);
    }

    let total_files = html_files.len();
    println!("Found {} HTML files to process", total_files);

    // Create progress bar
    let pb = Arc::new(Mutex::new(ProgressBar::new(total_files as u64)));
    pb.lock().unwrap().set_style(
        ProgressStyle::default_bar()
            .template("[{elapsed_precise}] {bar:40.cyan/blue} {pos}/{len} files ({eta})")
            .unwrap()
    );

    let processor = Arc::new(FileProcessor::new()?);

    // Process files in parallel
    let mut tasks = Vec::new();
    for path in html_files {
        let pb = Arc::clone(&pb);
        let config = config.clone();
        let processor = Arc::clone(&processor);
        let task = task::spawn(async move {
            let result = processor.process_file(&path, &config).await;
            pb.lock().unwrap().inc(1);
            if let Err(e) = result {
                eprintln!("Error processing {:?}: {}", path, e);
            }
        });
        tasks.push(task);

        // Limit concurrent tasks
        if tasks.len() >= cli.parallel {
            let done = tasks.remove(0);
            done.await?;
        }
    }

    // Wait for remaining tasks
    for task in tasks {
        task.await?;
    }

    pb.lock().unwrap().finish_with_message("Done!");
    Ok(())
} 
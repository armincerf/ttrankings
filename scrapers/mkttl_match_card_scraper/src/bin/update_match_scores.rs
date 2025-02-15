use anyhow::Result;
use dotenv::dotenv;
use mkttl_match_card_scraper::match_score_updater::MatchScoreUpdater;
use std::env;
use tracing_subscriber;

const HTML_FILES_DIR: &str = "/Users/alexdavis/ghq/github.com/armincerf/ttrankings/scrapers/mkttl_match_card_scraper/html_files";

#[tokio::main]
async fn main() -> Result<()> {
    // Load .env file
    dotenv().ok();
    
    // Initialize tracing subscriber
    tracing_subscriber::fmt::init();

    let database_url = env::var("DATABASE_URL")
        .expect("DATABASE_URL must be set in environment");

    println!("Starting match score update process...");
    MatchScoreUpdater::run_standalone(&database_url, HTML_FILES_DIR).await?;
    println!("Match score update process complete");

    Ok(())
} 
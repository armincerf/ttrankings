use anyhow::Result;
use clap::Parser;
use mkttl_match_card_scraper::analysis::{Analysis, DivisionFilter};

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// QuestDB host address
    #[arg(long, default_value = "localhost:9000")]
    host: String,

    /// Player name to analyze
    #[arg(long)]
    player: Option<String>,

    /// Run a custom query
    #[arg(long)]
    query: Option<String>,

    #[arg(short, long)]
    division: String,

    #[arg(short, long)]
    season: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let analysis = Analysis::new().await?;

    let filter = DivisionFilter {
        division: args.division,
        season: args.season,
    };

    let stats = analysis.get_division_player_stats(&filter).await?;
    
    // Print the stats
    println!("Player Statistics:");
    println!("=================\n");
    
    for player in stats {
        println!("{}", player.display());
    }

    Ok(())
} 
use anyhow::{Context, Result};
use questdb::ingress::{Buffer, ColumnName, Sender, TimestampNanos};
use serde::Deserialize;
use tracing::{error, info};
use scraper::Html;

use crate::config::ScraperConfig;
use crate::league_match::LeagueMatchParser;
use crate::cup_match::CupMatchParser;
use crate::types::{GameData, MatchType};
use crate::utils::detect_match_type;

#[derive(Debug, Deserialize)]
struct MatchRecord {
    url: String,
    raw_html: String,
}

pub struct GameScraper {
    quest_sender: Option<Sender>,
    client: reqwest::Client,
    league_match_parser: LeagueMatchParser,
    cup_match_parser: CupMatchParser,
    games: Vec<GameData>,
}

impl GameScraper {
    pub async fn new(config: &ScraperConfig) -> Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(
                config.scraping.request_timeout_secs,
            ))
            .build()
            .context("Failed to create HTTP client")?;

        Ok(Self {
            quest_sender: None,
            client,
            league_match_parser: LeagueMatchParser::new(),
            cup_match_parser: CupMatchParser::new(),
            games: Vec::new(),
        })
    }

    pub async fn run(&mut self) -> Result<()> {
        info!("Starting game data extraction");

        // Query for matches that haven't been processed yet
        let query = "SELECT url, raw_html FROM mkttl_matches WHERE url NOT IN (SELECT DISTINCT match_id FROM table_tennis_games)";
        let quest_query_url = format!(
            "http://localhost:9000/exec?query={}",
            urlencoding::encode(query)
        );

        let response = self
            .client
            .get(&quest_query_url)
            .send()
            .await
            .context("Failed to query QuestDB")?;

        if !response.status().is_success() {
            anyhow::bail!("Failed to query QuestDB: HTTP {}", response.status());
        }

        let json: serde_json::Value = response
            .json()
            .await
            .context("Failed to parse QuestDB response")?;

        let dataset = json["dataset"]
            .as_array()
            .context("Invalid response format - expected dataset array")?;

        info!("Found {} unprocessed matches", dataset.len());

        for row in dataset {
            let url = row[0].as_str().context("Invalid URL format")?;
            let html = row[1].as_str().context("Invalid HTML format")?;

            info!("Processing match {}", url);
            if let Err(e) = self.process_html(html, url) {
                error!("Failed to process match {}: {}", url, e);
                continue;
            }
        }

        info!("Completed game data extraction");
        Ok(())
    }

    pub fn parse_html(&self, html: &str, match_url: &str) -> Result<Vec<GameData>> {
        let document = Html::parse_document(html);
        let match_type = detect_match_type(&document)?;
        match match_type {
            MatchType::MkttlLeagueMatch => {
                self.league_match_parser.parse(html, match_url)
            }
            MatchType::MkttlChallengeCup => {
                self.cup_match_parser.parse(html, match_url)
            }
        }
    }

    pub fn process_html(&mut self, html: &str, match_url: &str) -> Result<()> {
        let games = self.parse_html(html, match_url)?;

        if let Some(ref mut sender) = self.quest_sender {
            let mut buffer = Buffer::new();
            for game in &games {
                GameScraper::write_game_to_buffer(&mut buffer, game)?;
            }

            // Flush the buffer to QuestDB
            info!("Flushing buffer to QuestDB");
            sender.flush(&mut buffer)?;
            info!("Buffer flushed successfully");
        }
        Ok(())
    }

    fn write_game_to_buffer(buffer: &mut Buffer, game: &GameData) -> Result<()> {
        info!(
            "Writing game to buffer: match_id={}, set={}, leg={}",
            game.match_id, game.set_number, game.leg_number
        );
        let event_time = TimestampNanos::new(game.event_start_time.timestamp_nanos_opt().unwrap());
        let original_time = game.original_start_time.map(|dt| TimestampNanos::new(dt.timestamp_nanos_opt().unwrap()));

        // Define all column names
        let match_id = ColumnName::new("match_id")?;
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
        let set_number = ColumnName::new("set_number")?;
        let leg_number = ColumnName::new("leg_number")?;
        let home_score = ColumnName::new("home_score")?;
        let away_score = ColumnName::new("away_score")?;
        let handicap_home = ColumnName::new("handicap_home")?;
        let handicap_away = ColumnName::new("handicap_away")?;
        let original_start_time = ColumnName::new("original_start_time")?;

        // Start with the table and chain all operations
        let mut table = buffer.table("table_tennis_games")?;

        // Write all required columns in sequence
        table = table
            .symbol(match_id, &game.match_id)?
            .symbol(competition_type, &game.competition_type)?
            .symbol(season, &game.season)?
            .symbol(division, &game.division)?
            .symbol(venue, &game.venue)?
            .symbol(home_team_name, &game.home_team_name)?
            .symbol(home_team_club, &game.home_team_club)?
            .symbol(away_team_name, &game.away_team_name)?
            .symbol(away_team_club, &game.away_team_club)?
            .symbol(home_player1, &game.home_player1)?
            .symbol(away_player1, &game.away_player1)?;

        // Write optional player fields
        if let Some(ref hp2) = game.home_player2 {
            table = table.symbol(home_player2, hp2)?;
        }
        if let Some(ref ap2) = game.away_player2 {
            table = table.symbol(away_player2, ap2)?;
        }

        // Write numeric columns
        table = table
            .column_i64(set_number, game.set_number as i64)?
            .column_i64(leg_number, game.leg_number as i64)?
            .column_i64(home_score, game.home_score as i64)?
            .column_i64(away_score, game.away_score as i64)?
            .column_i64(handicap_home, game.handicap_home as i64)?
            .column_i64(handicap_away, game.handicap_away as i64)?;

        // Write original start time if it exists
        if let Some(orig_time) = original_time {
            table = table.column_ts(original_start_time, orig_time)?;
        } else {
            table = table.column_ts(original_start_time, event_time)?;
        }

        // Write event start time and complete the row
        table.at(event_time)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::ScraperConfig;

    #[tokio::test]
    async fn test_game_scraper_new() {
        let config = ScraperConfig::default();
        let result = GameScraper::new(&config).await;
        assert!(result.is_ok());
    }
}

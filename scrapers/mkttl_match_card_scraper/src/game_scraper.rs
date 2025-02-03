use anyhow::Result;
use scraper::Html;

use crate::{
    config::ScraperConfig,
    league_match::LeagueMatchParser,
    cup_match::CupMatchParser,
    types::{GameData, MatchType},
    utils::detect_match_type,
};

pub struct GameScraper {
    config: ScraperConfig,
}

impl GameScraper {
    pub async fn new(config: &ScraperConfig) -> Result<Self> {
        Ok(Self {
            config: config.clone(),
        })
    }

    pub fn parse_html(&self, html: &str, match_id: &str) -> Result<Vec<GameData>> {
        let match_type = detect_match_type(&Html::parse_document(html))?;
        match match_type {
            MatchType::MkttlChallengeCup => {
                let parser = CupMatchParser::new();
                parser.parse(html, match_id)
            }
            MatchType::MkttlLeagueMatch => {
                let parser = LeagueMatchParser::new();
                parser.parse(html, match_id)
            }
        }
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

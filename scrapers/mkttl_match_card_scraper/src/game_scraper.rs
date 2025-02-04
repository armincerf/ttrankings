use anyhow::Result;
use scraper::Html;
use scraper::Selector;

use crate::{
    config::ScraperConfig,
    league_match::LeagueMatchParser,
    cup_match::CupMatchParser,
    types::{GameData, MatchType},
    utils::detect_match_type,
};

/// A scraper for parsing different types of table tennis game data from HTML.
/// 
/// This struct handles the parsing of both league matches and challenge cup matches,
/// delegating the actual parsing to specialized parsers based on the match type.
pub struct GameScraper {
    config: ScraperConfig,
}

impl GameScraper {
    /// Creates a new GameScraper instance with the provided configuration.
    ///
    /// # Arguments
    ///
    /// * `config` - The configuration for the scraper
    ///
    /// # Returns
    ///
    /// Returns a Result containing the new GameScraper instance
    pub fn new(config: &ScraperConfig) -> Self {
        Self {
            config: config.clone(),
        }
    }

    /// Returns true if the match has been played, false if it's a future match
    fn is_match_played(&self, document: &Html) -> bool {
        // First check if there's a message saying the scorecard hasn't been filled out
        let info_selector = Selector::parse(".info-message").unwrap();
        if let Some(info) = document.select(&info_selector).next() {
            let message = info.text().collect::<String>();
            if message.contains("not yet been filled out") {
                return false;
            }
        }

        // Then check for actual game data
        let games_selector = Selector::parse("#games-table tbody tr").unwrap();
        let result_selector = Selector::parse("td:nth-child(2)").unwrap();
        
        // Check each game row
        for game in document.select(&games_selector) {
            if let Some(result_cell) = game.select(&result_selector).next() {
                let result_text = result_cell.text().collect::<String>();
                // Return true only if we find a row that doesn't contain these placeholder texts
                if !result_text.contains("absent player") && 
                   !result_text.contains("not played") && 
                   !result_text.contains("void") &&
                   !result_text.is_empty() {
                    return true;
                }
            }
        }
        false
    }

    /// Parses HTML content and extracts game data.
    ///
    /// # Arguments
    ///
    /// * `html` - The HTML content to parse
    /// * `match_id` - The unique identifier for the match
    ///
    /// # Returns
    ///
    /// Returns a Result containing a vector of GameData if successful
    ///
    /// # Errors
    ///
    /// Will return an error if:
    /// - The match type cannot be detected from the HTML
    /// - The HTML parsing fails
    /// - The specific match parser encounters an error
    pub fn parse_html(&self, html: &str, match_id: &str) -> Result<Vec<GameData>> {
        let document = Html::parse_document(html);
        
        // Skip unplayed matches early
        if !self.is_match_played(&document) {
            return Ok(Vec::new());
        }
        
        let match_type = detect_match_type(&document)?;
        
        // Note: This match is exhaustive for the current MatchType enum.
        // If new variants are added to MatchType, this match will need to be updated.
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

    #[test]
    fn test_game_scraper_new() {
        let config = ScraperConfig::default();
        let scraper = GameScraper::new(&config);
        assert!(matches!(scraper, GameScraper { config: _ }));
        //check config is the same
        assert_eq!(scraper.config, config);
    }

    // TODO: Add tests for parse_html with:
    // - Valid league match HTML
    // - Valid cup match HTML
    // - Invalid HTML
    // - HTML with unrecognized match type
}

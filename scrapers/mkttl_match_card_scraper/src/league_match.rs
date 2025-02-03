use anyhow::{Context, Result};
use chrono::{DateTime, NaiveDateTime, Utc};
use scraper::{Html, Selector};
use crate::{types::{GameData, Players}, utils};

pub struct LeagueMatchParser;

impl LeagueMatchParser {
    pub fn new() -> Self {
        Self
    }

    pub fn parse(&self, html: &str, match_id: &str) -> Result<Vec<GameData>> {
        let document = Html::parse_document(html);
        let mut games = Vec::new();

        // Extract match metadata
        let competition_type = self.extract_competition_type(&document)?;
        let season = self.extract_season(&document)?;
        let division = self.extract_division(&document)?;
        let venue = self.extract_venue(&document)?;
        let event_start_time = self.extract_start_time(&document)?;
        let ((home_team_name, home_team_club), (away_team_name, away_team_club)) = self.extract_teams(&document)?;
        let report_html = self.extract_report_html(&document);

        // Extract games data
        let games_selector = Selector::parse("#games-table tbody tr").unwrap();

        for game in document.select(&games_selector) {
            let set_number = match self.extract_set_number(&game) {
                Ok(num) => num,
                Err(_) => continue, // Skip this game if we can't get the set number
            };

            // Try to extract game data, skip if not played
            match self.extract_game_data(&game) {
                Ok((players, scores)) => {
                    // For each leg in the game
                    let leg_scores = scores.split(", ").enumerate();
                    for (leg_idx, leg_score) in leg_scores {
                        match utils::parse_score(leg_score) {
                            Ok((home_score, away_score)) => {
                                let game_data = GameData {
                                    event_start_time,
                                    original_start_time: None,
                                    match_id: match_id.to_string(),
                                    set_number: set_number as i32,
                                    leg_number: (leg_idx + 1) as i32,
                                    competition_type: competition_type.clone(),
                                    season: season.clone(),
                                    division: division.clone(),
                                    venue: venue.clone(),
                                    home_team_name: home_team_name.clone(),
                                    home_team_club: home_team_club.clone(),
                                    away_team_name: away_team_name.clone(),
                                    away_team_club: away_team_club.clone(),
                                    home_player1: players.home_player1.clone(),
                                    home_player2: players.home_player2.clone(),
                                    away_player1: players.away_player1.clone(),
                                    away_player2: players.away_player2.clone(),
                                    home_score,
                                    away_score,
                                    handicap_home: 0,
                                    handicap_away: 0,
                                    report_html: report_html.clone(),
                                };

                                games.push(game_data);
                            }
                            Err(_) => continue, // Skip this leg if we can't parse the score
                        }
                    }
                }
                Err(_) => continue, // Skip this game if it wasn't played
            }
        }

        Ok(games)
    }

    fn extract_competition_type(&self, document: &Html) -> Result<String> {
        document
            .select(&Selector::parse("h2").unwrap())
            .next()
            .map(|el| el.text().collect::<String>().trim().to_string())
            .context("Could not find competition type")
    }

    fn extract_season(&self, document: &Html) -> Result<String> {
        let table_selector = Selector::parse("table").unwrap();
        let row_selector = Selector::parse("tr").unwrap();
        let cell_selector = Selector::parse("th, td").unwrap();

        for table in document.select(&table_selector) {
            for row in table.select(&row_selector) {
                let cells: Vec<_> = row.select(&cell_selector).collect();
                if cells.len() >= 2 {
                    if cells[0].text().collect::<String>().contains("Season") {
                        return Ok(cells[1].text().collect::<String>());
                    }
                }
            }
        }
        anyhow::bail!("Failed to extract season")
    }

    fn extract_division(&self, document: &Html) -> Result<String> {
        let selector = Selector::parse("h3").unwrap();
        document
            .select(&selector)
            .next()
            .map(|el| {
                let text = el.text().collect::<String>();
                if text.contains("-") {
                    text.split("-").nth(1).unwrap_or(&text).trim().to_string()
                } else {
                    text
                }
            })
            .context("Failed to extract division")
    }

    fn extract_venue(&self, document: &Html) -> Result<String> {
        let table_selector = Selector::parse("table").unwrap();
        let row_selector = Selector::parse("tr").unwrap();
        let cell_selector = Selector::parse("th, td").unwrap();

        for table in document.select(&table_selector) {
            for row in table.select(&row_selector) {
                let cells: Vec<_> = row.select(&cell_selector).collect();
                if cells.len() >= 2 {
                    if cells[0].text().collect::<String>().contains("Venue") {
                        // Extract text from all child nodes, including anchor tags
                        let venue = cells[1].text().collect::<String>();
                        return Ok(venue);
                    }
                }
            }
        }
        anyhow::bail!("Failed to extract venue")
    }

    fn extract_start_time(&self, document: &Html) -> Result<DateTime<Utc>> {
        let table_selector = Selector::parse("table").unwrap();
        let row_selector = Selector::parse("tr").unwrap();
        let cell_selector = Selector::parse("th, td").unwrap();

        let mut date_str = String::new();
        let mut time_str = String::new();

        // First try to find date and time in the table
        for table in document.select(&table_selector) {
            for row in table.select(&row_selector) {
                let cells: Vec<_> = row.select(&cell_selector).collect();
                if cells.len() >= 2 {
                    let header = cells[0].text().collect::<String>();
                    if header.contains("Date") || header.contains("New date") {
                        // Use the full date string
                        date_str = cells[1].text().collect::<String>();
                    } else if header.contains("Start time") {
                        time_str = cells[1].text().collect::<String>();
                    }
                }
            }
        }

        // If we couldn't find the date in the table, try the h4 tag
        if date_str.is_empty() {
            if let Some(h4) = document.select(&Selector::parse("h4").unwrap()).next() {
                date_str = h4.text().collect::<String>();
            }
        }

        if date_str.is_empty() || time_str.is_empty() {
            anyhow::bail!("Failed to extract date or time");
        }

        // Clean up the date string by removing the day of the week if present
        let date_str = if date_str.contains(" ") {
            let parts: Vec<&str> = date_str.split_whitespace().collect();
            if parts.len() == 4 {
                // Format: "Thursday 31 January 2025"
                format!("{} {} {}", parts[1], parts[2], parts[3])
            } else {
                date_str
            }
        } else {
            date_str
        };

        let datetime_str = format!("{} {}", date_str, time_str);
        // Try different date formats
        NaiveDateTime::parse_from_str(&datetime_str, "%d %B %Y %H:%M")
            .or_else(|_| NaiveDateTime::parse_from_str(&datetime_str, "%-d %B %Y %H:%M"))
            .or_else(|_| NaiveDateTime::parse_from_str(&datetime_str, "%d/%m/%Y %H:%M"))
            .or_else(|_| NaiveDateTime::parse_from_str(&datetime_str, "%-d/%m/%Y %H:%M"))
            .map(|naive_dt| DateTime::from_naive_utc_and_offset(naive_dt, Utc))
            .map_err(|e| anyhow::anyhow!("Failed to parse date and time: {}", e))
    }

    fn extract_teams(&self, document: &Html) -> Result<((String, String), (String, String))> {
        utils::extract_teams_from_og_url(document)
    }

    fn extract_set_number(&self, game: &scraper::ElementRef) -> Result<usize> {
        let set_selector = Selector::parse(".game-number").unwrap();
        game.select(&set_selector)
            .next()
            .map(|el| el.text().collect::<String>().parse::<usize>())
            .context("Failed to extract set number")?
            .context("Failed to parse set number")
    }

    fn extract_game_data(&self, game: &scraper::ElementRef) -> Result<(Players, String)> {
        let result_selector = Selector::parse("td:nth-child(2)").unwrap();
        let scores_selector = Selector::parse(".leg-scores").unwrap();

        let result_cell = game
            .select(&result_selector)
            .next()
            .context("Failed to find result cell")?;

        let result_text = result_cell.text().collect::<String>();
        let scores = game
            .select(&scores_selector)
            .next()
            .map(|el| el.text().collect::<String>())
            .context("Failed to extract scores")?;

        // Skip games that were not played at all
        if scores.contains("Not applicable") || 
           result_text.contains("not played") || 
           scores.trim().is_empty() {
            anyhow::bail!("Game was not played");
        }

        let players = self.extract_players(&result_cell)?;

        // For retired games or games that finished early, we don't want to include them
        // even if they have valid scores
        if result_text.contains("retired") || result_text.contains("finished early") {
            anyhow::bail!("Game was not played");
        }

        // For normal games, ensure there are valid scores
        if !scores.contains("-") || scores.split(", ").all(|s| s.trim().is_empty()) {
            anyhow::bail!("Game was not played");
        }

        Ok((players, scores))
    }

    fn extract_players(&self, cell: &scraper::ElementRef) -> Result<Players> {
        let player_selector = Selector::parse("a").unwrap();
        let players = cell.select(&player_selector);

        let text = cell.text().collect::<String>();
        let is_doubles = text.contains("&");

        if is_doubles {
            // Try both formats - individual links and combined links
            let all_players: Vec<_> = players.collect();
            
            if all_players.len() == 4 {
                // Individual links for each player
                Ok(Players {
                    home_player1: all_players[0].text().collect(),
                    home_player2: Some(all_players[1].text().collect()),
                    away_player1: all_players[2].text().collect(),
                    away_player2: Some(all_players[3].text().collect()),
                })
            } else if all_players.len() == 2 {
                // Combined links for each team
                let home_text = all_players[0].text().collect::<String>();
                let away_text = all_players[1].text().collect::<String>();

                // Split the player names on "&"
                let home_players: Vec<_> = home_text.split(" & ").collect();
                let away_players: Vec<_> = away_text.split(" & ").collect();

                if home_players.len() < 2 || away_players.len() < 2 {
                    return Err(anyhow::anyhow!("Not enough players found for doubles match"));
                }

                Ok(Players {
                    home_player1: home_players[0].trim().to_string(),
                    home_player2: Some(home_players[1].trim().to_string()),
                    away_player1: away_players[0].trim().to_string(),
                    away_player2: Some(away_players[1].trim().to_string()),
                })
            } else {
                Err(anyhow::anyhow!("Unexpected number of players for doubles match"))
            }
        } else {
            // For singles matches, just get the two players
            let all_players: Vec<_> = players.collect();
            if all_players.len() != 2 {
                return Err(anyhow::anyhow!("Expected 2 players for singles match"));
            }

            Ok(Players {
                home_player1: all_players[0].text().collect(),
                home_player2: None,
                away_player1: all_players[1].text().collect(),
                away_player2: None,
            })
        }
    }

    fn extract_report_html(&self, document: &Html) -> Option<String> {
        let report_selector = Selector::parse("#report-text").unwrap();
        document
            .select(&report_selector)
            .next()
            .map(|el| el.html())
    }
} 
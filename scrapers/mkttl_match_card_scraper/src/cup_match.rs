use anyhow::{Context, Result};
use chrono::{DateTime, NaiveDateTime, Utc};
use scraper::{Html, Selector};
use crate::{types::{GameData, Players}, utils};

pub struct CupMatchParser;

impl CupMatchParser {
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
        let (event_start_time, original_start_time) = self.extract_start_time(&document)?;
        let ((home_team_name, home_team_club, home_handicap), (away_team_name, away_team_club, away_handicap)) = self.extract_teams(&document)?;
        let report_html = self.extract_report_html(&document);

        // Extract games data
        let games_selector = Selector::parse("#games-table tbody tr").unwrap();

        for game in document.select(&games_selector) {
            let set_number = self.extract_set_number(&game)?;
            let (players, scores) = self.extract_game_data(&game)?;

            // Skip games where a player is absent or game not played
            if scores == "Away player absent" || scores == "Not applicable" {
                continue;
            }

            // For each leg in the game
            let leg_scores = scores.split(", ").enumerate();
            for (leg_idx, leg_score) in leg_scores {
                let (home_score, away_score) = utils::parse_score(leg_score)?;

                let game_data = GameData {
                    event_start_time,
                    original_start_time,
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
                    handicap_home: home_handicap,
                    handicap_away: away_handicap,
                    report_html: report_html.clone(),
                };

                games.push(game_data);
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
        let selector = Selector::parse("table.vertical tr").unwrap();
        for row in document.select(&selector) {
            let header = row.select(&Selector::parse("th").unwrap()).next();
            let value = row.select(&Selector::parse("td").unwrap()).next();
            
            if let (Some(header), Some(value)) = (header, value) {
                if header.text().collect::<String>().contains("Season") {
                    return Ok(value.text().collect::<String>());
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
        let selector = Selector::parse("table.vertical tr").unwrap();
        for row in document.select(&selector) {
            let header = row.select(&Selector::parse("th").unwrap()).next();
            let value = row.select(&Selector::parse("td").unwrap()).next();
            
            if let (Some(header), Some(value)) = (header, value) {
                if header.text().collect::<String>().contains("Venue") {
                    return Ok(value.text().collect::<String>());
                }
            }
        }
        anyhow::bail!("Failed to extract venue")
    }

    fn extract_start_time(&self, document: &Html) -> Result<(DateTime<Utc>, Option<DateTime<Utc>>)> {
        let mut new_date_str = String::new();
        let mut original_date_str = String::new();
        let mut time_str = String::new();

        // Try both table selectors
        let selectors = [
            Selector::parse("table.vertical tr").unwrap(),
            Selector::parse("table tr").unwrap(),
        ];

        // First try to find new date and original date
        for selector in &selectors {
            for row in document.select(selector) {
                let header = row.select(&Selector::parse("th").unwrap()).next();
                let value = row.select(&Selector::parse("td").unwrap()).next();
                
                if let (Some(header), Some(value)) = (header, value) {
                    let header_text = header.text().collect::<String>();
                    if header_text.contains("New date") {
                        new_date_str = value.text().collect::<String>();
                    } else if header_text.contains("Original date") {
                        original_date_str = value.text().collect::<String>();
                    } else if header_text.contains("Date") && new_date_str.is_empty() {
                        new_date_str = value.text().collect::<String>();
                    } else if header_text.contains("Start time") {
                        time_str = value.text().collect::<String>();
                    }
                }
            }
        }

        if time_str.is_empty() {
            anyhow::bail!("Failed to extract time");
        }

        // If we have a new date, use that for event_start_time, otherwise use original date
        let event_date_str = if !new_date_str.is_empty() {
            new_date_str.clone()
        } else if !original_date_str.is_empty() {
            original_date_str.clone()
        } else {
            anyhow::bail!("Failed to extract any date");
        };

        // Parse event start time
        let event_datetime_str = format!("{} {}", event_date_str, time_str);
        let event_start_time = DateTime::from_naive_utc_and_offset(
            NaiveDateTime::parse_from_str(&event_datetime_str, "%A %d %B %Y %H:%M")
                .or_else(|_| NaiveDateTime::parse_from_str(&event_datetime_str, "%A %-d %B %Y %H:%M"))?,
            Utc,
        );

        // Parse original start time if it exists and is different from event start time
        let original_start_time = if !original_date_str.is_empty() && new_date_str != original_date_str {
            let original_datetime_str = format!("{} {}", original_date_str, time_str);
            Some(DateTime::from_naive_utc_and_offset(
                NaiveDateTime::parse_from_str(&original_datetime_str, "%A %d %B %Y %H:%M")
                    .or_else(|_| NaiveDateTime::parse_from_str(&original_datetime_str, "%A %-d %B %Y %H:%M"))?,
                Utc,
            ))
        } else {
            None
        };

        Ok((event_start_time, original_start_time))
    }

    fn extract_teams(&self, document: &Html) -> Result<((String, String, i32), (String, String, i32))> {
        let ((home_team, home_club), (away_team, away_club)) = utils::extract_teams_from_og_url(document)?;
        
        // Extract handicaps from the teams table
        let teams_selector = Selector::parse("#teams-table tbody tr").unwrap();
        let mut home_handicap = 0;
        let mut away_handicap = 0;

        for team_row in document.select(&teams_selector) {
            let cells: Vec<_> = team_row.select(&Selector::parse("td").unwrap()).collect();
            if cells.len() >= 3 {
                let team_name = cells[0].text().collect::<String>();
                let handicap = cells[2].text().collect::<String>().parse::<i32>().unwrap_or(0);
                
                if team_name.contains(&home_team) {
                    home_handicap = handicap;
                } else if team_name.contains(&away_team) {
                    away_handicap = handicap;
                }
            }
        }

        Ok(((home_team, home_club, home_handicap), (away_team, away_club, away_handicap)))
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

        // Check if this is a game with an absent player or not played
        let result_text = result_cell.text().collect::<String>();
        if result_text.contains("absent player") {
            let players = self.extract_players(&result_cell)?;
            return Ok((players, "Away player absent".to_string()));
        } else if result_text.contains("not played") || result_text.contains("void") {
            let players = self.extract_players(&result_cell)?;
            return Ok((players, "Not applicable".to_string()));
        }

        let players = self.extract_players(&result_cell)?;

        let scores = game
            .select(&scores_selector)
            .next()
            .map(|el| {
                let mut score_text = String::new();
                for node in el.select(&Selector::parse(".leg-score").unwrap()) {
                    if !score_text.is_empty() {
                        score_text.push_str(", ");
                    }
                    score_text.push_str(&node.text().collect::<String>());
                }
                score_text
            })
            .context("Failed to extract scores")?;

        Ok((players, scores))
    }

    fn extract_players(&self, cell: &scraper::ElementRef) -> Result<Players> {
        let player_selector = Selector::parse("a").unwrap();
        let all_players: Vec<_> = cell.select(&player_selector).collect();
        let text = cell.text().collect::<String>();
        let is_doubles = text.contains("&");

        // Handle case where there's an absent player or game not played
        if text.contains("absent player") {
            let present_player = all_players.first().context("Failed to find present player")?;
            return Ok(Players {
                home_player1: present_player.text().collect(),
                home_player2: None,
                away_player1: "absent player".to_string(),
                away_player2: None,
            });
        } else if text.contains("not played") || text.contains("void") {
            if all_players.len() == 2 {
                return Ok(Players {
                    home_player1: all_players[0].text().collect(),
                    home_player2: None,
                    away_player1: all_players[1].text().collect(),
                    away_player2: None,
                });
            }
        }

        if is_doubles {
            // Try both formats - individual links and combined links            
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

                let home_players: Vec<&str> = home_text.split(" & ").collect();
                let away_players: Vec<&str> = away_text.split(" & ").collect();

                if home_players.len() == 2 && away_players.len() == 2 {
                    Ok(Players {
                        home_player1: home_players[0].to_string(),
                        home_player2: Some(home_players[1].to_string()),
                        away_player1: away_players[0].to_string(),
                        away_player2: Some(away_players[1].to_string()),
                    })
                } else {
                    anyhow::bail!("Failed to parse doubles players")
                }
            } else {
                anyhow::bail!("Unexpected number of player links in doubles game")
            }
        } else {
            if all_players.len() == 2 {
                Ok(Players {
                    home_player1: all_players[0].text().collect(),
                    home_player2: None,
                    away_player1: all_players[1].text().collect(),
                    away_player2: None,
                })
            } else {
                println!("All players: {:?}", all_players);
                anyhow::bail!("Unexpected number of player links in singles game")
            }
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
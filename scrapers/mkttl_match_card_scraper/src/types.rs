use chrono::{DateTime, Utc};
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GameData {
    pub event_start_time: DateTime<Utc>,
    pub original_start_time: Option<DateTime<Utc>>,
    pub match_id: String,
    pub set_number: i32,
    pub leg_number: i32,
    pub competition_type: String,
    pub season: String,
    pub division: String,
    pub venue: String,
    pub home_team_name: String,
    pub home_team_club: String,
    pub away_team_name: String,
    pub away_team_club: String,
    pub home_player1: String,
    pub home_player2: Option<String>,
    pub away_player1: String,
    pub away_player2: Option<String>,
    pub home_score: i32,
    pub away_score: i32,
    pub handicap_home: i32,
    pub handicap_away: i32,
    pub report_html: Option<String>,
}

#[derive(Debug)]
pub struct Players {
    pub home_player1: String,
    pub home_player2: Option<String>,
    pub away_player1: String,
    pub away_player2: Option<String>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum MatchType {
    MkttlLeagueMatch,
    MkttlChallengeCup,
}
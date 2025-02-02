use anyhow::Result;
use chrono::{DateTime, NaiveDateTime, Utc};
use questdb::ingress::{Buffer, Sender};
use serde::Deserialize;
use std::env;
use test_log::test;
use tracing::info;

// Import the crate using the package name from Cargo.toml
extern crate mkttl_match_card_scraper;
use mkttl_match_card_scraper::{config::ScraperConfig, game_scraper::GameScraper};

const TEST_LEAGUE_MATCH_URL: &str = "match-20250131-001";
const TEST_CUP_MATCH_URL: &str = "match-20250130-001";

#[derive(Debug, Deserialize)]
struct GameRecord {
    event_start_time: DateTime<Utc>,
    match_id: String,
    set_number: i32,
    leg_number: i32,
    competition_type: String,
    season: String,
    division: String,
    venue: String,
    home_team_name: String,
    home_team_club: String,
    away_team_name: String,
    away_team_club: String,
    home_player1: String,
    home_player2: Option<String>,
    away_player1: String,
    away_player2: Option<String>,
    home_score: i32,
    away_score: i32,
    handicap_home: i32,
    handicap_away: i32,
}

async fn process_league_match(game_scraper: &mut GameScraper) -> Result<()> {
    let html = include_str!("fixtures/league_match/match_104_68_2025_1_31.html");
    let expected_csv = include_str!("fixtures/league_match/match_104_68_2025_1_31.csv");
    process_match_data(game_scraper, TEST_LEAGUE_MATCH_URL, html, expected_csv).await
}

async fn process_cup_match(game_scraper: &mut GameScraper) -> Result<()> {
    let html = include_str!("fixtures/cup_match/match-20250130-001.html");
    let expected_csv = include_str!("fixtures/cup_match/match-20250130-001.csv");
    process_match_data(game_scraper, TEST_CUP_MATCH_URL, html, expected_csv).await
}

async fn process_match_data(game_scraper: &mut GameScraper, match_url: &str, html: &str, expected_csv: &str) -> Result<()> {
    // Clean up any existing test data by dropping and recreating the table
    let client = reqwest::Client::new();
    let cleanup_queries = [
        "DROP TABLE IF EXISTS table_tennis_games;",
        "CREATE TABLE IF NOT EXISTS table_tennis_games (
            -- When the match/leg started; used as the designated timestamp for time-series queries
            event_start_time TIMESTAMP,
            -- Only used if the event was rescheduled
            original_start_time TIMESTAMP,

            -- Identifiers
            match_id SYMBOL capacity 256 cache index,       -- identifier to group legs/sets into the same match
            set_number INT,        -- number of the set within a match
            leg_number INT,        -- leg (game) index within the set

            -- Competition metadata
            competition_type SYMBOL capacity 256 cache,   -- e.g. 'league', 'tournament', 'singles'
            season SYMBOL capacity 256 cache,             -- season identifier, may be empty if unknown
            division SYMBOL capacity 256 cache,           -- league division, may be empty for singles league

            -- Venue information
            venue SYMBOL capacity 256 cache,

            -- Home team data: both the specific team name and the broader club it belongs to
            home_team_name SYMBOL capacity 256 cache,
            home_team_club SYMBOL capacity 256 cache,

            -- Away team data: similarly split into team name and club
            away_team_name SYMBOL capacity 256 cache,
            away_team_club SYMBOL capacity 256 cache,

            -- Player names (up to two per side; for a singles match the secondary can be empty)
            home_player1 SYMBOL capacity 256 cache,
            home_player2 SYMBOL capacity 256 cache,     
            away_player1 SYMBOL capacity 256 cache,
            away_player2 SYMBOL capacity 256 cache,

            -- Scores for this leg; overall match totals can be computed via aggregation later
            home_score INT,
            away_score INT,

            -- Optional handicap bonus points if applicable
            handicap_home INT,
            handicap_away INT
        ) TIMESTAMP(event_start_time)
        PARTITION BY DAY WAL DEDUP UPSERT KEYS(match_id, set_number, leg_number, home_player1, home_player2, away_player1, away_player2, home_score, away_score);"
    ];

    for query in cleanup_queries {
        let cleanup_url = "http://localhost:9000/exec";
        client.post(cleanup_url)
            .header("Content-Type", "application/json")
            .body(format!("{{\"query\":\"{}\"}}", query))
            .send()
            .await?;
    }

    // Process the HTML
    game_scraper.process_html(html, match_url)?;

    // Query the results from QuestDB
    let query = format!(
        "SELECT event_start_time, match_id, set_number, leg_number, competition_type, season, division, venue, home_team_name, home_team_club, away_team_name, away_team_club, home_player1, home_player2, away_player1, away_player2, home_score, away_score, handicap_home, handicap_away, original_start_time FROM table_tennis_games WHERE match_id = '{}' ORDER BY set_number, leg_number;",
        match_url
    );
    let quest_query_url = format!(
        "http://localhost:9000/exec?query={}",
        urlencoding::encode(&query)
    );

    let response = client.get(&quest_query_url).send().await?;
    let json: serde_json::Value = response.json().await?;
    info!("QuestDB response: {}", serde_json::to_string_pretty(&json)?);
    let results = json["dataset"]
        .as_array()
        .expect("Expected array of results");

    // Parse expected CSV
    let mut expected_records = Vec::new();
    let mut rdr = csv::Reader::from_reader(expected_csv.as_bytes());
    
    // Get the header record to determine column indexes
    let headers = rdr.headers()?.clone();
    let get_column_index = |name: &str| -> Result<usize> {
        headers.iter().position(|h| h.trim() == name)
            .ok_or_else(|| anyhow::anyhow!("Column {} not found in CSV", name))
    };

    // Get indexes for each column
    let event_start_time_idx = get_column_index("event_start_time")?;
    let match_id_idx = get_column_index("match_id")?;
    let set_number_idx = get_column_index("set_number")?;
    let leg_number_idx = get_column_index("leg_number")?;
    let competition_type_idx = get_column_index("competition_type")?;
    let season_idx = get_column_index("season")?;
    let division_idx = get_column_index("division")?;
    let venue_idx = get_column_index("venue")?;
    let home_team_name_idx = get_column_index("home_team_name")?;
    let home_team_club_idx = get_column_index("home_team_club")?;
    let away_team_name_idx = get_column_index("away_team_name")?;
    let away_team_club_idx = get_column_index("away_team_club")?;
    let home_player1_idx = get_column_index("home_player1")?;
    let home_player2_idx = get_column_index("home_player2")?;
    let away_player1_idx = get_column_index("away_player1")?;
    let away_player2_idx = get_column_index("away_player2")?;
    let home_score_idx = get_column_index("home_score")?;
    let away_score_idx = get_column_index("away_score")?;
    let handicap_home_idx = get_column_index("handicap_home")?;
    let handicap_away_idx = get_column_index("handicap_away")?;

    for result in rdr.records() {
        let record = result?;
        let event_start_time = DateTime::from_naive_utc_and_offset(
            NaiveDateTime::parse_from_str(&record[event_start_time_idx], "%Y-%m-%dT%H:%M:%SZ")?,
            Utc,
        );
        let home_player2 = if record[home_player2_idx].is_empty() { None } else { Some(record[home_player2_idx].to_string()) };
        let away_player2 = if record[away_player2_idx].is_empty() { None } else { Some(record[away_player2_idx].to_string()) };

        expected_records.push(GameRecord {
            event_start_time,
            match_id: record[match_id_idx].to_string(),
            set_number: record[set_number_idx].parse()?,
            leg_number: record[leg_number_idx].parse()?,
            competition_type: record[competition_type_idx].to_string(),
            season: record[season_idx].to_string(),
            division: record[division_idx].to_string(),
            venue: record[venue_idx].to_string(),
            home_team_name: record[home_team_name_idx].to_string(),
            home_team_club: record[home_team_club_idx].to_string(),
            away_team_name: record[away_team_name_idx].to_string(),
            away_team_club: record[away_team_club_idx].to_string(),
            home_player1: record[home_player1_idx].to_string(),
            home_player2,
            away_player1: record[away_player1_idx].to_string(),
            away_player2,
            home_score: record[home_score_idx].parse()?,
            away_score: record[away_score_idx].parse()?,
            handicap_home: record[handicap_home_idx].parse()?,
            handicap_away: record[handicap_away_idx].parse()?,
        });
    }

    // Compare results
    assert_eq!(results.len(), expected_records.len(), "Number of results doesn't match expected");

    for (i, expected) in expected_records.iter().enumerate() {
        let actual = &results[i];
        
        assert_eq!(
            actual[0].as_str().unwrap(),
            format!("{}.000000Z", expected.event_start_time.format("%Y-%m-%dT%H:%M:%S")),
            "Mismatch in event_start_time at index {}", i
        );
        assert_eq!(
            actual[1].as_str().unwrap(),
            expected.match_id,
            "Mismatch in match_id at index {}", i
        );
        assert_eq!(
            actual[2].as_i64().unwrap() as i32,
            expected.set_number,
            "Mismatch in set_number at index {}", i
        );
        assert_eq!(
            actual[3].as_i64().unwrap() as i32,
            expected.leg_number,
            "Mismatch in leg_number at index {}", i
        );
        assert_eq!(
            actual[4].as_str().unwrap(),
            expected.competition_type,
            "Mismatch in competition_type at index {}", i
        );
        assert_eq!(
            actual[5].as_str().unwrap(),
            expected.season,
            "Mismatch in season at index {}", i
        );
        assert_eq!(
            actual[6].as_str().unwrap(),
            expected.division,
            "Mismatch in division at index {}", i
        );
        assert_eq!(
            actual[7].as_str().unwrap(),
            expected.venue,
            "Mismatch in venue at index {}", i
        );
        assert_eq!(
            actual[8].as_str().unwrap(),
            expected.home_team_name,
            "Mismatch in home_team_name at index {}", i
        );
        assert_eq!(
            actual[9].as_str().unwrap(),
            expected.home_team_club,
            "Mismatch in home_team_club at index {}", i
        );
        assert_eq!(
            actual[10].as_str().unwrap(),
            expected.away_team_name,
            "Mismatch in away_team_name at index {}", i
        );
        assert_eq!(
            actual[11].as_str().unwrap(),
            expected.away_team_club,
            "Mismatch in away_team_club at index {}", i
        );
        assert_eq!(
            actual[12].as_str().unwrap(),
            expected.home_player1,
            "Mismatch in home_player1 at index {}", i
        );
        
        match &expected.home_player2 {
            Some(p) => assert_eq!(
                actual[13].as_str().unwrap(),
                p,
                "Mismatch in home_player2 at index {}", i
            ),
            None => assert!(actual[13].is_null(), "Expected null home_player2 at index {}", i),
        }

        assert_eq!(
            actual[14].as_str().unwrap(),
            expected.away_player1,
            "Mismatch in away_player1 at index {}", i
        );

        match &expected.away_player2 {
            Some(p) => assert_eq!(
                actual[15].as_str().unwrap(),
                p,
                "Mismatch in away_player2 at index {}", i
            ),
            None => assert!(actual[15].is_null(), "Expected null away_player2 at index {}", i),
        }

        assert_eq!(
            actual[16].as_i64().unwrap() as i32,
            expected.home_score,
            "Mismatch in home_score at index {}", i
        );
        assert_eq!(
            actual[17].as_i64().unwrap() as i32,
            expected.away_score,
            "Mismatch in away_score at index {}", i
        );
        assert_eq!(
            actual[18].as_i64().unwrap() as i32,
            expected.handicap_home,
            "Mismatch in handicap_home at index {}", i
        );
        assert_eq!(
            actual[19].as_i64().unwrap() as i32,
            expected.handicap_away,
            "Mismatch in handicap_away at index {}", i
        );
    }

    Ok(())
}

#[test(tokio::test)]
async fn test_league_match() -> Result<()> {
    let config = ScraperConfig::default();
    let mut game_scraper = GameScraper::new(&config)?;
    process_league_match(&mut game_scraper).await
}

#[test(tokio::test)]
async fn test_cup_match() -> Result<()> {
    let config = ScraperConfig::default();
    let mut game_scraper = GameScraper::new(&config)?;
    process_cup_match(&mut game_scraper).await
} 
use anyhow::Result;
use chrono::{DateTime, NaiveDateTime, Utc};

// Import the crate using the package name from Cargo.toml
extern crate mkttl_match_card_scraper;
use mkttl_match_card_scraper::{config::ScraperConfig, game_scraper::GameScraper, types::GameData};

const TEST_LEAGUE_MATCH_URL: &str = "match-20250131-001";
const TEST_CUP_MATCH_URL: &str = "match-20250130-001";
const TEST_PARTIAL_MATCH_URL: &str = "match-20250129-001";
const TEST_NEW_CUP_MATCH_URL: &str = "match-20250204-001";

async fn process_league_match() -> Result<Vec<GameData>> {
    let config = ScraperConfig::default();
    let scraper = GameScraper::new(&config);
    scraper.parse_html(include_str!("fixtures/league_match/match_104_68_2025_1_31.html"), TEST_LEAGUE_MATCH_URL)
}

async fn process_cup_match() -> Result<Vec<GameData>> {
    let config = ScraperConfig::default();
    let scraper = GameScraper::new(&config);
    scraper.parse_html(include_str!("fixtures/cup_match/match-20250130-001.html"), TEST_CUP_MATCH_URL)
}

async fn process_new_cup_match() -> Result<Vec<GameData>> {
    let config = ScraperConfig::default();
    let scraper = GameScraper::new(&config);
    scraper.parse_html(include_str!("fixtures/cup_match/match-20250204-001.html"), TEST_NEW_CUP_MATCH_URL)
}

async fn process_partial_match() -> Result<Vec<GameData>> {
    let config = ScraperConfig::default();
    let scraper = GameScraper::new(&config);
    scraper.parse_html(include_str!("fixtures/league_match/partially_complete_match.html"), TEST_PARTIAL_MATCH_URL)
}

fn process_match_data(game_scraper: &GameScraper, match_url: &str, html: &str, expected_csv: &str) -> Result<()> {
    // Parse the HTML
    let actual_games = game_scraper.parse_html(html, match_url)?;

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
    let report_html_idx = get_column_index("report_html")?;

    for result in rdr.records() {
        let record = result?;
        let event_start_time = DateTime::from_naive_utc_and_offset(
            NaiveDateTime::parse_from_str(&record[event_start_time_idx], "%Y-%m-%dT%H:%M:%SZ")?,
            Utc,
        );
        let home_player2 = if record[home_player2_idx].is_empty() { None } else { Some(record[home_player2_idx].to_string()) };
        let away_player2 = if record[away_player2_idx].is_empty() { None } else { Some(record[away_player2_idx].to_string()) };

        expected_records.push(GameData {
            event_start_time,
            original_start_time: None,
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
            handicap_home: if record[handicap_home_idx].is_empty() { 0 } else { record[handicap_home_idx].parse()? },
            handicap_away: if record[handicap_away_idx].is_empty() { 0 } else { record[handicap_away_idx].parse()? },
            report_html: if record[report_html_idx].is_empty() { None } else { Some(record[report_html_idx].to_string()) },
        });
    }

    // Compare results
    assert_eq!(actual_games.len(), expected_records.len(), "Number of results doesn't match expected");

    for (i, expected) in expected_records.iter().enumerate() {
        let actual = &actual_games[i];
        
        assert_eq!(
            actual.event_start_time, expected.event_start_time,
            "Mismatch in event_start_time at index {}", i
        );
        assert_eq!(
            actual.match_id, expected.match_id,
            "Mismatch in match_id at index {}", i
        );
        assert_eq!(
            actual.set_number, expected.set_number,
            "Mismatch in set_number at index {}", i
        );
        assert_eq!(
            actual.leg_number, expected.leg_number,
            "Mismatch in leg_number at index {}", i
        );
        assert_eq!(
            actual.competition_type, expected.competition_type,
            "Mismatch in competition_type at index {}", i
        );
        assert_eq!(
            actual.season, expected.season,
            "Mismatch in season at index {}", i
        );
        assert_eq!(
            actual.division, expected.division,
            "Mismatch in division at index {}", i
        );
        assert_eq!(
            actual.venue, expected.venue,
            "Mismatch in venue at index {}", i
        );
        assert_eq!(
            actual.home_team_name, expected.home_team_name,
            "Mismatch in home_team_name at index {}", i
        );
        assert_eq!(
            actual.home_team_club, expected.home_team_club,
            "Mismatch in home_team_club at index {}", i
        );
        assert_eq!(
            actual.away_team_name, expected.away_team_name,
            "Mismatch in away_team_name at index {}", i
        );
        assert_eq!(
            actual.away_team_club, expected.away_team_club,
            "Mismatch in away_team_club at index {}", i
        );
        assert_eq!(
            actual.home_player1, expected.home_player1,
            "Mismatch in home_player1 at index {}", i
        );
        assert_eq!(
            actual.home_player2, expected.home_player2,
            "Mismatch in home_player2 at index {}", i
        );
        assert_eq!(
            actual.away_player1, expected.away_player1,
            "Mismatch in away_player1 at index {}", i
        );
        assert_eq!(
            actual.away_player2, expected.away_player2,
            "Mismatch in away_player2 at index {}", i
        );
        assert_eq!(
            actual.home_score, expected.home_score,
            "Mismatch in home_score at index {}", i
        );
        assert_eq!(
            actual.away_score, expected.away_score,
            "Mismatch in away_score at index {}", i
        );
        assert_eq!(
            actual.handicap_home, expected.handicap_home,
            "Mismatch in handicap_home at index {}", i
        );
        assert_eq!(
            actual.handicap_away, expected.handicap_away,
            "Mismatch in handicap_away at index {}", i
        );
    }

    Ok(())
}

#[tokio::test]
async fn test_league_match() -> Result<()> {
    let config = ScraperConfig::default();
    let scraper = GameScraper::new(&config);
    process_match_data(
        &scraper,
        TEST_LEAGUE_MATCH_URL,
        include_str!("fixtures/league_match/match_104_68_2025_1_31.html"),
        include_str!("fixtures/league_match/match_104_68_2025_1_31.csv"),
    )?;
    Ok(())
}

#[tokio::test]
async fn test_cup_match() -> Result<()> {
    let config = ScraperConfig::default();
    let scraper = GameScraper::new(&config);
    process_match_data(
        &scraper,
        TEST_CUP_MATCH_URL,
        include_str!("fixtures/cup_match/match-20250130-001.html"),
        include_str!("fixtures/cup_match/match-20250130-001.csv"),
    )?;
    Ok(())
}

#[tokio::test]
async fn test_partial_match() -> Result<()> {
    let config = ScraperConfig::default();
    let scraper = GameScraper::new(&config);
    process_match_data(
        &scraper,
        TEST_PARTIAL_MATCH_URL,
        include_str!("fixtures/league_match/partially_complete_match.html"),
        include_str!("fixtures/league_match/partially_complete_match.csv"),
    )?;
    Ok(())
}

#[tokio::test]
async fn test_new_cup_match() -> Result<()> {
    let config = ScraperConfig::default();
    let scraper = GameScraper::new(&config);
    process_match_data(
        &scraper,
        TEST_NEW_CUP_MATCH_URL,
        include_str!("fixtures/cup_match/match-20250204-001.html"),
        include_str!("fixtures/cup_match/match-20250204-001.csv"),
    )?;
    Ok(())
} 
use anyhow::{anyhow, Result};
use chrono::{NaiveDate, NaiveTime, TimeZone, Utc};
use reqwest;
use scraper::{Html, Selector};
use serde::Serialize;
use sqlx::{postgres::PgPoolOptions, types::time::OffsetDateTime};
use std::collections::HashMap;
use tracing::{debug, error, info};
use dotenv::dotenv;
use regex;
use sqlx::postgres::PgPool;

#[derive(Debug, Serialize, PartialEq, Clone)]
struct Fixture {
    date: NaiveDate,
    home_team: String,
    away_team: String,
    venue: String,
    division: String,
    competition_type: CompetitionType,
    status: FixtureStatus,
}

#[derive(Debug, Serialize, PartialEq, Clone)]
enum FixtureStatus {
    Completed(String), // Score as string (e.g. "3-2")
    NotYetPlayed,
    NotReceived,
}

#[derive(Debug, Clone)]
struct TeamInfo {
    team_id: i64,
    team_name: String,
}

impl TeamInfo {
    fn from_full_name(full_name: &str, teams_map: &HashMap<String, Vec<TeamInfo>>) -> Option<TeamInfo> {
        // Normalize the full name for comparison
        let normalized_full_name = full_name.to_lowercase();
        
        // Split the full name into club and team parts
        let parts: Vec<&str> = normalized_full_name.split_whitespace().collect();
        if parts.len() < 2 {
            error!("Invalid team name format: {}", full_name);
            return None;
        }

        // Try to find the club by trying different combinations of the first words
        for i in 1..parts.len() {
            let potential_club = parts[..i].join(" ");
            if let Some(teams) = teams_map.iter().find(|(club, _)| club.to_lowercase() == potential_club) {
                let team_name = parts[i..].join(" ");
                
                // Try exact match first
                if let Some(team) = teams.1.iter().find(|t| t.team_name.to_lowercase() == team_name) {
                    return Some(team.clone());
                }
                
                // If no exact match, try fuzzy match but be more strict
                if let Some(team) = teams.1.iter().find(|t| {
                    let t_norm = t.team_name.to_lowercase();
                    // Only match if one name contains the other completely
                    t_norm == team_name || team_name == t_norm
                }) {
                    debug!("Fuzzy matched team '{}' to database team '{}'", team_name, team.team_name);
                    return Some(team.clone());
                }
            }
        }

        error!("No matching team found for: {} (available teams for matching: {:?})", 
            full_name, 
            teams_map.iter()
                .flat_map(|(club, teams)| teams.iter().map(move |t| format!("{} {}", club, t.team_name)))
                .collect::<Vec<_>>()
        );
        None
    }
}

impl Fixture {
    fn to_utc_datetime(&self) -> OffsetDateTime {
        // Convert NaiveDate to DateTime<Utc> at 19:30
        let naive_time = NaiveTime::from_hms_opt(19, 30, 0).unwrap();
        let naive_datetime = self.date.and_time(naive_time);
        let utc_datetime = Utc.from_utc_datetime(&naive_datetime);
        // Convert to OffsetDateTime
        OffsetDateTime::from_unix_timestamp(utc_datetime.timestamp()).unwrap()
    }
}

#[derive(Debug, sqlx::Type, Serialize, Copy, Clone, PartialEq)]
#[sqlx(type_name = "competition_type_enum")]
pub enum CompetitionType {
    #[sqlx(rename = "MKTTL League")]
    MkttlLeague,
    #[sqlx(rename = "MKTTL Challenge Cup")]
    MkttlChallengeCup,
}

#[derive(Debug)]
struct MonthSummary {
    month: String,
    year: i32,
    match_count: i32,
}

trait HtmlFetcher {
    async fn fetch_html(&self, url: &str) -> Result<String>;
}

struct WebHtmlFetcher {
    client: reqwest::Client,
}

impl WebHtmlFetcher {
    fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
        }
    }
}

impl HtmlFetcher for WebHtmlFetcher {
    async fn fetch_html(&self, url: &str) -> Result<String> {
        Ok(self.client.get(url).send().await?.text().await?)
    }
}

struct FixtureScraper<F: HtmlFetcher> {
    html_fetcher: F,
    db_pool: Option<sqlx::PgPool>,
}

impl<F: HtmlFetcher> FixtureScraper<F> {
    pub fn new(html_fetcher: F) -> Self {
        Self {
            html_fetcher,
            db_pool: None,
        }
    }

    pub async fn with_db(mut self, database_url: &str) -> Result<Self> {
        self.db_pool = Some(
            PgPoolOptions::new()
                .max_connections(5)
                .connect(database_url)
                .await?,
        );
        Ok(self)
    }

    async fn get_or_create_league(&self) -> Result<i64> {
        let pool = self.db_pool.as_ref().ok_or_else(|| anyhow!("Database not connected"))?;
        
        // Get or create the MKTTL league
        let league_id = sqlx::query!(
            r#"
            INSERT INTO league (name, description)
            VALUES ('MKTTL', 'Milton Keynes Table Tennis League')
            ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
            RETURNING id
            "#
        )
        .fetch_one(pool)
        .await?
        .id;

        Ok(league_id)
    }

    async fn get_current_season(&self) -> Result<String> {
        let months = self.get_months_summary().await?;
        if months.is_empty() {
            return Err(anyhow!("No months found in fixture list"));
        }

        // Find the earliest and latest years in the data
        let mut years: Vec<i32> = months.iter().map(|m| m.year).collect();
        years.sort();
        let start_year = years.first().copied().unwrap();
        let end_year = years.last().copied().unwrap();

        Ok(format!("{}-{}", start_year, end_year))
    }

    async fn get_or_create_season(&self, league_id: i64) -> Result<i64> {
        let pool = self.db_pool.as_ref().ok_or_else(|| anyhow!("Database not connected"))?;
        
        // Get the current season from the website data
        let season_name = self.get_current_season().await?;
        
        let season_id = sqlx::query!(
            r#"
            INSERT INTO season (league_id, name)
            VALUES ($1, $2)
            ON CONFLICT (league_id, name) DO UPDATE SET name = EXCLUDED.name
            RETURNING id
            "#,
            league_id,
            season_name
        )
        .fetch_one(pool)
        .await?
        .id;

        Ok(season_id)
    }

    async fn get_or_create_division(&self, league_id: i64, division_name: &str) -> Result<i64> {
        let pool = self.db_pool.as_ref().ok_or_else(|| anyhow!("Database not connected"))?;
        
        // Extract the division name from the full string (e.g., "League - Premier Division" -> "Premier Division")
        let clean_division_name = division_name.split(" - ").last().unwrap_or(division_name);

        let division_id = sqlx::query!(
            r#"
            INSERT INTO division (league_id, name)
            VALUES ($1, $2)
            ON CONFLICT (league_id, name) DO UPDATE SET name = EXCLUDED.name
            RETURNING id
            "#,
            league_id,
            clean_division_name
        )
        .fetch_one(pool)
        .await?
        .id;

        Ok(division_id)
    }

    async fn get_or_create_venue(&self, venue_name: &str) -> Result<i64> {
        let pool = self.db_pool.as_ref().ok_or_else(|| anyhow!("Database not connected"))?;
        
        let venue_id = sqlx::query!(
            r#"
            INSERT INTO venue (name, address)
            VALUES ($1, $1)
            ON CONFLICT (name, address) DO UPDATE SET name = EXCLUDED.name
            RETURNING id
            "#,
            venue_name
        )
        .fetch_one(pool)
        .await?
        .id;

        Ok(venue_id)
    }

    async fn load_teams(&self) -> Result<HashMap<String, Vec<TeamInfo>>> {
        let pool = self.db_pool.as_ref().ok_or_else(|| anyhow!("Database not connected"))?;
        
        // Query to get all clubs and their teams for the current season
        let teams = sqlx::query!(
            r#"
            WITH current_season AS (
                SELECT id 
                FROM season 
                WHERE league_id = (SELECT id FROM league WHERE name = 'MKTTL')
                ORDER BY name DESC 
                LIMIT 1
            )
            SELECT DISTINCT ON (c.name, t.name)
                c.id as club_id,
                c.name as club_name,
                t.id as team_id,
                t.name as team_name
            FROM club c
            JOIN team t ON t.club_id = c.id
            WHERE t.season_id = (SELECT id FROM current_season)
            ORDER BY c.name, t.name, t.season_id DESC
            "#
        )
        .fetch_all(pool)
        .await?;

        // Group teams by club
        let mut teams_map: HashMap<String, Vec<TeamInfo>> = HashMap::new();
        for team in teams {
            teams_map.entry(team.club_name)
                .or_default()
                .push(TeamInfo {
                    team_id: team.team_id,
                    team_name: team.team_name,
                });
        }

        debug!("Loaded {} teams across {} clubs", 
            teams_map.values().map(|v| v.len()).sum::<usize>(),
            teams_map.len()
        );

        Ok(teams_map)
    }

    async fn save_fixture(&self, fixture: &Fixture, league_id: i64, season_id: i64, teams_map: &HashMap<String, Vec<TeamInfo>>) -> Result<()> {
        let pool = self.db_pool.as_ref().ok_or_else(|| anyhow!("Database not connected"))?;

        // Get division
        let division_id = self.get_or_create_division(league_id, &fixture.division).await?;

        // Get venue
        let venue_id = self.get_or_create_venue(&fixture.venue).await?;

        // Get home team info
        let home_team = match TeamInfo::from_full_name(&fixture.home_team, teams_map) {
            Some(team) => team,
            None => {
                error!("Home team not found in database: {}", fixture.home_team);
                return Ok(());
            }
        };

        // Get away team info
        let away_team = match TeamInfo::from_full_name(&fixture.away_team, teams_map) {
            Some(team) => team,
            None => {
                error!("Away team not found in database: {}", fixture.away_team);
                return Ok(());
            }
        };

        // Convert date to UTC datetime at 19:30
        let match_date = fixture.to_utc_datetime();

        // Check if the fixture already exists
        let existing_fixture = sqlx::query!(
            r#"
            SELECT id FROM matches 
            WHERE league_id = $1 
            AND season_id = $2 
            AND home_team_id = $3 
            AND away_team_id = $4 
            AND match_date = $5
            "#,
            league_id,
            season_id,
            home_team.team_id,
            away_team.team_id,
            match_date
        )
        .fetch_optional(pool)
        .await?;

        // Only insert if the fixture doesn't exist
        if existing_fixture.is_none() {
            // Insert the match
            sqlx::query!(
                r#"
                INSERT INTO matches (
                    league_id, season_id, division_id, match_date,
                    home_team_id, away_team_id, venue_id, competition_type
                )
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                "#,
                league_id,
                season_id,
                division_id,
                match_date,
                home_team.team_id,
                away_team.team_id,
                venue_id,
                fixture.competition_type as CompetitionType
            )
            .execute(pool)
            .await?;

            debug!("Inserted fixture: {} vs {} on {}", fixture.home_team, fixture.away_team, fixture.date);
        } else {
            debug!("Skipping existing fixture: {} vs {} on {}", fixture.home_team, fixture.away_team, fixture.date);
        }

        Ok(())
    }

    async fn save_fixtures(&self, fixtures: &[Fixture]) -> Result<()> {
        let league_id = self.get_or_create_league().await?;
        let season_id = self.get_or_create_season(league_id).await?;
        let teams_map = self.load_teams().await?;

        let mut success_count = 0;
        let mut error_count = 0;

        for fixture in fixtures {
            if let Err(e) = self.save_fixture(fixture, league_id, season_id, &teams_map).await {
                error!("Error saving fixture: {:?}", e);
                error_count += 1;
            } else {
                success_count += 1;
            }
        }

        if success_count > 0 {
            info!("Successfully processed {} fixtures", success_count);
        }
        if error_count > 0 {
            error!("Failed to process {} fixtures", error_count);
        }

        Ok(())
    }

    fn parse_months_html(&self, html: &str) -> Result<Vec<MonthSummary>> {
        let document = Html::parse_document(html);
        let row_selector = Selector::parse("table tbody tr").unwrap();
        let cell_selector = Selector::parse("td").unwrap();

        let mut months = Vec::new();
        for row in document.select(&row_selector) {
            let cells: Vec<_> = row.select(&cell_selector).collect();
            if cells.len() >= 2 {
                let month_cell = cells[0].text().collect::<String>();
                let matches_cell = cells[1].text().collect::<String>();

                // Extract month and year from text like "January 2025"
                let parts: Vec<&str> = month_cell.split_whitespace().collect();
                if parts.len() == 2 {
                    if let (Some(month), Some(year_str)) = (parts.first(), parts.get(1)) {
                        if let (Ok(year), Ok(match_count)) = (
                            year_str.parse::<i32>(),
                            matches_cell.trim().parse::<i32>(),
                        ) {
                            months.push(MonthSummary {
                                month: month.to_string(),
                                year,
                                match_count,
                            });
                        }
                    }
                }
            }
        }

        Ok(months)
    }

    fn parse_fixtures_html(&self, html: &str, current_date: NaiveDate) -> Result<Vec<Fixture>> {
        let document = Html::parse_document(html);
        let row_selector = Selector::parse("table tbody tr").unwrap();
        let cell_selector = Selector::parse("td").unwrap();
        let link_selector = Selector::parse("a").unwrap();

        let mut fixtures = Vec::new();
        for row in document.select(&row_selector) {
            let cells: Vec<_> = row.select(&cell_selector).collect();
            if cells.len() >= 7 {
                // Try to parse date from the sortable date cell first
                let date = cells.iter().find(|cell| {
                    cell.value().attr("data-label").map_or(false, |label| label == "Date (sortable)")
                }).and_then(|cell| {
                    let date_str = cell.text().collect::<String>().trim().to_string();
                    NaiveDate::parse_from_str(&date_str, "%Y-%m-%d").ok()
                }).or_else(|| {
                    // Fallback to parsing from the display date cell
                    let date_str = cells[2].text().collect::<String>().trim().to_string();
                    NaiveDate::parse_from_str(&date_str, "%d/%m/%Y").ok()
                });

                let date = match date {
                    Some(d) => d,
                    None => continue,
                };

                // Extract other fields
                let division = cells.iter()
                    .find(|cell| cell.value().attr("data-label").map_or(false, |label| label == "Competition"))
                    .map(|cell| cell.text().collect::<String>().trim().to_string())
                    .unwrap_or_else(|| cells[0].text().collect::<String>().trim().to_string());

                // Extract home team from URL
                let home_team = cells.iter()
                    .find(|cell| cell.value().attr("data-label").map_or(false, |label| label == "Home team"))
                    .and_then(|cell| cell.select(&link_selector).next())
                    .and_then(|link| link.value().attr("href"))
                    .map(|url| {
                        let parts: Vec<&str> = url.split('/').collect();
                        if parts.len() >= 2 {
                            let club = parts[parts.len() - 2].replace("-", " ");
                            let team = parts[parts.len() - 1].replace("-", " ");
                            format!("{} {}", club, team)
                        } else {
                            String::new()
                        }
                    })
                    .unwrap_or_default();

                // Extract away team from URL
                let away_team = cells.iter()
                    .find(|cell| cell.value().attr("data-label").map_or(false, |label| label == "Away team"))
                    .and_then(|cell| cell.select(&link_selector).next())
                    .and_then(|link| link.value().attr("href"))
                    .map(|url| {
                        let parts: Vec<&str> = url.split('/').collect();
                        if parts.len() >= 2 {
                            let club = parts[parts.len() - 2].replace("-", " ");
                            let team = parts[parts.len() - 1].replace("-", " ");
                            format!("{} {}", club, team)
                        } else {
                            String::new()
                        }
                    })
                    .unwrap_or_default();

                let venue = cells.iter()
                    .find(|cell| cell.value().attr("data-label").map_or(false, |label| label == "Venue"))
                    .map(|cell| cell.text().collect::<String>().trim().to_string())
                    .unwrap_or_default();

                // Find the score cell by data-label
                let score_cell = cells.iter()
                    .find(|cell| cell.value().attr("data-label").map_or(false, |label| label == "Score"))
                    .unwrap();

                let score_text = score_cell.text().collect::<String>();
                let score_text = score_text.trim();

                // First check if the match is in the future based on date
                let status = if date > current_date {
                    FixtureStatus::NotYetPlayed
                } else if score_text.contains("Not received") || score_text.contains("Not Received") || score_text.contains("not received") {
                    FixtureStatus::NotReceived
                } else if score_text.contains("Not yet played") || score_text.is_empty() || score_text == "-" || score_text == "\u{a0}" || score_text == "TBC" || score_text == "tbc" || score_text == "TBA" || score_text == "tba" {
                    // For matches on or before current date that are not played, mark as not received
                    FixtureStatus::NotReceived
                } else {
                    // Try to find a score pattern (e.g., "5-4", "6-3", "428-246", etc.)
                    let score_pattern = regex::Regex::new(r"^\s*\d+\s*-\s*\d+\s*$").unwrap();
                    if score_pattern.is_match(score_text) {
                        FixtureStatus::Completed(score_text.to_string())
                    } else if score_text.contains("Postponed") || score_text.contains("Conceded") || score_text.contains("Awarded") || score_text.contains("Walkover") || score_text.contains("Void") || score_text.contains("Cancelled") || score_text.contains("Forfeit") || score_text.contains("Defaulted") {
                        FixtureStatus::Completed(score_text.to_string())
                    } else {
                        // Try to parse it as a score with extra text
                        let score_pattern = regex::Regex::new(r"\d+\s*-\s*\d+").unwrap();
                        if let Some(score) = score_pattern.find(score_text) {
                            FixtureStatus::Completed(score.as_str().to_string())
                        } else {
                            // Try to parse it as a score with a dash instead of a hyphen
                            let score_pattern = regex::Regex::new(r"\d+\s*–\s*\d+").unwrap();
                            if let Some(score) = score_pattern.find(score_text) {
                                let normalized_score = score.as_str().replace('–', "-");
                                FixtureStatus::Completed(normalized_score)
                            } else {
                                continue // Skip if we can't determine the status
                            }
                        }
                    }
                };

                // Determine competition type based on the page content
                let competition_type = if division.contains("Challenge Cup") {
                    CompetitionType::MkttlChallengeCup
                } else {
                    CompetitionType::MkttlLeague
                };

                fixtures.push(Fixture {
                    date,
                    home_team,
                    away_team,
                    venue,
                    division,
                    competition_type,
                    status,
                });
            }
        }

        Ok(fixtures)
    }

    pub async fn get_months_summary(&self) -> Result<Vec<MonthSummary>> {
        let html = self
            .html_fetcher
            .fetch_html("https://www.mkttl.co.uk/fixtures-results/months")
            .await?;
        self.parse_months_html(&html)
    }

    pub async fn get_fixtures_for_month(&self, year: i32, month: u32, current_date: NaiveDate) -> Result<Vec<Fixture>> {
        let url = format!(
            "https://www.mkttl.co.uk/fixtures-results/months/{}/{}",
            year,
            month
        );
        let html = self.html_fetcher.fetch_html(&url).await?;
        self.parse_fixtures_html(&html, current_date)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;

    struct TestHtmlFetcher {
        fixtures_dir: PathBuf,
    }

    impl TestHtmlFetcher {
        fn new() -> Self {
            let mut fixtures_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
            fixtures_dir.push("tests/fixtures/fixutre_scraper");
            Self { fixtures_dir }
        }
    }

    impl HtmlFetcher for TestHtmlFetcher {
        async fn fetch_html(&self, url: &str) -> Result<String> {
            let filename = if url.ends_with("/fixtures-results/months") {
                "fixtures-results_months.html"
            } else if url.contains("/fixtures-results/months/2025/2") || url.contains("/fixtures-results/months/2025/02") {
                "fixtures-results_months_2025_02.html"
            } else {
                return Err(anyhow!("Unexpected URL in test: {}", url));
            };

            let path = self.fixtures_dir.join(filename);
            Ok(fs::read_to_string(path)?)
        }
    }

    #[tokio::test]
    async fn test_parse_months_summary() {
        let scraper = FixtureScraper::new(TestHtmlFetcher::new());
        let months = scraper.get_months_summary().await.unwrap();

        let expected = vec![
            MonthSummary { month: "September".to_string(), year: 2024, match_count: 69 },
            MonthSummary { month: "October".to_string(), year: 2024, match_count: 152 },
            MonthSummary { month: "November".to_string(), year: 2024, match_count: 131 },
            MonthSummary { month: "December".to_string(), year: 2024, match_count: 96 },
            MonthSummary { month: "January".to_string(), year: 2025, match_count: 114 },
            MonthSummary { month: "February".to_string(), year: 2025, match_count: 89 },
            MonthSummary { month: "March".to_string(), year: 2025, match_count: 96 },
        ];

        assert_eq!(months.len(), expected.len());
        for (actual, expected) in months.iter().zip(expected.iter()) {
            assert_eq!(actual.month, expected.month);
            assert_eq!(actual.year, expected.year);
            assert_eq!(actual.match_count, expected.match_count);
        }
    }

    #[tokio::test]
    async fn test_parse_february_fixtures() {
        let scraper = FixtureScraper::new(TestHtmlFetcher::new());
        let current_date = chrono::Local::now().date_naive();
        let fixtures = scraper.get_fixtures_for_month(2025, 2, current_date).await.unwrap();

        // Log total number of matches found
        println!("\nTotal matches found: {}", fixtures.len());

        // Count each type
        let completed = fixtures.iter().filter(|f| matches!(f.status, FixtureStatus::Completed(_))).count();
        let future = fixtures.iter().filter(|f| matches!(f.status, FixtureStatus::NotYetPlayed)).count();
        let cancelled = fixtures.iter().filter(|f| matches!(f.status, FixtureStatus::NotReceived)).count();

        // Log the breakdown
        println!("Status breakdown:");
        println!("  Completed matches: {}", completed);
        println!("  Future matches: {}", future);
        println!("  Cancelled matches: {}", cancelled);

        assert_eq!(completed, 56, "Expected 56 completed matches");
        assert_eq!(future, 49, "Expected 49 future matches");
        assert_eq!(cancelled, 4, "Expected 4 cancelled matches");

        // Also assert the total number of fixtures
        assert_eq!(fixtures.len(), 109, "Expected 109 total matches");
    }
}

// Main application code
#[tokio::main]
async fn main() -> Result<()> {
    // Load .env file
    dotenv().ok();
    
    // Initialize tracing subscriber
    tracing_subscriber::fmt::init();

    // Get database URL from environment and log it
    let database_url = std::env::var("DATABASE_URL")
        .map_err(|_| anyhow!("DATABASE_URL must be set"))?;
    
    info!("Using database URL: {}", database_url);

    // Create and run scraper
    let scraper = FixtureScraper::new(WebHtmlFetcher::new())
        .with_db(&database_url)
        .await?;

    info!("Starting fixture validation...");
    
    // Get all month summaries
    let months = scraper.get_months_summary().await?;
    info!("Found {} months to process", months.len());

    // Get current date for determining future matches
    let current_date = chrono::Local::now().date_naive();

    // Process each month
    for month in &months {
        let month_number = match month.month.as_str() {
            "January" => 1,
            "February" => 2,
            "March" => 3,
            "April" => 4,
            "May" => 5,
            "June" => 6,
            "July" => 7,
            "August" => 8,
            "September" => 9,
            "October" => 10,
            "November" => 11,
            "December" => 12,
            _ => continue,
        };

        // Get fixtures from the website for this month
        let fixtures = scraper.get_fixtures_for_month(month.year, month_number, current_date).await?;
        
        // Get the database pool
        let pool = scraper.db_pool.as_ref().ok_or_else(|| anyhow!("Database not connected"))?;

        // Count matches in database for this month
        let start_date = NaiveDate::from_ymd_opt(month.year, month_number, 1).unwrap();
        let end_date = if month_number == 12 {
            NaiveDate::from_ymd_opt(month.year + 1, 1, 1).unwrap()
        } else {
            NaiveDate::from_ymd_opt(month.year, month_number + 1, 1).unwrap()
        };

        let start_datetime = start_date.and_time(NaiveTime::from_hms_opt(0, 0, 0).unwrap());
        let end_datetime = end_date.and_time(NaiveTime::from_hms_opt(0, 0, 0).unwrap());

        let db_count = sqlx::query!(
            r#"
            SELECT COUNT(*) as count
            FROM matches
            WHERE match_date >= $1 AND match_date < $2
            "#,
            OffsetDateTime::from_unix_timestamp(Utc.from_utc_datetime(&start_datetime).timestamp()).unwrap(),
            OffsetDateTime::from_unix_timestamp(Utc.from_utc_datetime(&end_datetime).timestamp()).unwrap(),
        )
        .fetch_one(pool)
        .await?
        .count
        .unwrap_or(0) as i32;

        // Compare counts
        if db_count != month.match_count {
            info!("{} {}: Website shows {} matches, database has {}", 
                month.month, 
                month.year,
                month.match_count,
                db_count
            );

            // Find missing matches
            let mut missing_matches = Vec::new();
            let mut missing_count = 0;
            for fixture in &fixtures {
                let match_date = fixture.to_utc_datetime();
                
                // Check if match exists in database
                let exists = sqlx::query!(
                    r#"
                    SELECT COUNT(*) as count
                    FROM matches m
                    JOIN team home ON m.home_team_id = home.id
                    JOIN team away ON m.away_team_id = away.id
                    WHERE home.name = $1 
                    AND away.name = $2 
                    AND m.match_date = $3
                    "#,
                    fixture.home_team,
                    fixture.away_team,
                    match_date
                )
                .fetch_one(pool)
                .await?
                .count
                .unwrap_or(0) > 0;

                if !exists {
                    missing_matches.push(fixture);
                    missing_count += 1;
                }
            }

            if !missing_matches.is_empty() {
                info!("Found {} missing matches for {} {}", missing_count, month.month, month.year);

                // Only insert future matches
                let future_matches: Vec<_> = missing_matches.into_iter()
                    .filter(|f| f.date > current_date)
                    .collect();

                if !future_matches.is_empty() {
                    info!("Inserting {} future matches for {} {}", future_matches.len(), month.month, month.year);
                    // Convert Vec<&Fixture> to Vec<Fixture>
                    let owned_fixtures: Vec<Fixture> = future_matches.into_iter().map(|f| f.clone()).collect();
                    scraper.save_fixtures(&owned_fixtures).await?;
                }
            }
        } else {
            debug!("{} {}: Match count verified ({} matches)", 
                month.month, 
                month.year,
                month.match_count
            );
        }
    }

    info!("Fixture validation complete");
    Ok(())
} 
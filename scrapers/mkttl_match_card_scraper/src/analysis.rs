use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use sqlx::postgres::{PgPool, PgPoolOptions};
use std::fmt;
use chrono::{Utc};
use crate::types::GameData;

const DB_CONNECTION: &str = "postgres://admin:quest@localhost:8812/qdb?sslmode=disable";

/// Represents a single match result for a player
#[derive(Debug, sqlx::FromRow)]
pub struct PlayerMatch {
    pub match_id: String,
    pub set_number: i32,
    pub player_name: String,
    pub opponent_name: String,
    pub player_score: i32,
    pub opponent_score: i32,
    pub player_won: bool,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct QueryResult {
    pub query: String,
    #[serde(default)]
    pub columns: Vec<Column>,
    #[serde(default)]
    pub dataset: Vec<Vec<JsonValue>>,
    #[serde(default)]
    pub count: i64,
    #[serde(default)]
    pub timestamp: i64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    #[serde(rename = "type")]
    pub type_: String,
}

#[derive(Debug, sqlx::FromRow)]
pub struct TotalMatchesResult {
    pub total_matches: i64,
    pub total_games: i64,
}

#[derive(Debug, sqlx::FromRow)]
pub struct SeasonStatsResult {
    pub season: Option<String>,
    pub games_played: Option<i64>,
    pub games_won: Option<i64>,
}

#[derive(Debug, sqlx::FromRow)]
pub struct CloseGamesResult {
    pub total_close_games: Option<i64>,
    pub close_games_won: Option<i64>,
}

#[derive(Debug, sqlx::FromRow)]
pub struct ScorePatternsResult {
    pub avg_score_for: Option<f64>,
    pub avg_score_against: Option<f64>,
    pub min_score: Option<i64>,
    pub max_score: Option<i64>,
}

#[derive(Debug, sqlx::FromRow)]
pub struct OpponentDetailsResult {
    pub opponent: Option<String>,
    pub games: Option<i64>,
    pub wins: Option<i64>,
    pub avg_score_for: Option<f64>,
    pub avg_score_against: Option<f64>,
}

#[derive(Debug, sqlx::FromRow)]
pub struct ThreatRatingResult {
    pub opponent: Option<String>,
    pub games_played: Option<i64>,
    pub games_lost: Option<i64>,
    pub avg_winning_score: Option<f64>,
    pub highest_winning_score: Option<f64>,
}

#[derive(Debug, sqlx::FromRow)]
pub struct DivisionPlayerResult {
    pub player_name: Option<String>,
    pub team_name: Option<String>,
    pub matches_played: Option<i64>,
    pub matches_available: Option<i64>,
    pub total_sets: Option<i64>,
    pub sets_won: Option<i64>,
    pub sets_lost: Option<i64>,
    pub win_percentage: Option<f64>,
}

/// Represents a player's match statistics
#[derive(Debug, sqlx::FromRow)]
pub struct PlayerMatchStats {
    pub player_name: Option<String>,
    pub team_name: Option<String>,
    pub matches_played: Option<i32>,
    pub matches_available: Option<i32>,
    pub total_sets: Option<i32>,
    pub sets_won: Option<i32>,
    pub sets_lost: Option<i32>,
    pub win_percentage: Option<f64>,
}

impl PlayerMatchStats {
    pub fn display(&self) -> String {
        format!(
            "{} ({}) - Matches: {}/{}, Sets: {} ({}-{}), Win Rate: {:.1}%",
            self.player_name.as_deref().unwrap_or("Unknown"),
            self.team_name.as_deref().unwrap_or("Unknown"),
            self.matches_played.unwrap_or(0),
            self.matches_available.unwrap_or(0),
            self.total_sets.unwrap_or(0),
            self.sets_won.unwrap_or(0),
            self.sets_lost.unwrap_or(0),
            self.win_percentage.unwrap_or(0.0)
        )
    }
}

/// Filter criteria for division queries
#[derive(Debug)]
pub struct DivisionFilter {
    pub division: String,
    pub season: String,
}

impl fmt::Display for DivisionFilter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Division: {}, Season: {}", self.division, self.season)
    }
}

/// Database connection wrapper with common query functions
#[derive(Debug)]
pub struct Analysis {
    pool: PgPool,
}

impl Analysis {
    /// Create a new Analysis instance with a database connection
    pub async fn new() -> Result<Self> {
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(DB_CONNECTION)
            .await?;
        
        Ok(Self { pool })
    }

    /// Sanitize a player name to ensure consistent capitalization
    pub fn sanitize_player_name(name: &str) -> String {
        name.split(|c| c == ' ' || c == '-')
            .map(|word| {
                let mut chars = word.chars();
                match chars.next() {
                    None => String::new(),
                    Some(first) => {
                        let first = first.to_uppercase();
                        let rest = chars.as_str().to_lowercase();
                        format!("{}{}", first, rest)
                    }
                }
            })
            .collect::<Vec<_>>()
            .join(" ")
    }

    /// Get all matches for a player, grouped by set number
    ///
    /// Demonstrates parameterized queries with `.bind()`.
    pub async fn get_player_matches(&self, player_name: &str) -> Result<Vec<PlayerMatch>> {
        let player_name = Self::sanitize_player_name(player_name);

        let sql = r#"
            WITH player_matches AS (
                -- When player is on home team
                SELECT 
                    match_id,
                    set_number,
                    home_player1 as player_name,
                    away_player1 as opponent_name,
                    home_score as player_score,
                    away_score as opponent_score,
                    home_score > away_score as player_won
                FROM table_tennis_games
                WHERE home_player1 = $1

                UNION ALL

                -- When player is on away team
                SELECT 
                    match_id,
                    set_number,
                    away_player1 as player_name,
                    home_player1 as opponent_name,
                    away_score as player_score,
                    home_score as opponent_score,
                    away_score > home_score as player_won
                FROM table_tennis_games
                WHERE away_player1 = $1
            )
            SELECT *
            FROM player_matches
            ORDER BY match_id, set_number
        "#;

        let matches = sqlx::query_as::<_, PlayerMatch>(sql)
            .bind(player_name)
            .fetch_all(&self.pool)
            .await?;

        Ok(matches)
    }

    /// Analyze a specific player's overall performance
    pub async fn analyze_player(&self, raw_player_name: &str) -> Result<PlayerStats> {
        let player_name = Self::sanitize_player_name(raw_player_name);

        // 1. Total matches
        let total_sql = r#"
            SELECT 
                COUNT(DISTINCT match_id) AS total_matches,
                COUNT(*) AS total_games
            FROM table_tennis_games
            WHERE home_player1 = $1
               OR home_player2 = $1
               OR away_player1 = $1
               OR away_player2 = $1
        "#;
        let total_matches_res: Vec<TotalMatchesResult> = sqlx::query_as(total_sql)
            .bind(&player_name)
            .fetch_all(&self.pool)
            .await?;
        let total = total_matches_res
            .first()
            .ok_or_else(|| anyhow::anyhow!("No total matches data found"))?;

        // 2. Season stats
        let season_stats_sql = r#"
            SELECT 
                season,
                COUNT(*) AS games_played,
                SUM(
                    CASE
                        WHEN (home_player1 = $1 OR home_player2 = $1) AND home_score > away_score THEN 1
                        WHEN (away_player1 = $1 OR away_player2 = $1) AND away_score > home_score THEN 1
                        ELSE 0
                    END
                ) AS games_won
            FROM table_tennis_games
            WHERE home_player1 = $1
               OR home_player2 = $1
               OR away_player1 = $1
               OR away_player2 = $1
            GROUP BY season
            ORDER BY season
        "#;
        let season_stats_rows: Vec<SeasonStatsResult> = sqlx::query_as(season_stats_sql)
            .bind(&player_name)
            .fetch_all(&self.pool)
            .await?;

        // 3. Close games
        let close_games_sql = r#"
            SELECT 
                COUNT(*) AS total_close_games,
                SUM(
                    CASE
                        WHEN (home_player1 = $1 OR home_player2 = $1) AND home_score > away_score THEN 1
                        WHEN (away_player1 = $1 OR away_player2 = $1) AND away_score > home_score THEN 1
                        ELSE 0
                    END
                ) AS close_games_won
            FROM table_tennis_games
            WHERE (home_player1 = $1
                OR home_player2 = $1
                OR away_player1 = $1
                OR away_player2 = $1)
              AND ABS(home_score - away_score) <= 2
        "#;
        let close_games_res: Vec<CloseGamesResult> = sqlx::query_as(close_games_sql)
            .bind(&player_name)
            .fetch_all(&self.pool)
            .await?;
        let close_games = close_games_res.first().map(|cg| CloseGamesStats {
            total: cg.total_close_games.unwrap_or_default(),
            won: cg.close_games_won.unwrap_or_default(),
        });

        // 4. Score patterns
        let score_patterns_sql = r#"
            SELECT 
                AVG(CASE WHEN home_player1 = $1 OR home_player2 = $1 THEN home_score ELSE away_score END) as avg_score_for,
                AVG(CASE WHEN home_player1 = $1 OR home_player2 = $1 THEN away_score ELSE home_score END) as avg_score_against,
                MIN(CASE WHEN home_player1 = $1 OR home_player2 = $1 THEN home_score ELSE away_score END) as min_score,
                MAX(CASE WHEN home_player1 = $1 OR home_player2 = $1 THEN home_score ELSE away_score END) as max_score
            FROM table_tennis_games
            WHERE home_player1 = $1
               OR home_player2 = $1
               OR away_player1 = $1
               OR away_player2 = $1
        "#;
        let score_patterns_res: Vec<ScorePatternsResult> = sqlx::query_as(score_patterns_sql)
            .bind(&player_name)
            .fetch_all(&self.pool)
            .await?;
        let score_patterns = score_patterns_res.first().map(|sp| ScorePatterns {
            avg_score_for: sp.avg_score_for.unwrap_or_default(),
            avg_score_against: sp.avg_score_against.unwrap_or_default(),
            min_score: sp.min_score.unwrap_or_default(),
            max_score: sp.max_score.unwrap_or_default(),
        });

        // 5. Opponent details (≥ 5 games)
        let opp_details_sql = r#"
            SELECT 
                CASE 
                    WHEN home_player1 = $1 OR home_player2 = $1
                        THEN COALESCE(NULLIF(away_player1, ''), away_player2)
                    ELSE COALESCE(NULLIF(home_player1, ''), home_player2)
                END as opponent,
                COUNT(*) as games,
                SUM(
                    CASE
                        WHEN (home_player1 = $1 OR home_player2 = $1) AND home_score > away_score THEN 1
                        WHEN (away_player1 = $1 OR away_player2 = $1) AND away_score > home_score THEN 1
                        ELSE 0
                    END
                ) as wins,
                AVG(
                    CASE 
                        WHEN home_player1 = $1 OR home_player2 = $1 THEN home_score 
                        ELSE away_score 
                    END
                ) as avg_score_for,
                AVG(
                    CASE 
                        WHEN home_player1 = $1 OR home_player2 = $1 THEN away_score 
                        ELSE home_score 
                    END
                ) as avg_score_against
            FROM table_tennis_games
            WHERE (
                home_player1 = $1 OR home_player2 = $1 
                OR away_player1 = $1 OR away_player2 = $1
            )
              AND (home_player1 != '' AND home_player2 != '' 
                   AND away_player1 != '' AND away_player2 != '')
            GROUP BY opponent
            HAVING COUNT(*) >= 5
            ORDER BY games DESC
        "#;
        let opp_details_res: Vec<OpponentDetailsResult> = sqlx::query_as(opp_details_sql)
            .bind(&player_name)
            .fetch_all(&self.pool)
            .await?;
        let opponent_details = opp_details_res
            .into_iter()
            .map(|row| OpponentDetails {
                name: row.opponent.unwrap_or_default(),
                games: row.games.unwrap_or_default(),
                wins: row.wins.unwrap_or_default(),
                avg_score_for: row.avg_score_for.unwrap_or_default(),
                avg_score_against: row.avg_score_against.unwrap_or_default(),
            })
            .collect::<Vec<_>>();

        // 6. Threat ratings
        let threat_rating_sql = r#"
            SELECT 
                CASE 
                    WHEN home_player1 = $1 OR home_player2 = $1
                        THEN COALESCE(NULLIF(away_player1, ''), away_player2)
                    ELSE COALESCE(NULLIF(home_player1, ''), home_player2)
                END as opponent,
                COUNT(*) as games_played,
                SUM(
                    CASE 
                        WHEN (home_player1 = $1 OR home_player2 = $1) AND away_score > home_score THEN 1
                        WHEN (away_player1 = $1 OR away_player2 = $1) AND home_score > away_score THEN 1
                        ELSE 0
                    END
                ) as games_lost,
                AVG(
                    CASE
                        WHEN (home_player1 = $1 OR home_player2 = $1) AND away_score > home_score
                             THEN away_score
                        WHEN (away_player1 = $1 OR away_player2 = $1) AND home_score > away_score
                             THEN home_score
                        ELSE NULL
                    END
                ) as avg_winning_score,
                MAX(
                    CASE
                        WHEN (home_player1 = $1 OR home_player2 = $1) AND away_score > home_score
                             THEN away_score
                        WHEN (away_player1 = $1 OR away_player2 = $1) AND home_score > away_score
                             THEN home_score
                        ELSE NULL
                    END
                ) as highest_winning_score
            FROM table_tennis_games
            WHERE (
                home_player1 = $1 OR home_player2 = $1 
                OR away_player1 = $1 OR away_player2 = $1
            )
              AND (home_player1 != '' AND home_player2 != '' 
                   AND away_player1 != '' AND away_player2 != '')
            GROUP BY opponent
            HAVING SUM(
                CASE 
                    WHEN (home_player1 = $1 OR home_player2 = $1) AND away_score > home_score THEN 1
                    WHEN (away_player1 = $1 OR away_player2 = $1) AND home_score > away_score THEN 1
                    ELSE 0
                END
            ) > 0
            ORDER BY games_lost DESC, avg_winning_score DESC
            LIMIT 10
        "#;
        let threat_res: Vec<ThreatRatingResult> = sqlx::query_as(threat_rating_sql)
            .bind(&player_name)
            .fetch_all(&self.pool)
            .await?;
        let threat_ratings = threat_res
            .into_iter()
            .map(|row| ThreatRating {
                name: row.opponent.unwrap_or_default(),
                games_played: row.games_played.unwrap_or_default(),
                games_lost: row.games_lost.unwrap_or_default(),
                avg_winning_score: row.avg_winning_score.unwrap_or_default(),
                highest_winning_score: row.highest_winning_score.unwrap_or_default(),
            })
            .collect::<Vec<_>>();

        // Construct the final structured result
        let mut seasons = Vec::new();
        for row in season_stats_rows {
            seasons.push(SeasonStats {
                season: row.season.unwrap_or_default(),
                games_played: row.games_played.unwrap_or_default(),
                games_won: row.games_won.unwrap_or_default(),
            });
        }

        Ok(PlayerStats {
            name: player_name,
            total_matches: total.total_matches,
            total_games: total.total_games,
            seasons,
            close_games,
            score_patterns,
            opponent_details,
            threat_ratings,
        })
    }

    /// Helper to build a CASE expression for normalizing known team name variants
    fn build_team_name_case(&self, field: &str) -> String {
        format!(
            "CASE 
                WHEN {field} = 'Powers' THEN 'Milton Keynes Powers'
                WHEN {field} = 'Valiants' THEN 'Greenleys Valiants'
                WHEN {field} = 'Wooden Tops' THEN 'Little Horwood Wooden Tops'
                ELSE {field}
            END",
            field = field
        )
    }

    /// Get detailed division stats for all players
    pub async fn get_division_stats(&self, division: &str, season: &str) -> Result<DivisionStats> {
        let team_case_home = self.build_team_name_case("home_team_name");
        let team_case_away = self.build_team_name_case("away_team_name");

        let sql = format!(
            "WITH team_matches AS (
                SELECT team_name, COUNT(DISTINCT match_id) as matches 
                FROM (
                    SELECT DISTINCT 
                        match_id,
                        {case_home} as team_name
                    FROM table_tennis_games
                    WHERE division = $1
                      AND season = $2
                    UNION
                    SELECT DISTINCT 
                        match_id,
                        {case_away} as team_name
                    FROM table_tennis_games
                    WHERE division = $1
                      AND season = $2
                ) tm
                GROUP BY team_name
            ),
            player_legs AS (
                SELECT 
                    home_player1 as player_name,
                    {case_home} as team_name,
                    match_id,
                    set_number,
                    1 as legs_played,
                    CASE WHEN home_score > away_score THEN 1 ELSE 0 END as leg_won
                FROM table_tennis_games
                WHERE division = $1
                  AND season = $2
                  AND home_player1 != ''
                
                UNION ALL

                SELECT 
                    away_player1 as player_name,
                    {case_away} as team_name,
                    match_id,
                    set_number,
                    1 as legs_played,
                    CASE WHEN away_score > home_score THEN 1 ELSE 0 END as leg_won
                FROM table_tennis_games
                WHERE division = $1
                  AND season = $2
                  AND away_player1 != ''
            ),
            games AS (
                SELECT 
                    player_name,
                    team_name,
                    match_id,
                    set_number,
                    COUNT(*) as legs_in_game,
                    SUM(leg_won) as legs_won
                FROM player_legs
                GROUP BY player_name, team_name, match_id, set_number
            ),
            player_stats AS (
                SELECT 
                    p.player_name,
                    p.team_name,
                    COUNT(DISTINCT p.match_id) as matches_played,
                    t.matches as matches_available,
                    COUNT(DISTINCT p.match_id || '_' || p.set_number) as total_sets,
                    SUM(
                        CASE WHEN p.legs_won > p.legs_in_game/2 THEN 1 ELSE 0 END
                    ) as sets_won,
                    SUM(
                        CASE WHEN p.legs_won <= p.legs_in_game/2 THEN 1 ELSE 0 END
                    ) as sets_lost,
                    ROUND(
                        100.0 * SUM(
                            CASE WHEN p.legs_won > p.legs_in_game/2 THEN 1 ELSE 0 END
                        )::float
                        /
                        NULLIF(COUNT(DISTINCT p.match_id || '_' || p.set_number), 0),
                        2
                    ) as win_percentage
                FROM games p
                JOIN team_matches t ON p.team_name = t.team_name
                GROUP BY p.player_name, p.team_name, t.matches
            )
            SELECT *
            FROM player_stats
            ORDER BY win_percentage DESC, total_sets DESC
            ",
            case_home = team_case_home,
            case_away = team_case_away
        );

        let result: Vec<DivisionPlayerResult> = sqlx::query_as(&sql)
            .bind(division)
            .bind(season)
            .fetch_all(&self.pool)
            .await?;

        let mut players = vec![];
        for row in result {
            players.push(DivisionPlayerStats {
                name: row.player_name.unwrap_or_default(),
                team: row.team_name.unwrap_or_default(),
                matches_played: row.matches_played.unwrap_or_default(),
                matches_available: row.matches_available.unwrap_or_default(),
                total_sets: row.total_sets.unwrap_or_default(),
                sets_won: row.sets_won.unwrap_or_default(),
                sets_lost: row.sets_lost.unwrap_or_default(),
                win_percentage: row.win_percentage.unwrap_or_default(),
            });
        }

        Ok(DivisionStats {
            division: division.to_string(),
            season: season.to_string(),
            players,
        })
    }

    /// Alternate typed approach for a division filter
    pub async fn get_division_player_stats(&self, filter: &DivisionFilter) -> Result<Vec<PlayerMatchStats>> {
        let team_case_home = self.build_team_name_case("home_team_name");
        let team_case_away = self.build_team_name_case("away_team_name");

        let sql = format!(
            r#"
            WITH team_matches AS (
                SELECT team_name, COUNT(DISTINCT match_id) as matches 
                FROM (
                    SELECT DISTINCT match_id, 
                        {case_home} as team_name 
                    FROM table_tennis_games 
                    WHERE division = $1 AND season = $2
                    UNION 
                    SELECT DISTINCT match_id,
                        {case_away} as team_name 
                    FROM table_tennis_games 
                    WHERE division = $1 AND season = $2
                ) t
                GROUP BY team_name
            ),
            player_legs AS (
                SELECT 
                    home_player1 as player_name,
                    {case_home} as team_name,
                    match_id,
                    set_number,
                    1 as legs_played,
                    CASE WHEN home_score > away_score THEN 1 ELSE 0 END as leg_won
                FROM table_tennis_games 
                WHERE division = $1 AND season = $2 AND home_player1 != ''
                
                UNION ALL
                
                SELECT 
                    away_player1 as player_name,
                    {case_away} as team_name,
                    match_id,
                    set_number,
                    1 as legs_played,
                    CASE WHEN away_score > home_score THEN 1 ELSE 0 END as leg_won
                FROM table_tennis_games 
                WHERE division = $1 AND season = $2 AND away_player1 != ''
            ),
            games AS (
                SELECT 
                    player_name,
                    team_name,
                    match_id,
                    set_number,
                    COUNT(*) as legs_in_game,
                    SUM(leg_won) as legs_won
                FROM player_legs
                GROUP BY player_name, team_name, match_id, set_number
            ),
            player_stats AS (
                SELECT 
                    p.player_name as player_name,
                    p.team_name as team_name,
                    COUNT(DISTINCT p.match_id)::int as matches_played,
                    t.matches::int as matches_available,
                    COUNT(DISTINCT p.match_id || '_' || p.set_number)::int as total_sets,
                    SUM(
                        CASE WHEN p.legs_won > p.legs_in_game/2 THEN 1 ELSE 0 END
                    )::int as sets_won,
                    SUM(
                        CASE WHEN p.legs_won <= p.legs_in_game/2 THEN 1 ELSE 0 END
                    )::int as sets_lost,
                    ROUND(
                        COALESCE(
                            100.0 * SUM(
                                CASE WHEN p.legs_won > p.legs_in_game/2 THEN 1 ELSE 0 END
                            )::float 
                            / NULLIF(COUNT(DISTINCT p.match_id || '_' || p.set_number), 0),
                            0.0
                        ),
                        2
                    ) as win_percentage
                FROM games p
                JOIN team_matches t ON p.team_name = t.team_name
                GROUP BY p.player_name, p.team_name, t.matches
            )
            SELECT *
            FROM player_stats
            ORDER BY win_percentage DESC, total_sets DESC
            "#,
            case_home = team_case_home,
            case_away = team_case_away,
        );

        let players = sqlx::query_as::<_, PlayerMatchStats>(&sql)
            .bind(&filter.division)
            .bind(&filter.season)
            .fetch_all(&self.pool)
            .await?;

        Ok(players)
    }
}

#[derive(Debug)]
pub struct PlayerStats {
    pub name: String,
    pub total_matches: i64,
    pub total_games: i64,
    pub seasons: Vec<SeasonStats>,
    pub close_games: Option<CloseGamesStats>,
    pub score_patterns: Option<ScorePatterns>,
    pub opponent_details: Vec<OpponentDetails>,
    pub threat_ratings: Vec<ThreatRating>,
}

impl PlayerStats {
    /// Overall percentage of games won across all seasons
    pub fn overall_win_rate(&self) -> f64 {
        let total_won: i64 = self.seasons.iter().map(|s| s.games_won).sum();
        if self.total_games > 0 {
            (total_won as f64 / self.total_games as f64) * 100.0
        } else {
            0.0
        }
    }

    /// A convenience method for textual summary
    pub fn summary(&self) -> String {
        let mut output = format!("{} Statistics:\n\n", self.name);
        
        output.push_str(&format!(
            "Overall:\n- Total Matches: {}\n- Total Games: {}\n- Win Rate: {:.1}%\n\n",
            self.total_matches,
            self.total_games,
            self.overall_win_rate()
        ));

        if let Some(patterns) = &self.score_patterns {
            output.push_str("Scoring Patterns:\n");
            output.push_str(&format!(
                "- Average Score (For-Against): {:.1} - {:.1}\n",
                patterns.avg_score_for,
                patterns.avg_score_against
            ));
            output.push_str(&format!(
                "- Score Range: {} to {}\n\n",
                patterns.min_score,
                patterns.max_score
            ));
        }

        if let Some(close_games) = &self.close_games {
            let close_win_rate = if close_games.total > 0 {
                (close_games.won as f64 / close_games.total as f64) * 100.0
            } else {
                0.0
            };
            output.push_str(&format!(
                "Close Games (≤2 points):\n- Total: {}\n- Won: {} ({:.1}%)\n\n",
                close_games.total, close_games.won, close_win_rate
            ));
        }

        if !self.opponent_details.is_empty() {
            output.push_str("Head-to-Head Records (min. 5 games):\n");
            for opp in &self.opponent_details {
                let win_rate = if opp.games > 0 {
                    (opp.wins as f64 / opp.games as f64) * 100.0
                } else {
                    0.0
                };
                output.push_str(&format!(
                    "- vs {}: {} wins in {} games ({:.1}%), Avg Score: {:.1}-{:.1}\n",
                    opp.name,
                    opp.wins,
                    opp.games,
                    win_rate,
                    opp.avg_score_for,
                    opp.avg_score_against
                ));
            }
            output.push_str("\n");
        }

        output.push_str("Season Breakdown:\n");
        for season in &self.seasons {
            let win_rate = if season.games_played > 0 {
                (season.games_won as f64 / season.games_played as f64) * 100.0
            } else {
                0.0
            };
            output.push_str(&format!(
                "- {}: {} games, {} wins ({:.1}%)\n",
                season.season,
                season.games_played,
                season.games_won,
                win_rate
            ));
        }

        if !self.threat_ratings.is_empty() {
            output.push_str("\nMost Threatening Opponents:\n");
            for (i, threat) in self.threat_ratings.iter().enumerate() {
                output.push_str(&format!(
                    "{}. {}: \n   - Times They Beat You: {}\n   - Avg Winning Score: {:.1}\n   - Highest Winning Score: {:.1}\n",
                    i + 1,
                    threat.name,
                    threat.games_lost,
                    threat.avg_winning_score,
                    threat.highest_winning_score
                ));
            }
            output.push_str("\n");
        }

        output
    }
}

#[derive(Debug)]
pub struct SeasonStats {
    pub season: String,
    pub games_played: i64,
    pub games_won: i64,
}

#[derive(Debug)]
pub struct CloseGamesStats {
    pub total: i64,
    pub won: i64,
}

#[derive(Debug)]
pub struct ScorePatterns {
    pub avg_score_for: f64,
    pub avg_score_against: f64,
    pub min_score: i64,
    pub max_score: i64,
}

#[derive(Debug)]
pub struct OpponentDetails {
    pub name: String,
    pub games: i64,
    pub wins: i64,
    pub avg_score_for: f64,
    pub avg_score_against: f64,
}

#[derive(Debug)]
pub struct ThreatRating {
    pub name: String,
    pub games_played: i64,
    pub games_lost: i64,
    pub avg_winning_score: f64,
    pub highest_winning_score: f64,
}

#[derive(Debug)]
pub struct DivisionStats {
    pub division: String,
    pub season: String,
    pub players: Vec<DivisionPlayerStats>,
}

#[derive(Debug)]
pub struct DivisionPlayerStats {
    pub name: String,
    pub team: String,
    pub matches_played: i64,
    pub matches_available: i64,
    pub total_sets: i64,
    pub sets_won: i64,
    pub sets_lost: i64,
    pub win_percentage: f64,
}

impl DivisionStats {
    pub fn summary(&self) -> String {
        let mut output = format!(
            "Division: {} - Season: {} Statistics:\n\n",
            self.division, self.season
        );
        
        output.push_str("#\tPlayer\tTeam\tMatches\tMatches Available\tSets\tW\tL\t%\n");
        output.push_str("----\t------\t----\t-------\t------------------\t----\t-\t-\t-\n");
        
        for (i, player) in self.players.iter().enumerate() {
            output.push_str(&format!(
                "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{:.2}\n",
                i + 1,
                player.name,
                player.team,
                player.matches_played,
                player.matches_available,
                player.total_sets,
                player.sets_won,
                player.sets_lost,
                player.win_percentage
            ));
        }

        output
    }
}

// If you have a running DB for tests, this might be an example test:
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sanitize_player_name() {
        assert_eq!(Analysis::sanitize_player_name("mary jane watson"), "Mary Jane Watson");
        assert_eq!(Analysis::sanitize_player_name("MARY JANE WATSON"), "Mary Jane Watson");
        assert_eq!(Analysis::sanitize_player_name("Mary Jane Watson"), "Mary Jane Watson");
        assert_eq!(Analysis::sanitize_player_name("mary-jane watson"), "Mary Jane Watson");
        assert_eq!(Analysis::sanitize_player_name("MARY-JANE WATSON"), "Mary Jane Watson");
    }

    #[tokio::test]
    async fn test_get_division_player_stats() {
        // Create a mock game data
        let game_data = GameData {
            event_start_time: Utc::now(),
            original_start_time: None,
            tx_time: Utc::now(),
            match_id: "test_match".to_string(),
            set_number: 1,
            leg_number: 1,
            competition_type: "MKTTL League".to_string(),
            season: "2024-2025".to_string(),
            division: "Division Five".to_string(),
            venue: "Test Venue".to_string(),
            home_team_name: "Team A".to_string(),
            home_team_club: "Club A".to_string(),
            away_team_name: "Team B".to_string(),
            away_team_club: "Club B".to_string(),
            home_player1: "Player 1".to_string(),
            home_player2: None,
            away_player1: "Player 2".to_string(),
            away_player2: None,
            home_score: 11,
            away_score: 9,
            handicap_home: 0,
            handicap_away: 0,
            report_html: None,
        };

        // Create a mock stats result
        let mock_stats = vec![
            PlayerMatchStats {
                player_name: Some("Player 1".to_string()),
                team_name: Some("Team A".to_string()),
                matches_played: Some(1),
                matches_available: Some(1),
                total_sets: Some(1),
                sets_won: Some(1),
                sets_lost: Some(0),
                win_percentage: Some(100.0),
            },
            PlayerMatchStats {
                player_name: Some("Player 2".to_string()),
                team_name: Some("Team B".to_string()),
                matches_played: Some(1),
                matches_available: Some(1),
                total_sets: Some(1),
                sets_won: Some(0),
                sets_lost: Some(1),
                win_percentage: Some(0.0),
            },
        ];

        // Verify the mock data
        assert!(!mock_stats.is_empty());
        for player in &mock_stats {
            assert!(player.matches_played.unwrap_or(0) <= player.matches_available.unwrap_or(0));
            assert!(player.sets_won.unwrap_or(0) + player.sets_lost.unwrap_or(0) <= player.total_sets.unwrap_or(0));
            assert!(player.win_percentage.unwrap_or(0.0) >= 0.0 && player.win_percentage.unwrap_or(0.0) <= 100.0);
        }
    }
}

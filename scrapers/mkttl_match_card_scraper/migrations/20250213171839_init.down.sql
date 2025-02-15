-- Drop all indexes first
DROP INDEX IF EXISTS idx_csv_import_log_file_path;
DROP INDEX IF EXISTS idx_doubles_set_away_player2;
DROP INDEX IF EXISTS idx_doubles_set_away_player1;
DROP INDEX IF EXISTS idx_doubles_set_home_player2;
DROP INDEX IF EXISTS idx_doubles_set_home_player1;
DROP INDEX IF EXISTS idx_doubles_set_match;
DROP INDEX IF EXISTS idx_singles_set_away_player;
DROP INDEX IF EXISTS idx_singles_set_home_player;
DROP INDEX IF EXISTS idx_singles_set_match;
DROP INDEX IF EXISTS idx_team_season;
DROP INDEX IF EXISTS idx_matches_date;
DROP INDEX IF EXISTS idx_matches_season;

-- Drop tables in reverse order of creation (respecting foreign key constraints)
DROP TABLE IF EXISTS csv_import_log;
DROP TABLE IF EXISTS doubles_set CASCADE;
DROP TABLE IF EXISTS singles_set CASCADE;
DROP TABLE IF EXISTS player_league CASCADE;
DROP TABLE IF EXISTS matches CASCADE;
DROP TABLE IF EXISTS team CASCADE;
DROP TABLE IF EXISTS club CASCADE;
DROP TABLE IF EXISTS venue CASCADE;
DROP TABLE IF EXISTS player CASCADE;
DROP TABLE IF EXISTS season;
DROP TABLE IF EXISTS division;
DROP TABLE IF EXISTS league;

-- Drop enum type with CASCADE to ensure all dependencies are dropped
DROP TYPE IF EXISTS competition_type_enum CASCADE;
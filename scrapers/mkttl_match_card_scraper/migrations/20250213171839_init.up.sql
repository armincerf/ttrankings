-- =============================================================
-- Create Enum Types
-- =============================================================
CREATE TYPE competition_type_enum AS ENUM ('MKTTL League', 'MKTTL Challenge Cup');

-- =============================================================
-- Create Core Tables
-- =============================================================
-- 1. League Table
CREATE TABLE league (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL UNIQUE,
    description TEXT
);

-- 2. Division Table
CREATE TABLE division (
    id BIGSERIAL PRIMARY KEY,
    league_id BIGINT NOT NULL REFERENCES league(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    UNIQUE (league_id, name)
);

-- 3. Season Table
CREATE TABLE season (
    id BIGSERIAL PRIMARY KEY,
    league_id BIGINT NOT NULL REFERENCES league(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    -- e.g., '2015-2016'
    UNIQUE (league_id, name)
);

-- 4. Player Table
CREATE TABLE player (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    UNIQUE(name)
);

-- 5. Venue Table
CREATE TABLE venue (
    id BIGSERIAL PRIMARY KEY,
    name TEXT NOT NULL,
    address TEXT NOT NULL,
    UNIQUE(name, address)
);

-- 6. Club Table
CREATE TABLE club (
    id BIGSERIAL PRIMARY KEY,
    league_id BIGINT NOT NULL REFERENCES league(id) ON DELETE CASCADE,
    name TEXT NOT NULL UNIQUE,
    venue_id BIGINT NOT NULL REFERENCES venue(id) ON DELETE CASCADE,
    secretary_id BIGINT REFERENCES player(id) ON DELETE
    SET
        NULL
);

-- 7. Team Table
CREATE TABLE team (
    id BIGSERIAL PRIMARY KEY,
    league_id BIGINT NOT NULL REFERENCES league(id) ON DELETE CASCADE,
    season_id BIGINT NOT NULL REFERENCES season(id) ON DELETE CASCADE,
    division_id BIGINT NOT NULL REFERENCES division(id) ON DELETE CASCADE,
    club_id BIGINT NOT NULL REFERENCES club(id) ON DELETE CASCADE,
    name TEXT NOT NULL,
    home_night TEXT,
    captain_id BIGINT REFERENCES player(id) ON DELETE
    SET
        NULL,
        UNIQUE (league_id, season_id, club_id, name)
);

-- 8. Matches Table
CREATE TABLE matches (
    id BIGSERIAL PRIMARY KEY,
    league_id BIGINT NOT NULL REFERENCES league(id) ON DELETE CASCADE,
    season_id BIGINT NOT NULL REFERENCES season(id) ON DELETE CASCADE,
    division_id BIGINT REFERENCES division(id) ON DELETE CASCADE,
    match_date TIMESTAMPTZ NOT NULL,
    original_date TIMESTAMPTZ,
    home_score INTEGER,
    away_score INTEGER,
    -- if rescheduled
    home_team_id BIGINT NOT NULL REFERENCES team(id) ON DELETE
    SET
        NULL,
        away_team_id BIGINT NOT NULL REFERENCES team(id) ON DELETE
    SET
        NULL,
        venue_id BIGINT NOT NULL REFERENCES venue(id) ON DELETE
    SET
        NULL,
        competition_type competition_type_enum NOT NULL,
        csv_reference TEXT UNIQUE,
        CHECK (home_team_id <> away_team_id)
);

-- 9. Player-League Membership Table
CREATE TABLE player_league (
    player_id BIGINT NOT NULL REFERENCES player(id) ON DELETE CASCADE,
    league_id BIGINT NOT NULL REFERENCES league(id) ON DELETE CASCADE,
    PRIMARY KEY (player_id, league_id)
);

-- 10. Singles Sets Table
CREATE TABLE singles_set (
    id BIGSERIAL PRIMARY KEY,
    set_date DATE NOT NULL,
    match_id BIGINT NOT NULL REFERENCES matches(id) ON DELETE CASCADE,
    home_player BIGINT NOT NULL REFERENCES player(id) ON DELETE SET NULL,
    away_player BIGINT NOT NULL REFERENCES player(id) ON DELETE SET NULL,
    scores JSONB NOT NULL,
    winner BIGINT NOT NULL REFERENCES player(id) ON DELETE SET NULL,
    CHECK (winner = home_player OR winner = away_player),
    UNIQUE (match_id, home_player, away_player, set_date)
);

-- 11. Doubles Sets Table
CREATE TABLE doubles_set (
    id BIGSERIAL PRIMARY KEY,
    set_date DATE NOT NULL,
    match_id BIGINT NOT NULL REFERENCES matches(id) ON DELETE CASCADE,
    home_player1 BIGINT NOT NULL REFERENCES player(id) ON DELETE SET NULL,
    home_player2 BIGINT NOT NULL REFERENCES player(id) ON DELETE SET NULL,
    away_player1 BIGINT NOT NULL REFERENCES player(id) ON DELETE SET NULL,
    away_player2 BIGINT NOT NULL REFERENCES player(id) ON DELETE SET NULL,
    scores JSONB NOT NULL,
    home_won BOOLEAN NOT NULL,
    UNIQUE (match_id, home_player1, home_player2, away_player1, away_player2, set_date)
);

-- =============================================================
-- Indexes for Performance
-- =============================================================
-- Indexes on Matches Table
CREATE INDEX idx_matches_season ON matches(season_id);

CREATE INDEX idx_matches_date ON matches(match_date);

-- Index on Team Table
CREATE INDEX idx_team_season ON team(season_id);

-- Indexes on Singles Sets Table
CREATE INDEX idx_singles_set_match ON singles_set(match_id);

CREATE INDEX idx_singles_set_home_player ON singles_set(home_player);

CREATE INDEX idx_singles_set_away_player ON singles_set(away_player);

-- Indexes on Doubles Sets Table
CREATE INDEX idx_doubles_set_match ON doubles_set(match_id);

CREATE INDEX idx_doubles_set_home_player1 ON doubles_set(home_player1);

CREATE INDEX idx_doubles_set_home_player2 ON doubles_set(home_player2);

CREATE INDEX idx_doubles_set_away_player1 ON doubles_set(away_player1);

CREATE INDEX idx_doubles_set_away_player2 ON doubles_set(away_player2);

-- =============================================================
-- Add Set Constraints
-- =============================================================
DELETE FROM singles_set a USING singles_set b
WHERE a.id > b.id 
  AND a.match_id = b.match_id 
  AND a.home_player = b.home_player 
  AND a.away_player = b.away_player 
  AND a.set_date = b.set_date;

DELETE FROM doubles_set a USING doubles_set b
WHERE a.id > b.id 
  AND a.match_id = b.match_id 
  AND a.home_player1 = b.home_player1 
  AND a.home_player2 = b.home_player2 
  AND a.away_player1 = b.away_player1 
  AND a.away_player2 = b.away_player2 
  AND a.set_date = b.set_date;

ALTER TABLE singles_set ADD CONSTRAINT singles_set_unique_match_players_date 
    UNIQUE (match_id, home_player, away_player, set_date);

ALTER TABLE doubles_set ADD CONSTRAINT doubles_set_unique_match_players_date 
    UNIQUE (match_id, home_player1, home_player2, away_player1, away_player2, set_date);

-- =============================================================
-- Add created_at, updated_at columns
-- =============================================================
ALTER TABLE league       ADD COLUMN created_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
ALTER TABLE league       ADD COLUMN updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

ALTER TABLE division     ADD COLUMN created_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
ALTER TABLE division     ADD COLUMN updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

ALTER TABLE season       ADD COLUMN created_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
ALTER TABLE season       ADD COLUMN updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

ALTER TABLE player       ADD COLUMN created_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
ALTER TABLE player       ADD COLUMN updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

ALTER TABLE venue        ADD COLUMN created_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
ALTER TABLE venue        ADD COLUMN updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

ALTER TABLE club         ADD COLUMN created_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
ALTER TABLE club         ADD COLUMN updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

ALTER TABLE team         ADD COLUMN created_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
ALTER TABLE team         ADD COLUMN updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

ALTER TABLE matches      ADD COLUMN created_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
ALTER TABLE matches      ADD COLUMN updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

ALTER TABLE singles_set  ADD COLUMN created_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
ALTER TABLE singles_set  ADD COLUMN updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

ALTER TABLE doubles_set  ADD COLUMN created_at TIMESTAMPTZ NOT NULL DEFAULT NOW();
ALTER TABLE doubles_set  ADD COLUMN updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW();

-- =============================================================
-- Create csv_import_log
-- =============================================================
CREATE TABLE csv_import_log (
    id BIGSERIAL PRIMARY KEY,
    file_path TEXT NOT NULL UNIQUE,
    last_modified_time TIMESTAMPTZ NOT NULL,
    last_imported_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX idx_csv_import_log_file_path ON csv_import_log(file_path);

-- =============================================================
-- Add file_hash column
-- =============================================================
ALTER TABLE csv_import_log
    ADD COLUMN file_hash TEXT;
-- Add unique constraint for matches
ALTER TABLE matches ADD CONSTRAINT matches_unique_fixture
    UNIQUE (league_id, season_id, home_team_id, away_team_id, match_date);

-- Add unique constraint for teams
ALTER TABLE team ADD CONSTRAINT team_unique_name
    UNIQUE (league_id, season_id, club_id, name);

-- Add unique constraint for clubs
ALTER TABLE club ADD CONSTRAINT club_unique_name
    UNIQUE (league_id, name);

-- Add unique constraint for venues
ALTER TABLE venue ADD CONSTRAINT venue_unique_name_address
    UNIQUE (name, address);

-- Add unique constraint for divisions
ALTER TABLE division ADD CONSTRAINT division_unique_name
    UNIQUE (league_id, name);

-- Add unique constraint for seasons
ALTER TABLE season ADD CONSTRAINT season_unique_name
    UNIQUE (league_id, name);

-- Add unique constraint for league
ALTER TABLE league ADD CONSTRAINT league_unique_name
    UNIQUE (name); 
-- Remove unique constraints
ALTER TABLE matches DROP CONSTRAINT matches_unique_fixture;
ALTER TABLE team DROP CONSTRAINT team_unique_name;
ALTER TABLE club DROP CONSTRAINT club_unique_name;
ALTER TABLE venue DROP CONSTRAINT venue_unique_name_address;
ALTER TABLE division DROP CONSTRAINT division_unique_name;
ALTER TABLE season DROP CONSTRAINT season_unique_name;
ALTER TABLE league DROP CONSTRAINT league_unique_name; 
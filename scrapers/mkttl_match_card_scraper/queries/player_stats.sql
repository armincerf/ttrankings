WITH player_singles AS (
    -- Get singles matches stats
    SELECT 
        p.id as player_id,
        p.name as player_name,
        COUNT(*) as singles_played,
        COUNT(CASE WHEN ss.winner = p.id THEN 1 END) as singles_won,
        COUNT(CASE WHEN ss.winner != p.id THEN 1 END) as singles_lost
    FROM player p
    JOIN singles_set ss ON (ss.home_player = p.id OR ss.away_player = p.id)
    GROUP BY p.id, p.name
),
player_doubles AS (
    -- Get doubles matches stats
    SELECT 
        p.id as player_id,
        p.name as player_name,
        COUNT(*) as doubles_played,
        COUNT(CASE WHEN 
            ((p.id = ds.home_player1 OR p.id = ds.home_player2) AND ds.home_won) OR
            ((p.id = ds.away_player1 OR p.id = ds.away_player2) AND NOT ds.home_won)
        THEN 1 END) as doubles_won,
        COUNT(CASE WHEN 
            ((p.id = ds.home_player1 OR p.id = ds.home_player2) AND NOT ds.home_won) OR
            ((p.id = ds.away_player1 OR p.id = ds.away_player2) AND ds.home_won)
        THEN 1 END) as doubles_lost
    FROM player p
    JOIN doubles_set ds ON (
        ds.home_player1 = p.id OR 
        ds.home_player2 = p.id OR 
        ds.away_player1 = p.id OR 
        ds.away_player2 = p.id
    )
    GROUP BY p.id, p.name
),
player_divisions AS (
    -- Get all divisions a player has played in
    SELECT DISTINCT
        p.id as player_id,
        string_agg(DISTINCT d.name, ', ' ORDER BY d.name) as divisions
    FROM player p
    LEFT JOIN singles_set ss ON (ss.home_player = p.id OR ss.away_player = p.id)
    LEFT JOIN doubles_set ds ON (
        ds.home_player1 = p.id OR 
        ds.home_player2 = p.id OR 
        ds.away_player1 = p.id OR 
        ds.away_player2 = p.id
    )
    JOIN matches m ON (m.id = ss.match_id OR m.id = ds.match_id)
    JOIN division d ON d.id = m.division_id
    GROUP BY p.id
),
player_seasons AS (
    -- Get all seasons a player has played in
    SELECT DISTINCT
        p.id as player_id,
        string_agg(DISTINCT s.name, ', ' ORDER BY s.name) as seasons
    FROM player p
    LEFT JOIN singles_set ss ON (ss.home_player = p.id OR ss.away_player = p.id)
    LEFT JOIN doubles_set ds ON (
        ds.home_player1 = p.id OR 
        ds.home_player2 = p.id OR 
        ds.away_player1 = p.id OR 
        ds.away_player2 = p.id
    )
    JOIN matches m ON (m.id = ss.match_id OR m.id = ds.match_id)
    JOIN season s ON s.id = m.season_id
    GROUP BY p.id
),
player_teams AS (
    -- Get all teams a player has played for
    SELECT DISTINCT
        p.id as player_id,
        string_agg(DISTINCT t.name, ', ' ORDER BY t.name) as teams
    FROM player p
    LEFT JOIN singles_set ss ON (ss.home_player = p.id OR ss.away_player = p.id)
    LEFT JOIN doubles_set ds ON (
        ds.home_player1 = p.id OR 
        ds.home_player2 = p.id OR 
        ds.away_player1 = p.id OR 
        ds.away_player2 = p.id
    )
    JOIN matches m ON (m.id = ss.match_id OR m.id = ds.match_id)
    JOIN team t ON (t.id = m.home_team_id OR t.id = m.away_team_id)
    GROUP BY p.id
)

SELECT 
    COALESCE(ps.player_name, pd.player_name) as player_name,
    COALESCE(ps.singles_played, 0) + COALESCE(pd.doubles_played, 0) as matches_played,
    COALESCE(ps.singles_won, 0) + COALESCE(pd.doubles_won, 0) as matches_won,
    COALESCE(ps.singles_lost, 0) + COALESCE(pd.doubles_lost, 0) as matches_lost,
    COALESCE(pd.doubles_played, 0) as doubles_played,
    COALESCE(pd.doubles_won, 0) as doubles_won,
    COALESCE(pd.doubles_lost, 0) as doubles_lost,
    COALESCE(ps.singles_played, 0) as singles_played,
    COALESCE(ps.singles_won, 0) as singles_won,
    COALESCE(ps.singles_lost, 0) as singles_lost,
    COALESCE(pdiv.divisions, '') as divisions,
    COALESCE(psea.seasons, '') as seasons,
    COALESCE(pt.teams, '') as teams
FROM player_singles ps
FULL OUTER JOIN player_doubles pd ON ps.player_id = pd.player_id
LEFT JOIN player_divisions pdiv ON COALESCE(ps.player_id, pd.player_id) = pdiv.player_id
LEFT JOIN player_seasons psea ON COALESCE(ps.player_id, pd.player_id) = psea.player_id
LEFT JOIN player_teams pt ON COALESCE(ps.player_id, pd.player_id) = pt.player_id
WHERE COALESCE(ps.singles_played, 0) + COALESCE(pd.doubles_played, 0) > 0
ORDER BY matches_played DESC; 
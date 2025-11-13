CREATE TABLE player_tracking(
	player_name TEXT,
	first_active_season INTEGER,
	last_active_season INTEGER,
	season_active_state TEXT,
	season_active INTEGER[],
	current_season INTEGER,
	PRIMARY KEY(player_name, current_season)
);

INSERT INTO player_tracking
WITH yesterday AS(
	SELECT *
	FROM player_tracking
	WHERE current_season = 1996
),
today AS(
	SELECT 
		player_name,
		season
	FROM player_seasons
	WHERE season = 1997 AND player_name IS NOT NULL
	GROUP BY player_name, season
)

SELECT 
	COALESCE(y.player_name, t.player_name) AS player_name,
	COALESCE(y.first_active_season, t.season) AS first_active_season,
	COALESCE(t.season, y.last_active_season) AS last_active_season,
	CASE 
		WHEN y.player_name IS NULL AND t.player_name IS NOT NULL THEN 'New'
		WHEN t.season IS NULL AND y.last_active_season = y.current_season THEN 'Retired'
		WHEN t.season IS NULL AND y.last_active_season < y.current_season THEN 'Stayed Retired'
		WHEN y.last_active_season = t.season - 1 THEN 'Continued Playing'
		WHEN t.season IS NOT NULL AND y.last_active_season <> y.current_season THEN 'Returned from Retirement'
	END AS season_active_state,
	COALESCE(y.season_active,
		ARRAY[]::INTEGER[])
		|| CASE WHEN 
			t.player_name IS NOT NULL
				THEN ARRAY[t.season]
				ELSE ARRAY[]::INTEGER[]
				END AS season_active,
	COALESCE(t.season, y.current_season + 1) AS current_season
FROM today t
FULL OUTER JOIN yesterday y ON t.player_name = y.player_name;
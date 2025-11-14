WITH calendar_with_status AS (
    SELECT
        all_players.player_name,
        all_seasons.season,
        CASE
            WHEN ps.player_name IS NOT NULL THEN 1
            ELSE 0
        END AS is_active
    FROM (
        SELECT DISTINCT player_name FROM player_seasons WHERE player_name IS NOT NULL
    ) AS all_players
    CROSS JOIN (
        SELECT DISTINCT season FROM player_seasons
    ) AS all_seasons
	LEFT JOIN player_seasons ps
        ON all_players.player_name = ps.player_name
        AND all_seasons.season = ps.season
),
player_transitions AS (
	SELECT 
		player_name,
		season,
		is_active,
		LAG(is_active) OVER (PARTITION BY player_name ORDER BY season) AS active_prev,
		COALESCE(
            MAX(is_active) OVER (
                PARTITION BY player_name
                ORDER BY season
                ROWS BETWEEN UNBOUNDED PRECEDING AND 1 PRECEDING
            ),
            0
        ) AS ever_active_before,
        MIN(CASE WHEN is_active = 1 THEN season ELSE NULL END) OVER (
            PARTITION BY player_name
        ) AS first_active_season,
		MAX(CASE WHEN is_active = 1 THEN season ELSE NULL END) OVER (
            PARTITION BY player_name ORDER BY season
        ) AS last_active_season_so_far
	FROM calendar_with_status
)

SELECT 
	player_name,
	season,
	first_active_season,
	last_active_season_so_far,
	CASE
		WHEN is_active=1 AND (active_prev IS NULL OR ever_active_before=0) THEN 'New'
		WHEN is_active=1 AND active_prev=1 THEN 'Continued Playing'
		WHEN is_active=0 AND active_prev=1 THEN 'Retired'
		WHEN is_active=1 AND active_prev=0 AND ever_active_before=1 THEN 'Returned from Retirement'
		WHEN is_active=0 AND active_prev=0 AND ever_active_before=1 THEN 'Stayed Retired'
	END AS season_active_state
FROM player_transitions
ORDER BY player_name, season;

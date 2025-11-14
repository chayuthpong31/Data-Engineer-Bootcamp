WITH lebron_games AS (
	SELECT 
		gd.game_id,
		g.game_date_est AS game_date,
		CASE WHEN COALESCE(gd.pts, 0) > 10 THEN 1 ELSE 0 END AS is_over_10_pts
	FROM game_details gd 
	JOIN games g ON gd.game_id = g.game_id
	WHERE player_name = 'LeBron James'
),
find_starts AS(
	SELECT 
		game_id,
		is_over_10_pts,
		CASE 
			WHEN is_over_10_pts = 1 AND LAG(is_over_10_pts, 1, 0) OVER (ORDER BY game_date, game_id) = 0 THEN 1 
			ELSE 0
		END AS is_streak_start
	FROM lebron_games
),
group_streak AS(
	SELECT 
		game_id,
		is_over_10_pts,
		SUM(is_streak_start) OVER (ORDER BY game_date,game_id) AS streak_id
	FROM find_starts
)

SELECT
    COUNT(*) AS longest_streak_length
FROM group_streak
WHERE is_over_10_pts = 1 
GROUP BY streak_id
ORDER BY longest_streak_length DESC
LIMIT 1; 

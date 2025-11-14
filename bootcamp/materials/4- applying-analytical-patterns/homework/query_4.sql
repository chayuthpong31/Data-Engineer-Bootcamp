WITH pts_by_player_season AS(
	SELECT 
		gd.player_name,
		g.season,
		SUM(COALESCE(pts, 0)) AS total_pts
	FROM game_details gd 
	INNER JOIN games g ON gd.game_id = g.game_id
	GROUP BY gd.player_name, g.season
)
SELECT 
	player_name,
	season,
	total_pts
FROM pts_by_player_season
ORDER BY total_pts DESC
LIMIT 1;
WITH win_by_team AS(
	SELECT 
		game_id,
		home_team_id AS team_id,
		CASE 
			WHEN pts_home > pts_away THEN 1 ELSE 0 END AS is_win
	FROM games 
	UNION ALL 
	SELECT 
		game_id,
		visitor_team_id AS team_id,
		CASE 
			WHEN pts_home < pts_away THEN 1 ELSE 0 END AS is_win
	FROM games 
)
SELECT 
	team_id,
	SUM(is_win) AS total_wins
FROM win_by_team
GROUP BY team_id
ORDER BY total_wins DESC
LIMIT 1;
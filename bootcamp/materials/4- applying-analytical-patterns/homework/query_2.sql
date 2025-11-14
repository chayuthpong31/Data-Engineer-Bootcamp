SELECT 
	gd.player_name,
	gd.team_id,
	g.season,
	SUM(COALESCE(gd.pts, 0)) AS total_points,
	COUNT(DISTINCT CASE 
		WHEN (gd.team_id = g.home_team_id AND g.pts_home > g.pts_away) 
		OR (gd.team_id = g.visitor_team_id AND g.pts_home < g.pts_away) THEN gd.game_id
	END) AS total_wins
FROM game_details gd INNER JOIN games g ON gd.game_id = g.game_id
GROUP BY
    GROUPING SETS (
        (player_name, team_id),  
        (player_name, season), 
        (team_id)              
);

WITH pts_by_player_team AS (
    SELECT gd.player_name, gd.team_id, SUM(COALESCE(gd.pts,0)) AS total_points
    FROM game_details gd
    GROUP BY gd.player_name, gd.team_id
)
SELECT player_name, team_id, total_points
FROM pts_by_player_team
ORDER BY total_points DESC
LIMIT 1;
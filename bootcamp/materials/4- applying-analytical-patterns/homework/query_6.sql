WITH game_wins AS(
	SELECT 
		game_id,
		game_date_est AS game_date,
		home_team_id AS team_id,
		CASE 
			WHEN pts_home > pts_away THEN 1
			ELSE 0
		END is_win
	FROM games 
	UNION ALL
	SELECT 
		game_id,
		game_date_est AS game_date,
		visitor_team_id AS team_id,
		CASE 
			WHEN pts_home < pts_away THEN 1
			ELSE 0
		END is_win
	FROM games 
),
numbered_games AS (
	SELECT
		gw.team_id,
		gw.game_id,
		is_win,
		ROW_NUMBER() OVER (PARTITION BY team_id ORDER BY game_date,gw.game_id) AS row_num
	FROM game_wins gw
	JOIN games g ON gw.game_id = g.game_id
),
rolling AS (
	SELECT 
	    team_id,
		row_num,
		SUM(is_win) OVER (PARTITION BY team_id ORDER BY row_num ROWS BETWEEN 89 PRECEDING AND CURRENT ROW) AS wins_in_last_90
	FROM numbered_games
)

SELECT 
	team_id,
	MAX(wins_in_last_90) AS max_wins_in_any_90_game_stretch
FROM rolling
GROUP BY team_id
ORDER BY max_wins_in_any_90_game_stretch DESC LIMIT 1;

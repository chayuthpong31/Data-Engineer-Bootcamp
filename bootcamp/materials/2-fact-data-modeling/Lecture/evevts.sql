SELECT 
	*
FROM events;

INSERT INTO users_cumulated
WITH yesterday AS(
	SELECT
		*
	FROM users_cumulated
	WHERE date = '2023-01-10'
),
today AS(
	SELECT
		CAST(user_id AS TEXT),
		DATE(event_time) AS date_active
	FROM events
	WHERE DATE(event_time) = '2023-01-11' AND user_id IS NOT NULL
	GROUP BY user_id, DATE(event_time)
)

SELECT 
	COALESCE(t.user_id, y.user_id) AS user_id,
	CASE WHEN y.dates_active IS NULL
		THEN ARRAY[t.date_active]
		WHEN t.date_active IS NULL THEN y.dates_active
		ELSE y.dates_active || ARRAY[t.date_active]
	END AS dates_active,
	COALESCE(t.date_active, y.date + INTERVAL '1 day') AS date
	
FROM today t
FULL OUTER JOIN yesterday y ON t.user_id = y.user_id


DROP TABLE users_cumulated;
CREATE TABLE users_cumulated(
	user_id TEXT,
	-- The list of dates in the past where the user was active
	dates_active DATE[],
	-- The current date for the user
	date DATE,
	PRIMARY KEY (user_id, date)
)
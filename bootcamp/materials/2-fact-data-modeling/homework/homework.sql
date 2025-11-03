-- 1. A query to deduplicate game_details from Day 1 so there's no duplicates
SELECT DISTINCT * FROM game_details;

-- 2. A DDL for an user_devices_cumulated table
CREATE TABLE user_devices_cumulated (
	device_id TEXT,
	device_activity_datelist DATE[],
	date DATE,
	PRIMARY KEY(device_id, date)
);

-- 3. A cumulative query to generate device_activity_datelist from events
INSERT INTO user_devices_cumulated
WITH yesterday AS(
 	SELECT * 
 	FROM user_devices_cumulated
 	WHERE date = '2023-01-30'
),
today AS(
	SELECT 
		CAST(device_id AS TEXT),
		DATE(event_time) AS device_activity_datelist
	FROM events
	WHERE DATE(event_time) = '2023-01-31' AND device_id IS NOT NULL
	GROUP BY device_id, DATE(event_time)
)
SELECT 
	COALESCE(y.device_id, t.device_id) AS device_id,
	CASE WHEN y.device_activity_datelist IS NULL
		THEN ARRAY[t.device_activity_datelist]
		WHEN t.device_activity_datelist IS NULL THEN y.device_activity_datelist
		ELSE y.device_activity_datelist || t.device_activity_datelist
	END AS device_activity_datelist,
	COALESCE(t.device_activity_datelist, y.date + INTERVAL '1 day')
FROM today t FULL OUTER JOIN yesterday y ON y.device_id = t.device_id;

-- 4. A datelist_int generation query. Convert the device_activity_datelist column into a datelist_int column
WITH devices AS(
	SELECT *
	FROM user_devices_cumulated
	WHERE date = '2023-01-01'
),
series AS(
	SELECT *
	FROM generate_series(DATE('2023-01-01'), DATE('2023-01-31'), INTERVAL '1 day') as series_date
),
place_holder_ints AS(
	SELECT 
		CASE WHEN device_activity_datelist @> ARRAY[DATE(series_date)]
			THEN CAST(POW(2, 32 - (date - DATE(series_date))) AS BIGINT)
		ELSE 0
		END AS placeholder_int_value,
		*
	FROM devices CROSS JOIN series
)
SELECT 
	device_id,
    BIT_COUNT(CAST(CAST(SUM(placeholder_int_value) AS BIGINT) AS BIT(32))) AS datelist_int
FROM place_holder_ints
GROUP BY device_id;

-- 5. A DDL for hosts_cumulated table
TRUNCATE TABLE hosts_cumulated
CREATE TABLE hosts_cumulated(
	host TEXT,
	host_activity_datelist DATE[],
	date DATE,
	PRIMARY KEY(host, date)
);

-- 6. The incremental query to generate host_activity_datelist
INSERT INTO hosts_cumulated
WITH yesterday AS(
	SELECT *
	FROM hosts_cumulated
	WHERE date = '2023-01-30'
),
today AS(
	SELECT 
		host,
		DATE(event_time) AS host_activity_datelist
	FROM events
	WHERE DATE(event_time) = '2023-01-31' AND host IS NOT NULL
	GROUP BY host, DATE(event_time)
)
SELECT
	COALESCE(y.host, t.host) AS host,
	CASE WHEN y.host_activity_datelist IS NULL THEN ARRAY[t.host_activity_datelist]
		WHEN t.host_activity_datelist IS NULL THEN y.host_activity_datelist
		ELSE y.host_activity_datelist || t.host_activity_datelist
		END AS host_activity_datelist,
	COALESCE(t.host_activity_datelist, y.date + INTERVAL '1 day') 
FROM today t FULL OUTER JOIN yesterday y ON t.host = y.host;

-- 7. A monthly, reduced fact table DDL host_activity_reduced
CREATE TABLE host_activity_reduced(
	host TEXT,
	month DATE,
	hit_array REAL[],
	unique_visitors_array REAL[],
	PRIMARY KEY (host, month)
)

-- 8. An incremental query that loads host_activity_reduced
INSERT INTO host_activity_reduced
WITH daily_aggregate AS(
	SELECT 
		host,
		DATE(event_time) AS date,
		COUNT(1) AS num_site_hits,
		COUNT(DISTINCT user_id) AS unique_visitors
	FROM events
	WHERE DATE(event_time) = '2023-01-03' AND host IS NOT NULL
	GROUP BY host, DATE(event_time)
),yesterday_array AS(
	SELECT * 
	FROM host_activity_reduced
	WHERE month = '2023-01-01'
)
SELECT 
	COALESCE(da.host, ya.host) AS host,
	COALESCE(ya.month, DATE_TRUNC('month', da.date)) AS month,
	CASE WHEN ya.hit_array IS NOT NULL THEN 
			ya.hit_array || ARRAY[COALESCE(da.num_site_hits, 0)]
		WHEN ya.hit_array IS NULL THEN
			ARRAY_FILL(0, ARRAY[COALESCE(date - DATE(DATE_TRUNC('month', da.date)), 0)]) || ARRAY[COALESCE(da.num_site_hits, 0)]
		END AS hit_array,
	CASE WHEN ya.unique_visitors_array IS NOT NULL THEN
			ya.unique_visitors_array || ARRAY[COALESCE(da.unique_visitors, 0)]
		WHEN ya.unique_visitors_array IS NULL THEN
			ARRAY_FILL(0, ARRAY[COALESCE(date - DATE(DATE_TRUNC('month', da.date)), 0)]) || ARRAY[COALESCE(da.unique_visitors, 0)]
		END AS unique_visitors_array
FROM daily_aggregate da 
FULL OUTER JOIN yesterday_array ya ON da.host = ya.host
ON CONFLICT (host, month)
DO
UPDATE SET hit_array = EXCLUDED.hit_array, 
	unique_visitors_array = EXCLUDED.unique_visitors_array;

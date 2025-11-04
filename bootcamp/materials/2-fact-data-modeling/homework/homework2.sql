-- Keep 1 row per game_id, team_id, player_id with latest updated_at
WITH ranked AS (
  SELECT
    d.*,
    ROW_NUMBER() OVER (
      PARTITION BY game_id, team_id, player_id
      ORDER BY updated_at DESC NULLS LAST
    ) AS rn
  FROM nba_game_details d
)
SELECT * 
FROM ranked
WHERE rn = 1;

CREATE TABLE user_devices_cumulated (
  user_id TEXT NOT NULL,
  device_id TEXT NOT NULL,
  dates_active DATE[] NOT NULL DEFAULT '{}',
  as_of_date DATE NOT NULL,
  PRIMARY KEY (user_id, device_id, as_of_date)
);

-- :run_date = '2023-01-31'
WITH params AS (
  SELECT DATE '2023-01-31' AS run_date, 
         (DATE '2023-01-31' - INTERVAL '1 day')::DATE AS prev_date
),
yesterday AS (
  SELECT user_id, device_id, dates_active, as_of_date
  FROM user_devices_cumulated y
  JOIN params p ON y.as_of_date = p.prev_date
),
today AS (
  SELECT
    we.user_id,
    we.device_id,
    DATE(we.event_time) AS active_date
  FROM web_events we
  JOIN params p ON DATE(we.event_time) = p.run_date
  WHERE we.device_id IS NOT NULL
  GROUP BY we.user_id, we.device_id, DATE(we.event_time)
),
merged AS (
  SELECT
    COALESCE(y.user_id, t.user_id) AS user_id,
    COALESCE(y.device_id, t.device_id) AS device_id,
    CASE
      WHEN y.user_id IS NULL THEN ARRAY[t.active_date]
      WHEN t.user_id IS NULL THEN y.dates_active
      ELSE (
        SELECT ARRAY(
          SELECT DISTINCT d
          FROM unnest(y.dates_active || ARRAY[t.active_date]) AS d
          ORDER BY d ASC
        )
      )
    END AS dates_active,
    (SELECT run_date FROM params) AS as_of_date
  FROM today t
  FULL OUTER JOIN yesterday y ON y.user_id = t.user_id AND y.device_id = t.device_id
)
INSERT INTO user_devices_cumulated (user_id, device_id, dates_active, as_of_date)
SELECT user_id, device_id, dates_active, as_of_date
FROM merged
ON CONFLICT (user_id, device_id, as_of_date)
DO UPDATE SET dates_active = EXCLUDED.dates_active;

-- Generate 31-bit mask of activity within last 31 days
WITH d AS (
  SELECT user_id, device_id, as_of_date, dates_active
  FROM user_devices_cumulated
  WHERE as_of_date = DATE '2023-01-31'
),
expanded AS (
  SELECT
    user_id,
    device_id,
    (as_of_date - active_date) AS day_offset
  FROM d, unnest(dates_active) AS active_date
  WHERE active_date BETWEEN (as_of_date - INTERVAL '31 days')::DATE AND as_of_date
),
bits AS (
  SELECT user_id, device_id, (1::BIGINT << day_offset) AS bit_val
  FROM expanded
  WHERE day_offset BETWEEN 0 AND 31
)
SELECT user_id, device_id, SUM(bit_val)::BIGINT AS datelist_int
FROM bits
GROUP BY user_id, device_id;

CREATE TABLE hosts_cumulated (
  host TEXT NOT NULL,
  dates_active DATE[] NOT NULL DEFAULT '{}',
  as_of_date DATE NOT NULL,
  PRIMARY KEY (host, as_of_date)
);

-- :run_date = '2023-01-31'
WITH params AS (
  SELECT DATE '2023-01-31' AS run_date,
         (DATE '2023-01-31' - INTERVAL '1 day')::DATE AS prev_date
),
yesterday AS (
  SELECT host, dates_active, as_of_date
  FROM hosts_cumulated y
  JOIN params p ON y.as_of_date = p.prev_date
),
today AS (
  SELECT host, DATE(event_time) AS active_date
  FROM web_events we
  JOIN params p ON DATE(we.event_time) = p.run_date
  WHERE host IS NOT NULL
  GROUP BY host, DATE(we.event_time)
),
merged AS (
  SELECT
    COALESCE(y.host, t.host) AS host,
    CASE
      WHEN y.host IS NULL THEN ARRAY[t.active_date]
      WHEN t.host IS NULL THEN y.dates_active
      ELSE (
        SELECT ARRAY(
          SELECT DISTINCT d
          FROM unnest(y.dates_active || ARRAY[t.active_date]) AS d
          ORDER BY d ASC
        )
      )
    END AS dates_active,
    (SELECT run_date FROM params) AS as_of_date
  FROM today t
  FULL OUTER JOIN yesterday y ON y.host = t.host
)
INSERT INTO hosts_cumulated (host, dates_active, as_of_date)
SELECT host, dates_active, as_of_date
FROM merged
ON CONFLICT (host, as_of_date)
DO UPDATE SET dates_active = EXCLUDED.dates_active;


CREATE TABLE host_activity_reduced (
  host TEXT NOT NULL,
  month DATE NOT NULL,
  hit_array INT[] NOT NULL DEFAULT '{}',
  unique_visitors_array INT[] NOT NULL DEFAULT '{}',
  PRIMARY KEY (host, month)
);


-- :run_date = '2023-01-03'
WITH daily_aggregate AS (
  SELECT 
    host,
    DATE(event_time) AS event_date,
    COUNT(1) AS num_site_hits,
    COUNT(DISTINCT user_id) AS unique_visitors
  FROM web_events
  WHERE DATE(event_time) = DATE '2023-01-03'
  GROUP BY host, DATE(event_time)
),
yesterday_array AS (
  SELECT * 
  FROM host_activity_reduced
  WHERE month = DATE_TRUNC('month', DATE '2023-01-03')
)
SELECT 
  COALESCE(da.host, ya.host) AS host,
  COALESCE(ya.month, DATE_TRUNC('month', da.event_date)) AS month,
  CASE
    WHEN ya.hit_array IS NOT NULL THEN 
      ya.hit_array || ARRAY[COALESCE(da.num_site_hits, 0)]
    WHEN ya.hit_array IS NULL THEN
      ARRAY_FILL(0, ARRAY[EXTRACT(DAY FROM da.event_date - DATE_TRUNC('month', da.event_date))::INT]) 
      || ARRAY[COALESCE(da.num_site_hits, 0)]
  END AS hit_array,
  CASE
    WHEN ya.unique_visitors_array IS NOT NULL THEN
      ya.unique_visitors_array || ARRAY[COALESCE(da.unique_visitors, 0)]
    WHEN ya.unique_visitors_array IS NULL THEN
      ARRAY_FILL(0, ARRAY[EXTRACT(DAY FROM da.event_date - DATE_TRUNC('month', da.event_date))::INT]) 
      || ARRAY[COALESCE(da.unique_visitors, 0)]
  END AS unique_visitors_array
FROM daily_aggregate da 
FULL OUTER JOIN yesterday_array ya ON da.host = ya.host
ON CONFLICT (host, month)
DO UPDATE 
SET hit_array = EXCLUDED.hit_array,
    unique_visitors_array = EXCLUDED.unique_visitors_array;

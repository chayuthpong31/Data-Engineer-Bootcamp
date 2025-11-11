WITH combined AS(
SELECT 
  COALESCE(d.browser_type, 'N/A') AS browser_type, 
  COALESCE(d.os_type, 'N/A') AS os_type,
  we.*,
  CASE
      WHEN referrer LIKE '%zachwilson%' THEN 'On Site'
      WHEN referrer LIKE '%eczachly%' THEN 'On Site'
      WHEN referrer LIKE '%dataengieer.io%' THEN 'On Site'
      WHEN referrer LIKE '%t.co%' THEN 'Twitter'
      WHEN referrer LIKE '%linkedin%' THEN 'Linkedin'
      WHEN referrer LIKE '%instragram%' THEN 'Instragram'
      WHEN referrer IS NULL THEN 'Direct'
      ELSE 'Other'
    END AS referrer_mapped
FROM bootcamp.web_events we JOIN bootcamp.devices d
ON we.device_id = d.device_id
),
aggregated AS(
  SELECT 
  c1.user_id,c1.url AS to_url, c2.url as from_url, MIN(c1.event_time - c2.event_time) as duration
FROM combined c1 JOIN combined c2
  ON c1.user_id = c2.user_id
  AND DATE(c1.event_time) = DATE(c2.event_time)
  AND c1.event_time > c2.event_time
  GROUP BY c1.user_id,c1.url, c2.url
)
  
SELECT to_url, from_url,
  COUNT(1) AS number_of_users,
  MIN(duration) as min_duration,
  MAX(duration) as max_duration,
  AVG(duration) as avg_duration
FROM aggregated
GROUP BY to_url, from_url
  HAVING COUNT(1) > 1000
LIMIT 100
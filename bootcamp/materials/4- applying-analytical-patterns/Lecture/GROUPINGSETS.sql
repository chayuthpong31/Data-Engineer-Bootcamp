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
)

SELECT 
  COALESCE(referrer_mapped, '(overall') AS referrer ,
  COALESCE(browser_type, '(overall') AS browser_type ,
  COALESCE(os_type, '(overall') AS os_type ,
  COUNT(1)  AS number_of_site_hits,
  COUNT(CASE WHEN url = '/signup' THEN 1 END) AS number_of_signup_visits, 
  COUNT(CASE WHEN url = '/contact' THEN 1 END) AS number_of_contact_visits, 
  COUNT(CASE WHEN url = '/login' THEN 1 END) AS number_of_login_visits,
  CAST(COUNT(CASE WHEN url = '/signup' THEN 1 END) AS REAL)/COUNT(1) AS pct_visited_signup
FROM combined 
GROUP BY GROUPING SETS(
  (referrer_mapped, browser_type, os_type),
  (os_type),
  (browser_type),    
  (referrer_mapped),
  ()
)
ORDER BY CAST(COUNT(CASE WHEN url = '/signup' THEN 1 END) AS REAL)/COUNT(1) DESC
-- How many asteroids pass Earth each day, and how many of them are potentially hazardous.
SELECT
    approach_date,
    COUNT(DISTINCT asteroid_id)                                    AS asteroid_count,
    COUNT(DISTINCT CASE WHEN is_hazardous THEN asteroid_id END)    AS hazardous_count,
    ROUND(100 * COUNT(DISTINCT CASE WHEN is_hazardous THEN asteroid_id END)
              / COUNT(DISTINCT asteroid_id), 1)                    AS hazardous_pct
FROM neo_approaches
GROUP BY approach_date
ORDER BY approach_date

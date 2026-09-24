-- How many asteroids pass Earth each week (weeks start on Monday), and how many are potentially hazardous.
-- days_covered < 7 marks a partial week at the edges of the extracted date range.
SELECT
    CAST(date_trunc('WEEK', approach_date) AS DATE)                AS week_start,
    COUNT(DISTINCT approach_date)                                  AS days_covered,
    COUNT(DISTINCT asteroid_id)                                    AS asteroid_count,
    COUNT(DISTINCT CASE WHEN is_hazardous THEN asteroid_id END)    AS hazardous_count,
    ROUND(100 * COUNT(DISTINCT CASE WHEN is_hazardous THEN asteroid_id END)
              / COUNT(DISTINCT asteroid_id), 1)                    AS hazardous_pct
FROM neo_approaches
GROUP BY week_start
ORDER BY week_start

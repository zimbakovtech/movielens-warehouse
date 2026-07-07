-- How many ratings were submitted per calendar year.
SELECT
    d.year,
    COUNT(*) AS num_ratings
FROM fact_ratings f
JOIN dim_date d ON d.date_key = f.date_key
GROUP BY d.year
ORDER BY d.year;

-- The 15 most active users and how generous their ratings are on average.
SELECT
    u.user_id,
    COUNT(*)                AS num_ratings,
    ROUND(AVG(f.rating), 2) AS avg_rating
FROM fact_ratings f
JOIN dim_user u ON u.user_key = f.user_key
GROUP BY u.user_id
ORDER BY num_ratings DESC
LIMIT 15;

-- Distribution of rating values (0.5 .. 5.0 in half-star steps).
SELECT
    f.rating,
    f.rating_bucket,
    COUNT(*) AS num_ratings
FROM fact_ratings f
GROUP BY f.rating, f.rating_bucket
ORDER BY f.rating;

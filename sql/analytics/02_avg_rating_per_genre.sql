-- Average rating per genre, best-rated first.
-- The bridge table resolves the many-to-many movie <-> genre relationship,
-- so a rating of a two-genre movie counts towards both genres.
SELECT
    g.genre_name,
    ROUND(AVG(f.rating), 2) AS avg_rating,
    COUNT(*)                AS num_ratings
FROM fact_ratings f
JOIN bridge_movie_genre b ON b.movie_key = f.movie_key
JOIN dim_genre g ON g.genre_key = b.genre_key
GROUP BY g.genre_name
ORDER BY avg_rating DESC;

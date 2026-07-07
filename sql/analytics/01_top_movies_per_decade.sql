-- Top 10 highest-rated movies per decade, counting only movies
-- with at least 50 ratings so tiny sample sizes don't win.
WITH movie_stats AS (
    SELECT
        m.decade,
        m.movie_key,
        m.title,
        m.release_year,
        ROUND(AVG(f.rating), 2) AS avg_rating,
        COUNT(*)                AS num_ratings
    FROM fact_ratings f
    JOIN dim_movie m ON m.movie_key = f.movie_key
    WHERE m.decade IS NOT NULL
    GROUP BY m.decade, m.movie_key, m.title, m.release_year
    HAVING COUNT(*) >= 50
),
ranked AS (
    SELECT *,
           RANK() OVER (PARTITION BY decade ORDER BY avg_rating DESC) AS rank_in_decade
    FROM movie_stats
)
SELECT decade, rank_in_decade, title, release_year, avg_rating, num_ratings
FROM ranked
WHERE rank_in_decade <= 10
ORDER BY decade, rank_in_decade, title;

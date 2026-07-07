-- Which genres dominate each decade, measured by how many ratings the
-- movies released in that decade received.
-- ROW_NUMBER instead of RANK so each decade gets exactly 3 rows (ties broken
-- alphabetically), which keeps the chart simple.
WITH genre_decade AS (
    SELECT
        m.decade,
        g.genre_name,
        COUNT(*) AS num_ratings
    FROM fact_ratings f
    JOIN dim_movie m ON m.movie_key = f.movie_key
    JOIN bridge_movie_genre b ON b.movie_key = f.movie_key
    JOIN dim_genre g ON g.genre_key = b.genre_key
    WHERE m.decade IS NOT NULL
    GROUP BY m.decade, g.genre_name
),
ranked AS (
    SELECT *,
           ROW_NUMBER() OVER (
               PARTITION BY decade
               ORDER BY num_ratings DESC, genre_name
           ) AS rank_in_decade
    FROM genre_decade
)
SELECT decade, rank_in_decade, genre_name, num_ratings
FROM ranked
WHERE rank_in_decade <= 3
ORDER BY decade, rank_in_decade;

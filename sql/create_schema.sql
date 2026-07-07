-- Star schema for the MovieLens data warehouse.
-- Grain of fact_ratings: one row per rating a user gave to a movie.
--
-- Surrogate keys are generated in Spark during the ETL, so the key columns
-- are plain integers here instead of SERIAL.

DROP TABLE IF EXISTS fact_ratings CASCADE;
DROP TABLE IF EXISTS bridge_movie_genre CASCADE;
DROP TABLE IF EXISTS dim_genre CASCADE;
DROP TABLE IF EXISTS dim_date CASCADE;
DROP TABLE IF EXISTS dim_user CASCADE;
DROP TABLE IF EXISTS dim_movie CASCADE;

CREATE TABLE dim_movie (
    movie_key    INT PRIMARY KEY,
    movie_id     INT NOT NULL UNIQUE,   -- natural key from MovieLens
    title        TEXT NOT NULL,
    release_year INT,                   -- NULL when the title has no year
    decade       INT                    -- e.g. 1990
);

CREATE TABLE dim_user (
    user_key INT PRIMARY KEY,
    user_id  INT NOT NULL UNIQUE
);

CREATE TABLE dim_date (
    date_key    INT PRIMARY KEY,        -- smart key in yyyymmdd form, e.g. 20150326
    full_date   DATE NOT NULL UNIQUE,
    year        INT NOT NULL,
    month       INT NOT NULL,
    day         INT NOT NULL,
    day_of_week TEXT NOT NULL,          -- Monday .. Sunday
    quarter     INT NOT NULL
);

CREATE TABLE dim_genre (
    genre_key  INT PRIMARY KEY,
    genre_name TEXT NOT NULL UNIQUE
);

-- A movie has many genres and a genre has many movies, so the
-- many-to-many relationship goes through a bridge table.
CREATE TABLE bridge_movie_genre (
    movie_key INT NOT NULL REFERENCES dim_movie (movie_key),
    genre_key INT NOT NULL REFERENCES dim_genre (genre_key),
    PRIMARY KEY (movie_key, genre_key)
);

CREATE TABLE fact_ratings (
    rating_id     BIGINT PRIMARY KEY,
    user_key      INT NOT NULL REFERENCES dim_user (user_key),
    movie_key     INT NOT NULL REFERENCES dim_movie (movie_key),
    date_key      INT NOT NULL REFERENCES dim_date (date_key),
    rating        NUMERIC(2, 1) NOT NULL CHECK (rating BETWEEN 0.5 AND 5.0),
    rating_bucket TEXT NOT NULL CHECK (rating_bucket IN ('low', 'mid', 'high'))
);

-- indexes on the fact table foreign keys to speed up star joins
CREATE INDEX idx_fact_ratings_user_key  ON fact_ratings (user_key);
CREATE INDEX idx_fact_ratings_movie_key ON fact_ratings (movie_key);
CREATE INDEX idx_fact_ratings_date_key  ON fact_ratings (date_key);

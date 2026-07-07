"""Transform step: clean the raw data and shape it into the star schema.

Surrogate keys are generated here with row_number() so the warehouse tables
receive ready-made integer keys. The dataset is small (~100k ratings), so
the single-partition window that row_number() needs is not a problem.
"""
from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType

NO_GENRES = "(no genres listed)"


def build_dim_movie(movies_raw: DataFrame) -> DataFrame:
    """One row per movie, with release year and decade parsed out of the title."""
    movies = movies_raw.dropna(subset=["movieId", "title"]).dropDuplicates(["movieId"])

    # titles normally end in "(1995)"; regexp_extract returns "" when they don't
    year_str = F.regexp_extract(F.col("title"), r"\((\d{4})\)\s*$", 1)
    movies = (
        movies
        .withColumn("release_year", F.when(year_str != "", year_str.cast("int")))
        .withColumn("clean_title", F.trim(F.regexp_replace("title", r"\s*\(\d{4}\)\s*$", "")))
        .withColumn("decade", (F.floor(F.col("release_year") / 10) * 10).cast("int"))
    )

    return (
        movies
        .withColumn("movie_key", F.row_number().over(Window.orderBy("movieId")))
        .select(
            "movie_key",
            F.col("movieId").alias("movie_id"),
            F.col("clean_title").alias("title"),
            "release_year",
            "decade",
        )
    )


def build_dim_user(ratings_raw: DataFrame) -> DataFrame:
    """One row per distinct user seen in the ratings file."""
    return (
        ratings_raw
        .select("userId")
        .dropna()
        .distinct()
        .withColumn("user_key", F.row_number().over(Window.orderBy("userId")))
        .select("user_key", F.col("userId").alias("user_id"))
    )


def clean_ratings(ratings_raw: DataFrame) -> DataFrame:
    """Drop incomplete rows, deduplicate (user, movie) keeping the newest rating,
    and turn the Unix epoch timestamp into a calendar date."""
    window = Window.partitionBy("userId", "movieId").orderBy(F.col("timestamp").desc())
    return (
        ratings_raw
        .dropna(subset=["userId", "movieId", "rating", "timestamp"])
        .withColumn("rn", F.row_number().over(window))
        .filter(F.col("rn") == 1)
        .drop("rn")
        .withColumn("rating_date", F.to_date(F.timestamp_seconds("timestamp")))
    )


def build_dim_date(ratings: DataFrame) -> DataFrame:
    """One row per distinct date a rating was made on; date_key is yyyymmdd."""
    return (
        ratings
        .select(F.col("rating_date").alias("full_date"))
        .distinct()
        .withColumn("date_key", F.date_format("full_date", "yyyyMMdd").cast("int"))
        .withColumn("year", F.year("full_date"))
        .withColumn("month", F.month("full_date"))
        .withColumn("day", F.dayofmonth("full_date"))
        .withColumn("day_of_week", F.date_format("full_date", "EEEE"))
        .withColumn("quarter", F.quarter("full_date"))
        .select("date_key", "full_date", "year", "month", "day", "day_of_week", "quarter")
    )


def _explode_genres(movies_raw: DataFrame) -> DataFrame:
    """movieId + one genre per row; '(no genres listed)' is excluded."""
    return (
        movies_raw
        .dropna(subset=["movieId", "genres"])
        .select("movieId", F.explode(F.split("genres", r"\|")).alias("genre_name"))
        .filter((F.col("genre_name") != NO_GENRES) & (F.col("genre_name") != ""))
    )


def build_dim_genre(movies_raw: DataFrame) -> DataFrame:
    """One row per distinct genre."""
    return (
        _explode_genres(movies_raw)
        .select("genre_name")
        .distinct()
        .withColumn("genre_key", F.row_number().over(Window.orderBy("genre_name")))
        .select("genre_key", "genre_name")
    )


def build_bridge_movie_genre(
    movies_raw: DataFrame, dim_movie: DataFrame, dim_genre: DataFrame
) -> DataFrame:
    """Bridge table resolving the many-to-many movie <-> genre relationship."""
    movie_genre = _explode_genres(movies_raw).dropDuplicates(["movieId", "genre_name"])
    return (
        movie_genre
        .join(dim_movie, movie_genre.movieId == dim_movie.movie_id)
        .join(dim_genre, "genre_name")
        .select("movie_key", "genre_key")
    )


def build_fact_ratings(
    ratings: DataFrame, dim_user: DataFrame, dim_movie: DataFrame, dim_date: DataFrame
) -> DataFrame:
    """The fact table: one row per user rating of a movie, keyed to all dimensions."""
    bucket = (
        F.when(F.col("rating") < 2.5, "low")
        .when(F.col("rating") <= 3.5, "mid")
        .otherwise("high")
    )
    return (
        ratings
        .join(dim_user, ratings.userId == dim_user.user_id)
        .join(dim_movie, ratings.movieId == dim_movie.movie_id)
        .join(dim_date, ratings.rating_date == dim_date.full_date)
        .withColumn("rating_bucket", bucket)
        .withColumn("rating", F.col("rating").cast(DecimalType(2, 1)))
        .withColumn("rating_id", F.row_number().over(Window.orderBy("user_key", "movie_key")).cast("long"))
        .select("rating_id", "user_key", "movie_key", "date_key", "rating", "rating_bucket")
    )


def transform_all(movies_raw: DataFrame, ratings_raw: DataFrame) -> dict[str, DataFrame]:
    """Build every warehouse table. Dict order = FK-safe load order."""
    dim_movie = build_dim_movie(movies_raw)
    dim_user = build_dim_user(ratings_raw)
    ratings = clean_ratings(ratings_raw)
    dim_date = build_dim_date(ratings)
    dim_genre = build_dim_genre(movies_raw)
    bridge_movie_genre = build_bridge_movie_genre(movies_raw, dim_movie, dim_genre)
    fact_ratings = build_fact_ratings(ratings, dim_user, dim_movie, dim_date)
    return {
        "dim_movie": dim_movie,
        "dim_user": dim_user,
        "dim_date": dim_date,
        "dim_genre": dim_genre,
        "bridge_movie_genre": bridge_movie_genre,
        "fact_ratings": fact_ratings,
    }


if __name__ == "__main__":
    # smoke test without Postgres: run extract + transform and show what comes out
    from etl.extract import get_spark, read_movies, read_ratings

    spark = get_spark()
    tables = transform_all(read_movies(spark), read_ratings(spark))
    for name, df in tables.items():
        print(f"\n=== {name}: {df.count()} rows ===")
        df.show(5, truncate=False)
    spark.stop()

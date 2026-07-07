"""Extract step: read the raw MovieLens CSV files into Spark DataFrames.

Schemas are declared explicitly instead of using inferSchema, so a malformed
input file fails loudly instead of silently changing column types.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

import config

MOVIES_SCHEMA = StructType([
    StructField("movieId", IntegerType(), nullable=False),
    StructField("title", StringType(), nullable=True),
    StructField("genres", StringType(), nullable=True),
])

RATINGS_SCHEMA = StructType([
    StructField("userId", IntegerType(), nullable=False),
    StructField("movieId", IntegerType(), nullable=False),
    StructField("rating", DoubleType(), nullable=True),
    StructField("timestamp", LongType(), nullable=True),
])


def get_spark() -> SparkSession:
    """Build the SparkSession used by the whole pipeline."""
    return (
        SparkSession.builder
        .appName(config.SPARK_APP_NAME)
        # Postgres JDBC driver for the load step
        .config("spark.jars.packages", config.POSTGRES_JDBC_COORD)
        # MovieLens timestamps are Unix epoch (UTC); keep date conversion deterministic
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )


def _read_csv(spark: SparkSession, filename: str, schema: StructType) -> DataFrame:
    return (
        spark.read
        .option("header", True)
        # MovieLens quotes fields RFC 4180 style ("" inside quoted strings)
        .option("escape", '"')
        .schema(schema)
        .csv(str(config.RAW_DATA_DIR / filename))
    )


def read_movies(spark: SparkSession) -> DataFrame:
    return _read_csv(spark, "movies.csv", MOVIES_SCHEMA)


def read_ratings(spark: SparkSession) -> DataFrame:
    return _read_csv(spark, "ratings.csv", RATINGS_SCHEMA)

"""Orchestrate the full ETL pipeline: extract -> transform -> load."""
import time

from etl.extract import get_spark, read_movies, read_ratings
from etl.load import load_all, print_row_counts
from etl.transform import transform_all


def main() -> None:
    start = time.time()
    spark = get_spark()

    print("[1/3] extract: reading raw CSVs into Spark")
    movies_raw = read_movies(spark)
    ratings_raw = read_ratings(spark)

    print("[2/3] transform: building star schema tables")
    tables = transform_all(movies_raw, ratings_raw)

    print("[3/3] load: writing to Postgres via JDBC")
    load_all(tables)

    print("row counts in the warehouse:")
    print_row_counts()

    print(f"pipeline finished in {time.time() - start:.1f}s")
    spark.stop()


if __name__ == "__main__":
    main()

"""Load step: write the warehouse tables into Postgres over JDBC.

The target tables already exist (sql/create_schema.sql), so every write is a
plain JDBC append. Before loading, all tables are truncated so the pipeline
can be re-run without violating primary keys.
"""
import psycopg2
from pyspark.sql import DataFrame

import config

ALL_TABLES = [
    "fact_ratings",
    "bridge_movie_genre",
    "dim_genre",
    "dim_date",
    "dim_user",
    "dim_movie",
]


def _connect():
    return psycopg2.connect(
        host=config.DB_HOST,
        port=config.DB_PORT,
        dbname=config.DB_NAME,
        user=config.DB_USER,
        password=config.DB_PASSWORD,
    )


def truncate_tables() -> None:
    """Empty all warehouse tables so the load is idempotent."""
    conn = _connect()
    try:
        with conn, conn.cursor() as cur:
            cur.execute(f"TRUNCATE {', '.join(ALL_TABLES)} CASCADE")
    finally:
        conn.close()


def load_table(df: DataFrame, table: str) -> None:
    df.write.jdbc(
        url=config.JDBC_URL,
        table=table,
        mode="append",
        properties=config.JDBC_PROPERTIES,
    )


def load_all(tables: dict[str, DataFrame]) -> None:
    """Truncate everything, then load dimensions before the fact table
    (the dict from transform_all is already in FK-safe order)."""
    truncate_tables()
    for name, df in tables.items():
        load_table(df, name)
        print(f"  loaded {name}")


def print_row_counts() -> None:
    """Sanity check after the load: row count per warehouse table."""
    conn = _connect()
    try:
        with conn, conn.cursor() as cur:
            for table in reversed(ALL_TABLES):
                cur.execute(f"SELECT COUNT(*) FROM {table}")
                print(f"  {table}: {cur.fetchone()[0]} rows")
    finally:
        conn.close()

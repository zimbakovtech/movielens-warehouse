"""Central configuration for the MovieLens data warehouse project.

Every value can be overridden through environment variables (or a local
.env file), so nothing is hardcoded in the ETL / analytics code itself.
"""
import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

PROJECT_ROOT = Path(__file__).resolve().parent

# --- database connection ---
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = int(os.getenv("DB_PORT", "5432"))
DB_NAME = os.getenv("DB_NAME", "movielens_dw")
DB_USER = os.getenv("DB_USER", "dw_user")
DB_PASSWORD = os.getenv("DB_PASSWORD", "dw_pass")

# used by Spark to write into Postgres
JDBC_URL = f"jdbc:postgresql://{DB_HOST}:{DB_PORT}/{DB_NAME}"
JDBC_PROPERTIES = {
    "user": DB_USER,
    "password": DB_PASSWORD,
    "driver": "org.postgresql.Driver",
}

# Postgres JDBC driver; Spark downloads it from Maven Central on first run
POSTGRES_JDBC_COORD = "org.postgresql:postgresql:42.7.7"

# --- data locations ---
DATA_DIR = PROJECT_ROOT / "data"
RAW_DATA_DIR = DATA_DIR / "raw" / "ml-latest-small"
OUTPUT_DIR = PROJECT_ROOT / "output"

DATASET_URL = "https://files.grouplens.org/datasets/movielens/ml-latest-small.zip"

# --- spark ---
SPARK_APP_NAME = "movielens-warehouse"

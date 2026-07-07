# MovieLens Analytics Warehouse

Final project for the *Data Warehouses and Analytics* course. An end-to-end ETL
pipeline that ingests the MovieLens `ml-latest-small` dataset with **PySpark**,
shapes it into a **star schema**, loads it into **PostgreSQL** over JDBC, and
runs analytical SQL queries with charts on top.

## Architecture

```
 grouplens.org                    Spark (PySpark)                     PostgreSQL
+--------------+     +----------------------------------------+     +--------------+
| ml-latest-   |     |  extract.py     transform.py   load.py  |     | movielens_dw |
| small.zip    | --> |  CSVs with   -> clean, derive, -> JDBC  | --> |  star schema |
| (CSV files)  |     |  explicit       build dims +    append  |     |  (6 tables)  |
+--------------+     |  schemas        fact, gen keys          |     +------+-------+
                     +----------------------------------------+            |
                                                                           v
                                                              +------------------------+
                                                              | analytics/run_queries  |
                                                              | psycopg2 + matplotlib  |
                                                              | -> stdout + output/*.png |
                                                              +------------------------+
```

## Star schema

The **grain** of the fact table is *one row per rating a user gave to a movie* —
the finest level of detail the dataset offers, so any coarser view (per genre,
per year, per user) can be aggregated from it.

```
                dim_date                       dim_user
             (date_key PK)                  (user_key PK)
                    \                          /
                     \                        /
                      +----- fact_ratings ----+
                      | rating_id PK          |
                      | user_key FK           |
                      | movie_key FK          |
                      | date_key FK           |
                      | rating, rating_bucket |
                      +-----------+-----------+
                                  |
                              dim_movie
                            (movie_key PK)
                                  |
                        bridge_movie_genre
                       (movie_key, genre_key)
                                  |
                              dim_genre
                            (genre_key PK)
```

- **fact_ratings** — measures: `rating` (0.5–5.0) and the derived
  `rating_bucket` (`low` < 2.5, `mid` 2.5–3.5, `high` > 3.5).
- **dim_movie** — one row per movie; `release_year` is parsed out of the title
  ("Toy Story (1995)"), `decade` is derived from it. Titles without a year get
  NULLs.
- **dim_user** — one row per user (the dataset only provides user ids).
- **dim_date** — one row per calendar date a rating happened on, with a
  `yyyymmdd` smart key plus year/month/day/day-of-week/quarter attributes.
- **dim_genre** + **bridge_movie_genre** — see below.

Surrogate keys are generated in Spark (`row_number()`), so the DDL uses plain
integer keys instead of SERIAL and the load is a straight JDBC append.

### Why a bridge table for genres?

A movie has *many* genres and a genre belongs to *many* movies. That
many-to-many relationship cannot be modeled with a single foreign key on
either side without losing information:

- putting the pipe-separated genre string on `dim_movie` would make genre
  filtering/grouping a string-parsing exercise in SQL;
- duplicating fact rows per genre would inflate the fact table and break its
  grain (one rating would appear 3× for a 3-genre movie, corrupting counts
  and averages).

The classic warehouse answer is a **bridge table**: `bridge_movie_genre` holds
one row per (movie, genre) pair, keeping the fact table's grain intact. Genre
queries join `fact_ratings → bridge_movie_genre → dim_genre`. One conscious
trade-off: a rating of a two-genre movie counts once *per genre* in per-genre
aggregates, which is exactly the intended semantics for "average rating per
genre".

`"(no genres listed)"` is treated as *no genre*: such movies stay in
`dim_movie` but simply have no rows in the bridge.

## ETL steps

1. **Extract** (`etl/extract.py`) — reads `movies.csv` and `ratings.csv` into
   Spark DataFrames with explicit schemas (no `inferSchema`), RFC 4180 quote
   handling for titles containing commas/quotes.
2. **Transform** (`etl/transform.py`) —
   - drop rows with NULLs in required columns, deduplicate movies by id and
     ratings by (user, movie) keeping the newest;
   - parse `release_year` from the title with a regex anchored at the end,
     tolerate titles without a year; derive `decade`;
   - split the pipe-separated genre string, explode to rows, exclude
     `"(no genres listed)"`;
   - convert Unix epoch timestamps to dates (UTC) and build the date dimension;
   - derive `rating_bucket` (low/mid/high);
   - generate all surrogate keys with `row_number()`.
3. **Load** (`etl/load.py`) — truncates all warehouse tables (idempotent
   re-runs), then appends dimensions → bridge → fact via Spark JDBC in
   FK-safe order.

## How to run everything from scratch

Prerequisites: Docker, Python 3.11+ (tested on 3.13), Java 17 (required by
Spark). The Postgres JDBC driver is fetched automatically from Maven Central
by Spark on first run (`spark.jars.packages` in `etl/extract.py`).

```bash
# 0. python environment
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# 1. start the warehouse database (postgres 16 on localhost:5432)
docker compose up -d --wait

# 2. download the dataset into data/raw/
python scripts/download_data.py

# 3. create the star schema
docker compose exec -T postgres psql -U dw_user -d movielens_dw < sql/create_schema.sql

# 4. run the ETL pipeline (extract -> transform -> load)
python run_pipeline.py

# 5. run the analytical queries and generate charts into output/
python analytics/run_queries.py
```

Database connection settings default to the values in `docker-compose.yml`
(`movielens_dw` / `dw_user` / `dw_pass` on `localhost:5432`) and can be
overridden via environment variables or a `.env` file (see `.env.example`).

To sanity-check the Spark transformations without Postgres:

```bash
python -m etl.transform
```

## Analytical queries (`sql/analytics/`)

| # | Query | Chart |
|---|-------|-------|
| 1 | Top 10 highest-rated movies per decade (≥ 50 ratings) | `output/01_top_movies_per_decade.png` |
| 2 | Average rating per genre, ranked | `output/02_avg_rating_per_genre.png` |
| 3 | Rating volume trend by year | `output/03_rating_volume_by_year.png` |
| 4 | Rating distribution (half-star histogram, colored by bucket) | `output/04_rating_distribution.png` |
| 5 | Most active users and their average rating | – |
| 6 | Genre popularity by decade (top 3 per decade) | `output/06_genre_popularity_by_decade.png` |

## Project structure

```
├── docker-compose.yml          # postgres:16 warehouse
├── config.py                   # central config (env-overridable)
├── scripts/download_data.py    # fetch + unzip ml-latest-small
├── sql/
│   ├── create_schema.sql       # star schema DDL
│   └── analytics/              # one .sql file per analytical query
├── etl/
│   ├── extract.py              # CSVs -> Spark DataFrames (explicit schemas)
│   ├── transform.py            # cleaning + star schema shaping
│   └── load.py                 # JDBC load into Postgres
├── run_pipeline.py             # extract -> transform -> load
├── analytics/run_queries.py    # run queries, print tables, save charts
└── output/                     # generated charts (gitignored)
```

## Dataset

[MovieLens ml-latest-small](https://grouplens.org/datasets/movielens/) —
100,836 ratings of 9,742 movies by 610 users (1996–2018). Free for academic
use; downloaded automatically by `scripts/download_data.py`.

---

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

---

*© 2026 Damjan Zimbakov - FINKI Data Warehouses Course*

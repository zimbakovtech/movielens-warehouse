"""Run every analytical query in sql/analytics/, print the results as tables,
and save a chart into output/ for the queries where a chart makes sense."""
import sys
from pathlib import Path

import matplotlib

matplotlib.use("Agg")  # no display needed, we only save PNG files
import matplotlib.pyplot as plt
import psycopg2

# make the project root importable when running as `python analytics/run_queries.py`
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config

SQL_DIR = config.PROJECT_ROOT / "sql" / "analytics"


def run_query(conn, sql_path: Path):
    with conn.cursor() as cur:
        cur.execute(sql_path.read_text())
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
    return columns, rows


def print_table(columns, rows) -> None:
    widths = [
        max(len(str(col)), max((len(str(row[i])) for row in rows), default=0))
        for i, col in enumerate(columns)
    ]
    print(" | ".join(str(col).ljust(w) for col, w in zip(columns, widths)))
    print("-+-".join("-" * w for w in widths))
    for row in rows:
        print(" | ".join(str(val).ljust(w) for val, w in zip(row, widths)))


# --- charts -----------------------------------------------------------------

def chart_avg_rating_per_genre(rows, out_path: Path) -> None:
    genres = [r[0] for r in rows][::-1]  # reversed so the best genre is on top
    avgs = [float(r[1]) for r in rows][::-1]
    fig, ax = plt.subplots(figsize=(9, 6))
    ax.barh(genres, avgs, color="steelblue")
    ax.set_xlabel("Average rating")
    ax.set_xlim(0, 5)
    ax.set_title("Average rating per genre")
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)


def chart_rating_volume_by_year(rows, out_path: Path) -> None:
    years = [r[0] for r in rows]
    counts = [r[1] for r in rows]
    fig, ax = plt.subplots(figsize=(10, 5))
    ax.plot(years, counts, marker="o", color="steelblue")
    ax.set_xlabel("Year")
    ax.set_ylabel("Number of ratings")
    ax.set_title("Rating volume by year")
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)


def chart_rating_distribution(rows, out_path: Path) -> None:
    labels = [str(r[0]) for r in rows]
    counts = [r[2] for r in rows]
    bucket_colors = {"low": "indianred", "mid": "goldenrod", "high": "seagreen"}
    colors = [bucket_colors[r[1]] for r in rows]
    fig, ax = plt.subplots(figsize=(9, 5))
    ax.bar(labels, counts, color=colors)
    ax.set_xlabel("Rating")
    ax.set_ylabel("Number of ratings")
    ax.set_title("Rating distribution (color = rating bucket)")
    handles = [plt.Rectangle((0, 0), 1, 1, color=c) for c in bucket_colors.values()]
    ax.legend(handles, bucket_colors.keys(), title="bucket")
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)


def chart_genre_popularity_by_decade(rows, out_path: Path) -> None:
    # rows: (decade, rank_in_decade, genre_name, num_ratings)
    decades = sorted({r[0] for r in rows})
    ranks = sorted({r[1] for r in rows})
    width = 0.8 / len(ranks)
    fig, ax = plt.subplots(figsize=(12, 6))
    for j, rank in enumerate(ranks):
        by_decade = {r[0]: r for r in rows if r[1] == rank}
        xs = [i + j * width for i in range(len(decades))]
        heights = [by_decade[d][3] if d in by_decade else 0 for d in decades]
        ax.bar(xs, heights, width=width * 0.95, color=f"C{j}", label=f"#{rank} genre")
        for x, decade in zip(xs, decades):
            if decade in by_decade:
                ax.text(x, by_decade[decade][3], " " + by_decade[decade][2],
                        rotation=90, ha="center", va="bottom", fontsize=8)
    ax.set_xticks([i + 0.4 - width / 2 for i in range(len(decades))])
    ax.set_xticklabels([f"{d}s" for d in decades])
    ax.set_ylabel("Number of ratings")
    ax.set_title("Top 3 genres per movie decade (by rating volume)")
    ax.margins(y=0.25)  # headroom for the genre labels
    ax.legend()
    fig.tight_layout()
    fig.savefig(out_path, dpi=150)
    plt.close(fig)


# maps query file stem -> chart function
CHARTS = {
    "02_avg_rating_per_genre": chart_avg_rating_per_genre,
    "03_rating_volume_by_year": chart_rating_volume_by_year,
    "04_rating_distribution": chart_rating_distribution,
    "06_genre_popularity_by_decade": chart_genre_popularity_by_decade,
}


def main() -> None:
    config.OUTPUT_DIR.mkdir(exist_ok=True)
    conn = psycopg2.connect(
        host=config.DB_HOST,
        port=config.DB_PORT,
        dbname=config.DB_NAME,
        user=config.DB_USER,
        password=config.DB_PASSWORD,
    )
    try:
        for sql_path in sorted(SQL_DIR.glob("*.sql")):
            print(f"\n{'=' * 70}\n{sql_path.name}\n{'=' * 70}")
            columns, rows = run_query(conn, sql_path)
            print_table(columns, rows)
            chart_fn = CHARTS.get(sql_path.stem)
            if chart_fn:
                out_path = config.OUTPUT_DIR / f"{sql_path.stem}.png"
                chart_fn(rows, out_path)
                print(f"\nchart saved to {out_path.relative_to(config.PROJECT_ROOT)}")
    finally:
        conn.close()


if __name__ == "__main__":
    main()

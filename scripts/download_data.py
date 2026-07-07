"""Download and unzip the MovieLens ml-latest-small dataset into data/raw/."""
import io
import sys
import zipfile
from pathlib import Path

import requests

# make the project root importable when running as `python scripts/download_data.py`
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import config


def main() -> None:
    marker = config.RAW_DATA_DIR / "ratings.csv"
    if marker.exists():
        print(f"Dataset already present at {config.RAW_DATA_DIR}, skipping download.")
        return

    target_dir = config.DATA_DIR / "raw"
    target_dir.mkdir(parents=True, exist_ok=True)

    print(f"Downloading {config.DATASET_URL} ...")
    response = requests.get(config.DATASET_URL, timeout=120)
    response.raise_for_status()

    print("Unzipping ...")
    with zipfile.ZipFile(io.BytesIO(response.content)) as zf:
        zf.extractall(target_dir)

    csv_files = sorted(p.name for p in config.RAW_DATA_DIR.glob("*.csv"))
    print(f"Done. Files in {config.RAW_DATA_DIR}: {csv_files}")


if __name__ == "__main__":
    main()

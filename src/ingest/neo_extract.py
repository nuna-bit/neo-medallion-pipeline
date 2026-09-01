import argparse
import os
import json
import time
from datetime import date, timedelta
import requests

from dotenv import load_dotenv
load_dotenv()

API_KEY = os.environ["NASA_API_KEY"]
BASE_URL = "https://api.nasa.gov/neo/rest/v1/feed"
BRONZE_DIR = "data/bronze"


def daterange_windows(start: date, end: date, days: int = 7):
    current = start
    while current <= end:
        window_end = min(current + timedelta(days=days - 1), end)
        yield current, window_end
        current = window_end + timedelta(days=1)


def fetch_window(start: date, end: date) -> dict:
    params = {"start_date": start.isoformat(), "end_date": end.isoformat(), "api_key": API_KEY}
    response = requests.get(BASE_URL, params=params, timeout=30)
    response.raise_for_status()
    return response.json()


def save_raw(payload: dict, start: date, end: date) -> None:
    partition_dir = os.path.join(BRONZE_DIR, f"start={start.isoformat()}")
    os.makedirs(partition_dir, exist_ok=True)
    with open(os.path.join(partition_dir, f"neo_feed_{start}_{end}.json"), "w") as f:
        json.dump(payload, f)


def run(start: date, end: date) -> None:
    for window_start, window_end in daterange_windows(start, end):
        save_raw(fetch_window(window_start, window_end), window_start, window_end)
        time.sleep(1)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Extract NASA NEO feed data into the bronze layer.")
    parser.add_argument(
        "--start",
        type=date.fromisoformat,
        default=date(2026, 1, 1),
        help="Start date in YYYY-MM-DD format (default: 2026-01-01).",
    )
    parser.add_argument(
        "--end",
        type=date.fromisoformat,
        default=date(2026, 8, 31),
        help="End date in YYYY-MM-DD format (default: 2026-08-31).",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()
    run(args.start, args.end)

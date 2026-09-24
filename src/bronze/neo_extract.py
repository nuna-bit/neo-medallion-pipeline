import argparse
import os
import json
import time
from datetime import date, timedelta
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from dotenv import load_dotenv
load_dotenv()

BASE_URL = "https://api.nasa.gov/neo/rest/v1/feed"
BRONZE_DIR = "data/bronze"


def get_api_key() -> str:
    api_key = os.environ.get("NASA_API_KEY")
    if not api_key:
        raise RuntimeError("NASA_API_KEY is not set. Add it to your .env file or export it in your shell.")
    return api_key


def build_session(retries: int = 5, backoff_factor: float = 2.0) -> requests.Session:
    retry = Retry(
        total=retries,
        backoff_factor=backoff_factor,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        respect_retry_after_header=True,
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry))
    return session


def daterange_windows(start: date, end: date, days: int = 7):
    current = start
    while current <= end:
        window_end = min(current + timedelta(days=days - 1), end)
        yield current, window_end
        current = window_end + timedelta(days=1)


def fetch_window(session: requests.Session, api_key: str, start: date, end: date) -> dict:
    params = {"start_date": start.isoformat(), "end_date": end.isoformat(), "api_key": api_key}
    response = session.get(BASE_URL, params=params, timeout=30)
    response.raise_for_status()
    return response.json()


def redact_api_key(payload: dict, api_key: str) -> dict:
    # NASA echoes the api_key back inside the "links" URLs; keep it out of the stored files
    return json.loads(json.dumps(payload).replace(api_key, "REDACTED"))


def save_raw(payload: dict, start: date, end: date) -> None:
    partition_dir = os.path.join(BRONZE_DIR, f"start={start.isoformat()}")
    os.makedirs(partition_dir, exist_ok=True)
    with open(os.path.join(partition_dir, f"neo_feed_{start}_{end}.json"), "w") as f:
        json.dump(payload, f)


def run(start: date, end: date) -> None:
    if start > end:
        raise ValueError(f"start date {start} is after end date {end}")
    api_key = get_api_key()
    session = build_session()
    for window_start, window_end in daterange_windows(start, end):
        payload = redact_api_key(fetch_window(session, api_key, window_start, window_end), api_key)
        save_raw(payload, window_start, window_end)
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
    args = parser.parse_args()
    if args.start > args.end:
        parser.error(f"--start ({args.start}) must be on or before --end ({args.end})")
    return args


if __name__ == "__main__":
    args = parse_args()
    run(args.start, args.end)

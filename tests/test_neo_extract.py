import os
from datetime import date, timedelta

os.environ.setdefault("NASA_API_KEY", "test-key")

from src.ingest.neo_extract import daterange_windows


def test_daterange_windows_splits_into_seven_day_chunks_with_short_final_window():
    start = date(2026, 1, 1)
    end = date(2026, 1, 30)

    windows = list(daterange_windows(start, end, days=7))

    assert windows == [
        (date(2026, 1, 1), date(2026, 1, 7)),
        (date(2026, 1, 8), date(2026, 1, 14)),
        (date(2026, 1, 15), date(2026, 1, 21)),
        (date(2026, 1, 22), date(2026, 1, 28)),
        (date(2026, 1, 29), date(2026, 1, 30)),
    ]


def test_daterange_windows_covers_full_range_without_gaps_or_overlap():
    start = date(2026, 1, 1)
    end = date(2026, 1, 30)

    windows = list(daterange_windows(start, end, days=7))

    assert windows[0][0] == start
    assert windows[-1][1] == end
    for (_, prev_end), (next_start, _) in zip(windows, windows[1:]):
        assert next_start == prev_end + timedelta(days=1)


def test_daterange_windows_single_day_range_yields_one_window():
    start = date(2026, 3, 5)

    windows = list(daterange_windows(start, start, days=7))

    assert windows == [(start, start)]

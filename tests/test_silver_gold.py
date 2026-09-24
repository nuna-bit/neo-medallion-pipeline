import json
from datetime import date

from src.gold.build_gold import build_table
from src.silver.neo_approaches import read_bronze, transform


def asteroid(asteroid_id, approach_date, hazardous, orbiting_body="Earth"):
    return {
        "id": asteroid_id,
        "name": f"({asteroid_id})",
        "is_potentially_hazardous_asteroid": hazardous,
        "close_approach_data": [{"close_approach_date": approach_date, "orbiting_body": orbiting_body}],
    }


def write_bronze(root, start, near_earth_objects):
    partition = root / f"start={start}"
    partition.mkdir(parents=True)
    payload = {"element_count": sum(len(v) for v in near_earth_objects.values()), "near_earth_objects": near_earth_objects}
    (partition / f"neo_feed_{start}.json").write_text(json.dumps(payload))


def silver_rows(spark, bronze_dir):
    rows = transform(read_bronze(spark, str(bronze_dir))).collect()
    return sorted((r.asteroid_id, r.approach_date, r.is_hazardous) for r in rows)


def test_silver_flattens_one_row_per_asteroid_approach(spark, tmp_path):
    write_bronze(tmp_path, "2026-01-05", {
        "2026-01-05": [asteroid("a1", "2026-01-05", True), asteroid("a2", "2026-01-05", False)],
        "2026-01-06": [asteroid("a3", "2026-01-06", False)],
    })

    assert silver_rows(spark, tmp_path) == [
        ("a1", date(2026, 1, 5), True),
        ("a2", date(2026, 1, 5), False),
        ("a3", date(2026, 1, 6), False),
    ]


def test_silver_removes_duplicates_from_overlapping_bronze_files(spark, tmp_path):
    write_bronze(tmp_path, "2026-01-05", {"2026-01-06": [asteroid("a1", "2026-01-06", True)]})
    write_bronze(tmp_path, "2026-01-06", {"2026-01-06": [asteroid("a1", "2026-01-06", True)]})

    assert silver_rows(spark, tmp_path) == [("a1", date(2026, 1, 6), True)]


def test_silver_keeps_only_earth_approaches(spark, tmp_path):
    write_bronze(tmp_path, "2026-01-05", {
        "2026-01-05": [asteroid("a1", "2026-01-05", False), asteroid("a2", "2026-01-05", False, orbiting_body="Mars")],
    })

    assert silver_rows(spark, tmp_path) == [("a1", date(2026, 1, 5), False)]


def silver_df(spark, rows):
    return spark.createDataFrame(rows, "asteroid_id string, asteroid_name string, approach_date date, is_hazardous boolean")


def test_gold_daily_counts_asteroids_and_hazardous(spark):
    silver = silver_df(spark, [
        ("a1", "(a1)", date(2026, 1, 5), True),
        ("a2", "(a2)", date(2026, 1, 5), False),
        ("a3", "(a3)", date(2026, 1, 5), False),
        ("a4", "(a4)", date(2026, 1, 6), True),
    ])

    rows = build_table(spark, silver, "daily_neo_counts").collect()

    assert [(r.approach_date, r.asteroid_count, r.hazardous_count, float(r.hazardous_pct)) for r in rows] == [
        (date(2026, 1, 5), 3, 1, 33.3),
        (date(2026, 1, 6), 1, 1, 100.0),
    ]


def test_gold_weekly_groups_by_monday_and_flags_partial_weeks(spark):
    silver = silver_df(spark, [
        ("a1", "(a1)", date(2026, 1, 1), True),   # Thursday -> week of Mon 2025-12-29
        ("a2", "(a2)", date(2026, 1, 4), False),  # Sunday   -> same week
        ("a3", "(a3)", date(2026, 1, 5), False),  # Monday   -> week of 2026-01-05
    ])

    rows = build_table(spark, silver, "weekly_neo_counts").collect()

    assert [(r.week_start, r.days_covered, r.asteroid_count, r.hazardous_count) for r in rows] == [
        (date(2025, 12, 29), 2, 2, 1),
        (date(2026, 1, 5), 1, 1, 0),
    ]

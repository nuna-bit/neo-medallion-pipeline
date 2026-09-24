# neo-medallion-pipeline

A data engineering pipeline for NASA's [Near Earth Object (NEO) API](https://api.nasa.gov/),
built around the **medallion architecture**.

## The question it answers

> **How many asteroids pass Earth each day or week, and how many of them are potentially hazardous?**

NASA's NEO Feed lists every asteroid that makes a close approach to Earth on a given day, with
a flag (`is_potentially_hazardous_asteroid`) for the ones that are large enough and pass close
enough to be considered potentially hazardous.

## How the data flows

```
NASA NEO Feed API
      │
      ▼
BRONZE   data/bronze/start=<date>/neo_feed_<start>_<end>.json
      │  raw weekly JSON, exactly as returned by the API (API key redacted)
      ▼
SILVER   data/silver/neo_approaches/          (Parquet)
      │  one row per asteroid close approach to Earth
      ▼
GOLD     data/gold/daily_neo_counts/          (Parquet)
         data/gold/weekly_neo_counts/         (Parquet)
         asteroid and hazardous counts per day and per week
```

### Bronze — `src/bronze/neo_extract.py` (Python)

Calls the NEO Feed API in 7-day windows (the API's maximum per request) and stores each
response untouched, partitioned by window start date. Failed requests are retried on rate
limits (429) and server errors (5xx) with exponential backoff. The API key that NASA echoes
back inside the response links is replaced with `REDACTED` before saving.

### Silver — `src/silver/neo_approaches.py` (PySpark)

Flattens the nested JSON (week → day → asteroid → close approaches) into one clean table:

| column          | type    | description                                  |
|-----------------|---------|----------------------------------------------|
| `asteroid_id`   | string  | NASA's asteroid ID                           |
| `asteroid_name` | string  | e.g. `26663 (2000 XK47)`                     |
| `approach_date` | date    | day of the close approach                    |
| `is_hazardous`  | boolean | NASA's potentially-hazardous flag            |

It keeps only approaches to Earth, drops rows without an ID or date, and removes duplicates on
`(asteroid_id, approach_date)` so overlapping extractions don't double-count.

### Gold — `src/gold/*.sql` (Spark SQL)

| table               | grain                        | columns                                                                             |
|---------------------|------------------------------|-------------------------------------------------------------------------------------|
| `daily_neo_counts`  | one row per day              | `approach_date`, `asteroid_count`, `hazardous_count`, `hazardous_pct`               |
| `weekly_neo_counts` | one row per week (Mon start) | `week_start`, `days_covered`, `asteroid_count`, `hazardous_count`, `hazardous_pct`  |

`days_covered` is lower than 7 for weeks at the edges of the extracted date range, so partial
weeks are easy to spot. Counts use distinct asteroid IDs. `src/gold/build_gold.py` runs the SQL
files against the silver table and writes the results.

## Setup

1. Create a virtual environment and install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

2. Copy `.env.example` to `.env` and set your NASA API key (get one at
   https://api.nasa.gov/; `DEMO_KEY` works for small tests):

   ```bash
   cp .env.example .env
   ```

   The bronze script loads `.env` automatically.

3. Install **Java 17 or 21** (required by PySpark) and set `JAVA_HOME` to its install folder.

   On **Windows**, Spark also needs `winutils.exe` and `hadoop.dll`: put them in
   `C:\hadoop\bin` and set `HADOOP_HOME=C:\hadoop`.

## Running the pipeline

Run the layers in order from the project root:

```bash
python -m src.bronze.neo_extract --start 2026-01-01 --end 2026-01-14
python -m src.silver.neo_approaches
python -m src.gold.build_gold
```

`--start` and `--end` are optional (`YYYY-MM-DD`) and default to 2026-01-01 → 2026-08-31.
Silver and gold rebuild their tables from scratch on each run. The gold step also prints a
preview of both tables.

## Tests

```bash
pytest
```

The silver and gold tests start a local Spark session; they are skipped if Java isn't installed.

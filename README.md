# neo-medallion-pipeline

A data engineering pipeline for NASA's [Near Earth Object (NEO) API](https://api.nasa.gov/),
built around the **medallion architecture**:

- **Bronze** (`src/ingest/`) — raw data extraction. Fetches NEO feed data from the API
  in weekly windows and lands it as untouched JSON, partitioned by start date, under
  `data/bronze/`. *(Implemented.)*
- **Silver** (`src/transform/`) — cleaned, validated, and conformed data (PySpark).
  *(Coming later.)*
- **Gold** (`src/models/`) — business-level aggregates and models (SQL). *(Coming later.)*

Only the bronze layer is implemented so far.

## Setup

1. Create a virtual environment and install dependencies:

   ```bash
   pip install -r requirements.txt
   ```

2. Copy `.env.example` to `.env` and set your NASA API key (get one at
   https://api.nasa.gov/):

   ```bash
   cp .env.example .env
   ```

   Then export it into your shell environment, e.g.:

   ```bash
   export NASA_API_KEY=your_key_here
   ```

## Running the bronze extraction

```bash
python -m src.ingest.neo_extract --start 2026-01-01 --end 2026-01-14
```

`--start` and `--end` are optional (`YYYY-MM-DD`); they default to a preset range in
the script. Output lands in `data/bronze/start=<date>/neo_feed_<start>_<end>.json`.

## Tests

```bash
pytest
```

*(More details to come.)*

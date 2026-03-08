"""@bruin

# TODO: Set the asset name (recommended pattern: schema.asset_name).
# - Convention in this module: use an `ingestion.` schema for raw ingestion tables.
name: ingestion.trips

# TODO: Set the asset type.
# Docs: https://getbruin.com/docs/bruin/assets/python
type: python

# TODO: Pick a Python image version (Bruin runs Python in isolated environments).
# Example: python:3.11
image: python:3.11

# TODO: Set the connection.
connection: duckdb-default

# TODO: Choose materialization (optional, but recommended).
# Bruin feature: Python materialization lets you return a DataFrame (or list[dict]) and Bruin loads it into your destination.
# This is usually the easiest way to build ingestion assets in Bruin.
# Alternative (advanced): you can skip Bruin Python materialization and write a "plain" Python asset that manually writes
# into DuckDB (or another destination) using your own client library and SQL. In that case:
# - you typically omit the `materialization:` block
# - you do NOT need a `materialize()` function; you just run Python code
# Docs: https://getbruin.com/docs/bruin/assets/python#materialization
materialization:
  # TODO: choose `table` or `view` (ingestion generally should be a table)
  type: table
  # TODO: pick a strategy.
  # suggested strategy: append
  strategy: append

# TODO: Define output columns (names + types) for metadata, lineage, and quality checks.
# Tip: mark stable identifiers as `primary_key: true` if you plan to use `merge` later.
# Docs: https://getbruin.com/docs/bruin/assets/columns

@bruin"""

# TODO: Add imports needed for your ingestion (e.g., pandas, requests).
# - Put dependencies in the nearest `requirements.txt` (this template has one at the pipeline root).
# Docs: https://getbruin.com/docs/bruin/assets/python


# TODO: Only implement `materialize()` if you are using Bruin Python materialization.
# If you choose the manual-write approach (no `materialization:` block), remove this function and implement ingestion
# as a standard Python script instead.
def materialize():
    """
    TODO: Implement ingestion using Bruin runtime context.

    Required Bruin concepts to use here:
    - Built-in date window variables:
      - BRUIN_START_DATE / BRUIN_END_DATE (YYYY-MM-DD)
      - BRUIN_START_DATETIME / BRUIN_END_DATETIME (ISO datetime)
      Docs: https://getbruin.com/docs/bruin/assets/python#environment-variables
    - Pipeline variables:
      - Read JSON from BRUIN_VARS, e.g. `taxi_types`
      Docs: https://getbruin.com/docs/bruin/getting-started/pipeline-variables

    Design TODOs (keep logic minimal, focus on architecture):
    - Use start/end dates + `taxi_types` to generate a list of source endpoints for the run window.
    - Fetch data for each endpoint, parse into DataFrames, and concatenate.
    - Add a column like `extracted_at` for lineage/debugging (timestamp of extraction).
    - Prefer append-only in ingestion; handle duplicates in staging.
    """
    import os
    import json
    import tempfile
    import datetime
    from typing import List

    import pandas as pd
    import requests

    BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/"

    def _iter_months(start_date: str, end_date: str):
      # Yield (year, month) for each month where year-month >= start_date and < end_date
      start = datetime.datetime.strptime(start_date, "%Y-%m-%d").date().replace(day=1)
      end_exclusive = datetime.datetime.strptime(end_date, "%Y-%m-%d").date()
      cur = start
      while cur < end_exclusive:
        yield cur.year, cur.month
        # increment month
        if cur.month == 12:
          cur = cur.replace(year=cur.year + 1, month=1)
        else:
          cur = cur.replace(month=cur.month + 1)

    # Read pipeline/window variables from environment
    start_date = os.environ.get("BRUIN_START_DATE")
    end_date = os.environ.get("BRUIN_END_DATE")
    if not start_date or not end_date:
      raise RuntimeError("BRUIN_START_DATE and BRUIN_END_DATE must be provided in the environment")

    bruin_vars = os.environ.get("BRUIN_VARS", "{}")
    try:
      vars_json = json.loads(bruin_vars) if bruin_vars else {}
    except Exception:
      vars_json = {}

    taxi_types: List[str] = vars_json.get("taxi_types") or ["yellow"]

    dfs: List[pd.DataFrame] = []
    now_iso = datetime.datetime.utcnow().isoformat()

    for taxi in taxi_types:
      for year, month in _iter_months(start_date, end_date):
        fname = f"{taxi}_tripdata_{year}-{month:02d}.parquet"
        url = BASE_URL + fname
        try:
          resp = requests.get(url, timeout=30)
          if resp.status_code != 200:
            # skip missing months
            print(f"[ingest] skipping {url}: status {resp.status_code}")
            continue
          # write to a temp file, then let pandas read_parquet (works with pyarrow engine)
          with tempfile.NamedTemporaryFile(suffix=".parquet", delete=True) as tmp:
            tmp.write(resp.content)
            tmp.flush()
            df = pd.read_parquet(tmp.name)

          if not isinstance(df, pd.DataFrame):
            # safety: try to coerce
            df = pd.DataFrame(df)

          # Add lineage/debugging columns
          df["extracted_at"] = now_iso
          df["_source_file"] = fname
          dfs.append(df)
        except Exception as exc:  # pragma: no cover - network/IO related
          print(f"[ingest] failed to fetch {url}: {exc}")
          continue

    if not dfs:
      # return an empty dataframe (Bruin will create an empty table if needed)
      return pd.DataFrame()

    result = pd.concat(dfs, ignore_index=True, copy=False)
    return result



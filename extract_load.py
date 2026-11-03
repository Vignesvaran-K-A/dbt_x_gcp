"""
NYC 311 Data Ingestion Script (Extract & Load)

Pulls 311 Service Requests from the NYC Open Data (Socrata) API
and appends them to the BigQuery bronze layer (raw_nyc_311).

Supports:
- Command-line arguments (--start-date, --end-date, --days, --max-records, etc.)
- Environment variables (START_DATE, END_DATE, DAYS_BACK, MAX_RECORDS, SOCRATA_APP_TOKEN)
- Pagination for multi-page batch extractions
- Default fallback to yesterday's data
"""

import argparse
import os
from datetime import datetime, timedelta
import pandas as pd
import requests
from google.cloud import bigquery

SOCRATA_ENDPOINT = "https://data.cityofnewyork.us/resource/erm2-nwe9.json"
DEFAULT_PROJECT_ID = os.environ.get("GCP_PROJECT_ID", "nyc-311-pipeline-512717")
DEFAULT_TABLE_ID = f"{DEFAULT_PROJECT_ID}.bronze_layer.raw_nyc_311"
DEFAULT_BATCH_SIZE = 25000  # Socrata max is 50,000


def get_env_int(key: str, default: int | None = None) -> int | None:
    """Safely parse an integer from environment variables (ignoring empty strings)."""
    val = os.environ.get(key)
    if val and val.strip():
        try:
            return int(val.strip())
        except ValueError:
            pass
    return default


def parse_date(date_str: str | None, is_end_of_day: bool = False) -> str | None:
    """Format an input date string into Socrata SoQL ISO timestamp (YYYY-MM-DDTHH:MM:SS.000)."""
    if not date_str or not str(date_str).strip():
        return None
    cleaned = str(date_str).strip()
    dt = pd.to_datetime(cleaned)
    if is_end_of_day and dt.hour == 0 and dt.minute == 0 and dt.second == 0:
        dt = dt.replace(hour=23, minute=59, second=59)
    return dt.strftime("%Y-%m-%dT%H:%M:%S.000")


def build_where_clause(start_iso: str | None, end_iso: str | None) -> str:
    """Construct SoQL $where filter."""
    clauses = []
    if start_iso:
        clauses.append(f"created_date >= '{start_iso}'")
    if end_iso:
        clauses.append(f"created_date <= '{end_iso}'")
    return " AND ".join(clauses)


def fetch_data(
    start_date: str | None = None,
    end_date: str | None = None,
    days: int | None = None,
    max_records: int | None = None,
    batch_size: int = DEFAULT_BATCH_SIZE,
    app_token: str | None = None,
) -> pd.DataFrame:
    """
    Fetch 311 records from the Socrata API with pagination and optional date filtering.
    """
    now = datetime.now()

    # Resolve date boundaries
    if days is not None and days > 0:
        start_dt = now - timedelta(days=days)
        start_iso = start_dt.strftime("%Y-%m-%dT00:00:00.000")
        end_iso = now.strftime("%Y-%m-%dT%H:%M:%S.000")
    else:
        start_iso = parse_date(start_date, is_end_of_day=False)
        end_iso = parse_date(end_date, is_end_of_day=True)

        # If neither is provided, default to yesterday
        if not start_iso and not end_iso:
            yesterday = now - timedelta(days=1)
            start_iso = yesterday.strftime("%Y-%m-%dT00:00:00.000")

    where_clause = build_where_clause(start_iso, end_iso)

    headers = {}
    if app_token:
        headers["X-App-Token"] = app_token

    print("=" * 60)
    print("NYC 311 Data Extraction")
    print(f"  Where filter: {where_clause or 'ALL (no date filter)'}")
    print(f"  Max records:  {max_records if max_records else 'Unlimited'}")
    print(f"  Batch size:   {batch_size}")
    print("=" * 60)

    all_records = []
    offset = 0

    while True:
        current_limit = batch_size
        if max_records and (len(all_records) + current_limit > max_records):
            current_limit = max_records - len(all_records)

        if current_limit <= 0:
            break

        params = {
            "$order": "created_date ASC",
            "$limit": current_limit,
            "$offset": offset,
        }
        if where_clause:
            params["$where"] = where_clause

        print(f"Fetching batch: offset={offset}, limit={current_limit}...")
        response = requests.get(SOCRATA_ENDPOINT, params=params, headers=headers)
        response.raise_for_status()

        batch = response.json()
        if not batch or not isinstance(batch, list):
            print("No more records returned from API.")
            break

        all_records.extend(batch)
        print(f"  Received {len(batch)} records (Total fetched: {len(all_records)})")

        if len(batch) < current_limit:
            # Reached end of dataset for the given where clause
            break

        offset += len(batch)

        if max_records and len(all_records) >= max_records:
            print(f"Reached requested limit of {max_records} records.")
            break

    print(f"Extraction complete. Total records gathered: {len(all_records):,}")
    if not all_records:
        return pd.DataFrame()

    return pd.DataFrame(all_records)


def load_to_bigquery(df: pd.DataFrame, table_id: str = DEFAULT_TABLE_ID):
    """Append the extracted DataFrame to the BigQuery bronze layer table."""
    if df.empty:
        print("DataFrame is empty; skipping BigQuery load.")
        return

    print(f"Loading {len(df):,} records into BigQuery table `{table_id}`...")
    client = bigquery.Client()
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        autodetect=True,
    )

    df_str = df.astype(str)
    job = client.load_table_from_dataframe(df_str, table_id, job_config=job_config)
    job.result()
    print(f"Successfully loaded {len(df):,} rows into `{table_id}`.")


def main():
    parser = argparse.ArgumentParser(
        description="Extract NYC 311 service request data and load into BigQuery bronze layer."
    )
    parser.add_argument(
        "--start-date", "-s",
        type=str,
        default=os.environ.get("START_DATE"),
        help="Start date in YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS format",
    )
    parser.add_argument(
        "--end-date", "-e",
        type=str,
        default=os.environ.get("END_DATE"),
        help="End date in YYYY-MM-DD or YYYY-MM-DDTHH:MM:SS format",
    )
    parser.add_argument(
        "--days", "-d",
        type=int,
        default=get_env_int("DAYS_BACK"),
        help="Number of days back from today to fetch (e.g. 30)",
    )
    parser.add_argument(
        "--max-records", "-m",
        type=int,
        default=get_env_int("MAX_RECORDS", 50000),
        help="Maximum total records to fetch (default: 50000; set to 0 for unlimited)",
    )
    parser.add_argument(
        "--batch-size", "-b",
        type=int,
        default=DEFAULT_BATCH_SIZE,
        help=f"Batch size per API request (default: {DEFAULT_BATCH_SIZE})",
    )
    parser.add_argument(
        "--table",
        type=str,
        default=os.environ.get("BQ_TABLE_ID", DEFAULT_TABLE_ID),
        help=f"BigQuery destination table (default: {DEFAULT_TABLE_ID})",
    )
    parser.add_argument(
        "--app-token",
        type=str,
        default=os.environ.get("SOCRATA_APP_TOKEN"),
        help="Socrata Open Data App Token (optional)",
    )

    args = parser.parse_args()

    # 0 or negative means unlimited
    max_records = None if (args.max_records is None or args.max_records <= 0) else args.max_records

    df = fetch_data(
        start_date=args.start_date,
        end_date=args.end_date,
        days=args.days,
        max_records=max_records,
        batch_size=args.batch_size,
        app_token=args.app_token,
    )
    load_to_bigquery(df, table_id=args.table)


if __name__ == "__main__":
    main()

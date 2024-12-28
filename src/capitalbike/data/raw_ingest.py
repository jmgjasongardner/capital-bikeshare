from __future__ import annotations

import calendar
from datetime import datetime, timedelta
from io import BytesIO
import zipfile
import requests
import pandas as pd
import boto3


RAW_BUCKET = "capital-bikeshare-public"


def pull_and_write_from_cabi(year_month: str) -> None:
    """
    Download CaBi data for a YYYY or YYYYMM string, extract from zip,
    read CSV with pandas, convert to Parquet, and upload to S3.
    """
    url = f"https://s3.amazonaws.com/capitalbikeshare-data/{year_month}-capitalbikeshare-tripdata.zip"
    print(f"Downloading {url}")

    response = requests.get(url)
    if response.status_code != 200:
        print(f"Failed to download {url}")
        return

    zip_data = BytesIO(response.content)
    with zipfile.ZipFile(zip_data, "r") as zip_ref:
        file_list = zip_ref.namelist()
        # Always read the first file in the zip
        csv_name = file_list[0]
        df = pd.read_csv(
            zip_ref.open(csv_name),
            dtype={"start_station_id": "str", "end_station_id": "str"},
            low_memory=False,
        )

    # Clean station IDs
    df["start_station_id"] = pd.to_numeric(df["start_station_id"], errors="coerce")
    df["end_station_id"] = pd.to_numeric(df["end_station_id"], errors="coerce")
    df = df.dropna(subset=["start_station_id", "end_station_id"])

    # Convert station IDs back to strings for Parquet consistency
    df["start_station_id"] = df["start_station_id"].astype("Int64").astype("str")
    df["end_station_id"] = df["end_station_id"].astype("Int64").astype("str")

    out_key = f"{year_month}.parquet"
    print(f"Uploading cleaned data to s3://{RAW_BUCKET}/{out_key}")

    # Upload to S3
    s3 = boto3.client("s3")
    df.to_parquet(f"s3://{RAW_BUCKET}/{out_key}", index=False)

    print(f"✓ {year_month} written to S3")


def pull_missing_files(existing_keys: list[str]) -> None:
    """
    Check the last available YYYYMM in S3 and fetch the missing months until now.
    existing_keys = list of filenames currently in the bucket, e.g., ['201001.parquet', ...]
    """
    # Filter out non-year keys
    parquet_keys = [k for k in existing_keys if k.endswith(".parquet")]
    if not parquet_keys:
        raise RuntimeError("No existing parquet files found in bucket!")

    parquet_keys.sort()
    last_key = parquet_keys[-1].replace(".parquet", "")
    last_date = datetime.strptime(last_key, "%Y%m")

    today = datetime.now()

    while last_date.year < today.year or (last_date.year == today.year and last_date.month < today.month):
        print(f"Previously saved up through {last_date.strftime('%Y%m')} on S3")

        # Move forward one month
        days_in_month = calendar.monthrange(last_date.year, last_date.month)[1]
        last_date = last_date + timedelta(days=days_in_month)

        next_key = last_date.strftime("%Y%m")
        pull_and_write_from_cabi(next_key)

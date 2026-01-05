from __future__ import annotations

import calendar
from datetime import datetime, timedelta
from io import BytesIO
import zipfile

import boto3
import pandas as pd
import requests


CABI_ZIP_BASE_URL = "https://s3.amazonaws.com/capitalbikeshare-data"

DEFAULT_RAW_BUCKET = "capital-bikeshare-raw"
DEFAULT_RAW_PREFIX = "raw_monthly_parquet"


def _s3() -> boto3.client:
    return boto3.client("s3")


def download_month_zip(year_month: str) -> BytesIO:
    url = f"{CABI_ZIP_BASE_URL}/{year_month}-capitalbikeshare-tripdata.zip"
    resp = requests.get(url, timeout=120)
    resp.raise_for_status()
    return BytesIO(resp.content)


def extract_first_csv_from_zip(zip_buf: BytesIO) -> BytesIO:
    zip_buf.seek(0)
    with zipfile.ZipFile(zip_buf, "r") as zf:
        names = zf.namelist()
        csv_names = [n for n in names if n.lower().endswith(".csv")]
        target = csv_names[0] if csv_names else names[0]
        with zf.open(target) as f:
            return BytesIO(f.read())


def month_to_key(year_month: str) -> str:
    return f"{DEFAULT_RAW_PREFIX}/{year_month}.parquet"


def write_month_parquet_to_s3(
    year_month: str, bucket: str = DEFAULT_RAW_BUCKET
) -> None:
    zip_buf = download_month_zip(year_month)
    csv_buf = extract_first_csv_from_zip(zip_buf)

    df = pd.read_csv(csv_buf)

    out = BytesIO()
    df.to_parquet(out, index=False)
    out.seek(0)

    key = month_to_key(year_month)
    _s3().put_object(Bucket=bucket, Key=key, Body=out.getvalue())
    print(f"Wrote s3://{bucket}/{key} (rows={len(df):,})")


def backfill_from(start_from: str, bucket: str = DEFAULT_RAW_BUCKET) -> None:
    dt = datetime.strptime(start_from, "%Y%m")
    today = datetime.now()

    while dt.year < today.year or (dt.year == today.year and dt.month <= today.month):
        ym = dt.strftime("%Y%m")
        write_month_parquet_to_s3(ym, bucket=bucket)

        days_in_month = calendar.monthrange(dt.year, dt.month)[1]
        dt = dt + timedelta(days=days_in_month)

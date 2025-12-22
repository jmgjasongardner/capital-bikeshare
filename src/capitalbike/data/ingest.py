from __future__ import annotations

import os
from io import BytesIO
from typing import List, Set, Tuple

import boto3
import polars as pl
from dotenv import load_dotenv

from src.capitalbike.data.transform import normalize_trip_schema
from src.capitalbike.data.stations import load_stations, make_station_lookups

load_dotenv()

RAW_BUCKET = os.getenv("S3_BUCKET_RAW", "capital-bikeshare-public")
PROC_BUCKET = os.getenv("S3_BUCKET_PROCESSED", "capital-bikeshare-manipulated")
STATIONS_KEY = os.getenv("STATIONS_KEY", "bike_stations.parquet")
TRIPS_PREFIX = "trips"


# ---------- S3 helpers ----------


def _get_s3_client():
    return boto3.client("s3")


def _list_parquet_keys(bucket: str) -> List[str]:
    s3 = _get_s3_client()
    keys: List[str] = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".parquet"):
                keys.append(key)
    keys.sort()
    return keys


def _read_parquet_from_s3(bucket: str, key: str) -> pl.DataFrame:
    s3 = _get_s3_client()
    buf = BytesIO()
    s3.download_fileobj(bucket, key, buf)
    buf.seek(0)
    return pl.read_parquet(buf)


def _write_parquet_to_s3(df: pl.DataFrame, bucket: str, key: str) -> None:
    s3 = _get_s3_client()
    buf = BytesIO()
    df.write_parquet(buf, compression="zstd")
    buf.seek(0)
    s3.upload_fileobj(buf, bucket, key)


# ---------- Partition helpers ----------


def _existing_partitions() -> Set[Tuple[int, int]]:
    """
    Return set of (year, month) already present in trips/.
    """
    s3 = _get_s3_client()
    paginator = s3.get_paginator("list_objects_v2")

    out: Set[Tuple[int, int]] = set()

    for page in paginator.paginate(Bucket=PROC_BUCKET, Prefix=f"{TRIPS_PREFIX}/"):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if "/year=" in key and "/month=" in key:
                try:
                    year = int(key.split("year=")[1].split("/")[0])
                    month = int(key.split("month=")[1].split("/")[0])
                    out.add((year, month))
                except ValueError:
                    pass

    return out


def _raw_key_to_year_months(key: str) -> Set[Tuple[int, int]]:
    """
    Map raw parquet filename to the (year, month) values it contains.
    """
    stem = os.path.splitext(os.path.basename(key))[0]

    # YYYYMM
    if len(stem) == 6:
        return {(int(stem[:4]), int(stem[4:]))}

    # YYYY
    if len(stem) == 4:
        year = int(stem)
        return {(year, m) for m in range(1, 13)}

    return set()


# ---------- Core ETL ----------


def build_master_table(*, missing_months: bool = True) -> None:
    """
    Build the partitioned master trips table.

    missing_months=True (default):
        Only process (year, month) not already present in trips/.

    missing_months=False:
        Rebuild everything (overwrite behavior).
    """
    print(f"Loading station metadata from s3://{PROC_BUCKET}/{STATIONS_KEY}...")
    stations = load_stations()
    stations_start, stations_end = make_station_lookups(stations)

    processed_partitions: Set[Tuple[int, int]] = set()
    if missing_months:
        processed_partitions = _existing_partitions()
        print(f"Found {len(processed_partitions)} existing partitions")

    print(f"Listing raw parquet files in s3://{RAW_BUCKET}/...")
    raw_keys = _list_parquet_keys(RAW_BUCKET)
    if not raw_keys:
        raise RuntimeError(f"No parquet files found in bucket {RAW_BUCKET!r}")

    for key in raw_keys:
        candidate_months = _raw_key_to_year_months(key)

        if missing_months:
            candidate_months = candidate_months - processed_partitions
            if not candidate_months:
                print(f"Skipping {key} (no missing months)")
                continue

        print(f"\nProcessing raw file: s3://{RAW_BUCKET}/{key}")
        df = _read_parquet_from_s3(RAW_BUCKET, key)

        df = normalize_trip_schema(df)

        df = df.filter(pl.col("start_time").is_not_null())

        df = df.with_columns(
            [
                pl.col("start_time").dt.year().alias("year"),
                pl.col("start_time").dt.month().alias("month"),
            ]
        )

        if missing_months:
            df = df.filter(
                pl.struct(["year", "month"]).is_in(list(candidate_months))
            )

        # Drop any station cols before join (station table is authoritative)
        df = df.drop(
            [
                "start_station_name",
                "start_lat",
                "start_lng",
                "end_station_name",
                "end_lat",
                "end_lng",
            ],
            strict=False,
        )

        df = df.join(stations_start, on="start_station_id", how="left")
        df = df.join(stations_end, on="end_station_id", how="left")

        parts = df.partition_by(["year", "month"], maintain_order=False)

        for part in parts:
            if part.height == 0:
                continue

            year = int(part["year"][0])
            month = int(part["month"][0])

            part = part.drop(["year", "month"])

            base_name = os.path.splitext(os.path.basename(key))[0]
            out_key = (
                f"{TRIPS_PREFIX}/year={year:04d}/month={month:02d}/{base_name}.parquet"
            )

            print(
                f"  → Writing s3://{PROC_BUCKET}/{out_key} "
                f"(rows={part.height})"
            )
            _write_parquet_to_s3(part, PROC_BUCKET, out_key)

    print("Master partitioned trips table is up to date.")


def main() -> None:
    build_master_table(missing_months=True)


if __name__ == "__main__":
    main()

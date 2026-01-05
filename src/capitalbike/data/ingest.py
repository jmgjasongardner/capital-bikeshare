from __future__ import annotations

import os
from io import BytesIO
from typing import Set, Tuple

import boto3
import polars as pl

from src.capitalbike.data.transform import normalize_trip_schema
from src.capitalbike.data.stations import build_station_dimension


RAW_BUCKET = os.getenv("S3_BUCKET_RAW", "capital-bikeshare-public")
RAW_PREFIX = os.getenv("S3_PREFIX_RAW_MONTHLY", "")  # "" if monthly CSVs at bucket root

PROC_BUCKET = os.getenv("S3_BUCKET_PROCESSED", "capital-bikeshare-manipulated")
MASTER_PREFIX = os.getenv("S3_PREFIX_MASTER", "master/trips")  # partitioned target
STATIONS_KEY = os.getenv("S3_KEY_STATIONS", "dimensions/stations.parquet")


# --------------------------------------------------
# S3 helpers
# --------------------------------------------------
def _s3() -> boto3.client:
    return boto3.client("s3")


def _parse_s3_uri(uri: str) -> Tuple[str, str]:
    if not uri.startswith("s3://"):
        raise ValueError(f"Expected s3:// URI, got: {uri}")
    no_scheme = uri.replace("s3://", "", 1)
    bucket, key = no_scheme.split("/", 1)
    return bucket, key


def _write_parquet_to_s3(df: pl.DataFrame, s3_uri: str) -> None:
    bucket, key = _parse_s3_uri(s3_uri)
    buf = BytesIO()
    df.write_parquet(buf)
    buf.seek(0)
    _s3().put_object(Bucket=bucket, Key=key, Body=buf.getvalue())


def _list_keys(bucket: str, prefix: str) -> list[str]:
    s3 = _s3()
    keys: list[str] = []
    token = None
    while True:
        kwargs = {"Bucket": bucket, "Prefix": prefix}
        if token:
            kwargs["ContinuationToken"] = token
        resp = s3.list_objects_v2(**kwargs)
        for obj in resp.get("Contents", []):
            keys.append(obj["Key"])
        if resp.get("IsTruncated"):
            token = resp.get("NextContinuationToken")
        else:
            break
    return keys


def _existing_master_partitions() -> Set[Tuple[int, int]]:
    """Return {(year, month)} already written under MASTER_PREFIX."""
    prefix = f"{MASTER_PREFIX}/"
    keys = _list_keys(PROC_BUCKET, prefix)
    parts: Set[Tuple[int, int]] = set()

    for k in keys:
        # master/trips/year=2024/month=12/part.parquet
        if "year=" in k and "month=" in k and k.endswith(".parquet"):
            try:
                year = int(k.split("year=")[1].split("/")[0])
                month = int(k.split("month=")[1].split("/")[0])
                parts.add((year, month))
            except Exception:
                continue
    return parts


# --------------------------------------------------
# Public API
# --------------------------------------------------
def build_master_table(stations: pl.DataFrame, missing_months: bool = True) -> None:
    """
    Build (or update) the partitioned master trips table and station dimension.

    Handles:
    - Monthly files: YYYYMM.(csv|parquet)
    - Yearly bulk files: YYYY.(parquet)

    All data are written as monthly partitions:
      master/trips/year=YYYY/month=MM/part.parquet
    """

    raw_keys = [
        k
        for k in _list_keys(RAW_BUCKET, RAW_PREFIX)
        if k.endswith((".csv", ".parquet"))
    ]

    # --------------------------------------------
    # Determine file type from filename
    # --------------------------------------------
    def _key_kind(key: str) -> tuple[str, int | None, int | None]:
        """
        Returns:
          ("monthly", year, month)
          ("bulk", year, None)
          ("ignore", None, None)
        """
        name = key.split("/")[-1]
        stem = name.replace(".csv", "").replace(".parquet", "")

        if stem.isdigit() and len(stem) == 6:
            return "monthly", int(stem[:4]), int(stem[4:6])

        if stem.isdigit() and len(stem) == 4:
            return "bulk", int(stem), None

        return "ignore", None, None

    already = _existing_master_partitions() if missing_months else set()

    # --------------------------------------------
    # Process each raw file
    # --------------------------------------------
    for key in sorted(raw_keys):
        kind, year, month = _key_kind(key)

        if kind == "ignore":
            continue

        s3_path = f"s3://{RAW_BUCKET}/{key}"
        print(f"Processing {s3_path}")

        df_raw = (
            pl.read_parquet(s3_path)
            if key.endswith(".parquet")
            else pl.read_csv(s3_path, ignore_errors=True)
        )

        df = normalize_trip_schema(df_raw, stations)

        # ----------------------------------------
        # Monthly file (YYYYMM)
        # ----------------------------------------
        if kind == "monthly":
            if missing_months and (year, month) in already:
                print(f"Skipping existing {year}-{month:02d}")
                continue

            out_uri = (
                f"s3://{PROC_BUCKET}/{MASTER_PREFIX}/"
                f"year={year}/month={month}/part.parquet"
            )

            _write_parquet_to_s3(df, out_uri)
            print(f"Wrote {out_uri} (rows={df.height:,})")

        # ----------------------------------------
        # Yearly bulk file (YYYY) → split by month
        # ----------------------------------------
        else:  # kind == "bulk"
            df = df.with_columns(
                [
                    pl.col("started_at").dt.year().alias("_year"),
                    pl.col("started_at").dt.month().alias("_month"),
                ]
            )

            partitions = df.partition_by(["_year", "_month"], as_dict=True)

            for (y, m), subdf in sorted(partitions.items()):
                if missing_months and (y, m) in already:
                    print(f"Skipping existing {y}-{m:02d}")
                    continue

                out_uri = (
                    f"s3://{PROC_BUCKET}/{MASTER_PREFIX}/"
                    f"year={y}/month={m}/part.parquet"
                )

                _write_parquet_to_s3(
                    subdf.drop(["_year", "_month"]),
                    out_uri,
                )

                print(f"Wrote {out_uri} (rows={subdf.height:,})")

    # --------------------------------------------
    # Rebuild stations from authoritative master
    # --------------------------------------------
    print("Rebuilding station dimension from master trips...")

    trips_lf = pl.scan_parquet(
        f"s3://{PROC_BUCKET}/{MASTER_PREFIX}/year=*/month=*/part.parquet"
    )

    stations_new = build_station_dimension(trips_lf)

    _write_parquet_to_s3(
        stations_new,
        f"s3://{PROC_BUCKET}/{STATIONS_KEY}",
    )

    print(f"Stations written to s3://{PROC_BUCKET}/{STATIONS_KEY}")

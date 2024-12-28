from __future__ import annotations

import os
from io import BytesIO
from typing import Tuple

import boto3
import polars as pl
from dotenv import load_dotenv

load_dotenv()

PROC_BUCKET = os.getenv("S3_BUCKET_PROCESSED", "capital-bikeshare-manipulated")
STATIONS_KEY = os.getenv("STATIONS_KEY", "bike_stations.parquet")


def _get_s3_client():
    return boto3.client("s3")


def load_stations() -> pl.DataFrame:
    """
    Load station metadata from S3 into a Polars DataFrame.

    Expected columns (based on your file):
      - start_station_id
      - start_station_name
      - start_lat
      - start_lng
      - earliest  (optional metadata)
    """
    s3 = _get_s3_client()
    buf = BytesIO()
    s3.download_fileobj(PROC_BUCKET, STATIONS_KEY, buf)
    buf.seek(0)

    stations = pl.read_parquet(buf)

    required = {"start_station_id", "start_lat", "start_lng"}
    missing = required - set(stations.columns)
    if missing:
        raise ValueError(
            f"Station file missing required columns: {missing}. Got: {stations.columns}"
        )

    stations = stations.with_columns(
        pl.col("start_station_id").cast(pl.Utf8)
    )

    return stations


def make_station_lookups(
    stations: pl.DataFrame,
) -> Tuple[pl.DataFrame, pl.DataFrame]:
    """
    Create station lookup tables for joining onto trips.

    Returns:
      - stations_start: columns for joining on start_station_id
      - stations_end: same info but with end_* column names
    """
    # Start lookup
    stations_start = stations.select(
        [
            pl.col("start_station_id").cast(pl.Utf8),
            *[
                c
                for c in stations.columns
                if c in ("start_station_name", "start_lat", "start_lng")
            ],
        ]
    )

    # End lookup is just a renamed copy
    rename_map = {
        "start_station_id": "end_station_id",
        "start_station_name": "end_station_name",
        "start_lat": "end_lat",
        "start_lng": "end_lng",
    }

    stations_end = stations_start.rename(
        {k: v for k, v in rename_map.items() if k in stations_start.columns}
    )

    return stations_start, stations_end

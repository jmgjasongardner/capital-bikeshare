from __future__ import annotations

import os
from io import BytesIO
from typing import Tuple

import boto3
import polars as pl


PROC_BUCKET = os.getenv("S3_BUCKET_PROCESSED", "capital-bikeshare-manipulated")
MASTER_PREFIX = os.getenv("S3_PREFIX_MASTER", "master/trips")
STATIONS_KEY = os.getenv("S3_KEY_STATIONS", "dimensions/stations.parquet")
AGG_PREFIX = os.getenv("S3_PREFIX_AGG", "aggregates")


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


def _trips_scan() -> pl.LazyFrame:
    return pl.scan_parquet(
        f"s3://{PROC_BUCKET}/{MASTER_PREFIX}/year=*/month=*/part.parquet"
    )


def _stations_scan() -> pl.LazyFrame:
    return pl.scan_parquet(f"s3://{PROC_BUCKET}/{STATIONS_KEY}")


# --------------------------------------------------
# Aggregates
# --------------------------------------------------
def build_system_daily() -> None:
    trips = _trips_scan()

    out = (
        trips.with_columns(pl.col("started_at").dt.date().alias("date"))
        .group_by("date")
        .agg(
            [
                pl.len().alias("trips"),
                pl.col("duration_sec").mean().alias("avg_duration_sec"),
            ]
        )
        .sort("date")
        .collect()
    )

    out_uri = f"s3://{PROC_BUCKET}/{AGG_PREFIX}/system_daily.parquet"
    _write_parquet_to_s3(out, out_uri)
    print(f"System_daily written to {out_uri}")


def build_station_daily(sample: bool = False) -> None:
    trips = _trips_scan()
    stations = _stations_scan()

    base = (
        trips.with_columns(pl.col("started_at").dt.date().alias("date"))
        .group_by(["start_station_id", "date"])
        .agg(
            [
                pl.len().alias("num_checkouts"),
                pl.col("duration_sec").mean().alias("avg_duration_sec"),
            ]
        )
        .rename({"start_station_id": "station_id"})
    )

    out = (
        base.join(stations, on="station_id", how="left")
        .select(
            [
                "date",
                "station_id",
                "station_name",
                "lat",
                "lng",
                "num_checkouts",
                "avg_duration_sec",
            ]
        )
        .sort(["date", "station_id"])
        .collect()
    )

    if sample:
        out = out.sample(n=min(200_000, out.height), seed=42)

    name = "station_daily_sample" if sample else "station_daily"
    out_uri = f"s3://{PROC_BUCKET}/{AGG_PREFIX}/{name}.parquet"
    _write_parquet_to_s3(out, out_uri)
    print(f"{name} written to {out_uri}")


def build_station_hourly() -> None:
    trips = _trips_scan()
    stations = _stations_scan()

    base = (
        trips.with_columns(
            [
                pl.col("started_at").dt.date().alias("date"),
                pl.col("started_at").dt.hour().alias("hour"),
            ]
        )
        .group_by(["start_station_id", "date", "hour"])
        .agg(pl.len().alias("num_checkouts"))
        .rename({"start_station_id": "station_id"})
    )

    out = (
        base.join(
            stations.select(["station_id", "station_name", "lat", "lng"]),
            on="station_id",
            how="left",
        )
        .select(
            [
                "date",
                "hour",
                "station_id",
                "station_name",
                "lat",
                "lng",
                "num_checkouts",
            ]
        )
        .sort(["date", "hour", "station_id"])
        .collect()
    )

    out_uri = f"s3://{PROC_BUCKET}/{AGG_PREFIX}/station_hourly.parquet"
    _write_parquet_to_s3(out, out_uri)
    print(f"Station_hourly written to {out_uri}")


def build_all_summaries() -> None:
    build_system_daily()
    build_station_daily(sample=False)
    build_station_daily(sample=False)
    build_station_hourly()


if __name__ == "__main__":
    build_all_summaries()

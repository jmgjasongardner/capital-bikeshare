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
        trips
        .filter(pl.col("duration_sec") > 0)  # Filter out negative/zero durations
        .with_columns(pl.col("started_at").dt.date().alias("date"))
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

    # Aggregate checkouts (trips starting from this station)
    checkouts = (
        trips
        .filter(pl.col("duration_sec") > 0)  # Filter out negative/zero durations
        .with_columns(pl.col("started_at").dt.date().alias("date"))
        .group_by(["start_station_id", "date"])
        .agg(
            [
                pl.len().alias("num_checkouts"),
                pl.col("duration_sec").mean().alias("avg_duration_sec"),
                pl.col("bike_number").n_unique().alias("distinct_bikes_out"),
            ]
        )
        .rename({"start_station_id": "station_id"})
    )

    # Aggregate returns (trips ending at this station)
    returns = (
        trips.with_columns(pl.col("started_at").dt.date().alias("date"))
        .group_by(["end_station_id", "date"])
        .agg(pl.len().alias("num_returns"))
        .rename({"end_station_id": "station_id"})
    )

    # Join checkouts and returns
    base = checkouts.join(
        returns, on=["station_id", "date"], how="outer_coalesce", coalesce=True
    ).with_columns(
        [
            pl.col("num_checkouts").fill_null(0),
            pl.col("num_returns").fill_null(0),
            (pl.col("num_checkouts") - pl.col("num_returns")).alias("net_flow"),
        ]
    )

    out = (
        base.join(stations, on="station_id", how="left", coalesce=True)
        .select(
            [
                "date",
                "station_id",
                "station_name",
                "lat",
                "lng",
                "num_checkouts",
                "avg_duration_sec",
                "distinct_bikes_out",
                "num_returns",
                "net_flow",
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


def build_station_routes() -> None:
    """
    Build popular routes aggregate showing top station-to-station pairs.

    This creates a lightweight aggregate of the most popular routes for
    the "Popular Routes" feature in the StationExplorer page.
    """
    trips = _trips_scan()
    stations = _stations_scan()

    print("Aggregating routes by start/end station pairs...")

    # Aggregate trips by start/end station pair
    routes = (
        trips.group_by(["start_station_id", "end_station_id"])
        .agg(
            [
                pl.len().alias("trip_count"),
                pl.col("duration_sec").mean().alias("avg_duration_sec"),
            ]
        )
        .filter(
            # Filter out round trips (same start/end)
            (pl.col("start_station_id") != pl.col("end_station_id"))
            &
            # Filter noise (require at least 10 trips)
            (pl.col("trip_count") >= 10)
        )
        .sort("trip_count", descending=True)
        .head(10_000)  # Keep top 10K routes only
    )

    print("Joining station metadata...")

    # Join start station info
    routes_with_start = routes.join(
        stations.select([
            pl.col("station_id").alias("start_station_id"),
            pl.col("station_name").alias("start_station_name"),
            pl.col("lat").alias("start_lat"),
            pl.col("lng").alias("start_lng"),
        ]),
        on="start_station_id",
        how="left",
    )

    # Join end station info
    out = (
        routes_with_start.join(
            stations.select([
                pl.col("station_id").alias("end_station_id"),
                pl.col("station_name").alias("end_station_name"),
                pl.col("lat").alias("end_lat"),
                pl.col("lng").alias("end_lng"),
            ]),
            on="end_station_id",
            how="left",
        )
        .select([
            "start_station_id",
            "start_station_name",
            "start_lat",
            "start_lng",
            "end_station_id",
            "end_station_name",
            "end_lat",
            "end_lng",
            "trip_count",
            "avg_duration_sec",
        ])
        .sort("trip_count", descending=True)
        .collect()
    )

    out_uri = f"s3://{PROC_BUCKET}/{AGG_PREFIX}/station_routes.parquet"
    _write_parquet_to_s3(out, out_uri)
    print(f"Station_routes written to {out_uri} ({len(out):,} routes)")


def build_all_summaries() -> None:
    """Build all summary/aggregate tables from master trips data."""
    print("Building all summary tables...")
    print("=" * 60)

    build_system_daily()
    print()

    build_station_daily(sample=False)
    print()

    build_station_daily(sample=True)
    print()

    build_station_hourly()
    print()

    build_station_routes()
    print()

    print("=" * 60)
    print("All summaries built successfully!")


if __name__ == "__main__":
    build_all_summaries()

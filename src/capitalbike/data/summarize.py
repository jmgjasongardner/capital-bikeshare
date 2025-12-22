from __future__ import annotations

import os

import polars as pl
from dotenv import load_dotenv
from src.capitalbike.data.ingest import _write_parquet_to_s3

load_dotenv()

PROC_BUCKET = os.getenv("S3_BUCKET_PROCESSED", "capital-bikeshare-manipulated")


def _trips_scan() -> pl.LazyFrame:
    """
    Lazy scan of the partitioned master trips table on S3.
    """
    path = f"s3://{PROC_BUCKET}/trips/year=*/month=*/*.parquet"
    return pl.scan_parquet(path)


def build_station_daily() -> None:
    """
    Build daily station summary and write to S3:

      s3://{PROC_BUCKET}/station_daily.parquet
    """
    trips = _trips_scan()

    trips_with_date = trips.with_columns(
        pl.date("year", "month", "day").alias("date")
    )

    # Checkouts (starts)
    checkouts = (
        trips_with_date
        .group_by(["date", "start_station_id"])
        .agg(
            pl.len().alias("num_checkouts"),
            pl.mean("duration_sec").alias("avg_duration_sec"),
            pl.n_unique("bike_number").alias("distinct_bikes_out"),
        )
        .rename({"start_station_id": "station_id"})
    )

    # Returns (ends)
    returns = (
        trips_with_date
        .group_by(["date", "end_station_id"])
        .agg(
            pl.len().alias("num_returns"),
        )
        .rename({"end_station_id": "station_id"})
    )

    station_daily = (
        checkouts.join(
            returns,
            on=["date", "station_id"],
            how="outer_coalesce",
        )
        .with_columns(
            pl.col("num_checkouts").fill_null(0),
            pl.col("num_returns").fill_null(0),
        )
        .with_columns(
            (pl.col("num_checkouts") - pl.col("num_returns")).alias("net_flow")
        )
        .sort(["date", "station_id"])
    )

    df = station_daily.collect()
    out_path = f"s3://{PROC_BUCKET}/station_daily.parquet"
    _write_parquet_to_s3(
        df,
        PROC_BUCKET,
        "station_daily.parquet",
    )
    print(f"✅ station_daily written to {out_path}")


def build_station_hourly() -> None:
    """
    Build hourly station summary and write to S3:

      s3://{PROC_BUCKET}/station_hourly.parquet
    """
    trips = _trips_scan()

    trips_with_date_hour = trips.with_columns(
        [
            pl.date("year", "month", "day").alias("date"),
            pl.col("hour"),
        ]
    )

    checkouts = (
        trips_with_date_hour
        .group_by(["date", "hour", "start_station_id"])
        .agg(pl.len().alias("num_checkouts"))
        .rename({"start_station_id": "station_id"})
    )

    returns = (
        trips_with_date_hour
        .group_by(["date", "hour", "end_station_id"])
        .agg(pl.len().alias("num_returns"))
        .rename({"end_station_id": "station_id"})
    )

    station_hourly = (
        checkouts.join(
            returns,
            on=["date", "hour", "station_id"],
            how="outer_coalesce",
        )
        .with_columns(
            pl.col("num_checkouts").fill_null(0),
            pl.col("num_returns").fill_null(0),
        )
        .with_columns(
            (pl.col("num_checkouts") - pl.col("num_returns")).alias("net_flow")
        )
        .sort(["date", "hour", "station_id"])
    )

    df = station_hourly.collect()
    out_path = f"s3://{PROC_BUCKET}/station_hourly.parquet"
    _write_parquet_to_s3(
        df,
        PROC_BUCKET,
        "station_hourly.parquet",
    )
    print(f"✅ station_hourly written to {out_path}")


def build_all_summaries() -> None:
    build_station_daily()
    build_station_hourly()


def main() -> None:
    build_all_summaries()


if __name__ == "__main__":
    main()

from __future__ import annotations

import logging
import os
from io import BytesIO
from typing import Tuple

import boto3
import polars as pl
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure logging
logger = logging.getLogger(__name__)


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
        f"s3://{PROC_BUCKET}/{MASTER_PREFIX}/year=*/month=*/part.parquet",
        storage_options={
            "aws_region": os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
        }
    )


def _stations_scan() -> pl.LazyFrame:
    return pl.scan_parquet(
        f"s3://{PROC_BUCKET}/{STATIONS_KEY}",
        storage_options={
            "aws_region": os.getenv("AWS_DEFAULT_REGION", "us-east-1"),
        }
    )


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
    logger.info(f"System_daily written to {out_uri}")


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
        returns, on=["station_id", "date"], how="full", coalesce=True
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
    logger.info(f"{name} written to {out_uri}")


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
            coalesce=True,
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
    logger.info(f"Station_hourly written to {out_uri}")


def build_station_routes() -> None:
    """
    Build popular routes aggregate showing top station-to-station pairs.

    This creates a lightweight aggregate of the most popular routes for
    the "Popular Routes" feature in the StationExplorer page.
    """
    trips = _trips_scan()
    stations = _stations_scan()

    logger.info("Aggregating routes by start/end station pairs...")

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

    logger.info("Joining station metadata...")

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
        coalesce=True,
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
            coalesce=True,
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
    logger.info(f"Station_routes written to {out_uri} ({len(out):,} routes)")


def build_station_daily_detailed() -> None:
    """
    Build detailed station daily aggregates with member_type and rideable_type dimensions.

    This replaces the need to query raw trip data for filtered views, providing
    instant performance for member/bike type filtering in the Streamlit app.

    Includes separate duration averages for checkouts vs returns.
    """
    trips = _trips_scan()
    stations = _stations_scan()

    logger.info("Building station_daily_detailed with member_type and rideable_type dimensions...")

    # Ensure columns exist for pre-2020 data (fill nulls with "unknown")
    trips = trips.with_columns([
        pl.when(pl.col("member_type").is_null())
        .then(pl.lit("unknown"))
        .otherwise(pl.col("member_type"))
        .alias("member_type"),

        pl.when(pl.col("rideable_type").is_null())
        .then(pl.lit("unknown"))
        .otherwise(pl.col("rideable_type"))
        .alias("rideable_type"),
    ])

    # Aggregate checkouts (trips starting from this station)
    checkouts = (
        trips
        .filter(pl.col("duration_sec") > 0)
        .with_columns(pl.col("started_at").dt.date().alias("date"))
        .group_by(["start_station_id", "date", "member_type", "rideable_type"])
        .agg([
            pl.len().alias("num_checkouts"),
            pl.col("duration_sec").mean().alias("avg_duration_checkout_sec"),
            pl.col("bike_number").n_unique().alias("distinct_bikes_out"),
        ])
        .rename({"start_station_id": "station_id"})
    )

    # Aggregate returns (trips ending at this station)
    returns = (
        trips
        .filter(pl.col("duration_sec") > 0)
        .with_columns(pl.col("started_at").dt.date().alias("date"))
        .group_by(["end_station_id", "date", "member_type", "rideable_type"])
        .agg([
            pl.len().alias("num_returns"),
            pl.col("duration_sec").mean().alias("avg_duration_return_sec"),
        ])
        .rename({"end_station_id": "station_id"})
    )

    # Full outer join to preserve all station-date-member-rideable combinations
    base = checkouts.join(
        returns,
        on=["station_id", "date", "member_type", "rideable_type"],
        how="full",
        coalesce=True
    ).with_columns([
        pl.col("num_checkouts").fill_null(0),
        pl.col("num_returns").fill_null(0),
        (pl.col("num_checkouts") - pl.col("num_returns")).alias("net_flow"),
    ])

    # Join station metadata
    out = (
        base.join(stations, on="station_id", how="left", coalesce=True)
        .select([
            "date",
            "station_id",
            "station_name",
            "lat",
            "lng",
            "member_type",
            "rideable_type",
            "num_checkouts",
            "num_returns",
            "avg_duration_checkout_sec",
            "avg_duration_return_sec",
            "distinct_bikes_out",
            "net_flow",
        ])
        .sort(["date", "station_id", "member_type", "rideable_type"])
        .collect()
    )

    out_uri = f"s3://{PROC_BUCKET}/{AGG_PREFIX}/station_daily_detailed.parquet"
    _write_parquet_to_s3(out, out_uri)
    logger.info(f"station_daily_detailed written to {out_uri} ({len(out):,} rows)")


def build_system_daily_detailed() -> None:
    """
    Build system-wide daily aggregates with member_type and rideable_type dimensions.

    Provides fast filtering for system-level trends by member and bike type.
    """
    trips = _trips_scan()

    logger.info("Building system_daily_detailed with member_type and rideable_type dimensions...")

    # Ensure columns exist for pre-2020 data
    trips = trips.with_columns([
        pl.when(pl.col("member_type").is_null())
        .then(pl.lit("unknown"))
        .otherwise(pl.col("member_type"))
        .alias("member_type"),

        pl.when(pl.col("rideable_type").is_null())
        .then(pl.lit("unknown"))
        .otherwise(pl.col("rideable_type"))
        .alias("rideable_type"),
    ])

    out = (
        trips
        .filter(pl.col("duration_sec") > 0)
        .with_columns(pl.col("started_at").dt.date().alias("date"))
        .group_by(["date", "member_type", "rideable_type"])
        .agg([
            pl.len().alias("trips"),
            pl.col("duration_sec").mean().alias("avg_duration_sec"),
        ])
        .sort(["date", "member_type", "rideable_type"])
        .collect()
    )

    out_uri = f"s3://{PROC_BUCKET}/{AGG_PREFIX}/system_daily_detailed.parquet"
    _write_parquet_to_s3(out, out_uri)
    logger.info(f"system_daily_detailed written to {out_uri} ({len(out):,} rows)")


def build_time_aggregated() -> None:
    """
    Build time-based aggregates for the Time Aggregation page.

    Creates aggregates at 4 levels: day, day_of_week, month, year.
    Each level can be filtered by member_type and rideable_type.
    Includes separate counts for checkouts, returns, and net flow.
    """
    trips = _trips_scan()

    logger.info("Building time_aggregated with day/week/month/year dimensions...")

    # Ensure columns exist for pre-2020 data
    trips_filtered = trips.with_columns([
        pl.when(pl.col("member_type").is_null())
        .then(pl.lit("unknown"))
        .otherwise(pl.col("member_type"))
        .alias("member_type"),

        pl.when(pl.col("rideable_type").is_null())
        .then(pl.lit("unknown"))
        .otherwise(pl.col("rideable_type"))
        .alias("rideable_type"),
    ]).filter(pl.col("duration_sec") > 0)

    # Day level aggregation
    logger.info("  - Aggregating by day...")
    day_agg = (
        trips_filtered
        .with_columns(pl.col("started_at").dt.date().alias("agg_value_date"))
        .group_by(["agg_value_date", "member_type", "rideable_type"])
        .agg([
            pl.len().alias("total_trips"),
            pl.col("duration_sec").mean().alias("avg_duration_sec"),
        ])
        .with_columns([
            pl.lit("day").alias("agg_level"),
            pl.col("agg_value_date").cast(pl.Utf8).alias("agg_value"),
            pl.col("agg_value_date").cast(pl.Int32).alias("agg_sort_key"),
            pl.col("total_trips").alias("total_checkouts"),  # For time agg, trips are checkouts
            pl.col("total_trips").alias("total_returns"),     # Same count for returns
            pl.lit(0).alias("net_flow"),                      # Net flow is 0 for system-wide
        ])
        .select(["agg_level", "agg_value", "agg_sort_key", "member_type", "rideable_type", "total_checkouts", "total_returns", "net_flow", "total_trips", "avg_duration_sec"])
    )

    # Day of week aggregation
    logger.info("  - Aggregating by day of week...")
    dow_map = {0: "Monday", 1: "Tuesday", 2: "Wednesday", 3: "Thursday", 4: "Friday", 5: "Saturday", 6: "Sunday"}
    dow_agg = (
        trips_filtered
        .with_columns(pl.col("started_at").dt.weekday().alias("weekday"))
        .group_by(["weekday", "member_type", "rideable_type"])
        .agg([
            pl.len().alias("total_trips"),
            pl.col("duration_sec").mean().alias("avg_duration_sec"),
        ])
        .with_columns([
            pl.lit("day_of_week").alias("agg_level"),
            pl.col("weekday").replace(dow_map).alias("agg_value"),
            pl.col("weekday").cast(pl.Int32).alias("agg_sort_key"),  # Cast to Int32
            pl.col("total_trips").alias("total_checkouts"),
            pl.col("total_trips").alias("total_returns"),
            pl.lit(0).alias("net_flow"),
        ])
        .select(["agg_level", "agg_value", "agg_sort_key", "member_type", "rideable_type", "total_checkouts", "total_returns", "net_flow", "total_trips", "avg_duration_sec"])
    )

    # Month aggregation
    logger.info("  - Aggregating by month...")
    month_map = {1: "January", 2: "February", 3: "March", 4: "April", 5: "May", 6: "June",
                 7: "July", 8: "August", 9: "September", 10: "October", 11: "November", 12: "December"}
    month_agg = (
        trips_filtered
        .with_columns(pl.col("started_at").dt.month().alias("month"))
        .group_by(["month", "member_type", "rideable_type"])
        .agg([
            pl.len().alias("total_trips"),
            pl.col("duration_sec").mean().alias("avg_duration_sec"),
        ])
        .with_columns([
            pl.lit("month").alias("agg_level"),
            pl.col("month").replace(month_map).alias("agg_value"),
            pl.col("month").cast(pl.Int32).alias("agg_sort_key"),  # Cast to Int32
            pl.col("total_trips").alias("total_checkouts"),
            pl.col("total_trips").alias("total_returns"),
            pl.lit(0).alias("net_flow"),
        ])
        .select(["agg_level", "agg_value", "agg_sort_key", "member_type", "rideable_type", "total_checkouts", "total_returns", "net_flow", "total_trips", "avg_duration_sec"])
    )

    # Year aggregation
    logger.info("  - Aggregating by year...")
    year_agg = (
        trips_filtered
        .with_columns(pl.col("started_at").dt.year().alias("year"))
        .group_by(["year", "member_type", "rideable_type"])
        .agg([
            pl.len().alias("total_trips"),
            pl.col("duration_sec").mean().alias("avg_duration_sec"),
        ])
        .with_columns([
            pl.lit("year").alias("agg_level"),
            pl.col("year").cast(pl.Utf8).alias("agg_value"),
            pl.col("year").cast(pl.Int32).alias("agg_sort_key"),  # Ensure Int32
            pl.col("total_trips").alias("total_checkouts"),
            pl.col("total_trips").alias("total_returns"),
            pl.lit(0).alias("net_flow"),
        ])
        .select(["agg_level", "agg_value", "agg_sort_key", "member_type", "rideable_type", "total_checkouts", "total_returns", "net_flow", "total_trips", "avg_duration_sec"])
    )

    # Union all aggregation levels
    logger.info("  - Combining all aggregation levels...")
    out = pl.concat([
        day_agg.collect(),
        dow_agg.collect(),
        month_agg.collect(),
        year_agg.collect(),
    ])

    out_uri = f"s3://{PROC_BUCKET}/{AGG_PREFIX}/time_aggregated.parquet"
    _write_parquet_to_s3(out, out_uri)
    logger.info(f"time_aggregated written to {out_uri} ({len(out):,} rows)")


def build_all_summaries() -> None:
    """Build all summary/aggregate tables from master trips data."""
    logger.info("Building all summary tables...")
    logger.info("=" * 60)

    build_system_daily()
    logger.info("")

    build_system_daily_detailed()
    logger.info("")

    build_station_daily(sample=False)
    logger.info("")

    build_station_daily(sample=True)
    logger.info("")

    build_station_daily_detailed()
    logger.info("")

    build_station_hourly()
    logger.info("")

    build_station_routes()
    logger.info("")

    build_time_aggregated()
    logger.info("")

    logger.info("=" * 60)
    logger.info("All summaries built successfully!")


if __name__ == "__main__":
    build_all_summaries()

from __future__ import annotations

from typing import Mapping

import polars as pl


# --------------------------------------------------
# Station ID normalization (schema drift fix)
# --------------------------------------------------
def normalize_station_id(expr: pl.Expr) -> pl.Expr:
    """
    Normalize station IDs that may appear as floats, ints, or strings into Int64.

    Examples:
      31000.0    -> 31000
      "31000"    -> 31000
      "31000.0"  -> 31000
    """
    return expr.cast(pl.Utf8).str.replace(r"\.0$", "").cast(pl.Int64)


# --------------------------------------------------
# Trip schema normalization
# --------------------------------------------------
# Pre-format (older) -> canonical
_PRE_RENAME: Mapping[str, str] = {
    "Start date": "started_at",
    "End date": "ended_at",
    "Start station": "start_station_name",
    "End station": "end_station_name",
    "Start station number": "start_station_id",
    "End station number": "end_station_id",
    "Bike number": "bike_number",
    "Member type": "member_type",
}

# Post-format (newer) -> canonical (mostly identity)
_POST_RENAME: Mapping[str, str] = {
    "ride_id": "ride_id",
    "rideable_type": "rideable_type",
    "started_at": "started_at",
    "ended_at": "ended_at",
    "start_station_name": "start_station_name",
    "end_station_name": "end_station_name",
    "start_station_id": "start_station_id",
    "end_station_id": "end_station_id",
    "start_lat": "start_lat",
    "start_lng": "start_lng",
    "end_lat": "end_lat",
    "end_lng": "end_lng",
    "member_casual": "member_type",
}

CANONICAL_COLUMNS = [
    "ride_id",
    "rideable_type",
    "started_at",
    "ended_at",
    "duration_sec",
    "start_station_id",
    "start_station_name",
    "start_lat",
    "start_lng",
    "end_station_id",
    "end_station_name",
    "end_lat",
    "end_lng",
    "bike_number",
    "member_type",
    "day",
    "hour",
    "weekday",
]


def normalize_trip_schema(df: pl.DataFrame, stations: pl.DataFrame) -> pl.DataFrame:
    """
    Normalize a raw CaBi month DataFrame into a canonical schema.

    - Handles pre/post column names
    - Parses datetimes
    - Computes duration_sec
    - Normalizes station IDs to Int64 (critical)
    - Casts coordinates to Float64 when present
    - Fills missing canonical columns with nulls
    """
    # Rename pre-style columns if present
    if any(c in df.columns for c in _PRE_RENAME):
        df = df.rename({k: v for k, v in _PRE_RENAME.items() if k in df.columns})

    # Rename post-style columns (harmless if already canonical)
    df = df.rename({k: v for k, v in _POST_RENAME.items() if k in df.columns})

    # Parse datetimes (robust to already-datetime)
    if "started_at" in df.columns:
        df = df.with_columns(
            pl.col("started_at").str.strptime(pl.Datetime, strict=False)
        ).with_columns(
            [
                pl.col("started_at").dt.day().alias("day"),
                pl.col("started_at").dt.hour().alias("hour"),
                pl.col("started_at").dt.weekday().alias("weekday"),
            ]
        )

    if "ended_at" in df.columns:
        df = df.with_columns(pl.col("ended_at").str.strptime(pl.Datetime, strict=False))

    # Compute duration seconds when possible
    if "duration_sec" not in df.columns and {"started_at", "ended_at"}.issubset(
        df.columns
    ):
        df = df.with_columns(
            (pl.col("ended_at") - pl.col("started_at"))
            .dt.total_seconds()
            .cast(pl.Int64)
            .alias("duration_sec")
        )

    # Normalize station IDs (schema drift fix)
    for col in ["start_station_id", "end_station_id"]:
        if col in df.columns:
            df = df.with_columns(normalize_station_id(pl.col(col)).alias(col))

    df = df.join(
        stations.select(
            pl.col("station_id").alias("start_station_id"),
            pl.col("lat").alias("start_lat"),
            pl.col("lng").alias("start_lng"),
        ),
        on="start_station_id",
        how="left",
    ).join(
        stations.select(
            pl.col("station_id").alias("end_station_id"),
            pl.col("lat").alias("end_lat"),
            pl.col("lng").alias("end_lng"),
        ),
        on="end_station_id",
        how="left",
    )

    # Fill missing columns with nulls
    for col in CANONICAL_COLUMNS:
        if col not in df.columns:
            if col in {
                "ride_id",
                "rideable_type",
                "start_station_name",
                "end_station_name",
                "bike_number",
                "member_type",
            }:
                df = df.with_columns(pl.lit(None, dtype=pl.Utf8).alias(col))

            elif col in {"start_lat", "start_lng", "end_lat", "end_lng"}:
                df = df.with_columns(pl.lit(None, dtype=pl.Float64).alias(col))

            elif col in {"started_at", "ended_at"}:
                df = df.with_columns(pl.lit(None, dtype=pl.Datetime).alias(col))

            elif col == "duration_sec":
                df = df.with_columns(pl.lit(None, dtype=pl.Int64).alias(col))

    # Select canonical order
    return df.select(CANONICAL_COLUMNS)

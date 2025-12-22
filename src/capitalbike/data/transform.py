from __future__ import annotations

from typing import Dict

import polars as pl


MASTER_COLUMNS: Dict[str, pl.DataType] = {
    "start_time": pl.Datetime,
    "end_time": pl.Datetime,
    "duration_sec": pl.Int64,
    "start_station_id": pl.Utf8,
    "start_station_name": pl.Utf8,
    "end_station_id": pl.Utf8,
    "end_station_name": pl.Utf8,
    "bike_number": pl.Utf8,
    "member_type": pl.Utf8,
    "year": pl.Int32,
    "month": pl.Int32,
    "day": pl.Int32,
    "hour": pl.Int32,
    "weekday": pl.Int32,
}


def normalize_trip_schema(df: pl.DataFrame) -> pl.DataFrame:
    """
    Normalize a raw CaBi trip parquet into the unified schema.

    Handles older files with:
        - 'Start station number', 'End station number'
        - 'Start station', 'End station'
        - 'Start date', 'End date'

    And newer files that may already use:
        - 'start_station_id', 'end_station_id'
        - 'start_station_name', 'end_station_name'
        - 'start_time', 'end_time' or 'started_at' / 'ended_at'
    """

    rename_map: Dict[str, str] = {}

    # ----- Time columns -----
    if "Start date" in df.columns:
        rename_map["Start date"] = "start_time"
    if "End date" in df.columns:
        rename_map["End date"] = "end_time"
    if "started_at" in df.columns:
        rename_map["started_at"] = "start_time"
    if "ended_at" in df.columns:
        rename_map["ended_at"] = "end_time"

    # ----- Station IDs: old vs new -----
    # Old: Start station number / End station number
    if "Start station number" in df.columns:
        rename_map["Start station number"] = "start_station_id"
    if "End station number" in df.columns:
        rename_map["End station number"] = "end_station_id"

    # Old: station names
    if "Start station" in df.columns:
        rename_map["Start station"] = "start_station_name"
    if "End station" in df.columns:
        rename_map["End station"] = "end_station_name"

    # New: if they already exist, we just keep them
    # (no-op, but kept for clarity)

    # ----- Bike / member / duration -----
    if "Bike number" in df.columns:
        rename_map["Bike number"] = "bike_number"
    if "Member type" in df.columns:
        rename_map["Member type"] = "member_type"
    if "Duration" in df.columns:
        rename_map["Duration"] = "duration_sec"

    if rename_map:
        df = df.rename(rename_map)

    # Parse datetimes if present
    if "start_time" in df.columns:
        df = df.with_columns(
            pl.col("start_time")
            .cast(pl.Utf8)
            .str.strptime(pl.Datetime, strict=False)
        )

    if "end_time" in df.columns:
        df = df.with_columns(
            pl.col("end_time")
            .cast(pl.Utf8)
            .str.strptime(pl.Datetime, strict=False)
        )

    # Duration
    if "duration_sec" in df.columns:
        df = df.with_columns(pl.col("duration_sec").cast(pl.Int64))
    elif {"start_time", "end_time"} <= set(df.columns):
        df = df.with_columns(
            (pl.col("end_time") - pl.col("start_time"))
            .dt.seconds()
            .cast(pl.Int64)
            .alias("duration_sec")
        )

    # Cast IDs to Utf8 for joins
    for col in ("start_station_id", "end_station_id", "bike_number"):
        if col in df.columns:
            df = df.with_columns(pl.col(col).cast(pl.Utf8))

    # Member type cleanup (if present)
    if "member_type" in df.columns:
        df = df.with_columns(
            pl.col("member_type")
            .cast(pl.Utf8)
            .str.replace("Registered", "Member")
            .str.replace("Subscriber", "Member")
        )

    # Calendar fields
    if "start_time" in df.columns:
        df = df.with_columns(
            [
                pl.col("start_time").dt.day().alias("day"),
                pl.col("start_time").dt.hour().alias("hour"),
                pl.col("start_time").dt.weekday().alias("weekday"),
            ]
        )

    MASTER_COLUMNS = {
        "ride_id": pl.Utf8,
        "rideable_type": pl.Utf8,
        "start_time": pl.Datetime,
        "end_time": pl.Datetime,
        "duration_sec": pl.Int64,
        "start_station_id": pl.Utf8,
        "start_station_name": pl.Utf8,
        "end_station_id": pl.Utf8,
        "end_station_name": pl.Utf8,
        "start_lat": pl.Float64,
        "start_lng": pl.Float64,
        "end_lat": pl.Float64,
        "end_lng": pl.Float64,
        "bike_number": pl.Utf8,
        "member_type": pl.Utf8,
        "member_casual": pl.Utf8,
        "day": pl.Int8,
        "hour": pl.Int8,
        "weekday": pl.Int8,
    }

    # --- Enforce schema ---
    for col, dtype in MASTER_COLUMNS.items():
        if col not in df.columns:
            df = df.with_columns(pl.lit(None).cast(dtype).alias(col))
        else:
            df = df.with_columns(pl.col(col).cast(dtype))

    # Order columns consistently
    df = df.select(list(MASTER_COLUMNS.keys()))

    df = df.drop(
        [
            "start_station_name",
            "start_lat",
            "start_lng",
            "end_station_name",
            "end_lat",
            "end_lng",
        ]
    )

    return df

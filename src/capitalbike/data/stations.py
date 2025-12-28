from __future__ import annotations

import polars as pl

from src.capitalbike.data.transform import normalize_station_id


def build_station_dimension(trips: pl.LazyFrame | pl.DataFrame) -> pl.DataFrame:
    """
    Build ONE canonical station dimension table.

    Output schema:
      station_id        Int64
      station_name      Utf8
      lat               Float64
      lng               Float64
      earliest_seen     Datetime
      latest_seen       Datetime

    Stations are entities; start/end are roles.
    We union start+end observations then aggregate.
    """
    lf = trips.lazy() if isinstance(trips, pl.DataFrame) else trips

    lf = lf.with_columns(
        [
            normalize_station_id(pl.col("start_station_id")).alias("start_station_id"),
            normalize_station_id(pl.col("end_station_id")).alias("end_station_id"),
            pl.col("started_at").cast(pl.Datetime),
        ]
    )

    start_obs = lf.select(
        [
            pl.col("start_station_id").alias("station_id"),
            pl.col("start_station_name").alias("station_name"),
            pl.col("start_lat").alias("lat"),
            pl.col("start_lng").alias("lng"),
            pl.col("started_at").alias("seen_at"),
        ]
    )

    end_obs = lf.select(
        [
            pl.col("end_station_id").alias("station_id"),
            pl.col("end_station_name").alias("station_name"),
            pl.col("end_lat").alias("lat"),
            pl.col("end_lng").alias("lng"),
            pl.col("started_at").alias("seen_at"),
        ]
    )

    stations = (
        pl.concat([start_obs, end_obs], how="vertical")
        .filter(pl.col("station_id").is_not_null())
        .group_by(["station_id", "station_name"])
        .agg(
            [
                pl.col("lat").mean().cast(pl.Float64).alias("lat"),
                pl.col("lng").mean().cast(pl.Float64).alias("lng"),
                pl.col("seen_at").min().alias("earliest_seen"),
                pl.col("seen_at").max().alias("latest_seen"),
            ]
        )
        .sort("station_id")
        .sort("latest_seen", descending=True)
        .unique(subset="station_id", keep="first")
        .collect()
    )

    # Fail loudly if drift slips through
    assert stations["station_id"].dtype == pl.Int64

    return stations

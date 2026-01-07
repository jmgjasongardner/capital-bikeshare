"""
Geocode all stations to add city, state, and zip_code fields.

Uses Nominatim (OpenStreetMap) API for free reverse geocoding.
Respects API rate limits (1 request per second max).
"""

from __future__ import annotations

import os
import time
from io import BytesIO

import boto3
import polars as pl
import requests
from dotenv import load_dotenv

load_dotenv()

PROC_BUCKET = os.getenv("S3_BUCKET_PROCESSED", "capital-bikeshare-manipulated")
STATIONS_KEY = os.getenv("S3_KEY_STATIONS", "dimensions/stations.parquet")


def _s3() -> boto3.client:
    return boto3.client("s3")


def reverse_geocode(lat: float, lng: float) -> dict:
    """
    Reverse geocode a lat/lng coordinate using Nominatim API.

    Returns dict with keys: city, state, zip_code
    """
    url = "https://nominatim.openstreetmap.org/reverse"
    params = {
        "lat": lat,
        "lon": lng,
        "format": "json",
        "addressdetails": 1,
    }
    headers = {
        "User-Agent": "CapitalBikeshareAnalytics/1.0 (educational project)"
    }

    try:
        response = requests.get(url, params=params, headers=headers, timeout=10)
        response.raise_for_status()
        data = response.json()

        address = data.get("address", {})

        # Extract city (try multiple keys as OSM can return different ones)
        city = (
            address.get("city")
            or address.get("town")
            or address.get("village")
            or address.get("county")
            or "Unknown"
        )

        # Extract state
        state = address.get("state", "Unknown")

        # Extract zip code
        zip_code = address.get("postcode", "Unknown")

        return {
            "city": city,
            "state": state,
            "zip_code": zip_code,
        }

    except Exception as e:
        print(f"Error geocoding ({lat}, {lng}): {e}")
        return {
            "city": "Unknown",
            "state": "Unknown",
            "zip_code": "Unknown",
        }


def geocode_all_stations():
    """
    Load stations.parquet, geocode all stations, and save back with city/state/zip.
    """
    print("Loading stations from S3...")
    response = _s3().get_object(Bucket=PROC_BUCKET, Key=STATIONS_KEY)
    stations_df = pl.read_parquet(BytesIO(response["Body"].read()))

    print(f"Found {len(stations_df)} stations to geocode")

    # Check if geocoding columns already exist
    if "city" in stations_df.columns:
        print("Geocoding columns already exist. Skipping stations with existing data.")
        # Only geocode stations where city is null or "Unknown"
        to_geocode = stations_df.filter(
            (pl.col("city").is_null()) | (pl.col("city") == "Unknown")
        )
        already_geocoded = stations_df.filter(
            (pl.col("city").is_not_null()) & (pl.col("city") != "Unknown")
        )
        print(f"Already geocoded: {len(already_geocoded)}")
        print(f"Need to geocode: {len(to_geocode)}")
    else:
        to_geocode = stations_df
        already_geocoded = None
        print(f"Geocoding all {len(to_geocode)} stations...")

    # Geocode stations that need it
    geocoded_data = []
    for i, row in enumerate(to_geocode.iter_rows(named=True)):
        lat = row["lat"]
        lng = row["lng"]
        station_name = row["station_name"]

        print(f"[{i+1}/{len(to_geocode)}] Geocoding {station_name} ({lat}, {lng})...")

        geo_info = reverse_geocode(lat, lng)
        geocoded_data.append({
            "station_id": row["station_id"],
            "city": geo_info["city"],
            "state": geo_info["state"],
            "zip_code": geo_info["zip_code"],
        })

        # Respect API rate limit (1 request per second)
        if i < len(to_geocode) - 1:  # Don't sleep after last request
            time.sleep(1.1)

    # Create dataframe from geocoded data
    geocoded_df = pl.DataFrame(geocoded_data)

    # Join geocoded data with original stations
    if already_geocoded is not None:
        # Merge with already geocoded stations
        to_geocode_updated = to_geocode.drop(["city", "state", "zip_code"], strict=False).join(
            geocoded_df, on="station_id", how="left", coalesce=True
        )
        stations_updated = pl.concat([already_geocoded, to_geocode_updated])
    else:
        stations_updated = stations_df.join(
            geocoded_df, on="station_id", how="left", coalesce=True
        )

    # Save back to S3
    print("\nSaving updated stations to S3...")
    buf = BytesIO()
    stations_updated.write_parquet(buf)
    buf.seek(0)
    _s3().put_object(
        Bucket=PROC_BUCKET,
        Key=STATIONS_KEY,
        Body=buf.getvalue()
    )

    print(f"✓ Geocoding complete! Updated stations saved to s3://{PROC_BUCKET}/{STATIONS_KEY}")

    # Print sample
    print("\nSample geocoded stations:")
    print(stations_updated.select(["station_name", "city", "state", "zip_code"]).head(10))


if __name__ == "__main__":
    geocode_all_stations()

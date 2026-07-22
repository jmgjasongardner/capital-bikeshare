#!/usr/bin/env python
"""
Build all aggregate/summary tables from master trip data.

This script runs only the aggregation step (not data ingestion).
Use this to regenerate aggregates after adding new aggregation logic.

Usage:
    python scripts/build_aggregates.py
"""

import logging
import sys
import time
from pathlib import Path

# Add project root to Python path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from dotenv import load_dotenv

from src.capitalbike.data.summarize import (
    build_all_summaries,
    build_routes_by_member_rideable,
    build_trip_patterns,
    build_trip_duration_buckets,
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(message)s',  # Simple format for CLI output
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Build Capital Bikeshare aggregate tables")
    parser.add_argument(
        "--only-new",
        action="store_true",
        help="Build only the three new Trip Analytics tables (faster, ~5-10 min)",
    )
    args = parser.parse_args()

    logger.info("=" * 70)
    logger.info("Capital Bikeshare - Aggregate Builder")
    logger.info("=" * 70)
    logger.info("")

    # Load environment variables
    load_dotenv()

    # Record start time
    start_time = time.time()

    if args.only_new:
        logger.info("Building new Trip Analytics aggregates only...")
        logger.info("")
        build_routes_by_member_rideable()
        logger.info("")
        build_trip_patterns()
        logger.info("")
        build_trip_duration_buckets()
    else:
        logger.info("This will build all summary/aggregate tables from the master trip data.")
        logger.info("Expected duration: 15-20 minutes")
        logger.info("")
        build_all_summaries()

    # Calculate elapsed time
    elapsed_time = time.time() - start_time
    minutes = int(elapsed_time // 60)
    seconds = int(elapsed_time % 60)

    logger.info("")
    logger.info("=" * 70)
    logger.info(f"✅ All aggregates built successfully in {minutes}m {seconds}s!")
    logger.info("=" * 70)
    logger.info("")
    logger.info("Aggregate files available:")
    logger.info("  • station_daily.parquet")
    logger.info("  • station_daily_detailed.parquet")
    logger.info("  • system_daily.parquet")
    logger.info("  • system_daily_detailed.parquet")
    logger.info("  • station_hourly.parquet")
    logger.info("  • station_routes.parquet")
    logger.info("  • routes_by_member_rideable.parquet")
    logger.info("  • trip_patterns.parquet")
    logger.info("  • trip_duration_buckets.parquet")
    logger.info("  • time_aggregated.parquet")
    logger.info("")
    logger.info("Your Streamlit app is now ready with all features enabled! 🚀")
    logger.info("")


if __name__ == "__main__":
    main()

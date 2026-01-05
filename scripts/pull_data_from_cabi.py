from __future__ import annotations
import polars as pl

"""
Convenience script to run the full ETL from the command line.

Usage (from project root, with src/ on PYTHONPATH):

    python scripts/pull_data_from_cabi.py
"""

from src.capitalbike.data.ingest import build_master_table
from src.capitalbike.data.summarize import build_all_summaries
from dotenv import load_dotenv


def main() -> None:
    stations = pl.read_parquet(
        f"s3://capital-bikeshare-manipulated/dimensions/stations.parquet"
    )
    build_master_table(stations=stations)
    build_all_summaries()


if __name__ == "__main__":
    load_dotenv()
    main()

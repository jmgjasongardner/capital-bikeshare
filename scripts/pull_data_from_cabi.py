from __future__ import annotations

"""
Convenience script to run the full ETL from the command line.

Usage (from project root, with src/ on PYTHONPATH):

    python scripts/pull_data_from_cabi.py
"""

from src.capitalbike.data.ingest import build_master_table
from src.capitalbike.data.summarize import build_all_summaries


def main() -> None:
    build_master_table()
    build_all_summaries()


if __name__ == "__main__":
    main()

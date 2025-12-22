from __future__ import annotations

import argparse
import datetime
import boto3
from dotenv import load_dotenv, find_dotenv

from src.capitalbike.data.raw_ingest import pull_and_write_from_cabi, pull_missing_files

load_dotenv(find_dotenv(), override=True)

RAW_BUCKET = "capital-bikeshare-public"


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--resave_all_raw",
        help="Re-pull ALL CaBi data from the website and overwrite the raw S3 bucket.",
        action="store_true",
    )
    args = parser.parse_args()

    s3 = boto3.client("s3")
    existing = s3.list_objects_v2(Bucket=RAW_BUCKET).get("Contents", [])
    keys = [obj["Key"] for obj in existing]

    keys.sort()

    if args.resave_all_raw:
        print("Resaving ALL raw data (starting 2010 → present)…")

        for year in range(2010, datetime.datetime.now().year + 1):
            year_str = str(year)
            if year >= 2018:
                # Monthly files
                for month in range(1, 13):
                    ym = f"{year_str}{month:02d}"
                    pull_and_write_from_cabi(ym)
            else:
                pull_and_write_from_cabi(year_str)
    else:
        print("Checking for missing months…")
        pull_missing_files(keys)

    print("✓ Raw ingestion complete")


if __name__ == "__main__":
    main()

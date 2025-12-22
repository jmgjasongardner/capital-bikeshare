import pandas as pd
from dotenv import find_dotenv, load_dotenv
import argparse
import datetime
import pull_functions
import boto3
import logging

if __name__ == "__main__":

    load_dotenv(find_dotenv(), override=True)

    # TODO: set up command line args defaulting to only pull in the new months
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--resave_entire_public_bucket",
        help="Pull in and store off all data in public capital-bikeshare bucket",
        action="store_true",
    )
    parser.add_argument(
        "--resave_bike_num_data",
        help="Save over dataframe that shows individual bike numbers",
        action="store_true",
    )
    parser.add_argument(
        "--resave_station_coordinates",
        help="Save over dataframe that pulls in station coordinates",
        action="store_true",
    )
    args = parser.parse_args()

    # List of all files saved off to s3
    s3bucket = boto3.client("s3").list_objects_v2(Bucket="capital-bikeshare-public")
    keys = [obj["Key"] for obj in s3bucket.get("Contents", [])]

    keys.sort()

    if args.resave_entire_public_bucket:  # If true, resave everything
        for yr in range(2010, datetime.datetime.now().year + 1):
            year_str = str(yr)
            if yr >= 2018:  # Beginning in 2018 the CaBi files are month by month
                for month in [
                    str(month).zfill(2) for month in range(1, 13)
                ]:  # Two-digit month strings
                    year_month_str = year_str + month
                    pull_functions.pull_and_write_from_cabi(year_month_str)
            else:
                pull_functions.pull_and_write_from_cabi(year_str)
    else:  # False default, just check for and save missing files
        pull_functions.pull_missing_files(keys)

    # y = pd.read_csv(f's3://capital-bikeshare-public/{202404}.csv')
    # x = pd.read_csv(f's3://capital-bikeshare-public/{2010}.csv')
    # z = pd.read_csv(f's3://capital-bikeshare-public/{201801}.csv')

    if args.resave_bike_num_data:
        # 202003 has nine variables including bike number, 202004 does not exist, 202005 has 13 variables including electric vs classic, with no bike name
        keys_with_bike_data = [key for key in keys if key <= "202003.csv"]
        bike_nums = pd.DataFrame()
        for key in keys_with_bike_data:
            print(key)
            bike_nums = pd.concat(
                [bike_nums, pd.read_csv(f"s3://capital-bikeshare-public/{key}")]
            )
        print("Saving to s3")
        bike_nums.to_parquet(
            f"s3://capital-bikeshare-manipulated/bike_number_data.parquet", index=False
        )
        print("Saved to s3")

    if args.resave_station_coordinates:
        keys_with_coordinate_data = [key for key in keys if key >= "202005.csv"]
        # coords2 = coords[pd.to_numeric(coords['start_station_id'], errors='coerce').notna() & pd.to_numeric(coords['end_station_id'],
        #                                                                                     errors='coerce').notna()]
        # TODO: Figure out what is up with 202102 data types

        keys_with_coordinate_data.remove("202102.csv")

        coords = pd.DataFrame()
        for key in keys_with_coordinate_data:
            print(key)
            coords = pd.concat(
                [coords, pd.read_csv(f"s3://capital-bikeshare-public/{key}")]
            )
        print("Saving to s3")
        # coords.to_parquet(f's3://capital-bikeshare-manipulated/no_bike_number_data.parquet', index=False)
        print("Saved to s3")

        coords["started_at"] = pd.to_datetime(coords["started_at"])
        earliest_started_at = (
            coords.groupby(["start_station_id", "start_station_name"])
            .agg(
                {"start_lat": "mean", "start_lng": "mean", "started_at": ["min", "max"]}
            )
            .reset_index()
        )
        earliest_started_at.columns = [
            "start_station_id",
            "start_station_name",
            "start_lat",
            "start_lng",
            "earliest",
            "latest",
        ]
        print("Saving to s3")
        earliest_started_at.to_parquet(
            f"s3://capital-bikeshare-manipulated/bike_stations.parquet", index=False
        )
        print("Saved to s3")

    print("done")

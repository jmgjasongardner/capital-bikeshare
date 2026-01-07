import pandas as pd
import zipfile
import requests
from io import BytesIO
from datetime import datetime, timedelta
import calendar
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()


def pull_and_write_from_cabi(year_str):
    url = f"https://s3.amazonaws.com/capitalbikeshare-data/{year_str}-capitalbikeshare-tripdata.zip"
    # GET request to the URL to download the zip file
    response = requests.get(url)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        zip_data = BytesIO(response.content)

        # Extract zip file contents
        # Note: Pre-2018 ZIPs may contain multiple quarterly CSV files
        with zipfile.ZipFile(zip_data, "r") as zip_ref:
            file_list = zip_ref.namelist()
            csv_files = [f for f in file_list if f.lower().endswith('.csv')]

            # Read and concatenate all CSV files
            dfs = []
            for csv_file in csv_files:
                df_part = pd.read_csv(
                    zip_ref.open(csv_file),
                    dtype={"start_station_id": "str", "end_station_id": "str"},
                    low_memory=False,
                )
                dfs.append(df_part)

            # Concatenate all parts
            df = pd.concat(dfs, ignore_index=True)

            # Handle both pre-2018 and post-2018 column names
            start_col = "start_station_id" if "start_station_id" in df.columns else "Start station number"
            end_col = "end_station_id" if "end_station_id" in df.columns else "End station number"

            if start_col in df.columns:
                df[start_col] = pd.to_numeric(df[start_col], errors="coerce")
            if end_col in df.columns:
                df[end_col] = pd.to_numeric(df[end_col], errors="coerce")

            # Only drop NAs if these columns exist
            dropna_cols = [col for col in [start_col, end_col] if col in df.columns]
            if dropna_cols:
                df = df.dropna(subset=dropna_cols)

            df.to_parquet(
                f"s3://capital-bikeshare-public/{year_str}.parquet", index=False
            )
            print(
                f"{year_str} written to s3"
            )  # Display the first few rows of the DataFrame
    else:
        print("Failed to download the file.")


def pull_missing_files(keys: list):
    last_date = datetime.strptime(keys[-1].strip(".parquet"), "%Y%m")
    current_year = datetime.now().year
    current_month = datetime.now().month

    # Generate missing keys
    missing_keys = []
    while last_date.year < current_year or (
        last_date.year == current_year and last_date.month < current_month
    ):
        # Advance to the next month
        print(f'Previously saved up through {last_date.strftime("%Y%m")} on s3')
        last_date = last_date + timedelta(
            days=calendar.monthrange(last_date.year, last_date.month)[1]
        )
        pull_and_write_from_cabi(last_date.strftime("%Y%m"))

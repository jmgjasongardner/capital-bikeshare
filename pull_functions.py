import pandas as pd
import zipfile
import requests
from io import BytesIO
from datetime import datetime, timedelta
import calendar


def pull_and_write_from_cabi(year_str):
    url = f"https://s3.amazonaws.com/capitalbikeshare-data/{year_str}-capitalbikeshare-tripdata.zip"
    # GET request to the URL to download the zip file
    response = requests.get(url)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        zip_data = BytesIO(response.content)

        # Extract zip file contents
        with zipfile.ZipFile(zip_data, "r") as zip_ref:
            file_list = zip_ref.namelist()
            df = pd.read_csv(
                zip_ref.open(file_list[0]),
                dtype={"start_station_id": "str", "end_station_id": "str"},
                low_memory=False,
            )
            df["start_station_id"] = pd.to_numeric(
                df["start_station_id"], errors="coerce"
            )
            df["end_station_id"] = pd.to_numeric(df["end_station_id"], errors="coerce")
            df = df.dropna(subset=["start_station_id", "end_station_id"])

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

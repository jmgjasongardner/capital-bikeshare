import pandas as pd
from dotenv import find_dotenv, load_dotenv

if __name__ == "__main__":

    load_dotenv(find_dotenv(), override=True)

    # Prior to covid data
    pre = pd.read_parquet(
        f"s3://capital-bikeshare-manipulated/bike_number_data.parquet"
    )

    end_station_counts = (
        pre.groupby("end_station_name")
        .size()
        .reset_index(name="end_counts")
        .sort_values(by="end_counts", ascending=False)
    )
    start_station_counts = (
        pre.groupby("start_station_name")
        .size()
        .reset_index(name="start_counts")
        .sort_values(by="start_counts", ascending=False)
    )
    counts = start_station_counts.merge(
        end_station_counts,
        how="inner",
        left_on="start_station_name",
        right_on="end_station_name",
    )
    counts["diff"] = counts["end_counts"] - counts["start_counts"]
    counts["ratio"] = counts["end_counts"] / counts["start_counts"]
    counts = counts.sort_values("diff", ascending=False)

    # Bike station data
    stations = pd.read_parquet(
        f"s3://capital-bikeshare-manipulated/bike_stations.parquet"
    )

    # Post covid data
    post = pd.read_parquet(
        f"s3://capital-bikeshare-manipulated/no_bike_number_data.parquet"
    )

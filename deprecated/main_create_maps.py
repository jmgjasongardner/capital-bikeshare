import pandas as pd
import numpy as np
import polars as pl
from dotenv import find_dotenv, load_dotenv
import folium

# from sklearn.preprocessing import MinMaxScaler

if __name__ == "__main__":

    load_dotenv(find_dotenv(), override=True)

    pre = pd.read_parquet(
        f"s3://capital-bikeshare-manipulated/bike_number_data.parquet"
    )
    post = pd.read_parquet(
        f"s3://capital-bikeshare-manipulated/no_bike_number_data.parquet"
    )
    stations = pd.read_parquet(
        f"s3://capital-bikeshare-manipulated/bike_stations.parquet"
    )

    pre = pd.read_parquet("data/bike_number_data.parquet")

    data = (
        pre.groupby(["Start station number", "Start station"])
        .size()
        .reset_index(name="num_checkouts")
    )
    data = data.merge(
        stations,
        left_on=["Start station number", "Start station"],
        right_on=["start_station_id", "start_station_name"],
    )
    data["num_checkouts_adj"] = np.where(
        data["num_checkouts"] < 1000, 1000, data["num_checkouts"]
    )
    # data['log_num_checkouts'] = np.log(data['num_checkouts_adj'])
    data["sqrt_num_checkouts"] = np.sqrt(data["num_checkouts_adj"])
    # data['scaled_num_checkouts'] = MinMaxScaler().fit_transform(data[['num_checkouts_adj']])

    data_new = (
        post.groupby(["start_station_id", "start_station_name"])
        .size()
        .reset_index(name="num_checkouts")
    )
    data_new = data_new.merge(
        stations,
        left_on=["start_station_id", "start_station_name"],
        right_on=["start_station_id", "start_station_name"],
    )
    data_new["num_checkouts_adj"] = np.where(
        data_new["num_checkouts"] < 1000, 1000, data_new["num_checkouts"]
    )
    # data_new['log_num_checkouts'] = np.log(data_new['num_checkouts_adj'])
    data_new["sqrt_num_checkouts"] = np.sqrt(data_new["num_checkouts_adj"])
    # data_new['scaled_num_checkouts'] = MinMaxScaler().fit_transform(data_new[['num_checkouts_adj']])

    map_dc = folium.Map(location=[38.8951, -77.0364], zoom_start=12)

    # Add markers for each bike station

    for index, row in data.iterrows():
        folium.CircleMarker(
            location=[row["start_lat"], row["start_lng"]],
            radius=row["sqrt_num_checkouts"] / 50,  # Adjust size of bubbles
            # popup=f"{row['start_station_name']}: {row['num_checkouts']} bikes", # Click
            tooltip=f"Station: {row['start_station_name']}\nPrior: {row['num_checkouts']}",  # Hover
            color="blue",
            fill=True,
            fill_opacity=0.7,
        ).add_to(map_dc)

    for index, row in data_new.iterrows():
        folium.CircleMarker(
            location=[row["start_lat"], row["start_lng"]],
            radius=row["sqrt_num_checkouts"] / 50,  # Adjust size of bubbles
            # popup=f"{row['start_station_name']}: {row['num_checkouts']} bikes", # Click
            tooltip=f"Station: {row['start_station_name']}\nPost: {row['num_checkouts']}",  # Hover
            color="red",
            fill=True,
            fill_opacity=0.7,
        ).add_to(map_dc)

    map_dc.save("bike_stations_dc4.html")

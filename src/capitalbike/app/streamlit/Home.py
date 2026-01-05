import streamlit as st
import altair as alt
import polars as pl

from src.capitalbike.app.io import read_parquet_from_s3


st.title("Capital Bikeshare Overview")

# --------------------------------------------------
# Load data
# --------------------------------------------------
DATA_PATH = f"s3://{st.secrets['S3_BUCKET_PROCESSED']}/aggregates/station_daily.parquet"

df = read_parquet_from_s3(DATA_PATH)

# Convert to pandas only for plotting
pdf = df.to_pandas()

# --------------------------------------------------
# Plot
# --------------------------------------------------
chart = (
    alt.Chart(pdf)
    .mark_line()
    .encode(
        x="date:T",
        y="num_checkouts:Q",
        tooltip=["date:T", "num_checkouts:Q"],
    )
    .properties(
        title="Total Bikeshare Checkouts per Day",
        height=400,
    )
)

st.altair_chart(chart, width="stretch")

st.caption("Data refreshed monthly via automated pipeline.")

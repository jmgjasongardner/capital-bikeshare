import streamlit as st
import polars as pl
from datetime import timedelta

from src.capitalbike.app.io import read_parquet_from_s3


st.title("Station Table")

# --------------------------------------------------
# Load data
# --------------------------------------------------
@st.cache_data(ttl=3600)
def load_stations():
    """Load station dimension data."""
    return read_parquet_from_s3(
        f"s3://{st.secrets['S3_BUCKET_PROCESSED']}/dimensions/stations.parquet"
    )


@st.cache_data(ttl=3600)
def load_station_daily():
    """Load station-level daily metrics."""
    return read_parquet_from_s3(
        f"s3://{st.secrets['S3_BUCKET_PROCESSED']}/aggregates/station_daily.parquet"
    )


stations_df = load_stations()
daily_df = load_station_daily()

# Get date range for filters
min_date = daily_df["date"].min()
max_date = daily_df["date"].max()

# --------------------------------------------------
# Sidebar Filters
# --------------------------------------------------
st.sidebar.header("Filters")

# Date range filter
date_range = st.sidebar.date_input(
    "Date Range",
    value=(min_date, max_date),
    min_value=min_date,
    max_value=max_date,
    help="Filter metrics by date range",
)

# Handle date range
if isinstance(date_range, tuple) and len(date_range) == 2:
    start_date, end_date = date_range
else:
    start_date = end_date = date_range

# Status filter
one_month_ago = max_date - timedelta(days=30)
status_filter = st.sidebar.multiselect(
    "Station Status",
    ["Active", "Discontinued"],
    default=["Active", "Discontinued"],
    help="Filter by station operational status",
)

# Minimum trips filter
min_trips = st.sidebar.number_input(
    "Minimum Total Trips",
    min_value=0,
    value=0,
    step=100,
    help="Filter out stations with fewer than this many total trips",
)

# Zip code filter (if available)
if "zip_code" in stations_df.columns:
    zip_codes = sorted([z for z in stations_df["zip_code"].unique().to_list() if z and z != "Unknown"])
    selected_zips = st.sidebar.multiselect(
        "Zip Codes",
        zip_codes,
        default=[],
        help="Filter by zip code (leave empty for all)",
    )
else:
    selected_zips = []

# City filter (if available)
if "city" in stations_df.columns:
    cities = sorted([c for c in stations_df["city"].unique().to_list() if c and c != "Unknown"])
    selected_cities = st.sidebar.multiselect(
        "Cities",
        cities,
        default=[],
        help="Filter by city (leave empty for all)",
    )
else:
    selected_cities = []

# --------------------------------------------------
# Calculate Station Metrics
# --------------------------------------------------
# Filter daily data by date range
daily_filtered = daily_df.filter(pl.col("date").is_between(start_date, end_date))

# Aggregate metrics by station
station_metrics = (
    daily_filtered.group_by("station_id")
    .agg(
        [
            pl.sum("num_checkouts").alias("total_checkouts"),
            pl.sum("num_returns").alias("total_returns"),
            pl.mean("avg_duration_sec").alias("avg_duration_sec"),
            pl.sum("distinct_bikes_out").alias("total_distinct_bikes"),
            pl.first("station_name").alias("station_name"),
        ]
    )
    .with_columns(
        [
            (pl.col("total_checkouts") - pl.col("total_returns")).alias("net_flow"),
            (pl.col("total_checkouts") + pl.col("total_returns")).alias("total_trips"),
            (pl.col("avg_duration_sec") / 60).alias("avg_duration_min"),
        ]
    )
)

# Join with station metadata
# Conditionally include geocoding columns if they exist
select_cols = [
    "station_id",
    "station_name",
    "lat",
    "lng",
    "earliest_seen",
    "latest_seen",
    "total_checkouts",
    "total_returns",
    "total_trips",
    "net_flow",
    "avg_duration_min",
    "total_distinct_bikes",
]

if "city" in stations_df.columns:
    select_cols.insert(2, "city")
if "state" in stations_df.columns:
    select_cols.insert(3, "state")
if "zip_code" in stations_df.columns:
    select_cols.insert(4, "zip_code")

station_table = stations_df.join(
    station_metrics, on="station_id", how="left", coalesce=True
).select(select_cols)

# Fill nulls for stations with no activity in the selected date range
station_table = station_table.with_columns(
    [
        pl.col("total_checkouts").fill_null(0),
        pl.col("total_returns").fill_null(0),
        pl.col("total_trips").fill_null(0),
        pl.col("net_flow").fill_null(0),
        pl.col("avg_duration_min").fill_null(0),
        pl.col("total_distinct_bikes").fill_null(0),
    ]
)

# Add status column
station_table = station_table.with_columns(
    pl.when(pl.col("latest_seen") >= one_month_ago)
    .then(pl.lit("Active"))
    .otherwise(pl.lit("Discontinued"))
    .alias("status")
)

# Apply filters
if "Active" not in status_filter:
    station_table = station_table.filter(pl.col("status") != "Active")
if "Discontinued" not in status_filter:
    station_table = station_table.filter(pl.col("status") != "Discontinued")

if min_trips > 0:
    station_table = station_table.filter(pl.col("total_trips") >= min_trips)

# Apply zip code filter
if selected_zips and "zip_code" in station_table.columns:
    station_table = station_table.filter(pl.col("zip_code").is_in(selected_zips))

# Apply city filter
if selected_cities and "city" in station_table.columns:
    station_table = station_table.filter(pl.col("city").is_in(selected_cities))

# --------------------------------------------------
# Display Summary Stats
# --------------------------------------------------
st.markdown(f"### Station Overview ({len(station_table):,} stations)")

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Stations", f"{len(station_table):,}")
col2.metric(
    "Active Stations",
    f"{len(station_table.filter(pl.col('status') == 'Active')):,}",
)
col3.metric(
    "Discontinued",
    f"{len(station_table.filter(pl.col('status') == 'Discontinued')):,}",
)
col4.metric(
    "Total Trips (all stations)",
    f"{station_table['total_trips'].sum():,.0f}",
)

# --------------------------------------------------
# Sorting Controls
# --------------------------------------------------
st.markdown("---")

col1, col2 = st.columns([3, 1])

with col1:
    sort_by = st.selectbox(
        "Sort By",
        [
            "station_name",
            "total_trips",
            "total_checkouts",
            "total_returns",
            "net_flow",
            "avg_duration_min",
            "total_distinct_bikes",
            "earliest_seen",
            "latest_seen",
        ],
        index=1,  # Default to total_trips
        format_func=lambda x: {
            "station_name": "Station Name",
            "total_trips": "Total Trips",
            "total_checkouts": "Total Checkouts",
            "total_returns": "Total Returns",
            "net_flow": "Net Flow",
            "avg_duration_min": "Avg Duration (min)",
            "total_distinct_bikes": "Distinct Bikes",
            "earliest_seen": "First Seen",
            "latest_seen": "Last Seen",
        }[x],
    )

with col2:
    sort_order = st.selectbox("Order", ["Descending", "Ascending"])

# Apply sorting
station_table_sorted = station_table.sort(
    sort_by, descending=(sort_order == "Descending")
)

# --------------------------------------------------
# Display Table
# --------------------------------------------------
st.markdown("---")

# Format the dataframe for display
display_cols = [
    pl.col("station_name").alias("Station Name"),
    pl.col("status").alias("Status"),
]

# Add geocoding columns if they exist
if "city" in station_table_sorted.columns:
    display_cols.append(pl.col("city").alias("City"))
if "state" in station_table_sorted.columns:
    display_cols.append(pl.col("state").alias("State"))
if "zip_code" in station_table_sorted.columns:
    display_cols.append(pl.col("zip_code").alias("Zip Code"))

display_cols.extend([
    pl.col("lat").round(6).alias("Latitude"),
    pl.col("lng").round(6).alias("Longitude"),
    pl.col("total_trips").cast(pl.Int64).alias("Total Trips"),
    pl.col("total_checkouts").cast(pl.Int64).alias("Checkouts"),
    pl.col("total_returns").cast(pl.Int64).alias("Returns"),
    pl.col("net_flow").cast(pl.Int64).alias("Net Flow"),
    pl.col("avg_duration_min").round(1).alias("Avg Duration (min)"),
    pl.col("total_distinct_bikes").cast(pl.Int64).alias("Distinct Bikes"),
    pl.col("earliest_seen").alias("First Seen"),
    pl.col("latest_seen").alias("Last Seen"),
])

display_df = station_table_sorted.select(display_cols)

# Convert to pandas for better Streamlit display
display_pandas = display_df.to_pandas()

# Display using st.dataframe with column configuration
st.dataframe(
    display_pandas,
    use_container_width=True,
    height=600,
    column_config={
        "Status": st.column_config.TextColumn(
            "Status",
            help="Station operational status",
        ),
        "Total Trips": st.column_config.NumberColumn(
            "Total Trips",
            help="Sum of checkouts and returns",
            format="%d",
        ),
        "Checkouts": st.column_config.NumberColumn(
            "Checkouts",
            help="Number of trips starting from this station",
            format="%d",
        ),
        "Returns": st.column_config.NumberColumn(
            "Returns",
            help="Number of trips ending at this station",
            format="%d",
        ),
        "Net Flow": st.column_config.NumberColumn(
            "Net Flow",
            help="Checkouts - Returns (positive = more departures)",
            format="%+d",
        ),
        "Avg Duration (min)": st.column_config.NumberColumn(
            "Avg Duration (min)",
            help="Average trip duration in minutes",
            format="%.1f",
        ),
        "Distinct Bikes": st.column_config.NumberColumn(
            "Distinct Bikes",
            help="Number of unique bikes used at this station",
            format="%d",
        ),
    },
    hide_index=True,
)

# --------------------------------------------------
# Download Option
# --------------------------------------------------
st.markdown("---")

csv = display_pandas.to_csv(index=False).encode("utf-8")
st.download_button(
    label="📥 Download Table as CSV",
    data=csv,
    file_name=f"capital_bikeshare_stations_{start_date}_{end_date}.csv",
    mime="text/csv",
)

# --------------------------------------------------
# Help Text
# --------------------------------------------------
st.markdown("---")
st.markdown(
    """
    #### About This Table

    This table shows all Capital Bikeshare stations with aggregated metrics for the selected date range.

    **Columns:**
    - **Status**: 🟢 Active (seen in last 30 days) or 🔴 Discontinued
    - **Total Trips**: Sum of checkouts and returns
    - **Net Flow**: Checkouts minus Returns. Positive = more departures, Negative = more arrivals
    - **Avg Duration**: Average trip duration in minutes for trips from this station
    - **Distinct Bikes**: Number of unique bikes that departed from this station (pre-2020 data only)

    **Tips:**
    - Use the sidebar filters to narrow down stations by status or minimum trip count
    - Sort by any column using the dropdown above the table
    - Click column headers in the table to sort interactively
    - Download the filtered/sorted table as CSV using the button above
    """
)

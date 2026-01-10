import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent.parent
sys.path.insert(0, str(project_root))

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


@st.cache_data(ttl=3600)
def load_station_daily_detailed():
    """Load detailed station daily metrics with member/bike type dimensions."""
    try:
        return read_parquet_from_s3(
            f"s3://{st.secrets['S3_BUCKET_PROCESSED']}/aggregates/station_daily_detailed.parquet"
        )
    except Exception:
        # Fallback: Return None if file doesn't exist yet
        return None


stations_df = load_stations()
daily_df = load_station_daily()
detailed_df = load_station_daily_detailed()

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

# Calculate electric bike percentages (post-2020 data only)
# Only calculate if detailed data is available
if detailed_df is not None:
    detailed_filtered = detailed_df.filter(pl.col("date").is_between(start_date, end_date))

    electric_metrics = (
        detailed_filtered.filter(
            (pl.col("rideable_type").is_not_null()) &
            (pl.col("rideable_type") != "unknown")
        )
        .group_by("station_id")
        .agg([
            # Electric checkouts
            pl.sum(
                pl.when(pl.col("rideable_type") == "electric_bike")
                .then(pl.col("num_checkouts"))
                .otherwise(0)
            ).alias("electric_checkouts"),
            pl.sum("num_checkouts").alias("total_checkouts_with_type"),

            # Electric returns
            pl.sum(
                pl.when(pl.col("rideable_type") == "electric_bike")
                .then(pl.col("num_returns"))
                .otherwise(0)
            ).alias("electric_returns"),
            pl.sum("num_returns").alias("total_returns_with_type"),
        ])
        .with_columns([
            (pl.col("electric_checkouts") / pl.col("total_checkouts_with_type") * 100)
                .alias("pct_checkouts_electric"),
            (pl.col("electric_returns") / pl.col("total_returns_with_type") * 100)
                .alias("pct_returns_electric"),
        ])
        .select(["station_id", "pct_checkouts_electric", "pct_returns_electric"])
    )

    # Join electric metrics with station metrics
    station_metrics = station_metrics.join(
        electric_metrics, on="station_id", how="left", coalesce=True
    ).with_columns([
        pl.col("pct_checkouts_electric").fill_null(0),
        pl.col("pct_returns_electric").fill_null(0),
    ])
else:
    # Fallback: Add zero-filled electric bike percentage columns
    station_metrics = station_metrics.with_columns([
        pl.lit(0.0).alias("pct_checkouts_electric"),
        pl.lit(0.0).alias("pct_returns_electric"),
    ])

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
    "pct_checkouts_electric",
    "pct_returns_electric",
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
            "pct_checkouts_electric",
            "pct_returns_electric",
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
            "pct_checkouts_electric": "Electric Checkout %",
            "pct_returns_electric": "Electric Return %",
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
# Build list of columns to display (strings only, build expressions later)
display_col_names = ["station_name", "status"]

# Add geocoding columns if they exist
if "city" in station_table_sorted.columns:
    display_col_names.append("city")
if "state" in station_table_sorted.columns:
    display_col_names.append("state")
if "zip_code" in station_table_sorted.columns:
    display_col_names.append("zip_code")

# Add remaining metric columns
display_col_names.extend([
    "lat", "lng", "total_trips", "total_checkouts", "total_returns",
    "net_flow", "avg_duration_min", "total_distinct_bikes",
    "pct_checkouts_electric", "pct_returns_electric",
    "earliest_seen", "latest_seen"
])

# Convert to pandas first, then format
display_pandas = station_table_sorted.select(display_col_names).to_pandas()

# Rename columns for display
column_rename_map = {
    "station_name": "Station Name",
    "status": "Status",
    "city": "City",
    "state": "State",
    "zip_code": "Zip Code",
    "lat": "Latitude",
    "lng": "Longitude",
    "total_trips": "Total Trips",
    "total_checkouts": "Checkouts",
    "total_returns": "Returns",
    "net_flow": "Net Flow",
    "avg_duration_min": "Avg Duration (min)",
    "total_distinct_bikes": "Distinct Bikes",
    "pct_checkouts_electric": "Electric Checkout (%)",
    "pct_returns_electric": "Electric Return (%)",
    "earliest_seen": "First Seen",
    "latest_seen": "Last Seen",
}

# Only rename columns that exist
display_pandas = display_pandas.rename(
    columns={k: v for k, v in column_rename_map.items() if k in display_pandas.columns}
)

# Round numeric columns for display
if "Latitude" in display_pandas.columns:
    display_pandas["Latitude"] = display_pandas["Latitude"].round(6)
if "Longitude" in display_pandas.columns:
    display_pandas["Longitude"] = display_pandas["Longitude"].round(6)
if "Avg Duration (min)" in display_pandas.columns:
    display_pandas["Avg Duration (min)"] = display_pandas["Avg Duration (min)"].round(1)
if "Electric Checkout (%)" in display_pandas.columns:
    display_pandas["Electric Checkout (%)"] = display_pandas["Electric Checkout (%)"].round(1)
if "Electric Return (%)" in display_pandas.columns:
    display_pandas["Electric Return (%)"] = display_pandas["Electric Return (%)"].round(1)

# Display using st.dataframe with column configuration
st.dataframe(
    display_pandas,
    width='stretch',
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

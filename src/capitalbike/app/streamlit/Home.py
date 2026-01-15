import streamlit as st
import polars as pl

from src.capitalbike.app.io import read_parquet_from_s3
from src.capitalbike.viz.timeseries import create_system_timeseries


st.title("Capital Bikeshare System Overview")

# --------------------------------------------------
# Load data
# --------------------------------------------------
@st.cache_data(ttl=3600)
def load_system_daily():
    """Load system-level daily metrics with 1-hour cache."""
    return read_parquet_from_s3(
        f"s3://{st.secrets['S3_BUCKET_PROCESSED']}/aggregates/system_daily.parquet"
    )


df = load_system_daily()

# Get date range for filters
min_date = df["date"].min()
max_date = df["date"].max()

# --------------------------------------------------
# Filters
# --------------------------------------------------
st.subheader("Filters")

col1, col2, col3, col4 = st.columns(4)

with col1:
    date_range = st.date_input(
        "Date Range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
        help="Select date range to analyze",
    )

with col2:
    aggregation = st.selectbox(
        "Aggregate By",
        ["Daily", "Weekly", "Monthly"],
        index=0,
        help="Level of time aggregation",
    )

with col3:
    metric = st.selectbox(
        "Metric",
        ["Trips", "Avg Duration", "Both"],
        index=0,
        help="Which metric to display",
    )

with col4:
    show_trend = st.checkbox(
        "Show Trend Line",
        value=False,
        help="Overlay linear trend line",
    )

# --------------------------------------------------
# Filter data by date range
# --------------------------------------------------
# Handle single date selection (when user clicks on start/end date)
if isinstance(date_range, tuple) and len(date_range) == 2:
    start_date, end_date = date_range
else:
    start_date = end_date = date_range

df_filtered = df.filter(pl.col("date").is_between(start_date, end_date))

# --------------------------------------------------
# Display key metrics
# --------------------------------------------------
total_trips = df_filtered["trips"].sum()
avg_duration = df_filtered["avg_duration_sec"].mean() / 60  # Convert to minutes
num_days = len(df_filtered)
avg_daily_trips = total_trips / num_days if num_days > 0 else 0

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Trips", f"{total_trips:,.0f}")
col2.metric("Avg Duration", f"{avg_duration:.1f} min")
col3.metric("Days", f"{num_days:,}")
col4.metric("Avg Daily Trips", f"{avg_daily_trips:,.0f}")

# --------------------------------------------------
# Plot
# --------------------------------------------------
st.subheader("System-Wide Trends")

if len(df_filtered) > 0:
    fig = create_system_timeseries(
        df_filtered,
        aggregation=aggregation,
        metric=metric,
        show_trend=show_trend,
    )

    st.plotly_chart(fig, width='stretch')

    # Add helpful instructions
    st.caption(
        "💡 **Tip**: Use the **slider below the chart** to zoom into a specific time period. "
        "Click and drag on the chart to select a custom date range. Double-click to reset."
    )
else:
    st.warning("No data available for the selected date range.")

# --------------------------------------------------
# Additional insights
# --------------------------------------------------
with st.expander("About the Data"):
    st.markdown(
        f"""
        **Data Coverage**: {min_date.strftime('%B %d, %Y')} to {max_date.strftime('%B %d, %Y')}

        **Data Source**: Capital Bikeshare public trip history dataset

        **Refresh Schedule**: Data is automatically refreshed monthly via GitHub Actions

        **Note**: This dashboard uses pre-aggregated summary tables for fast performance.
        All visualizations are generated from ~{len(df):,} daily aggregates rather than
        individual trip records.
        """
    )

st.caption("Data refreshed monthly via automated pipeline.")

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent.parent
sys.path.insert(0, str(project_root))

import streamlit as st
import polars as pl
import plotly.express as px
import plotly.graph_objects as go

from src.capitalbike.app.io import read_parquet_from_s3


st.set_page_config(page_title="Time Aggregation Analytics", layout="wide")
st.title("⏰ Time Aggregation Analytics")

st.markdown("""
Explore Capital Bikeshare trip patterns aggregated by different time dimensions.
Analyze trends by day, day of week, month, or year to uncover seasonal and temporal patterns.
""")

# --------------------------------------------------
# Load data
# --------------------------------------------------
@st.cache_data(ttl=3600)
def load_time_aggregated():
    """Load time-based aggregates with day/week/month/year dimensions."""
    try:
        return read_parquet_from_s3(
            f"s3://{st.secrets['S3_BUCKET_PROCESSED']}/aggregates/time_aggregated.parquet"
        )
    except Exception:
        # File doesn't exist yet
        return None


time_df = load_time_aggregated()

# Check if data is available
if time_df is None:
    st.error("⚠️ Time aggregation data not available yet.")
    st.info("""
    **To enable this page, run the aggregation build:**

    ```bash
    python -c "from src.capitalbike.data.summarize import build_all_summaries; from dotenv import load_dotenv; load_dotenv(); build_all_summaries()"
    ```

    This will take approximately 15-20 minutes to complete.
    """)
    st.stop()

# --------------------------------------------------
# Sidebar Filters
# --------------------------------------------------
st.sidebar.header("Filters")

# Aggregation level selector
agg_level = st.sidebar.selectbox(
    "Aggregation Level",
    ["Day", "Day of Week", "Month", "Year"],
    help="Choose time aggregation granularity"
)

# Map to internal values
agg_level_map = {
    "Day": "day",
    "Day of Week": "day_of_week",
    "Month": "month",
    "Year": "year"
}

st.sidebar.markdown("---")

# Member type filter
member_filter = st.sidebar.multiselect(
    "Member Type",
    ["member", "casual", "unknown"],
    default=["member", "casual"],
    help="Filter by rider membership status"
)

# Bike type filter
rideable_filter = st.sidebar.multiselect(
    "Bike Type",
    ["classic_bike", "electric_bike", "docked_bike", "unknown"],
    default=["classic_bike", "electric_bike", "docked_bike"],
    help="Filter by bike type"
)

st.sidebar.markdown("---")

# Metric selector for visualization
viz_metric = st.sidebar.selectbox(
    "Visualization Metric",
    ["Total Checkouts", "Total Returns", "Net Flow", "Avg Duration (min)"],
    help="Choose which metric to visualize"
)

metric_map = {
    "Total Checkouts": "checkouts",
    "Total Returns": "returns",
    "Net Flow": "net_flow",
    "Avg Duration (min)": "avg_duration_min"
}

# --------------------------------------------------
# Filter and Aggregate Data
# --------------------------------------------------
# Filter by aggregation level and filters
filtered_df = time_df.filter(
    (pl.col("agg_level") == agg_level_map[agg_level]) &
    pl.col("member_type").is_in(member_filter) &
    pl.col("rideable_type").is_in(rideable_filter)
)

# Aggregate across member/bike types
summary_df = (
    filtered_df.group_by(["agg_value", "agg_sort_key"])
    .agg([
        pl.sum("total_checkouts").alias("checkouts"),
        pl.sum("total_returns").alias("returns"),
        pl.sum("net_flow").alias("net_flow"),
        pl.mean("avg_duration_sec").alias("avg_duration_sec"),
        pl.sum("total_trips").alias("total_trips"),
    ])
    .with_columns([
        (pl.col("avg_duration_sec") / 60).alias("avg_duration_min"),
    ])
    .sort("agg_sort_key")
)

# --------------------------------------------------
# Display Summary Metrics
# --------------------------------------------------
st.markdown("---")

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric(
        "Total Checkouts",
        f"{summary_df['checkouts'].sum():,}",
        help="Total number of bike checkouts in the selected period"
    )

with col2:
    st.metric(
        "Total Returns",
        f"{summary_df['returns'].sum():,}",
        help="Total number of bike returns in the selected period"
    )

with col3:
    st.metric(
        "Net Flow",
        f"{summary_df['net_flow'].sum():+,}",
        help="Difference between checkouts and returns (system-wide should be ~0)"
    )

with col4:
    st.metric(
        "Avg Duration",
        f"{summary_df['avg_duration_min'].mean():.1f} min",
        help="Average trip duration across all trips"
    )

# --------------------------------------------------
# Visualization
# --------------------------------------------------
st.markdown("---")
st.subheader(f"Trends by {agg_level}")

if len(summary_df) > 0:
    # Convert to pandas for Plotly
    summary_pandas = summary_df.to_pandas()

    viz_metric_col = metric_map[viz_metric]

    # Choose chart type based on aggregation level
    if agg_level in ["Day of Week", "Month"]:
        # Bar chart for categorical aggregations
        fig = px.bar(
            summary_pandas,
            x="agg_value",
            y=viz_metric_col,
            title=f"{viz_metric} by {agg_level}",
            labels={"agg_value": agg_level, viz_metric_col: viz_metric},
            color=viz_metric_col,
            color_continuous_scale="Blues",
        )

        fig.update_layout(
            xaxis_title=agg_level,
            yaxis_title=viz_metric,
            hovermode="x unified",
            showlegend=False,
        )

    else:
        # Line chart for time-based aggregations (Day, Year)
        fig = px.line(
            summary_pandas,
            x="agg_value",
            y=viz_metric_col,
            title=f"{viz_metric} over Time ({agg_level})",
            labels={"agg_value": agg_level, viz_metric_col: viz_metric},
            markers=True,
        )

        fig.update_traces(
            line_color="#1f77b4",
            line_width=2,
            marker=dict(size=6),
        )

        fig.update_layout(
            xaxis_title=agg_level,
            yaxis_title=viz_metric,
            hovermode="x unified",
        )

    st.plotly_chart(fig, use_container_width=True)
else:
    st.warning("No data available for the selected filters.")

# --------------------------------------------------
# Multi-Metric Comparison (for Day of Week and Month)
# --------------------------------------------------
if agg_level in ["Day of Week", "Month"] and len(summary_df) > 0:
    st.markdown("---")
    st.subheader("Multi-Metric Comparison")

    # Create a grouped bar chart with multiple metrics
    fig_multi = go.Figure()

    fig_multi.add_trace(go.Bar(
        name="Checkouts",
        x=summary_pandas["agg_value"],
        y=summary_pandas["checkouts"],
        marker_color="#1f77b4",
    ))

    fig_multi.add_trace(go.Bar(
        name="Returns",
        x=summary_pandas["agg_value"],
        y=summary_pandas["returns"],
        marker_color="#ff7f0e",
    ))

    fig_multi.update_layout(
        title=f"Checkouts vs Returns by {agg_level}",
        xaxis_title=agg_level,
        yaxis_title="Count",
        barmode="group",
        hovermode="x unified",
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=1.02,
            xanchor="right",
            x=1
        ),
    )

    st.plotly_chart(fig_multi, use_container_width=True)

# --------------------------------------------------
# Data Table
# --------------------------------------------------
st.markdown("---")
st.subheader("Data Table")

if len(summary_df) > 0:
    # Prepare display dataframe
    display_df = summary_df.select([
        pl.col("agg_value").alias(agg_level),
        pl.col("checkouts").cast(pl.Int64).alias("Checkouts"),
        pl.col("returns").cast(pl.Int64).alias("Returns"),
        pl.col("net_flow").cast(pl.Int64).alias("Net Flow"),
        pl.col("total_trips").cast(pl.Int64).alias("Total Trips"),
        pl.col("avg_duration_min").round(1).alias("Avg Duration (min)"),
    ])

    # Display table with column config
    st.dataframe(
        display_df.to_pandas(),
        use_container_width=True,
        column_config={
            agg_level: st.column_config.TextColumn(agg_level),
            "Checkouts": st.column_config.NumberColumn(
                "Checkouts",
                format="%d",
                help="Total checkouts"
            ),
            "Returns": st.column_config.NumberColumn(
                "Returns",
                format="%d",
                help="Total returns"
            ),
            "Net Flow": st.column_config.NumberColumn(
                "Net Flow",
                format="%+d",
                help="Checkouts - Returns"
            ),
            "Total Trips": st.column_config.NumberColumn(
                "Total Trips",
                format="%d",
                help="Total trips (checkouts + returns)"
            ),
            "Avg Duration (min)": st.column_config.NumberColumn(
                "Avg Duration (min)",
                format="%.1f",
                help="Average trip duration in minutes"
            ),
        },
        hide_index=True,
    )

    # CSV Export
    csv = display_df.to_pandas().to_csv(index=False)
    st.download_button(
        label="📥 Download as CSV",
        data=csv,
        file_name=f"time_aggregation_{agg_level_map[agg_level]}.csv",
        mime="text/csv",
        help="Download the filtered data as a CSV file"
    )
else:
    st.warning("No data to display.")

# --------------------------------------------------
# Insights Section
# --------------------------------------------------
if len(summary_df) > 0:
    st.markdown("---")
    st.subheader("📊 Key Insights")

    with st.expander("View Insights"):
        # Calculate some basic insights
        max_checkouts_row = summary_df.filter(
            pl.col("checkouts") == pl.col("checkouts").max()
        )

        max_returns_row = summary_df.filter(
            pl.col("returns") == pl.col("returns").max()
        )

        if len(max_checkouts_row) > 0:
            st.markdown(f"**Highest Checkout Activity:** {max_checkouts_row['agg_value'][0]} with {max_checkouts_row['checkouts'][0]:,} checkouts")

        if len(max_returns_row) > 0:
            st.markdown(f"**Highest Return Activity:** {max_returns_row['agg_value'][0]} with {max_returns_row['returns'][0]:,} returns")

        avg_duration = summary_df['avg_duration_min'].mean()
        st.markdown(f"**Average Trip Duration:** {avg_duration:.1f} minutes")

        total_trips = summary_df['total_trips'].sum()
        st.markdown(f"**Total Trips in Period:** {total_trips:,}")

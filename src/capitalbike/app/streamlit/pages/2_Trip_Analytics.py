import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent.parent
sys.path.insert(0, str(project_root))

import streamlit as st
import polars as pl
import plotly.graph_objects as go
import plotly.express as px
from datetime import timedelta
from streamlit_folium import st_folium

from src.capitalbike.app.io import read_parquet_from_s3
from src.capitalbike.viz.maps import create_route_map, create_system_routes_map


st.title("Trip Analytics")

# --------------------------------------------------
# Load data
# --------------------------------------------------
@st.cache_data(ttl=3600)
def load_station_routes():
    """Load popular routes data."""
    return read_parquet_from_s3(
        f"s3://{st.secrets['S3_BUCKET_PROCESSED']}/aggregates/station_routes.parquet"
    )


@st.cache_data(ttl=3600)
def load_system_daily():
    """Load system-level daily metrics."""
    return read_parquet_from_s3(
        f"s3://{st.secrets['S3_BUCKET_PROCESSED']}/aggregates/system_daily.parquet"
    )


@st.cache_data(ttl=3600)
def load_stations():
    """Load station dimension data."""
    return read_parquet_from_s3(
        f"s3://{st.secrets['S3_BUCKET_PROCESSED']}/dimensions/stations.parquet"
    )


routes_df = load_station_routes()
system_df = load_system_daily()
stations_df = load_stations()

# Get date range
min_date = system_df["date"].min()
max_date = system_df["date"].max()

# --------------------------------------------------
# Sidebar Filters
# --------------------------------------------------
st.sidebar.header("Filters")

date_range = st.sidebar.date_input(
    "Date Range",
    value=(max_date - timedelta(days=365), max_date),
    min_value=min_date,
    max_value=max_date,
    help="Filter trip data by date range (uses full dataset if no advanced filters)",
)

# Handle date range
if isinstance(date_range, tuple) and len(date_range) == 2:
    start_date, end_date = date_range
else:
    start_date = end_date = date_range

# Advanced filters
st.sidebar.markdown("---")
st.sidebar.subheader("Advanced Filters")
st.sidebar.info("⚠️ Advanced filters query raw trip data (slower)")

member_filter = st.sidebar.multiselect(
    "Member Type",
    ["member", "casual"],
    default=["member", "casual"],
    help="Filter by rider membership status",
)

rideable_filter = st.sidebar.multiselect(
    "Bike Type",
    ["classic_bike", "electric_bike", "docked_bike"],
    default=["classic_bike", "electric_bike", "docked_bike"],
    help="Filter by bike type (post-2020 data only)",
)

use_advanced_filters = len(member_filter) < 2 or len(rideable_filter) < 3

# --------------------------------------------------
# Load and filter trip data if needed
# --------------------------------------------------
if use_advanced_filters:
    @st.cache_data(ttl=3600)
    def load_filtered_trips(start, end, members, rideables):
        """Load and filter raw trips data."""
        import os

        trips = pl.scan_parquet(
            f"s3://{st.secrets['S3_BUCKET_PROCESSED']}/master/trips/year=*/month=*/part.parquet",
            storage_options={"aws_region": os.getenv("AWS_DEFAULT_REGION", "us-east-1")}
        )

        # Apply filters
        trips_filtered = trips.filter(
            (pl.col("started_at").dt.date().is_between(start, end)) &
            (pl.col("duration_sec") > 0)  # Remove invalid trips
        )

        # Member type filter (case-insensitive)
        if members and len(members) < 2:
            member_conditions = [pl.col("member_type").str.to_lowercase() == m for m in members]
            member_filter_expr = member_conditions[0]
            for cond in member_conditions[1:]:
                member_filter_expr = member_filter_expr | cond
            trips_filtered = trips_filtered.filter(member_filter_expr)

        # Rideable type filter
        if rideables and len(rideables) < 3:
            rideable_filter_expr = pl.col("rideable_type").is_in(rideables)
            trips_filtered = trips_filtered.filter(rideable_filter_expr)

        # Sample if too large (performance optimization)
        return trips_filtered.limit(500_000).collect()

    with st.spinner("Loading trip data with filters..."):
        trips_data = load_filtered_trips(start_date, end_date, member_filter, rideable_filter)

    st.info(f"📊 Showing {len(trips_data):,} trips (sampled for performance)")
else:
    trips_data = None

# --------------------------------------------------
# Tabs
# --------------------------------------------------
tab1, tab2, tab3, tab4 = st.tabs(
    ["📍 Popular Routes", "⏱️ Trip Duration Analysis", "🕐 Temporal Patterns", "🏆 Extremes & Records"]
)

# --------------------------------------------------
# TAB 1: Popular Routes
# --------------------------------------------------
with tab1:
    st.markdown("### Most Popular Routes")
    st.markdown("Routes ranked by total number of trips between station pairs.")

    # Show top N routes
    top_n = st.slider("Number of routes to display", 5, 50, 20, 5)

    if not use_advanced_filters:
        # Use pre-aggregated routes data
        top_routes = routes_df.head(top_n)
    else:
        # Aggregate from filtered trips
        if trips_data is not None and len(trips_data) > 0:
            top_routes = (
                trips_data.group_by(["start_station_name", "end_station_name"])
                .agg([
                    pl.len().alias("trip_count"),
                    pl.col("duration_sec").mean().alias("avg_duration_sec"),
                ])
                .sort("trip_count", descending=True)
                .head(top_n)
            )
        else:
            top_routes = None

    if top_routes is not None and len(top_routes) > 0:
        # Create route labels
        top_routes_display = top_routes.with_columns(
            (pl.col("start_station_name") + " → " + pl.col("end_station_name")).alias("route")
        )

        # Horizontal bar chart
        fig = go.Figure()

        fig.add_trace(
            go.Bar(
                x=top_routes_display["trip_count"][::-1],  # Reverse for top-to-bottom display
                y=top_routes_display["route"][::-1],
                orientation="h",
                marker=dict(
                    color=top_routes_display["trip_count"][::-1],
                    colorscale="Viridis",
                    showscale=False,
                ),
                hovertemplate=(
                    "<b>%{y}</b><br>"
                    "Trips: %{x:,}<br>"
                    "Avg Duration: %{customdata:.1f} min<extra></extra>"
                ),
                customdata=(top_routes_display["avg_duration_sec"][::-1] / 60),
            )
        )

        fig.update_layout(
            title=f"Top {top_n} Most Popular Routes",
            xaxis_title="Number of Trips",
            yaxis_title="",
            height=max(400, top_n * 20),
            margin=dict(l=300, r=20, t=60, b=40),
        )

        st.plotly_chart(fig, width='stretch')

        # Summary stats
        col1, col2, col3 = st.columns(3)
        col1.metric("Total Routes", f"{len(routes_df):,}")
        col2.metric(
            "Most Popular Route",
            f"{top_routes['trip_count'][0]:,} trips"
        )
        col3.metric(
            "Avg Trip Count (Top 20)",
            f"{top_routes.head(20)['trip_count'].mean():,.0f}"
        )

        # Route map visualization
        if not use_advanced_filters and len(top_routes) > 0:
            st.markdown("---")
            st.markdown("#### 🗺️ Route Map Visualization")
            st.markdown("Top routes visualized on the DC metro area map with color-coded popularity.")

            # Create map showing top routes across the system
            route_map = create_system_routes_map(
                top_routes,
                top_n=min(top_n, 15),  # Limit to 15 routes for performance
            )

            st_folium(route_map, width=1200, height=500, key="popular_routes_map", returned_objects=[])
            st.caption("💡 Routes are color-coded from blue (less popular) to red (most popular). Line thickness indicates relative popularity.")
        elif use_advanced_filters:
            st.info("💡 Route map is only available when using pre-aggregated data (no advanced filters).")

        # Show data table
        with st.expander("📊 View Route Data Table"):
            display_df = top_routes_display.select([
                pl.col("route").alias("Route"),
                pl.col("trip_count").alias("Total Trips"),
                (pl.col("avg_duration_sec") / 60).round(1).alias("Avg Duration (min)"),
            ])
            st.dataframe(display_df.to_pandas(), width='stretch', height=400)
    else:
        st.warning("No route data available for the selected filters.")

# --------------------------------------------------
# TAB 2: Trip Duration Analysis
# --------------------------------------------------
with tab2:
    st.markdown("### Trip Duration Distribution")

    if use_advanced_filters and trips_data is not None:
        # Use filtered trip data
        durations = trips_data["duration_sec"] / 60  # Convert to minutes
    else:
        # Use system-level avg duration as proxy
        st.info("💡 Using aggregated data. Apply advanced filters for detailed distribution.")
        durations = None

    if durations is not None and len(durations) > 0:
        # Filter outliers for better visualization (remove top 1%)
        p99 = durations.quantile(0.99)
        durations_filtered = durations.filter(durations <= p99)

        # Histogram
        fig = go.Figure()

        fig.add_trace(
            go.Histogram(
                x=durations_filtered.to_numpy(),
                nbinsx=50,
                marker=dict(color="#17becf", line=dict(color="white", width=1)),
                hovertemplate="Duration: %{x:.1f} min<br>Count: %{y:,}<extra></extra>",
            )
        )

        fig.update_layout(
            title="Trip Duration Distribution (99th percentile)",
            xaxis_title="Duration (minutes)",
            yaxis_title="Number of Trips",
            height=400,
            bargap=0.1,
        )

        st.plotly_chart(fig, width='stretch')

        # Summary statistics
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Mean Duration", f"{durations.mean():.1f} min")
        col2.metric("Median Duration", f"{durations.median():.1f} min")
        col3.metric("Std Deviation", f"{durations.std():.1f} min")
        col4.metric("99th Percentile", f"{p99:.1f} min")

        # Duration ranges
        st.markdown("#### Trip Duration Ranges")

        ranges = [
            ("Quick (0-10 min)", 0, 10),
            ("Short (10-20 min)", 10, 20),
            ("Medium (20-30 min)", 20, 30),
            ("Long (30-60 min)", 30, 60),
            ("Very Long (60+ min)", 60, float('inf')),
        ]

        range_data = []
        for label, min_dur, max_dur in ranges:
            if max_dur == float('inf'):
                count = len(durations.filter(durations >= min_dur))
            else:
                count = len(durations.filter((durations >= min_dur) & (durations < max_dur)))
            pct = (count / len(durations)) * 100
            range_data.append({"Range": label, "Count": count, "Percentage": pct})

        range_df = pl.DataFrame(range_data)

        # Pie chart
        fig = go.Figure()

        fig.add_trace(
            go.Pie(
                labels=range_df["Range"],
                values=range_df["Count"],
                hovertemplate="<b>%{label}</b><br>Trips: %{value:,}<br>%{percent}<extra></extra>",
                marker=dict(colors=px.colors.sequential.Teal),
            )
        )

        fig.update_layout(
            title="Trip Distribution by Duration Range",
            height=400,
        )

        st.plotly_chart(fig, width='stretch')

    else:
        st.warning("Apply advanced filters to see detailed duration analysis.")

# --------------------------------------------------
# TAB 3: Temporal Patterns
# --------------------------------------------------
with tab3:
    st.markdown("### Trip Patterns by Time")

    if use_advanced_filters and trips_data is not None:
        # Hour of day analysis
        st.markdown("#### Trips by Hour of Day")

        hourly = (
            trips_data.group_by("hour")
            .agg(pl.len().alias("trip_count"))
            .sort("hour")
        )

        fig = go.Figure()

        fig.add_trace(
            go.Bar(
                x=hourly["hour"],
                y=hourly["trip_count"],
                marker=dict(
                    color=hourly["trip_count"],
                    colorscale="Blues",
                    showscale=False,
                ),
                hovertemplate="<b>%{x}:00</b><br>Trips: %{y:,}<extra></extra>",
            )
        )

        fig.update_layout(
            title="Trip Volume by Hour of Day",
            xaxis_title="Hour",
            yaxis_title="Number of Trips",
            height=400,
            xaxis=dict(tickmode="linear", tick0=0, dtick=1),
        )

        st.plotly_chart(fig, width='stretch')

        # Day of week analysis
        st.markdown("#### Trips by Day of Week")

        weekday_labels = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]

        daily = (
            trips_data.group_by("weekday")
            .agg(pl.len().alias("trip_count"))
            .sort("weekday")
        )

        # Map weekday numbers to labels
        daily = daily.with_columns(
            pl.col("weekday").replace({i: weekday_labels[i] for i in range(7)}).alias("weekday_label")
        )

        fig = go.Figure()

        fig.add_trace(
            go.Bar(
                x=daily["weekday_label"],
                y=daily["trip_count"],
                marker=dict(color="#2ecc71"),
                hovertemplate="<b>%{x}</b><br>Trips: %{y:,}<extra></extra>",
            )
        )

        fig.update_layout(
            title="Trip Volume by Day of Week",
            xaxis_title="Day of Week",
            yaxis_title="Number of Trips",
            height=400,
        )

        st.plotly_chart(fig, width='stretch')

        # Peak hours identification
        st.markdown("#### Peak Hours")
        peak_hours = hourly.sort("trip_count", descending=True).head(3)

        col1, col2, col3 = st.columns(3)
        for i, col in enumerate([col1, col2, col3]):
            if i < len(peak_hours):
                hour = peak_hours["hour"][i]
                count = peak_hours["trip_count"][i]
                col.metric(
                    f"Peak #{i+1}",
                    f"{hour:02d}:00",
                    f"{count:,} trips"
                )

    else:
        st.info("💡 Apply advanced filters to see detailed temporal patterns.")

        # Show system-level trends as alternative
        st.markdown("#### System-Wide Daily Trends")

        daily_filtered = system_df.filter(
            pl.col("date").is_between(start_date, end_date)
        )

        fig = go.Figure()

        fig.add_trace(
            go.Scatter(
                x=daily_filtered["date"],
                y=daily_filtered["trips"],
                mode="lines",
                line=dict(color="#3498db", width=2),
                hovertemplate="<b>%{x|%Y-%m-%d}</b><br>Trips: %{y:,}<extra></extra>",
            )
        )

        fig.update_layout(
            title="Daily Trip Volume",
            xaxis_title="Date",
            yaxis_title="Number of Trips",
            height=400,
            hovermode="x",
        )

        st.plotly_chart(fig, width='stretch')

# --------------------------------------------------
# TAB 4: Extremes & Records
# --------------------------------------------------
with tab4:
    st.markdown("### Trip Extremes & Records")

    if use_advanced_filters and trips_data is not None:
        # Longest trips
        st.markdown("#### 🏆 Longest Trips")

        longest = trips_data.sort("duration_sec", descending=True).head(10)

        longest_display = longest.select([
            (pl.col("start_station_name") + " → " + pl.col("end_station_name")).alias("Route"),
            pl.col("started_at").dt.date().alias("Date"),
            (pl.col("duration_sec") / 3600).round(2).alias("Duration (hours)"),
            pl.col("member_type").alias("Member Type"),
        ])

        st.dataframe(longest_display.to_pandas(), width='stretch')

        # Shortest trips (but > 1 min to filter out errors)
        st.markdown("#### ⚡ Shortest Trips (> 1 minute)")

        shortest = (
            trips_data.filter(pl.col("duration_sec") > 60)
            .sort("duration_sec")
            .head(10)
        )

        shortest_display = shortest.select([
            (pl.col("start_station_name") + " → " + pl.col("end_station_name")).alias("Route"),
            pl.col("started_at").dt.date().alias("Date"),
            (pl.col("duration_sec") / 60).round(2).alias("Duration (minutes)"),
            pl.col("member_type").alias("Member Type"),
        ])

        st.dataframe(shortest_display.to_pandas(), width='stretch')

        # Statistics
        st.markdown("---")
        st.markdown("#### 📊 Overall Statistics")

        col1, col2, col3, col4 = st.columns(4)

        col1.metric(
            "Longest Trip",
            f"{(longest['duration_sec'][0] / 3600):.1f} hrs"
        )
        col2.metric(
            "Shortest Trip",
            f"{shortest['duration_sec'][0]:.0f} sec"
        )
        col3.metric(
            "Total Trips",
            f"{len(trips_data):,}"
        )

        # Most active day
        most_active_day = (
            trips_data.group_by(pl.col("started_at").dt.date().alias("date"))
            .agg(pl.len().alias("count"))
            .sort("count", descending=True)
            .head(1)
        )

        if len(most_active_day) > 0:
            col4.metric(
                "Most Active Day",
                most_active_day["date"][0].strftime("%Y-%m-%d"),
                f"{most_active_day['count'][0]:,} trips"
            )

        # Member vs Casual breakdown
        st.markdown("#### 👥 Member Type Breakdown")

        member_breakdown = (
            trips_data.group_by("member_type")
            .agg(pl.len().alias("count"))
            .sort("count", descending=True)
        )

        fig = go.Figure()

        fig.add_trace(
            go.Bar(
                x=member_breakdown["member_type"],
                y=member_breakdown["count"],
                marker=dict(color=["#3498db", "#e74c3c", "#95a5a6"]),
                hovertemplate="<b>%{x}</b><br>Trips: %{y:,}<extra></extra>",
            )
        )

        fig.update_layout(
            title="Trips by Member Type",
            xaxis_title="Member Type",
            yaxis_title="Number of Trips",
            height=400,
        )

        st.plotly_chart(fig, width='stretch')

        # Rideable type breakdown (if data available)
        if "rideable_type" in trips_data.columns:
            rideable_breakdown = (
                trips_data.filter(pl.col("rideable_type").is_not_null())
                .group_by("rideable_type")
                .agg(pl.len().alias("count"))
                .sort("count", descending=True)
            )

            if len(rideable_breakdown) > 0:
                st.markdown("#### 🚲 Bike Type Breakdown")

                fig = go.Figure()

                fig.add_trace(
                    go.Bar(
                        x=rideable_breakdown["rideable_type"],
                        y=rideable_breakdown["count"],
                        marker=dict(color=["#2ecc71", "#f39c12", "#9b59b6"]),
                        hovertemplate="<b>%{x}</b><br>Trips: %{y:,}<extra></extra>",
                    )
                )

                fig.update_layout(
                    title="Trips by Bike Type",
                    xaxis_title="Bike Type",
                    yaxis_title="Number of Trips",
                    height=400,
                )

                st.plotly_chart(fig, width='stretch')

    else:
        st.info("💡 Apply advanced filters to see extreme trip records and detailed breakdowns.")

        # Show some aggregate stats as alternative
        st.markdown("#### System-Wide Records")

        col1, col2 = st.columns(2)

        # Busiest day
        daily_filtered = system_df.filter(
            pl.col("date").is_between(start_date, end_date)
        )

        busiest_day = daily_filtered.sort("trips", descending=True).head(1)

        if len(busiest_day) > 0:
            col1.metric(
                "Busiest Day",
                busiest_day["date"][0].strftime("%Y-%m-%d"),
                f"{busiest_day['trips'][0]:,} trips"
            )

        # Quietest day
        quietest_day = daily_filtered.sort("trips").head(1)

        if len(quietest_day) > 0:
            col2.metric(
                "Quietest Day",
                quietest_day["date"][0].strftime("%Y-%m-%d"),
                f"{quietest_day['trips'][0]:,} trips"
            )

# --------------------------------------------------
# Help Section
# --------------------------------------------------
st.markdown("---")
with st.expander("ℹ️ About This Page"):
    st.markdown(
        """
        ## Trip Analytics

        This page provides insights into Capital Bikeshare trip patterns and characteristics.

        ### Data Sources
        - **Default Mode**: Uses pre-aggregated route data for fast performance
        - **Advanced Filters**: Queries raw trip data (slower, but more detailed)

        ### Tabs
        1. **Popular Routes**: Most frequently traveled station-to-station routes
        2. **Trip Duration Analysis**: Distribution of trip lengths and patterns
        3. **Temporal Patterns**: How trips vary by hour and day of week
        4. **Extremes & Records**: Longest/shortest trips, busiest days, member breakdowns

        ### Performance Notes
        - Pre-aggregated data loads instantly
        - Advanced filters may take 10-30 seconds on first load
        - Results are cached for 1 hour

        ### Tips
        - Use the date range filter to focus on specific time periods
        - Apply member/bike type filters for targeted analysis
        - Export data using the table views in each tab
        """
    )

import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent.parent
sys.path.insert(0, str(project_root))

import streamlit as st
import polars as pl
import plotly.graph_objects as go
import plotly.express as px
from datetime import date
from streamlit_folium import st_folium

from src.capitalbike.app.io import read_parquet_from_s3
from src.capitalbike.viz.maps import create_route_map, create_system_routes_map


st.title("Trip Analytics")

# --------------------------------------------------
# Load data
# --------------------------------------------------
@st.cache_data(ttl=86400)
def load_station_routes():
    return read_parquet_from_s3(
        f"s3://{st.secrets['S3_BUCKET_PROCESSED']}/aggregates/station_routes.parquet"
    )


@st.cache_data(ttl=86400)
def load_routes_by_member_rideable():
    return read_parquet_from_s3(
        f"s3://{st.secrets['S3_BUCKET_PROCESSED']}/aggregates/routes_by_member_rideable.parquet"
    )


@st.cache_data(ttl=86400)
def load_system_daily():
    return read_parquet_from_s3(
        f"s3://{st.secrets['S3_BUCKET_PROCESSED']}/aggregates/system_daily.parquet"
    )


@st.cache_data(ttl=86400)
def load_system_daily_detailed():
    return read_parquet_from_s3(
        f"s3://{st.secrets['S3_BUCKET_PROCESSED']}/aggregates/system_daily_detailed.parquet"
    )


@st.cache_data(ttl=86400)
def load_trip_patterns():
    return read_parquet_from_s3(
        f"s3://{st.secrets['S3_BUCKET_PROCESSED']}/aggregates/trip_patterns.parquet"
    )


@st.cache_data(ttl=86400)
def load_trip_duration_buckets():
    return read_parquet_from_s3(
        f"s3://{st.secrets['S3_BUCKET_PROCESSED']}/aggregates/trip_duration_buckets.parquet"
    )


@st.cache_data(ttl=86400)
def load_stations():
    return read_parquet_from_s3(
        f"s3://{st.secrets['S3_BUCKET_PROCESSED']}/dimensions/stations.parquet"
    )


routes_df = load_station_routes()
routes_typed_df = load_routes_by_member_rideable()
system_df = load_system_daily()
system_detailed_df = load_system_daily_detailed()
patterns_df = load_trip_patterns()
duration_df = load_trip_duration_buckets()
stations_df = load_stations()

min_date = system_df["date"].min()
max_date = system_df["date"].max()

# --------------------------------------------------
# Sidebar Filters
# --------------------------------------------------
st.sidebar.header("Filters")

with st.sidebar.form("filters_form"):
    date_range = st.date_input(
        "Date Range",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
        help="Filter by date range. Defaults to all-time.",
    )

    st.markdown("---")
    st.subheader("Member & Bike Type")

    member_filter = st.multiselect(
        "Member Type",
        ["member", "casual"],
        default=["member", "casual"],
        help="Filter by rider membership status",
    )

    rideable_filter = st.multiselect(
        "Bike Type",
        ["classic_bike", "electric_bike", "docked_bike"],
        default=["classic_bike", "electric_bike", "docked_bike"],
        help="Filter by bike type (post-2020 data only)",
    )

    st.form_submit_button("Apply Filters", use_container_width=True)

# Parse date range
if isinstance(date_range, tuple) and len(date_range) == 2:
    start_date, end_date = date_range
else:
    start_date = end_date = date_range

# Determine what's actively filtered
date_filtered = start_date != min_date or end_date != max_date
member_filtered = set(member_filter) != {"member", "casual"}
rideable_filtered = set(rideable_filter) != {"classic_bike", "electric_bike", "docked_bike"}
is_filtered = date_filtered or member_filtered or rideable_filtered

# year_month strings for filtering monthly tables
start_ym = start_date.strftime("%Y-%m")
end_ym = end_date.strftime("%Y-%m")


# --------------------------------------------------
# Filter helpers (all operate on in-memory DataFrames — fast)
# --------------------------------------------------
def _apply_member_rideable(df: pl.DataFrame) -> pl.DataFrame:
    if member_filtered:
        df = df.filter(pl.col("member_type").is_in(member_filter))
    if rideable_filtered:
        df = df.filter(pl.col("rideable_type").is_in(rideable_filter))
    return df


def filtered_system_detailed() -> pl.DataFrame:
    df = system_detailed_df.filter(pl.col("date").is_between(start_date, end_date))
    return _apply_member_rideable(df)


def filtered_patterns() -> pl.DataFrame:
    df = patterns_df.filter(
        (pl.col("year_month") >= start_ym) & (pl.col("year_month") <= end_ym)
    )
    return _apply_member_rideable(df)


def filtered_duration_buckets() -> pl.DataFrame:
    df = duration_df.filter(
        (pl.col("year_month") >= start_ym) & (pl.col("year_month") <= end_ym)
    )
    return _apply_member_rideable(df)


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

    top_n = st.slider("Number of routes to display", 5, 50, 20, 5)

    if not member_filtered and not rideable_filtered:
        # Use all-time pre-aggregated routes (has lat/lng for map)
        top_routes = routes_df.head(top_n)
        show_map = True
    else:
        # Aggregate filtered routes_typed_df in memory
        filtered_routes = _apply_member_rideable(routes_typed_df)
        top_routes = (
            filtered_routes.group_by(["start_station_name", "end_station_name"])
            .agg([
                pl.col("trip_count").sum().alias("trip_count"),
                pl.col("avg_duration_sec").mean().alias("avg_duration_sec"),
            ])
            .sort("trip_count", descending=True)
            .head(top_n)
        )
        show_map = False

    if date_filtered:
        st.info("ℹ️ Route counts reflect all-time data. Date filtering is not supported for route rankings.")

    if top_routes is not None and len(top_routes) > 0:
        top_routes_display = top_routes.with_columns(
            (pl.col("start_station_name") + " → " + pl.col("end_station_name")).alias("route")
        )

        fig = go.Figure()
        fig.add_trace(
            go.Bar(
                x=top_routes_display["trip_count"][::-1],
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

        col1, col2, col3 = st.columns(3)
        col1.metric("Total Routes", f"{len(routes_df):,}")
        col2.metric("Most Popular Route", f"{top_routes['trip_count'][0]:,} trips")
        col3.metric(
            "Avg Trip Count (Top 20)",
            f"{top_routes.head(20)['trip_count'].mean():,.0f}"
        )

        if show_map:
            st.markdown("---")
            st.markdown("#### 🗺️ Route Map Visualization")
            st.markdown("Top routes visualized on the DC metro area map with color-coded popularity.")
            route_map = create_system_routes_map(top_routes, top_n=min(top_n, 15))
            st_folium(route_map, width=1200, height=500, key="popular_routes_map", returned_objects=[])
            st.caption("💡 Routes are color-coded from blue (less popular) to red (most popular). Line thickness indicates relative popularity.")
        else:
            st.info("💡 Route map is available when no member/bike type filters are applied.")

        with st.expander("📊 View Route Data Table"):
            display_df = top_routes_display.select([
                pl.col("route").alias("Route"),
                pl.col("trip_count").alias("Total Trips"),
                (pl.col("avg_duration_sec") / 60).round(1).alias("Avg Duration (min)"),
            ])
            _df = display_df.to_pandas()
            _df.index += 1
            st.dataframe(_df, width='stretch', height=400)
    else:
        st.warning("No route data available for the selected filters.")

# --------------------------------------------------
# TAB 2: Trip Duration Analysis
# --------------------------------------------------
with tab2:
    st.markdown("### Trip Duration Distribution")

    dur_buckets = filtered_duration_buckets()

    if len(dur_buckets) == 0:
        st.warning("No duration data available for the selected filters.")
    else:
        # Aggregate bucket counts across all dimensions
        bucket_agg = (
            dur_buckets.group_by("bucket_start_min")
            .agg(pl.col("trip_count").sum())
            .sort("bucket_start_min")
        )

        total_trips = bucket_agg["trip_count"].sum()

        # Approximate stats from bucket midpoints
        midpoints = bucket_agg["bucket_start_min"].cast(pl.Float64) + 2.5
        counts = bucket_agg["trip_count"].cast(pl.Float64)
        mean_dur = float((midpoints * counts).sum() / total_trips)
        # Approximate median: find bucket where cumulative count crosses 50%
        cumulative = counts.cum_sum()
        median_bucket_idx = int((cumulative < total_trips * 0.5).sum())
        median_dur = float(midpoints[median_bucket_idx]) if median_bucket_idx < len(midpoints) else mean_dur
        # Approximate 99th percentile
        p99_bucket_idx = int((cumulative < total_trips * 0.99).sum())
        p99_dur = float(bucket_agg["bucket_start_min"][min(p99_bucket_idx, len(bucket_agg) - 1)]) + 5.0

        # Bar chart (histogram from buckets)
        labels = [
            f"{r}+" if r == 120 else f"{r}–{r+5}"
            for r in bucket_agg["bucket_start_min"].to_list()
        ]

        fig = go.Figure()
        fig.add_trace(
            go.Bar(
                x=labels,
                y=bucket_agg["trip_count"].to_list(),
                marker=dict(color="#17becf", line=dict(color="white", width=1)),
                hovertemplate="Duration: %{x} min<br>Trips: %{y:,}<extra></extra>",
            )
        )
        fig.update_layout(
            title="Trip Duration Distribution",
            xaxis_title="Duration (minutes)",
            yaxis_title="Number of Trips",
            height=400,
            bargap=0.05,
            xaxis=dict(tickangle=-45),
        )
        st.plotly_chart(fig, width='stretch')

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total Trips", f"{total_trips:,}")
        col2.metric("Mean Duration", f"{mean_dur:.1f} min")
        col3.metric("Median Duration", f"{median_dur:.1f} min")
        col4.metric("~99th Percentile", f"{p99_dur:.0f} min")

        # Duration range breakdown
        st.markdown("#### Trip Duration Ranges")
        ranges = [
            ("Quick (0–10 min)", 0, 2),
            ("Short (10–20 min)", 2, 4),
            ("Medium (20–30 min)", 4, 6),
            ("Long (30–60 min)", 6, 12),
            ("Very Long (60+ min)", 12, None),
        ]
        range_data = []
        for label, start_bucket_idx, end_bucket_idx in ranges:
            if end_bucket_idx is None:
                count = int(counts[start_bucket_idx:].sum())
            else:
                count = int(counts[start_bucket_idx:end_bucket_idx].sum())
            pct = (count / total_trips) * 100
            range_data.append({"Range": label, "Count": count, "Percentage": pct})

        range_df = pl.DataFrame(range_data)

        fig = go.Figure()
        fig.add_trace(
            go.Pie(
                labels=range_df["Range"],
                values=range_df["Count"],
                hovertemplate="<b>%{label}</b><br>Trips: %{value:,}<br>%{percent}<extra></extra>",
                marker=dict(colors=px.colors.sequential.Teal),
            )
        )
        fig.update_layout(title="Trip Distribution by Duration Range", height=400)
        st.plotly_chart(fig, width='stretch')

# --------------------------------------------------
# TAB 3: Temporal Patterns
# --------------------------------------------------
with tab3:
    st.markdown("### Trip Patterns by Time")

    pats = filtered_patterns()

    if len(pats) == 0:
        st.warning("No pattern data available for the selected filters.")
    else:
        # Hour of day
        st.markdown("#### Trips by Hour of Day")
        hourly = (
            pats.group_by("hour")
            .agg(pl.col("trip_count").sum())
            .sort("hour")
        )

        fig = go.Figure()
        fig.add_trace(
            go.Bar(
                x=hourly["hour"],
                y=hourly["trip_count"],
                marker=dict(color=hourly["trip_count"], colorscale="Blues", showscale=False),
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

        # Day of week
        st.markdown("#### Trips by Day of Week")
        weekday_labels = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
        daily = (
            pats.group_by("weekday")
            .agg(pl.col("trip_count").sum())
            .sort("weekday")
        )
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

        # Peak hours
        st.markdown("#### Peak Hours")
        peak_hours = hourly.sort("trip_count", descending=True).head(3)
        col1, col2, col3 = st.columns(3)
        for i, col in enumerate([col1, col2, col3]):
            if i < len(peak_hours):
                col.metric(f"Peak #{i+1}", f"{peak_hours['hour'][i]:02d}:00", f"{peak_hours['trip_count'][i]:,} trips")

    # System-wide daily trend always shown
    st.markdown("---")
    st.markdown("#### System-Wide Daily Trends")
    daily_sys = (
        filtered_system_detailed()
        .group_by("date")
        .agg(pl.col("trips").sum())
        .sort("date")
    )

    fig = go.Figure()
    fig.add_trace(
        go.Scatter(
            x=daily_sys["date"],
            y=daily_sys["trips"],
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
    st.markdown("### Extremes & Records")

    sys_filtered = filtered_system_detailed()

    if len(sys_filtered) == 0:
        st.warning("No data available for the selected filters.")
    else:
        # Busiest / quietest day
        st.markdown("#### System-Wide Records")
        daily_totals = (
            sys_filtered.group_by("date")
            .agg(pl.col("trips").sum())
            .sort("trips", descending=True)
        )

        col1, col2 = st.columns(2)
        if len(daily_totals) > 0:
            busiest = daily_totals.head(1)
            col1.metric(
                "Busiest Day",
                busiest["date"][0].strftime("%Y-%m-%d"),
                f"{busiest['trips'][0]:,} trips",
            )
            quietest = daily_totals.tail(1)
            col2.metric(
                "Quietest Day",
                quietest["date"][0].strftime("%Y-%m-%d"),
                f"{quietest['trips'][0]:,} trips",
            )

        st.markdown("---")

        # Member type breakdown
        st.markdown("#### 👥 Member Type Breakdown")
        member_breakdown = (
            sys_filtered.group_by("member_type")
            .agg(pl.col("trips").sum().alias("count"))
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

        # Rideable type breakdown
        rideable_breakdown = (
            sys_filtered.filter(pl.col("rideable_type") != "unknown")
            .group_by("rideable_type")
            .agg(pl.col("trips").sum().alias("count"))
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

        # Year-over-year totals
        st.markdown("#### 📅 Annual Trip Totals")
        yearly = (
            sys_filtered
            .with_columns(pl.col("date").dt.year().alias("year"))
            .group_by("year")
            .agg(pl.col("trips").sum())
            .sort("year")
        )

        fig = go.Figure()
        fig.add_trace(
            go.Bar(
                x=yearly["year"],
                y=yearly["trips"],
                marker=dict(color="#8e44ad"),
                hovertemplate="<b>%{x}</b><br>Trips: %{y:,}<extra></extra>",
            )
        )
        fig.update_layout(
            title="Annual Trip Volume",
            xaxis_title="Year",
            yaxis_title="Number of Trips",
            height=400,
        )
        st.plotly_chart(fig, width='stretch')

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
        All views use pre-aggregated summary tables — no raw data is scanned at query time,
        so filtering is near-instant regardless of the date range or filters selected.

        ### Tabs
        1. **Popular Routes**: Most frequently traveled station-to-station routes
        2. **Trip Duration Analysis**: Distribution of trip lengths and patterns
        3. **Temporal Patterns**: How trips vary by hour and day of week
        4. **Extremes & Records**: Busiest days, member breakdowns, annual totals

        ### Notes
        - Route rankings reflect all-time trip counts; date filtering does not affect route order
        - Bike type data is only available for post-2020 trips
        - Duration stats (mean, median, 99th pct) are approximated from 5-minute buckets
        """
    )

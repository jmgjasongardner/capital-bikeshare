import streamlit as st
import polars as pl
from streamlit_folium import st_folium

from src.capitalbike.app.io import read_parquet_from_s3
from src.capitalbike.viz.maps import create_station_map, create_route_map
from src.capitalbike.viz.station_analysis import (
    create_hourly_heatmap,
    create_flow_chart,
    create_top_routes_bar,
    create_station_overview_metrics,
)
from src.capitalbike.viz.timeseries import create_station_timeseries


st.title("Station Explorer")

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
def load_station_hourly():
    """Load station-level hourly metrics."""
    return read_parquet_from_s3(
        f"s3://{st.secrets['S3_BUCKET_PROCESSED']}/aggregates/station_hourly.parquet"
    )


@st.cache_data(ttl=3600)
def load_station_routes():
    """Load popular routes data (if available)."""
    try:
        return read_parquet_from_s3(
            f"s3://{st.secrets['S3_BUCKET_PROCESSED']}/aggregates/station_routes.parquet"
        )
    except Exception:
        return None


stations_df = load_stations()
daily_df = load_station_daily()
hourly_df = load_station_hourly()
routes_df = load_station_routes()

# Get date range for filters
min_date = daily_df["date"].min()
max_date = daily_df["date"].max()

# --------------------------------------------------
# Session State Initialization
# --------------------------------------------------
if "view_mode" not in st.session_state:
    st.session_state.view_mode = "Station Map"
if "selected_station_name" not in st.session_state:
    st.session_state.selected_station_name = None

# --------------------------------------------------
# Sidebar: View Mode Selector
# --------------------------------------------------
st.sidebar.header("View Mode")
view_mode = st.sidebar.radio(
    "Select View",
    ["Station Map", "Station Deep Dive"],
    key="view_mode",
    help="Choose between map overview or detailed station analysis",
)

# --------------------------------------------------
# VIEW 1: Station Map
# --------------------------------------------------
if view_mode == "Station Map":
    st.subheader("Interactive Station Map")

    # Filters
    col1, col2, col3 = st.columns(3)

    with col1:
        date_range = st.date_input(
            "Date Range",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date,
            help="Filter data by date range",
        )

    with col2:
        metric = st.selectbox(
            "Metric",
            ["total_checkouts", "total_returns", "avg_duration_sec", "net_flow"],
            format_func=lambda x: {
                "total_checkouts": "Total Checkouts",
                "total_returns": "Total Returns",
                "avg_duration_sec": "Avg Duration",
                "net_flow": "Net Flow",
            }[x],
            help="Metric to visualize on the map",
        )

    with col3:
        color_scheme = st.selectbox(
            "Color Scheme",
            ["YlOrRd", "Blues", "Viridis", "Greens"],
            help="Color scheme for markers",
        )

    # Advanced Filters (Member Type & Bike Type)
    with st.expander("🔍 Advanced Filters (Member & Bike Type)", expanded=False):
        st.info("⚠️ Applying member/bike type filters will query raw trip data and may take longer to load.")

        col1, col2 = st.columns(2)

        with col1:
            member_filter = st.multiselect(
                "Member Type",
                ["member", "casual"],
                default=["member", "casual"],
                help="Filter by rider membership status",
            )

        with col2:
            rideable_filter = st.multiselect(
                "Bike Type",
                ["classic_bike", "electric_bike", "docked_bike"],
                default=["classic_bike", "electric_bike", "docked_bike"],
                help="Filter by bike type (post-2020 data only)",
            )

        use_advanced_filters = len(member_filter) < 2 or len(rideable_filter) < 3

    # Handle date range
    if isinstance(date_range, tuple) and len(date_range) == 2:
        start_date, end_date = date_range
    else:
        start_date = end_date = date_range

    # Filter and aggregate daily data
    # If advanced filters are used, query raw trips data instead of aggregates
    if use_advanced_filters:
        st.spinner("Querying raw trip data with advanced filters...")

        @st.cache_data(ttl=3600)
        def load_and_filter_trips(start, end, members, rideables):
            """Load and aggregate raw trips with member/bike type filters."""
            import polars as pl
            import os

            trips = pl.scan_parquet(
                f"s3://{st.secrets['S3_BUCKET_PROCESSED']}/master/trips/year=*/month=*/part.parquet",
                storage_options={"aws_region": os.getenv("AWS_DEFAULT_REGION", "us-east-1")}
            )

            # Apply filters
            trips_filtered = trips.filter(
                pl.col("started_at").dt.date().is_between(start, end)
            )

            # Case-insensitive member type filter
            if members:
                member_conditions = [pl.col("member_type").str.to_lowercase() == m for m in members]
                member_filter = member_conditions[0]
                for cond in member_conditions[1:]:
                    member_filter = member_filter | cond
                trips_filtered = trips_filtered.filter(member_filter)

            # Rideable type filter (handle nulls for pre-2020 data)
            if rideables and len(rideables) < 3:
                rideable_filter = pl.col("rideable_type").is_in(rideables)
                trips_filtered = trips_filtered.filter(rideable_filter)

            # Aggregate by start station
            checkouts = (
                trips_filtered
                .group_by("start_station_id")
                .agg([
                    pl.len().alias("total_checkouts"),
                    pl.col("duration_sec").mean().alias("avg_duration_sec"),
                    pl.first("start_station_name").alias("station_name"),
                ])
                .rename({"start_station_id": "station_id"})
            )

            # Aggregate by end station
            returns = (
                trips_filtered
                .group_by("end_station_id")
                .agg(pl.len().alias("total_returns"))
                .rename({"end_station_id": "station_id"})
            )

            # Join and compute net flow
            result = (
                checkouts.join(returns, on="station_id", how="full", coalesce=True)
                .with_columns([
                    pl.col("total_checkouts").fill_null(0),
                    pl.col("total_returns").fill_null(0),
                ])
                .with_columns(
                    (pl.col("total_checkouts") - pl.col("total_returns")).alias("net_flow")
                )
                .collect()
            )

            return result

        daily_filtered = load_and_filter_trips(
            start_date, end_date, member_filter, rideable_filter
        )

        # Join with station coordinates
        station_agg = daily_filtered.join(
            stations_df.select(["station_id", "lat", "lng"]),
            on="station_id",
            how="left",
            coalesce=True
        )
    else:
        # Use pre-aggregated data (faster)
        daily_filtered = daily_df.filter(pl.col("date").is_between(start_date, end_date))

        # Aggregate by station
        if metric == "net_flow":
            # Calculate net flow
            station_agg = (
                daily_filtered.group_by("station_id")
                .agg(
                    [
                        pl.sum("num_checkouts").alias("total_checkouts"),
                        pl.sum("num_returns").alias("total_returns"),
                        (pl.sum("num_checkouts") - pl.sum("num_returns")).alias("net_flow"),
                        pl.mean("avg_duration_sec").alias("avg_duration_sec"),
                        pl.first("station_name").alias("station_name"),
                    ]
                )
                .join(
                    stations_df.select(["station_id", "lat", "lng"]),
                    on="station_id",
                    how="left",
                    coalesce=True,
                )
            )
        else:
            station_agg = (
                daily_filtered.group_by("station_id")
                .agg(
                    [
                        pl.sum("num_checkouts").alias("total_checkouts"),
                        pl.sum("num_returns").alias("total_returns"),
                        pl.mean("avg_duration_sec").alias("avg_duration_sec"),
                        pl.first("station_name").alias("station_name"),
                    ]
                )
                .join(
                    stations_df.select(["station_id", "lat", "lng"]),
                    on="station_id",
                    how="left",
                    coalesce=True,
                )
            )

    # Display station count
    st.info(f"Showing {len(station_agg):,} stations")

    # Create and display map
    use_clustering = len(station_agg) > 100

    if len(station_agg) > 0:
        folium_map = create_station_map(
            station_agg,
            metric_col=metric,
            color_scheme=color_scheme,
            use_clustering=use_clustering,
        )

        # Render map without capturing interactions to prevent continuous reloads
        st_folium(
            folium_map,
            width=1200,
            height=600,
            key="station_map",
            returned_objects=[],
        )
    else:
        st.warning("No station data available for the selected date range.")

# --------------------------------------------------
# VIEW 2: Station Deep Dive
# --------------------------------------------------
else:
    st.subheader("Station Deep Dive")

    # Filters for station selection
    col1, col2 = st.columns([3, 1])

    with col2:
        # Zip code filter (if geocoding data is available)
        if "zip_code" in stations_df.columns:
            zip_codes = sorted([z for z in stations_df["zip_code"].unique().to_list() if z and z != "Unknown"])
            selected_zip = st.selectbox(
                "Filter by Zip Code",
                ["All"] + zip_codes,
                help="Filter stations by zip code",
            )

            if selected_zip != "All":
                filtered_stations = stations_df.filter(pl.col("zip_code") == selected_zip)
            else:
                filtered_stations = stations_df
        else:
            filtered_stations = stations_df
            selected_zip = "All"

    with col1:
        # Station selector
        station_list = sorted(filtered_stations["station_name"].unique().to_list())

        # Use session state for default selection if available
        default_index = 0
        if st.session_state.selected_station_name and st.session_state.selected_station_name in station_list:
            default_index = station_list.index(st.session_state.selected_station_name)

        selected_station_name = st.selectbox(
            "Select Station",
            station_list,
            index=default_index,
            help="Choose a station to analyze in detail",
        )

        # Update session state when user changes selection
        st.session_state.selected_station_name = selected_station_name

    # Get station metadata
    station_info = stations_df.filter(pl.col("station_name") == selected_station_name)

    if len(station_info) == 0:
        st.error("Station not found.")
        st.stop()

    station_id = station_info["station_id"][0]
    station_lat = station_info["lat"][0]
    station_lng = station_info["lng"][0]

    # Filter data for this station
    station_daily = daily_df.filter(pl.col("station_id") == station_id).sort("date")
    station_hourly = hourly_df.filter(pl.col("station_id") == station_id)

    # --------------------------------------------------
    # Tab 1: Overview
    # --------------------------------------------------
    tab1, tab2, tab3, tab4 = st.tabs(
        ["📊 Overview", "🔥 Hourly Heatmap", "🗺️ Popular Routes", "⚖️ Capacity Pressure"]
    )

    with tab1:
        st.markdown(f"### {selected_station_name}")

        # Calculate overview metrics
        metrics = create_station_overview_metrics(station_daily)

        # Calculate trip breakdown
        total_checkouts = station_daily["num_checkouts"].sum()
        total_returns = station_daily["num_returns"].sum() if "num_returns" in station_daily.columns else total_checkouts
        total_net_flow = total_checkouts - total_returns

        # Display metrics
        col1, col2, col3 = st.columns(3)
        col1.metric("Total Checkouts", f"{total_checkouts:,}")
        col2.metric("Total Returns", f"{total_returns:,}")
        col3.metric("Net Flow", f"{total_net_flow:+,}", help="Positive = more checkouts, Negative = more returns")

        col1, col2, col3 = st.columns(3)
        col1.metric("Avg Duration", f"{metrics['avg_duration_min']:.1f} min")
        col2.metric("Active Days", f"{metrics['active_days']:,}")
        col3.metric("Avg Daily Trips", f"{metrics['avg_daily_trips']:.0f}")

        # Station info
        first_seen = station_info['earliest_seen'][0]
        last_seen = station_info['latest_seen'][0]

        # Format dates without seconds
        if hasattr(first_seen, 'strftime'):
            first_seen_str = first_seen.strftime('%Y-%m-%d')
        else:
            first_seen_str = str(first_seen).split(' ')[0]

        # Determine if station is currently active
        # If last seen is within the last month of available data, consider it active
        from datetime import datetime, timedelta

        # Get the most recent date in the dataset
        most_recent_date = max_date

        # Calculate one month ago from most recent date
        one_month_ago = most_recent_date - timedelta(days=30)

        # Determine status
        if hasattr(last_seen, 'date'):
            last_seen_date = last_seen.date()
        else:
            last_seen_date = last_seen

        if last_seen_date >= one_month_ago:
            status_str = "**Status**: 🟢 Currently Active"
        else:
            if hasattr(last_seen, 'strftime'):
                discontinued_date = last_seen.strftime('%Y-%m-%d')
            else:
                discontinued_date = str(last_seen).split(' ')[0]
            status_str = f"**Status**: 🔴 Discontinued {discontinued_date}"

        # Get geocoding info if available
        city = station_info["city"][0] if "city" in station_info.columns else "Unknown"
        state = station_info["state"][0] if "state" in station_info.columns else "Unknown"
        zip_code = station_info["zip_code"][0] if "zip_code" in station_info.columns else "Unknown"

        st.markdown(
            f"""
            **Location**: {station_lat:.6f}, {station_lng:.6f}

            **Address**: {city}, {state} {zip_code}

            **First Observed**: {first_seen_str}

            {status_str}
            """
        )

        # Mini time-series
        st.markdown("#### Daily Checkout Trend")
        if len(station_daily) > 0:
            fig = create_station_timeseries(
                station_daily, selected_station_name, metric="num_checkouts"
            )
            st.plotly_chart(fig, width='stretch')

    # --------------------------------------------------
    # Tab 2: Hourly Heatmap
    # --------------------------------------------------
    with tab2:
        st.markdown("### Hourly Demand Pattern")

        # Metric selector for heatmap
        heatmap_metric = st.selectbox(
            "Select Metric",
            ["Checkouts", "Returns", "Net Flow"],
            key="heatmap_metric",
            help="Choose which metric to visualize in the heatmap"
        )

        st.markdown(
            f"This heatmap shows the average number of **{heatmap_metric.lower()}** by hour of day and day of week."
        )

        if len(station_hourly) > 0:
            # The station_hourly data only has checkouts currently
            # We'll need to enhance this once we add hourly returns data
            fig = create_hourly_heatmap(station_hourly, selected_station_name, metric_name=heatmap_metric)
            st.plotly_chart(fig, width='stretch')

            st.info(
                "**Tip**: Darker colors indicate higher demand. "
                "Look for patterns like weekday commute peaks (8am, 5pm) or weekend leisure rides."
            )
        else:
            st.warning("No hourly data available for this station.")

    # --------------------------------------------------
    # Tab 3: Popular Routes
    # --------------------------------------------------
    with tab3:
        st.markdown("### Popular Routes")

        if routes_df is not None:
            # Direction selector
            route_direction = st.radio(
                "Route Direction",
                ["Outbound (From This Station)", "Inbound (To This Station)"],
                horizontal=True,
                help="Outbound shows where people go FROM this station. Inbound shows where people come FROM to reach this station."
            )

            is_outbound = route_direction.startswith("Outbound")

            if is_outbound:
                # Filter routes starting from this station
                station_routes = routes_df.filter(
                    pl.col("start_station_id") == station_id
                ).sort("trip_count", descending=True)

                header_text = "Top 10 Destinations"
                other_station_col = "end_station_name"
                origin_name = selected_station_name
                origin_lat_val = station_lat
                origin_lng_val = station_lng
            else:
                # Filter routes ending at this station
                station_routes = routes_df.filter(
                    pl.col("end_station_id") == station_id
                ).sort("trip_count", descending=True)

                # For inbound, we need to create a modified dataframe that swaps columns
                # so the visualization code works correctly
                station_routes = station_routes.select([
                    pl.col("start_station_id").alias("temp_start_id"),
                    pl.col("start_station_name").alias("end_station_name"),  # Origins become "destinations" in the chart
                    pl.col("start_lat").alias("temp_start_lat"),
                    pl.col("start_lng").alias("temp_start_lng"),
                    pl.col("end_station_id").alias("temp_end_id"),
                    pl.col("end_station_name").alias("temp_end_name"),
                    pl.col("end_lat").alias("temp_end_lat"),
                    pl.col("end_lng").alias("temp_end_lng"),
                    pl.col("trip_count"),
                    pl.col("avg_duration_sec"),
                ]).select([
                    pl.col("temp_end_id").alias("start_station_id"),
                    pl.col("temp_end_name").alias("start_station_name"),
                    pl.col("temp_end_lat").alias("start_lat"),
                    pl.col("temp_end_lng").alias("start_lng"),
                    pl.col("temp_start_id").alias("end_station_id"),
                    pl.col("end_station_name"),
                    pl.col("temp_start_lat").alias("end_lat"),
                    pl.col("temp_start_lng").alias("end_lng"),
                    pl.col("trip_count"),
                    pl.col("avg_duration_sec"),
                ])

                header_text = "Top 10 Origins"
                other_station_col = "end_station_name"
                origin_name = selected_station_name
                origin_lat_val = station_lat
                origin_lng_val = station_lng

            if len(station_routes) > 0:
                # Bar chart
                st.markdown(f"#### {header_text}")

                chart_title = f"Top 10 {'Destinations from' if is_outbound else 'Origins to'} {selected_station_name}"
                fig = create_top_routes_bar(
                    station_routes, chart_title, top_n=10, is_outbound=is_outbound
                )
                st.plotly_chart(fig, width='stretch')

                # Route map
                st.markdown("#### Route Map")
                route_map = create_route_map(
                    station_routes,
                    origin_station_name=selected_station_name,
                    origin_lat=origin_lat_val,
                    origin_lng=origin_lng_val,
                    top_n=10,
                )
                st_folium(route_map, width=1200, height=500, key="route_map", returned_objects=[])
            else:
                st.info(f"No {'outbound' if is_outbound else 'inbound'} route data available for this station.")
        else:
            st.warning(
                "Route data not yet available. Run `build_station_routes()` to generate this aggregate."
            )

    # --------------------------------------------------
    # Tab 4: Capacity Pressure
    # --------------------------------------------------
    with tab4:
        st.markdown("### Capacity Pressure Analysis")
        st.markdown(
            """
            This chart shows the **net flow** of bikes at this station over time:
            - **Positive values**: More checkouts than returns (station likely emptying)
            - **Negative values**: More returns than checkouts (station likely filling up)
            - **High pressure days** (marked in red) indicate days with extreme imbalance
            """
        )

        if len(station_daily) > 0 and "num_returns" in station_daily.columns:
            fig = create_flow_chart(station_daily, selected_station_name)
            st.plotly_chart(fig, width='stretch')

            # Additional insights
            net_flow = station_daily["num_checkouts"] - station_daily["num_returns"]
            avg_net_flow = net_flow.mean()

            if avg_net_flow > 10:
                st.info(
                    f"ℹ️ This station has an average net flow of **+{avg_net_flow:.0f}** bikes per day, "
                    "suggesting it's a popular **departure point** (e.g., residential area in morning)."
                )
            elif avg_net_flow < -10:
                st.info(
                    f"ℹ️ This station has an average net flow of **{avg_net_flow:.0f}** bikes per day, "
                    "suggesting it's a popular **arrival point** (e.g., office area in morning)."
                )
            else:
                st.success(
                    "✅ This station is relatively balanced with minimal net flow."
                )
        else:
            st.warning(
                "Net flow data not available. Ensure station_daily.parquet includes num_returns column."
            )

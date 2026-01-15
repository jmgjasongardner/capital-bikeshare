import sys
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent.parent.parent.parent
sys.path.insert(0, str(project_root))

import streamlit as st
import polars as pl
from streamlit_folium import st_folium

from src.capitalbike.app.io import read_parquet_from_s3_cached, read_parquet_filtered
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
# Session State Initialization (before any data loading)
# --------------------------------------------------
if "explorer_entered" not in st.session_state:
    st.session_state.explorer_entered = False
if "view_mode" not in st.session_state:
    st.session_state.view_mode = "Station Map"
if "selected_station_name" not in st.session_state:
    st.session_state.selected_station_name = None

# Filter defaults (stored in session state so they persist)
if "filter_start_date" not in st.session_state:
    st.session_state.filter_start_date = None  # Will be set after data loads
if "filter_end_date" not in st.session_state:
    st.session_state.filter_end_date = None
if "filter_metric" not in st.session_state:
    st.session_state.filter_metric = "total_checkouts"
if "filter_color_scheme" not in st.session_state:
    st.session_state.filter_color_scheme = "YlOrRd"
if "filter_member_types" not in st.session_state:
    st.session_state.filter_member_types = ["member", "casual"]
if "filter_rideable_types" not in st.session_state:
    st.session_state.filter_rideable_types = ["classic_bike", "electric_bike", "docked_bike"]


# --------------------------------------------------
# Data Loading Functions
# --------------------------------------------------
# Note: Using read_parquet_from_s3_cached (cache_resource) to avoid
# serialization overhead. This saves ~50% memory vs cache_data.

def load_stations():
    """Load station dimension data (~0.1 MB)."""
    return read_parquet_from_s3_cached(
        f"s3://{st.secrets['S3_BUCKET_PROCESSED']}/dimensions/stations.parquet"
    )


def load_station_daily():
    """Load station-level daily metrics (~190 MB)."""
    return read_parquet_from_s3_cached(
        f"s3://{st.secrets['S3_BUCKET_PROCESSED']}/aggregates/station_daily.parquet"
    )


def load_station_hourly_for_station(station_id: int):
    """
    Load hourly data for a specific station only.

    This avoids loading the full 1.6GB hourly dataset into memory.
    Only the rows for the requested station are loaded (~2-5 MB per station).
    """
    return read_parquet_filtered(
        f"s3://{st.secrets['S3_BUCKET_PROCESSED']}/aggregates/station_hourly.parquet",
        filter_col="station_id",
        filter_value=station_id,
    )


def load_station_routes():
    """Load popular routes data (~2 MB)."""
    try:
        return read_parquet_from_s3_cached(
            f"s3://{st.secrets['S3_BUCKET_PROCESSED']}/aggregates/station_routes.parquet"
        )
    except Exception:
        return None


def load_station_daily_detailed():
    """Load detailed station daily metrics (~574 MB)."""
    try:
        return read_parquet_from_s3_cached(
            f"s3://{st.secrets['S3_BUCKET_PROCESSED']}/aggregates/station_daily_detailed.parquet"
        )
    except Exception:
        return None


# --------------------------------------------------
# Configuration Screen (shown before data loads)
# --------------------------------------------------
if not st.session_state.explorer_entered:
    st.markdown("""
    Configure your view settings below, then click **Enter Station Explorer** to load the data.

    *Tip: Loading takes ~15 seconds on first visit (data is cached for subsequent views).*
    """)

    st.divider()

    with st.form("config_form"):
        st.subheader("Map Settings")

        col1, col2 = st.columns(2)

        with col1:
            config_metric = st.selectbox(
                "Metric to Display",
                ["total_checkouts", "total_returns", "avg_duration_sec", "net_flow"],
                index=0,
                format_func=lambda x: {
                    "total_checkouts": "Total Checkouts",
                    "total_returns": "Total Returns",
                    "avg_duration_sec": "Avg Duration",
                    "net_flow": "Net Flow",
                }[x],
                help="Metric shown on station markers",
            )

        with col2:
            config_color = st.selectbox(
                "Color Scheme",
                ["YlOrRd", "Blues", "Viridis", "Greens"],
                index=0,
                help="Color palette for map markers",
            )

        st.subheader("Data Filters")

        col1, col2 = st.columns(2)

        with col1:
            config_member = st.multiselect(
                "Member Types",
                ["member", "casual", "unknown"],
                default=["member", "casual"],
                help="Filter by rider membership status",
            )

        with col2:
            config_rideable = st.multiselect(
                "Bike Types",
                ["classic_bike", "electric_bike", "docked_bike", "unknown"],
                default=["classic_bike", "electric_bike", "docked_bike"],
                help="Filter by bike type",
            )

        st.caption("*Date range defaults to all available data. You can adjust after loading.*")

        st.divider()

        submitted = st.form_submit_button(
            "Enter Station Explorer",
            type="primary",
            use_container_width=True,
        )

        if submitted:
            # Save configuration to session state
            st.session_state.filter_metric = config_metric
            st.session_state.filter_color_scheme = config_color
            st.session_state.filter_member_types = config_member
            st.session_state.filter_rideable_types = config_rideable
            st.session_state.explorer_entered = True
            st.rerun()

    # Stop here - don't load data until user enters
    st.stop()


# --------------------------------------------------
# Load Data (only after user enters)
# --------------------------------------------------
# Progress container for loading feedback
progress_container = st.empty()

with progress_container.container():
    st.info("Loading Station Explorer data...")

    progress_bar = st.progress(0, text="Loading station locations...")
    stations_df = load_stations()

    progress_bar.progress(25, text="Loading daily station metrics...")
    daily_df = load_station_daily()

    progress_bar.progress(50, text="Loading route data...")
    routes_df = load_station_routes()

    progress_bar.progress(75, text="Loading detailed metrics...")
    detailed_df = load_station_daily_detailed()

    progress_bar.progress(100, text="Data loaded!")
    # Note: Hourly data (1.6GB) is loaded on-demand per station in Deep Dive

# Clear the progress display
progress_container.empty()

# Get date range for filters
min_date = daily_df["date"].min()
max_date = daily_df["date"].max()

# Set date defaults if not already set
if st.session_state.filter_start_date is None:
    st.session_state.filter_start_date = min_date
if st.session_state.filter_end_date is None:
    st.session_state.filter_end_date = max_date

# --------------------------------------------------
# Sidebar: View Mode & Filters
# --------------------------------------------------
st.sidebar.header("View Mode")
view_mode = st.sidebar.radio(
    "Select View",
    ["Station Map", "Station Deep Dive"],
    key="view_mode",
    help="Choose between map overview or detailed station analysis",
)

# Sidebar filter form (applies to Station Map view)
st.sidebar.divider()
st.sidebar.header("Filters")

with st.sidebar.form("filter_form"):
    # Date range
    sidebar_date_range = st.date_input(
        "Date Range",
        value=(st.session_state.filter_start_date, st.session_state.filter_end_date),
        min_value=min_date,
        max_value=max_date,
        help="Filter data by date range",
    )

    # Metric and color
    sidebar_metric = st.selectbox(
        "Metric",
        ["total_checkouts", "total_returns", "avg_duration_sec", "net_flow"],
        index=["total_checkouts", "total_returns", "avg_duration_sec", "net_flow"].index(
            st.session_state.filter_metric
        ),
        format_func=lambda x: {
            "total_checkouts": "Total Checkouts",
            "total_returns": "Total Returns",
            "avg_duration_sec": "Avg Duration",
            "net_flow": "Net Flow",
        }[x],
        help="Metric to visualize on the map",
    )

    sidebar_color = st.selectbox(
        "Color Scheme",
        ["YlOrRd", "Blues", "Viridis", "Greens"],
        index=["YlOrRd", "Blues", "Viridis", "Greens"].index(
            st.session_state.filter_color_scheme
        ),
        help="Color palette for markers",
    )

    # Member and bike type filters
    if detailed_df is not None:
        st.caption("**Advanced Filters**")
        sidebar_member = st.multiselect(
            "Member Types",
            ["member", "casual", "unknown"],
            default=st.session_state.filter_member_types,
            help="Filter by rider membership status",
        )

        sidebar_rideable = st.multiselect(
            "Bike Types",
            ["classic_bike", "electric_bike", "docked_bike", "unknown"],
            default=st.session_state.filter_rideable_types,
            help="Filter by bike type",
        )
    else:
        sidebar_member = ["member", "casual"]
        sidebar_rideable = ["classic_bike", "electric_bike", "docked_bike"]

    # Apply button
    filter_submitted = st.form_submit_button(
        "Apply Filters",
        type="primary",
        use_container_width=True,
    )

    if filter_submitted:
        # Update session state with new filter values
        if isinstance(sidebar_date_range, tuple) and len(sidebar_date_range) == 2:
            st.session_state.filter_start_date, st.session_state.filter_end_date = sidebar_date_range
        else:
            st.session_state.filter_start_date = st.session_state.filter_end_date = sidebar_date_range
        st.session_state.filter_metric = sidebar_metric
        st.session_state.filter_color_scheme = sidebar_color
        st.session_state.filter_member_types = sidebar_member
        st.session_state.filter_rideable_types = sidebar_rideable
        # Clear map cache to force regeneration
        st.session_state.map_filter_key = None
        st.rerun()

# Reset button (separate from form)
if st.sidebar.button("Reset to Defaults", use_container_width=True):
    st.session_state.explorer_entered = False
    st.session_state.filter_start_date = None
    st.session_state.filter_end_date = None
    st.session_state.filter_metric = "total_checkouts"
    st.session_state.filter_color_scheme = "YlOrRd"
    st.session_state.filter_member_types = ["member", "casual"]
    st.session_state.filter_rideable_types = ["classic_bike", "electric_bike", "docked_bike"]
    st.session_state.map_filter_key = None
    st.rerun()

# Use session state filter values
start_date = st.session_state.filter_start_date
end_date = st.session_state.filter_end_date
metric = st.session_state.filter_metric
color_scheme = st.session_state.filter_color_scheme
member_filter = st.session_state.filter_member_types
rideable_filter = st.session_state.filter_rideable_types

# --------------------------------------------------
# VIEW 1: Station Map
# --------------------------------------------------
if view_mode == "Station Map":
    st.subheader("Interactive Station Map")

    # Show current filter summary
    st.caption(
        f"**Active filters:** {start_date} to {end_date} | "
        f"Metric: {metric.replace('_', ' ').title()} | "
        f"Members: {', '.join(member_filter)} | "
        f"Bikes: {', '.join(r.replace('_', ' ') for r in rideable_filter)}"
    )

    # Create a unique filter key to determine when to recalculate data
    filter_key = (
        str(start_date),
        str(end_date),
        tuple(sorted(member_filter)),
        tuple(sorted(rideable_filter)),
        metric,
        color_scheme,
    )

    # Initialize session state for map caching
    if "map_filter_key" not in st.session_state:
        st.session_state.map_filter_key = None
    if "cached_station_agg" not in st.session_state:
        st.session_state.cached_station_agg = None
    if "cached_folium_map" not in st.session_state:
        st.session_state.cached_folium_map = None

    # Only recalculate if filters have changed
    if st.session_state.map_filter_key != filter_key:
        # Use detailed data if available, otherwise fall back to basic aggregates
        if detailed_df is not None:
            # Filter detailed daily data by date and member/bike type
            daily_filtered = detailed_df.filter(
                pl.col("date").is_between(start_date, end_date)
                & pl.col("member_type").is_in(member_filter)
                & pl.col("rideable_type").is_in(rideable_filter)
            )

            # Aggregate by station
            station_agg = (
                daily_filtered.group_by("station_id")
                .agg([
                    pl.sum("num_checkouts").alias("total_checkouts"),
                    pl.sum("num_returns").alias("total_returns"),
                    pl.mean("avg_duration_checkout_sec").alias("avg_duration_checkout_sec"),
                    pl.mean("avg_duration_return_sec").alias("avg_duration_return_sec"),
                    pl.first("station_name").alias("station_name"),
                ])
                .with_columns([
                    (pl.col("total_checkouts") - pl.col("total_returns")).alias("net_flow"),
                    (pl.col("avg_duration_checkout_sec") / 60).alias("avg_duration_checkout_min"),
                    (pl.col("avg_duration_return_sec") / 60).alias("avg_duration_return_min"),
                    # Keep avg_duration_sec for backward compatibility (use checkout duration)
                    (pl.col("avg_duration_checkout_sec")).alias("avg_duration_sec"),
                ])
                .join(
                    stations_df.select(["station_id", "lat", "lng"]),
                    on="station_id",
                    how="left",
                    coalesce=True,
                )
            )
        else:
            # Fallback: Use basic daily_df without advanced filters
            daily_filtered = daily_df.filter(pl.col("date").is_between(start_date, end_date))

            # Aggregate by station (old logic)
            station_agg = (
                daily_filtered.group_by("station_id")
                .agg([
                    pl.sum("num_checkouts").alias("total_checkouts"),
                    pl.sum("num_returns").alias("total_returns"),
                    pl.mean("avg_duration_sec").alias("avg_duration_sec"),
                    pl.first("station_name").alias("station_name"),
                ])
                .with_columns([
                    (pl.col("total_checkouts") - pl.col("total_returns")).alias("net_flow"),
                    # Use same duration for both checkout and return (no separation)
                    (pl.col("avg_duration_sec") / 60).alias("avg_duration_checkout_min"),
                    (pl.col("avg_duration_sec") / 60).alias("avg_duration_return_min"),
                ])
                .join(
                    stations_df.select(["station_id", "lat", "lng"]),
                    on="station_id",
                    how="left",
                    coalesce=True,
                )
            )

        # Create map with rich tooltips showing multiple metrics
        use_clustering = len(station_agg) > 100

        if len(station_agg) > 0:
            folium_map = create_station_map(
                station_agg,
                metric_col=metric,
                color_scheme=color_scheme,
                use_clustering=use_clustering,
                tooltip_cols={
                    "total_checkouts": "Checkouts",
                    "total_returns": "Returns",
                    "net_flow": "Net Flow",
                    "avg_duration_checkout_min": "Avg Duration - Checkout (min)",
                    "avg_duration_return_min": "Avg Duration - Return (min)",
                }
            )
        else:
            folium_map = None

        # Cache the results
        st.session_state.cached_station_agg = station_agg
        st.session_state.cached_folium_map = folium_map
        st.session_state.map_filter_key = filter_key
    else:
        # Use cached data (filters haven't changed)
        station_agg = st.session_state.cached_station_agg
        folium_map = st.session_state.cached_folium_map

    # Display station count
    st.info(f"Showing {len(station_agg):,} stations")

    # Render map if available
    if folium_map is not None:
        # Render map (no click tracking for instant zoom/pan)
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
    # Hourly data loaded on-demand (saves 1.6GB memory vs loading full dataset)
    station_hourly = load_station_hourly_for_station(station_id)

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

        # Handle stations with missing coordinates
        if station_lat is not None and station_lng is not None:
            location_str = f"{station_lat:.6f}, {station_lng:.6f}"
        else:
            location_str = "Coordinates unavailable"

        st.markdown(
            f"""
            **Location**: {location_str}

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
            ["Net Flow", "Checkouts", "Returns"],
            key="heatmap_metric",
            help="Choose which metric to visualize in the heatmap"
        )

        if heatmap_metric == "Net Flow":
            st.markdown(
                "This heatmap shows the average **net flow** (checkouts minus returns) by hour and day of week."
            )
            st.caption(
                "🔴 **Red (positive)** = more checkouts than returns (bikes leaving)  \n"
                "🔵 **Blue (negative)** = more returns than checkouts (bikes arriving)"
            )
        else:
            st.markdown(
                f"This heatmap shows the average number of **{heatmap_metric.lower()}** by hour of day and day of week."
            )

        if len(station_hourly) > 0:
            fig = create_hourly_heatmap(station_hourly, selected_station_name, metric_name=heatmap_metric)
            st.plotly_chart(fig, width='stretch')

            if heatmap_metric == "Net Flow":
                st.info(
                    "**Tip**: Net flow reveals station pressure patterns. "
                    "Red cells indicate times when bikes are leaving (potential emptying risk). "
                    "Blue cells indicate times when bikes are arriving (potential filling risk)."
                )
            else:
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
        st.markdown(f"### Popular Routes for {selected_station_name}")

        st.info(f"""
        📍 **Viewing routes for:** {selected_station_name}

        - **Outbound**: Shows top destinations where riders go FROM {selected_station_name}
        - **Inbound**: Shows top origins where riders come FROM to reach {selected_station_name}

        The map shows {selected_station_name} as a ⭐ red star, with lines showing the most popular routes.
        """)

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

                if is_outbound:
                    st.caption(f"⭐ Red star = {selected_station_name} | Blue lines = Routes to top 10 destinations")
                else:
                    st.caption(f"⭐ Red star = {selected_station_name} | Blue lines = Routes from top 10 origins")

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

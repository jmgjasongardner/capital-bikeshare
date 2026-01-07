"""
Reusable Folium map visualization components for Capital Bikeshare analytics.
"""

from __future__ import annotations

import polars as pl
import folium
from folium.plugins import MarkerCluster
import branca.colormap as cm


# DC metro area center coordinates
DC_CENTER = [38.9072, -77.0369]
DEFAULT_ZOOM = 12


def create_station_map(
    stations_df: pl.DataFrame,
    metric_col: str = "total_checkouts",
    color_scheme: str = "YlOrRd",
    zoom_start: int = DEFAULT_ZOOM,
    use_clustering: bool = False,
    tooltip_cols: dict[str, str] = None,
) -> folium.Map:
    """
    Create an interactive Folium map with station markers.

    Args:
        stations_df: DataFrame with columns: station_name, lat, lng, <metric_col>
        metric_col: Column name to use for color-coding circles
        color_scheme: Color scheme name (YlOrRd, Blues, Viridis, etc.)
        zoom_start: Initial zoom level
        use_clustering: Whether to use marker clustering (recommended for > 100 stations)
        tooltip_cols: Optional dict of {column_name: label} for additional metrics in tooltip

    Returns:
        Folium Map object
    """
    # Create base map
    m = folium.Map(
        location=DC_CENTER,
        zoom_start=zoom_start,
        tiles="OpenStreetMap",
    )

    # Ensure we have the required columns
    required_cols = ["station_name", "lat", "lng", metric_col]
    missing = [col for col in required_cols if col not in stations_df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    # Create colormap
    metric_values = stations_df[metric_col].to_list()
    vmin = min(metric_values)
    vmax = max(metric_values)

    colormap = _get_colormap(color_scheme, vmin, vmax)

    # Add colormap legend to map
    colormap.add_to(m)

    # Prepare marker cluster if needed
    if use_clustering:
        marker_cluster = MarkerCluster().add_to(m)
        target = marker_cluster
    else:
        target = m

    # Add circle markers for each station
    for row in stations_df.iter_rows(named=True):
        lat = row["lat"]
        lng = row["lng"]
        station_name = row["station_name"]
        metric_value = row[metric_col]

        # Skip if coordinates are invalid
        if not lat or not lng or lat == 0 or lng == 0:
            continue

        # Determine color based on metric value
        color = colormap(metric_value)

        # Scale radius based on metric value (min 5, max 20)
        radius = _scale_radius(metric_value, vmin, vmax, min_r=5, max_r=20)

        # Build tooltip with multiple metrics
        tooltip_parts = [f"<b>{station_name}</b>"]

        # Add main metric
        tooltip_parts.append(f"{_format_metric(metric_col)}: {metric_value:,.0f}")

        # Add additional metrics if provided
        if tooltip_cols:
            for col, label in tooltip_cols.items():
                if col in row:
                    value = row[col]
                    # Handle None/null values
                    if value is None:
                        tooltip_parts.append(f"{label}: N/A")
                    # Format floats vs integers differently
                    elif isinstance(value, float):
                        tooltip_parts.append(f"{label}: {value:,.1f}")
                    else:
                        tooltip_parts.append(f"{label}: {value:,}")

        tooltip_text = "<br>".join(tooltip_parts)

        # Add circle marker
        folium.CircleMarker(
            location=[lat, lng],
            radius=radius,
            popup=folium.Popup(tooltip_text, max_width=300),
            tooltip=tooltip_text,
            color=color,
            fill=True,
            fillColor=color,
            fillOpacity=0.7,
            weight=2,
        ).add_to(target)

    return m


def create_route_map(
    routes_df: pl.DataFrame,
    origin_station_name: str,
    origin_lat: float,
    origin_lng: float,
    top_n: int = 10,
) -> folium.Map:
    """
    Create a map showing popular routes from a specific origin station.

    Args:
        routes_df: DataFrame with columns: end_station_name, end_lat, end_lng, trip_count
        origin_station_name: Name of the origin station
        origin_lat: Origin latitude
        origin_lng: Origin longitude
        top_n: Number of top routes to display

    Returns:
        Folium Map object
    """
    # Create base map centered on origin
    m = folium.Map(
        location=[origin_lat, origin_lng],
        zoom_start=13,
        tiles="OpenStreetMap",
    )

    # Add origin station marker (star icon)
    folium.Marker(
        location=[origin_lat, origin_lng],
        popup=f"<b>{origin_station_name}</b> (Origin)",
        tooltip=origin_station_name,
        icon=folium.Icon(color="red", icon="star"),
    ).add_to(m)

    # Take top N routes
    top_routes = routes_df.head(top_n)

    # Get trip count range for line width scaling
    trip_counts = top_routes["trip_count"].to_list()
    min_trips = min(trip_counts)
    max_trips = max(trip_counts)

    # Add lines and destination markers for each route
    for idx, row in enumerate(top_routes.iter_rows(named=True), start=1):
        end_lat = row["end_lat"]
        end_lng = row["end_lng"]
        end_station_name = row["end_station_name"]
        trip_count = row["trip_count"]

        # Skip invalid coordinates
        if not end_lat or not end_lng:
            continue

        # Scale line weight based on trip count (min 2, max 8)
        weight = _scale_value(trip_count, min_trips, max_trips, min_val=2, max_val=8)

        # Draw polyline from origin to destination
        folium.PolyLine(
            locations=[[origin_lat, origin_lng], [end_lat, end_lng]],
            color="blue",
            weight=weight,
            opacity=0.6,
            popup=f"{trip_count:,} trips",
        ).add_to(m)

        # Add destination marker
        folium.CircleMarker(
            location=[end_lat, end_lng],
            radius=8,
            popup=f"<b>#{idx}: {end_station_name}</b><br>{trip_count:,} trips",
            tooltip=f"#{idx}: {end_station_name}",
            color="blue",
            fill=True,
            fillColor="lightblue",
            fillOpacity=0.7,
        ).add_to(m)

    return m


def create_system_routes_map(
    routes_df: pl.DataFrame,
    top_n: int = 20,
) -> folium.Map:
    """
    Create a map showing popular routes across the entire system.

    Unlike create_route_map which shows routes from a single origin,
    this function displays independent routes between any station pairs.

    Args:
        routes_df: DataFrame with columns: start_station_name, end_station_name,
                   start_lat, start_lng, end_lat, end_lng, trip_count
        top_n: Number of top routes to display

    Returns:
        Folium Map object
    """
    # Calculate map center from all stations in top routes
    top_routes = routes_df.head(top_n)

    # Collect all unique coordinates
    all_lats = []
    all_lngs = []

    for row in top_routes.iter_rows(named=True):
        if row.get("start_lat") and row.get("start_lng"):
            all_lats.append(row["start_lat"])
            all_lngs.append(row["start_lng"])
        if row.get("end_lat") and row.get("end_lng"):
            all_lats.append(row["end_lat"])
            all_lngs.append(row["end_lng"])

    # Calculate center
    if all_lats and all_lngs:
        center_lat = sum(all_lats) / len(all_lats)
        center_lng = sum(all_lngs) / len(all_lngs)
    else:
        # Default to DC center
        center_lat, center_lng = 38.9072, -77.0369

    # Create base map
    m = folium.Map(
        location=[center_lat, center_lng],
        zoom_start=12,
        tiles="OpenStreetMap",
    )

    # Get trip count range for color/weight scaling
    trip_counts = top_routes["trip_count"].to_list()
    min_trips = min(trip_counts) if trip_counts else 0
    max_trips = max(trip_counts) if trip_counts else 1

    # Create colormap from blue (low) to red (high)
    import matplotlib.colors as mcolors
    cmap = mcolors.LinearSegmentedColormap.from_list(
        "route_popularity", ["#3498db", "#f39c12", "#e74c3c"]
    )

    # Track unique stations to avoid duplicate markers
    station_markers = {}

    # Add lines for each route
    for idx, row in enumerate(top_routes.iter_rows(named=True), start=1):
        start_lat = row.get("start_lat")
        start_lng = row.get("start_lng")
        end_lat = row.get("end_lat")
        end_lng = row.get("end_lng")
        start_name = row.get("start_station_name", "Unknown")
        end_name = row.get("end_station_name", "Unknown")
        trip_count = row.get("trip_count", 0)

        # Skip if missing coordinates
        if not all([start_lat, start_lng, end_lat, end_lng]):
            continue

        # Calculate normalized color value (0-1)
        if max_trips > min_trips:
            norm_value = (trip_count - min_trips) / (max_trips - min_trips)
        else:
            norm_value = 0.5

        # Get color from colormap
        rgba = cmap(norm_value)
        hex_color = mcolors.to_hex(rgba)

        # Scale line weight (thicker = more popular)
        weight = _scale_value(trip_count, min_trips, max_trips, min_val=2, max_val=6)

        # Draw polyline
        folium.PolyLine(
            locations=[[start_lat, start_lng], [end_lat, end_lng]],
            color=hex_color,
            weight=weight,
            opacity=0.7,
            popup=f"<b>#{idx}: {start_name} → {end_name}</b><br>{trip_count:,} trips",
            tooltip=f"#{idx}: {trip_count:,} trips",
        ).add_to(m)

        # Add station markers (only once per station)
        for station_name, lat, lng in [
            (start_name, start_lat, start_lng),
            (end_name, end_lat, end_lng)
        ]:
            station_key = (lat, lng)
            if station_key not in station_markers:
                folium.CircleMarker(
                    location=[lat, lng],
                    radius=5,
                    popup=f"<b>{station_name}</b>",
                    tooltip=station_name,
                    color="#2c3e50",
                    fill=True,
                    fillColor="#ecf0f1",
                    fillOpacity=0.8,
                    weight=2,
                ).add_to(m)
                station_markers[station_key] = station_name

    return m


def _get_colormap(scheme: str, vmin: float, vmax: float) -> cm.LinearColormap:
    """
    Create a Branca colormap for the given scheme and value range.

    Args:
        scheme: Colormap scheme name
        vmin: Minimum value
        vmax: Maximum value

    Returns:
        LinearColormap object
    """
    # Map scheme names to Branca colormaps
    scheme_map = {
        "YlOrRd": cm.linear.YlOrRd_09,
        "Blues": cm.linear.Blues_09,
        "Viridis": cm.linear.viridis,
        "Greens": cm.linear.Greens_09,
        "Reds": cm.linear.Reds_09,
    }

    colormap = scheme_map.get(scheme, cm.linear.YlOrRd_09)
    colormap = colormap.scale(vmin, vmax)

    return colormap


def _scale_radius(value: float, vmin: float, vmax: float, min_r: float = 5, max_r: float = 20) -> float:
    """
    Scale a value to a radius within the given range.

    Args:
        value: Value to scale
        vmin: Minimum value in dataset
        vmax: Maximum value in dataset
        min_r: Minimum radius
        max_r: Maximum radius

    Returns:
        Scaled radius
    """
    if vmax == vmin:
        return (min_r + max_r) / 2

    # Linear scaling
    normalized = (value - vmin) / (vmax - vmin)
    return min_r + (normalized * (max_r - min_r))


def _scale_value(value: float, vmin: float, vmax: float, min_val: float, max_val: float) -> float:
    """
    Generic linear scaling function.

    Args:
        value: Value to scale
        vmin: Minimum value in dataset
        vmax: Maximum value in dataset
        min_val: Minimum output value
        max_val: Maximum output value

    Returns:
        Scaled value
    """
    if vmax == vmin:
        return (min_val + max_val) / 2

    normalized = (value - vmin) / (vmax - vmin)
    return min_val + (normalized * (max_val - min_val))


def _format_metric(metric_col: str) -> str:
    """
    Convert metric column name to display-friendly label.

    Args:
        metric_col: Column name

    Returns:
        Formatted label
    """
    labels = {
        "total_checkouts": "Total Checkouts",
        "num_checkouts": "Checkouts",
        "avg_duration_sec": "Avg Duration (min)",
        "net_flow": "Net Flow",
        "trip_count": "Trips",
    }

    return labels.get(metric_col, metric_col.replace("_", " ").title())

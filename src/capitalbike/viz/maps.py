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

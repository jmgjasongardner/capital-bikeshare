"""
Visualization components for station-level demand analysis.
"""

from __future__ import annotations

import polars as pl
import plotly.graph_objects as go
import plotly.express as px
import numpy as np


WEEKDAY_LABELS = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]
HOUR_LABELS = [f"{h:02d}:00" for h in range(24)]


def create_hourly_heatmap(hourly_df: pl.DataFrame, station_name: str) -> go.Figure:
    """
    Create a heatmap showing demand patterns by hour of day and day of week.

    Args:
        hourly_df: DataFrame with columns: date, hour, num_checkouts
                   Should be pre-filtered for a single station
        station_name: Name of the station for the title

    Returns:
        Plotly Figure object
    """
    # Add weekday column (Polars returns 1-7, we need 0-6 for indexing)
    df = hourly_df.with_columns(
        (pl.col("date").dt.weekday() - 1).alias("weekday")
    )

    # Aggregate by weekday and hour
    pivot_data = (
        df.group_by(["weekday", "hour"])
        .agg(pl.mean("num_checkouts").alias("avg_checkouts"))
        .sort(["weekday", "hour"])
    )

    # Create pivot table: rows=hours, columns=weekdays
    # Initialize matrix with zeros
    matrix = np.zeros((24, 7))

    for row in pivot_data.iter_rows(named=True):
        weekday = row["weekday"]
        hour = row["hour"]
        avg_checkouts = row["avg_checkouts"]

        # Ensure indices are within bounds
        if 0 <= weekday < 7 and 0 <= hour < 24:
            matrix[hour, weekday] = avg_checkouts

    # Create heatmap
    fig = go.Figure(
        data=go.Heatmap(
            z=matrix,
            x=WEEKDAY_LABELS,
            y=HOUR_LABELS,
            colorscale="YlOrRd",
            hovertemplate="<b>%{x} at %{y}</b><br>Avg Checkouts: %{z:.1f}<extra></extra>",
            colorbar=dict(title="Avg<br>Checkouts"),
        )
    )

    fig.update_layout(
        title=f"{station_name}: Hourly Demand Pattern",
        xaxis_title="Day of Week",
        yaxis_title="Hour of Day",
        height=600,
        xaxis=dict(side="bottom"),
        yaxis=dict(autorange="reversed"),  # Hour 00 at top
    )

    return fig


def create_flow_chart(daily_df: pl.DataFrame, station_name: str) -> go.Figure:
    """
    Create a line chart showing net flow (checkouts - returns) over time.

    Positive values indicate bikes are leaving the station (risk of emptying).
    Negative values indicate bikes are arriving (risk of filling up).

    Args:
        daily_df: DataFrame with columns: date, num_checkouts, num_returns
        station_name: Name of the station

    Returns:
        Plotly Figure object
    """
    # Calculate net flow
    df = daily_df.with_columns(
        (pl.col("num_checkouts") - pl.col("num_returns")).alias("net_flow")
    )

    # Calculate capacity pressure threshold (90th percentile)
    net_flow_abs = df["net_flow"].abs()
    threshold = net_flow_abs.quantile(0.9)

    # Identify high-pressure days
    high_pressure = df.filter(net_flow_abs > threshold)

    fig = go.Figure()

    # Add main net flow line
    fig.add_trace(
        go.Scatter(
            x=df["date"],
            y=df["net_flow"],
            name="Net Flow",
            mode="lines",
            line=dict(color="#17becf", width=1.5),
            hovertemplate="<b>%{x|%Y-%m-%d}</b><br>Net Flow: %{y:+.0f}<extra></extra>",
        )
    )

    # Add zero reference line
    fig.add_hline(
        y=0,
        line_dash="dash",
        line_color="gray",
        annotation_text="Balanced",
        annotation_position="right",
    )

    # Add threshold lines
    fig.add_hline(
        y=threshold,
        line_dash="dot",
        line_color="red",
        opacity=0.5,
        annotation_text=f"High Pressure (+{threshold:.0f})",
        annotation_position="top right",
    )
    fig.add_hline(
        y=-threshold,
        line_dash="dot",
        line_color="red",
        opacity=0.5,
        annotation_text=f"High Pressure (-{threshold:.0f})",
        annotation_position="bottom right",
    )

    # Highlight high-pressure days
    if len(high_pressure) > 0:
        fig.add_trace(
            go.Scatter(
                x=high_pressure["date"],
                y=high_pressure["net_flow"],
                name="High Pressure Days",
                mode="markers",
                marker=dict(color="red", size=8, symbol="diamond"),
                hovertemplate="<b>%{x|%Y-%m-%d}</b><br>High Pressure: %{y:+.0f}<extra></extra>",
            )
        )

    fig.update_layout(
        title=f"{station_name}: Capacity Pressure Analysis",
        xaxis_title="Date",
        yaxis_title="Net Flow (Checkouts - Returns)",
        hovermode="x",
        height=400,
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
        annotations=[
            dict(
                text="<i>Positive = likely emptying  |  Negative = likely filling</i>",
                xref="paper",
                yref="paper",
                x=0.5,
                y=-0.15,
                showarrow=False,
                font=dict(size=10, color="gray"),
            )
        ],
    )

    return fig


def create_top_routes_bar(routes_df: pl.DataFrame, station_name: str, top_n: int = 10) -> go.Figure:
    """
    Create a horizontal bar chart showing top destination stations.

    Args:
        routes_df: DataFrame with columns: end_station_name, trip_count
        station_name: Origin station name
        top_n: Number of top routes to show

    Returns:
        Plotly Figure object
    """
    # Take top N and reverse for horizontal bar (so #1 is at top)
    top_routes = routes_df.head(top_n).reverse()

    fig = go.Figure()

    fig.add_trace(
        go.Bar(
            x=top_routes["trip_count"],
            y=top_routes["end_station_name"],
            orientation="h",
            marker=dict(
                color=top_routes["trip_count"],
                colorscale="Blues",
                showscale=False,
            ),
            hovertemplate="<b>%{y}</b><br>Trips: %{x:,}<extra></extra>",
        )
    )

    fig.update_layout(
        title=f"Top {top_n} Destinations from {station_name}",
        xaxis_title="Number of Trips",
        yaxis_title="",
        height=400,
        margin=dict(l=200, r=20, t=60, b=40),  # Extra left margin for long station names
    )

    return fig


def create_station_overview_metrics(
    daily_df: pl.DataFrame,
) -> dict[str, any]:
    """
    Calculate overview metrics for a station.

    Args:
        daily_df: DataFrame with columns: date, num_checkouts, num_returns, avg_duration_sec

    Returns:
        Dictionary with keys: total_trips, avg_duration_min, active_days, avg_daily_trips
    """
    total_checkouts = daily_df["num_checkouts"].sum()
    total_returns = daily_df["num_returns"].sum() if "num_returns" in daily_df.columns else total_checkouts
    total_trips = (total_checkouts + total_returns) // 2  # Average to avoid double-counting

    avg_duration_min = daily_df["avg_duration_sec"].mean() / 60 if "avg_duration_sec" in daily_df.columns else 0

    active_days = len(daily_df)

    avg_daily_trips = total_trips / active_days if active_days > 0 else 0

    return {
        "total_trips": int(total_trips),
        "avg_duration_min": float(avg_duration_min),
        "active_days": active_days,
        "avg_daily_trips": float(avg_daily_trips),
    }

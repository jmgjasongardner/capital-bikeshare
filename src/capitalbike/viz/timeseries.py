"""
Reusable Plotly time-series visualization components for Capital Bikeshare analytics.
"""

from __future__ import annotations

import polars as pl
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import numpy as np
from datetime import datetime, date


def create_system_timeseries(
    df: pl.DataFrame,
    aggregation: str = "Daily",
    metric: str = "Trips",
    show_trend: bool = False,
) -> go.Figure:
    """
    Create an interactive time-series chart for system-level metrics.

    Args:
        df: DataFrame with columns: date, trips, avg_duration_sec
        aggregation: "Daily", "Weekly", or "Monthly"
        metric: "Trips", "Avg Duration", or "Both"
        show_trend: Whether to overlay a linear trend line

    Returns:
        Plotly Figure object
    """
    # Resample data based on aggregation level
    df_agg = _resample_data(df, aggregation)

    # Create figure (dual-axis if "Both" selected)
    if metric == "Both":
        fig = make_subplots(specs=[[{"secondary_y": True}]])
    else:
        fig = go.Figure()

    # Add traces based on metric selection
    if metric in ["Trips", "Both"]:
        fig.add_trace(
            go.Scatter(
                x=df_agg["date"],
                y=df_agg["trips"],
                name="Trips",
                mode="lines",
                line=dict(color="#1f77b4", width=2),
                hovertemplate="<b>%{x|%Y-%m-%d}</b><br>Trips: %{y:,}<extra></extra>",
            ),
            secondary_y=False if metric == "Both" else None,
        )

        if show_trend:
            trend_line = _calculate_trend(df_agg["date"], df_agg["trips"])
            fig.add_trace(
                go.Scatter(
                    x=df_agg["date"],
                    y=trend_line,
                    name="Trend",
                    mode="lines",
                    line=dict(color="#1f77b4", width=2, dash="dash"),
                    hovertemplate="Trend: %{y:,.0f}<extra></extra>",
                ),
                secondary_y=False if metric == "Both" else None,
            )

    if metric in ["Avg Duration", "Both"]:
        # Convert seconds to minutes for readability
        duration_min = df_agg["avg_duration_sec"] / 60

        fig.add_trace(
            go.Scatter(
                x=df_agg["date"],
                y=duration_min,
                name="Avg Duration",
                mode="lines",
                line=dict(color="#ff7f0e", width=2),
                hovertemplate="<b>%{x|%Y-%m-%d}</b><br>Duration: %{y:.1f} min<extra></extra>",
            ),
            secondary_y=True if metric == "Both" else None,
        )

    # Update layout
    title = f"Capital Bikeshare {aggregation} Trends"
    fig.update_layout(
        title=title,
        hovermode="x unified",
        height=500,
        xaxis=dict(
            title="Date",
            rangeslider=dict(visible=True),
            rangeselector=dict(
                buttons=list(
                    [
                        dict(count=1, label="1m", step="month", stepmode="backward"),
                        dict(count=6, label="6m", step="month", stepmode="backward"),
                        dict(count=1, label="YTD", step="year", stepmode="todate"),
                        dict(count=1, label="1y", step="year", stepmode="backward"),
                        dict(step="all", label="All"),
                    ]
                )
            ),
        ),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1),
    )

    # Update axis labels
    if metric == "Both":
        fig.update_yaxes(title_text="Number of Trips", secondary_y=False)
        fig.update_yaxes(title_text="Avg Duration (minutes)", secondary_y=True)
    elif metric == "Trips":
        fig.update_yaxes(title_text="Number of Trips")
    else:  # Avg Duration
        fig.update_yaxes(title_text="Average Duration (minutes)")

    return fig


def _resample_data(df: pl.DataFrame, aggregation: str) -> pl.DataFrame:
    """
    Resample daily data to weekly or monthly aggregation.

    Args:
        df: DataFrame with daily data (date, trips, avg_duration_sec)
        aggregation: "Daily", "Weekly", or "Monthly"

    Returns:
        Resampled DataFrame
    """
    if aggregation == "Daily":
        return df

    # Convert aggregation to Polars interval string
    interval_map = {
        "Weekly": "1w",
        "Monthly": "1mo",
    }
    interval = interval_map.get(aggregation, "1d")

    # Resample using group_by_dynamic
    resampled = (
        df.sort("date")
        .group_by_dynamic("date", every=interval)
        .agg(
            [
                pl.sum("trips").alias("trips"),
                pl.mean("avg_duration_sec").alias("avg_duration_sec"),
            ]
        )
    )

    return resampled


def _calculate_trend(dates: pl.Series, values: pl.Series) -> np.ndarray:
    """
    Calculate linear trend line using least squares regression.

    Args:
        dates: Series of dates
        values: Series of numeric values

    Returns:
        Array of trend values
    """
    # Convert dates to numeric values (days since first date)
    date_list = dates.to_list()
    first_date = min(date_list)

    x = np.array([(d - first_date).days for d in date_list])
    y = values.to_numpy()

    # Remove any NaNs
    mask = ~np.isnan(y)
    x = x[mask]
    y = y[mask]

    # Fit linear trend
    coeffs = np.polyfit(x, y, 1)
    trend = np.polyval(coeffs, x)

    # Create full-length array with NaNs where original data had NaNs
    full_trend = np.full(len(dates), np.nan)
    full_trend[mask] = trend

    return full_trend


def create_station_timeseries(
    df: pl.DataFrame,
    station_name: str,
    metric: str = "num_checkouts",
) -> go.Figure:
    """
    Create a time-series chart for a specific station.

    Args:
        df: DataFrame with columns: date, num_checkouts, avg_duration_sec
        station_name: Name of the station for the title
        metric: Column name to plot (num_checkouts, avg_duration_sec, net_flow)

    Returns:
        Plotly Figure object
    """
    metric_labels = {
        "num_checkouts": "Number of Checkouts",
        "avg_duration_sec": "Avg Duration (minutes)",
        "net_flow": "Net Flow (checkouts - returns)",
    }

    y_data = df[metric]
    if metric == "avg_duration_sec":
        y_data = y_data / 60  # Convert to minutes

    fig = go.Figure()

    fig.add_trace(
        go.Scatter(
            x=df["date"],
            y=y_data,
            mode="lines",
            line=dict(color="#2ca02c", width=1.5),
            fill="tozeroy",
            fillcolor="rgba(44, 160, 44, 0.1)",
            hovertemplate="<b>%{x|%Y-%m-%d}</b><br>%{y:,.1f}<extra></extra>",
        )
    )

    fig.update_layout(
        title=f"{station_name}: {metric_labels.get(metric, metric)}",
        xaxis_title="Date",
        yaxis_title=metric_labels.get(metric, metric),
        hovermode="x",
        height=300,
        margin=dict(l=20, r=20, t=40, b=20),
    )

    return fig

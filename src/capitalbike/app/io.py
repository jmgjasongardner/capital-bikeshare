from __future__ import annotations

import streamlit as st
import polars as pl
import s3fs


# Shared S3 filesystem (reused across calls to avoid connection overhead)
@st.cache_resource
def _get_s3fs() -> s3fs.S3FileSystem:
    """Get a cached S3 filesystem instance."""
    return s3fs.S3FileSystem(
        key=st.secrets["AWS_ACCESS_KEY_ID"],
        secret=st.secrets["AWS_SECRET_ACCESS_KEY"],
        client_kwargs={"region_name": st.secrets["AWS_DEFAULT_REGION"]},
    )


@st.cache_resource(ttl=3600)
def read_parquet_from_s3_cached(path: str) -> pl.DataFrame:
    """
    Read a parquet file from S3 with resource caching.

    Uses @st.cache_resource instead of @st.cache_data to avoid
    serialization overhead. The DataFrame is stored directly in memory
    without pickling, reducing memory usage by ~50%.

    Note: The returned DataFrame should not be mutated.
    """
    fs = _get_s3fs()
    with fs.open(path, "rb") as f:
        return pl.read_parquet(f)


@st.cache_data(show_spinner="Loading data from S3…")
def read_parquet_from_s3(path: str) -> pl.DataFrame:
    """
    Read a parquet file from S3 with data caching.

    Uses @st.cache_data which serializes the result. This is safer
    for mutable operations but uses more memory.
    """
    fs = _get_s3fs()
    with fs.open(path, "rb") as f:
        return pl.read_parquet(f)


@st.cache_data(ttl=3600, show_spinner=False)
def read_parquet_filtered(path: str, filter_col: str, filter_value) -> pl.DataFrame:
    """
    Read a parquet file from S3 with a filter applied.

    Uses Polars scan + filter + collect pattern for memory-efficient
    loading of subsets. Only the matching rows are loaded into memory.

    Args:
        path: S3 path (s3://bucket/key)
        filter_col: Column name to filter on
        filter_value: Value to match (uses equality filter)

    Returns:
        Filtered DataFrame
    """
    # Use Polars lazy scan with S3 for predicate pushdown
    return (
        pl.scan_parquet(
            path,
            storage_options={
                "aws_access_key_id": st.secrets["AWS_ACCESS_KEY_ID"],
                "aws_secret_access_key": st.secrets["AWS_SECRET_ACCESS_KEY"],
                "aws_region": st.secrets["AWS_DEFAULT_REGION"],
            },
        )
        .filter(pl.col(filter_col) == filter_value)
        .collect()
    )

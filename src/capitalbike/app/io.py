from __future__ import annotations

import streamlit as st
import polars as pl
import s3fs


@st.cache_data(show_spinner="Loading data from S3…")
def read_parquet_from_s3(path: str) -> pl.DataFrame:
    """
    Read a parquet file from S3 with Streamlit caching.
    """
    fs = s3fs.S3FileSystem(
        key=st.secrets["AWS_ACCESS_KEY_ID"],
        secret=st.secrets["AWS_SECRET_ACCESS_KEY"],
        client_kwargs={"region_name": st.secrets["AWS_DEFAULT_REGION"]},
    )

    with fs.open(path, "rb") as f:
        return pl.read_parquet(f)

import streamlit as st
from pathlib import Path

# --------------------------------------------------
# App config
# --------------------------------------------------
st.set_page_config(
    page_title="Capital Bikeshare Analytics",
    page_icon="🚲",
    layout="wide",
    initial_sidebar_state="expanded",
)

# --------------------------------------------------
# Sidebar
# --------------------------------------------------
st.sidebar.title("🚲 Capital Bikeshare")
st.sidebar.caption("Monthly-updated system analytics")

# --------------------------------------------------
# Page registry - Updated for new pages/ structure
# --------------------------------------------------
PAGES = {
    "Home": "Home",
    "Station Explorer": "pages/1_Station_Explorer",
    "Trip Analytics": "pages/2_Trip_Analytics",
    "Station Table": "pages/3_Station_Table",
}

selection = st.sidebar.radio("Go to", list(PAGES.keys()))

# --------------------------------------------------
# Dynamic page loader
# --------------------------------------------------
pages_dir = Path(__file__).parent / "src" / "capitalbike" / "app" / "streamlit"
page_file = pages_dir / f"{PAGES[selection]}.py"

exec(page_file.read_text())

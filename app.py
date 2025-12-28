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
# Page registry
# --------------------------------------------------
PAGES = {
    "Home": "Home",
    "Bike Journeys": "BikeJourneys",
    "Station Explorer": "StationExplorer",
}

selection = st.sidebar.radio("Go to", list(PAGES.keys()))

# --------------------------------------------------
# Dynamic page loader
# --------------------------------------------------
pages_dir = Path(__file__).parent / "src" / "capitalbike" / "app" / "streamlit"
page_file = pages_dir / f"{PAGES[selection]}.py"

exec(page_file.read_text())

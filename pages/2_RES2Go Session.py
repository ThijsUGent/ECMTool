import streamlit as st
import pandas as pd
import concurrent.futures
from io import BytesIO
from zipfile import ZipFile
from urllib.parse import urlparse
import requests
import time

# ------------------ Download Functions ------------------

ZIP_URLS = {
    "PV NUTS2": "https://zenodo.org/api/records/8340501/files/EMHIRES_PV_NUTS2.zip/content",
    # Add other ZIPs if needed:
    # "WIND ONSHORE NUTS2": "https://zenodo.org/api/records/8340501/files/EMHIRES_WIND_ONSHORE_NUTS2.zip/content",
    # "ENSPRESO": "https://cidportal.jrc.ec.europa.eu/ftp/jrc-opendata/ENSPRESO/ENSPRESO_Integrated_Data.zip"
}

def fetch_zip_resource_bg(url: str, name: str, progress_key: str) -> ZipFile:
    """Download a ZIP file in chunks and update session_state progress."""
    response = requests.get(url, stream=True)
    response.raise_for_status()
    buffer = BytesIO()

    total_size = int(response.headers.get("content-length", 0))
    filename = urlparse(url).path.split("/")[-1] or f"{name}.zip"

    downloaded = 0
    start_time = time.time()

    for chunk in response.iter_content(chunk_size=1_048_576):  # 1 MB
        buffer.write(chunk)
        downloaded += len(chunk)

        # Update session_state progress
        if progress_key in st.session_state:
            st.session_state[progress_key][name] = {
                "downloaded": downloaded,
                "total": total_size,
                "speed_mbps": (downloaded * 8 / 1e6) / max(time.time() - start_time, 1e-6)
            }

    buffer.seek(0)
    return ZipFile(buffer)

@st.cache_resource
def get_executor():
    return concurrent.futures.ProcessPoolExecutor(max_workers=3)

# ------------------ Main Streamlit Page ------------------

st.title("🔎 Session State Explorer")

# --- Display Pathways and Clusters ---
if st.session_state:
    col1, col2 = st.columns([1, 1])

    # ======== PATHWAY (col1) ========
    with col1:
        if "Pathway name" in st.session_state:
            st.subheader("📂 Pathway")
            pathways_list = list(st.session_state["Pathway name"].keys())

            # Remove pathway
            if pathways_list:
                selected_pathway = st.selectbox(
                    "Select a pathway to remove:", pathways_list, key="remove_pathway"
                )
                if st.button("🗑️ Remove selected pathway"):
                    del st.session_state["Pathway name"][selected_pathway]
                    st.success(f"Pathway **{selected_pathway}** removed from session.")
                    st.rerun()

            pathways_list = list(st.session_state["Pathway name"].keys())
            if pathways_list:
                tab_list = st.tabs(pathways_list)
                for i, pathway in enumerate(pathways_list):
                    with tab_list[i]:
                        st.markdown(f"### Pathway: **{pathway}**")
                        sector_map = {}
                        for sector_product, df in st.session_state["Pathway name"][pathway].items():
                            parts = sector_product.split("_")
                            if len(parts) < 2: 
                                continue
                            sector, product = parts[0], "_".join(parts[1:])
                            if not df.empty:
                                sector_map.setdefault(sector, []).append((product, df))

                        for sector, products in sector_map.items():
                            with st.expander(f"Sector: {sector}"):
                                for product, df in products:
                                    st.write(f"**Product:** {product}")
                                    st.dataframe(df)
            else:
                st.info("No pathways available to display.")

    # ======== CLUSTER (col2) ========
    with col2:
        if "Cluster name" in st.session_state:
            st.subheader("📍 Cluster")
            cluster_list = list(st.session_state["Cluster name"].keys())

            # Remove cluster
            if cluster_list:
                selected_cluster = st.selectbox(
                    "Select a cluster to remove:", cluster_list, key="remove_cluster"
                )
                if st.button("🗑️ Remove selected cluster"):
                    del st.session_state["Cluster name"][selected_cluster]
                    st.success(f"Cluster **{selected_cluster}** removed from session.")
                    st.rerun()

            cluster_list = list(st.session_state["Cluster name"].keys())
            if cluster_list:
                tab_list = st.tabs(cluster_list)
                for i, cluster in enumerate(cluster_list):
                    with tab_list[i]:
                        st.markdown(f"### Cluster: **{cluster}**")
                        st.write(st.session_state["Cluster name"][cluster])
            else:
                st.info("No clusters available to display.")
else:
    st.info("Session state is currently empty.")

# --- Data Download Section ---
if "archives" not in st.session_state:

    if "download_futures" not in st.session_state:
        st.session_state.download_progress = {name: {"downloaded": 0, "total": 1, "speed_mbps": 0} for name in ZIP_URLS}
        st.session_state.download_futures = {}

    download_clicked = st.button("⬇️ Download data to start using profile load")

    if download_clicked:
        # Submit each ZIP download
        for name, url in ZIP_URLS.items():
            st.session_state.download_futures[name] = get_executor().submit(
                fetch_zip_resource_bg, url, name, "download_progress"
            )

# --- Show progress bars if downloads are in progress ---
if "download_futures" in st.session_state:
    all_done = True
    for name, info in st.session_state.download_progress.items():
        downloaded_mb = info["downloaded"] / 1024**2
        total_mb = info["total"] / 1024**2
        speed = info["speed_mbps"]
        st.progress(min(downloaded_mb / max(total_mb, 1e-6), 1.0), text=f"{name}: {downloaded_mb:.1f}/{total_mb:.1f} MB ({speed:.1f} Mbps)")
        if name in st.session_state.download_futures and not st.session_state.download_futures[name].done():
            all_done = False

    if all_done:
        st.session_state.archives = {name: f.result() for name, f in st.session_state.download_futures.items()}
        st.success("✅ All datasets downloaded and ready!")
else:
    if "archives" in st.session_state:
        st.info("Time series data already loaded")
import streamlit as st
import pandas as pd
from io import BytesIO
from zipfile import ZipFile
import requests
from tool_modules.loading_data import ZIP_FILES

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
            if pathways_list:
                selected_pathway = st.selectbox(
                    "Select a pathway to remove:", pathways_list, key="remove_pathway"
                )
                if st.button("🗑️ Remove selected pathway"):
                    del st.session_state["Pathway name"][selected_pathway]
                    st.success(f"Pathway **{selected_pathway}** removed from session.")
                    st.rerun()

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
            if cluster_list:
                selected_cluster = st.selectbox(
                    "Select a cluster to remove:", cluster_list, key="remove_cluster"
                )
                if st.button("🗑️ Remove selected cluster"):
                    del st.session_state["Cluster name"][selected_cluster]
                    st.success(f"Cluster **{selected_cluster}** removed from session.")
                    st.rerun()

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
st.title("⬇️ Download Time Series Data")

if "archives" not in st.session_state:
    st.session_state.archives = {}

def fetch_file_from_zip_with_progress(url: str, target_file: str, dataset_name: str):
    response = requests.get(url, stream=True)
    response.raise_for_status()
    total_length = int(response.headers.get("content-length", 0))
    buffer = BytesIO()
    downloaded = 0
    progress_bar = st.progress(0)
    status_text = st.empty()

    for chunk in response.iter_content(chunk_size=8192):
        if chunk:
            buffer.write(chunk)
            downloaded += len(chunk)
            percent = downloaded / total_length
            progress_bar.progress(percent)
            status_text.text(f"Downloading {dataset_name}... {percent*100:.1f}%")

    buffer.seek(0)
    with ZipFile(buffer) as zf:
        if target_file not in zf.namelist():
            raise FileNotFoundError(f"{target_file} not found in ZIP")
        with zf.open(target_file) as f:
            if target_file.endswith(".csv"):
                return pd.read_csv(f, sep=";")
            else:
                if dataset_name == "PV":
                    return pd.read_excel(f, usecols=lambda x: x == "time_step" or x in ["BE23"])
                elif dataset_name == "WIND":
                    return pd.read_excel(f, usecols=lambda x: x in ["Time step", "BE23"])
                else:
                    return pd.read_excel(f)


for name, info in ZIP_FILES.items():
    if name not in st.session_state.archives:
        if st.button(f"⬇️ Download {name}"):
            df = fetch_file_from_zip_with_progress(info["url"], info["file"], name)
            st.session_state.archives[name] = df
            st.success(f"{name} downloaded! Shape: {df.shape}")
            st.dataframe(df.head())

# Preview already downloaded datasets
if st.session_state.archives:
    st.write("### 📂 Available Archives")
    for dataset, df in st.session_state.archives.items():
        st.write(f"- **{dataset}**: {df.shape[0]} rows × {df.shape[1]} columns")
        st.dataframe(df.head(5))
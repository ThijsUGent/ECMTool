import streamlit as st
import pandas as pd
import concurrent.futures
from io import BytesIO
from tool_modules.loading_data import ZIP_FILES, fetch_file_from_zip
import concurrent.futures

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


st.title("⬇️ Download Time Series Data")

# Executor for background downloads
if "executor" not in st.session_state:
    st.session_state.executor = concurrent.futures.ThreadPoolExecutor(max_workers=2)

if "download_futures" not in st.session_state:
    st.session_state.download_futures = {}

if "archives" not in st.session_state:
    st.session_state.archives = {}

# Start downloads
for name, info in ZIP_FILES.items():
    if name not in st.session_state.archives and name not in st.session_state.download_futures:
        if st.button(f"⬇️ Download {name}"):
            future = st.session_state.executor.submit(
    fetch_file_from_zip, info["url"], info["file"], name  
)
            st.session_state.download_futures[name] = future
            st.rerun()

# Show progress / completed
for name, future in list(st.session_state.download_futures.items()):
    if future.done():
        try:
            st.session_state.archives[name] = future.result()
            st.success(f"✅ {name} downloaded and ready!")
        except Exception as e:
            st.error(f"❌ Failed to download {name}: {e}")
        del st.session_state.download_futures[name]
    else:
        st.info(f"⏳ {name} is still downloading...")

# Preview
# Preview summary with first 5 rows
if st.session_state.archives:
    st.write("### 📂 Available Archives")
    for dataset, df in st.session_state.archives.items():
        st.write(f"- **{dataset}**: {df.shape[0]} rows × {df.shape[1]} columns")
        st.dataframe(df.head(5))
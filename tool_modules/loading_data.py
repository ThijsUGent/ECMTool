import streamlit as st
import requests
from zipfile import ZipFile
from io import BytesIO

# Define the datasets
ZIP_FILES = {
    "ENSPRESO": {
        "url": "https://cidportal.jrc.ec.europa.eu/ftp/jrc-opendata/ENSPRESO/ENSPRESO_Integrated_Data.zip",
        "file": "ENSPRESO_Integrated_NUTS2_Data.csv",
    },
    # "WIND": {
    #     "url": "https://zenodo.org/api/records/8340501/files/EMHIRES_WIND_ONSHORE_NUTS2.zip/content",
    #     "file": "EMHIRES_WIND_NUTS2_June2019.xlsx",
    # },
    # "PV": {
    #     "url": "https://zenodo.org/api/records/8340501/files/EMHIRES_PV_NUTS2.zip/content",
    #     "file": "EMHIRES_PVGIS_TSh_CF_n2_19862015_reformatt.xlsx",
    # },
}

# Initialize session state
if "archives" not in st.session_state:
    st.session_state.archives = {}

st.title("⬇️ Download Time Series Data")

def fetch_file_from_zip_to_bytes(url: str, target_file: str) -> BytesIO:
    """
    Download a ZIP file and return a BytesIO containing the target file.
    """
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
            status_text.text(f"Downloading {target_file}... {percent*100:.1f}%")

    buffer.seek(0)

    # Extract the target file into BytesIO
    with ZipFile(buffer) as zf:
        if target_file not in zf.namelist():
            raise FileNotFoundError(f"{target_file} not found in ZIP")
        with zf.open(target_file) as f:
            file_bytes = BytesIO(f.read())

    return file_bytes
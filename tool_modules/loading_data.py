import streamlit as st
import requests
from zipfile import ZipFile
from io import BytesIO
import time
from urllib.parse import urlparse

# Available ZIP files (from Zenodo + ENSPRESO)
zip_files = {
    "PV NUTS2": "https://zenodo.org/api/records/8340501/files/EMHIRES_PV_NUTS2.zip/content",
    # "WIND ONSHORE NUTS2": "https://zenodo.org/api/records/8340501/files/EMHIRES_WIND_ONSHORE_NUTS2.zip/content",
    # "ENSPRESO": "https://cidportal.jrc.ec.europa.eu/ftp/jrc-opendata/ENSPRESO/ENSPRESO_Integrated_Data.zip"
}


@st.cache_resource(show_spinner=False)
def fetch_zip_resource_bg(url: str, name: str, progress_key: str) -> ZipFile:
    """
    Download a ZIP file in chunks, updating session_state progress.
    """
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

def load_archives() -> dict:
    """
    Load all configured ZIP archives and return a dictionary:
    {
        "PV NUTS2": { "file1.csv": <BytesIO>, ... },
        "WIND ONSHORE NUTS2": { ... },
        "ENSPRESO": { ... }
    }
    """
    archives_dict = {}

    for name, url in zip_files.items():
        zip_file = fetch_zip_resource_bg(url)
        files_dict = {}
        for fname in zip_file.namelist():
            if fname.endswith((".csv", ".xlsx")):
                # Read each file into BytesIO (not DataFrame)
                with zip_file.open(fname) as f:
                    files_dict[fname] = BytesIO(f.read())
        archives_dict[name] = files_dict

    return archives_dict


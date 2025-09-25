import streamlit as st
import requests
from zipfile import ZipFile
from io import BytesIO
import pandas as pd
import time
from urllib.parse import urlparse

ZIP_FILES = {
    "ENSPRESO": {
        "url": "https://cidportal.jrc.ec.europa.eu/ftp/jrc-opendata/ENSPRESO/ENSPRESO_Integrated_Data.zip",
        "file": "ENSPRESO_Integrated_NUTS2_Data.csv",
    },
    "WIND": {
        "url": "https://zenodo.org/api/records/8340501/files/EMHIRES_WIND_ONSHORE_NUTS2.zip/content",
        "file": "EMHIRES_WIND_NUTS2_June2019.csv",
    },
    "PV": {
        "url": "https://zenodo.org/api/records/8340501/files/EMHIRES_PV_NUTS2.zip/content",
        "file": "EMHIRES_PVGIS_TSh_CF_n2_19862015_reformatt.xlsx",
    },
}


def fetch_file_from_zip(url: str, target_file: str, dataset_name: str) -> pd.DataFrame:
    """
    Download a ZIP and return the target file as a DataFrame.
    
    dataset_name: "PV", "WIND", or "ENSPRESO" to select which columns to read.
    """
    response = requests.get(url, stream=True)
    response.raise_for_status()
    buffer = BytesIO(response.content)

    with ZipFile(buffer) as zf:
        if target_file not in zf.namelist():
            raise FileNotFoundError(f"{target_file} not found in ZIP")
        with zf.open(target_file) as f:
            if target_file.endswith(".csv"):
                return pd.read_csv(f, sep=";")
            else:  # Excel
                if dataset_name == "PV":
                    # Keep only 'time_step' and a specific column like 'BE23'
                    return pd.read_excel(f, usecols=lambda x: x == "time_step" or x in ["BE23"])
                elif dataset_name == "WIND":
                    # Keep only 'Time step' and a specific column like 'BE23'
                    return pd.read_excel(f, usecols=lambda x: x in ["Time step", "BE23"])
                else:
                    return pd.read_excel(f)
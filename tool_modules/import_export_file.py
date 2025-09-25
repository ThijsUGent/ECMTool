from io import StringIO
import pandas as pd
import streamlit as st


def export_to_txt(df):
    """
    Converts a DataFrame into CSV format and returns it as a plain text string.

    Parameters:
        df (pd.DataFrame): DataFrame to be exported.

    Returns:
        str: CSV-formatted string.
    """
    return df.to_csv(index=False)


def import_to_dataframe(uploaded_file_or_path):
    """
    Reads a .csv or .txt file into a DataFrame.
    Handles both Streamlit file uploader and local file paths.

    Parameters:
        uploaded_file_or_path (Union[str, UploadedFile]): Path or uploaded file.

    Returns:
        pd.DataFrame: DataFrame containing the file contents. Returns an empty DataFrame if invalid.
    """
    try:
        if hasattr(uploaded_file_or_path, "read"):  # Streamlit file uploader
            content = uploaded_file_or_path.read().decode("utf-8")
            df = pd.read_csv(StringIO(content), sep=",")
        elif isinstance(uploaded_file_or_path, str):  # Local file path
            df = pd.read_csv(uploaded_file_or_path, sep=",")
        else:
            st.error("Unsupported input type. Please upload a .csv or .txt file.")
            return pd.DataFrame()

        # Validate required columns
        required_columns = {"route_weight", "route_name", "sector_name", "product_name"}
        missing_columns = required_columns - set(df.columns)
        if missing_columns:
            st.error("The uploaded file does not contain the required format.")
            st.error(f"Missing columns: {', '.join(missing_columns)}")
            return pd.DataFrame()

        return df

    except Exception as e:
        st.error(f"Error reading file: {e}")
        return pd.DataFrame()

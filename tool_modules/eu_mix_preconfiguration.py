import pandas as pd
import streamlit as st
from tool_modules.categorisation import *


def eu_mix_configuration_id_weight(pathway_name):
    """
    Returns a DataFrame mapping configuration_id to weighted share (%) 
    for a given EU-mix year (e.g., 'EU-mix-2030'), based on model_configuration.csv.

    Parameters:
        pathway_name (str): Name of the EU-mix pathway (e.g., 'EU-mix-2040').

    Returns:
        pd.DataFrame: DataFrame containing configuration_id and their corresponding weights for the specified year.
    """
    columns = ["configuration_id", "sector_id", "product_id",
               "route_name", "route_weight"]

    model_config_path = "data/model_configuration.csv"

    # Load the configuration data
    model_configuration = pd.read_csv(model_config_path)
    # List of known EU-mix route names (to be excluded)
    eumix = ["EU-mix-2018", "EU-mix-2030", "EU-mix-2040", "EU-mix-2050"]

    # Filter out rows corresponding to EU-mix routes
    model_configuration = model_configuration[
        ~model_configuration["route_name"].isin(eumix)
    ]

    # Extract the year from the pathway name
    year = pathway_name.split("-")[-1]
    mix_column = f"mix_{year}"
    # Filter and compute weights
    df_upload = model_configuration[model_configuration[mix_column] != 0].copy(
    )
    df_upload["route_weight"] = df_upload[mix_column] * 100

    # Load and clean the per-ton configuration data
    perton_path = "data/perton_all.csv"
    perton_ALL_AIDRES = pd.read_csv(perton_path)


    perton_ALL_AIDRES = perton_ALL_AIDRES.groupby(
        "configuration_id").first().reset_index()

    perton_ALL_AIDRES = process_configuration_dataframe(perton_ALL_AIDRES)

    # perton_ALL_mix_AIDRES = perton_ALL_AIDRES[perton_ALL_AIDRES["configuration_name"].str.contains(
    #     "mix")]

    df_upload = perton_ALL_AIDRES.merge(
    df_upload[["configuration_id", "route_weight"]],
    on="configuration_id",
    how="inner",
    
)
    df_upload["route_name"] = df_upload["configuration_name"]

    return df_upload

from sklearn.cluster import KMeans
import os
import io
import math
import base64
from io import BytesIO

import numpy as np
import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import streamlit as st
import pydeck as pdk
import plotly.express as px
import plotly.graph_objects as go
from PIL import Image, ImageDraw
from shapely import wkt, wkb
from sklearn.cluster import DBSCAN, KMeans
from sklearn.preprocessing import StandardScaler
from shapely.geometry import Point

from geopy.distance import geodesic

from tool_modules.clustering import *
from tool_modules.convert import *
from tool_modules.graph_output import *

type_ener_feed = ["electricity_[mwh/t]",
                  "electricity_[gj/t]",
                  "alternative_fuel_mixture_[gj/t]",
                  "biomass_[gj/t]",
                  "biomass_waste_[gj/t]",
                  "coal_[gj/t]",
                  "coke_[gj/t]",
                  "crude_oil_[gj/t]",
                  "hydrogen_[gj/t]",
                  "methanol_[gj/t]",
                  "ammonia_[gj/t]",
                  "naphtha_[gj/t]",
                  "natural_gas_[gj/t]",
                  "plastic_mix_[gj/t]",
                  "alternative_fuel_mixture_[t/t]",
                  "biomass_[t/t]",
                  "biomass_waste_[t/t]",
                  "coal_[t/t]",
                  "coke_[t/t]",
                  "crude_oil_[t/t]",
                  "hydrogen_[t/t]",
                  "methanol_[t/t]",
                  "ammonia_[t/t]",
                  "naphtha_[t/t]",
                  "natural_gas_[t/t]",
                  "plastic_mix_[t/t]"]

# Original color map
color_map = {
    "electricity_[gj/t]": "#ffeda0",   # yellow pastel
    "alternative_fuel_mixture_[gj/t]": "#fdd49e",  # light orange
    "biomass_[gj/t]": "#c7e9c0",       # light green
    "biomass_waste_[gj/t]": "#a1d99b",  # green pastel
    "coal_[gj/t]": "#cccccc",          # light grey
    "coke_[gj/t]": "#bdbdbd",          # grey
    "crude_oil_[gj/t]": "#fdae6b",     # orange pastel
    "hydrogen_[gj/t]": "#b7d7f4",      # light blue
    "methanol_[gj/t]": "#fdd0a2",      # soft orange
    "ammonia_[gj/t]": "#d9f0a3",       # lime pastel
    "naphtha_[gj/t]": "#fcbba1",       # peach
    "natural_gas_[gj/t]": "#89a0d0",   # light blue
    "plastic_mix_[gj/t]": "#e5e5e5",   # very light grey
}

# Extend with cleaned keys (underscore removed, suffix dropped, and underscore replaced with space)
color_map.update({
    " ".join(key.split("_")[:-1]): value
    for key, value in color_map.items()
})

# Radius scale
def _get_radius(df):
    top_1_val = df["total_energy"].quantile(0.99)
    top_10_val = df["total_energy"].quantile(0.90)
    mean_val = df["total_energy"].mean()

    def classify(val):
        if val >= top_1_val:
            return 20000
        elif val >= top_10_val:
            return 12000
        elif val >= mean_val:
            return 9700
        else:
            return 8000

    radius_series = df["total_energy"].apply(classify)

    thresholds = {
        f"Large (≥ {top_1_val:.0f})": 20000,
        f"Medium (≥ {top_10_val:.0f})": 12000,
        f"Average (≥ {mean_val:.0f})": 9700,
        f"Small (< {mean_val:.0f})": 8000,
    }

    return radius_series, thresholds



@st.cache_resource(show_spinner=False)
def get_NUTS2_polygons():
    """Download and cache the NUTS2 polygons (2013)."""
    url = "https://gisco-services.ec.europa.eu/distribution/v2/nuts/geojson/NUTS_RG_01M_2013_4326_LEVL_2.geojson"
    return gpd.read_file(url)


def matching_NUTS2(list_sites):
    # ✅ Cached instead of fetching each time
    gdf_polygon_NUTS2_2013 = get_NUTS2_polygons()

    # Ensure list_sites is a GeoDataFrame in EPSG:4326
    if not isinstance(list_sites, gpd.GeoDataFrame):
        if {"lon", "lat"}.issubset(list_sites.columns):
            geometry = [Point(xy) for xy in zip(list_sites["lon"], list_sites["lat"])]
            list_sites = gpd.GeoDataFrame(list_sites, geometry=geometry, crs="EPSG:4326")
        else:
            raise ValueError("list_sites must be a GeoDataFrame or a DataFrame with 'lon' and 'lat' columns")

    if list_sites.crs is None or list_sites.crs.to_epsg() != 4326:
        list_sites = list_sites.to_crs(epsg=4326)

    # Spatial join to assign NUTS2 regions
    result = gpd.sjoin(list_sites, gdf_polygon_NUTS2_2013, how="left", predicate="intersects")

    # Return only relevant columns (site + matched NUTS2 code)
    gdf_list_NUTS2_cluster = result[["geometry", "NUTS_ID", "NAME_LATN"]].drop_duplicates()
    return gdf_list_NUTS2_cluster


product_updates = {
    "cement": "Cement product",
    "chemical-PE": "Polyethylene",
    "chemical-PEA": "Poly-ethyl-acetate",
    "chemical-olefins": "Olefins",
    "fertiliser-ammonia": "Ammonia",
    "fertiliser-nitric-acid": "Nitric acid",
    "fertiliser-urea": "Urea",
    "glass-container": "Container glass",
    "glass-fibre": "Glass fibre",
    "glass-float": "Float glass",
    "refineries-light-liquid-fuel": "Light liquid fuel",
    "steel-secondary" : "Secondary steel", 
    "steel-primary" : "Primary steel"
}

# Existing dictionary
dict_product_by_sector_AIDRES = {
    "Cement": ["Cement"],
    "Chemical": ["Polyethylene", "Polyethylene acetate", "Olefins"],
    "Fertilisers": ["Ammonia", "Nitric acid", "Urea"],
    "Glass": ["Container glass", "Glass fibre", "Float glass"],
    "Refineries": ["Light liquid fuel"],
    "Steel": ["Primary steel", "Secondary steel"],
}

def map_per_pathway():
    st.subheader("Maps - European scale")
    
    if "map_view_state" not in st.session_state:
        st.session_state.map_view_state = {
            "latitude": 50.85,  # default lat
            "longitude": 4.35,  # default lon
            "zoom": 5
        }

    if "Pathway name" not in st.session_state or not st.session_state["Pathway name"]:
        st.info("No selections stored yet.")
        return
    
    AIDRES_Data_check = False
    # Loop over ALL pathways
    for pathway, pathway_dict in st.session_state["Pathway name"].items():

        # Loop over ALL AIDRES sectors
        for sector, products in dict_product_by_sector_AIDRES.items():
            sector_dfs = []

            # Collect DataFrames for each product of this sector
            for product in products:
                key = f"{sector}_{product}"
                if key in pathway_dict:
                    sector_dfs.append(pathway_dict[key])

            # If no matching product keys found, skip sector
            if not sector_dfs:
                continue

            # Check if ALL product dfs in this sector are empty
            if all(df.empty for df in sector_dfs):
                continue
            else : 
                AIDRES_Data_check = True
    if not AIDRES_Data_check:
        pathways = list(st.session_state["Pathway name"].keys())
        pathways_str = " / ".join(pathways)

        st.warning(
            f"⚠️ {pathways_str} do not contain AIDRES data. "
            "Please include AIDRES production routes to use map features."
        )
        return
    path = "data/production_site.csv"
    df = pd.read_csv(path)

    df = df[df["wp1_model_product_name"] != "not included in blue-print model"]

    df["wp1_model_product_name"] = df["wp1_model_product_name"].replace(product_updates)



    pathways_names = list(
        st.session_state["Pathway name"].keys())

    # Initialisation of all sectors
    sectors_all_list = ["Chemical", "Cement",
                        "Refineries", "Fertilisers", "Steel", "Glass"] 

    type_ener_feed_gj = [item for item in type_ener_feed if "[gj/t]" in item]
    type_ener_feed_t = [item for item in type_ener_feed if "[t/t]" in item]
    type_ener_feed_mwh = [item for item in type_ener_feed if "[mwh/t]" in item]

    # Labels only (without unit suffix)
    type_ener_name = [" ".join(item.split("_")[:-1])
                      for item in type_ener_feed_gj]
    type_feed_name = [" ".join(item.split("_")[:-1])
                      for item in type_ener_feed_t]

    col1, col2 = st.columns([1, 4])  # 3:1 ratio, left wide, right narrow

    with col1:

        with st.expander("Choose countries"):
            country_dict = {
                "Belgium": "BE",
                "Netherlands": "NL",
                "France": "FR",
                "Germany": "DE",
                "Luxembourg": "LU",
                "Italy": "IT",
                "Spain": "ES",
                "Austria": "AT",
                "Denmark": "DK",
                "Sweden": "SE",
                "Norway": "NO",
                "Finland": "FI",
                "Ireland": "IE",
                "Portugal": "PT",
                "Poland": "PL",
                "Czech Republic": "CZ",
                "Hungary": "HU",
                "Greece": "GR"
            }

            selected_countries = st.multiselect(
                "Select countries", list(country_dict.keys()))
            country_codes = [country_dict[country]
                             for country in selected_countries]
        unit = st.radio(
            "Select unit", ["GJ", "t"], horizontal=True
        )
        if unit == "GJ":
            with st.expander("Energy carriers"):
                select_all_energy = st.toggle(
                    "Select all", key="select_all_energy", value=True)

                if select_all_energy:
                    default_ener = type_ener_name
                else:
                    default_ener = []

                options_energy = type_ener_name
                values_energy = type_ener_feed_gj

                selected_energy_labels = st.pills(
                    "Choose energy carriers",
                    options_energy,
                    default=default_ener,
                    label_visibility="visible",
                    selection_mode="multi",
                    key="energy_pills"
                )

                selected_columns = [
                    values_energy[options_energy.index(label)] for label in selected_energy_labels
                ]

        elif unit == "t":
            st.markdown("""*Electricity excluded*""")
            with st.expander("Feedstock"):
                select_all_feed = st.toggle(
                    "Select all", key="select_all_feed", value=True)

                if select_all_feed:
                    default_feed = type_feed_name
                else:
                    default_feed = []

                options_feed = type_feed_name
                values_feed = type_ener_feed_t

                selected_feed_labels = st.pills(
                    "Choose feedstock",
                    options_feed,
                    default=default_feed,
                    label_visibility="visible",
                    selection_mode="multi",
                    key="feed_pills"
                )

                selected_columns = [
                    values_feed[options_feed.index(label)] for label in selected_feed_labels
                ]
        st.divider()
        st.text('Edit utilisation rate sector')
        st.markdown(""" *Default value 100 %* """)
        with st.expander("Utilisation rate"):
            sector_utilization = _get_utilization_rates(sectors_all_list)
        dict_gdf = {}
        pathways_names_filtered = []
        for pathway in pathways_names:
            gdf_prod_x_perton = _get_gdf_prod_x_perton(
                df, pathway, sector_utilization, selected_columns)
            # Convert to GeoDataFrame
            if gdf_prod_x_perton is not None:
                dict_gdf[pathway] = gdf_prod_x_perton
                pathways_names_filtered.append(pathway)
        st.divider()

        choice = st.radio("Cluster method", [
                          "DBSCAN", "KMEANS"], horizontal=True)
        choice_cluster, param1, param2, param4 = _edit_clustering(
            choice)
        dict_gdf_clustered = {}

        empty_gdf = gpd.GeoDataFrame()  # or pd.DataFrame() if not spatial

        for pathway in pathways_names_filtered:
            # assign empty GeoDataFrame
            dict_gdf_clustered[pathway] = empty_gdf

        for pathway in pathways_names_filtered:
            dict_gdf[pathway] = dict_gdf[pathway].rename(
                columns={
                    "direct_emission_[tco2/t] ton": "Direct CO2 emissions (t)"}
            )
            if country_codes:
                dict_gdf[pathway] = dict_gdf[pathway][
                    dict_gdf[pathway]["nuts3_code"].str[:2].isin(country_codes)
                ]

    with col2:
        col_map_param, col_layers = st.columns([2, 1])
        with col_map_param:
            if pathways_names_filtered == []:
                # or pathways_names.tolist() if it's a Pandas Series
                names = list(pathways_names)
                # Format each name in italics using Markdown
                italic_names = [f"*{name}*" for name in names]

                if len(italic_names) == 1:
                    text = f"{italic_names[0]} not in AIDRES scope"
                elif len(italic_names) == 2:
                    text = f"{italic_names[0]} & {italic_names[1]} not in AIDRES scope"
                else:
                    text = f"{', '.join(italic_names[:-1])} & {italic_names[-1]} not in AIDRES scope"

                st.markdown(text)
                st.warning("Please select a pathway within AIDRES scope")
                st.stop()

            pathway = st.radio("Select a pathway",
                               pathways_names_filtered, horizontal=True)
            gdf_clustered = _run_clustering(
                choice_cluster, dict_gdf[pathway], param1, param2, param4)
            dict_gdf_clustered[pathway] = gdf_clustered

            sectors_included = dict_gdf_clustered[pathway]["aidres_sector_name"].unique(
            ).tolist()
            sector_seleted = st.pills("Sector(s) included within the pathway:",
                                      sectors_included, selection_mode="multi", default=sectors_included)
            st.markdown(
                '<span style="font-size: 0.85em;">*Only sectors & products included in AIDRES database*</span>', unsafe_allow_html=True)

            map_choice_toggle = st.toggle("Site view")

            if map_choice_toggle:
                map_choice = "site"
                toggle_text = "Site "
            else:
                map_choice = "cluster centroid"
                toggle_text = "Cluster centroid "
            with col_layers:

                gdf_layer = None
                layer_options = None
                # Define display labels and internal values
                layer_options = {
                    "RES potential": "enspresso",
                    "RES production": "RES"
                }

                # # Show radio with display labels
                # layer_label = st.pills(
                #     "Add a layer", list(layer_options.keys()))

                # if layer_label:
                #     # Get internal value
                #     layer = layer_options[layer_label]
                #     if layer == "enspresso":
                #         st.write("Under construction")

                #     elif layer == "RES":
                #         gdf_layer = _layer_RES_generation()
                #         st.write(gdf_layer.columns)

            # Selected sectors
            dict_gdf_clustered[pathway].copy()

            dict_gdf_clustered[pathway] = dict_gdf_clustered[pathway][dict_gdf_clustered[pathway]
                                                                      ["aidres_sector_name"].isin(sector_seleted)]

            mapped_sites = dict_gdf_clustered[pathway]

        if map_choice == "cluster centroid":
            df_selected_site = None
            gdf_clustered_centroid = summarise_clusters_by_centroid(
                dict_gdf_clustered[pathway])
            st.markdown(
                """*Click on a cluster centroid to see details **below the map***""")
            st.divider()

            # --- Toggle appears directly under the map ---
            col1, col2 = st.columns(2)
            with col1:
                st.session_state["legend_show"] = st.checkbox(
                    "Show legend", value=st.session_state.get("legend_show_last", True)
                )
            with col2:
                show_polygon = st.checkbox("Show polygon", value=False)
                if show_polygon == True:
                    st.write("Uncheck polygon to click on a pie chart")

            st.markdown(f"Energy clusters from {pathway} using {choice}")
            ## ADD POLYGON LAYER 
            if show_polygon:
                extra_layer_polygon=mapping_cluster_polygons(dict_gdf_clustered[pathway])
            else:
                extra_layer_polygon= None

            df_selected = _mapping_chart_per_ener_feed_cluster(
                gdf_clustered_centroid, color_map, unit, extra_layer=extra_layer_polygon)
            st.divider()

        if map_choice == "site":
            df_selected = None
            color_choice = st.radio(
                "Site color select", ["cluster", "sector"], horizontal=True)
            st.divider()
            if color_choice == "sector":
                if "legend_show_last" not in st.session_state:
                    st.session_state["legend_show_last"] = True

                st.session_state["legend_show"] = st.toggle(
                    "Show legend", value=st.session_state["legend_show_last"])
            # TITLE
            st.markdown(
                f"AIDRES sites colored by {color_choice} using {choice} ")

            df_selected_site = _mapping_chart_per_ener_feed_sites(
                dict_gdf_clustered[pathway], color_choice, gdf_layer)
            st.divider()
        if df_selected_site is not None:

            _chart_site(df_selected_site, unit)

        if df_selected is not None:
            
            chart = st.radio("Select an option ", ["Treemap",
                             "Sankey Diagram"])
            df_filtered_cluster = _site_within_cluster(
                df_selected, pathway, dict_gdf_clustered)
            if chart == "Treemap":
                tree_map(df_selected)
            elif chart == "Sankey Diagram":
                sankey(df_filtered_cluster, unit)
            # CO2
            if isinstance(df_selected, pd.DataFrame) and not df_selected.empty:
                emission = df_selected["Direct CO2 emissions (t)"].iloc[0]
                emission, unit_CO2 = energy_convert(emission, "t", elec=False)
                st.write(
                    f"Direct CO2 emissions per annum : {emission:.2f} {unit_CO2}")
            st.write(st.session_state["map_view_state"])
            with st.expander("Time profiles"):
                # Step 1: Extract NUTS3 codes 
                #2013
                gdf_list_NUTS2_cluster = matching_NUTS2(df_filtered_cluster)
                NUTS3_cluster_list_2013 = gdf_list_NUTS2_cluster["NUTS_ID"].unique().tolist()

                #2021
                NUTS3_cluster_list_2021=df_filtered_cluster["nuts3_code"].unique().tolist()
                
                # Step 2: Derive NUTS2 codes by removing last character from each NUTS3 code
                NUTS2_cluster_list_2021 = list(
                    set([code[:-1] for code in NUTS3_cluster_list_2021]))

                # Ensure session_state key exists
                if "saved_clusters" not in st.session_state:
                    st.session_state.saved_clusters = pd.DataFrame(
                        columns=["name", "NUTS2_2013",'NUTS2_2021', "electricity", "unit"])
                # Step 4: Suggest next available cluster name
                existing_names = st.session_state.saved_clusters["name"].tolist(
                )
                cluster_index = 1
                while True:
                    suggested_name = f"Cluster {cluster_index} ({pathway})"
                    if suggested_name not in existing_names:
                        break
                    cluster_index += 1

                st.write(
                    "Save this cluster and analyse it in the profile load section")
                cluster_name = st.text_input(
                    "Enter a name for the cluster", value=suggested_name)
                if cluster_name in existing_names:
                    st.warning("Cluster already exist")

                # Button to save current selection
                if st.button("💾 Save this cluster"):
                    # Create one-row DataFrame with list in NUTS2 column
                    new_data = pd.DataFrame([{
                        "name": cluster_name,
                        "NUTS2_2013": NUTS3_cluster_list_2013,
                        "NUTS2_2021" : NUTS2_cluster_list_2021,
                        "electricity": df_selected["electricity"].iloc[0] if isinstance(df_selected["electricity"], pd.Series) else df_selected["electricity"],
                        "unit": df_selected["unit"].iloc[0] if isinstance(df_selected["unit"], pd.Series) else df_selected["unit"]
                    }])

                    # Append to session_state
                    st.session_state.saved_clusters = pd.concat(
                        [st.session_state.saved_clusters, new_data], ignore_index=True)

                    st.success("Configuration saved!")

                # Optional: Display saved configurations
                if not st.session_state.saved_clusters.empty:
                    st.subheader("Saved Configurations")
                    st.dataframe(st.session_state.saved_clusters)
            with st.expander("Show or download sites within the cluster"):
                st.text(
                    "It is possible to download the cluster configuration to use it in the cluster tool")
                if df_filtered_cluster is not None and unit == "GJ":
                    ############
                    df_filtered_cluster["unit"] = unit
                    df_filtered_cluster_show = df_filtered_cluster[[
                        "site_name", "aidres_sector_name", "product_name", "prod_cap", "prod_rate", "utilization_rate", "total_energy", "Direct CO2 emissions (t)", "unit", "nuts3_code", "geometry"]]

                    df_filtered_cluster_show.columns = [
                        col.replace(
                            'utilization rate', 'utilisation rate').replace("site_name", "site").replace("aidres_sector_name", "sector").replace("product_name", "product").replace("prod_cap", "production capacity (kt)").replace("prod_rate", "production rate (kt)").replace("total_energy", "total energy")
                        for col in df_filtered_cluster_show.columns
                    ]
                    df_filtered_cluster_download = df_filtered_cluster[["site_name", "sector_name", "product_name", "prod_cap", "prod_rate", "utilization_rate", "total_energy", "Direct CO2 emissions (t)", "unit", "nuts3_code"]]
                    st.write(df_filtered_cluster_show)
                    cluster = st.text_input(
                        "Enter a name for the cluster",)
                    st.download_button(
                        label="Download cluster configuration",
                        data=df_filtered_cluster_download.to_csv(
                            index=False, sep=","),
                        file_name=f"Cluster_{cluster}.csv",
                        mime='text/plain'
                    )
                else : 
                    st.warning("Please select GJ unit")
        with st.expander("File sites energy consumption (GEOJson PyPSA compatible)"):
            if unit == "GJ":

                cols = [
                "site_name",
                "nuts3_code",
                "cluster",
                "geometry",
                "aidres_sector_name",
                "production_route_name",
                "prod_cap",
                "prod_rate",
                "utilization_rate",
                "product_name",
                "electricity_[gj/t] ton",
                "alternative_fuel_mixture_[gj/t] ton",
                "biomass_[gj/t] ton",
                "biomass_waste_[gj/t] ton",
                "coal_[gj/t] ton",
                "coke_[gj/t] ton",
                "crude_oil_[gj/t] ton",
                "hydrogen_[gj/t] ton",
                "methanol_[gj/t] ton",
                "ammonia_[gj/t] ton",
                "naphtha_[gj/t] ton",
                "natural_gas_[gj/t] ton",
                "plastic_mix_[gj/t] ton",
                "sector_name",
                "total_energy",
                "Direct CO2 emissions (t)"
            ]
                st.write(mapped_sites[cols])
                # Filter + select columns
                columns_nienke = [
                    "site_name",
                    "geometry",
                    "electricity_[gj/t] ton",
                    "alternative_fuel_mixture_[gj/t] ton",
                    "biomass_[gj/t] ton",
                    "biomass_waste_[gj/t] ton",
                    "coal_[gj/t] ton",
                    "coke_[gj/t] ton",
                    "crude_oil_[gj/t] ton",
                    "hydrogen_[gj/t] ton",
                    "methanol_[gj/t] ton",
                    "ammonia_[gj/t] ton",
                    "naphtha_[gj/t] ton",
                    "natural_gas_[gj/t] ton",
                    "plastic_mix_[gj/t] ton",
                    "total_energy",
                ]
                # select columns
                mapped_sites = mapped_sites[columns_nienke]

                # Convert geometry from WKT if needed
                if isinstance(mapped_sites["geometry"].iloc[0], str):
                    mapped_sites["geometry"] = mapped_sites["geometry"].apply(
                        wkt.loads)

                # Convert to GeoDataFrame
                gdf = gpd.GeoDataFrame(
                    mapped_sites, geometry="geometry", crs="EPSG:4326")

                # Write GeoJSON to BytesIO
                buffer = io.BytesIO()
                gdf.to_file(buffer, driver="GeoJSON")
                buffer.seek (0)  # reset pointer

                # Streamlit download button
                st.download_button(
                    label="Download GeoJSON for PyPSA",
                    data=buffer,
                    file_name=f"{pathway}_{'_'.join(country_codes)}.geojson",
                    mime="application/geo+json"
                )   
            else :
                st.warning("Select GJ unit")


def _chart_site(df, unit):
    df["unit"] = unit
    desired_cols = [
        "site_name",
        "sector_name", "product_name", "prod_cap",
        "prod_rate",
        "utilization_rate",
        "total_energy",
        "alternative fuel mixture",
        "ammonia",
        "biomass",
        "biomass waste",
        "coal",
        "coke",
        "crude oil",
        "electricity",
        "hydrogen",
        "methanol",
        "naphtha",
        "natural gas",
        "plastic mix",
        "unit"
    ]

    # Only keep columns that exist in df
    existing_cols = [col for col in desired_cols if col in df.columns]
    st.write(df[existing_cols])


def _site_within_cluster(df_selected, pathway, dict_gdf_clustered):
    if df_selected is not None:
        df = dict_gdf_clustered[pathway]
        nbr_cluster = int(df_selected["cluster"])
        df_filtered_cluster = df[df["cluster"] == nbr_cluster]
        return df_filtered_cluster
    else:
        return None


def _get_utilization_rates(sectors):
    sector_utilization_defaut = {
        "Fertilisers": 100,
        "Steel": 100,
        "Cement": 100,
        "Refineries": 100,
        "Chemical": 100,
        "Glass": 100,
    }
    sector_utilization = {}
    for sector in sectors:
        st.text("Ulisation rate (%)")
        value = st.slider(f"{sector}", 0, 100,
                          value=sector_utilization_defaut[sector])
        sector_utilization[sector] = value
    return sector_utilization


def _get_gdf_prod_x_perton(df, pathway, sector_utilization, selected_columns):
    perton = st.session_state["Pathway name"][pathway]

    list_keys = list(perton.keys())
    sectors_list = []
    for key in list_keys:
        sectors_list.append(key.split("_")[0])
    unique_sectors = set(sectors_list)
        # Convert WKB hex to geometry
    df['geometry'] = df['geom'].apply(lambda x: wkb.loads(bytes.fromhex(x)))

    # Convert to GeoDataFrame
    gdf_production_site = gpd.GeoDataFrame(
        df.drop(columns="geom"), geometry='geometry', crs="EPSG:4326")

    for sector, utilization_rate in sector_utilization.items():

        # Matching sector & utlisation rate
        matching = gdf_production_site["aidres_sector_name"] == sector
        gdf_production_site.loc[matching,
                                "utilization_rate"] = utilization_rate

    # Condition : prod_rate if prod_cap extist, but not prod_rate
    prod_rate_cap_utli_condi_1 = gdf_production_site["utilization_rate"] / \
        100 * gdf_production_site["prod_cap"]

    condition_1 = (gdf_production_site["prod_rate"].isna() &
                   gdf_production_site["prod_cap"].notna() &
                   gdf_production_site["utilization_rate"].notna())

    gdf_production_site["prod_rate"] = np.where(
        condition_1, prod_rate_cap_utli_condi_1, gdf_production_site["prod_rate"])

    # Condition : prod_rate if prod_rate exist but not prod_cap
    prod_rate_cap_utli_condi_2 = gdf_production_site["utilization_rate"] / \
        100 * gdf_production_site["prod_rate"]

    condition_2 = (gdf_production_site["prod_rate"].notna() &
                   gdf_production_site["prod_cap"].isna() &
                   gdf_production_site["utilization_rate"].notna())

    gdf_production_site["prod_rate"] = np.where(
        condition_2, prod_rate_cap_utli_condi_2, gdf_production_site["prod_rate"])

    # Condition : prod_rate if prod_rate exist and prod_cap
    prod_rate_cap_utli_condi_3 = gdf_production_site["utilization_rate"] / \
        100 * gdf_production_site["prod_cap"]

    condition_3 = (gdf_production_site["prod_rate"].isna() &
                   gdf_production_site["prod_cap"].isna() &
                   gdf_production_site["utilization_rate"].notna())

    gdf_production_site["prod_rate"] = np.where(
        condition_3, prod_rate_cap_utli_condi_3, gdf_production_site["prod_rate"])

    # Multiply per ton with matching product
    sectors_products = list(perton.keys())
    df_pathway_weighted = pd.DataFrame()
    columns = selected_columns + ["direct_emission_[tco2/t]"]

    dfs_dict_path = st.session_state["Pathway name"][pathway]
    df_path = pd.concat(dfs_dict_path.values(), ignore_index=True)

    for sector_product in sectors_products:
        product = sector_product.split("_")[-1]
        sector = sector_product.split("_")[0]
        dfs_dict_path = st.session_state["Pathway name"][pathway]
        df_path = pd.concat(dfs_dict_path.values(), ignore_index=True)

        df_filtered = df_path[df_path["product_name"] == product]

        if not df_filtered.empty:
            def weighted_avg(df, value_cols, weight_col):
                return pd.Series({
                    col: np.average(df[col], weights=df[weight_col]) for col in value_cols
                })

            df_filtered_weight = df_filtered.groupby("product_name").apply(
                weighted_avg, value_cols=columns, weight_col="route_weight"
            ).reset_index()

            df_filtered_weight["sector_name"] = sector  # retain sector info

            # Correct way to append data to the final DataFrame
            df_pathway_weighted = pd.concat(
                [df_pathway_weighted, df_filtered_weight], ignore_index=True)
    # Optional: only display once, outside the loop
    gdf_prod_x_perton = gdf_production_site.merge(
        df_pathway_weighted,
        how="left",
        left_on="wp1_model_product_name",
        right_on="product_name",
    )
    for column in columns:
        gdf_prod_x_perton[column] = gdf_prod_x_perton[column] * \
            gdf_prod_x_perton["prod_rate"] * 1000  # prod rate kt

    gdf_prod_x_perton['total_energy'] = gdf_prod_x_perton[columns].sum(
        axis=1)

    for column in columns:
        gdf_prod_x_perton.rename(
            columns={column: f"{column} ton"}, inplace=True)
    gdf_prod_x_perton = gdf_prod_x_perton[gdf_prod_x_perton["sector_name"].notnull(
    )]
    return gdf_prod_x_perton


def _mapping_chart_per_ener_feed_cluster(gdf, color_map, unit, extra_layer = None):
    """
    Generates an interactive pydeck map with pie chart icons representing energy feedstock
    distribution cluster location on a GeoDataFrame.

    Parameters:
    - gdf: GeoDataFrame containing spatial points and energy feedstock data columns.
    - color_map: dict mapping energy feed feedstock names to color hex codes.
    - unit: string indicating unit of energy (e.g., 'GJ').
    - gdf_layer: optional GeoDataFrame for an extra GeoJson overlay layer.

    Returns:
    - A DataFrame of the selected cluster’s data on user interaction, else None.
    """

    import base64
    import math
    import pydeck as pdk
    import pandas as pd
    import streamlit as st

    # Extract keys from color_map, representing the energy feed types to display.
    type_ener_feed = list(color_map.keys())

    # Identify energy columns by checking for columns containing "[" (likely units).
    energy_cols = [col for col in gdf.columns if "[" in col]

    # Create a rename mapping to clean column names by removing trailing underscores and units.
    rename_map = {col: " ".join(col.split("_")[:-1]) for col in energy_cols}
    gdf = gdf.rename(columns=rename_map)

    # Filter energy columns to those matching any feed type in color_map keys.
    energy_cols = [col for col in gdf.columns if any(
        col.startswith(feed.split("_")[0]) for feed in type_ener_feed)]

    # Warn user if all rows have zero energy sums for the selected feeds (nothing to display).
    if (gdf[energy_cols].sum(axis=1) == 0).all():
        return st.warning("Select feedstock(s) or energy carrier(s)")

    # Calculate total energy per row by summing the relevant energy columns.
    gdf["total_energy"] = gdf[energy_cols].sum(axis=1)

    # Keep only rows where total energy is positive.
    gdf = gdf[gdf["total_energy"] > 0].copy()

    # Attach unit and create rounded total energy column for display.
    gdf["unit"] = unit
    gdf["total_energy_rounded"] = gdf["total_energy"].round().astype(int)

    # Extract longitude and latitude from the geometry column for mapping.
    gdf["lon"] = gdf.geometry.x
    gdf["lat"] = gdf.geometry.y

    # Calculate radius for circle icons proportional to total energy; also get legend sizes.
    gdf["radius"], radius_legend = _get_radius(gdf)

    # Helper function to convert polar coordinates to cartesian for SVG arc drawing.
    def polar_to_cartesian(cx, cy, r, angle_deg):
        angle_rad = math.radians(angle_deg)
        return cx + r * math.cos(angle_rad), cy + r * math.sin(angle_rad)

    # Generates an SVG pie chart as a base64 encoded image for each row’s energy distribution.
    def generate_pie_svg_base64(row):
        values = row[energy_cols]
        segments = [(col, val)
                    for col, val in zip(energy_cols, values) if val > 0]
        if not segments:
            return ""

        total = sum(val for _, val in segments)
        cx, cy, r = 50, 50, 50  # center x,y and radius for the SVG pie
        paths = []

        # Single segment: draw a full circle with the color.
        if len(segments) == 1:
            col, _ = segments[0]
            colour = color_map.get(col, "#000000")
            paths.append(
                f'<circle cx="{cx}" cy="{cy}" r="{r}" fill="{colour}" />')
        else:
            # Multiple segments: draw pie slices as SVG paths.
            start_angle = 0
            for col, val in segments:
                pct = val / total
                end_angle = start_angle + pct * 360
                x1, y1 = polar_to_cartesian(cx, cy, r, start_angle)
                x2, y2 = polar_to_cartesian(cx, cy, r, end_angle)
                large_arc_flag = 1 if end_angle - start_angle > 180 else 0
                colour = color_map.get(col, "#000000")
                d = f"M {cx},{cy} L {x1},{y1} A {r},{r} 0 {large_arc_flag} 1 {x2},{y2} Z"
                paths.append(f'<path d="{d}" fill="{colour}" />')
                start_angle = end_angle

        svg = f'<svg width="100" height="100" xmlns="http://www.w3.org/2000/svg">{"".join(paths)}</svg>'
        b64 = base64.b64encode(svg.encode("utf-8")).decode("utf-8")
        return f"data:image/svg+xml;base64,{b64}"

    # Apply pie SVG generation to each row, storing the data URI in 'icon_url'.
    gdf["icon_url"] = gdf.apply(generate_pie_svg_base64, axis=1)

    # Converts total energy into formatted string with unit for tooltip display.
    def build_total_html(row):
        total_formatted, unit_formatted = energy_convert(
            row['total_energy_rounded'], row['unit'], False)
        return f"{total_formatted} {unit_formatted}"

    # Generates the HTML snippet for the pie legend in the tooltip per data row.
    def generate_pie_legend(row):
        values = row[energy_cols]
        segments = [(col, val)
                    for col, val in zip(energy_cols, values) if val > 0]
        if not segments:
            return ""
        legend_rows = []
        for col, val in segments:
            colour = color_map.get(col, "#000000")
            clean_label = col.replace("_", " ").replace("[gj/t]", "").strip()
            legend_rows.append(f"""
                <div style="display: flex; align-items: center; margin-bottom: 2px;">
                    <div style="width: 12px; height: 12px; background-color: {colour}; margin-right: 6px; border-radius: 2px;"></div>
                    <span style="font-size: 11px; color: white;">{clean_label}</span>
                </div>
            """)
        return "".join(legend_rows)

    # Add total energy and pie legend HTML columns for tooltip usage.
    gdf["total_html"] = gdf.apply(build_total_html, axis=1)
    gdf["pie_html"] = gdf.apply(generate_pie_legend, axis=1)

    # Prepare icon data with URLs and dimensions for the IconLayer.
    icon_data = gdf.copy()
    icon_data["icon"] = icon_data.apply(lambda row: {
        "url": row["icon_url"],
        "width": 100,
        "height": 100,
        "anchorY": 50
    }, axis=1)

    # Layers list to hold pydeck layers.
    layers = []

    # Optionally add an extra polygon layer if provided.
    if extra_layer is not None :
        layers.append(extra_layer)

    # Add the pie chart icons as an IconLayer.
    icon_layer = pdk.Layer(
        "IconLayer",
        id="pie_chart_icons",
        data=icon_data,
        get_icon="icon",
        get_position=["lon", "lat"],
        get_size="radius",
        size_scale=0.002,
        pickable=True
    )
    layers.append(icon_layer)

    # Tooltip config to show total energy and a small pie legend on hover.
    tooltip = {
        "html": """
            <b>Total energy:</b> {total_html}<br/>{pie_html}
        """,
        "style": {
            "backgroundColor": "rgba(0,0,0,0.7)",
            "color": "white",
            "fontSize": "12px",
            "padding": "10px",
            "borderRadius": "5px"
        }
    }

    # Define view state from session state
    view_state = pdk.ViewState(
    latitude=st.session_state.map_view_state["latitude"],
    longitude=st.session_state.map_view_state["longitude"],
    zoom=st.session_state.map_view_state["zoom"]
)

    # Use session state to toggle legend visibility.

    if st.session_state["legend_show"]:
        st.session_state["legend_show_last"] = True
        map_col, legend_col = st.columns([0.8, 0.2])
    else:
        map_col = st.container()
        legend_col = None

    # Render the pydeck map in the main column.
    with map_col:
        deck = pdk.Deck(
            layers=layers,
            initial_view_state=view_state,
            tooltip=tooltip,
            map_style=None,
        )



        event = st.pydeck_chart(
            deck, selection_mode="single-object", on_select="rerun")

    # Render the legend HTML in the sidebar column if visible.
    if legend_col:
        color_map_legend_full = {
            "alternative fuel mixture": "#fdd49e",
            "biomass": "#c7e9c0",
            "biomass waste": "#a1d99b",
            "coal": "#cccccc",
            "coke": "#bdbdbd",
            "crude oil": "#fdae6b",
            "hydrogen": "#b7d7f4",
            "methanol": "#fdd0a2",
            "ammonia": "#d9f0a3",
            "naphtha": "#fcbba1",
            "natural gas": "#89a0d0",
            "plastic mix": "#e5e5e5"
        }

        legend_full = """
            <style>
            .legend-item {
                display: flex;
                align-items: center;
                margin-top: 8px;
                font-family: Arial, sans-serif;
                font-size: 14px;
                color: #333;
            }
            .color-box {
                width: 12px;
                height: 12px;
                margin-right: 8px;
                border-radius: 2px;
                flex-shrink: 0;
            }
            .note {
                margin-top: 16px;
                font-size: 11px;
                font-style: italic;
                color: #666;
                font-family: Arial, sans-serif;
            }
            </style>
            """
        # Optional entry for electricity, only if unit is GJ
        if unit == "GJ":
            legend_full += """
        <div class="legend-item">
            <div class="color-box" style="background-color: #ffeda0;"></div>
            electricity
        </div>
        """

        # Loop through fuels
        for fuel_legend, color_legend in color_map_legend_full.items():
            legend_full += f"""
        <div class="legend-item">
            <div class="color-box" style="background-color: {color_legend};"></div>
            {fuel_legend}
        </div>
        """

        # Add the note
        legend_full += """
        <div class="note">
        Circle size ∝ total cluster energy per year
        </div>
        """
        with legend_col:
            st.markdown("""**Legend**""")
            st.markdown(legend_full, unsafe_allow_html=True)

    # If the user selects a pie chart, extract that data as a DataFrame.
    df = None
    if event.selection and "objects" in event.selection:
        cluster_objs = event.selection["objects"].get("pie_chart_icons", [])
        if cluster_objs:
            df = pd.DataFrame([cluster_objs[0]])
    return df


def _mapping_chart_per_ener_feed_sites(gdf, color_choice, gdf_layer,):
    import matplotlib.pyplot as plt
    import base64
    import io
    from PIL import Image, ImageDraw
    import pydeck as pdk
    import pandas as pd
    import streamlit as st

    # --- Preprocessing (your original preprocessing) ---
    type_ener_feed = list(color_map.keys())
    energy_cols = [col for col in gdf.columns if "[" in col]
    rename_map = {col: " ".join(col.split("_")[:-1]) for col in energy_cols}
    gdf = gdf.rename(columns=rename_map)

    energy_cols = [col for col in gdf.columns if any(
        col.startswith(feed.split("_")[0]) for feed in type_ener_feed)]

    if (gdf[energy_cols].sum(axis=1) == 0).all():
        return st.warning("Select feedstock(s) or energy carrier(s)")
    elec = energy_cols == ["electricity"]

    gdf['lon'] = gdf.geometry.x
    gdf['lat'] = gdf.geometry.y
    gdf["total_energy"] = gdf[energy_cols].sum(axis=1)
    gdf["radius"], thresholds = _get_radius(gdf)

    # --- Cluster colouring ---
    base_cmap = plt.get_cmap("tab20")
    max_colors = base_cmap.N
    unique_clusters = sorted(gdf["cluster"].unique())

    base_colors_rgb = [
        [int(255 * c) for c in base_cmap(i)[:3]]
        for i in range(max_colors)
    ]

    cluster_color_map = {
        cluster: [0, 0, 0] if cluster == -
        1 else base_colors_rgb[idx % max_colors]
        for idx, cluster in enumerate(unique_clusters)
    }

    # --- Sector colouring ---
    sector_cmap = {
        "Cement":      [102, 102, 102],   # grey
        "Chemical":    [31, 119, 180],    # blue
        "Fertilisers": [44, 160, 44],     # green
        "Glass":       [148, 103, 189],   # purple
        "Refineries":  [255, 127, 14],    # orange
        "Steel":       [127, 127, 127],   # dark grey
    }

    if color_choice == "cluster":
        gdf["color"] = gdf["cluster"].map(cluster_color_map)
    elif color_choice == "sector":
        gdf["color"] = gdf["sector_name"].map(sector_cmap)

    def generate_base64_icon_from_color_pil(rgb, size=128):
        img = Image.new("RGBA", (size, size), (0, 0, 0, 0))
        draw = ImageDraw.Draw(img)
        draw.ellipse([(0, 0), (size - 1, size - 1)], fill=tuple(rgb) + (255,))
        buffered = io.BytesIO()
        img.save(buffered, format="PNG")
        return base64.b64encode(buffered.getvalue()).decode()

    gdf["icon_base64"] = gdf["color"].apply(
        generate_base64_icon_from_color_pil)
    gdf["icon_data"] = gdf["icon_base64"].apply(lambda b64: {
        "url": f"data:image/png;base64,{b64}",
        "width": 128,
        "height": 128,
        "anchorY": 128,
    })

    icon_layer = pdk.Layer(
        "IconLayer",
        id="site_icons",
        data=gdf,
        get_icon="icon_data",
        get_position='[lon, lat]',
        get_size="radius",
        size_scale=0.0007,
        pickable=True
    )

    # Define view state from session state
    view_state = pdk.ViewState(
    latitude=st.session_state.map_view_state["latitude"],
    longitude=st.session_state.map_view_state["longitude"],
    zoom=st.session_state.map_view_state["zoom"]
)
    legend_site_html_vertical = """
<style>
.legend-horizontal {
    font-family: sans-serif;
    font-size: 14px;
    display: flex;
    flex-direction: column;  /* Make it vertical */
    gap: 10px;               /* Smaller gap between items */
    margin-top: 10px;
}
.legend-item {
    display: flex;
    align-items: center;
}
.legend-color {
    width: 15px;
    height: 15px;
    margin-right: 6px;
    flex-shrink: 0;
}
</style>
<div class="legend-horizontal">
    <div class="legend-item"><div class="legend-color" style="background: rgb(102, 102, 102);"></div>Cement</div>
    <div class="legend-item"><div class="legend-color" style="background: rgb(31, 119, 180);"></div>Chemical</div>
    <div class="legend-item"><div class="legend-color" style="background: rgb(44, 160, 44);"></div>Fertilisers</div>
    <div class="legend-item"><div class="legend-color" style="background: rgb(148, 103, 189);"></div>Glass</div>
    <div class="legend-item"><div class="legend-color" style="background: rgb(255, 127, 14);"></div>Refineries</div>
    <div class="legend-item"><div class="legend-color" style="background: rgb(127, 127, 127);"></div>Steel</div>
</div>
"""
    if st.session_state["legend_show"] and color_choice == "sector":
        map_col, legend_col = st.columns([0.8, 0.2])
        with map_col:
            chart = pdk.Deck(
                layers=[icon_layer],
                initial_view_state=view_state,
                tooltip={"text": "Cluster: {cluster}\nSite Name: {site_name}"},
                map_style=None
            )
            event = st.pydeck_chart(
                chart,
                selection_mode="single-object",
                on_select="rerun"
            )
        with legend_col:
            st.markdown("""**Legend**""")
            st.markdown(legend_site_html_vertical, unsafe_allow_html=True)
    else:
        chart = pdk.Deck(
            layers=[icon_layer],
            initial_view_state=view_state,
            tooltip={"text": "Cluster: {cluster}\nSite Name: {site_name}"},
            map_style=None
        )
            
        event = st.pydeck_chart(
            chart,
            selection_mode="single-object",
            on_select="rerun"
        )

    selected = event.selection
    if selected and "objects" in selected:
        selected = selected["objects"].get("site_icons", [])

    df = None
    if selected:
        df = pd.DataFrame([selected[0]])  # Only first selected object
    return df


def _edit_clustering(choice):
    if choice == "DBSCAN":
        st.markdown(
            """<small><i>DBSCAN clustering is based on the density of points. It requires two parameters: the minimum number of sites and the distance between sites (in km).</i></small>""",
            unsafe_allow_html=True
        )
        min_samples = st.slider(
            "Minimum number of sites", 1, 10, step=1, value=5)
        radius = st.slider("Distance between sites (km)",
                           1, 100, step=1, value=10)
        return choice, min_samples, radius, None

    elif choice == "KMEANS":
        st.markdown(
            """<small><i>KMeans clustering requires the number of clusters to be defined. It clusters the sites based on their location only.</i></small>""",
            unsafe_allow_html=True
        )
        n_cluster = st.slider("Number of clusters", 1, 200, step=1, value=100)
        kmeans_option = st.segmented_control(
            "Clustering option", options=["Weighted", "Threshold"]
        )

        if kmeans_option == "Weighted":
            st.markdown(
                """<small><i>Weighted KMeans clustering clusters the sites based on their location, weighted by their total energy demand or direct CO2 emission.</i></small>""",
                unsafe_allow_html=True
            )
            value_type = st.radio(
                'Select threshold type', ('Energy', 'Emissions'), horizontal=True)
            return "KMEANS_WEIGHTED", n_cluster, value_type, None

        elif kmeans_option == "Threshold":

            value_type = st.radio(
                'Select threshold type', ('Energy', 'Emissions'), horizontal=True)
            if value_type == "Energy":
                st.markdown(
                    """<small><i>Only clusters with total energy consumption above the defined threshold are kept. Optionally, smaller clusters can be reassigned to the closest valid one.</i></small>""",
                    unsafe_allow_html=True
                )
            else:
                st.markdown(
                    """<small><i>Only clusters with total CO₂ emissions above the defined threshold are kept. Optionally, smaller clusters can be reassigned to the closest valid one.</i></small>""",
                    unsafe_allow_html=True
                )

            redistribute = st.radio("Redistribute undersized clusters?", [
                                    "No", "Yes"], index=0)
            return "KMEANS_THRESHOLD", n_cluster, value_type, redistribute
        else:
            return "KMEANS", n_cluster, None, None

def _run_clustering(choice, gdf, param1, param2, param4):
    if choice == "DBSCAN":
        min_samples, radius = param1, param2
        gdf = gdf.copy()
        gdf_filtered = gdf.drop_duplicates(subset="aidres_site_id")
        gdf_clustered_single = cluster_gdf_dbscan(
            gdf_filtered, min_samples, radius)
        cluster_map = dict(
            zip(gdf_clustered_single["aidres_site_id"], gdf_clustered_single["cluster"]))
        gdf["cluster"] = gdf["aidres_site_id"].map(cluster_map)
        gdf_clustered = gdf

    elif choice == "KMEANS":
        n_cluster = param1
        gdf_clustered = cluster_gdf_kmeans(gdf, n_cluster)

    elif choice == "KMEANS_WEIGHTED":
        n_cluster, value_type = param1, param2
        gdf_clustered = cluster_gdf_kmeans_weight(gdf, value_type, n_cluster)

    elif choice == "KMEANS_THRESHOLD":
        n_cluster, value_type, redistribute = param1, param2, param4
        gdf_clustered = kmeans_threshold(
            gdf, n_cluster, value_type, redistribute)

    else:
        return gdf  # fallback

    if (gdf_clustered["cluster"] != -1).any():
        return gdf_clustered
    else:
        return gdf

def mapping_cluster_polygons(gdf):
    import pydeck as pdk
    import streamlit as st
    import pandas as pd
    from shapely.geometry import MultiPoint, Polygon
    gdf['lon'] = gdf.geometry.x
    gdf['lat'] = gdf.geometry.y

    # --- Ensure cluster column exists ---
    if "cluster" not in gdf.columns:
        return st.warning("No 'cluster' column found in GeoDataFrame")

    # --- Filter out noise clusters if needed ---
    cluster_ids = [c for c in gdf["cluster"].unique() if c != -1]

    # --- Polygon layer for clusters ---
    polygons = []
    for cluster_id in gdf["cluster"].unique():
        if cluster_id == -1:
            continue
        cluster_points = gdf[gdf["cluster"] == cluster_id]
        if len(cluster_points) >= 3:  # Need at least 3 points for polygon
            hull = cluster_points.unary_union.convex_hull
            if hull.geom_type == "Polygon":
                poly_coords = [[list(coord) for coord in hull.exterior.coords]]
                polygons.append({
                    "polygon": poly_coords[0]
                })

    polygon_layer = pdk.Layer(
        "PolygonLayer",
        data=polygons,
        get_polygon="polygon",
        get_fill_color=[128, 128, 128, 80],  # semi-transparent grey
        pickable=False,
        get_line_color=[0, 0, 0],            # black border
        line_width_min_pixels=0.5,
    )

    view_state = pdk.ViewState(
        latitude=gdf["lat"].mean(),
        longitude=gdf["lon"].mean(),
        zoom=5
    )

    return polygon_layer  # optional, for debugging or further processing
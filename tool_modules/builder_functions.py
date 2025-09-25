# -------------------------------
# Imports
# -------------------------------
from tool_modules.categorisation import *
from tool_modules.eu_mix_preconfiguration import *
from tool_modules.import_export_file import *

import json
from pathlib import Path
import numpy as np
import pandas as pd
import streamlit as st

# -------------------------------
# Default AIDRES sectors
# -------------------------------
sectors_list_AIDRES = ["Cement", "Chemical", "Fertilisers", "Glass", "Refineries", "Steel"]

# -------------------------------
# Helper function: edit dataframe selection and weighting
# -------------------------------
def edit_dataframe_selection_and_weighting(df_product, columns_to_show_selection, sector, product, mode_key, df_upload=None):
    """
    Allows user to select and assign weights to production routes using Streamlit DataEditors.

    Args:
        df_product (pd.DataFrame): Subset of main dataframe for one sector-product.
        columns_to_show_selection (list): Columns to display in the selection editor.
        sector (str): Sector name.
        product (str): Product name.
        mode_key (str): Prefix for Streamlit widget keys to differentiate mode (e.g., 'eumix', 'custom').
        df_upload (pd.DataFrame, optional): Previously uploaded selection with 'route_name' and 'route_weight'.

    Returns:
        edited_selected_df (pd.DataFrame): Edited DataFrame with selected routes, parameters, and weights.
        modified (bool): True if changes were made, False otherwise.
    """
    # Columns for parameter editing
    columns_param = [
        "electricity_[gj/t]", "alternative_fuel_mixture_[gj/t]", "biomass_[gj/t]",
        "biomass_waste_[gj/t]", "coal_[gj/t]", "coke_[gj/t]", "crude_oil_[gj/t]",
        "hydrogen_[gj/t]", "methanol_[gj/t]", "ammonia_[gj/t]", "naphtha_[gj/t]",
        "natural_gas_[gj/t]", "plastic_mix_[gj/t]", "alternative_fuel_mixture_[t/t]",
        "biomass_[t/t]", "biomass_waste_[t/t]", "coal_[t/t]", "coke_[t/t]",
        "crude_oil_[t/t]", "hydrogen_[t/t]", "methanol_[t/t]", "ammonia_[t/t]",
        "naphtha_[t/t]", "natural_gas_[t/t]", "plastic_mix_[t/t]",
        "co2_allowance_[eur/t]", "direct_emission_[tco2/t]", "total_emission_[tco2/t]",
        "direct_emission_reduction_[%]", "total_emission_reduction_[%]", "captured_co2_[tco2/t]"
    ]

    # Apply uploaded weights if provided
    if df_upload is not None:
        df_product["selected"] = df_product["route_name"].isin(df_upload["route_name"])
        df_upload_map = df_upload.set_index("route_name")["route_weight"].to_dict()
        df_product["route_weight"] = df_product["route_name"].map(df_upload_map).fillna(0)
    else:
        df_product["selected"] = False
        df_product["route_weight"] = None

    # Reorder columns to place 'selected' first
    cols = df_product.columns.tolist()
    cols.remove("selected")
    df_product = df_product[["selected"] + cols].reset_index(drop=True)

    # Streamlit DataEditor for selection
    edited_df = st.data_editor(
        df_product,
        num_rows="dynamic",
        column_config={"selected": st.column_config.CheckboxColumn("selected")},
        column_order=columns_to_show_selection,
        hide_index=True,
        key=f"editor_{mode_key}_{sector}_{product}"
    )
    edited_df["sector_name"] = sector
    edited_df["product_name"] = product

    # Extract only selected routes
    selected_df = edited_df[edited_df["selected"] == True].copy()
    modified = not edited_df.equals(df_product)

    # Initialize default weights if selection modified
    if modified:
        if not selected_df.empty:
            selected_df["route_weight"] = round(1 / len(selected_df), 4) * 100
        else:
            selected_df["route_weight"] = 0

    # Tabs for editing weights and parameters
    tab1, tab2 = st.tabs(["Edit the weight (%)", "Edit route parameters"])

    # --- Edit route weights ---
    with tab1:
        edited_selected_df = st.data_editor(
            selected_df,
            num_rows="fixed",
            column_config={"route_weight": st.column_config.NumberColumn("route_weight")},
            disabled=selected_df.columns.difference(["route_weight"]).tolist(),
            hide_index=True,
            column_order=["route_name", "route_weight"],
            use_container_width=True,
            key=f"weight_editor_{mode_key}_{sector}_{product}"
        )

    # --- Edit route parameters ---
    with tab2:
        column_to_show = ["route_name"] + columns_param
        edited_selected_df = st.data_editor(
            edited_selected_df,
            num_rows="fixed",
            disabled=selected_df.columns.difference(columns_param).tolist(),
            hide_index=True,
            column_order=column_to_show,
            use_container_width=True,
            key=f"param_editor_{mode_key}_{sector}_{product}"
        )

        # Flag modified routes
        for route in edited_selected_df["route_name"].unique():
            edited_subset = edited_selected_df[edited_selected_df["route_name"] == route][columns_param]
            original_subset = selected_df[selected_df["route_name"] == route][columns_param]

            if not edited_subset.equals(original_subset):
                edited_selected_df.loc[edited_selected_df["route_name"] == route, "route_name"] = route + " modified"
                modified = True

    # Display changes
    if modified:
        st.markdown("**Changes made:**")
        col1, col2 = st.columns(2)

        # --- Column 1: Weight changes ---
        with col1:
            st.text("⚖️ Weight changes")
            found_weight_change = False
            for route in edited_selected_df["route_name"].unique():
                base_route = route.replace(" modified", "")
                edited_row = edited_selected_df[edited_selected_df["route_name"] == route]
                original_row = selected_df[selected_df["route_name"] == base_route]
                if not original_row.empty:
                    old_weight = original_row["route_weight"].values[0]
                    new_weight = edited_row["route_weight"].values[0]
                    if old_weight != new_weight:
                        st.write(f"- `{base_route}`: `{old_weight}` → `{new_weight}`")
                        found_weight_change = True
            if not found_weight_change:
                st.write("No weight changes.")

        # --- Column 2: Parameter changes ---
        with col2:
            st.text("⚙️ Parameter changes")
            found_param_change = False
            for route in edited_selected_df["route_name"].unique():
                base_route = route.replace(" modified", "")
                edited_row = edited_selected_df[edited_selected_df["route_name"] == route]
                original_row = selected_df[selected_df["route_name"] == base_route]
                if not original_row.empty:
                    for col in edited_row.columns:
                        if col in ["route_weight", "route_name"] or col not in original_row.columns:
                            continue
                        old_value = original_row[col].values[0]
                        new_value = edited_row[col].values[0]
                        if old_value != new_value:
                            st.write(f"- {base_route} *modified* | **{col}**: `{old_value}` → `{new_value}`")
                            found_param_change = True
            if not found_param_change:
                st.write("No parameter changes.")

    # --- Append new rows to global df_perton_ALL_sector ---
    if "df_perton_ALL_sector" in st.session_state:
        new_rows = edited_selected_df[
            ~edited_selected_df["route_name"].isin(st.session_state.df_perton_ALL_sector["route_name"])
        ]
        if not new_rows.empty:
            new_rows = st.session_state.df_new_sector[
            ~st.session_state.df_new_sector['route_name'].isin(st.session_state.df_perton_ALL_sector['route_name'])
        ]

            # Concatenate safely
            st.session_state.df_perton_ALL_sector = pd.concat(
                [st.session_state.df_perton_ALL_sector, new_rows],
                ignore_index=True
            )

    return edited_selected_df, modified

# -------------------------------
# Function: Preconfigure path
# -------------------------------
def preconfigure_path(df, columns_to_show_selection):
    """
    Preconfigure pathways using pre-made EU-MIX scenarios or custom selection.

    Returns:
        dict_routes_selected (dict): Selected routes per sector-product.
        selected_mix (str): Scenario selected.
        pathway_name (str): Name of the configured pathway.
    """
    modified = False
    dict_routes_selected = {}

    # Load scenario descriptions
    pathway_description = pd.read_csv("data/premade_pathway/premade_pathway_description.csv")
    premade_choice = pathway_description["Scenario"].tolist()
    selected_mix = st.radio("Select an EU-MIX scenario", premade_choice, index=0)
    info = pathway_description[pathway_description["Scenario"] == selected_mix].iloc[0]

    # Display description and reference
    st.markdown(f"**Description:** {info['Description']}")
    if pd.notna(info["Reference"]) and info["Reference"].strip():
        if "ReferenceURL" in info and pd.notna(info["ReferenceURL"]) and info["ReferenceURL"].strip():
            st.markdown(f"**Reference:** [{info['Reference']}]({info['ReferenceURL']})")
        else:
            st.markdown(f"**Reference:** {info['Reference']}")

    pathway_name = selected_mix
    # Load corresponding df_upload based on scenario
    if selected_mix in ["EU-MIX-2018", "EU-MIX-2030", "EU-MIX-2040", "EU-MIX-2050"]:
        df_upload = eu_mix_configuration_id_weight(pathway_name)
    else:
        file_mapping = {
            "IEA Net Zero Emissions Scenario": "data/premade_pathway/ECM_Tool_IEA-NET-ZERO-2050.txt",
            "Electrification ECM Scenario": "data/premade_pathway/ECM_Tool_EU-MIX-2050-ELECTRIFICATION.txt"
        }
        df_upload = pd.read_csv(file_mapping[selected_mix])

    # Filter unique sectors
    filtered_df = df[df["route_name"].isin(df_upload["route_name"])]
    unique_sectors = sorted(filtered_df["sector_name"].unique())

    # Edit pathway UI
    if st.checkbox("Edit pathway"):
        dict_routes_selected, modified = _edit_pathway_ui(df, df_upload, unique_sectors, columns_to_show_selection)
    else:
        # Default: pre-fill with df_upload
        for sector in unique_sectors:
            all_products = sorted(df[df["sector_name"] == sector]["product_name"].unique())
            for product in all_products:
                df_product = df[(df["sector_name"] == sector) & (df["product_name"] == product)].copy()
                df_product["selected"] = df_product["route_name"].isin(df_upload["route_name"])
                df_upload_map = df_upload.set_index("route_name")["route_weight"].to_dict()
                df_product["route_weight"] = df_product["route_name"].map(df_upload_map).fillna(0)
                total_weight = df_product["route_weight"].sum()
                if 99.95 <= total_weight <= 100.05 or total_weight == 0:
                    dict_routes_selected[f"{sector}_{product}"] = df_product
                else:
                    st.warning(f"Sum of weights should be ~100%, not {total_weight:.2f}")

    if modified:
        pathway_name += " modified"

    return dict_routes_selected, selected_mix, pathway_name

# -------------------------------
# Function: Create path from scratch
# -------------------------------
def create_path(df, columns_to_show_selection):
    """
    Allow user to create a new custom pathway interactively.
    """
    dict_routes_selected = {}
    pathway_name = "Pathway 1"
    df = st.session_state.df_perton_ALL_sector

    dict_routes_selected, _ = _edit_pathway_ui(df, None, sorted(df["sector_name"].unique()), columns_to_show_selection, mode="custom")

    return dict_routes_selected, pathway_name

# -------------------------------
# Function: Upload path from file
# -------------------------------
def upload_path(df, columns_to_show_selection):
    """
    Allow user to upload a pathway CSV and edit.
    """
    modified = False
    dict_routes_selected = {}
    st.text("Drag & drop .csv pathway file")
    uploaded_file = st.file_uploader("Upload your pathway file here", type=["csv"])
    pathway_name = "Upload file"

    if uploaded_file is not None:
        pathway_name = uploaded_file.name.replace("Pathway_", "").rsplit(".", 1)[0]
        df_upload = import_to_dataframe(uploaded_file)
        # Append new sectors
        _append_new_sectors(df_upload)
        # Get only new rows where 'route_name' is not already in the existing DataFrame
        new_rows = st.session_state.df_new_sector[
            ~st.session_state.df_new_sector['route_name'].isin(st.session_state.df_perton_ALL_sector['route_name'])
        ]

        # Concatenate safely
        st.session_state.df_perton_ALL_sector = pd.concat(
            [st.session_state.df_perton_ALL_sector, new_rows],
            ignore_index=True
        )

        dict_routes_selected, modified = _edit_pathway_ui(st.session_state.df_perton_ALL_sector, df_upload, sorted(df_upload["sector_name"].unique()), columns_to_show_selection)
        if modified:
            pathway_name += " modified"

    return dict_routes_selected, pathway_name

# -------------------------------
# Internal helper: pathway UI editor
# -------------------------------
def sanitize_key(name):
    """Convert a string to a safe Streamlit key (no spaces or special chars)."""
    return str(name).replace(" ", "_").replace("-", "_").replace(".", "_").lower()


def _edit_pathway_ui(df, df_upload, sectors_list, columns_to_show_selection, mode="eumix"):
    """
    Shared UI logic for creating or editing pathways.

    Parameters:
    - df (pd.DataFrame): Main dataframe with sector/product/routes.
    - df_upload (pd.DataFrame or None): Previously uploaded routes with weights.
    - sectors_list (list): Preselected sectors.
    - columns_to_show_selection (list): Columns to show in editor.
    - mode (str): Either 'eumix' or 'custom' for pathway type.

    Returns:
    - dict_routes_selected (dict): Edited routes per sector-product.
    - modified (bool): True if any changes were made.
    """
    modified = False
    dict_routes_selected = {}
    if df_upload is not None :
        sectors_list_pathway = list(df_upload["sector_name"].unique())
    else :
        sectors_list_pathway = None

    # Inject small CSS for popover buttons
    st.markdown("""
    <style>
    div[data-testid="stPopover"] button {font-size: 2px !important; padding: 0px 6px !important; line-height: 1 !important;}
    </style>
    """, unsafe_allow_html=True)

    # Layout columns for sectors + new sector popover
    col1, col2 = st.columns([5, 1])
    with col2:
        # Add new sector
        new_sector_key = "new_sector_input"
        save_sector_key = "save_sector"
        with st.popover("➕"):
            new_sector = st.text_input("New sector name", key=new_sector_key)
            if st.button("✅ Save", key=save_sector_key):
                # Avoid duplicates
                existing_sectors = sectors_list_AIDRES + st.session_state.get("sectors_list_new", [])
                if new_sector.strip() and new_sector not in existing_sectors:
                    st.session_state.new_sector = new_sector
                    st.session_state.sectors_list_new = st.session_state.get("sectors_list_new", [])
                    st.session_state.sectors_list_new.append(new_sector)
                    st.rerun()
                    

    with col1:
        # Combine predefined and newly added sectors
        sector_list = sectors_list_AIDRES + st.session_state.get("sectors_list_new", [])
        selected_sectors = st.pills("Sector(s)", sector_list, selection_mode="multi", default=sectors_list_pathway)

        if len(selected_sectors) < 1:
            st.warning("Select at least one sector")
            return {}, modified

        tabs = st.tabs(selected_sectors)
        for i, sector in enumerate(selected_sectors):
            # Ensure session state for new products exists
            product_list_key = f"new_product_{sanitize_key(sector)}_list"
            if product_list_key not in st.session_state:
                st.session_state[product_list_key] = []

            with tabs[i]:
                all_products = sorted(df[df["sector_name"] == sector]["product_name"].unique())
                
                # Add new product popover
                with st.popover("➕"):
                    new_product_key = f"new_product_input_{sanitize_key(sector)}"
                    save_product_key = f"save_product_{sanitize_key(sector)}"
                    new_product = st.text_input("New product name", key=new_product_key)
                    if st.button("✅ Save", key=save_product_key):
                        if new_product.strip() and new_product not in all_products:
                            st.session_state[f"new_product_{sanitize_key(sector)}"] = new_product
                            st.session_state[product_list_key].append(new_product)
                            st.rerun()

                # Extend product list with newly added products
                all_products.extend(st.session_state[product_list_key])

                # Edit each product
                for product in all_products:
                    with st.expander(f"{product}", expanded=False):
                        df_product = df[(df["sector_name"] == sector) & (df["product_name"] == product)].copy()
                        if df_upload is None or mode == "custom":
                            df_product["selected"] = False

                        # Edit selection + weighting
                        edited_selected_df_product, was_modified = edit_dataframe_selection_and_weighting(
                            df_product, columns_to_show_selection, sector, product, mode, df_upload
                        )

                        total_weight = edited_selected_df_product["route_weight"].sum()
                        if 99.95 <= total_weight <= 100.05 or total_weight == 0:
                            dict_routes_selected[f"{sector}_{product}"] = edited_selected_df_product
                        else:
                            st.warning(f"Sum of weights should be ~100%, not {total_weight:.2f}")

                        if was_modified:
                            modified = True
    return dict_routes_selected, modified
# -------------------------------
# Internal helper: append new sectors from uploaded file
# -------------------------------
def _append_new_sectors(df_upload):
    """
    Add new sectors from uploaded pathway to session_state.
    """
    for sector in df_upload["sector_name"].unique():
        if sector not in sectors_list_AIDRES:
            sector_df = df_upload[df_upload["sector_name"] == sector]
            if not st.session_state.df_new_sector.empty:
                sector_df = sector_df[~sector_df["route_name"].isin(st.session_state.df_new_sector["route_name"])]
            if not sector_df.empty:
                st.session_state.df_new_sector = pd.concat([st.session_state.df_new_sector, sector_df], ignore_index=True)
            if sector not in st.session_state.sectors_list_new:
                st.session_state.sectors_list_new.append(sector)
                
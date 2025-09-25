# Streamlit interface and data tools
import streamlit as st
import pandas as pd
import numpy as np
from pathlib import Path
import json

from tool_modules.import_export_file import *
from tool_modules.eu_mix_preconfiguration import *
from tool_modules.categorisation import *
from tool_modules.builder_functions import *

# Mapping short codes to readable product names
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

# Mapping products to detailed descriptions
def_product = {
    "Cement product": (
        "Cement products include Portland Cement II (BV325R) and "
        "Limestone Calcined Clay Cement (LC3), based on the AIDRES report: "
        "https://op.europa.eu/en/publication-detail/-/publication/577d820d-5115-11ee-9220-01aa75ed71a1"
    ),
    "chemical-PE": (
        "Polyethylene (PE) products, derived from ethylene, "
        "commonly used in packaging, films, and molded items. "
        "Details based on the AIDRES report."
    ),
    "chemical-PEA": (
        "Polyethylene acetate (PEA) products, derived from polyethylene "
        "with acetate modifications, used in coatings, adhesives, "
        "and specialty plastics. Based on the AIDRES report."
    ),
    "chemical-olefins": (
        "Olefins (ethylene, propylene, and related derivatives), "
        "used as building blocks for plastics, chemicals, and fuels. "
        "Details from the AIDRES report."
    ),
    "fertiliser-ammonia": (
        "Ammonia-based fertiliser products, produced from nitrogen "
        "and hydrogen, widely used as feedstock for other fertilisers. "
        "Based on the AIDRES report."
    ),
    "fertiliser-nitric-acid": (
        "Nitric acid-based fertiliser products, typically used in the "
        "production of ammonium nitrate and related compounds. "
        "Details from the AIDRES report."
    ),
    "fertiliser-urea": (
        "Urea-based fertiliser products, the most widely used nitrogen fertiliser, "
        "produced from ammonia and carbon dioxide. Based on the AIDRES report."
    ),
    "glass-container": (
        "Container glass products, primarily bottles and jars used "
        "for packaging food, beverages, and other goods. "
        "Details from the AIDRES report."
    ),
    "glass-fibre": (
        "Glass fibre products, used in insulation, composites, and "
        "reinforcement materials. Based on the AIDRES report."
    ),
    "glass-float": (
        "Float glass products, manufactured by floating molten glass on a "
        "bed of molten metal, used in windows, facades, and mirrors. "
        "Details from the AIDRES report."
    ),
    "refineries-light-liquid-fuel": (
        "Light liquid fuel products, including gasoline and other "
        "refined fractions derived from crude oil. "
        "Based on the AIDRES report."
    ),
}

def select_page():

    # --- init session_state ---
    if "adding_sector" not in st.session_state:
        st.session_state.adding_sector = False
    if "sectors_list_new" not in st.session_state:
        st.session_state.sectors_list_new = []
    if "new_sector" not in st.session_state:
        st.session_state.new_sector = ""
    if "df_new_sector" not in st.session_state:
        st.session_state.df_new_sector = pd.DataFrame()

    # Load and clean the per-ton configuration data
    perton_path = "data/perton_all.csv"
    perton_ALL_AIDRES = pd.read_csv(perton_path)


    perton_ALL_AIDRES = perton_ALL_AIDRES.groupby(
        "configuration_id").first().reset_index()

    perton_ALL_AIDRES = process_configuration_dataframe(perton_ALL_AIDRES)

    perton_ALL_no_mix_AIDRES = perton_ALL_AIDRES[~perton_ALL_AIDRES["configuration_name"].str.contains(
        "mix")]

    perton_ALL_no_mix_AIDRES["route_name"] = perton_ALL_no_mix_AIDRES["configuration_name"].replace(
        "route_name")
    
    # Replace product names using the mapping
    perton_ALL_no_mix_AIDRES["product_name"] = perton_ALL_no_mix_AIDRES["product_name"].replace(product_updates)


    if "df_perton_ALL_sector" not in st.session_state: 
        st.session_state.df_perton_ALL_sector = perton_ALL_no_mix_AIDRES


    

    # Prechoice radio doc link

    if "pathway_configuration_prechoice" not in st.session_state:
        st.session_state["pathway_configuration_prechoice"] = 0

    pathway_configuration_prechoice = st.session_state.get(
        "pathway_configuration_prechoice", 0)

    st.subheader("Pathway Configuration")
    # creates two columns with a 4:1 width ratio
    col1, col2 = st.columns([4, 1])

    with col1:
        st.write(
            "Select a ready-made path, upload another path or create your path")

       
       

        # Columns to be shown in the route selection editor
        columns_to_show_selection = [
            "selected",
            "route_name",
            "horizon",
            "energy_feedstock",
            "technology_category",
            "hydrogen_source",
        ]

        selection = st.radio(
            "Choose an option",
            ["Ready-made path", "Upload a path", "Create a path"],
            horizontal=True, index=pathway_configuration_prechoice
        )
        aidres_mix_checked = selection == "Ready-made path"
        upload_mix_checked = selection == "Upload a path"
        create_mix_checked = selection == "Create a path"

        # --- EU-MIX AUTOMATED PATHWAY SELECTION ---
        if aidres_mix_checked:
            dict_routes_selected, selected_mix, pathway_name = preconfigure_path(
                st.session_state.df_perton_ALL_sector, columns_to_show_selection)

    # --- CUSTOM FROM-SCRATCH PATHWAY BUILDING ---
        if create_mix_checked:
            dict_routes_selected, pathway_name = create_path(
                st.session_state.df_perton_ALL_sector, columns_to_show_selection)
    # --- IMPORT PATHWAY FROM .txt FILE ---
        if upload_mix_checked:
            dict_routes_selected, pathway_name = upload_path(
                st.session_state.df_perton_ALL_sector, columns_to_show_selection)
        if any(not df.empty for df in dict_routes_selected.values()):
            # At least one DataFrame is not empty

            # Keep only the selected rows (where route_weight not 0)
            for key in dict_routes_selected.keys():
                dict_routes_selected[key] = dict_routes_selected[key][dict_routes_selected[key]
                                                                      ["route_weight"] != 0]
        st.markdown(
            "**Save the pathway before proceeding.**")
        with col2:
            st.text("Save the pathway")

            # --- PATHWAY NAMING AND SAVING ---
            pathway_name = st.text_input(
                "Enter a name for your pathway", value=pathway_name)

            if dict_routes_selected:

                if "Pathway name" not in st.session_state:
                    st.session_state["Pathway name"] = {}

                # Save button
                if st.button("Save pathway",type="primary"):
                    
                    if pathway_name.strip() == "":
                        st.warning("Please enter a name for the pathway.")
                    elif pathway_name in st.session_state["Pathway name"]:
                        st.warning(
                            f"A pathway named '{pathway_name}' already exists.")
                    else:
                        st.session_state["Pathway name"][pathway_name] = dict_routes_selected
                        st.session_state.new_sector = ""
                        st.success(f"Pathway '{pathway_name}' saved.")
                        for name in st.session_state["Pathway name"].keys():
                            dict = st.session_state["Pathway name"][name]
                            append_new_sectors(dict)

 
                if pathway_name.strip() == "":
                    st.warning("Please enter a name for the pathway.")
                else:
                    # Combine all selected routes into a single DataFrame
                    combined_df = pd.concat(dict_routes_selected.values(), ignore_index=True)
                    
                    # Export to text using your custom function
                    exported_txt = export_to_txt(combined_df)

                    # Direct download button
                    st.download_button(
                        label="Download Pathway",
                        data=exported_txt,
                        file_name=f"Pathway_{pathway_name}.csv",
                        mime="text/plain",
                        type="secondary"
                    )

            else:
                st.text("Please upload or create a pathway before saving.")

                
def append_new_sectors(uploaded_dict):
    """
    Add new sectors from uploaded pathway to session_state.
    Add new products to session_state.
    """

    # Default sector → product mapping
    default_dict = {
        "Cement": ["Cement product"],
        "Chemical": ["Polyethylene", "Polyethylene acetate", "Olefins"],
        "Fertilisers": ["Ammonia", "Nitric acid", "Urea"],
        "Glass": ["Container glass", "Glass fibre", "Float glass"],
        "Refineries": ["Light liquid fuel"],
        "Steel": ["Primary steel", "Secondary steel"],
    }

    # Initialize in session_state if missing
    if "dict_product_by_sector" not in st.session_state:
        st.session_state["dict_product_by_sector"] = default_dict.copy()
    if "sectors_list_new" not in st.session_state:
        st.session_state["sectors_list_new"] = []
    if "product_list_new" not in st.session_state:
        st.session_state["product_list_new"] = []

    dict_product_by_sector = st.session_state["dict_product_by_sector"]

    # Loop through uploaded items
    for sector_product in uploaded_dict.keys():
        sector = sector_product.split("_")[0].capitalize()   # e.g. "fertiliser-urea" → "Fertiliser"
        product = product_updates.get(sector_product, sector_product)  # map to clean name if exists

        # Add new sector if not already in list
        if sector not in dict_product_by_sector:
            dict_product_by_sector[sector] = []
            if sector not in st.session_state["sectors_list_new"]:
                st.session_state["sectors_list_new"].append(sector)

        # Add new product if not already in mapping
        if product not in dict_product_by_sector[sector]:
            dict_product_by_sector[sector].append(product)
            if product not in st.session_state["product_list_new"]:
                st.session_state["product_list_new"].append(product)

    # Save back
    st.session_state["dict_product_by_sector"] = dict_product_by_sector
        
    
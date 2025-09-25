import streamlit as st
import streamlit as st
import pandas as pd
import numpy as np
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

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

    # --- Sector and energy/feedstock types ---
    
sectors_list_all = ["Chemical", "Cement",
                        "Refineries", "Fertilisers", "Steel", "Glass"] + st.session_state.get("sectors_list_new", [])

def cluster_results():
    sectors_list_all = ["Chemical", "Cement",
                        "Refineries", "Fertilisers", "Steel", "Glass"] + st.session_state.get("sectors_list_new", [])
    st.subheader("Cluster results")

    # --- Check session state ---
    if "Cluster name" not in st.session_state or "Pathway name" not in st.session_state:
        st.warning("Please save at least one cluster and one pathway first.")
        return

    clusters_list = list(st.session_state["Cluster name"].keys())



    type_ener_feed_gj = [item for item in type_ener_feed if "[gj/t]" in item]
    type_ener_feed_t = [item for item in type_ener_feed if "[t/t]" in item]

    type_ener_name = [" ".join(item.split("_")[:-1])
                      for item in type_ener_feed_gj]
    type_feed_name = [" ".join(item.split("_")[:-1])
                      for item in type_ener_feed_t]

    
    if clusters_list:
            # Select one sector to display
            select_cluster = st.selectbox(
                "Select a cluster", clusters_list)
    else:
        st.warning("No clusters available. Please save a cluster first.")
        return
    
    df = st.session_state["Cluster name"][select_cluster].copy()
    unit = "GJ"

    col_left, col_right = st.columns([1, 3])
    with col_left:

        selected_columns = []

        if unit == "GJ":
            with st.expander("Energy carriers"):
                select_all_energy = st.toggle(
                    "Select all", key="select_all_energy", value=True)
                default_ener = type_ener_name if select_all_energy else []

                selected_energy_labels = st.pills(
                    "Choose energy carriers",
                    type_ener_name,
                    default=default_ener,
                    label_visibility="visible",
                    selection_mode="multi",
                    key="energy_pills"
                )

                selected_columns = [
                    type_ener_feed_gj[type_ener_name.index(label)] for label in selected_energy_labels
                ]

        elif unit == "t":
            st.markdown("*Excluded electricity*")
            with st.expander("Feedstock"):
                select_all_feed = st.toggle(
                    "Select all", key="select_all_feed", value=True)
                default_feed = type_feed_name if select_all_feed else []

                selected_feed_labels = st.pills(
                    "Choose feedstock",
                    type_feed_name,
                    default=default_feed,
                    label_visibility="visible",
                    selection_mode="multi",
                    key="feed_pills"
                )

                selected_columns = [
                    type_ener_feed_t[type_feed_name.index(label)] for label in selected_feed_labels
                ]

        st.divider()
        st.markdown("**Edit utilisation rate per sector**  \n*Default: 100%*")
        with st.expander("Utilisation rate"):
            sector_utilization = _get_utilization_rates(sectors_list_all)

        st.divider()
        chart = st.radio("Select chart type", ["Sankey Diagram", "Treemap"])

        st.divider()
        # Select number of pathway to display
        pathway_names = list(st.session_state["Pathway name"].keys())
        max_display = min(len(pathway_names), 3)
        choices = list(range(1, max_display + 1))
        number_to_display = st.radio(
            "Number of paths to display", choices, horizontal=True, key=f'{choices}'
        )

    with col_right:
        
        cols = st.columns(number_to_display)

        column_pathway_pairs = []
        for col, name in zip(cols, pathway_names[:number_to_display]):
            with col:
                selected_pathway = st.selectbox(
                    f"Choose a pathway", pathway_names, key=f"select_{name}", index=pathway_names.index(name)
                )
                column_pathway_pairs.append((col, selected_pathway))
        _display_cluster_pathway(column_pathway_pairs, select_cluster,
                                 selected_columns, unit, sector_utilization, chart)

        # Display the selected pathway


def _display_cluster_pathway(column_pathway_pairs, cluster, selected_columns, unit, sector_utilization, chart):

    for col, selected_pathway in column_pathway_pairs:
        with col:
            df_perton,df_perton_summary = _get_df_prod_x_perton_cluster(
                selected_pathway, sector_utilization, selected_columns, cluster)
            
            df_perton['unit'] = unit
            if chart == "Sankey Diagram":
                sankey(df_perton,unit)
            elif chart == "Treemap":
                tree_map(df_perton_summary)
            with st.expander("Show sites consumption"):
                st.write(df_perton)

            ## TIMES PROFILES
            with st.expander("Time profiles"):
                st.write(df_perton_summary)
                # Step 1: Extract NUTS3 codes
                if "nuts3_code"  in df_perton.columns :
                    NUTS3_cluster_list = df_perton["nuts3_code"].unique(
                    ).tolist()
                else:
                    st.warning("The cluster must include AIDRES sites to identify local RES potential")
                    return

                # Step 2: Derive NUTS2 codes by removing last character from each NUTS3 code
                NUTS2_cluster_list = list(
                    set([code[:-1] for code in NUTS3_cluster_list]))

                # Ensure session_state key exists
                if "saved_clusters" not in st.session_state:
                    st.session_state.saved_clusters = pd.DataFrame(
                        columns=["name", "NUTS2", "electricity", "unit"])
                # Step 4: Suggest next available cluster name
                existing_names = st.session_state.saved_clusters["name"].tolist(
                )
                cluster_index = 1
                while True:
                    suggested_name = f"Cluster {cluster_index}"
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
                        "NUTS2": NUTS2_cluster_list,
                        "electricity": df_perton_summary["electricity"].iloc[0] if isinstance(df_perton_summary["electricity"], pd.Series) else df_perton_summary["electricity"],
                        "unit": df_perton_summary["unit"].iloc[0] if isinstance(df_perton_summary["unit"], pd.Series) else df_perton_summary["unit"]
                    }])

                    # Append to session_state
                    st.session_state.saved_clusters = pd.concat(
                        [st.session_state.saved_clusters, new_data], ignore_index=True)

                    st.success("Configuration saved!")

                # Optional: Display saved configurations
                if not st.session_state.saved_clusters.empty:
                    st.subheader("Saved Configurations")
                    st.dataframe(st.session_state.saved_clusters)



def _get_utilization_rates(sectors):
    sectors_list_all = ["Chemical", "Cement",
                        "Refineries", "Fertilisers", "Steel", "Glass"] + st.session_state.get("sectors_list_new", [])
    # Loop through all sectors and set default value
    sector_utilization_defaut = {}
    for sector in sectors_list_all:
        sector_utilization_defaut[sector] = 100
    sector_utilization = {}

    for sector in sectors:
        st.text("Ulisation rate (%)")
        value = st.slider(f"{sector}", 0, 100,
                          value=sector_utilization_defaut[sector])
        sector_utilization[sector] = value
    return sector_utilization


def _get_df_prod_x_perton_cluster(pathway, sector_utilization, selected_columns, cluster):
    perton = st.session_state["Pathway name"][pathway]
    df = st.session_state["Cluster name"][cluster].copy()

        # Set default utilization_rate column if it doesn't exist
    if "utilization_rate" not in df.columns:
        df["utilization_rate"] = 100

    # Apply sector utilization rates
    for sector, utilization_rate in sector_utilization.items():
        matching = df["sector_name"] == sector
        df.loc[matching, "utilization_rate"] = utilization_rate

    # Compute production rate if prod_cap exists
    prod_rate_calc = df["utilization_rate"] / 100 * df["prod_cap"]
    condition = df["prod_cap"].notna() & df["utilization_rate"].notna()
    df["prod_rate"] = np.where(condition, prod_rate_calc, df["prod_cap"])

    # Prepare weighted pathway data
    df_pathway_weighted = pd.DataFrame()
    df_path = pd.concat(perton.values(), ignore_index=True)

    for sector_product in perton.keys():
        product = sector_product.split("_")[-1]
        sector = sector_product.split("_")[0]

        df_filtered = df_path[df_path["product_name"] == product]
        if df_filtered.empty:
            continue

        def weighted_avg(df_, value_cols, weight_col):
            return pd.Series({
                col: np.average(df_[col], weights=df_[weight_col]) for col in value_cols
            })

        df_filtered_weight = df_filtered.groupby("product_name").apply(
            weighted_avg, value_cols=selected_columns, weight_col="route_weight"
        ).reset_index()

        df_filtered_weight["sector_name"] = sector
        df_pathway_weighted = pd.concat(
            [df_pathway_weighted, df_filtered_weight], ignore_index=True
        )
    # Merge once with suffixes to avoid duplicates
    df_prod_x_perton = df.merge(
        df_pathway_weighted,
        how="left",
        on="product_name",
        suffixes=("", "_weighted")
    )
    # Overwrite sector_name with weighted version if it exists
    if "sector_name_weighted" in df_prod_x_perton.columns:
        df_prod_x_perton["sector_name"] = df_prod_x_perton["sector_name_weighted"]
        df_prod_x_perton.drop(columns=["sector_name_weighted"], inplace=True)
    # Multiply per ton with production rate (kt → t)
    for col in selected_columns:
        if col in df_prod_x_perton.columns:
            df_prod_x_perton[col] = df_prod_x_perton[col] * df_prod_x_perton["prod_rate"] * 1000
            
        # Total energy
    df_prod_x_perton['total_energy'] = df_prod_x_perton[selected_columns].sum(axis=1)

    df_prod_x_perton["unit"]="GJ"
    selected_columns_ton =[]
    for col in selected_columns:
        if col in df_prod_x_perton.columns:
            df_prod_x_perton.rename(
                columns={col: f"{col} ton"}, inplace=True)
        selected_columns_ton.append(f"{col} ton") 
    
    # Keep only rows with valid sector_name
    df_prod_x_perton = df_prod_x_perton[df_prod_x_perton["sector_name"].notnull()]
    df_prod_x_perton["cluster"]=00
    df_perton_summary = df_prod_x_perton.groupby(
                    "cluster")[selected_columns_ton].sum().reset_index()
    type_ener_feed = list(color_map.keys())
    df_perton_summary["unit"]="GJ"
    energy_cols = [col for col in df_perton_summary.columns if "[" in col]
    rename_map = {col: " ".join(col.split("_")[:-1]) for col in energy_cols}
    df_perton_summary = df_perton_summary.rename(columns=rename_map)
    return df_prod_x_perton,df_perton_summary

import streamlit as st
import pandas as pd
import numpy as np



# Existing dictionary
dict_product_by_sector = {
    "Cement": ["Cement product"],
    "Chemical": ["Polyethylene", "Polyethylene acetate", "Olefins"],
    "Fertilisers": ["Ammonia", "Nitric acid", "Urea"],
    "Glass": ["Container glass", "Glass fibre", "Float glass"],
    "Refineries": ["Light liquid fuel"],
    "Steel": ["Primary steel", "Secondary steel"],
}




def cluster_configuration():
    sectors_list_all=_list_ini()


    st.subheader("Cluster configuration")

    if "cluster_configuration_prechoice" not in st.session_state:
        st.session_state["cluster_configuration_prechoice"] = 0

    choice = st.radio(
        "Select an option",
        ["Create a cluster", "Upload a cluster"],
        horizontal=True,
        index=st.session_state["cluster_configuration_prechoice"]
    )

    df_cluster = pd.DataFrame()
    cluster_name = "Cluster 1"

    if choice == "Create a cluster":
        df_cluster = _cluster_product_selection()

    if choice == "Upload a cluster":
        df_cluster, cluster_name = upload_cluster()

    if not df_cluster.empty and df_cluster["site_name"].notnull().any():
        # Input cluster name first
        cluster_name = st.text_input(
            "Enter a name for your Cluster", value=cluster_name
        )

        with st.expander("Sites in the cluster", expanded=True):
            st.dataframe(df_cluster, hide_index=True, use_container_width=True)

        # Save button
        if "Cluster name" not in st.session_state:
            st.session_state["Cluster name"] = {}

        if st.button("Save Cluster", type="primary"):
            if cluster_name.strip() == "":
                st.warning("Please enter a name for the cluster.")
            elif cluster_name in st.session_state["Cluster name"]:
                st.warning(f"A cluster named '{cluster_name}' already exists.")
            else:
                st.session_state["Cluster name"][cluster_name] = df_cluster
                st.success(f"Cluster '{cluster_name}' saved.")
        # Download button
        if cluster_name.strip() == "":
            st.warning("Please enter a name for the cluster.")
        else:
            combined_df = pd.concat([df_cluster], ignore_index=True)
            exported_txt = combined_df.to_csv(index=False, sep=",")
            st.download_button(
                    label="Download Cluster",
                    data=exported_txt,
                    file_name=f"Cluster_{cluster_name}.csv",
                    mime="text/plain",
                    type="secondary"
                )

        

        

            

    else:
        st.text("Please upload or create a cluster before saving.")


def _cluster_product_selection():
    # Dictionary to collect selected sites per sector-product
    dict_cluster_selected = {}

    sectors_list_all=_list_ini()


    selected_sectors = st.pills(
        "Sector(s)", sectors_list_all, selection_mode="multi"
    )

    if not selected_sectors:
        st.warning("Please select at least 1 sector")
        return pd.DataFrame()

    tabs = st.tabs(selected_sectors)

    for i, sector in enumerate(selected_sectors):
        with tabs[i]:
            all_products = list(st.session_state.df_perton_ALL_sector[st.session_state.df_perton_ALL_sector["sector_name"] == sector]["product_name"].unique())
            for product in all_products:

                with st.expander(f"{product}", expanded=False):
                    st.write("Production capacity in kt")
                    df = _get_df_site_parameters(product)
                    df["product_name"] = product
                    df["sector_name"] = sector
                    dict_cluster_selected[f"{sector}_{product}"] = df

    if all(df.empty for df in dict_cluster_selected.values()):
        st.warning("Create at least one site in the cluster.")
        return pd.DataFrame()

    df_cluster = pd.concat(dict_cluster_selected.values())
    return df_cluster


def _get_df_site_parameters(product):
    # --- Define the template DataFrame ---
    df_template = pd.DataFrame({
        'site_name': pd.Series(dtype='str'),
        'prod_cap': pd.Series(dtype='float64'),
    })

    # --- Let the user edit the DataFrame with column config ---
    edited_df = st.data_editor(
        df_template,
        num_rows="dynamic",
        hide_index=True,  # hides the index column
        use_container_width=True,
        key=f"site_parameters_{product}",
        column_order=['site_name', 'prod_cap',"unit"],
        column_config={
            "site_name": st.column_config.TextColumn(
                "site_name",
                help="Enter the site name"
            ),
            "prod_cap": st.column_config.NumberColumn(
                "prod_cap",
                help="Enter production capacity",
                min_value=0.0,
                format="%.2f"
            ),
        }
    )

    return edited_df


def upload_cluster():
    sectors_list_plus_other = _list_ini()


    # Initialization
    df_cluster = pd.DataFrame()
    modified = False
    st.text("Drag & drop .csv cluster file")
    uploaded_file = st.file_uploader(
        "Upload your cluster file here", type=["csv"]
    )
    cluster_name = "Upload file"
    if uploaded_file:
        # Remove the "Cluster_" prefix and ".csv" suffix
        file_name = uploaded_file.name
        if file_name.startswith("Cluster_") and file_name.endswith(".csv"):
            cluster_name = file_name[len("Cluster_"):-len(".csv")]
        # Read the uploaded file directly into a DataFrame
        df = pd.read_csv(uploaded_file, sep=",")
        # Try to infer sector and product columns, or fallback to template
        if set(['sector_name', 'product_name']).issubset(df.columns):
            tabs = st.tabs(sectors_list_plus_other)
            for i, sector in enumerate(sectors_list_plus_other):
                with tabs[i]:
                    dict_product_by_sector=st.session_state["dict_product_by_sector"]
                    all_products = list(st.session_state.df_perton_ALL_sector[st.session_state.df_perton_ALL_sector["sector_name"] == sector]["product_name"].unique())

                    for product in all_products:
                        with st.expander(f"{product}", expanded=False):
                            st.write("Production capicity in kt")
                            df_product = df[(df['sector_name'] == sector) & (
                                df['product_name'] == product)].copy()
                            unit = df_product["unit"]
                            if not df_product.empty:
                                df_product = df_product
                            else:
                                df_product = pd.DataFrame({
                                    "site_name": pd.Series(dtype='str'),
                                    'prod_cap': pd.Series(dtype='float64'),
                                })

                            # --- Let the user edit the DataFrame with column config ---
                            edited_df = st.data_editor(
                                df_product,
                                num_rows="dynamic",
                                hide_index=True,
                                use_container_width=True,
                                key=f"site_parameters_{sector}_{product}",
                                column_order=[
                                    'site_name', 'prod_cap','unit'],
                                column_config={
                                    "site_name": st.column_config.TextColumn(
                                        "site_name",
                                        help="Enter the site name"
                                    ),
                                    "prod_cap": st.column_config.NumberColumn(
                                        "prod_cap",
                                        help=f"Enter production capacity in {unit}",
                                        min_value=0.0,
                                        format="%.2f"
                                    ),
                                }
                            )

                            if not edited_df.empty:
                                edited_df['product_name'] = product
                                edited_df['sector_name'] = sector
                            df_cluster = pd.concat(
                                [df_cluster, edited_df], ignore_index=True)
                            if not df_product.equals(edited_df):
                                modified = True
        else:
            st.warning(
                "Uploaded file not in the good format")
            return pd.DataFrame(), cluster_name

    else:
        st.warning("Please upload a cluster file.")
        return pd.DataFrame(), cluster_name

    if modified:
        cluster_name += " modified"
    return df_cluster, cluster_name

def _list_ini():
    
    if "sectors_list_new" not in st.session_state:
        st.session_state["sectors_list_new"] = []

    new_sector_list = st.session_state["sectors_list_new"]

    sectors_list_AIDRES = list(dict_product_by_sector.keys())

    sectors_list_all_ini = sectors_list_AIDRES + new_sector_list
    return sectors_list_all_ini

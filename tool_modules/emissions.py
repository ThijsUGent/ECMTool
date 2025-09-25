import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px  # Correct import for plotting


def emissions_pathway():
    st.subheader("CO2 emissions")
    columns = [
        "direct_emission_[tco2/t]",
        "total_emission_[tco2/t]",
        "direct_emission_reduction_[%]",
        "total_emission_reduction_[%]",
        "captured_co2_[tco2/t]", "route_weight"
    ]

    # Mapping full KPI names to clean labels for UI display
    kpi_labels_map = {
        "direct_emission_[tco2/t]": "Direct emission",
        "total_emission_[tco2/t]": "Total emission",
        "direct_emission_reduction_[%]": "Direct emission reduction",
        "total_emission_reduction_[%]": "Total emission reduction",
        "captured_co2_[tco2/t]": "Captured CO2"
    }
    label_to_kpi = {v: k for k, v in kpi_labels_map.items()}
    kpi_labels = list(kpi_labels_map.values())

    if "Pathway name" not in st.session_state or not st.session_state["Pathway name"]:
        st.info("No selections stored yet.")
        return

    pathways_names = list(st.session_state["Pathway name"].keys())
    nbr_pathway = len(pathways_names)
    pathway_emission = {}

    product_list = []  # accumulate all products

    for name in pathways_names:
        df_pathway = pd.DataFrame()
        pathway_dict = st.session_state["Pathway name"][name]
        sector_product_list = pathway_dict.keys()

        for sector_product in sector_product_list:
            df = pathway_dict[sector_product]
            df = df[columns]

            # Compute weighted averages
            weights = df["route_weight"]
            weighted_averages = df.drop(columns="route_weight").apply(
                lambda col: np.average(col, weights=weights)
            )
            weighted_averages = weighted_averages.to_frame().T

            product = sector_product.split("_")[-1]
            product_list.append(product)
            weighted_averages["product"] = product

            df_pathway = pd.concat([df_pathway, weighted_averages])

        pathway_emission[name] = df_pathway

    product_list = list(set(product_list))  # unique products

    col1, col2 = st.columns([1, 5])
    with col1:
        nbr_col = st.radio("Number of pathways (max 3)",
                           options=list(range(1, min(4, nbr_pathway + 1))))
        combine = st.checkbox("Combine all products")
        if not combine:
            selected_product = st.multiselect(
                "Select product(s)", product_list)
        else:
            selected_product = None

        # Use clean labels in radio buttons
        kpi_label = st.radio("Select KPI", kpi_labels)
        # Map selected label back to full column name
        kpi = label_to_kpi[kpi_label]

    # Compute global max KPI value for consistent y-axis scaling
    max_kpi_value = 0
    if not combine:
        for name in pathways_names:
            df = pathway_emission[name]
            if selected_product:
                df = df[df["product"].isin(selected_product)]
            if not df.empty:
                current_max = df[kpi].max()
                if current_max > max_kpi_value:
                    max_kpi_value = current_max
    else:
        combined_df = pd.DataFrame()
        for name in pathways_names:
            df = pathway_emission[name]
            df["pathway"] = name
            combined_df = pd.concat([combined_df, df])
        max_kpi_value = combined_df[kpi].max() if not combined_df.empty else 0

    ymax = max_kpi_value * 1.05 if max_kpi_value > 0 else None

    with col2:
        if not combine:
            cols = st.columns(nbr_col)
            selected_pathways = []
            for i, col in enumerate(cols):
                with col:
                    # Exclude already selected pathways from options
                    available_pathways = [
                        p for p in pathways_names if p not in selected_pathways]
                    if not available_pathways:
                        st.write("No more pathways available.")
                        continue
                    name = st.selectbox(
                        f"Select pathway {i+1}", options=available_pathways, key=f"pathway_select_{i}"
                    )
                    selected_pathways.append(name)

                    df = pathway_emission[name]
                    if selected_product:
                        df = df[df["product"].isin(selected_product)]
                    _product_plot(df, kpi, ymax=ymax,
                                  key=f"product_plot_{i}", name=name)
        else:
            _all_plot(combined_df, kpi, pathways_names,
                      ymax=ymax, key="all_plot")


def _product_plot(df, kpi, ymax=None, key=None, name=None):
    if df.empty:
        st.write("No data to plot.")
        return
    title = kpi.split('_[')[0].replace(
        '_', ' ').capitalize() + " CO2"
    plot_data = df.groupby("product")[kpi].mean().reset_index()
    fig = px.bar(plot_data, x="product", y=kpi, title=f"{title} by tonne of product {name}",
                 labels={"product": "Product", kpi: kpi})
    if ymax:
        fig.update_yaxes(range=[0, ymax])
    st.plotly_chart(fig, use_container_width=True, key=key)


def _all_plot(df, kpi, pathways_names, ymax=None, key=None):
    if df.empty:
        st.write("No data to plot.")
        return
    plot_data = df.groupby("pathway")[kpi].sum().reindex(
        pathways_names).reset_index()
    fig = px.bar(plot_data, x="pathway", y=kpi, title=f"Sum of {kpi} across pathways",
                 labels={"pathway": "Pathway", kpi: f"Total {kpi}"})
    if ymax:
        fig.update_yaxes(range=[0, ymax])
    st.plotly_chart(fig, use_container_width=True, key=key)

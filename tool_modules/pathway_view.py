import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import plotly.express as px


def view_page():
    st.subheader("Pathway visualisation")
    if "Pathway name" not in st.session_state or not st.session_state["Pathway name"]:
        st.info("No selections stored yet.")
        return

    # Initialisation of all sectors
    # Existing dictionary
    dict_product_by_sector = {
        "Cement": ["Cement product"],
        "Chemical": ["Polyethylene", "Polyethylene acetate", "Olefins"],
        "Fertilisers": ["Ammonia", "Nitric acid", "Urea"],
        "Glass": ["Container glass", "Glass fibre", "Float glass"],
        "Refineries": ["Light liquid fuel"],
        "Steel": ["Primary steel", "Secondary steel"],
    }

    sectors_list_AIDRES = list(dict_product_by_sector.keys())
    sectors_list_all = sectors_list_AIDRES + st.session_state.get("sectors_list_new", [])


    # Select number of pathway to display
    pathway_names = list(st.session_state["Pathway name"].keys())
    max_display = min(len(pathway_names), 3)
    choices = list(range(1, max_display + 1))
    number_to_display = st.radio(
        "Number of paths to display", choices, horizontal=True, key=f'{choices}'
    )

    # Select one sector to display
    selected_sector = st.segmented_control(
        "Select a sector", sectors_list_all, default=sectors_list_all[0])

    cols = st.columns(number_to_display)

    column_pathway_pairs = []
    for col, name in zip(cols, pathway_names[:number_to_display]):
        with col:
            selected_pathway = st.selectbox(
                f"Choose a pathway", pathway_names, key=f"select_{name}", index=pathway_names.index(name)
            )
            column_pathway_pairs.append((col, selected_pathway))

    _display_pathway(column_pathway_pairs, selected_sector)


def _plot_configurations(df, selected_pathway, product, sector, col):

    if not df.empty:

        st.subheader
        st.subheader(f"{product}")

        # Sort configurations to maintain consistent order
        df_sorted = df.sort_values(
            by=["route_name", "energy_feedstock"])

        # Create a colour map for energy_feedstock types
        energy_feedstock_types = df_sorted["energy_feedstock"].unique()
        colours = px.colors.qualitative.Plotly
        colour_map = {energy_feedstock: colours[i % len(colours)]
                      for i, energy_feedstock in enumerate(energy_feedstock_types)}

        # Add a colour column for Plotly
        df_sorted["colour"] = df_sorted["energy_feedstock"].map(colour_map)

        # Create a bar per energy_feedstock with a single y-value (product) and stack them
        fig = px.bar(
            df_sorted,
            x="route_weight",
            y=[product]*len(df_sorted),
            color="energy_feedstock",
            color_discrete_map=colour_map,
            orientation="h",
            hover_data=["route_weight", "route_name"]
        )

        # add a horizontal line manually
        config_breaks = df_sorted["route_weight"].cumsum().values[:-1]
        for x_pos in config_breaks:
            fig.add_vline(x=x_pos, line_dash="dash", line_color="grey")

        fig.update_layout(
            barmode='stack',
            xaxis_title="Weight",
            yaxis_title="",
            showlegend=True,
            height=250,
            title=f"Configuration Weights for {selected_pathway} - {product}",
            margin=dict(t=30, b=30)
        )

        st.plotly_chart(fig, use_container_width=True,
                        key=f"plot_{selected_pathway} - {sector} - {product} - {col}")


def _display_pathway(column_pathway_pairs, sector):

    for col, selected_pathway in column_pathway_pairs:
        with col:
            dict_routes_selected = st.session_state["Pathway name"][selected_pathway]
            keys = dict_routes_selected.keys()
            keys_filtered = [k for k in keys if k.startswith(sector + "_")]
            for k in keys_filtered:
                product = k.split("_")[1]
                if len(product.split("-")) > 1:
                    product = "-".join(product.split("-")[1:])

                df = dict_routes_selected[k]
                _plot_configurations(df, selected_pathway,
                                     product, sector, col)

import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import matplotlib.pyplot as plt
from matplotlib.sankey import Sankey
import matplotlib.pyplot as plt
from floweaver import *


# Custom modules
from tool_modules.convert import *

# ----------------------------------------
# ENERGY FEED TYPES
# ----------------------------------------
# List of possible energy carriers and units used in the dataset
type_ener_feed = [
    "electricity_[mwh/t]", "electricity_[gj/t]",
    "alternative_fuel_mixture_[gj/t]", "biomass_[gj/t]", "biomass_waste_[gj/t]",
    "coal_[gj/t]", "coke_[gj/t]", "crude_oil_[gj/t]", "hydrogen_[gj/t]",
    "methanol_[gj/t]", "ammonia_[gj/t]", "naphtha_[gj/t]", "natural_gas_[gj/t]",
    "plastic_mix_[gj/t]",
    "alternative_fuel_mixture_[t/t]", "biomass_[t/t]", "biomass_waste_[t/t]",
    "coal_[t/t]", "coke_[t/t]", "crude_oil_[t/t]", "hydrogen_[t/t]",
    "methanol_[t/t]", "ammonia_[t/t]", "naphtha_[t/t]", "natural_gas_[t/t]",
    "plastic_mix_[t/t]"
]

# ----------------------------------------
# COLOR MAP FOR VISUALIZATIONS
# ----------------------------------------
# Each energy carrier is assigned a color for consistency across charts
color_map = {
    "electricity_[gj/t]": "#ffeda0",        # pastel yellow
    "alternative_fuel_mixture_[gj/t]": "#fdd49e",  # light orange
    "biomass_[gj/t]": "#c7e9c0",           # light green
    "biomass_waste_[gj/t]": "#a1d99b",      # pastel green
    "coal_[gj/t]": "#cccccc",               # light grey
    "coke_[gj/t]": "#bdbdbd",               # medium grey
    "crude_oil_[gj/t]": "#fdae6b",          # orange pastel
    "hydrogen_[gj/t]": "#b7d7f4",           # light blue
    "methanol_[gj/t]": "#fdd0a2",           # soft orange
    "ammonia_[gj/t]": "#d9f0a3",            # lime pastel
    "naphtha_[gj/t]": "#fcbba1",            # peach
    "natural_gas_[gj/t]": "#89a0d0",        # muted blue
    "plastic_mix_[gj/t]": "#e5e5e5",        # very light grey
}

# Extend colormap with cleaned versions (drop suffix, replace underscores with spaces)
color_map.update({
    " ".join(key.title().split("_")[:-1]): value
    for key, value in color_map.items()
})
# Extend colormap with cleaned versions (drop suffix, replace underscores with spaces)
color_map.update({
    " ".join(key.split("_")[:-1]): value
    for key, value in color_map.items()
})

# ----------------------------------------
# FUNCTION: TREE MAP
# ----------------------------------------
def tree_map(df: pd.DataFrame):
    """
    Generate a treemap of energy consumption by Energy carrier.

    Parameters
    ----------
    df : pd.DataFrame
        Must contain energy consumption columns with matching names from `type_ener_feed`.
        Expected to contain a single row representing one observation.

    Output
    ------
    Displays a Plotly treemap chart in Streamlit.
    """
    if df is not None and not df.empty:
        # Select only columns relevant to energy Energy carriers
        columns_plot = [
            col for col in df.columns
            if any(col.startswith(feed.split("_")[0]) for feed in type_ener_feed)
        ]

        # Extract single observation (row)
        row = df.iloc[0]

        # Special case: if only electricity column exists
        elec = columns_plot == ["Electricity"]

        # Prepare long-form DataFrame for treemap plotting
        df_long = pd.DataFrame({
            "energy_source": columns_plot,
            "value": [row[col] for col in columns_plot],
            "color_value": [color_map.get(col, "#cccccc") for col in columns_plot],
        })

        # Clean label for readability and extract unit if available
        df_long["label"] = df_long["energy_source"].str.replace(
            r"_\[.*?\]$", "", regex=True).str.replace("_", " ").str.title()
        df_long["unit"] = row.get("unit", "")

        # Compute total energy with proper conversion
        total_energy = df_long["value"].sum()
        unit = df_long["unit"].iloc[0]
        total_energy, unit_real = energy_convert(total_energy, unit, elec)

        # Create treemap
        fig = px.treemap(
            df_long,
            path=["label"],
            values="value",
            color="energy_source",
            hover_data={"value": True, "unit": True},
            color_discrete_map=dict(zip(df_long["energy_source"], df_long["color_value"]))
        )

        fig.update_layout(
            title=dict(
            text=f"Energy Treemap Energy share <br>"
                 f"<sub>Total energy per annum: {total_energy:.2f} {unit_real}</sub>",
            font=dict(size=20, family="Arial Black", color="black"),
            x=0.5, xanchor="center"
        ),
            font=dict(family="Arial, sans-serif", size=16, color="black")
        )

        st.plotly_chart(fig)

# ----------------------------------------
# FUNCTION: SANKEY DIAGRAM
# ----------------------------------------


def sankey(df: pd.DataFrame, unit: str):
    """
    Sankey diagram (Plotly) with toggle for Energy carrier → Sector, Energy carrier → Product, or full chain.
    Fonts are optimized for readability.
    """
    with open("tool_modules/style.css") as f:
        st.markdown(f"<style>{f.read()}</style>", unsafe_allow_html=True)

    if df is None:
        return

    # Toggle selection
    option = st.radio(
        "Select Sankey view:",
        ["Energy carrier → Sector", "Energy carrier → Product", "Energy carrier → Sector → Product"],
        index=2
    )

    # Identify energy Energy carrier columns
    energy_cols = [
        col for col in df.columns
        if any(col.startswith(feed) for feed in type_ener_feed)
    ]

    # Labels
    carrier_labels = [" ".join(col.title().split("_")[:-1]) for col in energy_cols]
    sector_labels = df['sector_name'].unique().tolist()
    product_labels = df['product_name'].unique().tolist()

    labels = carrier_labels + sector_labels + product_labels
    label_indices = {label: i for i, label in enumerate(labels)}

    # Build Sankey links
    sources, targets, values = [], [], []
    for _, row in df.iterrows():
        sector = row['sector_name']
        product = row['product_name']
        sector_idx = label_indices[sector]
        product_idx = label_indices[product]

        for i, col in enumerate(energy_cols):
            value = row[col]
            if pd.notna(value) and value > 0:
                carrier_idx = label_indices[carrier_labels[i]]

                if option == "Energy carrier → Sector":
                    sources.append(carrier_idx)
                    targets.append(sector_idx)
                    values.append(value)

                elif option == "Energy carrier → Product":
                    sources.append(carrier_idx)
                    targets.append(product_idx)
                    values.append(value)

                else:  # Energy carrier → Sector → Product
                    # Energy carrier → Sector
                    sources.append(carrier_idx)
                    targets.append(sector_idx)
                    values.append(value)
                    # Sector → Product
                    sources.append(sector_idx)
                    targets.append(product_idx)
                    values.append(value)

    # Link colors
    link_colors = [color_map.get(labels[s], 'lightgray') for s in sources]

    # Node colors
    node_colors = []
    for label in labels:
        if label in color_map:
            node_colors.append(color_map[label])
        elif label in sector_labels:
            node_colors.append('lightgrey')
        else:
            node_colors.append('darkgrey')

    # Build Sankey figure
    fig = go.Figure(data=[go.Sankey(
        node=dict(
            pad=25,
            thickness=28,
            line=dict(color="black", width=0.3),
            label=labels,
            color=node_colors,
        ),
        link=dict(
            source=sources,
            target=targets,
            value=values,
            color=link_colors
        )
    )])

    # Compute total energy
    total_energy = df["total_energy"].sum()
    total_energy, unit_real = energy_convert(total_energy, unit)

    # Update layout with better fonts
    fig.update_layout(
        hovermode='x',
        title=dict(
            text=f"Energy Flow Sankey Diagram ({option})<br>"
                 f"<sub>Total energy per annum: {total_energy:.2f} {unit_real}</sub>",
            font=dict(size=20, family="Arial Black", color="black"),
            x=0.5, xanchor="center"
        ),
        hoverlabel=dict(font=dict(size=14, family="Arial", color="black")),
    )

    st.plotly_chart(fig, use_container_width=True)
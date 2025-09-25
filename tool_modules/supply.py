import streamlit as st
import pandas as pd
import geopandas as gpd
import pydeck as pdk
import json


def clean_numeric_column(series):
    return (
        series.astype(str)
        .str.replace(" ", "", regex=False)
        .str.replace(",", ".", regex=False)
        .replace(":", None)
        .astype(float)
    )


def eurostat_production():
    # Load Eurostat NUTS3 solar and wind production per m2 (2023)
    NUTS3_solar_MWh_m2_2023 = pd.read_csv(
        "data/Energy_production/Solar_MWh_m2_2023.csv")
    NUTS3_wind_MWh_m2_2023 = pd.read_csv(
        "data/Energy_production/WindOnshore_MWh_m2_2023.csv")

    # Load NUTS3 area data
    NUTS3_area = pd.read_excel(
        "data/NUTS/NUTS3_area.xlsx", sheet_name="Sheet 1", skiprows=9)
    NUTS3_area.rename(columns={
        NUTS3_area.columns[0]: "Region name",
        NUTS3_area.columns[1]: "Area (m2)"
    }, inplace=True)

    # Load NUTS shapefile, filter NUTS3 regions
    gdf = gpd.read_file(
        "data/NUTS/NUTS_RG_20M_2021_4326/NUTS_RG_20M_2021_4326.shp")
    gdf_NUTS3 = gdf[gdf["LEVL_CODE"] == 3].copy()
    gdf_NUTS3.rename(columns={"NUTS_ID": "NUTS3",
                     "NUTS_NAME": "Region name"}, inplace=True)

    # Merge geometry and area with Eurostat data
    NUTS3_area = NUTS3_area[["Region name", "Area (m2)"]]
    eurostat_solar = NUTS3_solar_MWh_m2_2023.rename(
        columns={"NUTS3": "NUTS3", "Value": "Value_solar", "Region name": "Region name"})
    eurostat_wind = NUTS3_wind_MWh_m2_2023.rename(
        columns={"NUTS3": "NUTS3", "Value": "Value_wind", "Region name": "Region name"})

    # Merge solar and wind production on Region name
    production_df = pd.merge(
        eurostat_solar, eurostat_wind, on="Region name", suffixes=("_solar", "_wind"))

    # Merge with area data
    production_df = production_df.merge(
        NUTS3_area, on="Region name", how="left")

    # Clean numeric columns
    production_df["Value_wind"] = clean_numeric_column(
        production_df["Value_wind"])
    production_df["Value_solar"] = clean_numeric_column(
        production_df["Value_solar"])
    production_df["Area (m2)"] = clean_numeric_column(
        production_df["Area (m2)"])

    # Calculate production (TWh) = production per m2 * area * 1e-6
    production_df["Wind Onshore (TWh)"] = production_df["Value_wind"] * \
        production_df["Area (m2)"] * 1e-6
    production_df["Solar (TWh)"] = production_df["Value_solar"] * \
        production_df["Area (m2)"] * 1e-6

    # Add NUTS3 column from geometry shapefile (match by Region name)
    production_df = production_df.merge(
        gdf_NUTS3[["NUTS3", "Region name"]], on="Region name", how="left")

    # Aggregate from NUTS3 to NUTS2 by truncating first 4 characters of NUTS3 code
    production_df["NUTS2"] = production_df["NUTS3"].str[:4]
    production_NUTS2 = production_df.groupby("NUTS2", as_index=False)[
        ["Wind Onshore (TWh)", "Solar (TWh)"]].sum()

    return production_NUTS2


def enspreso(scenario):
    # Load ENSPRESO data (NUTS2 level)
    df = pd.read_csv(
        "data/ENSPRESO/ENSPRESO_Integrated_NUTS2_Data2021.csv", encoding="latin1")

    # Select relevant columns, rename NUTS2 code column
    df = df[[
        "nuts2_code",
        "biomass_production_twh_medium_total",
        "biomass_production_twh_low_total",
        "biomass_production_twh_high_total",
        "wind_onshore_production_twh_medium",
        "wind_onshore_production_twh_low",
        "wind_onshore_production_twh_high",
        "solar_production_twh_medium_total",
        "solar_production_twh_low_total",
        "solar_production_twh_high_total"
    ]]
    df.rename(columns={"nuts2_code": "NUTS2"}, inplace=True)

    # Filter columns based on selected scenario
    scenario_cols = [col for col in df.columns if scenario in col]
    selected_cols = ["NUTS2"] + scenario_cols
    df_scenario = df[selected_cols].copy()

    # Compute total ENSPRESO production for wind and solar at scenario level
    wind_col = next(
        (col for col in df_scenario.columns if "wind" in col.lower()), None)
    solar_col = next(
        (col for col in df_scenario.columns if "solar" in col.lower()), None)

    df_scenario["ENSPRESO Production (TWh)"] = df_scenario[wind_col] + \
        df_scenario[solar_col]

    return df_scenario[["NUTS2", wind_col, solar_col, "ENSPRESO Production (TWh)"]]


def load_nuts2_geometry():
    gdf = gpd.read_file(
        "data/NUTS/NUTS_RG_20M_2021_4326/NUTS_RG_20M_2021_4326.shp")
    gdf = gdf[gdf["LEVL_CODE"] == 2].copy()
    gdf.rename(columns={"NUTS_ID": "NUTS2"}, inplace=True)
    return gdf


def merge_and_calculate_ratios(production_NUTS2, enspreso_df):
    gdf = load_nuts2_geometry()

    # Merge Eurostat production with ENSPRESO data on NUTS2
    merged = production_NUTS2.merge(enspreso_df, on="NUTS2", how="left")

    # Merge with geometries
    merged_gdf = gdf.merge(merged, on="NUTS2", how="left")

    # Calculate untapped ratios (%)
    merged_gdf["Total Production (TWh)"] = merged_gdf["Wind Onshore (TWh)"] + \
        merged_gdf["Solar (TWh)"]

    merged_gdf["untapped ratio RES"] = (
        merged_gdf["Total Production (TWh)"] /
        merged_gdf["ENSPRESO Production (TWh)"]
    ) * 100

    wind_col = next(
        (col for col in enspreso_df.columns if "wind" in col.lower()), None)
    solar_col = next(
        (col for col in enspreso_df.columns if "solar" in col.lower()), None)

    merged_gdf["untapped ratio wind"] = (
        merged_gdf["Wind Onshore (TWh)"] / merged_gdf[wind_col]
    ) * 100
    merged_gdf["untapped ratio solar"] = (
        merged_gdf["Solar (TWh)"] / merged_gdf[solar_col]
    ) * 100

    # Replace infinite values and NaNs for plotting
    merged_gdf.replace([float("inf"), -float("inf")],
                       float("nan"), inplace=True)

    return gpd.GeoDataFrame(merged_gdf, geometry="geometry")


def get_filtered_gdf(resource, gdf):
    if resource == "wind":
        return gdf[["geometry", "untapped ratio wind"]].rename(columns={"untapped ratio wind": "value"})
    elif resource == "solar":
        return gdf[["geometry", "untapped ratio solar"]].rename(columns={"untapped ratio solar": "value"})
    elif resource == "total":
        return gdf[["geometry", "untapped ratio RES"]].rename(columns={"untapped ratio RES": "value"})
    else:
        raise ValueError("Invalid resource type")


def get_fill_color_expr(resource):
    if resource == "total":
        # interpolate R, G, B from dark green (0,100,0) to white (255,255,255)
        return "[0 + 255 * (properties.value / 100), 100 + 155 * (properties.value / 100), 0 + 255 * (properties.value / 100), 160]"
    elif resource == "wind":
        return "[0, 100 + 155 * (properties.value / 100), 255, 160]"
    elif resource == "solar":
        return "[255, 100 + 155 * (properties.value / 100), 0, 160]"
    else:
        return "[200, 200, 200, 160]"


def add_legend(resource):
    if resource == "total":
        gradient = "linear-gradient(to right, rgb(0,100,0), rgb(200,255,200))"
    elif resource == "wind":
        gradient = "linear-gradient(to right, rgb(0,60,130), rgb(0,210,255))"
    elif resource == "solar":
        gradient = "linear-gradient(to right, rgb(255,120,0), rgb(255,255,0))"
    else:
        gradient = "linear-gradient(to right, grey, white)"

    st.markdown(f"""
    <style>
    .legend-container {{
        display: flex;
        flex-direction: column;
        align-items: center;
        margin-top: 15px;
        font-family: sans-serif;
    }}
    .legend-bar {{
        background: {gradient};
        height: 20px;
        width: 300px;
        border: 1px solid #555;
        margin-bottom: 5px;
    }}
    .legend-labels {{
        display: flex;
        justify-content: space-between;
        width: 300px;
        font-size: 14px;
    }}
    </style>
    <div class="legend-container">
        <div class="legend-bar"></div>
        <div class="legend-labels">
            <span>0%</span>
            <span>50%</span>
            <span>100%</span>
        </div>
    </div>
    """, unsafe_allow_html=True)


def mapping(gdf, resource):
    st.title("Map of Renewable Energy Sources (RES) Analysis")

    # Ensure CRS is EPSG:4326
    if gdf.crs is not None and gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(epsg=4326)

    gdf = gdf.dropna(subset=["value"])
    gdf["lon"] = gdf.geometry.centroid.x
    gdf["lat"] = gdf.geometry.centroid.y

    geojson_data = json.loads(gdf.to_json())

    get_fill_color = get_fill_color_expr(resource)

    layer = pdk.Layer(
        "GeoJsonLayer",
        data=geojson_data,
        opacity=0.6,
        stroked=True,
        filled=True,
        extruded=False,
        wireframe=True,
        get_fill_color=get_fill_color,
        get_line_color=[0, 0, 0, 255],
        lineWidthMinPixels=2,
        pickable=True,
    )

    view_state = pdk.ViewState(
        longitude=gdf["lon"].mean(),
        latitude=gdf["lat"].mean(),
        zoom=4
    )

    st.pydeck_chart(pdk.Deck(
        map_style="mapbox://styles/mapbox/light-v9",
        initial_view_state=view_state,
        layers=[layer],
        tooltip={
            "html": "<b>NUTS2:</b> {NUTS2}<br/><b>Untapped:</b> {value}%",
            "style": {
                "backgroundColor": "white",
                "color": "black"
            }
        }
    ))

    add_legend(resource)


def supply():
    st.title("Renewable Energy Production vs ENSPRESO Potential")

    scenario = st.selectbox(
        "Select ENSPRESO scenario:",
        options=["medium", "low", "high"],
        index=0,
        help="Choose the ENSPRESO scenario to compare"
    )

    # Load data
    with st.spinner("Loading Eurostat production data..."):
        production_NUTS2 = eurostat_production()

    with st.spinner("Loading ENSPRESO data..."):
        enspreso_df = enspreso(scenario)

    with st.spinner("Merging datasets and calculating ratios..."):
        merged_gdf = merge_and_calculate_ratios(production_NUTS2, enspreso_df)

    # Select resource type for map display
    resource = st.radio(
        "Select resource to display on the map:",
        options=["total", "wind", "solar"],
        index=0,
        help="Choose to display total RES, wind only, or solar only untapped potential."
    )

    # Filter GeoDataFrame for mapping
    filtered_gdf = get_filtered_gdf(resource, merged_gdf)

    # Add necessary columns for tooltip
    filtered_gdf["NUTS2"] = merged_gdf["NUTS2"]

    # Show map
    mapping(filtered_gdf, resource)
    powerplant_map()


def powerplant_map():
    # Load the powerplant data
    path = "data/EnergyMonitor/EU_powerplants_February_2025.csv"
    df = pd.read_csv(path)

    # Filter out rows with missing coordinates
    df = df.dropna(subset=["Latitude", "Longitude"])

    # Optional: set tooltips
    tooltip = {
        "html": "<b>{Name}</b><br/>Fuel: {Fuel_type}<br/>Capacity: {Capacity_MW} MW",
        "style": {
            "backgroundColor": "white",
            "color": "black",
            "fontSize": "12px"
        }
    }

    # Define the pydeck layer
    layer = pdk.Layer(
        "ScatterplotLayer",
        data=df,
        get_position='[Longitude, Latitude]',
        get_radius=10000,  # in meters, adjust as needed
        get_fill_color='[200, 30, 0, 160]',  # RGBA
        pickable=True
    )

    # Define the view
    view_state = pdk.ViewState(
        latitude=df["Latitude"].mean(),
        longitude=df["Longitude"].mean(),
        zoom=4,
        pitch=0
    )

    # Render the map
    st.pydeck_chart(pdk.Deck(
        layers=[layer],
        initial_view_state=view_state,
        tooltip=tooltip
    ))

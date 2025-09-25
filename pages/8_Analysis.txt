import pydeck as pdk
import streamlit as st
import pandas as pd
import plotly.express as px
import geopandas as gpd
from shapely import wkt
import json

df_2018 = pd.read_csv("data/results clustering/DBSCAN_report_EU-MIX-2018.csv")
df_2050 = pd.read_csv(
    "data/results clustering/DBSCAN_report_ElectrificationECM.csv")
list_df = [df_2018, df_2050]
scenario_names = ["EU-MIX-2018", "Electrification-2050"]


def industrialindex(scenario, grouped, RES_source):

    df_enspreso = pd.read_csv(
        "data/ENSPRESO/ENSPRESO_Integrated_NUTS2_Data2021.csv")
    # Load NUTS shapefile and filter to level 2 only
    gdf_nuts = gpd.read_file(
        "data/NUTS/NUTS_RG_20M_2021_4326/NUTS_RG_20M_2021_4326.shp")
    gdf_nuts2 = gdf_nuts[gdf_nuts["LEVL_CODE"] == 2][["NUTS_ID", "geometry"]]

    # Force all prefixes to wind or solar only — never biomass
    if RES_source == "wind":
        columns_selected = [f"wind_onshore_production_twh_{scenario}"]
    elif RES_source == "solar":
        columns_selected = [f"solar_production_twh_{scenario}_total"]
    elif RES_source == "solar+wind":
        columns_selected = [
            f"wind_onshore_production_twh_{scenario}", f"solar_production_twh_{scenario}_total"]

    scenario = scenario.lower()
    suffix = "total"

    # Match only solar and wind columns for the given scenario and _total

    selected_columns = ["nuts2_code"] + [
        col for col in columns_selected
    ]

    filtered_enspreso = df_enspreso[selected_columns]

    # Add summed RES_total column
    if len(selected_columns) > 1:
        res_columns = selected_columns[1:]  # skip nuts2_code
        filtered_enspreso["RES_total"] = filtered_enspreso[res_columns].sum(
            axis=1)

    # Create a lookup dictionary for NUTS2 -> RES_total
    nuts2_res_map = dict(
        zip(filtered_enspreso["nuts2_code"], filtered_enspreso["RES_total"])
    )

    # Sum RES_total values from matching NUTS2 codes per cluster
    def sum_res_from_nuts2(nuts2_list):
        return sum(nuts2_res_map.get(code, 0) for code in nuts2_list)

    grouped["RES"] = grouped["NUTS2"].apply(sum_res_from_nuts2)
    grouped["industrial_res_index"] = (
        grouped["electricity"] / 3_600_000) / grouped["RES"]
    # Function to merge all NUTS2 geometries corresponding to a list of NUTS2 codes

    def merge_nuts2_geometries(nuts2_list):
        matched = gdf_nuts2[gdf_nuts2["NUTS_ID"].isin(nuts2_list)]
        return matched.unary_union if not matched.empty else None

    # Apply this function to grouped["NUTS2"]
    grouped["nuts2_geometry"] = grouped["NUTS2"].apply(merge_nuts2_geometries)

    return grouped


def mapping(gdf, layer, RES_source, title):
    import geopandas as gpd
    import json
    import pydeck as pdk
    import streamlit as st
    st.markdown(f"#### {title}")
    # Define colour scales
    color_scale = {
        "wind": [0, 112, 192],        # blue
        "solar": [255, 165, 0],       # orange-yellow
        "solar+wind": [0, 100, 0]     # dark green
    }

    layers = []

    # Prepare centroid GeoDataFrame with lon/lat and set CRS
    centroids_gdf = gpd.GeoDataFrame(
        gdf[["cluster", "industrial_res_index", "cluster_centroid"]].copy(),
        geometry="cluster_centroid",
        crs="EPSG:4326"
    )
    centroids_gdf["lon"] = centroids_gdf["cluster_centroid"].apply(
        lambda point: point.x if point else None)
    centroids_gdf["lat"] = centroids_gdf["cluster_centroid"].apply(
        lambda point: point.y if point else None)

    # Drop Shapely geometry columns to avoid serialization errors in Pydeck
    centroids_gdf = centroids_gdf.drop(
        columns=["cluster_centroid", "geometry"], errors='ignore')

    if layer in ["NUTS2_potential", "both"]:
        nuts_gdf = gpd.GeoDataFrame(
            gdf.dropna(subset=["nuts2_geometry"]).copy(),
            geometry="nuts2_geometry",
            crs="EPSG:4326"
        )
        nuts_gdf = nuts_gdf.copy()
        nuts_gdf = nuts_gdf[["cluster_index", "RES", "nuts2_geometry"]]

        nuts_gdf = nuts_gdf.set_geometry("nuts2_geometry")

        assert all(nuts_gdf.geometry.type.isin(['Polygon', 'MultiPolygon']))
        nuts_json = json.loads(nuts_gdf.to_json())
        nuts_layer = pdk.Layer(
            "GeoJsonLayer",
            data=nuts_json,
            get_fill_color=color_scale[RES_source] + [100],
            get_line_color=[255, 255, 255],
            pickable=True,
            stroked=True,
            filled=True
        )
        layers.append(nuts_layer)

    if layer in ["Cluster", "both"]:
        cluster_layer = pdk.Layer(
            "ScatterplotLayer",
            data=centroids_gdf,
            get_position=["lon", "lat"],
            get_radius=20000,
            get_fill_color="[industrial_res_index * 100 * 2.55, 0, 0, 160]",
            pickable=True,
            tooltip={
                "text": "Cluster: {cluster}\nIndex: {industrial_res_index:.0%}"}
        )
        layers.append(cluster_layer)

    view_state = pdk.ViewState(latitude=48, longitude=10, zoom=4, pitch=0)

    col1, col2 = st.columns([4, 1])

    with col1:
        st.pydeck_chart(pdk.Deck(
            layers=layers,
            initial_view_state=view_state,
            map_style="light",
            tooltip={
                "text": "Cluster: {cluster}\nIndex: {industrial_res_index}"}
        ))

    with col2:
        st.markdown("""
        <style>
        .legend-container {
            background-color: #f9f9f9;
            padding: 10px;
            border-radius: 5px;
            font-family: Arial, sans-serif;
            margin-top: 10px;
            width: 100%;
        }
        .legend-title {
            font-weight: bold;
            margin-bottom: 8px;
        }
        .color-bar {
            height: 15px;
            background: linear-gradient(to right, rgba(255,0,0,0.2), rgba(255,0,0,1));
            margin-bottom: 5px;
        }
        .res-boxes {
            display: flex;
            flex-direction: column;
            gap: 6px;
            margin-top: 8px;
        }
        .res-row {
            display: flex;
            align-items: center;
            gap: 6px;
        }
        .res-box {
            width: 20px;
            height: 20px;
            display: inline-block;
            border: 1px solid #000;
        }
        </style>

        <div class="legend-container">
            <div class="legend-title">Legend</div>
            <div><strong>Cluster colour = Industrial RES index</strong></div>
            <div class="color-bar"></div>
            <div style="font-size: 12px; display: flex; justify-content: space-between;">
                <span>0</span><span>1</span>
            </div>
            <div style="margin-top: 10px;"><strong>Region fill = RES potential</strong></div>
            <div class="res-boxes">
                <div class="res-row"><div class="res-box" style="background-color: rgb(0,112,192);"></div><span style="font-size: 12px;">Wind</span></div>
                <div class="res-row"><div class="res-box" style="background-color: rgb(255,165,0);"></div><span style="font-size: 12px;">Solar</span></div>
                <div class="res-row"><div class="res-box" style="background-color: rgb(0,100,0);"></div><span style="font-size: 12px;">Wind+Solar</span></div>
            </div>
        </div>
        """, unsafe_allow_html=True)


def industrial_index_plot(gdf):
    st.write(gdf["industrial_res_index"])


# Your sector colors
SECTOR_COLORS = {
    "Cement": "rgb(102, 102, 102)",
    "Chemical": "rgb(31, 119, 180)",
    "Fertilisers": "rgb(44, 160, 44)",
    "Glass": "rgb(148, 103, 189)",
    "Refineries": "rgb(255, 127, 14)",
    "Steel": "rgb(127, 127, 127)"
}


def plot_cluster_sector_sites_stacked(df, sort_by="total_energy", title=""):
    """
    Plots a stacked bar chart of number of sites per cluster,
    stacked by sector with colors defined in SECTOR_COLORS.
    Clusters on x-axis are sorted by total energy or electricity.
    Adds points on a secondary y-axis showing total energy or electricity per cluster.

    Parameters:
    - df: DataFrame with columns 'cluster', 'sector_name', 'site_name', 'total_energy', 'electricity_[gj/t] ton'
    - sort_by: "total_energy" or "electricity"
    - title: plot title
    """
    # Filter clustered sites only
    clustered_sites_df = df[df["cluster"] != -1]

    # Aggregate counts and sums per cluster-sector
    cluster_sector_counts = (
        clustered_sites_df.groupby(['cluster', 'sector_name'])
        .agg(
            num_sites=('site_name', 'count'),
            total_energy=('total_energy', 'sum'),
            electricity_GJ=('electricity_[gj/t] ton', 'sum')
        )
        .reset_index()
    )

    # Aggregate sorting metric per cluster (sum over sectors)
    cluster_sums = (
        cluster_sector_counts.groupby('cluster')
        .agg(
            total_energy_sum=('total_energy', 'sum'),
            electricity_sum=('electricity_GJ', 'sum')
        )
        .reset_index()
    )

    # Convert units for plotting secondary y-axis:
    # total_energy in PJ (divide joules by 1e15)
    cluster_sums['total_energy_PJ'] = cluster_sums['total_energy_sum'] / 1e15
    # electricity in TWh (divide GJ by 3,600,000)
    cluster_sums['electricity_TWh'] = cluster_sums['electricity_sum'] / 3_600_000

    if sort_by == "total_energy":
        cluster_sums = cluster_sums.sort_values(
            by='total_energy_PJ', ascending=False)
        sort_metric = 'total_energy_PJ'
        metric_label = 'Total Energy (PJ)'
    elif sort_by == "electricity":
        cluster_sums = cluster_sums.sort_values(
            by='electricity_TWh', ascending=False)
        sort_metric = 'electricity_TWh'
        metric_label = 'Electricity (TWh)'
    else:
        raise ValueError("sort_by must be 'total_energy' or 'electricity'")

    # Define cluster order based on sorting
    cluster_order = cluster_sums['cluster'].tolist()

    # Convert 'cluster' column to categorical with order to control x-axis order
    cluster_sector_counts['cluster'] = pd.Categorical(
        cluster_sector_counts['cluster'], categories=cluster_order, ordered=True)
    cluster_sums['cluster'] = pd.Categorical(
        cluster_sums['cluster'], categories=cluster_order, ordered=True)

    # Plot stacked bar chart: clusters on x-axis, number of sites on y-axis
    fig = px.bar(
        cluster_sector_counts,
        x='cluster',
        y='num_sites',
        color='sector_name',
        color_discrete_map=SECTOR_COLORS,
        labels={
            'cluster': 'Cluster',
            'num_sites': 'Number of Sites',
            'sector_name': 'Sector'
        },
        title=title
    )

    # Add scatter trace for total energy or electricity on secondary y-axis with dots only (no line)
    fig.add_scatter(
        x=cluster_sums['cluster'],
        y=cluster_sums[sort_metric],
        mode='markers',
        name=metric_label,
        marker=dict(color='black', size=10, symbol='circle'),
        yaxis='y2'
    )

    # Update layout to add secondary y-axis on right side and legend below plot
    fig.update_layout(
        barmode='stack',
        xaxis=dict(categoryorder='array', categoryarray=cluster_order),
        yaxis=dict(title='Number of Sites'),
        yaxis2=dict(
            title=metric_label,
            overlaying='y',
            side='right',
            showgrid=False
        ),
        legend=dict(
            orientation='h',
            y=-0.2,
            x=0,
            xanchor='left',
            yanchor='top'
        ),
        template='plotly_white'
    )

    st.plotly_chart(fig, use_container_width=True)


def plot_elec_share(gdf, title):
    fig = px.scatter(
        gdf,
        x="cluster_index",
        y="electricity_share",
        labels={
            "cluster_index": "Cluster Index",
            "electricity_share": "Electricity Share (%)"
        },
        title=f"{title}"
    )
    st.plotly_chart(fig)

# # Usage: plot sector stacked charts for both scenarios and both sorting metrics
# for df, name in zip(list_df, scenario_names):
#     st.write(f"## Scenario: {name}")

#     st.write("### Sector plot ordered by total energy")
#     plot_cluster_sector_sites_stacked(
#         df,
#         sort_by="total_energy",
#         title=f"Sites by cluster and sector (total energy) - {name}"
#     )


#     st.write("### Sector plot ordered by electricity")
#     plot_cluster_sector_sites_stacked(
#         df,
#         sort_by="electricity",
#         title=f"Sites by cluster and sector (electricity) - {name}"
#     )
for df, name in zip(list_df, scenario_names):
    st.write(df)
    df["geometry"] = df["geometry"].apply(wkt.loads)
    gdf = gpd.GeoDataFrame(df, geometry="geometry")
    # Total number of sites (all rows)
    total_sites = df["site_name"].count()

    # Number of unclustered sites (cluster == -1)
    unclustered_count = df[df["cluster"] == -1]["site_name"].count()

    # Number of clustered sites per cluster (cluster != -1)
    clustered_sites_df = df[df["cluster"] != -1]

    cluster_counts = clustered_sites_df.groupby(
        "cluster")["site_name"].count().reset_index()

    cluster_counts.rename(columns={"site_name": "num_sites"}, inplace=True)
    clustered_total = cluster_counts["num_sites"].sum()

    # Check total sites sum correctly
    assert total_sites == (clustered_total + unclustered_count), \
        f"Site count mismatch: total={total_sites}, clustered={clustered_total}, unclustered={unclustered_count}"

    # Prepare data for plotting
    grouped = clustered_sites_df.groupby("cluster").agg(
        total_energy=('total_energy', 'sum'),
        electricity=('electricity_[gj/t] ton', 'sum'),
        nuts3_codes=('nuts3_code', lambda x: list(x.unique()))
    ).reset_index()

    # Derive NUTS2 codes from NUTS3 codes by removing the last character and taking unique values
    grouped["NUTS2"] = grouped["nuts3_codes"].apply(
        lambda codes: list({code[:-1] for code in codes}))

    # Create continuous cluster index starting at 1
    cluster_labels_sorted = sorted(grouped["cluster"].unique())
    cluster_index_map = {orig_label: idx + 1 for idx,
                         orig_label in enumerate(cluster_labels_sorted)}
    grouped["cluster_index"] = grouped["cluster"].map(cluster_index_map)

    # ____Cluster centroid___
    # Make sure df has a valid geometry column as GeoSeries
    gdf = gpd.GeoDataFrame(df, geometry="geometry")

    # Filter only clustered sites
    clustered_gdf = gdf[gdf["cluster"] != -1]

    # Compute centroid per cluster
    cluster_centroids = clustered_gdf.dissolve(
        by="cluster").centroid.reset_index()
    cluster_centroids.columns = ["cluster", "cluster_centroid"]

    # Merge centroid into grouped dataframe
    grouped = grouped.merge(cluster_centroids, on="cluster", how="left")

    grouped_convert = grouped.copy()
    # Convert units
    grouped_convert["electricity_TWh"] = grouped["electricity"] / 3_600_000
    grouped_convert["total_energy_PJ"] = grouped["total_energy"] / 1_000_000

    custom_colorscale = [
        [0.0, "#5c4033"],  # dark brown
        [0.5, "#cc7722"],  # bronze/orange
        [1.0, "#ffff66"]   # light yellow
    ]

    fig = px.scatter(
        grouped_convert,
        x="cluster_index",
        y="total_energy_PJ",
        color="electricity_TWh",
        color_continuous_scale=custom_colorscale,
        labels={
            "cluster_index": "Cluster (index)",
            "total_energy_PJ": "Total Energy (PJ)",
            "electricity_TWh": "Electricity (TWh)"
        },
        title=f"Cluster Energy Profile - {name}"
    )

    fig.update_traces(marker=dict(size=12))
    fig.update_layout(
        coloraxis_colorbar=dict(title="Electricity (TWh)"),
        template="plotly_white",
        xaxis=dict(tickmode='linear', dtick=1)
    )

    st.plotly_chart(fig, use_container_width=True)

    RES_source = st.radio("Select RES source:", [
                          "wind", "solar", "solar+wind"], key=f'res_source{name}')
    enspreso_level = st.radio(
        "ENSPRESO level", ["high", "medium", "low"], key=f'level{name}')

    gdf = industrialindex(enspreso_level, grouped, RES_source)

    gdf["electricity_share"] = gdf["electricity"]/gdf["total_energy"]*100

    st.write(gdf)

    title_share = f"Share of electrcity for industrial clusters with {name}"

    plot_elec_share(gdf, title_share)

    layer = st.radio("Select layer", [
                     'Cluster', 'NUTS2_potential', 'both'], key=f'layer{name}')

    title = f"Map of industrial clusters with {name} and ENSPRESO {enspreso_level}"

    mapping(gdf, layer, RES_source, title)

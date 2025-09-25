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
from shapely.geometry import MultiPoint
from sklearn.cluster import DBSCAN, KMeans
from sklearn.preprocessing import StandardScaler

from geopy.distance import geodesic
from tool_modules.convert import *


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


def kmeans_threshold(gdf, n_clusters, value_type, redistribute) -> gpd.GeoDataFrame:
    """
    Perform KMeans clustering on a GeoDataFrame using lat/lon coordinates,
    then only retain clusters whose total value (energy or emissions) exceeds the threshold.
    Optionally redistributes points from undersized clusters to closest valid cluster.

    Parameters:
    gdf (GeoDataFrame): Input GeoDataFrame with Point geometries.
    value_type (str): "Energy" or "Emissions".
    threshold (float): Minimum total value per cluster (in GJ or ktCO₂).

    Returns:
    GeoDataFrame: GeoDataFrame with a new 'cluster' column.
    """

    value_col_map = {
        "Energy": "total_energy",
        "Emissions": "Direct CO2 emissions (t)"
    }
    if value_type not in value_col_map:
        raise ValueError("value_type must be either 'Energy' or 'Emissions'")

    value_col = value_col_map[value_type]
    if value_col not in gdf.columns:
        raise KeyError(f"Column '{value_col}' not found in GeoDataFrame")

    # Coordinates extraction
    gdf['lon'] = gdf.geometry.x
    gdf['lat'] = gdf.geometry.y

    coords = gdf[['lat', 'lon']].to_numpy()
    coords_scaled = StandardScaler().fit_transform(coords)

    # KMeans clustering
    kmeans = KMeans(n_clusters, random_state=0, n_init="auto")
    gdf['kmeans_label'] = kmeans.fit_predict(coords_scaled)
    # Cluster value totals
    cluster_totals = gdf.groupby("kmeans_label")[value_col].sum()
    # Step 1: Get raw min/max values from cluster totals
    min_val = int(min(cluster_totals))
    max_val = int(max(cluster_totals))
    # Step 2: Define base unit depending on value_type
    if value_type == "Energy":
        base_unit = "GJ"
    elif value_type == "Emissions":
        base_unit = "t"
    else:
        st.error(f"Unknown value type: {value_type}")
        st.stop()

    # Ensure integers with proper rounding
    min_val_rounded = math.floor(min_val)
    max_val_rounded = math.ceil(max_val)

    # Step 4: Use slider directly in converted units
    limit_converted = st.slider(
        f"{value_type} threshold ({base_unit})",
        min_value=min_val_rounded,
        max_value=max_val_rounded - 1,
        value=min_val_rounded,
        step=1
    )

    # Step 6: Filter valid clusters
    valid_clusters = cluster_totals[cluster_totals >=
                                    limit_converted].index.to_numpy()

    # Assign clusters (invalid clusters get -1)
    gdf['cluster'] = -1
    gdf.loc[gdf['kmeans_label'].isin(
        valid_clusters), 'cluster'] = gdf['kmeans_label']

    # Optionally reassign small-cluster points
    if redistribute == "Yes" and len(valid_clusters) > 0:
        valid_centroids = (
            gdf[gdf['cluster'] != -1]
            .groupby("cluster")[["lat", "lon"]]
            .mean()
            .to_numpy()
        )

        mask_unassigned = gdf['cluster'] == -1
        unassigned_coords = gdf.loc[mask_unassigned, ["lat", "lon"]].to_numpy()

        dist_matrix = np.linalg.norm(
            unassigned_coords[:, None, :] - valid_centroids[None, :, :],
            axis=2
        )

        nearest = dist_matrix.argmin(axis=1)
        gdf.loc[mask_unassigned, 'cluster'] = valid_clusters[nearest]

    return gdf.drop(columns=["kmeans_label"])


def cluster_gdf_dbscan(gdf, min_samples, radius):
    """
    Perform DBSCAN clustering on a GeoDataFrame using lat/lon.

    Parameters:
    gdf (GeoDataFrame): Input GeoDataFrame with Point geometries.
    min_samples (int): Minimum number of points to form a cluster.
    eps (float): Maximum distance between points in the same cluster (in degrees).

    Returns:
    GeoDataFrame: GeoDataFrame with an added 'cluster' column.
    """
    # Ensure geometry is in lat/lon
    gdf.loc[:, "lat"] = gdf.geometry.y
    gdf.loc[:, "long"] = gdf.geometry.x
    coords_rad = np.radians(gdf[["lat", "long"]])

    db = DBSCAN(
        eps=radius / 6371.0,  # Convert radius from km to radians
        min_samples=min_samples,
        metric="haversine",
        algorithm="ball_tree",
    ).fit(coords_rad)
    gdf['cluster'] = db.labels_

    return gdf


# Summarise clusters values and aggreagatge in centroid cluster (exculde -1)
def summarise_clusters_by_centroid(gdf_clustered):
    """
    Summarise clustered GeoDataFrame by computing the centroid of each cluster
    and summing all type_ener_feed columns cluster.

    Parameters:
    gdf_clustered (GeoDataFrame): Clustered GeoDataFrame with 'cluster' column.

    Returns:
    GeoDataFrame: GeoDataFrame with one row cluster, centroid location, and
                  sum of all type_ener_feed columns.
    """
    if 'cluster' not in gdf_clustered.columns:
        raise ValueError("GeoDataFrame must contain a 'cluster' column.")
    columns = [col for col in gdf_clustered.columns if any(
        col.startswith(f"{feed} ") for feed in type_ener_feed) or col == "total_energy" or col == "Direct CO2 emissions (t)"]

    def _cluster_centroid(cluster_df):
        """Compute centroid of a cluster."""
        if cluster_df.empty:
            return None
        points = MultiPoint(cluster_df.geometry.tolist())
        return [points.centroid.y, points.centroid.x]

    if (gdf_clustered["cluster"] != -1).any():
        cluster_centers = (
            gdf_clustered[gdf_clustered["cluster"] != -
                          1].groupby("cluster").apply(_cluster_centroid)
        )
    else:
        return gdf_clustered
    centroids_df = pd.DataFrame(
        cluster_centers.tolist(),
        columns=["Latitude", "Longitude"],
        index=cluster_centers.index,
    )

    gdf_clustered_center = gpd.GeoDataFrame(
        centroids_df,
        geometry=gpd.points_from_xy(
            centroids_df.Longitude, centroids_df.Latitude),
        crs="EPSG:4326",
    )

    # Group by cluster and sum energy columns
    summary = gdf_clustered.groupby(
        "cluster")[columns].sum().reset_index()

    # Compute geometry as centroid of cluster
    gdf_summary = summary.merge(gdf_clustered_center, on="cluster")

    gdf_summary = gpd.GeoDataFrame(
        gdf_summary, geometry='geometry', crs="EPSG:4326")
    return gdf_summary


# KMeans clustering for GeoDataFrame
def cluster_gdf_kmeans(gdf, n_clusters=5):
    """
    Perform KMeans clustering on a GeoDataFrame using lat/lon.

    Parameters:
    gdf (GeoDataFrame): Input GeoDataFrame with Point geometries.
    n_clusters (int): The number of clusters to form.

    Returns:
    GeoDataFrame: GeoDataFrame with an added 'cluster' column.
    """
    # Ensure geometry is in lat/lon
    gdf['lon'] = gdf.geometry.x
    gdf['lat'] = gdf.geometry.y

    coords = gdf[['lat', 'lon']].to_numpy()
    scaler = StandardScaler()
    coords_scaled = scaler.fit_transform(coords)

    kmeans = KMeans(n_clusters=n_clusters, random_state=0).fit(coords_scaled)
    gdf['cluster'] = kmeans.labels_

    return gdf


def cluster_gdf_kmeans_weight(gdf, value_type, n_clusters=5):
    """
    Perform KMeans clustering on a GeoDataFrame using lat/lon and weighted by total_energy or emissions.

    Parameters:
        gdf (GeoDataFrame): Input GeoDataFrame with Point geometries and relevant columns.
        value_type (str): Either "Energy" or "Emissions".
        n_clusters (int): The number of clusters to form.

    Returns:
        GeoDataFrame: GeoDataFrame with an added 'cluster' column.
    """
    # Ensure geometry is in lat/lon
    gdf['lon'] = gdf.geometry.x
    gdf['lat'] = gdf.geometry.y

    coords = gdf[['lat', 'lon']].to_numpy()
    scaler = StandardScaler()
    coords_scaled = scaler.fit_transform(coords)

    # Select weight column based on value_type
    if value_type == "Energy":
        col = 'total_energy'
    elif value_type == "Emissions":
        col = 'Direct CO2 emissions (t)'
    else:
        st.error(f"Unsupported value_type: {value_type}")
        return None

    if col not in gdf.columns:
        st.error(
            f"GeoDataFrame must contain '{col}' column for {value_type} clustering.")
        return None

    # Convert weights to float safely
    weights = gdf[col].astype(float).to_numpy()

    # Handle NaNs by replacing them with 0
    weights = np.nan_to_num(weights, nan=0.0)

    max_weight = weights.max()
    if max_weight == 0:
        st.error("All weights are zero, clustering cannot proceed.")
        return None

    weights_normalised = weights / max_weight

    kmeans = KMeans(n_clusters=n_clusters, random_state=0)
    kmeans.fit(coords_scaled, sample_weight=weights_normalised)

    gdf['cluster'] = kmeans.labels_

    return gdf

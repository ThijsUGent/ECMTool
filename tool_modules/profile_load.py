import pandas as pd
import plotly.graph_objects as go
import streamlit as st
import numpy as np
import calendar
import plotly.express as px
from tool_modules.loading_data import *

country_offshore = [
    "BE", "BG", "HR", "CY", "DK", "EE", "FI", "FR", "DE", "EL",
    "IE", "IT", "LV", "LT", "MT", "NL", "PL", "PT", "RO", "ES",
    "SE", "UK"
]


def profile_load():
    st.title("Industrial Load Profile Matching")
    st.subheader("Renewable Profile Selection")

    # --- Step 1: Button to trigger loading ---
    # Check if data is loaded
    if "archives" not in st.session_state:
        st.warning("Please download the time series data on Page 2 (`2_RES2Go session.py`) first.")

        # Provide a clickable link to the other page
        st.page_link("pages/2_RES2Go Session.py", label=" **RES2Go Session** ",
             icon="👤", use_container_width=True)

        # Stop this page until data exists
        st.stop()

    # --- Step 2: once archives are loaded, show the rest of the app ---
    archives = st.session_state.archives


    # col1 = selection and parameters, col2 = plot
    col1, col2 = st.columns([1, 2])

    with col1:
        # init values
        index_unit = 0
        elec_demand = 3.6
        NUTS2_list_2013, NUTS2_list_2021, NUTS2_list = [], [], []

        method_choice = st.radio(
            "Select the method to assess electricity profile",
            ["From cluster", "Manual input"]
        )
        if method_choice == "From cluster":
            if "saved_clusters" in st.session_state:
                df_cluster = st.session_state["saved_clusters"]
                if df_cluster is not None and not df_cluster.empty:
                    cluster_names = df_cluster["name"].unique()
                    cluster_selected = st.selectbox(
                        'Cluster name', cluster_names)
                    
                    NUTS2_list_2013 = [
                        nuts for nuts in df_cluster[df_cluster["name"] == cluster_selected]["NUTS2_2013"].values[0]]
                    NUTS2_list_2021 = [
                        nuts for nuts in df_cluster[df_cluster["name"] == cluster_selected]["NUTS2_2021"].values[0]]
                    
                    elec_demand = df_cluster[df_cluster["name"]
                                            == cluster_selected]["electricity"].iloc[0]
                    unit = df_cluster[df_cluster["name"] ==
                                    cluster_selected]["unit"].iloc[0]
                    if unit == "GJ":
                        index_unit = 0
                else:
                    st.warning("No saved clusters in session state.")
                    return
            else:
                st.warning("No saved clusters in session state.")
                return
        else:
            country = st.text_input(
                "Enter country code (e.g. 'DE')", value="BE")
            NUTS2 = st.text_input("Or enter NUTS2 region code (e.g. 'BE23')")
            if NUTS2:
                NUTS2_list = [NUTS2]
            else:
                NUTS2_list = []

        scenario = st.select_slider(
            "ENSPRESO Scenario", ["low", "medium", "high" ])

        # offshore

        country_list = list(set([code[:2] for code in NUTS2_list]))

        # Check if any of the countries are in the offshore list
        if any(c in country_offshore for c in country_list):
            offshore_choice = st.checkbox(
                "See national offshore potential", value=False)
            if offshore_choice:
                df_offshore = pd.read_csv(
                    "data/ENSPRESO/Offshore_ENSPRESO.csv")
                df_show = df_offshore[df_offshore["Country Code"].isin(
                    country_list)]
                columns_to_show = [
                    "NUTS2Nr",
                    "Scenario",
                    "Offshore categories",
                    "TWh_ < 15 % _2030_100m_ALLTIMESLICESAVERAGE_V112",
                    "TWh_15 % -20 % _2030_100m_ALLTIMESLICESAVERAGE_V112",
                    "TWh_20 % -25 % _2030_100m_ALLTIMESLICESAVERAGE_V112",
                    "TWh_Morethan25 % _2030_100m_ALLTIMESLICESAVERAGE_V112",
                    "TWH_ > 20 %",
                    "CountryElec_TWh",
                    "Turbines_TOT"
                ]

                # Safely display only existing columns
                existing_cols = [
                    col for col in columns_to_show if col in df_show.columns]

        if not NUTS2_list_2013 or not NUTS2_list_2021:
            st.warning("Please enter at least one NUTS2 region.")
            return
        solar_total = 0
        wind_total = 0
        for nuts2 in NUTS2_list_2013:
            solar, wind = enspreso_extract(nuts2, scenario)
            if solar is not None:
                solar_total += solar
            if wind is not None:
                wind_total += wind

        energy_volume_solar = st.number_input(
            "Set solar energy volume (TWh)", value=solar_total)
        energy_volume_wind = st.number_input(
            "Set onshore wind energy volume (TWh)", value=wind_total)
        year = st.slider("Select year for solar/wind data", 1986, 2015, 2015)

        data_source = st.radio("Select industry data source", [
                               "ELMAS"])
        if data_source == "JERICHO-E":
            sector = st.radio(
                "Select a sector",
                ["Food", "Glass, Ceramics, Stones", "Automotive", "Chemical",
                    "Paper", "Mechanical Engineering", "Iron, Steel", "Other Industry"]
            )
            profile, time, label = jericho_data(sector)
        else:
            profile, time, label = elmas_data()

        if profile is None or time is None:
            return

        unit = st.radio("Target energy unit", ["GJ", "TWh"], index=index_unit)
        target_energy = st.number_input(
            f"Set target energy ({unit})", value=elec_demand)
        if unit == "TWh":
            target_energy *= 3.6e6  # Convert to GJ

        # Scale industry profile
        industry_scaled = scale_profile_to_energy(
            profile, target_energy) / 3.6  # to MWh
        start_time = pd.Timestamp(f"{year}-01-01 00:00")
        time_year = pd.date_range(
            start=start_time, periods=len(time), freq="H")
        df_industry = pd.DataFrame(
            {"Time": time_year, "Industry": industry_scaled})
    

    # Fetch solar and wind profiles
    solar_profile, solar_time = solar_generation(country=country if method_choice == "Manual input" else None,
                                                 NUTS2=NUTS2_list_2013, energy_volume=energy_volume_solar, year=year)
    wind_profile, wind_time = onshore_generation(country=country if method_choice == "Manual input" else None,
                                                 NUTS2=NUTS2_list_2021, energy_volume=energy_volume_wind, year=year)


    df_solar = pd.DataFrame(
        {"Time": pd.to_datetime(solar_time), "Solar": solar_profile})
    df_wind = pd.DataFrame(
        {"Time": pd.to_datetime(wind_time), "Wind": wind_profile})

    empty_dfs = []

    if df_solar.empty:
        empty_dfs.append("Solar data")
    if df_wind.empty:
        empty_dfs.append("Wind data")
    if df_industry.empty:
        empty_dfs.append("Industry data")

    if empty_dfs:
        st.warning(f"Missing data in: {', '.join(empty_dfs)}. Check selection and inputs.")
        return

    # Merge all
    df = df_solar.merge(df_wind, on="Time").merge(df_industry, on="Time")
    df["Renewables"] = df["Solar"] + df["Wind"]
    df["Mismatch"] = np.where(
        df["Renewables"] < df["Industry"], df["Industry"] - df["Renewables"], 0)
    energy_deficit_TWh = df["Mismatch"].sum() / 1e6

    with col2:
        st.markdown(
            f"### ⚠️ Total Energy Deficit in {year}: **{energy_deficit_TWh:.2f} TWh**")

        # Curve selection
        st.markdown("### 📊 Select Curves to Display")
        show_industry = st.checkbox("Show Industry", value=True)
        show_renewables = st.checkbox("Show Renewables (Total)", value=True)
        show_solar = st.checkbox("Show Solar", value=False)
        show_wind = st.checkbox("Show Wind", value=False)
        show_deficit = st.checkbox("Show Deficit (Mismatch)", value=False)

        y_columns = []
        line_colors = {}

        if show_renewables:
            y_columns.append("Renewables")
            line_colors["Renewables"] = "green"
        if show_solar:
            y_columns.append("Solar")
            line_colors["Solar"] = "orange"
        if show_wind:
            y_columns.append("Wind")
            line_colors["Wind"] = "blue"
        if show_industry:
            y_columns.append("Industry")
            line_colors["Industry"] = "grey"

        fig = go.Figure()
        for col in y_columns:
            fig.add_trace(go.Scatter(
                x=df["Time"], y=df[col], mode="lines", name=col, line=dict(color=line_colors[col])))

        if show_deficit:
            fig.add_trace(go.Scatter(
                x=df["Time"], y=df["Mismatch"],
                name="Deficit", mode="lines", line=dict(width=0),
                fill="tozeroy", fillcolor="rgba(255,0,0,0.3)", hoverinfo="skip"
            ))

        fig.update_layout(
            title=f"Profile load with {data_source} Dataset",
            xaxis_title="Time",
            yaxis_title="Energy (MW)",
            legend_title="Profile",
            xaxis=dict(tickformat="%H:%M<br>%d %b", tickmode="auto")
        )

        st.plotly_chart(fig, use_container_width=True)


def scale_profile_to_energy(profile, target_energy):
    current_total = np.sum(profile)
    if current_total == 0:
        raise ValueError("Profile sum is zero — cannot scale.")
    return profile * (target_energy / current_total)


def elmas_data():
    df_time_cluster = pd.read_csv(
        "data/ELMAS_dataset/Time_series_18_clusters.csv", sep=";")
    df_cluster_NACE = pd.read_csv(
        "data/ELMAS_dataset/Clusters_after_manual_reclassification.csv", sep=";")
    # df_nace = pd.read_csv(
    #     "data/ELMAS_dataset/NACE_classification.csv", sep=';')

    # df_cluster_NACE["Class_description"] = df_cluster_NACE["Class"].map(
    #     df_nace.set_index("Class")["Class_description"]
    # )
    # st.write(df_cluster_NACE[df_cluster_NACE["Cluster"] == 1])
    df_time_cluster["Industry"] = df_time_cluster["1"].str.replace(
        ",", ".", regex=False).astype(float)
    profile = df_time_cluster["Industry"].values
    time = df_time_cluster["Time"]
    profile = np.array(profile, dtype=float) * 0.0036  # kWh to GJ
    return profile, time, "ELMAS baseyear 2018"


def jericho_data(sector):
    df_industry = pd.read_csv(
        "data/E-JERICHO/industrial_standard_load_profiles.csv")

    if sector not in df_industry.columns:
        st.warning("Sector not found in dataset.")
        return None, None, None

    profile = df_industry[sector].astype(float)
    df_industry["datetime"] = pd.to_datetime(
        "2022-01-01") + pd.to_timedelta(df_industry["hour"], unit="h")
    time = df_industry["datetime"]

    return profile, time, sector


def solar_generation(country, energy_volume, year, NUTS2=None):
    
    if NUTS2:
        if isinstance(NUTS2, list) and len(NUTS2) > 0:
            # First, read only the 'time_step' column to avoid errors if some NUTS2 columns are missing
            if "archives" in st.session_state:
                archives = st.session_state.archives

                file_obj = archives["PV"]["EMHIRES_PVGIS_TSh_CF_n2_19862015_reformatt.xlsx"]
                file_obj.seek(0)  # rewind to start
                df_all = pd.read_excel(file_obj, usecols=lambda x: x == "time_step" or x in NUTS2 )
            else :
                return None,None

            # Add Date column

            start_date = pd.Timestamp("1986-01-01")
            df_all["Date"] = pd.to_timedelta(df_all["time_step"], unit="h") + start_date

            # Filter by year
            df_all = df_all[df_all["Date"].dt.year == year]

            # Keep only valid NUTS2 columns that exist in df_all

            valid_columns = [col for col in NUTS2 if col in df_all.columns]
            if not valid_columns:
                st.warning("None of the specified NUTS2 regions were found in the CSV.")
                return None, None

            # Compute capacity factor profile
            capacity_factor_profile = df_all[valid_columns].sum(axis=1)
            data_source = df_all[["Date"]].copy()


        else:
            data_source = pd.read_csv("data/EMHIRES/EMHIRES_PVGIS_TSh_CF_n2_19862015_reformatt.csv",
                                      usecols=["time_step", NUTS2])
            start_date = pd.Timestamp("1986-01-01")
            data_source["Date"] = pd.to_timedelta(
                data_source["time_step"], unit="h") + start_date
            data_source = data_source[data_source["Date"].dt.year == year]

            region = NUTS2
            if region not in data_source.columns:
                st.warning("NUTS2 region not found in dataset.")
                return None, None
            capacity_factor_profile = data_source[region]
    else:
        data_source = pd.read_csv(
            "data/EMHIRES/EMHIRESPV_TSh_CF_Country_19862015.csv", usecols=["Date", country])
        data_source["Date"] = pd.to_datetime(
            data_source["Date"], dayfirst=True)
        data_source = data_source[data_source["Date"].dt.year == year]
        if country not in data_source.columns:
            st.warning("Country not found in dataset.")
            return None, None
        capacity_factor_profile = data_source[country]

    profile = capacity_factor_profile * \
        (energy_volume * 1e6 / capacity_factor_profile.sum())
    time = data_source["Date"]
    return profile, time


def onshore_generation(country, energy_volume, NUTS2=None, year=None):
    if NUTS2:
        if isinstance(NUTS2, list):
            if "archives" in st.session_state:
                archives = st.session_state.archives

                file_obj = archives["WIND"]["EMHIRES_WIND_NUTS2_June2019.csv"]
                file_obj.seek(0)  # rewind to start
                df_all = pd.read_excel(file_obj, usecols=["Time step"] + NUTS2 )
            else :
                return None,None
            start_date = pd.Timestamp("1986-01-01")
            df_all["Date"] = pd.to_timedelta(
                df_all["Time step"], unit="h") + start_date
            df_all = df_all[df_all["Date"].dt.year == year]

            valid_columns = [col for col in NUTS2 if col in df_all.columns]
            if not valid_columns:
                st.warning("None of the specified NUTS2 regions were found.")
                return None, None

            capacity_factor_profile = df_all[valid_columns].sum(axis=1)
            data_source = df_all[["Date"]].copy()
        else:
            data_source = pd.read_csv(
                "data/EMHIRES/EMHIRES_WIND_NUTS2_June2019.csv", usecols=["Time step", NUTS2])
            start_date = pd.Timestamp("1986-01-01")
            data_source["Date"] = pd.to_timedelta(
                data_source["Time step"], unit="h") + start_date
            data_source = data_source[data_source["Date"].dt.year == year]

            region = NUTS2
            if region not in data_source.columns:
                st.warning("NUTS2 region not found in dataset.")
                return None, None
            capacity_factor_profile = data_source[region]
    else:
        data_source = pd.read_csv(
            "data/EMHIRES/EMHIRES_WIND_COUNTRY_June2019.csv", usecols=["Date", country])

        # Format: "dd/mm/yyyy"
        data_source["Date"] = pd.to_datetime(
            data_source["Date"], format="%d/%m/%Y")

        # Add an hourly time per row (0 to 23, repeated for each day)
        num_rows = len(data_source)
        hours = list(range(24)) * (num_rows // 24)

        # Ensure list matches dataframe length
        if len(hours) < num_rows:
            hours += list(range(num_rows - len(hours)))

        # Add 'DateTime' with hourly resolution
        data_source["Date"] = data_source["Date"] + \
            pd.to_timedelta(hours, unit="h")
        # Select year
        data_source = data_source[data_source["Date"].dt.year == year]
        if country not in data_source.columns:
            st.warning("Country not found in dataset.")
            return None, None
        capacity_factor_profile = data_source[country]

    profile = capacity_factor_profile * \
        (energy_volume * 1e6 / capacity_factor_profile.sum())
    time = data_source["Date"]
    return profile, time


def enspreso_extract(NUTS2, level):
    col = [
        "nuts2_code",
        f"wind_onshore_production_twh_{level}",
        f"solar_production_twh_{level}_total"
    ]
    if "archives" in st.session_state:
        archives = st.session_state.archives

        file_obj = archives["ENSPRESO"]["ENSPRESO_Integrated_NUTS2_Data.csv"]
        file_obj.seek(0)  # rewind to start
        df = pd.read_csv(file_obj, sep = ';')
    else :
        return None,None
    df_filter = df[df["nuts2_code"] == NUTS2]

    if df_filter.empty:
        return None, None

    wind_series = df_filter[col[1]]
    solar_series = df_filter[col[2]]

    # Get the first (and presumably only) value as float, or None if missing
    wind = float(wind_series.iloc[0]) if not wind_series.empty else None
    solar = float(solar_series.iloc[0]) if not solar_series.empty else None

    return solar, wind

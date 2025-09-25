# aidres_app.py
import streamlit as st
import pandas as pd

st.set_page_config(layout="wide")
logo = "images/logo_UGent_EN_RGB_2400_color.png"
logo_side = "images/logo_side_bar.png"
st.logo(logo_side, size="large",
        link="https://www.ugent.be/ea/emsme/en/research/research-ensy/energy-systems-clusters/ecm", icon_image=logo)

st.subheader("Methodology")
st.markdown("""Methodology described in the preprint *link*.""")

with st.expander(" **AIDRES** "):
        
        st.markdown("""
        AIDRES (Advancing Industrial Decarbonisation by Assessing the Future Use of Renewable Energies in Industrial Processes) is an EU-funded project supporting the decarbonisation of European industry through data-driven analysis, modelling and mapping.

        ---

        ### Overview

        AIDRES develops and integrates:

        - A detailed **database** of energy-intensive industrial sites across the EU-27, mapped at NUTS-3 level.
        - **Techno-economic models** to simulate decarbonisation scenarios by 2030 and 2050 for key industrial sectors.
        - A **geographical and resource-matching framework** to identify the optimal use of renewable energy in industry.
        - A methodology for **industrial symbiosis**, exploring shared heat, energy, and carbon flows at cluster level.

        ---

        ### Project Objectives

        - Assess how industrial demand for electricity, and energy feedstocks can shift to **renewable sources**.
        - Develop pathways combining **electrification**, **green hydrogen**, **carbon capture**, and **process innovation**.
        - Map and match **local renewable energy supply potential** with **industrial energy demand**.
        - Support EU industrial and energy policy with spatially explicit data and techno-economic insights.

        """)

        st.markdown("""
        ---

        ### Project Partners

        The AIDRES project is coordinated by **EnergyVille/VITO** and involves contributions from:

        - KU Leuven  
        - Ghent University  
        - EPFL
        - Energyville
        - VUB
        - Vito
        - DECHEMA

        ---

        ### Access the Full Report

        You can read the full AIDRES final report here:  
        [AIDRES Report on EU Publications Portal](https://op.europa.eu/en/publication-detail/-/publication/577d820d-5115-11ee-9220-01aa75ed71a1/language-en)

        For more details, also visit:  
        [JRC – AIDRES Project Page](https://jeodpp.jrc.ec.europa.eu/ftp/jrc-opendata/EIGL-Data/AIDRES/)

        ---

        ### Citation

        Girardin, L., Vallee, D., Correa Laguna, J. et al. (2023).  
        *Advancing industrial decarbonisation by assessing the future use of renewable energies in industrial processes (AIDRES)*.  
        European Commission – Joint Research Centre.

        """)

with st.expander(" **ENSPRESO** "):
    st.markdown("""
    ENSPRESO (Energy System Potentials for Renewable Energy Sources) is a **comprehensive open dataset** developed by the **Joint Research Centre (JRC)** of the European Commission.  
    It provides detailed, EU-wide, **spatially explicit data** on the potentials of renewable energy sources (RES) to support long-term energy system modelling and policy analysis.

    ---

    ### Overview

    ENSPRESO includes:

    - **Biomass potentials** (agricultural residues, forestry, waste streams, energy crops).  
    - **Wind energy potentials** (onshore and offshore, with land-use restrictions).  
    - **Solar energy potentials** (PV and CSP, considering geospatial constraints).  
    - Spatial resolution at **NUTS-0, NUTS-2, and grid levels**, harmonised across the EU-28.  

    The database is designed to be compatible with **energy system models** such as PRIMES, JRC-EU-TIMES, and other techno-economic models used for EU energy scenarios.

    ---

    ### Project Objectives

    - Provide a **consistent and harmonised database** of RES potentials across Europe.  
    - Support EU energy and climate policy with transparent, **open-access datasets**.  
    - Enable detailed **scenario analysis** for renewable deployment up to 2050.  
    - Facilitate research on **energy transition pathways** and regional resource availability.  

    ---

    ### Access the Database

    You can access ENSPRESO datasets via:  
    [ENSPRESO Data Portal (JRC Open Data)](https://data.jrc.ec.europa.eu/dataset/jrc-10133-10001)  

    Full technical documentation is available here:  
    [ENSPRESO Documentation (JRC Publications)](https://publications.jrc.ec.europa.eu/repository/handle/JRC109123)

    ---

    ### Citation

    Ruiz, P., Nijs, W., Tarvydas, D., Sgobbi, A., Zucker, A., Pilli, R., Jonsson, K., Camia, A., Thiel, C. (2019).  
    *ENSPRESO – an open, EU-28 wide, transparent and coherent database of wind, solar and biomass energy potentials*.  
    European Commission – Joint Research Centre.  
    DOI: [10.2760/162516](https://doi.org/10.2760/162516)

    """)

# --- ELMAS ---
with st.expander(" **ELMAS** "):
    st.markdown("""
    **ELMAS** (European Load, Market And System dataset) is a comprehensive open database developed by **Mines Paris – PSL**.  
    It provides highly detailed electricity demand and market data for Europe, covering multiple years with high temporal resolution.

    ---

    ### Overview

    ELMAS includes:

    - **Electricity load data** at national and regional levels.  
    - **Market information** (generation, demand, and cross-border flows).  
    - **System operation data** with hourly and sub-hourly granularity.  
    - Harmonised coverage for **EU countries plus selected neighbouring regions**.  

    The dataset is designed to support **energy system modelling, policy analysis, and academic research** on European electricity markets.

    ---

    ### Project Objectives

    - Provide an **open-access, harmonised dataset** for European electricity demand and markets.  
    - Facilitate research on **system integration of renewables** and **market dynamics**.  
    - Support long-term **energy transition modelling** at pan-European scale.  

    ---

    ### Access the Database

    ELMAS is available at:  
    [ELMAS Dataset – Mines Paris PSL](https://zenodo.org/record/6627778)  

    Documentation and related research are available via:  
    [Mines Paris PSL Energy Centre](https://www.centre-energie-minesponts.fr/)  

    ---

    ### Citation

    Gaudard, L., Hadjsaid, N., Bompard, E., et al. (2022).  
    *ELMAS: An open European database for electricity load, market and system*.  
    Mines Paris – PSL.  
    DOI: [10.5281/zenodo.6627778](https://doi.org/10.5281/zenodo.6627778)  
    """)


# --- EMHIRES ---
with st.expander(" **EMHIRES** "):
    st.markdown("""
    **EMHIRES** (European Meteorological High-resolution RES generation) is the **first European wind and solar power generation time series dataset** at high temporal and spatial resolution, developed by the **Joint Research Centre (JRC)**.  

    It provides **hourly time series** of renewable energy generation, essential for assessing variability, integration, and flexibility needs in the European power system.

    ---

    ### Overview

    EMHIRES includes:

    - **Wind power generation time series** (onshore and offshore).  
    - **Solar PV generation time series**.  
    - **Geographically explicit data** covering the entire EU-28.  
    - Historical and synthetic series spanning multiple years.  
    - Data compatible with **energy system and market models**.  

    ---

    ### Project Objectives

    - Provide high-quality, **open-access renewable generation time series** for Europe.  
    - Enable research on **variability and integration of RES** in power systems.  
    - Support EU policy on **grid adequacy, security of supply, and flexibility needs**.  

    ---

    ### Access the Database

    EMHIRES datasets are accessible via the JRC Data Portal:  
    [EMHIRES Dataset – JRC Open Data](https://data.jrc.ec.europa.eu/collection/emhires)  

    Technical documentation:  
    [EMHIRES Report (JRC)](https://publications.jrc.ec.europa.eu/repository/handle/JRC103442)  

    ---

    ### Citation

    Gonzalez Aparicio, I., Zucker, A., Careri, F., Monforti-Ferrario, F., Huld, T., Badger, J. (2016).  
    *EMHIRES dataset: Wind and solar power generation time series at high resolution in the EU*.  
    European Commission – Joint Research Centre.  
    DOI: [10.2790/831549](https://doi.org/10.2790/831549)  
    """)
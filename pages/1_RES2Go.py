import streamlit as st
from tool_modules.eu_mix_preconfiguration import *
from tool_modules.categorisation import *
from tool_modules.pathway_select import *
from tool_modules.pathway_view import *
from tool_modules.pathway_perton import *
from tool_modules.import_export_file import *
from tool_modules.maps import *
from tool_modules.cluster_results import *
from tool_modules.cluster_configuration import *
from tool_modules.emissions import *
from tool_modules.profile_load import *
from tool_modules.supply import *


st.set_page_config(layout="wide")

# Logos
logo = "images/logo_UGent_EN_RGB_2400_color.png"
logo_side = "images/logo_side_bar.png"
st.logo(logo_side, size="large",
        link="https://www.ugent.be/ea/emsme/en/research/research-ensy/energy-systems-clusters/ecm",
        icon_image=logo)

# Initialise session state
if "tool_section_prechoice" not in st.session_state:
    st.session_state["tool_section_prechoice"] = 0

if "tool_subsection_prechoice" not in st.session_state:
    st.session_state["tool_subsection_prechoice"] = 0

if "tool_subsection_prechoice_from_doc" not in st.session_state:
    st.session_state["tool_subsection_prechoice_from_doc"] = 0

tool_section_prechoice = st.session_state.get("tool_section_prechoice", 0)
tool_subsection_prechoice = st.session_state.get(
    "tool_subsection_prechoice", 0)
tool_subsection_prechoice_from_doc = st.session_state.get(
    "tool_subsection_prechoice_from_doc", 0)

# Sidebar tool section
tool_section_choices = ["Pathway",
                        "Maps - European scale", "Cluster - micro scale", "Profile load"]
tool_section_index = min(tool_section_prechoice, len(tool_section_choices) - 1)

tool_section = st.sidebar.radio(
    "",
    tool_section_choices,
    key="tool_section",
    index=tool_section_index
)

# === PATHWAY ===
if tool_section == "Pathway":
    st.session_state["tool_section_prechoice_doc"] = 0

    pathway_choices = [
        "Pathway configuration",
        "Production route consumption",
        "CO2 Emissions",
        "Pathway visualisation"
    ]
    pathway_index = min(tool_subsection_prechoice_from_doc,
                        len(pathway_choices) - 1)

    pathway_subsection = st.sidebar.radio(
        "Select a page",
        pathway_choices,
        index=pathway_index,
        key="pathway_sub"
    )

    if pathway_subsection == "Pathway configuration":
        select_page()
        st.session_state["tool_subsection_prechoice"] = 0

    elif pathway_subsection == "Production route consumption":
        perton_page()
        st.session_state["tool_subsection_prechoice"] = 1

    elif pathway_subsection == "CO2 Emissions":
        st.session_state["tool_subsection_prechoice"] = 2
        emissions_pathway()

    elif pathway_subsection == "Pathway visualisation":
        st.session_state["tool_subsection_prechoice"] = 3
        view_page()

# === MAPS ===
elif tool_section == "Maps - European scale":
    st.session_state["tool_section_prechoice_doc"] = 1

    maps_choices = ["Map per pathway"]
    maps_index = 0  # Only one item, safe default

    maps_subsection = st.sidebar.radio(
        "Select a page",
        maps_choices,
        index=maps_index,
        key="maps_sub"
    )

    if maps_subsection == "Map per pathway":
        map_per_pathway()

# === CLUSTER ===
elif tool_section == "Cluster - micro scale":
    st.session_state["tool_section_prechoice_doc"] = 2

    cluster_choices = ["Cluster configuration", "Cluster results"]
    cluster_index = min(tool_subsection_prechoice_from_doc, len(cluster_choices) - 1)

    cluster_subsection = st.sidebar.radio(
        "Select a page",
        cluster_choices,
        index=cluster_index,
        key="cluster_sub"
    )

    if cluster_subsection == "Cluster configuration":
        st.session_state["tool_subsection_prechoice"] = 0
        cluster_configuration()

    elif cluster_subsection == "Cluster results":
        st.session_state["tool_subsection_prechoice"] = 1
        cluster_results()

# === PROFILE LOAD ===
elif tool_section == "Profile load":
    st.session_state["tool_section_prechoice_doc"] = 3
    profile_load()

# === Low Carbon supply ===
elif tool_section == "Low Carbon supply":
    st.session_state["tool_section_prechoice_doc"] = 4
    supply()

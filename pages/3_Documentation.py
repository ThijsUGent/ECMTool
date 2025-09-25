import streamlit as st
from doc.doc_pathway import *
from doc.doc_cluster import *
from doc.doc_maps import *

st.set_page_config(layout="wide")
logo = "images/logo_UGent_EN_RGB_2400_color.png"
logo_side = "images/logo_side_bar.png"
st.logo(logo_side, size="large",
        link="https://www.ugent.be/ea/emsme/en/research/research-ensy/energy-systems-clusters/ecm", icon_image=logo)

st.title("Documentation")

tab_options = [
    "Pathway",
    "Maps - European scale",
    "Cluster - micro scale",
    "Glossary"
]
if "tool_section_prechoice_doc" not in st.session_state:
    st.session_state["tool_section_prechoice_doc"] = 0


tool_section_prechoice_doc = st.session_state["tool_section_prechoice_doc"]

tabs_choice = st.segmented_control(
    "Select section", tab_options, default=tab_options[tool_section_prechoice_doc])

if tabs_choice == "Pathway":
    st.session_state["tool_section_prechoice"] = 0
    st.markdown("## 📘 Pathway")
    st.page_link("pages/1_RES2Go.py",
                 label="Go to Pathway section →", icon="🧭")
    st.markdown("""This tool uses the AIDRES database to extract the energy need for each product through configurable pathways. Users can create and save multiple pathways that represent different scenarios, serving as the basis for subsequent analyses.""")
    doc_pathway()

elif tabs_choice == "Maps - European scale":
    st.session_state["tool_section_prechoice"] = 1
    st.markdown("## 🗺️ Maps - European scale")
    st.page_link("pages/1_RES2Go.py", label="Go to Maps section →", icon="🧭")
    st.markdown("""The spatial data from AIDRES is visualised at the European scale to assess and compare the energy need across different regions. Sites are clustered based on geographical proximity and sector characteristics, helping to identify regional potentials for decarbonisation and clustering.""")
    doc_maps()
elif tabs_choice == "Cluster - micro scale":
    st.session_state["tool_section_prechoice"] = 2
    st.markdown("## 🔬 Cluster - micro scale")
    st.page_link("pages/1_RES2Go.py",
                 label="Go to Cluster section →", icon="🧭")
    st.markdown("""The micro scale section allows full customisation of clusters. Users can define the annual production of multiple products within a cluster, download existing cluster configurations, or upload their own. The results can be compared visually using treemaps and sankey diagrams, supporting deeper insights into energy flows and interconnections.""")
    doc_cluster()
elif tabs_choice == "Glossary":
    st.markdown("## 📚 Glossary")
    st.markdown("""
    The glossary provides definitions and explanations of key terms and concepts used throughout the tool.  
    It ensures users have a clear understanding of the terminology related to energy systems, industrial symbiosis, and the AIDRES database.
    """)

    st.markdown("""
    **Production route**  
    An industrial process used to produce a specific product.

    **Sector**  
    An industrial sector within the AIDRES scope, defined using NACE codes (cement, steel, chemicals, fertilisers, glass and refiniries)

    **Pathway**  
    A combination of one or more production routes, covering single or multiple sectors and products, aggregated to represent specific technology scenarios.

    **Cluster**  
    A group of industrial sites located in close spatial proximity. In the *cluster micro scale* section, a cluster may be considered an isolated system and may follow its own  pathway.
    """)

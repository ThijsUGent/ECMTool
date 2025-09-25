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
from doc.doc_pathway import *

st.set_page_config(layout="wide", initial_sidebar_state="expanded")
logo = "images/logo_UGent_EN_RGB_2400_color.png"
logo_side = "images/logo_side_bar.png"
st.logo(logo_side, size="large",
        link="https://www.ugent.be/ea/emsme/en/research/research-ensy/energy-systems-clusters/ecm", icon_image=logo)


st.subheader("UGent ECM RES2Go")
st.markdown("""*Assessing and addressing the future of renewable energy in industrial processes and clusters*""")
st.markdown("""
    The RES2Go tool starts from the [AIDRES project](https://op.europa.eu/en/publication-detail/-/publication/577d820d-5115-11ee-9220-01aa75ed71a1/language-en), creating a variety of energy demand projections for industrial sites and clusters with a high level of product, process and location-based flexibilities and functionalities. Users can forecast energy pathways by modifying products and processes, creating clusters at micro to macro level, and assess future demand of multiple energy sources.   """)

st.markdown("""
    :arrow_down: Start to check tutorial in documentation section and use the tool :arrow_down:
    """)
st.page_link("pages/3_Documentation.py",
             label="Documentation", icon="📖", use_container_width=True)
st.page_link("pages/1_RES2Go.py", label=" **RES2Go** ",
             icon="⚙️", use_container_width=True)
st.page_link("pages/8_About.py", label="About",
             icon="ℹ️", use_container_width=True)

st.markdown("""
    *Contact us:*
    """)
st.page_link("pages/7_Contact.py", label="Contact",
             icon="✉️", use_container_width=True)

# st.video


import streamlit as st

st.set_page_config(layout="wide")
logo = "images/logo_UGent_EN_RGB_2400_color.png"
logo_side = "images/logo_side_bar.png"
st.logo(logo_side, size="large",
        link="https://www.ugent.be/ea/emsme/en/research/research-ensy/energy-systems-clusters/ecm", icon_image=logo)

st.header(
    "[ECM](https://www.ugent.be/ea/emsme/en/research/research-ensy/energy-systems-clusters/ecm)")
st.markdown("""The Energy & Cluster Management group is matured from the synergy between environment and entrepreneurship, between study and implementation of industrial symbiosis, performed since 1998. Research topics focus on concerted energy & resource management at business parks, on industrial sites or in cross-sectorial clusters. It concerns characteristic interdisciplinary research, combining engineering & modelling skills with economic analyses, spatial planning, policy settings and social responsibility.

The main line of research is on advancing the ECM LESTS (Legal, Economic, Spatial, Technical and Social) survey for assessing park management intensity into a toolbox for driving energy symbiosis and circular economy principles in and across process industries. Typical ECM projects focus on energy efficiency, resource optimisation, site integration, waste(heat) recovery, regional clustering, etc.""")
st.subheader(
    "[PIECE project](https://www.ugent.be/ea/emsme/en/research/research-ensy/energy-systems-clusters/ecm/piece)")

st.markdown("""PIECE is a direct ECM collaboration with EU DG ENER on the potential for energy clustering and flexibility to support the low-carbon transition in industry, framed by a master’s thesis project in an Erasmus exchange with the University of Nantes in France. In a first step the low-carbon energy potential across Europe is evaluated using public databases such as ENSPRESO and ExtremOS, as well as the industrial demand based on AIDRES scenarios and input from the project's industry advisory board. Both supply and demand are integrated in a cluster-based approach, which serves as basis to map the potential in Europe and target the most relevant regions through case studies. The selected industrial hubs are assessed for challenges and opportunities to adopt low-carbon energy in a competitive cluster environment. The project results in support for industrial stakeholders and policymakers to advance towards a net-zero European industrial tissue.
            
The main objective of this master’s thesis project is to investigate how to reduce emissions in energy-intensive industry by clustering energy demand and providing low-carbon energy solutions while advancing industrial competitiveness in Europe.""")

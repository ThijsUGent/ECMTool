import streamlit as st
st.set_page_config(layout="wide")
logo = "images/logo_UGent_EN_RGB_2400_color.png"
logo_side = "images/logo_side_bar.png"
st.logo(logo_side, size="large",
        link="https://www.ugent.be/ea/emsme/en/research/research-ensy/energy-systems-clusters/ecm", icon_image=logo)
st.title("Acknowledgements")

st.markdown("""
We express our sincere gratitude to all who made the PIECE project possible.

**Special thanks to:**

- The Directorate-General for Energy **(DG ENER)** for coordinating and managing the AIDRES project.  
- **DG REGIO** and the **Industrial Board** for their valuable and constructive feedback throughout the process.  
- **Nantes Université**, Ghent University (**UGent**), and the **Erasmus+** programme for supporting the internship of **Thijs Duvillard** that contributed to this work.  
- Supervisors: ir Nienke Dhondt and prof. Greet Van Eetvelde

**Ghent, July 2025**
""")

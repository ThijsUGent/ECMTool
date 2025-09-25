import streamlit as st

# Page configuration
st.set_page_config(layout="wide")

# Logos
logo = "images/logo_UGent_EN_RGB_2400_color.png"
logo_side = "images/logo_side_bar.png"
st.logo(
    logo_side, 
    size="large",
    link="https://www.ugent.be/ea/emsme/en/research/research-ensy/energy-systems-clusters/ecm", 
    icon_image=logo
)

# About / Information
st.markdown(
    """
## About

This tool results from the **PIECE** project, developed by the **ECM research group** at [Energy and Cluster Management (ECM) — Department of ElectroMechanical, Systems and Metal Engineering — Ghent University](https://www.ugent.be/ea/emsme/en/research/ensy/energy-systems-clusters/ecm).

---

### Beta version  

**RES2Go beta 2.0** – September 2025  

[RES2Go beta](https://ugent-ecm-res2gobeta.streamlit.app/ECM)

---

### License  

**RES2Go – AGPLv3 License**

Copyright (c) 2025 Ghent University, ECM Research Group

This program is free software: you can redistribute it and/or modify
it under the terms of the GNU Affero General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

This program is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
GNU Affero General Public License for more details.

See <https://www.gnu.org/licenses/>.

The source code remains the property of the ECM Research Group at Ghent University 
and is publicly available under the terms of the AGPLv3 license.

For inquiries, please contact [ecm@ugent.be](mailto:ecm@ugent.be)

---

### Citation  

T. Duvillard, N. Dhont, G. Van Eetvelde\*, *RES2Go tool*, University of Ghent, 2025.  

\* Contact: [ecm@ugent.be](mailto:ecm@ugent.be)
"""
)
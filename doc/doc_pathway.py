import streamlit as st


def doc_pathway():

    if "tool_subsection_prechoice" not in st.session_state:
        st.session_state["tool_subsection_prechoice"] = 0

    tool_subsection_prechoice = st.session_state.get(
        "tool_subsection_prechoice", 0)

    subsection = st.radio("Subsection:", [
        "Pathway configuration",
        "Production route consumption",
        "CO2 emissions",
        "Pathway visualisation",
    ], index=tool_subsection_prechoice, horizontal=True)

    if subsection == "Pathway configuration":
        with st.expander("Pre-made Pathways"):
            st.video("video/pathway/config_premade_540p.mp4",
                     subtitles="video/pathway/config_premade.vtt")
            if st.button("Go to Pre-made pathway →"):
                st.session_state["tool_subsection_prechoice"] = 0
                # Use inside pathway configuration to select good mode
                st.session_state["pathway_configuration_prechoice"] = 0
                st.switch_page("pages/1_Tool.py")

        with st.expander("Create a Pathway"):
            st.video("video/pathway/config_create_540p.mp4",
                     subtitles="video/pathway/config_create.vtt", end_time="1m47s")
            if st.button("Go to Create a pathway →"):
                st.session_state["tool_subsection_prechoice"] = 0
                st.session_state["pathway_configuration_prechoice"] = 2
                st.switch_page("pages/1_Tool.py")

        with st.expander("Upload a Pathway"):
            st.video("video/pathway/config_upload_540p.mp4",
                     subtitles="video/pathway/config_upload.vtt")
            if st.button("Go to Upload a pathway →"):
                st.session_state["tool_subsection_prechoice"] = 0
                st.session_state["pathway_configuration_prechoice"] = 1
                st.switch_page("pages/1_Tool.py")

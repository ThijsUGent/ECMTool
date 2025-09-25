import streamlit as st


def doc_cluster():
    if "tool_section_prechoice" not in st.session_state:
        st.session_state["tool_section_prechoice"] = 0

    if "tool_subsection_prechoice_from_doc" not in st.session_state:
        st.session_state["tool_subsection_prechoice_from_doc"] = 0

    tool_section_prechoice = st.session_state.get(
        "tool_section_prechoice", 0)
    tool_subsection_prechoice = st.session_state.get(
        "tool_subsection_prechoice", 0)

    subsection = st.radio("Subsection:", [
        "Cluster configuration",
        "Cluster results",
    ], horizontal=True, key="radio_cluster_subsection", index=tool_subsection_prechoice)

    if subsection == "Cluster configuration":
        with st.expander("Create a cluster", expanded=False):
            st.video("video/cluster/cluster_create_540p.mp4",
                     subtitles="video/cluster/cluster_create.vtt")
            st.markdown(" ")
            if st.button("Go to Pre-made pathway →", key="btn_cluster_create_goto"):
                st.session_state["tool_section_prechoice"] = 0
                st.session_state["tool_section_prechoice"] = 2
                st.session_state["tool_subsection_prechoice_from_doc"] = 0
                # Inside cluster configuration page
                st.session_state["cluster_configuration_prechoice"] = 0
                st.switch_page("pages/1_Tool.py")

        with st.expander("Upload a cluster"):
            st.video("video/cluster/cluster_upload_540p.mp4", end_time="1m47s")
            st.markdown(" ")
            if st.button("Go to Create a pathway →", key="btn_cluster_upload_goto"):
                st.session_state["tool_section_prechoice"] = 0
                st.session_state["tool_section_prechoice"] = 2
                st.session_state["tool_subsection_prechoice_from_doc"] = 0
                st.session_state["cluster_configuration_prechoice"] = 1
                st.switch_page("pages/1_Tool.py")

    if subsection == "Cluster results":
        with st.expander("Visualise results"):
            st.video("video/cluster/cluster_view_540p.mp4"
                     )
            st.markdown(" ")
            if st.button("Go to Pre-made pathway →", key="btn_cluster_results_goto"):
                st.session_state["tool_section_prechoice"] = 0
                st.session_state["tool_section_prechoice"] = 2
                st.session_state["tool_subsection_prechoice_from_doc"] = 1
                st.switch_page("pages/1_Tool.py")

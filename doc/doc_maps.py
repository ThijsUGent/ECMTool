import streamlit as st


def doc_maps():
    if "tool_subsection_prechoice" not in st.session_state:
        st.session_state["tool_subsection_prechoice"] = 0

    tool_subsection_prechoice = st.session_state.get(
        "tool_subsection_prechoice", 0)

    tool_subsection_prechoice = st.session_state.get(
        "tool_subsection_prechoice", 0)

    choices = ["Mapping", "Clustering"]
    safe_index = min(tool_subsection_prechoice, len(choices) - 1)

    subsection = st.radio(
        "Subsection:",
        choices,
        index=safe_index,
        horizontal=True,
        key="radio_subsection_maps"
    )

    if subsection == "Mapping":

        with st.expander("Site view", expanded=False):
            st.video("video/maps/maps_mapping_views_540p.mp4")
            st.markdown(" ")
            if st.button("Go to Mapping →", key="btn_mapping_site_view"):
                st.session_state["tool_subsection_prechoice_from_doc"] = 1
                st.session_state["pathway_configuration_prechoice"] = 0
                st.switch_page("pages/1_Tool.py")

        with st.expander("Utilisation rate editing"):
            st.video("video/maps/maps_mapping_utilisation_540p.mp4")
            st.markdown(" ")
            if st.button("Go to Mapping →", key="btn_mapping_utilisation"):
                st.session_state["tool_subsection_prechoice_from_doc"] = 1
                st.session_state["pathway_configuration_prechoice"] = 0
                st.switch_page("pages/1_Tool.py")

        with st.expander("Energy carrier & sector selection"):
            st.video("video/maps/maps_mapping_sectors_540p.mp4")
            st.markdown(" ")
            if st.button("Go to Mapping →", key="btn_mapping_sector_selection"):
                st.session_state["tool_subsection_prechoice_from_doc"] = 1
                st.session_state["pathway_configuration_prechoice"] = 0
                st.switch_page("pages/1_Tool.py")

        with st.expander("Cluster export"):
            st.video("video/maps/maps_mapping_clusterexport_540p.mp4")
            st.markdown(" ")
            if st.button("Go to Mapping →", key="btn_mapping_cluster_export"):
                st.session_state["tool_subsection_prechoice_from_doc"] = 1
                st.session_state["pathway_configuration_prechoice"] = 0
                st.switch_page("pages/1_Tool.py")

    if subsection == "Clustering":

        with st.expander("Change cluster algorithm"):
            st.video("video/maps/maps_clustering_change_540p.mp4")
            st.markdown(" ")
            if st.button("Go to Mapping →", key="btn_clustering_algorithm"):
                st.session_state["tool_subsection_prechoice_from_doc"] = 1
                st.session_state["pathway_configuration_prechoice"] = 0
                st.switch_page("pages/1_Tool.py")

        with st.expander("DBSCAN parameters"):
            st.markdown(" ")
            if st.button("Go to Mapping →", key="btn_clustering_dbscan"):
                st.session_state["tool_subsection_prechoice_from_doc"] = 1
                st.session_state["pathway_configuration_prechoice"] = 0
                st.switch_page("pages/1_Tool.py")

        with st.expander("KMEANS parameters"):
            st.markdown(" ")
            if st.button("Go to Mapping →", key="btn_clustering_kmeans"):
                st.session_state["tool_subsection_prechoice_from_doc"] = 1
                st.session_state["pathway_configuration_prechoice"] = 0
                st.switch_page("pages/1_Tool.py")

        with st.expander("KMEANS weighted parameters"):
            st.markdown(" ")
            if st.button("Go to Mapping →", key="btn_clustering_kmeans_weighted"):
                st.session_state["tool_subsection_prechoice_from_doc"] = 1
                st.session_state["pathway_configuration_prechoice"] = 0
                st.switch_page("pages/1_Tool.py")

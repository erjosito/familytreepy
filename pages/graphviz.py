import streamlit as st
from familytree import FamilyTree
import graphviz

def show_graphviz(user_role='user'):
    # Verify authentication
    st.header(f"Welcome, {st.user.name}!")

    # Page content
    # st.write("This page shows graph visualizations using the [pyvis library](https://pyvis.readthedocs.io/en/latest/).")

    # Initialize azure storage credentials from secrets.toml file
    azure_storage_account = st.secrets['storage']["azure_storage_account"]
    azure_storage_container = st.secrets['storage']["azure_storage_container"]
    azure_storage_key = st.secrets['storage']["azure_storage_key"]
    azure_storage_sas = st.secrets['storage']["azure_storage_sas"]

    # Initialize the family tree with Azure storage as backend
    tree = FamilyTree(backend='azstorage', azstorage_account=azure_storage_account, azstorage_key=azure_storage_key, azstorage_container="familytreejson", azstorage_blob='familytree.gml')
    # Assign generation levels for a hierarchical layout
    tree.assign_generation_levels()

    # Graph filter
    left, center, right = st.columns(3)
    person_list = tree.get_person_list()
    person_list.sort()
    # st.write(len(person_list), "people found in the family tree.")    # DEBUG
    selected_node_id = None
    with left:
        selected_person = st.selectbox("Select a person to center the graph on (optional):", options=[""] + person_list)
        if selected_person:
            selected_node_id = tree.get_person_by_full_name(selected_person)
            # if selected_node_id:
            #     st.write(f"Centering graph on: {selected_person} (ID: {selected_node_id})")
            # else:
            #     st.write(f"Person '{selected_person}' not found in the tree.")
    with right:
        degree = st.slider("Select graph degree (number of relationship hops from center person):", min_value=1, max_value=5, value=2, step=1)
    # Create subgraph
    subgraph = None
    if selected_person and selected_node_id:
        subgraph = tree.get_subgraph_degrees(selected_node_id, degree=degree)
        if subgraph.number_of_nodes() == 0:
            st.warning(f"No relationships found within {degree} degrees of '{selected_person}'. Showing full graph instead.")
            subgraph = tree.graph
    # Draw graph using graphviz
    if subgraph:
        source_graph = subgraph
    else:
        source_graph = tree.graph

    # Convert networkx graph to a graphviz object
    dot = graphviz.Digraph(comment='Family Tree', format='dot')
    dot.attr(rankdir='TB')  # Top to Bottom layout
    # Add nodes and edges to the graph
    for person in source_graph.nodes(data=True):
        full_name = f"{person[1].get('firstname')} {person[1].get('lastname')}"
        full_name = full_name.strip().replace(' ', '\n')  # Replace blanks with newlines for better display
        dot.node(str(person[0]), label=full_name)
        if 'profilepic' in person[1] and person[1]['profilepic']:
            image_url = person[1]['profilepic'] + "?" + azure_storage_sas
            # See:
            # - https://graphviz.org/docs/attrs/image/
            # - https://gitlab.com/graphviz/graphviz/-/blob/main/contrib/dot_url_resolve.py
            # dot.node(str(person[0]), label=full_name, image=image_url, shape='circle', labelloc='b', imagescale='true', width='0.5', height='0.5')
            dot.node(str(person[0]), label=full_name, image=image_url)
    for edge in source_graph.edges(data=True):
        dot.edge(str(edge[0]), str(edge[1]), label=edge[2]['type'])

    # Render the graph
    st.graphviz_chart(dot)

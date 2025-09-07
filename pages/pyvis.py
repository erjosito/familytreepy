import tempfile
import os
import streamlit as st
import streamlit.components.v1 as components
from pyvis import network as net
from familytree import FamilyTree
import matplotlib.pyplot as plt


def show_pyvis(user_role='user'):
    # Verify authentication
    st.header(f"Welcome, {st.user.name}!")

    # Page content
    st.write("This page shows graph visualizations using the [pyvis library](https://pyvis.readthedocs.io/en/latest/).")

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

    subgraph = None
    if selected_person and selected_node_id:
        subgraph = tree.get_subgraph_degrees(selected_node_id, degree=degree)
        if subgraph.number_of_nodes() == 0:
            st.warning(f"No relationships found within {degree} degrees of '{selected_person}'. Showing full graph instead.")
            subgraph = tree.graph

    # Create a pyvis network, either with the whole graph or the subgraph (filtered)
    # Additional possible options: notebook=True, cdn_resources='in_line', heading='Family Tree'
    tree_pyvis = net.Network(height='465px', bgcolor='#222222', font_color='white')
    if subgraph:
        tree_pyvis.from_nx(subgraph)
    else:
        tree_pyvis.from_nx(tree.graph)

    # Set hierarchical layout options
    # UD = Up-Down, can also be LR (Left-Right)
    # sortMethod: hubsize, directed
    tree_pyvis.set_options('''
    var options = {
        "layout": {
            "hierarchical": {
                "enabled": true,
                "levelSeparation": 150,
                "nodeSpacing": 200,
                "treeSpacing": 300,
                "direction": "UD",
                "sortMethod": "directed"
            }
        }
    }''')

    # Customize node titles and images
    for node in tree_pyvis.nodes:
        if 'firstname' in node and 'lastname' in node:
            node['label'] = node['firstname'] + ' ' + node['lastname']
        elif 'firstname' in node:
            node['label'] = node['firstname']
        elif 'lastname' in node:
            node['label'] = node['lastname']
        else:
            node['label'] = '?'
        # Replace the blanks with line breaks
        node['label'] = node['label'].replace(' ', '\n')
        if selected_node_id and node['id'] == selected_node_id:
            node['color'] = 'red'
        else:
            node['color'] = 'gray'
        # if 'level' in node:
        #     node['label'] = f"{node['label']} ({node['level']})"
        if 'profilepic' in node:
            node['shape'] = 'circularImage'
            node['image'] = node['profilepic'] + "?" + azure_storage_sas
            node['size'] = 25
    for edge in tree_pyvis.edges:
        if edge['type'] == 'isChildOf':
            edge['color'] = 'white'
        else:
                edge['color'] = 'yellow'

    # Additional options
    # tree_pyvis.show_buttons(filter_=['physics'])    # Doesnt seem to do anything, and it conflicts with hierarchical layout

    # Display the pyvis graph using stvis
    try:
        temp_folder = tempfile.TemporaryDirectory()
        temp_filename = 'pyvis_graph.html'
        temp_path = os.path.join(temp_folder.name, temp_filename)
        tree_pyvis.save_graph(temp_path)
        HtmlFile = open(temp_path,'r',encoding='utf-8')
    except Exception as e:
        st.error(f"Error creating temporary file: {e}")
        return
    components.html(HtmlFile.read(), height=485)


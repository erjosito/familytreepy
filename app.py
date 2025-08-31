import streamlit as st
import uuid
import datetime
from st_link_analysis import st_link_analysis, NodeStyle, EdgeStyle
from familytree import FamilyTree

# Initialize the family tree
tree = FamilyTree(backend='local', localfile='C:\\Users\\jomore\\Downloads\\familytree.gml')
tree_st = tree.format_for_st_link_analysis()

# Required for graph interaction
COMPONENT_KEY = "NODE_ACTIONS"
selected_nodes = []

def onchange_callback():
    val = st.session_state[COMPONENT_KEY]
    # if val["action"] == "remove":
    #     st.write("Ask to remove nodes:")
    #     st.write(val["data"]["node_ids"])
    # elif val["action"] == "expand":
    #     st.write("Ask to expand nodes:")
    #     st.write(val["data"]["node_ids"])
    selected_nodes = val["data"]["node_ids"]

# Asks for login if no user is logged in
if not st.user.is_logged_in:
    st.header("This app is private, please log in")
    if st.button("Log in with a Microsoft account"):
        st.login()
    st.stop()

if st.button("Log out"):
    st.logout()
st.header(f"Welcome, {st.user.name}!")

if not hasattr(st.session_state, "graph"):
    st.session_state.graph = tree_st
node_styles = [
    NodeStyle("person", "#FF7F3E", "name", "person"),
]
edge_styles = [
    EdgeStyle("isChildOf", caption='label', directed=True),
    EdgeStyle("isSpouseOf", caption='label', directed=True)
]
layout = {"name": "cose", "animate": "end", "nodeDimensionsIncludeLabels": False}
elements = st.session_state.graph
with st.container(border=True):
    vals = st_link_analysis(elements, node_styles=node_styles, edge_styles=edge_styles, layout=layout, key=COMPONENT_KEY, node_actions=['expand'], on_change=onchange_callback)
    if vals:
        selected_node_id = vals['data']['node_ids'][0]
        selected_node = tree.get_person(selected_node_id)
        st.write("Selected ID: " + str(selected_node_id))
        col1, col2, col3, col4, col5 = st.columns(5)
        if col1.button("Add child"):
            st.write("Adding child...")
            # Add child logic here
        if col2.button("Add spouse"):
            st.write("Adding spouse...")
            # Add spouse logic here
        if col3.button("Add parent"):
            st.write("Adding parent...")
            # Add parent logic here
        editpersoncontainer = col4.empty()
        with editpersoncontainer.container():
            with st.popover(label="Edit"):
                st.write("Editing " + selected_node_id)
                if "firstname" in selected_node:
                    default_firstname = selected_node['firstname']
                else:
                    default_firstname = ""
                if "lastname" in selected_node:
                    default_lastname = selected_node['lastname']
                else:
                    default_lastname = ""
                if "birthdate" in selected_node:
                    default_birthdate = datetime.date(selected_node['birthdate'])
                else:
                    default_birthdate = datetime.date.today()
                if "birthplace" in selected_node:
                    default_birthplace = selected_node['birthplace']
                else:
                    default_birthplace = ""
                if "isAlive" in selected_node:
                    default_isAlive = selected_node['isAlive']
                else:
                    default_isAlive = True
                if "deathdate" in selected_node:
                    default_deathdate = datetime.date(selected_node['deathdate'])
                else:
                    default_deathdate = datetime.date.today()
                firstname = st.text_input("First name", value=default_firstname)
                lastname = st.text_input("Last name", value=default_lastname)
                birthdate = st.date_input("Select a date", value=default_birthdate, key="edit_birthdate")
                birthplace = st.text_input("Birthplace", value=default_birthplace)
                isAlive = st.checkbox("Is alive", value=default_isAlive, key="edit_isalive")
                if not isAlive:
                    deathdate = st.date_input("Select a date", value=default_deathdate, key="edit_deathdate")
                col1, col2 = st.columns(2)
                if col1.button("Cancel", key="edit_cancel"):
                    editpersoncontainer.empty()
                    st.rerun()
                if col2.button("OK", key="edit_ok"):
                    if isAlive:
                        deathdate = None
                    if firstname != default_firstname:
                        tree.update_person(selected_node_id, firstname=firstname)
                    if lastname != default_lastname:
                        tree.update_person(selected_node_id, lastname=lastname)
                    if birthdate != default_birthdate:
                        tree.update_person(selected_node_id, birthdate=str(birthdate))
                    if birthplace != default_birthplace:
                        tree.update_person(selected_node_id, birthplace=birthplace)
                    if (not isAlive) and (deathdate != default_deathdate):
                        tree.update_person(selected_node_id, deathdate=str(deathdate))
                    editpersoncontainer.empty()
                    tree_st = tree.format_for_st_link_analysis()
                    st.rerun()
        if col5.button("Remove"):
            st.write("Removing person...")
            # Remove person logic here
    else:
        st.write("Please double click on a node to see its details.")

# st_link_analysis(tree, node_styles=node_styles, edge_styles=edge_styles, layout=layout, key="xyz")

# DEBUG: tree object
st.write("Loaded tree:")
st.write(tree_st)

######################
# Tree functionality #
######################

left, middle, right = st.columns(3)
# Add test data with confirmation (using state would probably be cleaner)
addtest_container = left.empty()
with addtest_container.container():
    with st.popover(label="Add test data"):
        st.write("Do you really want to delete all existing data and replace it with test data?")
        col1, col2 = st.columns(2)
        if col1.button("Cancel", key="add_test_cancel"):
            addtest_container.empty()
            st.rerun()
        if col2.button("Yes, add data", key="add_test_ok"):
            tree.add_test_family()
            addtest_container.empty()
            st.rerun()
# Delete all button with confirmation (using state would probably be cleaner)
deleteall_container = middle.empty()
with deleteall_container.container():
    with st.popover(label="Delete all"):
        st.write("Do you really want to delete all nodes?")
        col1, col2 = st.columns(2)
        if col1.button("Cancel", key="delete_cancel"):
            deleteall_container.empty()
            st.rerun()
        if col2.button("Yes, delete all", key="delete_ok"):
            tree.delete_all()
            deleteall_container.empty()
            st.rerun()

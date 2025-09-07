import streamlit as st
import uuid
import os
from pathlib import Path
import datetime
from st_link_analysis import st_link_analysis, NodeStyle, EdgeStyle
from st_link_analysis.component.layouts import LAYOUTS
from familytree import FamilyTree
from azure.storage.blob import BlobServiceClient

# Global constants
COMPONENT_KEY = "NODE_ACTIONS"

# Function to upload file to Azure Storage, used to upload images
def upload_to_azure_storage(file, account_name=None, container_name=None, account_key=None, overwrite=True):
    file_extension = os.path.splitext(file.name)[1]
    blob_name = str(uuid.uuid4()) + file_extension
    blob_service_client = BlobServiceClient.from_connection_string(f"DefaultEndpointsProtocol=https;AccountName={account_name};AccountKey={account_key}")
    blob_client = blob_service_client.get_blob_client(container=container_name, blob=blob_name)
    blob_client.upload_blob(file, overwrite=overwrite)
    blob_url = f"https://{account_name}.blob.core.windows.net/{container_name}/{blob_name}"
    return blob_url

# Updates the object used by the graph representation
def refresh_tree(tree):
    tree_st = tree.format_for_st_link_analysis()
    st.session_state.graph = tree_st
    elements = st.session_state.graph

# Updates the selected nodes when a user double-clicks on a person
def onchange_callback():
    val = st.session_state[COMPONENT_KEY]
    # if val["action"] == "remove":
    #     st.write("Ask to remove nodes:")
    #     st.write(val["data"]["node_ids"])
    # elif val["action"] == "expand":
    #     st.write("Ask to expand nodes:")
    #     st.write(val["data"]["node_ids"])
    selected_nodes = val["data"]["node_ids"]

def show_st_link_analysis(user_role='user'):
    # Verify authentication
    st.header(f"Welcome, {st.user.name}!")

    # Description
    st.write("This page uses the StreamLit module [streamlit-link-analysis](https://github.com/ikko/streamlit-link-analysis) for visualizing the graph. This module allows to select nodes/links in the graph, so this page contains logic to update the graph and to show the pictures of an individual node.")

    # Initialize azure storage credentials from secrets.toml file
    azure_storage_account = st.secrets['storage']["azure_storage_account"]
    azure_storage_container = st.secrets['storage']["azure_storage_container"]
    azure_storage_key = st.secrets['storage']["azure_storage_key"]
    azure_storage_sas = st.secrets['storage']["azure_storage_sas"]

    # Initialize the family tree
    # tree = FamilyTree(backend='local', localfile='C:\\Users\\jomore\\Downloads\\familytree.gml')
    tree = FamilyTree(backend='azstorage', azstorage_account=azure_storage_account, azstorage_key=azure_storage_key, azstorage_container="familytreejson", azstorage_blob='familytree.gml')

    # Generate a Streamlit-compatible representation of the graph
    tree_st = tree.format_for_st_link_analysis()

    # Required for graph interaction
    LAYOUT_NAMES = list(LAYOUTS.keys())
    selected_nodes = []

    if not hasattr(st.session_state, "graph"):
        st.session_state.graph = tree_st
    node_styles = [
        NodeStyle("person", "#FF7F3E", "name", "person"),
    ]
    edge_styles = [
        EdgeStyle("isChildOf", caption='label', directed=True),
        EdgeStyle("isSpouseOf", caption='label', directed=True)
    ]

    layout = st.selectbox("Choose the graph layout for the st-link-analysis representation:", LAYOUT_NAMES, index=0)
    # layout = {"name": "cose", "animate": "end", "nodeDimensionsIncludeLabels": False}

    elements = st.session_state.graph
    with st.container(border=True):
        vals = st_link_analysis(elements, node_styles=node_styles, edge_styles=edge_styles, layout=layout, key=COMPONENT_KEY, node_actions=['expand'], on_change=onchange_callback)
        if vals:
            selected_node_id = vals['data']['node_ids'][0]
            selected_node = tree.get_person(selected_node_id)
            st.write("Selected ID: " + str(selected_node_id))
            col1, col2, col3, col4, col5 = st.columns(5)
            #################
            #   Add child   #
            #################
            addchildcontainer = col1.empty()
            with addchildcontainer.container():
                with st.popover(label="Add child"):
                    st.write("Adding child to " + selected_node_id)
                    default_firstname = ""
                    default_lastname = ""
                    default_birthdate = None
                    default_birthplace = ""
                    default_isAlive = True
                    default_deathdate = None
                    firstname = st.text_input("First name", value=default_firstname, key='addchild_firstname')
                    lastname = st.text_input("Last name", value=default_lastname, key='addchild_lastname')
                    birthdate = st.date_input("Select a date", value=default_birthdate, min_value='1700-01-01', key="addchild_birthdate")
                    birthplace = st.text_input("Birthplace", value=default_birthplace, key='addchild_birthplace')
                    isAlive = st.checkbox("Is alive", value=default_isAlive, key="addchild_isalive")
                    if not isAlive:
                        deathdate = st.date_input("Select a date", value=default_deathdate, min_value='1700-01-01', key="addchild_deathdate")
                    col11, col12 = st.columns(2)
                    if col11.button("Cancel", key="addchild_cancel"):
                        addchildcontainer.empty()
                        st.rerun()
                    if col12.button("OK", key="addchild_ok"):
                        if isAlive:
                            deathdate = None
                        # Attributes initialized with the isAlive property, everything else is optional
                        attributes = {
                            'isAlive': isAlive
                        }
                        if firstname != default_firstname:
                            attributes['firstname'] = firstname
                        if lastname != default_lastname:
                            attributes['lastname'] = lastname
                        if birthdate != default_birthdate:
                            attributes['birthdate'] = str(birthdate)
                        if birthplace != default_birthplace:
                            attributes['birthplace'] = birthplace
                        if (not isAlive) and (deathdate != default_deathdate):
                            attributes['deathdate'] = str(deathdate)
                        tree.add_child(selected_node_id, **attributes)
                        addchildcontainer.empty()
                        refresh_tree(tree)
                        st.rerun()
            ##################
            #   Add spouse   #
            ##################
            addspousecontainer = col2.empty()
            with addspousecontainer.container():
                with st.popover(label="Add spouse"):
                    st.write("Adding spouse to " + selected_node_id)
                    default_firstname = ""
                    default_lastname = ""
                    default_birthdate = None
                    default_birthplace = ""
                    default_isAlive = True
                    default_deathdate = None
                    firstname = st.text_input("First name", value=default_firstname, key='addspouse_firstname')
                    lastname = st.text_input("Last name", value=default_lastname, key='addspouse_lastname')
                    birthdate = st.date_input("Select a date", value=default_birthdate, min_value='1700-01-01', key="addspouse_birthdate")
                    birthplace = st.text_input("Birthplace", value=default_birthplace, key='addspouse_birthplace')
                    isAlive = st.checkbox("Is alive", value=default_isAlive, key="addspouse_isalive")
                    if not isAlive:
                        deathdate = st.date_input("Select a date", value=default_deathdate, min_value='1700-01-01', key="addspouse_deathdate")
                    col21, col22 = st.columns(2)
                    if col21.button("Cancel", key="addspouse_cancel"):
                        addspousecontainer.empty()
                        st.rerun()
                    if col22.button("OK", key="addspouse_ok"):
                        if isAlive:
                            deathdate = None
                        # Attributes initialized with the isAlive property, everything else is optional
                        attributes = {
                            'isAlive': isAlive
                        }
                        if firstname != default_firstname:
                            attributes['firstname'] = firstname
                        if lastname != default_lastname:
                            attributes['lastname'] = lastname
                        if birthdate != default_birthdate:
                            attributes['birthdate'] = str(birthdate)
                        if birthplace != default_birthplace:
                            attributes['birthplace'] = birthplace
                        if (not isAlive) and (deathdate != default_deathdate):
                            attributes['deathdate'] = str(deathdate)
                        tree.add_spouse(selected_node_id, **attributes)
                        addspousecontainer.empty()
                        refresh_tree(tree)
                        st.rerun()
            ##################
            #   Add parent   #
            ##################
            addparentcontainer = col3.empty()
            with addparentcontainer.container():
                with st.popover(label="Add parent"):
                    st.write("Adding parent to " + selected_node_id)
                    default_firstname = ""
                    default_lastname = ""
                    default_birthdate = None
                    default_birthplace = ""
                    default_isAlive = True
                    default_deathdate = None
                    firstname = st.text_input("First name", value=default_firstname, key='addparent_firstname')
                    lastname = st.text_input("Last name", value=default_lastname, key='addparent_lastname')
                    birthdate = st.date_input("Select a date", value=default_birthdate, min_value='1700-01-01', key="addparent_birthdate")
                    birthplace = st.text_input("Birthplace", value=default_birthplace, key='addparent_birthplace')
                    isAlive = st.checkbox("Is alive", value=default_isAlive, key="addparent_isalive")
                    if not isAlive:
                        deathdate = st.date_input("Select a date", value=default_deathdate, min_value='1700-01-01', key="addparent_deathdate")
                    col31, col32 = st.columns(2)
                    if col31.button("Cancel", key="addparent_cancel"):
                        addparentcontainer.empty()
                        st.rerun()
                    if col32.button("OK", key="addparent_ok"):
                        if isAlive:
                            deathdate = None
                        # Attributes initialized with the isAlive property, everything else is optional
                        attributes = {
                            'isAlive': isAlive
                        }
                        if firstname != default_firstname:
                            attributes['firstname'] = firstname
                        if lastname != default_lastname:
                            attributes['lastname'] = lastname
                        if birthdate != default_birthdate:
                            attributes['birthdate'] = str(birthdate)
                        if birthplace != default_birthplace:
                            attributes['birthplace'] = birthplace
                        if (not isAlive) and (deathdate != default_deathdate):
                            attributes['deathdate'] = str(deathdate)
                        tree.add_parent(selected_node_id, **attributes)
                        addparentcontainer.empty()
                        refresh_tree(tree)
                        st.rerun()
            ############
            #   Edit   #
            ############
            editpersoncontainer = col4.empty()
            with editpersoncontainer.container():
                with st.popover(label="Edit"):
                    st.write("Editing " + selected_node_id)
                    if selected_node:
                        date_format = "%a %b %d %Y"
                        if "firstname" in selected_node:
                            default_firstname = selected_node['firstname']
                        else:
                            default_firstname = ""
                        if "lastname" in selected_node:
                            default_lastname = selected_node['lastname']
                        else:
                            default_lastname = ""
                        if "birthdate" in selected_node and len(selected_node['birthdate']) > 0:
                            try:
                                default_birthdate = datetime.datetime.strptime(selected_node['birthdate'], date_format).date()
                            except:
                                default_birthdate = datetime.date.today()  # In case the string cannot be converted to a date
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
                        if "deathdate" in selected_node and len(selected_node['deathdate']) > 0:
                            default_deathdate = datetime.datetime.strptime(selected_node['deathdate'], date_format).date()
                        else:
                            default_deathdate = datetime.date.today()
                        firstname = st.text_input("First name", value=default_firstname, key='edit_firstname')
                        lastname = st.text_input("Last name", value=default_lastname, key='edit_lastname')
                        birthdate = st.date_input("Select a date", value=default_birthdate, key="edit_birthdate", min_value='1700-01-01')
                        birthplace = st.text_input("Birthplace", value=default_birthplace, key='edit_birthplace')
                        isAlive = st.checkbox("Is alive", value=default_isAlive, key="edit_isalive")
                        if not isAlive:
                            deathdate = st.date_input("Select a date", value=default_deathdate, min_value='1700-01-01', key="edit_deathdate")
                        col41, col42 = st.columns(2)
                        if col41.button("Cancel", key="edit_cancel"):
                            editpersoncontainer.empty()
                            st.rerun()
                        if col42.button("OK", key="edit_ok"):
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
                            refresh_tree(tree)
                            st.rerun()
                    else:
                        st.write("No node found for " + selected_node_id)
            ##############
            #   Delete   #
            ##############
            deletepersoncontainer = col5.empty()
            with deletepersoncontainer.container():
                with st.popover(label="Delete"):
                    st.write("Delete " + selected_node_id + "?")
                    col51, col52 = st.columns(2)
                    if col51.button("Cancel", key="delete_cancel"):
                        deletepersoncontainer.empty()
                        st.rerun()
                    if col52.button("Yes, delete", key="delete_ok"):
                        tree.delete_person(selected_node_id)
                        deletepersoncontainer.empty()
                        refresh_tree(tree)
                        st.rerun()
            ###############
            # Add picture #
            ###############
            addprofilepiccontainer = col1.empty()
            with addprofilepiccontainer.container():
                with st.popover(label="Add profile picture"):
                    st.write("Select picture file to add to " + selected_node_id)
                    uploaded_file = st.file_uploader("Choose a picture", type=["png", "jpg"], key="addprofilepic_uploader")
                    col61, col62 = st.columns(2)
                    if col61.button("Cancel", key="addprofilepic_cancel"):
                        addprofilepiccontainer.empty()
                        st.rerun()
                    if col62.button("OK", key="addprofilepic_ok"):
                        if uploaded_file is not None:
                            # Upload to Azure Storage
                            picture_url = upload_to_azure_storage(uploaded_file, azure_storage_account, azure_storage_container, azure_storage_key)
                            tree.add_profile_picture(selected_node_id, picture_url)
                        addprofilepiccontainer.empty()
                        st.rerun()
            addpiccontainer = col2.empty()
            with addpiccontainer.container():
                with st.popover(label="Add picture"):
                    st.write("Select picture file to add to " + selected_node_id)
                    uploaded_file = st.file_uploader("Choose a picture", type=["png", "jpg"], key="addpic_uploader")
                    col71, col72 = st.columns(2)
                    if col71.button("Cancel", key="addpic_cancel"):
                        addpiccontainer.empty()
                        st.rerun()
                    if col72.button("OK", key="addpic_ok"):
                        if uploaded_file is not None:
                            # Upload to Azure Storage
                            picture_url = upload_to_azure_storage(uploaded_file, azure_storage_account, azure_storage_container, azure_storage_key)
                            tree.add_picture(selected_node_id, picture_url)
                        addpiccontainer.empty()
                        st.rerun()
            #################
            # View pictures #
            #################
            viewprofilepiccontainer = col1.empty()
            with viewprofilepiccontainer.container():
                with st.popover(label="View profile picture"):
                    st.write("Profile picture for " + selected_node_id)
                    if selected_node:
                        picture_url = selected_node.get('profilepic', None)
                        if picture_url:
                            picture_url = picture_url + "?" + azure_storage_sas
                            st.image(picture_url, width=50)
                        else:
                            st.write("No profile picture found")
                    if st.button("Close", key="viewprofilepic_cancel"):
                        viewprofilepiccontainer.empty()
                        st.rerun()
            viewpiccontainer = col2.empty()
            with viewpiccontainer.container():
                with st.popover(label="View pictures"):
                    st.write("Pictures for " + selected_node_id)
                    if selected_node:
                        if 'pictures' in selected_node:
                            sas_links = [x + "?" + azure_storage_sas for x in selected_node['pictures']]
                            # for picture_url in sas_links:
                            #     st.write(picture_url)
                            st.image(sas_links, width=200)
                        else:
                            st.write("No pictures found")
                    if st.button("Close", key="viewpic_cancel"):
                        viewpiccontainer.empty()
                        st.rerun()


    # st_link_analysis(tree, node_styles=node_styles, edge_styles=edge_styles, layout=layout, key="xyz")

    # DEBUG: tree object
    # st.write("Loaded tree:")
    # st.write(tree_st)

    ######################
    # Tree functionality #
    ######################

    left, middle, right = st.columns(3)
    # Add test data with confirmation (using state would probably be cleaner)
    addtest_container = left.empty()
    with addtest_container.container():
        with st.popover(label="Add test data"):
            st.write("Do you really want to delete all existing data and replace it with test data?")
            coll1, coll2 = st.columns(2)
            if coll1.button("Cancel", key="add_test_cancel"):
                addtest_container.empty()
                st.rerun()
            if coll2.button("Yes, add data", key="add_test_ok"):
                tree.add_test_family()
                addtest_container.empty()
                st.rerun()
    # Delete all button with confirmation (using state would probably be cleaner)
    deleteall_container = middle.empty()
    with deleteall_container.container():
        with st.popover(label="Delete all"):
            st.write("Do you really want to delete all nodes?")
            colm1, colm2 = st.columns(2)
            if colm1.button("Cancel", key="deleteall_cancel"):
                deleteall_container.empty()
                st.rerun()
            if colm2.button("Yes, delete all", key="deleteall_ok"):
                tree.delete_all()
                deleteall_container.empty()
                st.rerun()
    # Import from file containing an export from the 3rd party app "Family Tree"
    import_container = right.empty()
    with import_container.container():
        with st.popover(label="Import"):
            st.write("Select a file containing data exported by the third-party app 'Family Tree':")
            import_filename = st.text_input("File path", value=Path.home(), key="import_filename")
            import_picsfolder = st.text_input("Pictures folder", value=Path.home(), key="import_picsfolder")
            colr1, colr2 = st.columns(2)
            if colr1.button("Cancel", key="import_cancel"):
                import_container.empty()
                st.rerun()
            if colr2.button("Yes, import tree", key="import_ok"):
                if len(import_picsfolder) > 0:
                    nodes_added = tree.import_from_app_json(
                        import_filename, 
                        import_pics=True, 
                        pics_folder=import_picsfolder, 
                        azure_storage_account=azure_storage_account, 
                        azure_storage_key=azure_storage_key, 
                        azure_storage_container=azure_storage_container)
                else:
                    nodes_added = tree.import_from_app_json(
                        import_filename,
                        import_pics=False,
                    )
                st.write(f"Imported {nodes_added} nodes.")
                import_container.empty()
                st.rerun()

import streamlit as st
import pyvis
from familytree import FamilyTree
import matplotlib.pyplot as plt

def show_pyvis():
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
    tree_pyvis = pyvis.Network()
    tree_pyvis.from_nx(tree.graph)
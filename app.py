import streamlit as st
import uuid
from gremlin_python.driver import client, serializer
from st_link_analysis import st_link_analysis, NodeStyle, EdgeStyle


# https://learn.microsoft.com/en-us/azure/cosmos-db/gremlin/quickstart-python?tabs=azure-cli
def add_test_family(client):
    insert_vertex_query = (
    "g.addV('person')"
    ".property('id', prop_id)"
    ".property('name', prop_name)"
    )
    dad_uuid = str(uuid.uuid4())
    mom_uuid = str(uuid.uuid4())
    kid_uuid = str(uuid.uuid4())
    client.submit(message=insert_vertex_query, bindings={"prop_id": dad_uuid, "prop_name": "John Doe"}).all().result()
    client.submit(message=insert_vertex_query, bindings={"prop_id": mom_uuid, "prop_name": "Jane Doe"}).all().result()
    client.submit(message=insert_vertex_query, bindings={"prop_id": kid_uuid, "prop_name": "JackJack Doe"}).all().result()
    insert_edge_childof_query = (
        "g.V(prop_source_id)"
        ".addE('isChildOf')"
        ".to(g.V(prop_target_id))"
    )
    insert_edge_spouseof_query = (
        "g.V(prop_source_id)"
        ".addE('isSpouseOf')"
        ".to(g.V(prop_target_id))"
    )
    client.submit(message=insert_edge_childof_query, bindings={"prop_source_id": kid_uuid, "prop_target_id": dad_uuid}).all().result()
    client.submit(message=insert_edge_childof_query, bindings={"prop_source_id": kid_uuid, "prop_target_id": mom_uuid}).all().result()
    client.submit(message=insert_edge_spouseof_query, bindings={"prop_source_id": dad_uuid, "prop_target_id": mom_uuid}).all().result()

def load_graph(client):
    find_vertices_query = (
        "g.V().hasLabel('person')"
    )
    nodes = client.submit(
        message=find_vertices_query,
        bindings={
        },
    ).all().result()
    find_edges_query = (
        "g.E()"
    )
    edges = client.submit(
        message=find_edges_query,
        bindings={
        },
    ).all().result()
    tree = {
        'nodes': [{'data': {'id': node['id'], 'label': node['label'], 'name': node['properties']['name'][0]['value']}} for node in nodes],
        'edges': [{'data': {'id': edge['id'], 'label': edge['label'], 'source': edge['outV'], 'target': edge['inV']}} for edge in edges]
    }
    return tree

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

# https://learn.microsoft.com/en-us/azure/cosmos-db/gremlin/quickstart-python?tabs=azure-cli
client = client.Client(
    url=f"wss://{st.secrets["cosmosdb"].host}.gremlin.cosmos.azure.com:443/",
    traversal_source="g",
    username="/dbs/familytree1/colls/familytree1",
    password=f"{st.secrets["cosmosdb"].password}",
    message_serializer=serializer.GraphSONSerializersV2d0()
)

tree = load_graph(client=client)
if not hasattr(st.session_state, "graph"):
    st.session_state.graph = tree
node_styles = [
    NodeStyle("person", "#FF7F3E", "name", "person"),
]
edge_styles = [
    EdgeStyle("isChildOf", caption='label', directed=True),
    EdgeStyle("isSpouseOf", caption='label', directed=False)
]
layout = {"name": "cose", "animate": "end", "nodeDimensionsIncludeLabels": False}
elements = st.session_state.graph
with st.container(border=True):
    vals = st_link_analysis(elements, node_styles=node_styles, edge_styles=edge_styles, layout=layout, key=COMPONENT_KEY, node_actions=['expand'], on_change=onchange_callback)
    if vals:
        col1, col2, col3, col4 = st.columns(4)
        if col1.button("Add child"):
            st.write("Adding child...")
            # Add child logic here
        if col2.button("Add spouse"):
            st.write("Adding spouse...")
            # Add spouse logic here
        if col3.button("Add parent"):
            st.write("Adding parent...")
            # Add parent logic here
        if col4.button("Remove person"):
            st.write("Removing person...")
            # Remove person logic here
    else:
        st.write("Please double click on a node to see its details.")

# st_link_analysis(tree, node_styles=node_styles, edge_styles=edge_styles, layout=layout, key="xyz")

# st.write("Loaded tree:")
# st.write(tree)

left, middle, right = st.columns(3)
if left.button("Add a test family"):
    st.write("Adding a test family...")
    add_test_family(client=client)
if middle.button("Delete all nodes"):
    st.write("Deleting all nodes...")
    client.submit("g.V().drop()").all().result()
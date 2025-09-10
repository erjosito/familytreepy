import networkx as nx
import uuid
import os
import json
import tempfile
from gremlin_python.driver import client, serializer
from azure.storage.blob import BlobServiceClient


class FamilyTree:
    # Backends can be local, azstorage, cosmosdb
    # If local is specified, the following parameter must be provided: localfile
    # If azstorage is specified, the following parameters must be provided: azstorage, azstoragekey
    # If cosmosdb is specified, the following parameters must be provided: cosmosdbhost, cosmosdbkey
    def __init__(self, 
                 backend='local', autosave=True,
                 localfile=None, 
                 azstorage_account=None, azstorage_key=None, azstorage_container=None, azstorage_blob=None, 
                 cosmosdb_host=None, cosmosdb_db=None, cosmosdb_collection=None, cosmosdb_key=None,
                 verbose=False):
        self.backend = backend
        self.localfile = localfile
        self.azstorage_account = azstorage_account
        self.azstorage_container = azstorage_container
        self.azstorage_blob = azstorage_blob
        self.azstorage_key = azstorage_key
        self.cosmosdb_host = cosmosdb_host
        self.cosmosdb_db = cosmosdb_db
        self.cosmosdb_collection = cosmosdb_collection
        self.cosmosdb_key = cosmosdb_key
        self.autosave = autosave
        if self.backend == "local" and len(self.localfile) > 0:
            self.tempfile = os.path.splitext(self.localfile)[0] + "_temp" + os.path.splitext(self.localfile)[1]
        # Create new graph or load it
        if self.backend == 'local':
            if not self.localfile:
                raise ValueError("Local file must be specified to load data when using backend=local")
            elif not os.path.exists(self.localfile):
                self.graph = nx.DiGraph()
                self.save_local()
            else:
                try:
                    self.load_local()
                except Exception as e:
                    # Error loading local file, initializating empty graph
                    self.graph = nx.DiGraph()
        # To Do: mimick local behavior
        elif self.backend == 'azstorage':
            if not self.azstorage_account or not self.azstorage_key or not self.azstorage_container or not self.azstorage_blob:
                raise ValueError("All Azure Storage parameters must be provided: azstorage_account, azstorage_key, azstorage_container, azstorage_blob")
            # load_azstorage() will return true if successful
            elif self.load_azstorage():
                if verbose:
                    print("DEBUG: Graph loaded successfully")
            else:
                # Error loading Azure Storage file, initializing empty graph
                self.graph = nx.DiGraph()
                try:
                    self.save_azstorage()
                except Exception as e:
                    print(f"Error saving Azure Storage file: {e}")
        elif self.backend == 'cosmosdb':
            self.load_cosmosdb()
        elif self.backend == 'cosmosdb':
            if not all([self.cosmosdb_host, self.cosmosdb_db, self.cosmosdb_collection, self.cosmosdb_key]):
                raise ValueError("All CosmosDB parameters must be provided: cosmosdb_host, cosmosdb_db, cosmosdb_collection, cosmosdb_key")
            self.cosmosdb_client = client.Client(
                url=f"wss://{self.cosmosdb_host}.gremlin.cosmos.azure.com:443/",
                traversal_source="g",
                username="/dbs/{self.cosmosdb_db}/colls/{self.cosmosdb_collection}",
                password=f"{self.cosmosdb_key}",
                message_serializer=serializer.GraphSONSerializersV2d0()
            )
        else:
            raise ValueError("Invalid backend specified")
    def save(self):
        # Save the graph to the specified backend
        if self.backend == 'local':
            self.save_local()
        elif self.backend == 'azstorage':
            self.save_azstorage()
        elif self.backend == 'cosmosdb':
            self.save_cosmosdb()
        else:
            raise ValueError("Invalid backend specified")
    def save_local(self):
        # Save the graph to a local file
        # if self.localfile and self.tempfile:
        #     try:
        #         nx.write_gml(self.graph, self.tempfile)
        #     except Exception as e:
        #         print(f"Error saving graph to temporary file: {e}")
        #     finally:
        #         # Move the temporary file to the original location
        #         if os.path.exists(self.tempfile):
        #             os.replace(self.tempfile, self.localfile)
        if self.localfile:
            temp_dir = tempfile.TemporaryDirectory()
            temp_file = os.path.join(temp_dir.name, "graph.gml")
            try:
                nx.write_gml(self.graph, temp_file)
            except Exception as e:
                print(f"Error saving graph to temporary file: {e}")
            finally:
                # Move the temporary file to the original location
                if os.path.exists(temp_file):
                    os.replace(temp_file, self.localfile)
        else:
            raise ValueError("Local file must be specified to save data when using backend=local")
    # Save the graph to a local file in a local temporary directory and upload it to blob storage
    def save_azstorage(self):
        if self.azstorage_account and self.azstorage_key and self.azstorage_container and self.azstorage_blob:
            blob_service_client = BlobServiceClient.from_connection_string(f"DefaultEndpointsProtocol=https;AccountName={self.azstorage_account};AccountKey={self.azstorage_key}")
            blob_client = blob_service_client.get_blob_client(container=self.azstorage_container, blob=self.azstorage_blob)
            temp_dir = tempfile.TemporaryDirectory()
            temp_file = os.path.join(temp_dir.name, "graph.gml")
            try:
                nx.write_gml(self.graph, temp_file)
                with open(temp_file, "rb") as data:
                    blob_client.upload_blob(data, overwrite=True)
            except Exception as e:
                print(f"Error saving graph to Azure Storage: {e}")
    # To Do: export the graph to CosmosDB
    def save_cosmosdb(self):
        # Save the graph to Azure Cosmos DB
        pass
    def load(self):
        if self.backend == 'local':
            self.load_local()
        elif self.backend == 'azstorage':
            self.load_azstorage()
        elif self.backend == 'cosmosdb':
            self.load_cosmosdb()
        else:
            raise ValueError("Invalid backend specified")
    def load_local(self):
        # Load the graph from a local file
        if self.localfile:
            self.graph = nx.read_gml(self.localfile)
        else:
            raise ValueError("Local file must be specified to load data when using backend=local")
    def load_azstorage(self):
        # Download the graph from Azure Storage to a temp file and open it
        if self.azstorage_account and self.azstorage_key and self.azstorage_container and self.azstorage_blob:
            blob_service_client = BlobServiceClient.from_connection_string(f"DefaultEndpointsProtocol=https;AccountName={self.azstorage_account};AccountKey={self.azstorage_key}")
            blob_client = blob_service_client.get_blob_client(container=self.azstorage_container, blob=self.azstorage_blob)
            temp_dir = tempfile.TemporaryDirectory()
            temp_file = os.path.join(temp_dir.name, "graph.gml")
            try:
                with open(temp_file, mode="wb") as f:
                    f.write(blob_client.download_blob().readall())
                self.graph = nx.read_gml(temp_file)
                return True
            except Exception as e:
                # print(f"Error loading graph from Azure Storage: {e}")
                return False
        else:
            raise ValueError("Local file must be specified to load data when using backend=local")
    def load_cosmosdb(self):
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
        # To Do: Process the loaded nodes and edges and turn them into a networkx graph
    def set_localfile(self, localfile):
        self.localfile = localfile
    ###############
    #    Import   #
    ###############
    def import_from_app_json(self, json_data_file, import_pics=False, pics_folder=None, azure_storage_account=None, azure_storage_key=None, azure_storage_container=None):
        # Clear the existing graph
        self.graph.clear()
        # Optionally, upload the images to the provided Azure Storage account, verifying that the provided folder exists
        if import_pics and pics_folder and azure_storage_account and azure_storage_key and azure_storage_container and os.path.exists(pics_folder):
            blob_service_client = BlobServiceClient.from_connection_string(f"DefaultEndpointsProtocol=https;AccountName={azure_storage_account};AccountKey={azure_storage_key}")
            # blob_service_client = BlobServiceClient(account_url=azure_storage_account, credential=azure_storage_key)
            # container_client = blob_service_client.get_container_client(azure_storage_container)
            for root, dirs, files in os.walk(pics_folder):
                for filename in files:
                    file_path = os.path.join(root, filename)
                    # blob_client = container_client.get_blob_client(blob=filename)
                    blob_client = blob_service_client.get_blob_client(container=azure_storage_container, blob=filename)
                    with open(file_path, "rb") as data:
                        blob_client.upload_blob(data, overwrite=True)
        # Load the JSON file into a JSON object, and raise an exception if the file contains invalid JSON
        try:
            with open(json_data_file, 'r', encoding='utf8') as f:
                json_data = json.load(f)
        except json.JSONDecodeError as e:
            raise ValueError(f"Invalid JSON file: {e}")
        # Add the nodes into the graph
        if 'nodes' in json_data and 'infos' in json_data:
            # Disable autosave for the import
            old_autosave_value = self.autosave
            self.autosave = False
            # First add the nodes
            node_count = 0
            for node_id in json_data['nodes']:
                if 'infoId' in json_data['nodes'][node_id]:
                    node_info_id = json_data['nodes'][node_id]["infoId"]
                    node_info = json_data['infos'][node_info_id]
                    new_node_attributes = {
                        'label': 'person',
                        'firstname': node_info.get('firstName', ''),
                        'lastname': node_info.get('lastName', ''),
                        'birthdate': node_info.get('dob', ''),
                        'isAlive': not node_info.get('isAbsent', False),
                        'deathdate': node_info.get('dod', ''),
                    }
                    # Add the picture info if the option 'import_pics' is enabled
                    if import_pics and 'profilePhoto' in node_info:
                        new_node_attributes['pictures'] = [ f"https://{azure_storage_account}.blob.core.windows.net/{azure_storage_container}/{node_info['profilePhoto']}" ]
                    self.add_person(id=node_id, **new_node_attributes)
                else:
                    raise ValueError(f"Node {node_id} is missing 'infoId' attribute")
                node_count += 1
            # Now add the edges
            for node_id in json_data['nodes']:
                if 'relations' in json_data['nodes'][node_id]:
                    for relation in json_data['nodes'][node_id]['relations']:
                        target_id = relation.get('to')
                        if target_id in self.graph and node_id in self.graph:
                            if relation.get('type') == 'parent':
                                self.add_relationship(node_id, target_id, type='isChildOf')
                            elif relation.get('type') == 'child':
                                self.add_relationship(target_id, node_id, type='isChildOf')
                            elif relation.get('type') == 'spouse':
                                self.add_relationship(node_id, target_id, type='isSpouseOf')
                                self.add_relationship(target_id, node_id, type='isSpouseOf')
                            else:
                                raise ValueError(f"Invalid relationship type {relation.get('type')} for nodes {node_id} and {target_id}")
                        else:
                            raise ValueError(f"Cannot create relationship: one of the nodes {node_id} or {target_id} does not exist")
            # Restore previous autosave value and save the tree
            self.save()
            self.autosave = old_autosave_value
            # Done!
            return node_count
        else:
            raise ValueError("Invalid JSON format: 'nodes' and 'infos' keys are required")

    ###############
    #     Add     #
    ###############
    def add_person(self, id=None, **attributes):
        if id is not None:
            person_id = id  # Not enforcing that ID is a valid UUID
        else:
            person_id = str(uuid.uuid4())
        self.graph.add_node(person_id, **attributes)
        if self.autosave:
            self.save()
        if self.backend == 'cosmosdb':
            # Add the person to CosmosDB
            gremlin_query = f"g.addV('person').property('id', '{person_id}')"
            for key, value in attributes.items():
                gremlin_query += f".property('{key}', '{value}')"
            self.cosmosdb_client.submit(gremlin_query).all().result()
        return person_id
    # Relationships can be of types "isChildOf" or "isSpouseOf"
    def add_relationship(self, person1_id, person2_id, type):
        if person1_id not in self.graph or person2_id not in self.graph:
            raise ValueError("Both persons must be in the family tree")
        if type not in ['isChildOf', 'isSpouseOf']:
            raise ValueError("Invalid relationship type " + type + ". Valid relationship types are: isChildOf, isSpouseOf")
        self.graph.add_edge(person1_id, person2_id, type=type)
        if self.autosave:
            self.save()
        if self.backend == 'cosmosdb':
            # Add the relationship to CosmosDB
            gremlin_query = f"g.V('{person1_id}').addE('{type}').to(g.V('{person2_id}'))"
            self.cosmosdb_client.submit(gremlin_query).all().result()
    def add_child(self, root_id, include_spouse=True, **child_attributes):
        if root_id not in self.graph:
            raise ValueError("Root person must be in the family tree")
        child_id = self.add_person(**child_attributes)
        self.add_relationship(child_id, root_id, type='isChildOf')
        if include_spouse:
            spouse_ids = self.get_spouses(root_id)
            if len(spouse_ids) > 0:
                for spouse_id in spouse_ids:
                    self.add_relationship(child_id, spouse_id, type='isChildOf')
        return child_id
    def add_spouse(self, root_id, **spouse_attributes):
        if root_id not in self.graph:
            raise ValueError("Root person must be in the family tree")
        spouse_id = self.add_person(**spouse_attributes)
        # In a directed graph you need to include the link twice
        self.add_relationship(root_id, spouse_id, type='isSpouseOf')
        self.add_relationship(spouse_id, root_id, type='isSpouseOf')
    def add_parent(self, root_id, **parent_attributes):
        if root_id not in self.graph:
            raise ValueError("Root person must be in the family tree")
        parent_id = self.add_person(**parent_attributes)
        self.add_relationship(root_id, parent_id, type='isChildOf')
    ###############
    #   Update    #
    ###############
    def update_person(self, person_id, **attributes):
        if person_id not in self.graph:
            raise ValueError("Person must be in the family tree")
        for key, value in attributes.items():
            self.graph.nodes[person_id][key] = value
        if self.autosave:
            self.save()
            if self.backend == 'cosmosdb':
                # Update the person in CosmosDB -- THIS DOES NOT WORK
                gremlin_query = f"g.V('{person_id}').fold().coalesce(unfold() ,addV('person').property('id','1'))"
                for key, value in attributes.items():
                    gremlin_query += f".property('{key}', '{value}')"
                self.cosmosdb_client.submit(gremlin_query).all().result()
    #################
    #   Subgraphs   #
    #################
    # Get a subgraph centered on a person, including all nodes within 'degree' relationship hops
    def get_subgraph_degrees(self, person_id, degree=1):
        if person_id not in self.graph:
            raise ValueError("Person must be in the family tree")
        nodes_within_degree = nx.single_source_shortest_path_length(self.graph.to_undirected(), person_id, cutoff=degree).keys()
        return self.graph.subgraph(nodes_within_degree).copy()
    # Get a subgraph containing all paths between two persons
    def get_subgraph_between(self, person1_id, person2_id):
        if person1_id not in self.graph or person2_id not in self.graph:
            raise ValueError("Both persons must be in the family tree")
        try:
            paths = list(nx.all_shortest_paths(self.graph.to_undirected(), source=person1_id, target=person2_id))
            nodes_in_paths = set()
            for path in paths:
                nodes_in_paths.update(path)
            return self.graph.subgraph(nodes_in_paths).copy()
        except nx.NetworkXNoPath:
            return nx.DiGraph()
    # Get the longest chain of ancestors in the tree using edges of type 'isChildOf'
    def get_longest_ancestor_chain(self):
        def dfs(current_node, visited):
            visited.add(current_node)
            max_length = 0
            for neighbor in self.graph.successors(current_node):
                if self.graph[current_node][neighbor]['type'] == 'isChildOf' and neighbor not in visited:
                    length = dfs(neighbor, visited)
                    max_length = max(max_length, length)
            visited.remove(current_node)
            return max_length + 1
        longest_chain = 0
        for node in self.graph.nodes():
            chain_length = dfs(node, set())
            longest_chain = max(longest_chain, chain_length)
        return longest_chain
    # Add a level attribute to each node indicating its generation level.
    def assign_generation_levels(self, debug=False):
        # Recursive function to assign levels
        def assign_level(node_id, level):
            if debug:
                print(f"DEBUG: Assigning level {level} to node {node_id} ({self.graph.nodes[node_id].get('firstname', '')} {self.graph.nodes[node_id].get('lastname', '')})")
            self.graph.nodes[node_id]['level'] = level                      # This marks the node as visited
            # Look for neighbors with outgoing edges (successors): parents (isChildOf) and spouses
            for neighbor in self.graph.successors(node_id):
                if self.graph[node_id][neighbor]['type'] == 'isChildOf':
                    if 'level' not in self.graph.nodes[neighbor]:
                        assign_level(neighbor, level - 1)
                elif self.graph[node_id][neighbor]['type'] == 'isSpouseOf':
                    if 'level' not in self.graph.nodes[neighbor]:
                        assign_level(neighbor, level)
            # Look for neighbors with incoming edges (predecessors): children and spouses
            for neighbor in self.graph.predecessors(node_id):
                if self.graph[neighbor][node_id]['type'] == 'isChildOf':
                    if 'level' not in self.graph.nodes[neighbor]:
                        assign_level(neighbor, level + 1)
                elif self.graph[neighbor][node_id]['type'] == 'isSpouseOf':     # Although this shouldnt be required, since the isSpouseOf relationship is bidirectional
                    if 'level' not in self.graph.nodes[neighbor]:
                        assign_level(neighbor, level)
        # Start from any node, from example, the first one, and assign levels recursively to all its neighbors
        if len(self.graph.nodes) > 0:
            # Take the first node as root
            root_node_id = list(self.graph.nodes)[0]
        else:
            root_node_id = None
        if root_node_id:
            assign_level(root_node_id, 0)
            # Get the minimum level assigned
            min_level = min((data['level'] for node, data in self.graph.nodes(data=True) if 'level' in data), default=None)
            if min_level != 0:
                # Increase all levels by the negative value of min_level to make the lowest level 0
                for node in self.graph.nodes():
                    if 'level' in self.graph.nodes[node]:
                        self.graph.nodes[node]['level'] += -1 * min_level

    ###############
    #     Get     #
    ###############
    def get_person(self, person_id):
        if person_id in self.graph:
            return self.graph.nodes[person_id]
        else:
            return None
    def get_spouses(self, person_id):
        if person_id not in self.graph:
            raise ValueError("Person must be in the family tree")
        spouse_ids = []
        neighbors = self.graph.adj[person_id]
        for i in neighbors:
            if neighbors[i]['type'] == 'isSpouseOf':
                spouse_ids.append(i)
        return spouse_ids
    # For all nodes of type 'person' return a list containing a combination of their first and last names, if they exist
    def get_person_list(self):
        # return [ (node, self.graph.nodes[node].get('firstname', '') + ' ' + self.graph.nodes[node].get('lastname', '')).strip()
        person_list = []
        for node in self.graph.nodes():
            full_name = (self.graph.nodes[node].get('firstname', '') + ' ' + self.graph.nodes[node].get('lastname', '')).strip()
            if len(full_name) > 0:
                person_list.append(full_name)
        return person_list
    # Get a node matching a full name (first + last name)
    def get_person_by_full_name(self, full_name):
        for node in self.graph.nodes():
            node_full_name = (self.graph.nodes[node].get('firstname', '') + ' ' + self.graph.nodes[node].get('lastname', '')).strip()
            if node_full_name.lower() == full_name.lower():
                return node
        return None
    # Return a (sub)graph formatted for representation with the Streamlit Link Analysis library
    def format_for_st_link_analysis(self, root_id=None, degree=None):
        nodes = []
        edges = []
        # Optionally filter the graph to a subgraph centered on root_id within 'degree' hops
        if root_id and degree:
            subgraph = self.get_subgraph_degrees(root_id, degree=degree)
        else:
            subgraph = self.graph
        # Build the nodes and edges lists in a dictionary format
        for person_id, person_data in subgraph.nodes(data=True):
            person = person_data
            person["id"] = person_id
            person["label"] = "person"
            person["fullname"] = (person.get('firstname', '') + ' ' + person.get('lastname', '')).strip()
            person["fullname_linebreaks"] = person["fullname"].replace(' ', '\n')
            nodes.append({"data": person})
        for source, target, edge_data in subgraph.edges(data=True):
            edges.append({
                "data": {
                    "id": source + "to" + target,
                    "source": source,
                    "target": target,
                    "label": edge_data.get("type", ""),
                }
            })
        return {"nodes": nodes, "edges": edges}
    ###############
    #   Pictures  #
    ###############
    def add_profile_picture(self, person_id, picture_url):
        if person_id not in self.graph:
            raise ValueError("Person must be in the family tree")
        self.graph.nodes[person_id]["profilepic"] = picture_url
        if self.autosave:
            self.save()
    def add_picture(self, person_id, picture_url):
        if person_id not in self.graph:
            raise ValueError("Person must be in the family tree")
        if 'pictures' not in self.graph.nodes[person_id]:
            self.graph.nodes[person_id]["pictures"] = []
        self.graph.nodes[person_id]["pictures"].append(picture_url)
        if self.autosave:
            self.save()

    ###############
    #    Delete   #
    ###############
    def delete_person(self, person_id):
        if person_id in self.graph:
            self.graph.remove_node(person_id)
        if self.autosave:
            self.save()
        if self.backend == 'cosmosdb':
            # Remove the node from CosmosDB
            pass
        return person_id
    def delete_all(self):
        self.graph.clear()

    ###############
    #    Debug    #
    ###############
    def print(self):
        print("Family Tree:")
        for person in self.graph.nodes(data=True):
            print(f" - {person[0]}: {person[1]}")
        for relationship in self.graph.edges(data=True):
            print(f" - {relationship[0]} {relationship[2]['type']} {relationship[1]}")
    def add_test_family(self):
        # Add a test family
        self.delete_all()
        father_id = self.add_person(firstname="John", surname="Doe", isAlive=True, birthdate="1980-01-01", birthplace="City A")
        mother_id = self.add_spouse(father_id, firstname="Jane", surname="Doe", isAlive=True, birthdate="1980-01-01", birthplace="City B")
        child1_id = self.add_child(father_id, firstname="Alice", surname="Doe", isAlive=True, birthdate="2015-01-01", birthplace="City A")

    ####################
    # Generate Image   #
    ####################
    def generate_image(self, root_person_id=None, degrees=None, 
                       canvas_width=1200, canvas_height=800, 
                       root_folder='.', azure_storage_sas=None,
                       image_path='.', image_filename='familytree.png', 
                       verbose=False):
        from PIL import Image, ImageDraw, ImageFont, ImageFilter
        import numpy as np
        import requests
        import tempfile
        import os
        import random
        import argparse

        def assign_vlevels(graph, debug=False):
            # Recursive function to assign levels
            def assign_vlevel(node_id, vlevel):
                if debug:
                    print(f"DEBUG: Assigning vlevel {vlevel} to node {node_id} ({graph.nodes[node_id].get('firstname', '')} {graph.nodes[node_id].get('lastname', '')})")
                graph.nodes[node_id]['vlevel'] = vlevel                      # This marks the node as visited
                # Look for neighbors with outgoing edges (successors): parents (isChildOf) and spouses
                for neighbor in graph.successors(node_id):
                    if graph[node_id][neighbor]['type'] == 'isChildOf':
                        if 'vlevel' not in graph.nodes[neighbor]:
                            assign_vlevel(neighbor, vlevel - 1)
                    elif graph[node_id][neighbor]['type'] == 'isSpouseOf':
                        if 'vlevel' not in graph.nodes[neighbor]:
                            assign_vlevel(neighbor, vlevel)
                # Look for neighbors with incoming edges (predecessors): children and spouses
                for neighbor in graph.predecessors(node_id):
                    if graph[neighbor][node_id]['type'] == 'isChildOf':
                        if 'vlevel' not in graph.nodes[neighbor]:
                            assign_vlevel(neighbor, vlevel + 1)
                    elif graph[neighbor][node_id]['type'] == 'isSpouseOf':     # Although this shouldnt be required, since the isSpouseOf relationship is bidirectional
                        if 'vlevel' not in graph.nodes[neighbor]:
                            assign_vlevel(neighbor, vlevel)
            # Start from any node, from example, the first one, and assign vlevels recursively to all its neighbors
            if len(graph.nodes) > 0:
                # Take the first node as root
                root_node_id = list(graph.nodes)[0]
            else:
                root_node_id = None
            if root_node_id:
                assign_vlevel(root_node_id, 0)
                # Get the minimum level assigned and make it 0
                min_vlevel = min((data['vlevel'] for node, data in graph.nodes(data=True) if 'vlevel' in data), default=None)
                if min_vlevel != 0:
                    # Increase all levels by the negative value of min_vlevel to make the lowest level 0
                    for node in graph.nodes():
                        if 'vlevel' in graph.nodes[node]:
                            graph.nodes[node]['vlevel'] += -1 * min_vlevel
                min_vlevel = min((data['vlevel'] for node, data in graph.nodes(data=True) if 'vlevel' in data), default=None)
                max_vlevel = max((data['vlevel'] for node, data in graph.nodes(data=True) if 'vlevel' in data), default=None)
                if debug:
                    print(f"DEBUG: Generation vertical levels assigned from {min_vlevel} to {max_vlevel}")
                return graph, [min_vlevel, max_vlevel]

        # Not really used, still need to work on optimize hlevel placement
        def set_family(graph, debug=False):
            # For each vertical level, assign a family ID to each family group
            families = {}
            for person_id in graph.nodes():
                if 'parent_family_id' not in graph.nodes[person_id]:
                    # This person has not been assigned to a family yet as parent
                    # Look for their children
                    children = [neighbor for neighbor in graph.predecessors(person_id) if graph[neighbor][person_id]['type'] == 'isChildOf']
                    # If the person has children, assign everybody to the same family
                    if len(children) > 0:
                        family_id = None
                        # Try to get the family ID from one of the children
                        for child in children:
                            if 'child_family_id' in graph.nodes[child]:
                                family_id = graph.nodes[child]['child_family_id']
                                break
                        # Otherwise, create a new family ID
                        if not family_id:
                            family_id = str(uuid.uuid4())
                        for child in children:
                            graph.nodes[child]['child_family_id'] = family_id
                        graph.nodes[person_id]['parent_family_id'] = family_id
                        # Also assign the spouse(s) to the same family
                        spouses = [neighbor for neighbor in graph.successors(person_id) if graph[person_id][neighbor]['type'] == 'isSpouseOf']
                        for spouse in spouses:
                            graph.nodes[spouse]['parent_family_id'] = family_id
                        # Add the family to the families dictionary
                        if family_id not in families:
                            families[family_id] = {
                                'parents': [person_id] + spouses,
                                'children': children
                            }
                        else:
                            families[family_id]['parents'].extend([person_id] + spouses)
                            families[family_id]['children'].extend(children)
            return graph, families
        # Function that returns the IDs of the spouses of a person
        # Used to place children without spouses to the left of the chart, to avoid crossing lines
        def get_spouse_ids(graph, person_id):
            spouse_ids = []
            neighbors = graph.adj[person_id]
            for i in neighbors:
                if neighbors[i]['type'] == 'isSpouseOf':
                    spouse_ids.append(i)
            return spouse_ids
        # Function that returns the IDs of the parents of the spouses of a person
        # Used to place couples where the spouse has no uplinks to the left of the chart, to avoid crossing lines
        def get_spouse_parents(graph, person_id):
            parents = []
            spouses = get_spouse_ids(graph, person_id)
            for spouse in spouses:
                spouse_parents = [neighbor for neighbor in graph.successors(spouse) if graph[spouse][neighbor]['type'] == 'isChildOf']
                parents.extend(spouse_parents)
            return parents
        # This functions assigns the position in each vertical level (hlevel) to each person
        # This function is critical to reduce the amount of crossing lines in the final image between parents and children
        def assign_hlevels(graph, vlevels=None, debug=False):
            # Recursive function to assign hlevels
            def assign_hlevel(node_id, hlevels):
                # Assign the level to the target person
                if not 'hlevel' in graph.nodes[node_id]:
                    graph.nodes[node_id]['hlevel'] = hlevels[graph.nodes[node_id]['vlevel']]
                    hlevels[graph.nodes[node_id]['vlevel']] += 1
                # Assign a consecutive level to spouses on the same vlevel
                for neighbor in graph.successors(node_id):
                    if graph[node_id][neighbor]['type'] == 'isSpouseOf':
                        if not 'hlevel' in graph.nodes[neighbor]:
                            graph.nodes[neighbor]['hlevel'] = hlevels[graph.nodes[neighbor]['vlevel']]
                            hlevels[graph.nodes[neighbor]['vlevel']] += 1
                # Start picking hlevels for the children that have no spouses
                for neighbor in graph.predecessors(node_id):
                    if graph[neighbor][node_id]['type'] == 'isChildOf':
                        if len(get_spouse_ids(graph, neighbor)) == 0:
                            if not 'hlevel' in graph.nodes[neighbor]:
                                hlevels = assign_hlevel(neighbor, hlevels)
                # Continue picking hlevels for the children whose spouses have no parents
                for neighbor in graph.predecessors(node_id):
                    if graph[neighbor][node_id]['type'] == 'isChildOf':
                        if len(get_spouse_ids(graph, neighbor)) > 0 and len(get_spouse_parents(graph, neighbor)) == 0:
                            if not 'hlevel' in graph.nodes[neighbor]:
                                hlevels = assign_hlevel(neighbor, hlevels)
                # Finally pick hlevels for the children whose spouses have parents
                for neighbor in graph.predecessors(node_id):
                    if graph[neighbor][node_id]['type'] == 'isChildOf':
                        if len(get_spouse_ids(graph, neighbor)) > 0 and len(get_spouse_parents(graph, neighbor)) > 0:
                            if not 'hlevel' in graph.nodes[neighbor]:
                                hlevels = assign_hlevel(neighbor, hlevels)
                # And for the parents
                for neighbor in graph.predecessors(node_id):
                    if graph[neighbor][node_id]['type'] == 'isChildOf':
                        if not 'hlevel' in graph.nodes[neighbor]:
                            hlevels = assign_hlevel(neighbor, hlevels)
                return hlevels
            if vlevels is None:
                return None
            hlevels = [0 for x in range(vlevels[0], vlevels[1] + 1)]  # List of horizontal levels for each vertical level
            # For each vertical level, assign horizontal levels to each person in that level
            for vlevel in range(vlevels[0], vlevels[1] + 1):
                if verbose:
                    print(f"DEBUG: Assigning horizontal levels for vertical level {vlevel}")
                persons_in_level = [node for node, data in graph.nodes(data=True) if data.get('vlevel') == vlevel]
                if debug:
                    print(f"DEBUG: Persons in vertical level {vlevel}: {persons_in_level}")
                hlevel = 0
                for person in persons_in_level:
                    hlevels = assign_hlevel(person, hlevels)
            # Reduce in 1 the values of hlevels
            hlevels = [x - 1 for x in hlevels]
            if debug:
                print(f"DEBUG: Horizontal levels assigned: {hlevels}")
            return graph, hlevels

        # Load up tree from Azure Storage
        if root_person_id and degrees:
            subgraph = self.get_subgraph_degrees(root_person_id, degree=degrees)
        else:
            subgraph = self.graph

        # Assign levels
        subgraph, vlevels = assign_vlevels(subgraph, debug=verbose)
        subgraph, hlevels = assign_hlevels(subgraph, vlevels=vlevels, debug=verbose)
        subgraph, families = set_family(subgraph, debug=verbose)
        if verbose:
            print(f"DEBUG: Vertical levels: {vlevels}, Horizontal levels: {hlevels}")
            print(f"DEBUG: Families found: {len(families)}")

        # Image Generation - Canvas variables
        tempdir = tempfile.TemporaryDirectory()
        top_border = 0.05 # percent of height
        bottom_border = 0.05 # percent of height
        side_border = 0.05 # percent of width
        spacing_couple = 5
        spacing_siblings = 5
        personpic_v_ratio = 0.4 # person picture height as percentage of level height
        personpic_h_ratio = 0.4 # person picture width as percentage of level width
        min_spacing_family = 5
        pic_aspect_ratio = 1
        text_vert_fraction = 0.2
        text_vert_offset = 1.5   # Higher when text is wrapped
        atextwrap = 10
        aframewratio = 0.2      # Width of the text plate
        aframehratio = 0.2      # Height of the text plate
        char_textframe_ratio = 10
        size_ratio_for_long_text_plate = 2
        root_folder = 'imagegen'
        fontfile = os.path.join(root_folder, 'Seraphine.ttf')
        backgroundfile = os.path.join(root_folder, 'parchment.jpg')
        usable_canvas_height = round(canvas_height * (1 - top_border - bottom_border), 0)
        usable_canvas_width = round(canvas_width * (1 - 2 * side_border), 0)
        vlevel_height = int(round(usable_canvas_height / (vlevels[1] - vlevels[0] + 1), 0))
        hlevel_widths = [int(round(usable_canvas_width / hlevel, 0)) for hlevel in hlevels]  # The width of each "slice" at each level
        # We calculate different pic sizes for each horizontal level
        personpic_hs = [int(round(vlevel_height * personpic_v_ratio, 0)) for _ in hlevel_widths]
        personpic_ws = [int(round(hlevel_width * personpic_h_ratio, 0)) for hlevel_width in hlevel_widths]
        picframe_scale_factor = 1.2
        spouse_spacing_factor = 0.6 # Reduce the distance between spouses by this factor. The lower, the closer they get.
        vlevels_spacing_factor = 0.8
        hlevels_spacing_factor = 0.8
        # So that the vertical lines between levels are not exactly in the middle of the level, to avoid crossing lines
        parent_child_line_variability = 0.1
        # Percentage of the vertical lines between parent and child that goes down from the parent picture before going horizontal
        # The higher the value, the closer the line to the child's picture
        parent_child_line_length_ratio = 0.7
        # Color matrix for lines
        color_matrix = [(255,0,0), (0,255,0), (0,0,255), (255,255,0), (255,0,255), (0,255,255), (128,0,0), (0,128,0), (0,0,128), (128,128,0), (128,0,128), (0,128,128)]

        # For each vertical level, take the smaller of the two dimensions to keep aspect ratio
        # If the height is much smaller, it means that we can use wide name plates instead of the wrapped names with line breaks
        # The wide_nameplates variables are not used for now, we always use 2-line wrapped names 'firstname\nlastname' (aka partially wide nameplates)
        wide_nameplates = [False for _ in hlevel_widths]
        for i in range(len(personpic_ws)):
            if personpic_hs[i] < personpic_ws[i]:
                if size_ratio_for_long_text_plate * personpic_hs[i] < personpic_ws[i]:
                    wide_nameplates[i] = True
                personpic_ws[i] = personpic_hs[i]
            else:
                personpic_hs[i] = personpic_ws[i]

        # Calculate positions and add full names
        for person_id, person_data in subgraph.nodes(data=True):
            # Get picture size for this vlevel
            personpic_w = personpic_ws[person_data['vlevel']]
            personpic_h = personpic_hs[person_data['vlevel']]
            # Coordinates (image center) depending on vlevel and hlevel
            hlevel_width = int(round(usable_canvas_width / (hlevels[person_data['vlevel']] + 1), 0))    # Reset the hlevel width for this specific vlevel
            person_pic_center_x = int(round(canvas_width * side_border + (person_data['hlevel'] + 0.5) * hlevel_width, 0))
            person_pic_center_y = int(round(canvas_height * top_border + (person_data['vlevel'] + 0.5) * vlevel_height, 0))
            person_pic_topleft_x = int(round(person_pic_center_x - personpic_w / 2, 0))
            person_pic_topleft_y = int(round(person_pic_center_y - personpic_h / 2, 0))
            # Store these data in the graph as well
            subgraph.nodes[person_id]['pic_center'] = (person_pic_center_x, person_pic_center_y)
            subgraph.nodes[person_id]['pic_topleft'] = (person_pic_topleft_x, person_pic_topleft_y)
            # Full names
            person_full_name = (person_data.get('firstname', '') + ' ' + person_data.get('lastname', '')).strip()
            person_data['full_name'] = person_full_name
            person_data['full_name_wrapped'] = person_full_name.replace(' ', '\n')
            # DEBUG
            if verbose:
                print(f"DEBUG: Processing person ID: {person_id}, Name: {person_data.get('firstname','')} {person_data.get('lastname','')}")
                print(f"DEBUG: Person vlevel: {person_data.get('vlevel','')}, hlevel: {person_data.get('hlevel','')}")
                print(f"DEBUG: Person picture center: ({person_pic_center_x}, {person_pic_center_y}), topleft: ({person_pic_topleft_x}, {person_pic_topleft_y}), usable canvas {usable_canvas_width}x{usable_canvas_height}, vlevel height {vlevel_height}, hlevel width {hlevel_width}")

        # Bring spouses closer together
        for person_id, person_data in subgraph.nodes(data=True):
            if 'spouse_position' in person_data:
                continue    # Already processed
            else:
                for neighbor in subgraph.successors(person_id):
                    if subgraph[person_id][neighbor]['type'] == 'isSpouseOf':
                        distance = abs(subgraph.nodes[neighbor]['pic_center'][0] - subgraph.nodes[person_id]['pic_center'][0])
                        new_distance = int(round(distance * spouse_spacing_factor, 0))
                        personpic_w = personpic_ws[subgraph.nodes[person_id]['vlevel']]
                        personpic_h = personpic_hs[subgraph.nodes[person_id]['vlevel']]
                        # Make sure they dont get closer than their picture width
                        if new_distance < personpic_w * picframe_scale_factor:
                            new_distance = personpic_w * picframe_scale_factor
                        # DEBUG
                        if verbose:
                            print(f"DEBUG: Bringing spouses {person_id} and {neighbor} closer together, from distance {distance} to {new_distance}")
                            print(f"DEBUG: Person picture size: {personpic_w}x{personpic_h}")
                            print(f"DEBUG: Original positions were {subgraph.nodes[person_id]['pic_center']} and {subgraph.nodes[neighbor]['pic_center']}")
                        # Mark the relative position of the spouses depending on their hlevel
                        if subgraph.nodes[person_id]['hlevel'] < subgraph.nodes[neighbor]['hlevel']:
                            subgraph.nodes[person_id]['spouse_position'] = 'left'
                            subgraph.nodes[neighbor]['spouse_position'] = 'right'
                            subgraph.nodes[person_id]['pic_center'] = (int(round(subgraph.nodes[person_id]['pic_center'][0] + (distance-new_distance) / 2, 0)), subgraph.nodes[person_id]['pic_center'][1])
                            subgraph.nodes[neighbor]['pic_center'] = (int(round(subgraph.nodes[neighbor]['pic_center'][0] - (distance-new_distance) / 2, 0)), subgraph.nodes[neighbor]['pic_center'][1])
                            subgraph.nodes[person_id]['pic_topleft'] = (int(round(subgraph.nodes[person_id]['pic_topleft'][0] + (distance-new_distance) / 2, 0)), subgraph.nodes[person_id]['pic_topleft'][1])
                            subgraph.nodes[neighbor]['pic_topleft'] = (int(round(subgraph.nodes[neighbor]['pic_topleft'][0] - (distance-new_distance) / 2, 0)), subgraph.nodes[neighbor]['pic_topleft'][1])
                        else:
                            subgraph.nodes[neighbor]['spouse_position'] = 'right'
                            subgraph.nodes[person_id]['spouse_position'] = 'left'
                            subgraph.nodes[neighbor]['pic_center'] = (int(round(subgraph.nodes[neighbor]['pic_center'][0] + (distance-new_distance) / 2, 0)), subgraph.nodes[neighbor]['pic_center'][1])
                            subgraph.nodes[person_id]['pic_center'] = (int(round(subgraph.nodes[person_id]['pic_center'][0] - (distance-new_distance) / 2, 0)), subgraph.nodes[person_id]['pic_center'][1])
                            subgraph.nodes[neighbor]['pic_topleft'] = (int(round(subgraph.nodes[neighbor]['pic_topleft'][0] + (distance-new_distance) / 2, 0)), subgraph.nodes[neighbor]['pic_topleft'][1])
                            subgraph.nodes[person_id]['pic_topleft'] = (int(round(subgraph.nodes[person_id]['pic_topleft'][0] - (distance-new_distance) / 2, 0)), subgraph.nodes[person_id]['pic_topleft'][1])
                        # DEBUG
                        if verbose:
                            print(f"DEBUG: New positions are {subgraph.nodes[person_id]['pic_center']} and {subgraph.nodes[neighbor]['pic_center']}")
                        # Add as well a random offset so that horizontal lines to children are not overlapping with other families
                        random_offset = random.randint(-100, 100)
                        subgraph.nodes[person_id]['offset'] = random_offset
                        subgraph.nodes[neighbor]['offset'] = random_offset
                        # Add the midpoint between the two spouses too
                        spouses_midpoint_x = int(round((subgraph.nodes[person_id]['pic_center'][0] + subgraph.nodes[neighbor]['pic_center'][0]) / 2, 0))
                        subgraph.nodes[person_id]['spouses_midpoint_x'] = spouses_midpoint_x
                        subgraph.nodes[neighbor]['spouses_midpoint_x'] = spouses_midpoint_x

        # For each vlevel, set an additional field on the nodes similar to hlevel but where spouses count as one single position
        for vlevel in range(vlevels[0], vlevels[1] + 1):
            if verbose:
                print(f"DEBUG: Assigning spouse horizontal levels for vertical level {vlevel}")
            persons_in_level = [node for node, data in subgraph.nodes(data=True) if data.get('vlevel') == vlevel]
            if verbose:
                print(f"DEBUG: Persons in vertical level {vlevel}: {persons_in_level}")
            hlevel_spouse = 0
            for hlevel in range(0, hlevels[vlevel] + 1):
                persons_in_hlevel = [node for node in persons_in_level if subgraph.nodes[node].get('hlevel') == hlevel]
                if len(persons_in_hlevel) == 0:
                    print(f"WARNING: No persons found in vlevel {vlevel} and hlevel {hlevel}")
                elif len(persons_in_hlevel) > 1:
                    print(f'WARNING: More than one person found in vlevel {vlevel} and hlevel {hlevel}: {persons_in_hlevel}')
                else:
                    for person in persons_in_hlevel:
                        # Do not increase the hlevel_spouse if the person has a spouse to its right (it will be the same position)
                        if 'spouse_position' in subgraph.nodes[person] and subgraph.nodes[person]['spouse_position'] == 'left':
                            subgraph.nodes[person]['hlevel_spouse'] = hlevel_spouse
                        # If the right spouse or no spouse, increase the hlevel_spouse
                        elif 'spouse_position' in subgraph.nodes[person] and subgraph.nodes[person]['spouse_position'] == 'right':
                            subgraph.nodes[person]['hlevel_spouse'] = hlevel_spouse
                            hlevel_spouse += 1
                        else:
                            subgraph.nodes[person]['hlevel_spouse'] = hlevel_spouse
                            hlevel_spouse += 1

        # Create background
        array = np.zeros([canvas_height, canvas_width, 3], dtype=np.uint8)
        im = Image.fromarray(array)
        d = ImageDraw.Draw(im)
        backgroundpic = Image.open(backgroundfile) 
        backgroundpic = backgroundpic.resize((canvas_width, canvas_height))
        im.paste(backgroundpic,(0,0))

        # Draw the lines between people
        for edge in subgraph.edges(data=True):
            if verbose:
                print(f"DEBUG: Processing edge from {edge[0]} to {edge[1]}, type: {edge[2]['type']}")
            if 'pic_center' in subgraph.nodes[edge[0]] and 'pic_center' in subgraph.nodes[edge[1]]:
                person1_center = subgraph.nodes[edge[0]]['pic_center']
                person2_center = subgraph.nodes[edge[1]]['pic_center']
                if edge[2]['type'] == 'isChildOf':
                    child_center = person1_center
                    parent_center = person2_center
                    # Take color from parent
                    line_color = color_matrix[subgraph.nodes[edge[1]]['hlevel_spouse'] % len(color_matrix)]
                    # Draw line from bottom of parent to top of child
                    parent_top = (parent_center[0], parent_center[1] - int(personpic_h / 2))
                    parent_bottom = (parent_center[0], parent_center[1] + int(personpic_h / 2))
                    parent_center = subgraph.nodes[edge[1]]['pic_center']
                    child_top = (child_center[0], child_center[1] - int(personpic_h / 2))
                    child_bottom = (child_center[0], child_center[1] + int(personpic_h / 2))
                    # If the parent has a spouse, draw the line from the midpoint between the two parents
                    if 'spouses_midpoint_x' in subgraph.nodes[edge[1]]:
                        parent_midpoint_x = subgraph.nodes[edge[1]]['spouses_midpoint_x']
                    else:
                        parent_midpoint_x = parent_center[0]
                    # Vertical distance to go down
                    vdistance = int(round(parent_child_line_length_ratio * (child_center[1] - parent_center[1]), 0))
                    # Offset is a percentage value that can go from -100 to +100, and it is applied to the vertical distance between parents and children
                    if 'offset' in subgraph.nodes[edge[1]]:
                        vdistance = vdistance * (1 + parent_child_line_variability * subgraph.nodes[edge[1]]['offset'] / 100)
                    # d.line([child_bottom, parent_top], fill=(0, 0, 0), width=2)
                    # We need to draw 3 lines:
                    # 1. Vertical line from the midpoint between the two parents centers to the bottom of the level
                    source = (parent_midpoint_x, parent_center[1])
                    destination = (parent_midpoint_x, parent_center[1] + vdistance)
                    d.line([source, destination], fill=line_color, width=2)
                    if verbose:
                        print(f"DEBUG: Drawing isChildOf line 1 from {parent_bottom} to {child_top}: {source} to {destination}")
                    # 2. Horizontal line from the bottom of the level below the parents to the bottom of the level over the child
                    source = destination
                    destination = (child_top[0], parent_center[1] + vdistance)
                    d.line([source, destination], fill=line_color, width=2)
                    if verbose:
                        print(f"DEBUG: Drawing isChildOf line 2 from {parent_bottom} to {child_top}: {source} to {destination}")
                    # 3. Vertical line to the top of the child
                    source = destination
                    destination = child_top
                    d.line([source, destination], fill=line_color, width=2)
                elif edge[2]['type'] == 'isSpouseOf':
                    # Take color from either of the two (should be the same)
                    line_color = color_matrix[subgraph.nodes[edge[1]]['hlevel_spouse'] % len(color_matrix)]
                    # Draw line between the two people at the same vertical level
                    if subgraph.nodes[edge[0]]['vlevel'] == subgraph.nodes[edge[1]]['vlevel']:
                        person1_right = (person1_center[0] + int(personpic_w / 2), person1_center[1])
                        person2_left = (person2_center[0] - int(personpic_w / 2), person2_center[1])
                        d.line([person1_right, person2_left], fill=line_color, width=3)
                        if verbose:
                            print(f"DEBUG: Drawing isSpouseOf line from {person1_right} to {person2_left}")
                    else:
                        if verbose:
                            print(f"DEBUG: Skipping isSpouseOf line between different vlevels: {subgraph.nodes[edge[0]]['vlevel']} and {subgraph.nodes[edge[1]]['vlevel']}")
            else:
                if verbose:
                    print(f"DEBUG: Missing pic_center for one of the nodes in edge from {edge[0]} to {edge[1]}")

        def get_left_person(person_id, graph):
            person_vlevel = graph.nodes[person_id]['vlevel']
            person_hlevel = graph.nodes[person_id]['hlevel']
            left_person = None
            for node, data in graph.nodes(data=True):
                if data.get('vlevel') == person_vlevel and data.get('hlevel') == person_hlevel - 1:
                    left_person = node
                    break
            return left_person

        def get_right_person(person_id, graph):
            person_vlevel = graph.nodes[person_id]['vlevel']
            person_hlevel = graph.nodes[person_id]['hlevel']
            right_person = None
            for node, data in graph.nodes(data=True):
                if data.get('vlevel') == person_vlevel and data.get('hlevel') == person_hlevel + 1:
                    right_person = node
                    break
            return right_person

        def get_hdistance(person1, person2, graph):
            if person1 and person2:
                if graph.nodes[person1] and graph.nodes[person2]:
                    if 'pic_center' in graph.nodes[person1] and 'pic_center' in graph.nodes[person2]:
                        return abs(graph.nodes[person1]['pic_center'][0] - graph.nodes[person2]['pic_center'][0])
            return None

        # Loop through people and load their photo and resize picture and place where it goes
        for person_id, person_data in subgraph.nodes(data=True):
            # Get picture size for this vlevel
            personpic_w = personpic_ws[person_data['vlevel']]
            personpic_h = personpic_hs[person_data['vlevel']]
            # Mask for image and plate
            mask_im = Image.new("L", (personpic_w, personpic_h), 0)
            draw = ImageDraw.Draw(mask_im)
            draw.ellipse((0, 0, personpic_w, personpic_h), fill=255)
            mask_implate = Image.new("L", (int(personpic_w*picframe_scale_factor), int(personpic_h*picframe_scale_factor)), 0)
            draw = ImageDraw.Draw(mask_implate)
            draw.ellipse((0, 0, int(personpic_w*picframe_scale_factor), int(personpic_h*picframe_scale_factor)), fill=255)
            # Coordinates (image center) depending on vlevel and hlevel
            person_pic_center_x = subgraph.nodes[person_id]['pic_center'][0]
            person_pic_center_y = subgraph.nodes[person_id]['pic_center'][1]
            person_pic_topleft_x = subgraph.nodes[person_id]['pic_topleft'][0]
            person_pic_topleft_y = subgraph.nodes[person_id]['pic_topleft'][1]
            # Load profile picture if available, otherwise load default person.png
            if 'profilepic' in person_data and azure_storage_sas:
                pic_url = person_data['profilepic'] + "?"+ azure_storage_sas
                pic_file = os.path.join(tempdir.name, os.path.basename(person_data['profilepic']))
                # Don't show the SAS token in verbose/debug mode
                # if verbose:
                #     print(f"DEBUG: Downloading profile picture from {pic_url} to {pic_file}...")
                # urllib.request.urlretrieve(pic_url, os.path.join(tempdir.name, os.path.basename(person_data['profilepic'])))
                response = requests.get(pic_url)
                with open(pic_file, "wb") as file:
                    file.write(response.content)
                    file.close()
                if verbose:
                    print(f"DEBUG: Profile picture saved to {pic_file}, opening now...")
                # If file exists, open it
                if os.path.exists(os.path.join(tempdir.name, os.path.basename(person_data['profilepic']))):
                    personpic = Image.open(os.path.join(tempdir.name, os.path.basename(person_data['profilepic'])))
                else:
                    if verbose:
                        print(f"DEBUG: File {os.path.join(tempdir.name, os.path.basename(person_data['profilepic']))} does not exist, using default person.png")
                    personpic = Image.open(os.path.join(root_folder, "person.png"))
            else:
                personpic = Image.open(os.path.join(root_folder, "person.png"))
            # Resize picture
            personpic = personpic.resize((personpic_w, personpic_h))
            # Add picture frame
            picframe = Image.open(os.path.join(root_folder, "picframe.png"))
            picframe_w = int(personpic_w * picframe_scale_factor)
            picframe_h = int(personpic_h * picframe_scale_factor)
            picframe_x = person_pic_center_x - int(picframe_w / 2)
            picframe_y = person_pic_center_y - int(picframe_h / 2)
            picframe = picframe.resize((picframe_w, picframe_h))
            im.paste(picframe, (picframe_x, picframe_y), mask_implate)
            # Add picture
            im.paste(personpic, (person_pic_topleft_x, person_pic_topleft_y), mask_im)

            # Add nameplate (can be wide or narrow/wrapped)
            # Only add nameplate if there is enough space in the vlevel, and if it is not a spouse couple (since they are closer together)
            use_wide_nameplate = False
            if wide_nameplates[person_data['vlevel']] and not 'spouse_position' in person_data:
                use_wide_nameplate = True
            # If the distance between this person and the next ones (right/left) in the same vlevel is smaller than 1.5 times the picture width, use wrapped nameplate
            left_person_id = get_left_person(person_id, subgraph)
            right_person_id = get_right_person(person_id, subgraph)
            left_hdistance = get_hdistance(person_id, left_person_id, subgraph)
            right_hdistance = get_hdistance(person_id, right_person_id, subgraph)
            closest_hdistance = min([d for d in [left_hdistance, right_hdistance] if d is not None], default=None)
            if closest_hdistance is not None and closest_hdistance > 1.5 * personpic_w:
                use_wide_nameplate = True
            # Common variables for both wide and wrapped nameplates
            textframe = Image.open(os.path.join(root_folder, "textframe.png"))
            scale_down_ratio = 1.0
            # Coordinates depending on wide/narrow nameplate
            use_wide_nameplate = True       # Override for testing
            if use_wide_nameplate:
                use_full_wide_nametemplate = False          # Override for testing
                if use_full_wide_nametemplate:
                    text = person_data['full_name']
                    num_of_chars = len(text)
                    textframe_w = int(personpic_w * (1 + aframewratio) * num_of_chars / char_textframe_ratio)
                    textframe_h = int(personpic_h * text_vert_fraction + personpic_h * aframehratio)
                    # Scale down the text box if it is too wide for the hlevel
                    if hlevels_spacing_factor * textframe_w > hlevel_width - personpic_w:
                        scale_down_ratio = hlevels_spacing_factor * (hlevel_width - personpic_w) / textframe_w  # The 0.9 is to leave a bit of margin
                        textframe_w = int(textframe_w * scale_down_ratio)
                        textframe_h = int(textframe_h * scale_down_ratio)
                    textframe_x = person_pic_center_x - int(textframe_w / 2)
                    textframe_y = person_pic_topleft_y + personpic_h
                    # textframe_y = person_pic_topleft_y + personpic_h - int(personpic_h * aframehratio / 2)
                    # Person name text
                    # d = ImageDraw.Draw(im)
                    text_x = person_pic_center_x
                    text_y = textframe_y + int(textframe_h / 2)
                    # text_y = person_pic_topleft_y + text_vert_offset * personpic_h + personpic_h*text_vert_fraction
                    fontsize = int(personpic_h * text_vert_fraction * scale_down_ratio)
                else:
                    # This is the preferred schema, "partially wide nameplates"
                    # Use a 2-line text, firstname + \n + lastname
                    text = ''
                    if 'firstname' in person_data and len(person_data['firstname']) > 0:
                        text += person_data['firstname']
                    if 'lastname' in person_data and len(person_data['lastname']) > 0:
                        if len(text) > 0:
                            text += '\n'
                        text += person_data['lastname']
                    # Just in case there is no firstname or lastname, use 1 line
                    if 'firstname' in person_data and 'lastname' in person_data and len(person_data['firstname']) > 0 and len(person_data['lastname']) > 0:
                        num_of_lines = 2
                    else:
                        num_of_lines = 1
                    num_of_chars = max(len(person_data['firstname']), len(person_data['lastname']))
                    textframe_w = int(personpic_w * (1 + aframewratio) * num_of_chars / char_textframe_ratio)
                    # textframe_w = int(personpic_w * (1 + aframewratio))
                    textframe_h = int((personpic_h * text_vert_fraction + personpic_h * aframehratio) * num_of_lines)
                    # Scale down the text box if it is too high for the vlevel
                    vscale_down_ratio = 1.0
                    if textframe_h > (vlevel_height - personpic_h):
                        vscale_down_ratio = vlevels_spacing_factor * (vlevel_height - personpic_h) / textframe_h
                        textframe_w = int(textframe_w * vscale_down_ratio)
                        textframe_h = int(textframe_h * vscale_down_ratio)
                    hscale_down_ratio = 1.0
                    # Scale down the text box if it is too wide for the hlevel
                    if textframe_w > (hlevel_width - personpic_w) * hlevels_spacing_factor:
                        hscale_down_ratio = hlevels_spacing_factor * (hlevel_width - personpic_w) / textframe_w  # The 0.9 is to leave a bit of margin
                        textframe_w = int(textframe_w * hscale_down_ratio)
                        textframe_h = int(textframe_h * scale_down_ratio)
                    # Calculate the size of the text frame and the text
                    textframe_x = person_pic_center_x - int(textframe_w / 2)
                    textframe_y = person_pic_topleft_y + personpic_h
                    text_x = person_pic_center_x
                    text_y = textframe_y + int(textframe_h / 2)
                    fontsize = int(personpic_h * text_vert_fraction * vscale_down_ratio * hscale_down_ratio)
            else:
                text = person_data['full_name_wrapped']
                num_of_lines = text.count('\n') + 1
                textframe_w = int(personpic_w * (1 + aframewratio))
                textframe_h = int((personpic_h * text_vert_fraction + personpic_h * aframehratio) * num_of_lines)
                # Scale down the text box if it is too high for the vlevel
                scale_down_ratio = 1.0
                if textframe_h * hlevels_spacing_factor > vlevel_height - personpic_h:
                    scale_down_ratio = hlevels_spacing_factor * (vlevel_height - personpic_h) / textframe_h
                    textframe_w = int(textframe_w * scale_down_ratio)
                    textframe_h = int(textframe_h * scale_down_ratio)
                textframe_x = person_pic_center_x - int(textframe_w / 2)
                textframe_y = person_pic_topleft_y + personpic_h
                # textframe_y = person_pic_topleft_y + personpic_h - int(personpic_h * aframehratio / 2)
                # Person name text
                # d = ImageDraw.Draw(im)
                text_x = person_pic_center_x
                text_y = textframe_y + int(textframe_h / 2)
                # text_y = person_pic_topleft_y + text_vert_offset * personpic_h + personpic_h*text_vert_fraction
                fontsize = int(personpic_h * text_vert_fraction * scale_down_ratio)
            textframe = textframe.resize((textframe_w, textframe_h))
            im.paste(textframe, (textframe_x, textframe_y), textframe)
            d.multiline_text(
                (int(text_x),
                int(text_y)
                ),
                text, 
                font=ImageFont.truetype(fontfile, fontsize), 
                align='center', 
                anchor='mm', 
                fill=(240, 240, 240)
            )
            # Close picture
            personpic.close()

        # Finish up
        # im.show()
        im.save(os.path.join(image_path, image_filename))
        im.close()
        return im

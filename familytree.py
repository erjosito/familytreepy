import networkx as nx
import uuid
import os
import json
from gremlin_python.driver import client, serializer
from azure.storage.blob import BlobServiceClient


class FamilyTree:
    # Backends can be local, azstorage, cosmosdb
    # If local is specified, the following parameter must be provided: localfile
    # If azstorage is specified, the following parameters must be provided: azstorage, azstoragekey
    # If cosmosdb is specified, the following parameters must be provided: cosmosdbhost, cosmosdbkey
    def __init__(self, backend='local', localfile=None, azstorage_account=None, azstorage_key=None, cosmosdb_host=None, cosmosdb_db=None, cosmosdb_collection=None, cosmosdb_key=None, autosave=True):
        self.backend = backend
        self.localfile = localfile
        self.azstorage_account = azstorage_account
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
            self.load_azstorage()
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
        if self.localfile and self.tempfile:
            try:
                nx.write_gml(self.graph, self.tempfile)
            except Exception as e:
                print(f"Error saving graph to temporary file: {e}")
            finally:
                # Move the temporary file to the original location
                if os.path.exists(self.tempfile):
                    os.replace(self.tempfile, self.localfile)
        else:
            raise ValueError("Local file must be specified to save data when using backend=local")
    def save_azstorage(self):
        # Save the graph to Azure Storage
        pass
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
                        blob_client.upload_blob(data)
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
            self.save_local()
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
    def format_for_st_link_analysis(self):
        nodes = []
        edges = []
        for person_id, person_data in self.graph.nodes(data=True):
            person = person_data
            person["id"] = person_id
            person["label"] = "person"
            nodes.append({"data": person})
        for source, target, edge_data in self.graph.edges(data=True):
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

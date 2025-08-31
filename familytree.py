import networkx as nx
import uuid
import os
from gremlin_python.driver import client, serializer

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
        if self.localfile:
            nx.write_gml(self.graph, self.localfile)
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
    #     Add     #
    ###############
    def add_person(self, **attributes):
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
    #    Delete   #
    ###############
    def delete_person(self, person_id):
        if person_id in self.graph:
            self.graph.remove_node(person_id)
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

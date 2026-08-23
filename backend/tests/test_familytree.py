"""Unit tests for the FamilyTree class."""

import os
import tempfile
from types import SimpleNamespace

import networkx as nx
import pytest
from azure.core import MatchConditions

import familytree as familytree_module
from familytree import ConcurrentWriteError, FamilyTree
from backend.app.schemas.relationship_schema import load_relationship_schema
from tree_validation import TreeValidationError


@pytest.fixture()
def schema():
    config_path = os.path.join(os.path.dirname(__file__), "..", "..", "config", "relationship_types.json")
    return load_relationship_schema(config_path)


@pytest.fixture()
def tree(tmp_path, schema):
    """Create a FamilyTree backed by a temp file, cleaned up automatically."""
    local_file = str(tmp_path / "test_tree.gml")
    return FamilyTree(backend="local", localfile=local_file, relationship_schema=schema, autosave=False)


def test_azure_save_uses_loaded_etag(monkeypatch, schema):
    initial = nx.DiGraph()
    initial.add_node("person-1", firstname="Initial")
    content = ("\n".join(nx.generate_gml(initial)) + "\n").encode()

    class Downloader:
        properties = SimpleNamespace(etag='"etag-1"')

        def readall(self):
            return content

    class BlobClient:
        def __init__(self):
            self.upload_options = None

        def download_blob(self):
            return Downloader()

        def upload_blob(self, _data, **options):
            self.upload_options = options
            return {"etag": '"etag-2"'}

    blob_client = BlobClient()
    service = SimpleNamespace(
        get_blob_client=lambda **_kwargs: blob_client,
    )
    monkeypatch.setattr(
        familytree_module.BlobServiceClient,
        "from_connection_string",
        lambda _connection_string: service,
    )
    azure_tree = FamilyTree(
        backend="azstorage",
        azstorage_account="account",
        azstorage_key="key",
        azstorage_container="container",
        azstorage_blob="tree.gml",
        relationship_schema=schema,
        autosave=False,
    )

    azure_tree.graph.nodes["person-1"]["firstname"] = "Updated"
    azure_tree.save()

    assert blob_client.upload_options["etag"] == '"etag-1"'
    assert blob_client.upload_options["match_condition"] == MatchConditions.IfNotModified
    assert azure_tree._storage_etag == '"etag-2"'


def test_azure_save_surfaces_concurrent_write(monkeypatch, schema):
    initial = nx.DiGraph()
    initial.add_node("person-1")
    content = ("\n".join(nx.generate_gml(initial)) + "\n").encode()

    class Downloader:
        properties = SimpleNamespace(etag='"etag-1"')

        def readall(self):
            return content

    class BlobClient:
        def download_blob(self):
            return Downloader()

        def upload_blob(self, _data, **_options):
            raise familytree_module.ResourceModifiedError("stale")

    service = SimpleNamespace(get_blob_client=lambda **_kwargs: BlobClient())
    monkeypatch.setattr(
        familytree_module.BlobServiceClient,
        "from_connection_string",
        lambda _connection_string: service,
    )
    azure_tree = FamilyTree(
        backend="azstorage",
        azstorage_account="account",
        azstorage_key="key",
        azstorage_container="container",
        azstorage_blob="tree.gml",
        relationship_schema=schema,
        autosave=False,
    )

    with pytest.raises(ConcurrentWriteError):
        azure_tree.save()


def test_azure_load_failure_never_initializes_empty_tree(monkeypatch, schema):
    uploads = []

    class BlobClient:
        def download_blob(self):
            raise OSError("temporary storage failure")

        def upload_blob(self, *_args, **_kwargs):
            uploads.append(True)

    service = SimpleNamespace(get_blob_client=lambda **_kwargs: BlobClient())
    monkeypatch.setattr(
        familytree_module.BlobServiceClient,
        "from_connection_string",
        lambda _connection_string: service,
    )

    with pytest.raises(OSError, match="temporary storage failure"):
        FamilyTree(
            backend="azstorage",
            azstorage_account="account",
            azstorage_key="key",
            azstorage_container="container",
            azstorage_blob="tree.gml",
            relationship_schema=schema,
            autosave=False,
        )

    assert uploads == []


# ------------------------------------------------------------------
# Adding persons
# ------------------------------------------------------------------

class TestAddPerson:
    def test_add_person_returns_id(self, tree):
        pid = tree.add_person(firstname="Alice", lastname="Smith")
        assert pid is not None
        assert len(pid) > 0

    def test_add_person_with_custom_id(self, tree):
        pid = tree.add_person(id="custom-1", firstname="Bob", lastname="Jones")
        assert pid == "custom-1"

    def test_person_attributes_stored(self, tree):
        pid = tree.add_person(firstname="Carol", lastname="White", birthdate="1990-05-10")
        person = tree.get_person(pid)
        assert person["firstname"] == "Carol"
        assert person["lastname"] == "White"
        assert person["birthdate"] == "1990-05-10"

    def test_invalid_date_is_rejected(self, tree):
        with pytest.raises(TreeValidationError) as exc:
            tree.add_person(firstname="Invalid", birthdate="31/02/2000")
        assert exc.value.issues[0].code == "invalid_date"

    def test_birth_after_death_requires_explicit_override(self, tree):
        with pytest.raises(TreeValidationError) as exc:
            tree.add_person(
                firstname="Historically",
                birthdate="2000",
                deathdate="1900",
                isAlive=False,
            )
        assert exc.value.issues[0].severity == "warning"

        person_id = tree.add_person(
            firstname="Historically",
            birthdate="2000",
            deathdate="1900",
            isAlive=False,
            override_warnings=True,
        )
        assert tree.get_person(person_id)["deathdate"] == "1900"


# ------------------------------------------------------------------
# Adding relationships
# ------------------------------------------------------------------

class TestAddRelationship:
    def test_add_child_of(self, tree):
        parent = tree.add_person(firstname="Parent", lastname="One")
        child = tree.add_person(firstname="Child", lastname="One")
        tree.add_relationship(child, parent, type="isChildOf")
        rels = tree.get_relationships(child)
        assert any(r["type"] == "isChildOf" and r["target"] == parent for r in rels)

    def test_add_spouse_of(self, tree):
        p1 = tree.add_person(firstname="Spouse", lastname="A")
        p2 = tree.add_person(firstname="Spouse", lastname="B")
        tree.add_relationship(p1, p2, type="isSpouseOf")
        rels = tree.get_relationships(p1)
        assert any(r["type"] == "isSpouseOf" for r in rels)

    def test_invalid_relationship_type_raises(self, tree):
        p1 = tree.add_person(firstname="A", lastname="A")
        p2 = tree.add_person(firstname="B", lastname="B")
        with pytest.raises(ValueError, match="Invalid relationship type"):
            tree.add_relationship(p1, p2, type="isFriendOf")

    def test_self_relationship_is_rejected(self, tree):
        person = tree.add_person(firstname="Self")
        with pytest.raises(TreeValidationError) as exc:
            tree.add_relationship(person, person, type="isSpouseOf")
        assert exc.value.issues[0].code == "self_relationship"

    def test_duplicate_relationship_is_rejected(self, tree):
        p1 = tree.add_person(firstname="A")
        p2 = tree.add_person(firstname="B")
        tree.add_relationship(p1, p2, type="isSpouseOf")
        with pytest.raises(TreeValidationError) as exc:
            tree.add_relationship(p1, p2, type="isSpouseOf")
        assert exc.value.issues[0].code == "duplicate_relationship"

    def test_parent_child_cycle_is_rejected(self, tree):
        grandchild = tree.add_person(firstname="Grandchild")
        parent = tree.add_person(firstname="Parent")
        grandparent = tree.add_person(firstname="Grandparent")
        tree.add_relationship(grandchild, parent, type="isChildOf")
        tree.add_relationship(parent, grandparent, type="isChildOf")

        with pytest.raises(TreeValidationError) as exc:
            tree.add_relationship(grandparent, grandchild, type="isChildOf")
        assert any(issue.code == "parent_child_cycle" for issue in exc.value.issues)

    def test_implausible_parent_age_can_be_overridden(self, tree):
        child = tree.add_person(firstname="Child", birthdate="2000")
        parent = tree.add_person(firstname="Parent", birthdate="1995")
        with pytest.raises(TreeValidationError) as exc:
            tree.add_relationship(child, parent, type="isChildOf")
        assert any(issue.code == "parent_too_young" for issue in exc.value.issues)

        tree.add_relationship(
            child,
            parent,
            type="isChildOf",
            override_warnings=True,
        )
        assert tree.graph.has_edge(child, parent)

    def test_relationship_event_outside_lifetime_warns(self, tree):
        deceased = tree.add_person(
            firstname="Deceased",
            birthdate="1900",
            deathdate="1980",
            isAlive=False,
        )
        spouse = tree.add_person(firstname="Spouse", birthdate="1900")
        with pytest.raises(TreeValidationError) as exc:
            tree.add_relationship(
                deceased,
                spouse,
                type="isSpouseOf",
                start_date="1990",
            )
        assert any(issue.code == "event_after_death" for issue in exc.value.issues)

        tree.add_relationship(
            deceased,
            spouse,
            type="isSpouseOf",
            start_date="1990",
            override_warnings=True,
        )

    def test_relationship_end_before_start_warns(self, tree):
        p1 = tree.add_person(firstname="A")
        p2 = tree.add_person(firstname="B")
        tree.add_relationship(p1, p2, type="isSpouseOf", start_date="2000")
        with pytest.raises(TreeValidationError) as exc:
            tree.deactivate_relationship(p1, p2, end_date="1990")
        assert any(
            issue.code == "relationship_end_before_start"
            for issue in exc.value.issues
        )
        assert tree.graph[p1][p2]["is_active"] is True


class TestUpdatePerson:
    def test_warning_does_not_mutate_without_override(self, tree):
        person = tree.add_person(
            firstname="Person",
            birthdate="1900",
            deathdate="1980",
            isAlive=False,
        )
        with pytest.raises(TreeValidationError):
            tree.update_person(person, birthdate="2000")
        assert tree.get_person(person)["birthdate"] == "1900"

        tree.update_person(person, birthdate="2000", override_warnings=True)
        assert tree.get_person(person)["birthdate"] == "2000"

    def test_related_chronology_is_rechecked(self, tree):
        child = tree.add_person(firstname="Child", birthdate="2000")
        parent = tree.add_person(firstname="Parent", birthdate="1970")
        tree.add_relationship(child, parent, type="isChildOf")

        with pytest.raises(TreeValidationError) as exc:
            tree.update_person(parent, birthdate="1995")
        assert any(issue.code == "parent_too_young" for issue in exc.value.issues)
        assert tree.get_person(parent)["birthdate"] == "1970"


# ------------------------------------------------------------------
# get_relationships with include_inactive
# ------------------------------------------------------------------

class TestGetRelationships:
    def test_active_only_by_default(self, tree):
        p1 = tree.add_person(firstname="X", lastname="X")
        p2 = tree.add_person(firstname="Y", lastname="Y")
        tree.add_relationship(p1, p2, type="isSpouseOf")
        tree.deactivate_relationship(p1, p2)
        rels = tree.get_relationships(p1, include_inactive=False)
        assert len(rels) == 0

    def test_include_inactive(self, tree):
        p1 = tree.add_person(firstname="X", lastname="X")
        p2 = tree.add_person(firstname="Y", lastname="Y")
        tree.add_relationship(p1, p2, type="isSpouseOf")
        tree.deactivate_relationship(p1, p2)
        rels = tree.get_relationships(p1, include_inactive=True)
        assert len(rels) >= 1


# ------------------------------------------------------------------
# deactivate_relationship
# ------------------------------------------------------------------

class TestDeactivateRelationship:
    def test_deactivate_sets_inactive_and_end_date(self, tree):
        p1 = tree.add_person(firstname="A", lastname="A")
        p2 = tree.add_person(firstname="B", lastname="B")
        tree.add_relationship(p1, p2, type="isSpouseOf")
        tree.deactivate_relationship(p1, p2, end_date="2024-01-01")
        edge = tree.graph[p1][p2]
        assert edge["is_active"] is False
        assert edge["end_date"] == "2024-01-01"

    def test_cannot_deactivate_permanent(self, tree):
        parent = tree.add_person(firstname="P", lastname="P")
        child = tree.add_person(firstname="C", lastname="C")
        tree.add_relationship(child, parent, type="isChildOf")
        with pytest.raises(ValueError, match="Cannot deactivate permanent"):
            tree.deactivate_relationship(child, parent)


# ------------------------------------------------------------------
# get_siblings
# ------------------------------------------------------------------

class TestGetSiblings:
    def test_siblings_inferred_from_shared_parent(self, tree):
        parent = tree.add_person(firstname="Parent", lastname="P")
        child1 = tree.add_person(firstname="Child", lastname="One")
        child2 = tree.add_person(firstname="Child", lastname="Two")
        tree.add_relationship(child1, parent, type="isChildOf")
        tree.add_relationship(child2, parent, type="isChildOf")
        siblings = tree.get_siblings(child1)
        assert child2 in siblings
        assert child1 not in siblings

    def test_no_siblings_when_only_child(self, tree):
        parent = tree.add_person(firstname="Parent", lastname="P")
        child = tree.add_person(firstname="Only", lastname="Child")
        tree.add_relationship(child, parent, type="isChildOf")
        assert tree.get_siblings(child) == []


# ------------------------------------------------------------------
# format_for_api
# ------------------------------------------------------------------

class TestFormatForApi:
    def test_returns_nodes_and_edges(self, tree):
        p1 = tree.add_person(firstname="One", lastname="A")
        p2 = tree.add_person(firstname="Two", lastname="B")
        tree.add_relationship(p1, p2, type="isSpouseOf")
        result = tree.format_for_api()
        assert "nodes" in result
        assert "edges" in result
        assert len(result["nodes"]) == 2
        assert len(result["edges"]) >= 1

    def test_node_has_id_and_fullname(self, tree):
        pid = tree.add_person(firstname="Jane", lastname="Doe")
        result = tree.format_for_api()
        node = next(n for n in result["nodes"] if n["id"] == pid)
        assert node["fullname"] == "Jane Doe"

    def test_assigns_generation_levels_to_disconnected_families(self, tree):
        parent1 = tree.add_person(firstname="Parent", lastname="One")
        child1 = tree.add_person(firstname="Child", lastname="One")
        parent2 = tree.add_person(firstname="Parent", lastname="Two")
        child2 = tree.add_person(firstname="Child", lastname="Two")
        tree.add_relationship(child1, parent1, type="isChildOf")
        tree.add_relationship(child2, parent2, type="isChildOf")

        nodes = {node["id"]: node for node in tree.format_for_api()["nodes"]}

        assert all("level" in node for node in nodes.values())
        assert nodes[parent1]["level"] < nodes[child1]["level"]
        assert nodes[parent2]["level"] < nodes[child2]["level"]

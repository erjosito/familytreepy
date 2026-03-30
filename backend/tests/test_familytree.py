"""Unit tests for the FamilyTree class."""

import os
import tempfile

import pytest

from familytree import FamilyTree
from backend.app.schemas.relationship_schema import load_relationship_schema


@pytest.fixture()
def schema():
    config_path = os.path.join(os.path.dirname(__file__), "..", "..", "config", "relationship_types.json")
    return load_relationship_schema(config_path)


@pytest.fixture()
def tree(tmp_path, schema):
    """Create a FamilyTree backed by a temp file, cleaned up automatically."""
    local_file = str(tmp_path / "test_tree.gml")
    return FamilyTree(backend="local", localfile=local_file, relationship_schema=schema, autosave=False)


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

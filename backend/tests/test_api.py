"""Integration tests for FastAPI endpoints."""

import os
import sys
import tempfile

import pytest
from fastapi.testclient import TestClient

# Ensure project root is importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from familytree import FamilyTree
from backend.app.schemas.relationship_schema import load_relationship_schema
from backend.app.dependencies import get_tree
from backend.app.main import app


@pytest.fixture(autouse=True)
def _override_tree(tmp_path):
    """Override the get_tree dependency with a temp-file-backed FamilyTree."""
    config_path = os.path.join(os.path.dirname(__file__), "..", "..", "config", "relationship_types.json")
    schema = load_relationship_schema(config_path)
    local_file = str(tmp_path / "test_api_tree.gml")
    test_tree = FamilyTree(backend="local", localfile=local_file, relationship_schema=schema, autosave=False)
    app.dependency_overrides[get_tree] = lambda: test_tree
    yield test_tree
    app.dependency_overrides.pop(get_tree, None)


@pytest.fixture()
def client():
    return TestClient(app)


# ------------------------------------------------------------------
# Health
# ------------------------------------------------------------------

def test_health(client, monkeypatch):
    monkeypatch.setenv("APP_REVISION", "test-revision")
    monkeypatch.setenv("APP_BUILD_TIME", "2026-08-19T12:00:00Z")

    resp = client.get("/api/health")
    assert resp.status_code == 200
    assert resp.json() == {
        "status": "ok",
        "version": "0.5.0",
        "revision": "test-revision",
        "built_at": "2026-08-19T12:00:00Z",
    }


# ------------------------------------------------------------------
# Persons CRUD
# ------------------------------------------------------------------

def test_create_person(client):
    resp = client.post("/api/persons", json={"firstname": "Alice", "lastname": "Test"})
    assert resp.status_code == 201
    data = resp.json()
    assert "id" in data


def test_list_persons(client):
    client.post("/api/persons", json={"firstname": "One", "lastname": "Person", "alias": "First"})
    resp = client.get("/api/persons")
    assert resp.status_code == 200
    persons = resp.json()
    assert len(persons) >= 1
    assert any(p["fullname"] == "One Person" and p["alias"] == "First" for p in persons)


def test_get_person(client):
    create_resp = client.post("/api/persons", json={"firstname": "Detail", "lastname": "Person"})
    pid = create_resp.json()["id"]
    resp = client.get(f"/api/persons/{pid}")
    assert resp.status_code == 200
    data = resp.json()
    assert data["id"] == pid
    assert data["fullname"] == "Detail Person"


def test_get_person_not_found(client):
    resp = client.get("/api/persons/nonexistent-id")
    assert resp.status_code == 404


def test_update_person(client):
    create_resp = client.post("/api/persons", json={"firstname": "Old", "lastname": "Name"})
    pid = create_resp.json()["id"]
    resp = client.put(f"/api/persons/{pid}", json={"firstname": "New"})
    assert resp.status_code == 200
    assert resp.json()["updated"] is True
    detail = client.get(f"/api/persons/{pid}").json()
    assert detail["firstname"] == "New"


def test_delete_person(client):
    create_resp = client.post("/api/persons", json={"firstname": "Del", "lastname": "Me"})
    pid = create_resp.json()["id"]
    resp = client.delete(f"/api/persons/{pid}")
    assert resp.status_code == 200
    assert resp.json()["deleted"] is True
    assert client.get(f"/api/persons/{pid}").status_code == 404


# ------------------------------------------------------------------
# Relationships
# ------------------------------------------------------------------

def _create_two_persons(client):
    p1 = client.post("/api/persons", json={"firstname": "Rel", "lastname": "A"}).json()["id"]
    p2 = client.post("/api/persons", json={"firstname": "Rel", "lastname": "B"}).json()["id"]
    return p1, p2


def test_create_relationship(client):
    p1, p2 = _create_two_persons(client)
    resp = client.post("/api/relationships", json={"source": p1, "target": p2, "type": "isSpouseOf"})
    assert resp.status_code == 201
    assert resp.json()["created"] is True


def test_deactivate_relationship(client):
    p1, p2 = _create_two_persons(client)
    client.post("/api/relationships", json={"source": p1, "target": p2, "type": "isSpouseOf"})
    resp = client.put(f"/api/relationships/{p1}/{p2}/deactivate", json={"end_date": "2024-06-01"})
    assert resp.status_code == 200
    assert resp.json()["deactivated"] is True


# ------------------------------------------------------------------
# Graph
# ------------------------------------------------------------------

def test_get_graph(client):
    p1, p2 = _create_two_persons(client)
    client.post("/api/relationships", json={"source": p1, "target": p2, "type": "isSpouseOf"})
    resp = client.get("/api/graph")
    assert resp.status_code == 200
    data = resp.json()
    assert "nodes" in data
    assert "edges" in data
    assert len(data["nodes"]) >= 2


# ------------------------------------------------------------------
# Schema endpoints
# ------------------------------------------------------------------

def test_person_schema(client):
    resp = client.get("/api/schema/person")
    assert resp.status_code == 200
    data = resp.json()
    assert "firstname" in data


def test_relationship_schema(client):
    resp = client.get("/api/schema/relationships")
    assert resp.status_code == 200
    data = resp.json()
    assert "isChildOf" in data
    assert "isSpouseOf" in data


# ------------------------------------------------------------------
# Renderers
# ------------------------------------------------------------------

def test_renderers_list(client):
    resp = client.get("/api/renderers")
    assert resp.status_code == 200
    renderers = resp.json()
    names = [r["name"] for r in renderers]
    assert "classical_tree" in names
    assert "radial_ancestors" in names

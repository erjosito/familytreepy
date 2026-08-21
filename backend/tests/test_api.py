"""Integration tests for FastAPI endpoints."""

import os
import sys
import tempfile

import pytest
from fastapi.testclient import TestClient

# Ensure project root is importable
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from familytree import FamilyTree
from backend.app.auth import require_auth
from backend.app.change_history import ChangeHistoryStore
from backend.app.schemas.relationship_schema import load_relationship_schema
from backend.app.dependencies import get_history_store, get_tree
from backend.app.main import app


@pytest.fixture(autouse=True)
def _override_tree(tmp_path):
    """Override the get_tree dependency with a temp-file-backed FamilyTree."""
    config_path = os.path.join(os.path.dirname(__file__), "..", "..", "config", "relationship_types.json")
    schema = load_relationship_schema(config_path)
    local_file = str(tmp_path / "test_api_tree.gml")
    test_tree = FamilyTree(backend="local", localfile=local_file, relationship_schema=schema, autosave=False)
    history_store = ChangeHistoryStore(
        backend="local",
        local_file=str(tmp_path / "test_api_history.jsonl"),
    )
    app.dependency_overrides[get_tree] = lambda: test_tree
    app.dependency_overrides[get_history_store] = lambda: history_store
    yield test_tree
    app.dependency_overrides.pop(get_tree, None)
    app.dependency_overrides.pop(get_history_store, None)


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
        "version": "0.7.0",
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


def test_person_chronology_warning_is_structured_and_overridable(client):
    create_resp = client.post(
        "/api/persons",
        json={
            "firstname": "Chronology",
            "birthdate": "2000",
            "deathdate": "1900",
            "isAlive": False,
        },
    )
    assert create_resp.status_code == 422
    detail = create_resp.json()["detail"]
    assert detail["code"] == "validation_warning"
    assert detail["issues"][0]["code"] == "birth_after_death"
    assert client.get("/api/persons").json() == []

    override_resp = client.post(
        "/api/persons",
        json={
            "firstname": "Chronology",
            "birthdate": "2000",
            "deathdate": "1900",
            "isAlive": False,
            "override_warnings": True,
        },
    )
    assert override_resp.status_code == 201


def test_invalid_person_date_identifies_field(client):
    resp = client.post(
        "/api/persons",
        json={"firstname": "Invalid", "birthdate": "not-a-date"},
    )
    assert resp.status_code == 422
    issue = resp.json()["detail"]["issues"][0]
    assert issue["severity"] == "error"
    assert issue["field"] == "birthdate"


def test_update_rechecks_related_chronology_without_mutating(client):
    parent = client.post(
        "/api/persons",
        json={"firstname": "Parent", "birthdate": "1970"},
    ).json()["id"]
    child = client.post(
        "/api/persons",
        json={
            "firstname": "Child",
            "birthdate": "2000",
            "relationships": [
                {
                    "related_person_id": parent,
                    "type": "isChildOf",
                    "new_person_role": "source",
                }
            ],
        },
    ).json()["id"]

    warning = client.put(
        f"/api/persons/{parent}",
        json={"birthdate": "1995"},
    )
    assert warning.status_code == 422
    assert any(
        issue["code"] == "parent_too_young"
        for issue in warning.json()["detail"]["issues"]
    )
    assert client.get(f"/api/persons/{parent}").json()["birthdate"] == "1970"

    override = client.put(
        f"/api/persons/{parent}",
        json={"birthdate": "1995", "override_warnings": True},
    )
    assert override.status_code == 200
    assert client.get(f"/api/persons/{child}").status_code == 200


def test_delete_person(client):
    create_resp = client.post("/api/persons", json={"firstname": "Del", "lastname": "Me"})
    pid = create_resp.json()["id"]
    resp = client.delete(f"/api/persons/{pid}")
    assert resp.status_code == 200
    assert resp.json()["deleted"] is True
    assert resp.json()["revision_id"]
    assert client.get(f"/api/persons/{pid}").status_code == 404


def test_history_records_actor_and_before_after_values(client):
    created = client.post(
        "/api/persons",
        json={"firstname": "History", "lastname": "Person"},
    )
    pid = created.json()["id"]
    updated = client.put(f"/api/persons/{pid}", json={"firstname": "Updated"})
    assert updated.status_code == 200

    history = client.get(f"/api/history?entity_id={pid}").json()
    assert [entry["operation"] for entry in history] == ["update", "create"]
    assert history[0]["actor"] == "dev@localhost"
    assert history[0]["before"]["attributes"]["firstname"] == "History"
    assert history[0]["after"]["attributes"]["firstname"] == "Updated"
    assert history[0]["can_rollback"] is True


def test_deleted_person_can_be_restored_with_relationships(client):
    parent, child = _create_two_persons(client)
    client.post(
        "/api/relationships",
        json={"source": child, "target": parent, "type": "isChildOf"},
    )
    deleted = client.delete(f"/api/persons/{parent}")
    revision_id = deleted.json()["revision_id"]
    assert client.get(f"/api/persons/{parent}").status_code == 404

    rollback = client.post(f"/api/history/{revision_id}/rollback")
    assert rollback.status_code == 200
    restored = client.get(f"/api/persons/{parent}")
    assert restored.status_code == 200
    relationships = client.get("/api/relationships").json()
    assert any(
        relationship["source"] == child and relationship["target"] == parent
        for relationship in relationships
    )

    history = client.get(f"/api/history?entity_id={parent}").json()
    assert history[0]["operation"] == "rollback"
    assert history[0]["metadata"]["rollback_of"] == revision_id
    assert next(entry for entry in history if entry["id"] == revision_id)["can_rollback"] is False


def test_rollback_rejects_later_changes_to_same_person(client):
    created = client.post("/api/persons", json={"firstname": "First"})
    pid = created.json()["id"]
    first_update = client.put(f"/api/persons/{pid}", json={"firstname": "Second"})
    first_revision = first_update.json()["revision_id"]
    client.put(f"/api/persons/{pid}", json={"firstname": "Third"})

    rollback = client.post(f"/api/history/{first_revision}/rollback")
    assert rollback.status_code == 409
    assert "changed after" in rollback.json()["detail"]
    assert client.get(f"/api/persons/{pid}").json()["firstname"] == "Third"


def test_history_access_and_rollback_are_scoped_to_the_actor(client):
    alice = {"email": "alice@example.com", "name": "Alice", "roles": []}
    bob = {"email": "bob@example.com", "name": "Bob", "roles": []}
    app.dependency_overrides[require_auth] = lambda: alice
    try:
        created = client.post("/api/persons", json={"firstname": "Private"})
        pid = created.json()["id"]
        deleted = client.delete(f"/api/persons/{pid}")
        revision_id = deleted.json()["revision_id"]

        assert client.get("/api/history").status_code == 403
        app.dependency_overrides[require_auth] = lambda: bob
        assert client.post(f"/api/history/{revision_id}/rollback").status_code == 403

        app.dependency_overrides[require_auth] = lambda: alice
        assert client.post(f"/api/history/{revision_id}/rollback").status_code == 200
    finally:
        app.dependency_overrides.pop(require_auth, None)


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


def test_created_reverse_relationships_can_be_undone_independently(client):
    p1, p2 = _create_two_persons(client)
    first = client.post(
        "/api/relationships",
        json={"source": p1, "target": p2, "type": "isSpouseOf"},
    )
    second = client.post(
        "/api/relationships",
        json={"source": p2, "target": p1, "type": "isSpouseOf"},
    )
    assert first.status_code == 201
    assert second.status_code == 201

    rollback = client.post(
        f"/api/history/{second.json()['revision_id']}/rollback"
    )
    assert rollback.status_code == 200
    relationships = client.get("/api/relationships").json()
    assert len(relationships) == 1
    assert relationships[0]["source"] == p1
    assert relationships[0]["target"] == p2


def test_relationship_hard_integrity_errors_are_structured(client):
    child, parent = _create_two_persons(client)

    self_resp = client.post(
        "/api/relationships",
        json={"source": child, "target": child, "type": "isSpouseOf"},
    )
    assert self_resp.status_code == 422
    assert self_resp.json()["detail"]["issues"][0]["code"] == "self_relationship"

    first = client.post(
        "/api/relationships",
        json={"source": child, "target": parent, "type": "isChildOf"},
    )
    assert first.status_code == 201
    duplicate = client.post(
        "/api/relationships",
        json={"source": child, "target": parent, "type": "isChildOf"},
    )
    assert duplicate.status_code == 422
    assert duplicate.json()["detail"]["issues"][0]["code"] == "duplicate_relationship"

    cycle = client.post(
        "/api/relationships",
        json={"source": parent, "target": child, "type": "isChildOf"},
    )
    assert cycle.status_code == 422
    assert any(
        issue["code"] == "parent_child_cycle"
        for issue in cycle.json()["detail"]["issues"]
    )


def test_create_person_with_relationships_is_atomic(client):
    parent = client.post(
        "/api/persons",
        json={"firstname": "Young parent", "birthdate": "1995"},
    ).json()["id"]
    payload = {
        "firstname": "Child",
        "birthdate": "2000",
        "relationships": [
            {
                "related_person_id": parent,
                "type": "isChildOf",
                "new_person_role": "source",
            }
        ],
    }

    warning = client.post("/api/persons", json=payload)
    assert warning.status_code == 422
    assert any(
        issue["code"] == "parent_too_young"
        for issue in warning.json()["detail"]["issues"]
    )
    assert len(client.get("/api/persons").json()) == 1

    created = client.post(
        "/api/persons",
        json={**payload, "override_warnings": True},
    )
    assert created.status_code == 201
    child = created.json()["id"]
    relationships = client.get("/api/relationships").json()
    assert any(
        relationship["source"] == child and relationship["target"] == parent
        for relationship in relationships
    )


def test_deactivate_relationship(client):
    p1, p2 = _create_two_persons(client)
    client.post("/api/relationships", json={"source": p1, "target": p2, "type": "isSpouseOf"})
    resp = client.put(f"/api/relationships/{p1}/{p2}/deactivate", json={"end_date": "2024-06-01"})
    assert resp.status_code == 200
    assert resp.json()["deactivated"] is True


def test_deleted_relationship_can_be_restored(client):
    p1, p2 = _create_two_persons(client)
    client.post(
        "/api/relationships",
        json={"source": p1, "target": p2, "type": "isSpouseOf"},
    )
    deleted = client.delete(f"/api/relationships/{p1}/{p2}")
    assert deleted.status_code == 200
    assert client.get("/api/relationships").json() == []

    rollback = client.post(
        f"/api/history/{deleted.json()['revision_id']}/rollback"
    )
    assert rollback.status_code == 200
    relationships = client.get("/api/relationships").json()
    assert len(relationships) == 1
    assert relationships[0]["source"] == p1
    assert relationships[0]["target"] == p2


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

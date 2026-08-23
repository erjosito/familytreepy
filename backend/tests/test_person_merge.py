"""Duplicate suggestion and safe merge tests."""

import copy
import json
import os

import pytest
from fastapi.testclient import TestClient

from backend.app.auth import require_auth
from backend.app.change_history import ChangeHistoryStore
from backend.app.dependencies import get_history_store, get_tree
from backend.app.main import app
from backend.app.person_merge import build_merge_preview, duplicate_suggestions
from backend.app.schemas.relationship_schema import load_relationship_schema
from familytree import FamilyTree


@pytest.fixture()
def merge_context(tmp_path):
    schema = load_relationship_schema(
        os.path.join(
            os.path.dirname(__file__), "..", "..", "config", "relationship_types.json"
        )
    )
    tree = FamilyTree(
        backend="local",
        localfile=str(tmp_path / "merge.gml"),
        relationship_schema=schema,
        autosave=False,
    )
    store = ChangeHistoryStore(
        backend="local", local_file=str(tmp_path / "history.jsonl")
    )
    app.dependency_overrides[get_tree] = lambda: tree
    app.dependency_overrides[get_history_store] = lambda: store
    app.dependency_overrides[require_auth] = lambda: {
        "email": "admin@example.com",
        "role": "admin",
        "roles": ["admin"],
    }
    yield TestClient(app), tree, store
    app.dependency_overrides.clear()


def _person(client, **fields):
    response = client.post("/api/persons", json=fields)
    assert response.status_code == 201, response.text
    return response.json()["id"]


def _merge(client, source, target, **options):
    payload = {"source_id": source, "target_id": target, **options}
    preview = client.post("/api/persons/merge/preview", json=payload)
    assert preview.status_code == 200, preview.text
    payload["preview_token"] = preview.json()["preview_token"]
    return client.post("/api/persons/merge", json=payload)


def test_suggestions_explain_normalized_alias_dates_and_relatives(merge_context):
    client, tree, _ = merge_context
    relative = _person(client, firstname="Shared", lastname="Parent")
    existing = _person(
        client,
        firstname="José",
        lastname="Smith",
        alias="Pepe",
        birthdate="1980-02-01",
    )
    tree.add_relationship(existing, relative, type="isChildOf", override_warnings=True)

    response = client.post(
        "/api/persons/duplicate-suggestions",
        json={
            "firstname": "Pepe",
            "lastname": "",
            "alias": "Jose Smith",
            "birthdate": "01/02/1980",
            "relative_ids": [relative],
        },
    )
    assert response.status_code == 200
    suggestion = response.json()["suggestions"][0]
    assert suggestion["person_id"] == existing
    assert {reason["code"] for reason in suggestion["reasons"]} == {
        "name_alias_overlap",
        "matching_birthdate",
        "shared_close_relatives",
    }


def test_create_and_update_return_nonblocking_suggestions(merge_context):
    client, _, _ = merge_context
    original = _person(client, firstname="Ada", lastname="Lovelace", birthdate="1815")
    duplicate = client.post(
        "/api/persons",
        json={"firstname": "ADA", "lastname": "Lovelace", "birthdate": "1815"},
    )
    assert duplicate.status_code == 201
    assert duplicate.json()["duplicate_suggestions"][0]["person_id"] == original

    other = _person(client, firstname="Different")
    updated = client.put(
        f"/api/persons/{other}",
        json={"firstname": "Ada", "lastname": "Lovelace", "birthdate": "1815"},
    )
    assert updated.status_code == 200
    suggested_ids = {
        suggestion["person_id"]
        for suggestion in updated.json()["duplicate_suggestions"]
    }
    assert original in suggested_ids


def test_create_suggestions_include_pending_relatives(merge_context):
    client, tree, _ = merge_context
    parent = _person(client, firstname="Shared", lastname="Parent")
    existing = _person(client, firstname="Alex", lastname="Example")
    tree.add_relationship(existing, parent, type="isChildOf", override_warnings=True)

    response = client.post(
        "/api/persons",
        json={
            "firstname": "Alex",
            "lastname": "Example",
            "relationships": [
                {
                    "related_person_id": parent,
                    "type": "isChildOf",
                    "new_person_role": "source",
                }
            ],
        },
    )

    assert response.status_code == 201
    suggestion = response.json()["duplicate_suggestions"][0]
    assert suggestion["person_id"] == existing
    assert "shared_close_relatives" in {
        reason["code"] for reason in suggestion["reasons"]
    }


def test_merge_preview_requires_admin(merge_context):
    client, _, _ = merge_context
    source = _person(client, firstname="Source")
    target = _person(client, firstname="Target")
    app.dependency_overrides[require_auth] = lambda: {
        "email": "reader@example.com",
        "role": "reader",
        "roles": [],
    }
    response = client.post(
        "/api/persons/merge/preview",
        json={"source_id": source, "target_id": target},
    )
    assert response.status_code == 403
    execution = client.post(
        "/api/persons/merge",
        json={"source_id": source, "target_id": target, "preview_token": "token"},
    )
    assert execution.status_code == 403


def test_preview_and_merge_require_explicit_field_choices(merge_context):
    client, tree, store = merge_context
    source = _person(client, firstname="Jon", birthplace="London")
    target = _person(client, firstname="John", birthplace="Paris")

    preview = client.post(
        "/api/persons/merge/preview",
        json={"source_id": source, "target_id": target},
    ).json()
    assert {item["field"] for item in preview["field_conflicts"]} == {
        "firstname",
        "birthplace",
    }
    rejected = client.post(
        "/api/persons/merge",
        json={
            "source_id": source,
            "target_id": target,
            "preview_token": preview["preview_token"],
        },
    )
    assert rejected.status_code == 400
    assert source in tree.graph
    assert store.list()[-1]["operation"] == "create"

    choices = {"firstname": "source", "birthplace": "target"}
    preview = client.post(
        "/api/persons/merge/preview",
        json={
            "source_id": source,
            "target_id": target,
            "field_choices": choices,
        },
    ).json()
    merged = client.post(
        "/api/persons/merge",
        json={
            "source_id": source,
            "target_id": target,
            "field_choices": choices,
            "preview_token": preview["preview_token"],
        },
    )
    assert merged.status_code == 200
    assert source not in tree.graph
    assert tree.graph.nodes[target]["firstname"] == "Jon"
    assert tree.graph.nodes[target]["birthplace"] == "Paris"
    merge_records = [record for record in store.list() if record["operation"] == "merge"]
    assert len(merge_records) == 1
    assert merge_records[0]["metadata"]["provenance"]["source_id"] == source


def test_execution_requires_preview_and_merge_validation_is_atomic(merge_context):
    client, tree, store = merge_context
    source = _person(client, firstname="Same")
    target = _person(client, firstname="Same")
    middle = _person(client, firstname="Middle")
    no_preview = client.post(
        "/api/persons/merge",
        json={"source_id": source, "target_id": target},
    )
    assert no_preview.status_code == 400

    tree.graph.add_edge(source, middle, type="isChildOf")
    tree.graph.add_edge(middle, target, type="isChildOf")
    before = copy.deepcopy(tree.graph)
    revision_count = len(store.list())
    preview = client.post(
        "/api/persons/merge/preview",
        json={"source_id": source, "target_id": target},
    ).json()
    invalid = client.post(
        "/api/persons/merge",
        json={
            "source_id": source,
            "target_id": target,
            "preview_token": preview["preview_token"],
        },
    )
    assert invalid.status_code == 422
    assert dict(tree.graph.nodes(data=True)) == dict(before.nodes(data=True))
    assert list(tree.graph.edges(data=True)) == list(before.edges(data=True))
    assert len(store.list()) == revision_count


def test_merge_unions_notes_pictures_and_repoints_relationships(merge_context):
    client, tree, _ = merge_context
    source = _person(
        client,
        firstname="Same",
        profilepic="source-profile.jpg",
        pictures=["shared.jpg", "source.jpg"],
        extra={
            "tags": ["source-tag"],
            "notes_json": json.dumps([{"text": "source"}, {"text": "shared"}]),
        },
    )
    target = _person(
        client,
        firstname="Same",
        profilepic="target-profile.jpg",
        pictures=["target.jpg", "shared.jpg"],
        extra={
            "tags": ["target-tag"],
            "notes_json": json.dumps([{"text": "target"}, {"text": "shared"}]),
        },
    )
    relative = _person(client, firstname="Relative")
    tree.add_relationship(source, relative, type="isChildOf", override_warnings=True)

    response = _merge(
        client,
        source,
        target,
        field_choices={"profilepic": "target"},
    )
    assert response.status_code == 200, response.text
    attrs = tree.graph.nodes[target]
    assert attrs["pictures"] == [
        "target.jpg",
        "shared.jpg",
        "source.jpg",
        "target-profile.jpg",
        "source-profile.jpg",
    ]
    assert attrs["profilepic"] == "target-profile.jpg"
    assert attrs["tags"] == ["target-tag", "source-tag"]
    assert json.loads(attrs["notes_json"]) == [
        {"text": "target"},
        {"text": "shared"},
        {"text": "source"},
    ]
    assert tree.graph.has_edge(target, relative)


def test_merge_removes_self_links_and_deduplicates_identical_edges(merge_context):
    client, tree, _ = merge_context
    source = _person(client, firstname="Same")
    target = _person(client, firstname="Same")
    relative = _person(client, firstname="Relative")
    tree.graph.add_edge(source, target, type="isSpouseOf", is_active=True)
    tree.graph.add_edge(source, relative, type="isChildOf")
    tree.graph.add_edge(target, relative, type="isChildOf")

    preview = build_merge_preview(tree, source, target)
    assert len(preview["relationships"]["self_links_removed"]) == 1
    assert len(preview["relationships"]["duplicates_removed"]) == 1
    response = _merge(client, source, target)
    assert response.status_code == 200
    assert not tree.graph.has_edge(target, target)
    assert list(tree.graph.edges()).count((target, relative)) == 1


def test_relationship_attribute_conflict_requires_explicit_choice(merge_context):
    client, tree, _ = merge_context
    source = _person(client, firstname="Same")
    target = _person(client, firstname="Same")
    relative = _person(client, firstname="Relative")
    tree.graph.add_edge(source, relative, type="isSpouseOf", start_date="2000")
    tree.graph.add_edge(target, relative, type="isSpouseOf", start_date="2001")

    preview = client.post(
        "/api/persons/merge/preview",
        json={"source_id": source, "target_id": target},
    ).json()
    conflict = preview["relationship_conflicts"][0]
    assert conflict["resolved"] is False
    rejected = client.post(
        "/api/persons/merge",
        json={
            "source_id": source,
            "target_id": target,
            "preview_token": preview["preview_token"],
        },
    )
    assert rejected.status_code == 400
    accepted = _merge(
        client,
        source,
        target,
        relationship_choices={conflict["key"]: "source"},
    )
    assert accepted.status_code == 200
    assert tree.graph[target][relative]["start_date"] == "2000"


def test_merge_rollback_restores_people_and_relationships_and_detects_conflict(
    merge_context,
):
    client, tree, _ = merge_context
    source = _person(client, firstname="Same", alias="Source")
    target = _person(client, firstname="Same")
    relative = _person(client, firstname="Relative")
    tree.add_relationship(source, relative, type="isChildOf", override_warnings=True)
    merged = _merge(client, source, target).json()
    rollback = client.post(f"/api/history/{merged['revision_id']}/rollback")
    assert rollback.status_code == 200, rollback.text
    assert source in tree.graph and target in tree.graph
    assert tree.graph.nodes[source]["alias"] == "Source"
    assert tree.graph.has_edge(source, relative)
    assert not tree.graph.has_edge(target, relative)

    merged_again = _merge(client, source, target).json()
    tree.graph.nodes[target]["firstname"] = "Later change"
    conflict = client.post(f"/api/history/{merged_again['revision_id']}/rollback")
    assert conflict.status_code == 409
    assert source not in tree.graph


def test_stale_preview_and_journal_failure_do_not_partially_mutate(
    merge_context, monkeypatch
):
    client, tree, store = merge_context
    source = _person(client, firstname="Same")
    target = _person(client, firstname="Same")
    preview = client.post(
        "/api/persons/merge/preview",
        json={"source_id": source, "target_id": target},
    ).json()
    tree.graph.nodes[target]["alias"] = "changed"
    stale = client.post(
        "/api/persons/merge",
        json={
            "source_id": source,
            "target_id": target,
            "preview_token": preview["preview_token"],
        },
    )
    assert stale.status_code == 409
    assert source in tree.graph

    before = copy.deepcopy(tree.graph)
    fresh_preview = client.post(
        "/api/persons/merge/preview",
        json={"source_id": source, "target_id": target},
    ).json()
    monkeypatch.setattr(store, "append", lambda record: (_ for _ in ()).throw(OSError("down")))
    with pytest.raises(OSError, match="down"):
        client.post(
            "/api/persons/merge",
            json={
                "source_id": source,
                "target_id": target,
                "preview_token": fresh_preview["preview_token"],
            },
        )
    assert dict(tree.graph.nodes(data=True)) == dict(before.nodes(data=True))
    assert list(tree.graph.edges(data=True)) == list(before.edges(data=True))


def test_preview_token_is_bound_to_people_and_choices(merge_context):
    client, tree, _ = merge_context
    source = _person(client, firstname="Source")
    target = _person(client, firstname="Target")
    other = _person(client, firstname="Other")
    preview = client.post(
        "/api/persons/merge/preview",
        json={
            "source_id": source,
            "target_id": target,
            "field_choices": {"firstname": "source"},
        },
    ).json()

    wrong_pair = client.post(
        "/api/persons/merge",
        json={
            "source_id": source,
            "target_id": other,
            "field_choices": {"firstname": "source"},
            "preview_token": preview["preview_token"],
        },
    )
    assert wrong_pair.status_code == 409
    assert source in tree.graph

    changed_choice = client.post(
        "/api/persons/merge",
        json={
            "source_id": source,
            "target_id": target,
            "field_choices": {"firstname": "target"},
            "preview_token": preview["preview_token"],
        },
    )
    assert changed_choice.status_code == 409
    assert source in tree.graph

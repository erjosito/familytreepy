"""Unit tests for durable history and transactional journaling."""

import os

import pytest

from backend.app.change_history import (
    ChangeHistoryStore,
    apply_audited_change,
    new_record,
)
from backend.app.schemas.relationship_schema import load_relationship_schema
from familytree import FamilyTree


@pytest.fixture()
def tree(tmp_path):
    config_path = os.path.join(
        os.path.dirname(__file__),
        "..",
        "..",
        "config",
        "relationship_types.json",
    )
    return FamilyTree(
        backend="local",
        localfile=str(tmp_path / "history_tree.gml"),
        relationship_schema=load_relationship_schema(config_path),
        autosave=False,
    )


def test_local_history_is_append_only(tmp_path):
    store = ChangeHistoryStore(
        backend="local",
        local_file=str(tmp_path / "history.jsonl"),
    )
    first = new_record(
        actor="first@example.com",
        operation="create",
        entity_type="person",
        entity_id="person-1",
        before=None,
        after={"attributes": {"firstname": "First"}, "relationships": []},
    )
    second = new_record(
        actor="second@example.com",
        operation="update",
        entity_type="person",
        entity_id="person-1",
        before=first["after"],
        after={"attributes": {"firstname": "Second"}, "relationships": []},
    )

    store.append(first)
    store.append(second)

    assert store.list() == [first, second]
    assert store.get(first["id"]) == first


def test_journal_failure_restores_graph_state(tree, tmp_path):
    person_id = tree.add_person(firstname="Before")

    class FailingStore(ChangeHistoryStore):
        def append(self, record):
            raise OSError("history unavailable")

    store = FailingStore(
        backend="local",
        local_file=str(tmp_path / "unwritten.jsonl"),
    )

    with pytest.raises(OSError, match="history unavailable"):
        apply_audited_change(
            tree=tree,
            store=store,
            actor="editor@example.com",
            operation="update",
            entity_type="person",
            entity_id=person_id,
            mutation=lambda: tree.update_person(person_id, firstname="After"),
        )

    assert tree.get_person(person_id)["firstname"] == "Before"

"""Append-only change journal and conflict-safe compensating rollback."""

from __future__ import annotations

import copy
import json
import os
import threading
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

from azure.core.exceptions import ResourceExistsError, ResourceNotFoundError
from azure.storage.blob import BlobClient, BlobServiceClient


class HistoryConflictError(ValueError):
    """Raised when rollback would overwrite a later change."""


class HistoryNotFoundError(ValueError):
    """Raised when a requested revision does not exist."""


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def _json_value(value: Any) -> Any:
    return json.loads(json.dumps(value, default=str))


def person_snapshot(tree, person_id: str) -> dict[str, Any] | None:
    if person_id not in tree.graph:
        return None
    relationships = []
    seen: set[tuple[str, str]] = set()
    for source, target in [
        *tree.graph.out_edges(person_id),
        *tree.graph.in_edges(person_id),
    ]:
        if (source, target) in seen:
            continue
        seen.add((source, target))
        relationships.append(
            {
                "source": source,
                "target": target,
                "attributes": _json_value(dict(tree.graph[source][target])),
            }
        )
    relationships.sort(key=lambda item: (item["source"], item["target"]))
    return {
        "attributes": _json_value(dict(tree.graph.nodes[person_id])),
        "relationships": relationships,
    }


def relationship_snapshot(
    tree,
    source: str,
    target: str,
    *,
    include_reverse: bool = False,
) -> dict[str, Any] | None:
    if not tree.graph.has_edge(source, target):
        return None
    relationship_type = tree.graph[source][target].get("type")
    edges = []
    edge_pairs = [(source, target)]
    if include_reverse:
        edge_pairs.append((target, source))
    for edge_source, edge_target in edge_pairs:
        if not tree.graph.has_edge(edge_source, edge_target):
            continue
        attributes = dict(tree.graph[edge_source][edge_target])
        if attributes.get("type") != relationship_type:
            continue
        edges.append(
            {
                "source": edge_source,
                "target": edge_target,
                "attributes": _json_value(attributes),
            }
        )
    edges.sort(key=lambda item: (item["source"], item["target"]))
    return {"edges": edges}


def entity_snapshot(
    tree,
    entity_type: str,
    entity_id: str,
    *,
    source: str | None = None,
    target: str | None = None,
    include_reverse: bool = False,
) -> dict[str, Any] | None:
    if entity_type == "person":
        return person_snapshot(tree, entity_id)
    if entity_type == "relationship" and source and target:
        return relationship_snapshot(
            tree,
            source,
            target,
            include_reverse=include_reverse,
        )
    raise ValueError(f"Unsupported history entity type: {entity_type}")


class ChangeHistoryStore:
    """Store newline-delimited revisions locally or in an Azure append blob."""

    def __init__(
        self,
        *,
        backend: str,
        local_file: str | None = None,
        account: str | None = None,
        key: str | None = None,
        container: str | None = None,
        blob: str | None = None,
    ):
        self.backend = backend
        self.local_file = local_file
        self.account = account
        self.key = key
        self.container = container
        self.blob = blob
        self._lock = threading.Lock()

    def _append_blob_client(self) -> BlobClient:
        if not all((self.account, self.key, self.container, self.blob)):
            raise ValueError("Azure history storage is not fully configured")
        connection_string = (
            "DefaultEndpointsProtocol=https;"
            f"AccountName={self.account};AccountKey={self.key}"
        )
        return BlobClient.from_connection_string(
            connection_string,
            container_name=self.container,
            blob_name=self.blob,
        )

    def append(self, record: dict[str, Any]) -> dict[str, Any]:
        serialized = json.dumps(record, separators=(",", ":"), sort_keys=True) + "\n"
        with self._lock:
            if self.backend == "local":
                if not self.local_file:
                    raise ValueError("Local history file is not configured")
                path = Path(self.local_file)
                path.parent.mkdir(parents=True, exist_ok=True)
                with path.open("a", encoding="utf-8") as handle:
                    handle.write(serialized)
                    handle.flush()
                    os.fsync(handle.fileno())
            elif self.backend == "azstorage":
                client = self._append_blob_client()
                try:
                    client.create_append_blob()
                except ResourceExistsError:
                    pass
                client.append_block(serialized.encode("utf-8"))
            else:
                raise ValueError(f"Unsupported history backend: {self.backend}")
        return record

    def list(self) -> list[dict[str, Any]]:
        with self._lock:
            if self.backend == "local":
                if not self.local_file or not Path(self.local_file).exists():
                    return []
                content = Path(self.local_file).read_text(encoding="utf-8")
            elif self.backend == "azstorage":
                try:
                    blob_client = BlobServiceClient.from_connection_string(
                        "DefaultEndpointsProtocol=https;"
                        f"AccountName={self.account};AccountKey={self.key}"
                    ).get_blob_client(container=self.container, blob=self.blob)
                    content = blob_client.download_blob().readall().decode("utf-8")
                except ResourceNotFoundError:
                    return []
            else:
                raise ValueError(f"Unsupported history backend: {self.backend}")
        records = []
        for line in content.splitlines():
            if line.strip():
                records.append(json.loads(line))
        return records

    def get(self, revision_id: str) -> dict[str, Any]:
        for record in self.list():
            if record["id"] == revision_id:
                return record
        raise HistoryNotFoundError("Revision not found")


def new_record(
    *,
    actor: str,
    operation: str,
    entity_type: str,
    entity_id: str,
    before: dict[str, Any] | None,
    after: dict[str, Any] | None,
    metadata: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "id": str(uuid.uuid4()),
        "timestamp": _utc_now().isoformat(),
        "actor": actor,
        "operation": operation,
        "entity_type": entity_type,
        "entity_id": entity_id,
        "before": _json_value(before),
        "after": _json_value(after),
        "metadata": _json_value(metadata or {}),
    }


def apply_audited_change(
    *,
    tree,
    store: ChangeHistoryStore,
    actor: str,
    operation: str,
    entity_type: str,
    entity_id: str,
    mutation: Callable[[], Any],
    source: str | None = None,
    target: str | None = None,
    include_reverse: bool = False,
    metadata: dict[str, Any] | None = None,
) -> tuple[Any, dict[str, Any]]:
    original_graph = copy.deepcopy(tree.graph)
    previous_autosave = tree.autosave
    before = entity_snapshot(
        tree,
        entity_type,
        entity_id,
        source=source,
        target=target,
        include_reverse=include_reverse,
    )
    try:
        tree.autosave = False
        result = mutation()
        after = entity_snapshot(
            tree,
            entity_type,
            entity_id,
            source=source,
            target=target,
            include_reverse=include_reverse,
        )
        if previous_autosave:
            tree.save()
        record = new_record(
            actor=actor,
            operation=operation,
            entity_type=entity_type,
            entity_id=entity_id,
            before=before,
            after=after,
            metadata=metadata,
        )
        try:
            store.append(record)
        except Exception:
            tree.graph = original_graph
            if previous_autosave:
                tree.save()
            raise
        return result, record
    except Exception:
        tree.graph = original_graph
        raise
    finally:
        tree.autosave = previous_autosave


def _restore_person(tree, person_id: str, state: dict[str, Any] | None) -> None:
    if person_id in tree.graph:
        tree.graph.remove_node(person_id)
    if state is None:
        return
    tree.graph.add_node(person_id, **copy.deepcopy(state["attributes"]))
    for relationship in state["relationships"]:
        source = relationship["source"]
        target = relationship["target"]
        if source not in tree.graph or target not in tree.graph:
            raise HistoryConflictError(
                "A related person changed or was deleted after this revision"
            )
        tree.graph.add_edge(
            source,
            target,
            **copy.deepcopy(relationship["attributes"]),
        )


def _restore_relationship(
    tree,
    source: str,
    target: str,
    state: dict[str, Any] | None,
    *,
    include_reverse: bool,
) -> None:
    relationship_type = None
    if tree.graph.has_edge(source, target):
        relationship_type = tree.graph[source][target].get("type")
    edge_pairs = [(source, target)]
    if include_reverse:
        edge_pairs.append((target, source))
    for edge_source, edge_target in edge_pairs:
        if not tree.graph.has_edge(edge_source, edge_target):
            continue
        if (
            relationship_type is None
            or tree.graph[edge_source][edge_target].get("type") == relationship_type
        ):
            tree.graph.remove_edge(edge_source, edge_target)
    if state is None:
        return
    for edge in state["edges"]:
        if edge["source"] not in tree.graph or edge["target"] not in tree.graph:
            raise HistoryConflictError("A related person was deleted after this revision")
        tree.graph.add_edge(
            edge["source"],
            edge["target"],
            **copy.deepcopy(edge["attributes"]),
        )


def rollback_revision(
    *,
    tree,
    store: ChangeHistoryStore,
    revision: dict[str, Any],
    actor: str,
    rollback_days: int,
) -> dict[str, Any]:
    timestamp = datetime.fromisoformat(revision["timestamp"])
    if _utc_now() - timestamp > timedelta(days=rollback_days):
        raise HistoryConflictError("The rollback window for this revision has expired")
    records = store.list()
    if any(
        record.get("metadata", {}).get("rollback_of") == revision["id"]
        for record in records
    ):
        raise HistoryConflictError("This revision has already been rolled back")

    metadata = revision.get("metadata", {})
    source = metadata.get("source")
    target = metadata.get("target")
    include_reverse = bool(metadata.get("include_reverse"))
    current = entity_snapshot(
        tree,
        revision["entity_type"],
        revision["entity_id"],
        source=source,
        target=target,
        include_reverse=include_reverse,
    )
    if current != revision["after"]:
        raise HistoryConflictError(
            "This entity changed after the selected revision; refresh history before retrying"
        )

    def restore() -> None:
        if revision["entity_type"] == "person":
            _restore_person(tree, revision["entity_id"], revision["before"])
        elif revision["entity_type"] == "relationship" and source and target:
            _restore_relationship(
                tree,
                source,
                target,
                revision["before"],
                include_reverse=include_reverse,
            )
        else:
            raise ValueError("Revision does not contain rollback metadata")

    _, compensation = apply_audited_change(
        tree=tree,
        store=store,
        actor=actor,
        operation="rollback",
        entity_type=revision["entity_type"],
        entity_id=revision["entity_id"],
        mutation=restore,
        source=source,
        target=target,
        include_reverse=include_reverse,
        metadata={
            "rollback_of": revision["id"],
            "source": source,
            "target": target,
            "include_reverse": include_reverse,
        },
    )
    return compensation

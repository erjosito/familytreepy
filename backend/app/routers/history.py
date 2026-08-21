"""History listing and compensating rollback endpoints."""

import os
from datetime import datetime, timedelta, timezone

from fastapi import APIRouter, Depends, HTTPException, Query

from backend.app.auth import require_auth
from backend.app.change_history import (
    ChangeHistoryStore,
    HistoryConflictError,
    HistoryNotFoundError,
    rollback_revision,
)
from backend.app.dependencies import get_history_store, get_tree

router = APIRouter(prefix="/api/history", tags=["history"])


def _is_admin(user: dict) -> bool:
    return user.get("role") == "admin" or "admin" in user.get("roles", [])


def _with_rollback_state(
    records: list[dict],
    rollback_days: int,
) -> list[dict]:
    rollback_targets = {
        record.get("metadata", {}).get("rollback_of")
        for record in records
        if record.get("metadata", {}).get("rollback_of")
    }
    now = datetime.now(timezone.utc)
    enriched = []
    for record in records:
        timestamp = datetime.fromisoformat(record["timestamp"])
        expires_at = timestamp + timedelta(days=rollback_days)
        item = dict(record)
        item["expires_at"] = expires_at.isoformat()
        item["can_rollback"] = (
            record["operation"] != "rollback"
            and record["id"] not in rollback_targets
            and now <= expires_at
        )
        enriched.append(item)
    return enriched


def _as_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


@router.get("")
def list_history(
    actor: str | None = None,
    operation: str | None = None,
    entity_type: str | None = None,
    entity_id: str | None = None,
    from_date: datetime | None = None,
    to_date: datetime | None = None,
    limit: int = Query(default=100, ge=1, le=500),
    user=Depends(require_auth),
    store: ChangeHistoryStore = Depends(get_history_store),
):
    if not _is_admin(user):
        raise HTTPException(status_code=403, detail="Admin access required")
    records = store.list()
    if actor:
        records = [record for record in records if actor.lower() in record["actor"].lower()]
    if operation:
        records = [record for record in records if record["operation"] == operation]
    if entity_type:
        records = [record for record in records if record["entity_type"] == entity_type]
    if entity_id:
        records = [record for record in records if entity_id.lower() in record["entity_id"].lower()]
    if from_date:
        records = [
            record for record in records
            if datetime.fromisoformat(record["timestamp"]) >= _as_utc(from_date)
        ]
    if to_date:
        records = [
            record for record in records
            if datetime.fromisoformat(record["timestamp"]) <= _as_utc(to_date)
        ]
    records.sort(key=lambda record: record["timestamp"], reverse=True)
    rollback_days = int(os.getenv("HISTORY_ROLLBACK_DAYS", "30"))
    return _with_rollback_state(records, rollback_days)[:limit]


@router.post("/{revision_id}/rollback")
def rollback_history(
    revision_id: str,
    user=Depends(require_auth),
    tree=Depends(get_tree),
    store: ChangeHistoryStore = Depends(get_history_store),
):
    try:
        revision = store.get(revision_id)
    except HistoryNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    actor = user.get("email") or user.get("name") or "unknown"
    if not _is_admin(user) and revision["actor"].lower() != actor.lower():
        raise HTTPException(status_code=403, detail="You can only undo your own changes")
    try:
        compensation = rollback_revision(
            tree=tree,
            store=store,
            revision=revision,
            actor=actor,
            rollback_days=int(os.getenv("HISTORY_ROLLBACK_DAYS", "30")),
        )
    except HistoryConflictError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    return {"rolled_back": revision_id, "revision_id": compensation["id"]}

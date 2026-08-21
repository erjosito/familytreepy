"""Relationship endpoints."""

from fastapi import APIRouter, HTTPException, Depends
from backend.app.models import RelationshipCreate, RelationshipDeactivate
from backend.app.change_history import ChangeHistoryStore, apply_audited_change
from backend.app.dependencies import get_history_store, get_tree
from backend.app.auth import require_auth
from tree_validation import TreeValidationError

router = APIRouter(prefix="/api/relationships", tags=["relationships"], dependencies=[Depends(require_auth)])


@router.get("")
def list_relationships(include_inactive: bool = False, tree=Depends(get_tree)):
    """List all relationships, optionally including inactive ones."""
    return tree.get_relationships(include_inactive=include_inactive)


@router.post("", status_code=201)
def create_relationship(
    body: RelationshipCreate,
    tree=Depends(get_tree),
    user=Depends(require_auth),
    history: ChangeHistoryStore = Depends(get_history_store),
):
    """Add a new relationship between two persons."""
    try:
        _, revision = apply_audited_change(
            tree=tree,
            store=history,
            actor=user.get("email") or user.get("name") or "unknown",
            operation="create",
            entity_type="relationship",
            entity_id=f"{body.source}:{body.target}",
            source=body.source,
            target=body.target,
            metadata={"source": body.source, "target": body.target},
            mutation=lambda: tree.add_relationship(
                body.source,
                body.target,
                type=body.type,
                start_date=body.start_date,
                override_warnings=body.override_warnings,
            ),
        )
    except TreeValidationError as e:
        raise HTTPException(status_code=422, detail=e.to_detail()) from e
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    return {
        "source": body.source,
        "target": body.target,
        "type": body.type,
        "created": True,
        "revision_id": revision["id"],
    }


@router.put("/{source_id}/{target_id}/deactivate")
def deactivate_relationship(
    source_id: str,
    target_id: str,
    body: RelationshipDeactivate,
    tree=Depends(get_tree),
    user=Depends(require_auth),
    history: ChangeHistoryStore = Depends(get_history_store),
):
    """Soft-delete a deactivatable relationship."""
    try:
        _, revision = apply_audited_change(
            tree=tree,
            store=history,
            actor=user.get("email") or user.get("name") or "unknown",
            operation="deactivate",
            entity_type="relationship",
            entity_id=f"{source_id}:{target_id}",
            source=source_id,
            target=target_id,
            include_reverse=True,
            metadata={
                "source": source_id,
                "target": target_id,
                "include_reverse": True,
            },
            mutation=lambda: tree.deactivate_relationship(
                source_id,
                target_id,
                end_date=body.end_date,
                override_warnings=body.override_warnings,
            ),
        )
    except TreeValidationError as e:
        raise HTTPException(status_code=422, detail=e.to_detail()) from e
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    return {
        "source": source_id,
        "target": target_id,
        "deactivated": True,
        "revision_id": revision["id"],
    }


@router.put("/{source_id}/{target_id}/reactivate")
def reactivate_relationship(
    source_id: str,
    target_id: str,
    tree=Depends(get_tree),
    user=Depends(require_auth),
    history: ChangeHistoryStore = Depends(get_history_store),
):
    """Re-activate a previously deactivated relationship."""
    try:
        _, revision = apply_audited_change(
            tree=tree,
            store=history,
            actor=user.get("email") or user.get("name") or "unknown",
            operation="reactivate",
            entity_type="relationship",
            entity_id=f"{source_id}:{target_id}",
            source=source_id,
            target=target_id,
            include_reverse=True,
            metadata={
                "source": source_id,
                "target": target_id,
                "include_reverse": True,
            },
            mutation=lambda: tree.reactivate_relationship(source_id, target_id),
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return {
        "source": source_id,
        "target": target_id,
        "reactivated": True,
        "revision_id": revision["id"],
    }


@router.post("/activate-all")
def activate_all_relationships(
    tree=Depends(get_tree),
    user=Depends(require_auth),
    history: ChangeHistoryStore = Depends(get_history_store),
):
    """Set all relationships to active."""
    inactive = [
        (source, target)
        for source, target, data in tree.graph.edges(data=True)
        if data.get("is_active") is False
    ]
    revisions = []
    processed: set[tuple[str, str]] = set()
    for source, target in inactive:
        if (source, target) in processed:
            continue
        reverse = (target, source)
        processed.add((source, target))
        processed.add(reverse)
        _, revision = apply_audited_change(
            tree=tree,
            store=history,
            actor=user.get("email") or user.get("name") or "unknown",
            operation="reactivate",
            entity_type="relationship",
            entity_id=f"{source}:{target}",
            source=source,
            target=target,
            include_reverse=True,
            metadata={
                "source": source,
                "target": target,
                "include_reverse": True,
            },
            mutation=lambda source=source, target=target: tree.reactivate_relationship(
                source, target
            ),
        )
        revisions.append(revision["id"])
    return {"activated": True, "revision_ids": revisions}


@router.delete("/{source_id}/{target_id}")
def delete_relationship(
    source_id: str,
    target_id: str,
    tree=Depends(get_tree),
    user=Depends(require_auth),
    history: ChangeHistoryStore = Depends(get_history_store),
):
    """Soft-delete a relationship by removing it from the active graph and journaling it."""
    try:
        _, revision = apply_audited_change(
            tree=tree,
            store=history,
            actor=user.get("email") or user.get("name") or "unknown",
            operation="delete",
            entity_type="relationship",
            entity_id=f"{source_id}:{target_id}",
            source=source_id,
            target=target_id,
            include_reverse=True,
            metadata={
                "source": source_id,
                "target": target_id,
                "include_reverse": True,
            },
            mutation=lambda: tree.delete_relationship(source_id, target_id),
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return {
        "source": source_id,
        "target": target_id,
        "deleted": True,
        "revision_id": revision["id"],
    }

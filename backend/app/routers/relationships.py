"""Relationship endpoints."""

from fastapi import APIRouter, HTTPException, Depends
from backend.app.models import RelationshipCreate, RelationshipDeactivate
from backend.app.dependencies import get_tree
from backend.app.auth import require_auth

router = APIRouter(prefix="/api/relationships", tags=["relationships"], dependencies=[Depends(require_auth)])


@router.get("")
def list_relationships(include_inactive: bool = False, tree=Depends(get_tree)):
    """List all relationships, optionally including inactive ones."""
    return tree.get_relationships(include_inactive=include_inactive)


@router.post("", status_code=201)
def create_relationship(body: RelationshipCreate, tree=Depends(get_tree)):
    """Add a new relationship between two persons."""
    try:
        tree.add_relationship(
            body.source, body.target, type=body.type, start_date=body.start_date
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return {"source": body.source, "target": body.target, "type": body.type, "created": True}


@router.put("/{source_id}/{target_id}/deactivate")
def deactivate_relationship(
    source_id: str, target_id: str, body: RelationshipDeactivate, tree=Depends(get_tree)
):
    """Soft-delete a deactivatable relationship."""
    try:
        tree.deactivate_relationship(source_id, target_id, end_date=body.end_date)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return {"source": source_id, "target": target_id, "deactivated": True}


@router.put("/{source_id}/{target_id}/reactivate")
def reactivate_relationship(
    source_id: str, target_id: str, tree=Depends(get_tree)
):
    """Re-activate a previously deactivated relationship."""
    try:
        tree.reactivate_relationship(source_id, target_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return {"source": source_id, "target": target_id, "reactivated": True}


@router.post("/activate-all")
def activate_all_relationships(tree=Depends(get_tree)):
    """Set all relationships to active."""
    tree.activate_all_relationships()
    return {"activated": True}


@router.delete("/{source_id}/{target_id}")
def delete_relationship(source_id: str, target_id: str, tree=Depends(get_tree)):
    """Permanently delete a relationship between two persons."""
    try:
        tree.delete_relationship(source_id, target_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    return {"source": source_id, "target": target_id, "deleted": True}

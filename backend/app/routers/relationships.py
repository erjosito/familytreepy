"""Relationship endpoints."""

from fastapi import APIRouter, HTTPException, Depends
from backend.app.models import RelationshipCreate, RelationshipDeactivate
from backend.app.dependencies import get_tree

router = APIRouter(prefix="/api/relationships", tags=["relationships"])


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

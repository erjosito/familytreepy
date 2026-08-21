"""Person CRUD endpoints."""

import os
import uuid
from fastapi import APIRouter, HTTPException, Depends, UploadFile, File
from backend.app.models import PersonCreate, PersonUpdate, PersonResponse
from backend.app.change_history import ChangeHistoryStore, apply_audited_change
from backend.app.dependencies import get_history_store, get_tree
from backend.app.auth import require_auth
from tree_validation import (
    TreeValidationError,
    enforce_issues,
    validate_person_dates,
    validate_relationship,
)

router = APIRouter(prefix="/api/persons", tags=["persons"], dependencies=[Depends(require_auth)])

_MAX_UPLOAD_SIZE = 10 * 1024 * 1024  # 10 MB
_ALLOWED_EXTENSIONS = {".jpg", ".jpeg", ".png", ".gif", ".webp", ".bmp", ".heic", ".heif", ".tif", ".tiff"}


def _actor(user: dict) -> str:
    return user.get("email") or user.get("name") or "unknown"


def _audit_person_change(
    *,
    tree,
    history: ChangeHistoryStore,
    user: dict,
    person_id: str,
    mutation,
    change: str,
):
    return apply_audited_change(
        tree=tree,
        store=history,
        actor=_actor(user),
        operation="update",
        entity_type="person",
        entity_id=person_id,
        mutation=mutation,
        metadata={"change": change},
    )[1]


async def _validate_image_upload(file: UploadFile) -> bytes:
    """Validate image upload: extension, content-type, and size."""
    ext = os.path.splitext(file.filename or "")[1].lower()
    if ext not in _ALLOWED_EXTENSIONS:
        raise HTTPException(
            status_code=400,
            detail=f"File extension '{ext}' not allowed. Allowed: {', '.join(sorted(_ALLOWED_EXTENSIONS))}",
        )
    content_type = (file.content_type or "").lower()
    if content_type and not content_type.startswith("image/") and content_type != "application/octet-stream":
        raise HTTPException(status_code=400, detail=f"Content-Type '{content_type}' not allowed. Expected image/*")
    content = await file.read()
    if len(content) > _MAX_UPLOAD_SIZE:
        raise HTTPException(status_code=400, detail="File exceeds 10 MB limit")
    return content


@router.get("", response_model=list[dict])
def list_persons(tree=Depends(get_tree)):
    """List all persons with fields needed by selectors and search."""
    result = []
    for node_id in tree.graph.nodes():
        data = tree.graph.nodes[node_id]
        fullname = (data.get("firstname", "") + " " + data.get("lastname", "")).strip()
        result.append({"id": node_id, "fullname": fullname, "alias": data.get("alias", "")})
    return result


@router.get("/{person_id}")
def get_person(person_id: str, tree=Depends(get_tree)):
    """Get person details + relationships."""
    person = tree.get_person(person_id)
    if person is None:
        raise HTTPException(status_code=404, detail="Person not found")
    data = dict(person)
    data["id"] = person_id
    data["fullname"] = (data.get("firstname", "") + " " + data.get("lastname", "")).strip()
    data["relationships"] = tree.get_relationships(person_id, include_inactive=True)
    data["siblings"] = tree.get_siblings(person_id)
    return data


@router.post("", response_model=dict, status_code=201)
def create_person(
    body: PersonCreate,
    tree=Depends(get_tree),
    user=Depends(require_auth),
    history: ChangeHistoryStore = Depends(get_history_store),
):
    """Add a new person."""
    attrs = body.model_dump(
        exclude_none=True,
        exclude={"extra", "relationships", "override_warnings"},
    )
    if body.extra:
        attrs.update(
            {
                key: value
                for key, value in body.extra.items()
                if key not in {"override_warnings", "relationships"}
            }
        )
    person_id = str(uuid.uuid4())
    while person_id in tree.graph:
        person_id = str(uuid.uuid4())

    try:
        staged_graph = tree.graph.copy()
        staged_graph.add_node(person_id, **attrs)
        issues = validate_person_dates(attrs, person_id)
        staged_relationships = []
        for relationship in body.relationships or []:
            if relationship.new_person_role == "source":
                source, target = person_id, relationship.related_person_id
            else:
                source, target = relationship.related_person_id, person_id
            relationship_issues = validate_relationship(
                staged_graph,
                source,
                target,
                relationship.type,
                start_date=relationship.start_date,
            )
            issues.extend(relationship_issues)
            if not any(issue.severity == "error" for issue in relationship_issues):
                staged_graph.add_edge(
                    source,
                    target,
                    type=relationship.type,
                    start_date=relationship.start_date,
                )
            staged_relationships.append(
                (source, target, relationship.type, relationship.start_date)
            )
        enforce_issues(issues, override_warnings=body.override_warnings)

        def add_person_and_relationships():
            tree.add_person(
                id=person_id,
                override_warnings=True,
                **attrs,
            )
            for source, target, relationship_type, start_date in staged_relationships:
                tree.add_relationship(
                    source,
                    target,
                    type=relationship_type,
                    start_date=start_date,
                    override_warnings=True,
                )

        _, revision = apply_audited_change(
            tree=tree,
            store=history,
            actor=_actor(user),
            operation="create",
            entity_type="person",
            entity_id=person_id,
            mutation=add_person_and_relationships,
        )
    except TreeValidationError as exc:
        raise HTTPException(status_code=422, detail=exc.to_detail()) from exc
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"id": person_id, "revision_id": revision["id"]}


@router.put("/{person_id}")
def update_person(
    person_id: str,
    body: PersonUpdate,
    tree=Depends(get_tree),
    user=Depends(require_auth),
    history: ChangeHistoryStore = Depends(get_history_store),
):
    """Update person attributes. Send empty string to clear a field."""
    if tree.get_person(person_id) is None:
        raise HTTPException(status_code=404, detail="Person not found")
    attrs = body.model_dump(
        exclude_none=True,
        exclude={"extra", "override_warnings"},
    )
    if body.extra:
        attrs.update(
            {
                key: value
                for key, value in body.extra.items()
                if key not in {"clear_fields", "override_warnings"}
            }
        )
    # Handle field clearing: empty string means delete the attribute
    clear_fields = {key for key, value in attrs.items() if value == ""}
    attrs = {key: value for key, value in attrs.items() if key not in clear_fields}
    try:
        _, revision = apply_audited_change(
            tree=tree,
            store=history,
            actor=_actor(user),
            operation="update",
            entity_type="person",
            entity_id=person_id,
            mutation=lambda: tree.update_person(
                person_id,
                clear_fields=clear_fields,
                override_warnings=body.override_warnings,
                **attrs,
            ),
        )
    except TreeValidationError as exc:
        raise HTTPException(status_code=422, detail=exc.to_detail()) from exc
    return {"id": person_id, "updated": True, "revision_id": revision["id"]}


@router.post("/{person_id}/profilepic")
async def upload_profile_pic(
    person_id: str,
    file: UploadFile = File(...),
    tree=Depends(get_tree),
    user=Depends(require_auth),
    history: ChangeHistoryStore = Depends(get_history_store),
):
    """Upload a profile picture to Azure Storage and set it on the person."""
    if tree.get_person(person_id) is None:
        raise HTTPException(status_code=404, detail="Person not found")

    content = await _validate_image_upload(file)

    account = os.getenv("AZURE_STORAGE_ACCOUNT", "")
    key = os.getenv("AZURE_STORAGE_KEY", "")
    container = os.getenv("AZURE_STORAGE_PICS_CONTAINER", "familytreepics")

    if not account or not key:
        raise HTTPException(status_code=500, detail="Azure Storage not configured")

    try:
        from azure.storage.blob import BlobServiceClient

        ext = os.path.splitext(file.filename or ".jpg")[1] or ".jpg"
        blob_name = f"{uuid.uuid4()}{ext}"
        conn_str = f"DefaultEndpointsProtocol=https;AccountName={account};AccountKey={key}"
        blob_client = BlobServiceClient.from_connection_string(conn_str).get_blob_client(
            container=container, blob=blob_name
        )
        blob_client.upload_blob(content, overwrite=True)
        blob_url = f"https://{account}.blob.core.windows.net/{container}/{blob_name}"
        revision = _audit_person_change(
            tree=tree,
            history=history,
            user=user,
            person_id=person_id,
            mutation=lambda: tree.add_profile_picture(person_id, blob_url),
            change="profile_picture",
        )
        return {"url": blob_url, "revision_id": revision["id"]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Upload failed: {e}")


@router.post("/{person_id}/pictures")
async def upload_picture(
    person_id: str,
    file: UploadFile = File(...),
    tree=Depends(get_tree),
    user=Depends(require_auth),
    history: ChangeHistoryStore = Depends(get_history_store),
):
    """Upload a picture and add it to the person's pictures list.

    After upload, optionally tag other persons via PUT /api/persons/{id}/pictures/tag.
    """
    if tree.get_person(person_id) is None:
        raise HTTPException(status_code=404, detail="Person not found")

    content = await _validate_image_upload(file)

    account = os.getenv("AZURE_STORAGE_ACCOUNT", "")
    key = os.getenv("AZURE_STORAGE_KEY", "")
    container = os.getenv("AZURE_STORAGE_PICS_CONTAINER", "familytreepics")

    if not account or not key:
        raise HTTPException(status_code=500, detail="Azure Storage not configured")

    try:
        from azure.storage.blob import BlobServiceClient

        ext = os.path.splitext(file.filename or ".jpg")[1] or ".jpg"
        blob_name = f"{uuid.uuid4()}{ext}"
        conn_str = f"DefaultEndpointsProtocol=https;AccountName={account};AccountKey={key}"
        blob_client = BlobServiceClient.from_connection_string(conn_str).get_blob_client(
            container=container, blob=blob_name
        )
        blob_client.upload_blob(content, overwrite=True)
        blob_url = f"https://{account}.blob.core.windows.net/{container}/{blob_name}"
        revision = _audit_person_change(
            tree=tree,
            history=history,
            user=user,
            person_id=person_id,
            mutation=lambda: tree.add_picture(person_id, blob_url),
            change="picture_added",
        )
        return {"url": blob_url, "revision_id": revision["id"]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Upload failed: {e}")


from pydantic import BaseModel as _BaseModel


class _TagRequest(_BaseModel):
    url: str
    person_ids: list[str]


@router.put("/{person_id}/pictures/tag")
def tag_picture(
    person_id: str,
    body: _TagRequest,
    tree=Depends(get_tree),
    user=Depends(require_auth),
    history: ChangeHistoryStore = Depends(get_history_store),
):
    """Add a picture URL to additional persons' pictures lists (tagging)."""
    if tree.get_person(person_id) is None:
        raise HTTPException(status_code=404, detail="Person not found")
    tagged = []
    revisions = []
    for pid in body.person_ids:
        if tree.get_person(pid) is not None and pid != person_id:
            revision = _audit_person_change(
                tree=tree,
                history=history,
                user=user,
                person_id=pid,
                mutation=lambda pid=pid: tree.add_picture(pid, body.url),
                change="picture_tagged",
            )
            tagged.append(pid)
            revisions.append(revision["id"])
    return {"url": body.url, "tagged": tagged, "revision_ids": revisions}


class _RemovePicRequest(_BaseModel):
    url: str


@router.delete("/{person_id}/pictures")
def remove_picture(
    person_id: str,
    body: _RemovePicRequest,
    tree=Depends(get_tree),
    user=Depends(require_auth),
    history: ChangeHistoryStore = Depends(get_history_store),
):
    """Remove a picture URL from a person's pictures list."""
    if tree.get_person(person_id) is None:
        raise HTTPException(status_code=404, detail="Person not found")
    revision = _audit_person_change(
        tree=tree,
        history=history,
        user=user,
        person_id=person_id,
        mutation=lambda: tree.remove_picture(person_id, body.url),
        change="picture_removed",
    )
    return {"removed": True, "revision_id": revision["id"]}


class _PeopleInPicRequest(_BaseModel):
    url: str


@router.post("/{person_id}/pictures/people")
def get_people_in_picture(person_id: str, body: _PeopleInPicRequest, tree=Depends(get_tree)):
    """Return all persons tagged in a picture."""
    return tree.get_people_in_picture(body.url)


class _UntagRequest(_BaseModel):
    url: str
    person_id: str


@router.put("/{person_id}/pictures/untag")
def untag_picture(
    person_id: str,
    body: _UntagRequest,
    tree=Depends(get_tree),
    user=Depends(require_auth),
    history: ChangeHistoryStore = Depends(get_history_store),
):
    """Remove a picture URL from a specific person's pictures list (untag)."""
    target = tree.get_person(body.person_id)
    if target is None:
        raise HTTPException(status_code=404, detail="Target person not found")
    revision = _audit_person_change(
        tree=tree,
        history=history,
        user=user,
        person_id=body.person_id,
        mutation=lambda: tree.remove_picture(body.person_id, body.url),
        change="picture_untagged",
    )
    return {
        "url": body.url,
        "untagged": body.person_id,
        "revision_id": revision["id"],
    }


# ---- Notes -----------------------------------------------------------------

import json as _json
from datetime import datetime, timezone


class _NoteCreate(_BaseModel):
    text: str
    author: str


class _NoteDelete(_BaseModel):
    index: int


def _get_notes(tree, person_id: str) -> list[dict]:
    """Parse the notes JSON string from a person node."""
    raw = tree.graph.nodes[person_id].get("notes_json", "[]")
    try:
        return _json.loads(raw) if isinstance(raw, str) else []
    except (ValueError, TypeError):
        return []


def _save_notes(tree, person_id: str, notes: list[dict]) -> None:
    """Serialize notes back to the person node."""
    tree.graph.nodes[person_id]["notes_json"] = _json.dumps(notes)
    if tree.autosave:
        tree.save()


@router.get("/{person_id}/notes")
def get_notes(person_id: str, tree=Depends(get_tree)):
    """Get all notes for a person."""
    if tree.get_person(person_id) is None:
        raise HTTPException(status_code=404, detail="Person not found")
    return _get_notes(tree, person_id)


@router.post("/{person_id}/notes", status_code=201)
def add_note(
    person_id: str,
    body: _NoteCreate,
    tree=Depends(get_tree),
    user=Depends(require_auth),
    history: ChangeHistoryStore = Depends(get_history_store),
):
    """Add a note to a person."""
    if tree.get_person(person_id) is None:
        raise HTTPException(status_code=404, detail="Person not found")
    notes = _get_notes(tree, person_id)
    note = {
        "text": body.text,
        "author": body.author,
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }
    notes.append(note)
    revision = _audit_person_change(
        tree=tree,
        history=history,
        user=user,
        person_id=person_id,
        mutation=lambda: _save_notes(tree, person_id, notes),
        change="note_added",
    )
    return {**note, "revision_id": revision["id"]}


@router.delete("/{person_id}/notes/{note_index}")
def delete_note(
    person_id: str,
    note_index: int,
    tree=Depends(get_tree),
    user=Depends(require_auth),
    history: ChangeHistoryStore = Depends(get_history_store),
):
    """Delete a note by index."""
    if tree.get_person(person_id) is None:
        raise HTTPException(status_code=404, detail="Person not found")
    notes = _get_notes(tree, person_id)
    if note_index < 0 or note_index >= len(notes):
        raise HTTPException(status_code=404, detail="Note not found")
    notes.pop(note_index)
    revision = _audit_person_change(
        tree=tree,
        history=history,
        user=user,
        person_id=person_id,
        mutation=lambda: _save_notes(tree, person_id, notes),
        change="note_deleted",
    )
    return {"deleted": True, "revision_id": revision["id"]}


@router.delete("/{person_id}")
def delete_person(
    person_id: str,
    tree=Depends(get_tree),
    user=Depends(require_auth),
    history: ChangeHistoryStore = Depends(get_history_store),
):
    """Soft-delete a person by removing it from the active graph and journaling its tombstone."""
    if tree.get_person(person_id) is None:
        raise HTTPException(status_code=404, detail="Person not found")
    _, revision = apply_audited_change(
        tree=tree,
        store=history,
        actor=_actor(user),
        operation="delete",
        entity_type="person",
        entity_id=person_id,
        mutation=lambda: tree.delete_person(person_id),
    )
    return {"id": person_id, "deleted": True, "revision_id": revision["id"]}

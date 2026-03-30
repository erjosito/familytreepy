"""Person CRUD endpoints."""

import os
import uuid
from fastapi import APIRouter, HTTPException, Depends, UploadFile, File
from backend.app.models import PersonCreate, PersonUpdate, PersonResponse
from backend.app.dependencies import get_tree

router = APIRouter(prefix="/api/persons", tags=["persons"])


@router.get("", response_model=list[dict])
def list_persons(tree=Depends(get_tree)):
    """List all persons (summary: id + fullname)."""
    result = []
    for node_id in tree.graph.nodes():
        data = tree.graph.nodes[node_id]
        fullname = (data.get("firstname", "") + " " + data.get("lastname", "")).strip()
        result.append({"id": node_id, "fullname": fullname})
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
def create_person(body: PersonCreate, tree=Depends(get_tree)):
    """Add a new person."""
    attrs = body.model_dump(exclude_none=True, exclude={"extra"})
    if body.extra:
        attrs.update(body.extra)
    person_id = tree.add_person(**attrs)
    return {"id": person_id}


@router.put("/{person_id}")
def update_person(person_id: str, body: PersonUpdate, tree=Depends(get_tree)):
    """Update person attributes. Send empty string to clear a field."""
    if tree.get_person(person_id) is None:
        raise HTTPException(status_code=404, detail="Person not found")
    attrs = body.model_dump(exclude_none=True, exclude={"extra"})
    if body.extra:
        attrs.update(body.extra)
    # Handle field clearing: empty string means delete the attribute
    for key, value in list(attrs.items()):
        if value == "" and key in tree.graph.nodes[person_id]:
            del tree.graph.nodes[person_id][key]
            del attrs[key]
    tree.update_person(person_id, **attrs)
    return {"id": person_id, "updated": True}


@router.post("/{person_id}/profilepic")
async def upload_profile_pic(person_id: str, file: UploadFile = File(...), tree=Depends(get_tree)):
    """Upload a profile picture to Azure Storage and set it on the person."""
    if tree.get_person(person_id) is None:
        raise HTTPException(status_code=404, detail="Person not found")

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
        content = await file.read()
        blob_client.upload_blob(content, overwrite=True)
        blob_url = f"https://{account}.blob.core.windows.net/{container}/{blob_name}"
        tree.add_profile_picture(person_id, blob_url)
        return {"url": blob_url}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Upload failed: {e}")


@router.post("/{person_id}/pictures")
async def upload_picture(
    person_id: str,
    file: UploadFile = File(...),
    tree=Depends(get_tree),
):
    """Upload a picture and add it to the person's pictures list.

    After upload, optionally tag other persons via PUT /api/persons/{id}/pictures/tag.
    """
    if tree.get_person(person_id) is None:
        raise HTTPException(status_code=404, detail="Person not found")

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
        content = await file.read()
        blob_client.upload_blob(content, overwrite=True)
        blob_url = f"https://{account}.blob.core.windows.net/{container}/{blob_name}"
        tree.add_picture(person_id, blob_url)
        return {"url": blob_url}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Upload failed: {e}")


from pydantic import BaseModel as _BaseModel


class _TagRequest(_BaseModel):
    url: str
    person_ids: list[str]


@router.put("/{person_id}/pictures/tag")
def tag_picture(person_id: str, body: _TagRequest, tree=Depends(get_tree)):
    """Add a picture URL to additional persons' pictures lists (tagging)."""
    if tree.get_person(person_id) is None:
        raise HTTPException(status_code=404, detail="Person not found")
    tagged = []
    for pid in body.person_ids:
        if tree.get_person(pid) is not None and pid != person_id:
            tree.add_picture(pid, body.url)
            tagged.append(pid)
    return {"url": body.url, "tagged": tagged}


class _RemovePicRequest(_BaseModel):
    url: str


@router.delete("/{person_id}/pictures")
def remove_picture(person_id: str, body: _RemovePicRequest, tree=Depends(get_tree)):
    """Remove a picture URL from a person's pictures list."""
    if tree.get_person(person_id) is None:
        raise HTTPException(status_code=404, detail="Person not found")
    tree.remove_picture(person_id, body.url)
    return {"removed": True}


@router.delete("/{person_id}")
def delete_person(person_id: str, tree=Depends(get_tree)):
    """Delete a person."""
    if tree.get_person(person_id) is None:
        raise HTTPException(status_code=404, detail="Person not found")
    tree.delete_person(person_id)
    return {"id": person_id, "deleted": True}

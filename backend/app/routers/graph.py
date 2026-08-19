"""Graph, schema, renderer, and import endpoints."""

import os
import re
from urllib.parse import urlparse

from fastapi import APIRouter, HTTPException, Depends
from fastapi.responses import Response
from backend.app.models import GraphResponse, ImportGmlRequest
from backend.app.dependencies import get_tree, get_person_schema, get_relationship_schema
from backend.app.renderers import RendererRegistry
from backend.app.auth import require_auth

router = APIRouter(prefix="/api", tags=["graph"], dependencies=[Depends(require_auth)])

# Separate router for endpoints that don't require auth (e.g., image proxy used by Cytoscape)
public_router = APIRouter(prefix="/api", tags=["graph-public"])

# Regex for detecting private/reserved IP addresses in hostnames
_PRIVATE_IP_RE = re.compile(
    r"^(10\.|172\.(1[6-9]|2\d|3[01])\.|192\.168\.|169\.254\.|127\.|0\.0\.0\.0|localhost)"
)


def _get_sas_token() -> str:
    """Get or auto-generate a read-only SAS token for Azure Storage."""
    sas = os.getenv("AZURE_STORAGE_SAS", "")
    if not sas:
        account = os.getenv("AZURE_STORAGE_ACCOUNT", "")
        key = os.getenv("AZURE_STORAGE_KEY", "")
        if account and key:
            try:
                from datetime import datetime, timedelta, timezone
                from azure.storage.blob import generate_account_sas, ResourceTypes, AccountSasPermissions
                sas = generate_account_sas(
                    account_name=account,
                    account_key=key,
                    resource_types=ResourceTypes(object=True),
                    permission=AccountSasPermissions(read=True),
                    expiry=datetime.now(timezone.utc) + timedelta(hours=1),
                )
            except Exception:
                pass
    return sas


@router.get("/graph", response_model=GraphResponse)
def get_graph(
    root_id: str | None = None,
    degree: int | None = None,
    include_inactive: bool = False,
    tree=Depends(get_tree),
):
    """Get graph data (nodes + edges), optionally filtered to a subgraph."""
    if root_id and not tree.get_person(root_id):
        raise HTTPException(status_code=404, detail=f"Person '{root_id}' not found")
    return tree.format_for_api(root_id=root_id, degree=degree, include_inactive=include_inactive)


@router.get("/schema/person")
def get_person_schema_endpoint(schema=Depends(get_person_schema)):
    """Return the person attribute schema for dynamic form generation."""
    return schema.to_dict()


@router.get("/schema/relationships")
def get_relationship_schema_endpoint(schema=Depends(get_relationship_schema)):
    """Return the relationship type schema."""
    return schema.to_dict()


@router.get("/config/storage")
def get_storage_config():
    """Return storage config needed by the frontend (SAS token for image URLs)."""
    return {"sas_token": _get_sas_token()}


@public_router.get("/proxy/image")
def proxy_image(url: str):
    """Proxy an Azure Blob image to avoid browser CORS issues."""
    parsed = urlparse(url)
    if parsed.scheme != "https":
        raise HTTPException(status_code=400, detail="Only HTTPS URLs are allowed")
    hostname = (parsed.hostname or "").lower()
    if _PRIVATE_IP_RE.match(hostname):
        raise HTTPException(status_code=400, detail="Private/reserved addresses are not allowed")
    if not hostname.endswith(".blob.core.windows.net"):
        raise HTTPException(status_code=400, detail="Only Azure Blob Storage URLs are allowed")
    # Restrict to our own storage account only
    own_account = os.getenv("AZURE_STORAGE_ACCOUNT", "")
    if own_account and not hostname.startswith(f"{own_account}."):
        raise HTTPException(status_code=400, detail="Only images from the configured storage account are allowed")

    import requests as req
    sas = _get_sas_token()
    full_url = f"{url}?{sas}" if sas and "?" not in url else url
    try:
        resp = req.get(full_url, timeout=10)
        if resp.status_code != 200:
            raise HTTPException(status_code=resp.status_code, detail="Failed to fetch image")
        content_type = resp.headers.get("content-type", "image/jpeg")
        return Response(content=resp.content, media_type=content_type)
    except req.RequestException as e:
        raise HTTPException(status_code=502, detail=f"Image proxy error: {e}")


@router.post("/import/gml")
def import_gml(body: ImportGmlRequest, tree=Depends(get_tree)):
    """Import a tree from an Azure Blob Storage GML file."""
    import os
    from familytree import FamilyTree

    # Use server-configured credentials only (never accept credentials in request body)
    account = os.getenv("AZURE_STORAGE_ACCOUNT")
    key = os.getenv("AZURE_STORAGE_KEY")
    if not account or not key:
        raise HTTPException(status_code=400, detail="Azure Storage credentials not configured on server")

    try:
        source = FamilyTree(
            backend="azstorage",
            azstorage_account=account,
            azstorage_key=key,
            azstorage_container=body.azstorage_container,
            azstorage_blob=body.azstorage_blob,
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Failed to load GML: {e}")

    # Merge the imported graph into the current tree
    imported_count = 0
    for node_id, data in source.graph.nodes(data=True):
        if node_id not in tree.graph:
            tree.graph.add_node(node_id, **data)
            imported_count += 1
    for src, tgt, data in source.graph.edges(data=True):
        if not tree.graph.has_edge(src, tgt):
            tree.graph.add_edge(src, tgt, **data)
    tree.save()

    return {"imported_persons": imported_count, "total_persons": tree.graph.number_of_nodes()}


@router.get("/renderers")
def list_renderers():
    """Return list of available image renderer names + descriptions."""
    return RendererRegistry.list_all()


@router.post("/graph/image")
def generate_image(
    root_id: str,
    degree: int = 3,
    renderer: str = "classical_tree",
    canvas_width: int = 2000,
    canvas_height: int = 1500,
    font_scale: float = 1.0,
    line_width: int = 2,
    color_scheme: str = "sepia",
    tree=Depends(get_tree),
):
    """Generate a high-resolution image of a graph section."""
    r = RendererRegistry.get(renderer)
    if r is None:
        available = [x["name"] for x in RendererRegistry.list_all()]
        raise HTTPException(status_code=400, detail=f"Unknown renderer '{renderer}'. Available: {available}")
    if not tree.get_person(root_id):
        raise HTTPException(status_code=404, detail=f"Person '{root_id}' not found")
    subgraph = tree.get_subgraph_degrees(root_id, degree=degree)
    sas = _get_sas_token()
    opts: dict = {
        "root_id": root_id,
        "canvas_width": canvas_width,
        "canvas_height": canvas_height,
        "font_scale": font_scale,
        "line_width": line_width,
        "color_scheme": color_scheme,
    }
    if sas:
        opts["azure_storage_sas"] = sas
    image_bytes = r.render(subgraph, options=opts)
    return Response(content=image_bytes, media_type="image/png")

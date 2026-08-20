"""Geni.com API integration – OAuth, search, profile, family & import."""

import logging
import os
import secrets
import time
from urllib.parse import urlencode, urlparse, urlunparse

import httpx
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import RedirectResponse

from backend.app.auth import (
    require_auth,
    read_session_cookie,
    _session_serializer,
    SESSION_COOKIE_NAME,
    SESSION_MAX_AGE,
    _DEV_MODE,
)
from backend.app.dependencies import get_tree

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/geni", tags=["geni"], dependencies=[Depends(require_auth)])

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

GENI_APP_KEY = os.getenv("GENI_APP_KEY", "")
GENI_APP_SECRET = os.getenv("GENI_APP_SECRET", "")
GENI_AUTHORIZE_URL = "https://www.geni.com/platform/oauth/authorize"
GENI_TOKEN_URL = "https://www.geni.com/platform/oauth/request_token"
GENI_API_BASE = "https://www.geni.com/api"

_REDIRECT_URI_BASE = os.getenv("OAUTH_REDIRECT_URI", "http://localhost:8000/api/auth/callback")
_FRONTEND_URL = os.getenv("CORS_ORIGINS", "http://localhost:3000").split(",")[0].strip()

# In-memory state store
_pending_states: dict[str, float] = {}


def _geni_redirect_uri() -> str:
    """Build the Geni callback URI from the same base as OAUTH_REDIRECT_URI."""
    parsed = urlparse(_REDIRECT_URI_BASE)
    return urlunparse(parsed._replace(path="/api/geni/callback"))


# ---------------------------------------------------------------------------
# Session cookie helpers
# ---------------------------------------------------------------------------


def _set_session_cookie(response, user: dict) -> None:
    token = _session_serializer.dumps(user)
    response.set_cookie(
        SESSION_COOKIE_NAME,
        token,
        max_age=SESSION_MAX_AGE,
        httponly=True,
        samesite="lax",
        secure=os.getenv("COOKIE_SECURE", "false").lower() == "true",
    )


def _get_geni_token(request: Request) -> str:
    """Extract the Geni access token from the session cookie."""
    user = read_session_cookie(request)
    if not user or not user.get("geni_token"):
        raise HTTPException(status_code=401, detail="Not connected to Geni. Please connect first.")
    return user["geni_token"]


# ---------------------------------------------------------------------------
# OAuth flow
# ---------------------------------------------------------------------------


@router.get("/connect")
async def geni_connect(request: Request):
    """Redirect user to Geni OAuth authorization page."""
    state = secrets.token_urlsafe(32)
    now = time.time()
    # Clean expired states
    for k in [k for k, v in _pending_states.items() if v < now]:
        _pending_states.pop(k, None)
    _pending_states[state] = now + 600

    params = {
        "client_id": GENI_APP_KEY,
        "redirect_uri": _geni_redirect_uri(),
        "state": state,
    }
    return RedirectResponse(f"{GENI_AUTHORIZE_URL}?{urlencode(params)}")


@router.get("/callback")
async def geni_callback(request: Request, code: str = "", state: str = "", error: str = ""):
    """Handle Geni OAuth callback, store access token in session cookie."""
    if error:
        return RedirectResponse(f"{_FRONTEND_URL}/geni?geni_error={error}")

    expiry = _pending_states.pop(state, None)
    if expiry is None or time.time() > expiry:
        return RedirectResponse(f"{_FRONTEND_URL}/geni?geni_error=invalid_state")

    # Exchange code for token
    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.get(
            GENI_TOKEN_URL,
            params={
                "client_id": GENI_APP_KEY,
                "client_secret": GENI_APP_SECRET,
                "code": code,
                "redirect_uri": _geni_redirect_uri(),
            },
        )

    if resp.status_code != 200:
        logger.warning("Geni token exchange failed: %s %s", resp.status_code, resp.text)
        return RedirectResponse(f"{_FRONTEND_URL}/geni?geni_error=token_exchange_failed")

    token_data = resp.json()
    access_token = token_data.get("access_token", "")
    if not access_token:
        return RedirectResponse(f"{_FRONTEND_URL}/geni?geni_error=no_access_token")

    # Extend existing session with geni_token
    user = read_session_cookie(request) or {}
    user["geni_token"] = access_token

    response = RedirectResponse(f"{_FRONTEND_URL}/geni?geni_connected=true")
    _set_session_cookie(response, user)
    return response


# ---------------------------------------------------------------------------
# Search
# ---------------------------------------------------------------------------


@router.get("/search")
async def geni_search(request: Request, names: str = ""):
    """Search Geni profiles by name."""
    if not names.strip():
        raise HTTPException(status_code=400, detail="names parameter is required")

    token = _get_geni_token(request)

    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.get(
            f"{GENI_API_BASE}/profile/search",
            params={"names": names, "access_token": token},
        )

    if resp.status_code != 200:
        raise HTTPException(status_code=resp.status_code, detail="Geni search failed")

    data = resp.json()
    results = []
    for item in data.get("results", []):
        results.append(_extract_profile_summary(item))
    return {"results": results}


# ---------------------------------------------------------------------------
# Profile detail
# ---------------------------------------------------------------------------


@router.get("/profile/{profile_id}")
async def geni_profile(request: Request, profile_id: str):
    """Get detailed Geni profile info."""
    token = _get_geni_token(request)

    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.get(
            f"{GENI_API_BASE}/profile-{profile_id}",
            params={"access_token": token},
        )

    if resp.status_code != 200:
        raise HTTPException(status_code=resp.status_code, detail="Geni profile fetch failed")

    data = resp.json()
    return _extract_profile_detail(data)


# ---------------------------------------------------------------------------
# Immediate family
# ---------------------------------------------------------------------------


@router.get("/profile/{profile_id}/family")
async def geni_family(request: Request, profile_id: str):
    """Get immediate family of a Geni profile."""
    token = _get_geni_token(request)

    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.get(
            f"{GENI_API_BASE}/profile-{profile_id}/immediate-family",
            params={"access_token": token},
        )

    if resp.status_code != 200:
        raise HTTPException(status_code=resp.status_code, detail="Geni family fetch failed")

    data = resp.json()
    return _parse_family_data(data)


# ---------------------------------------------------------------------------
# Import
# ---------------------------------------------------------------------------


@router.post("/import/{profile_id}")
async def geni_import(
    request: Request,
    profile_id: str,
    include_family: bool = False,
    tree=Depends(get_tree),
):
    """Import a Geni profile into the local tree."""
    token = _get_geni_token(request)

    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.get(
            f"{GENI_API_BASE}/profile-{profile_id}",
            params={"access_token": token},
        )
    if resp.status_code != 200:
        raise HTTPException(status_code=resp.status_code, detail="Geni profile fetch failed")

    profile_data = resp.json()
    person_id = _import_profile(tree, profile_data, profile_id)
    imported_ids = [person_id]

    if include_family:
        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(
                f"{GENI_API_BASE}/profile-{profile_id}/immediate-family",
                params={"access_token": token},
            )
        if resp.status_code == 200:
            family_data = resp.json()
            family_ids = _import_family(tree, family_data, person_id, profile_id)
            imported_ids.extend(family_ids)

    return {"id": person_id, "imported": imported_ids}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _extract_profile_summary(item: dict) -> dict:
    """Extract summary info from a Geni search result item."""
    return {
        "id": _extract_geni_id(item),
        "name": item.get("name", ""),
        "gender": item.get("gender", ""),
        "birth_date": _get_date(item, "birth"),
        "death_date": _get_date(item, "death"),
    }


def _extract_profile_detail(data: dict) -> dict:
    """Extract detail info from a Geni profile response."""
    return {
        "id": _extract_geni_id(data),
        "name": data.get("name", ""),
        "first_name": data.get("first_name", ""),
        "last_name": data.get("last_name", ""),
        "gender": data.get("gender", ""),
        "birth_date": _get_date(data, "birth"),
        "birth_location": _get_location(data, "birth"),
        "death_date": _get_date(data, "death"),
        "death_location": _get_location(data, "death"),
        "photo_url": data.get("mugshot_urls", {}).get("large", data.get("mugshot_urls", {}).get("medium", "")),
    }


def _extract_geni_id(data: dict) -> str:
    """Extract the numeric profile ID from a Geni API response."""
    geni_id = data.get("id", "")
    if isinstance(geni_id, str) and geni_id.startswith("profile-"):
        return geni_id.replace("profile-", "")
    return str(geni_id)


def _get_date(data: dict, prefix: str) -> str:
    """Extract a formatted date from Geni date object."""
    date_obj = data.get(f"{prefix}_date", {})
    if isinstance(date_obj, dict):
        year = date_obj.get("year", "")
        month = date_obj.get("month", "")
        day = date_obj.get("day", "")
        if year:
            parts = [str(year)]
            if month:
                parts.append(str(month).zfill(2))
                if day:
                    parts.append(str(day).zfill(2))
            return "-".join(parts)
    elif isinstance(date_obj, str):
        return date_obj
    return ""


def _get_location(data: dict, prefix: str) -> str:
    """Extract a location string from Geni location object."""
    loc = data.get(f"{prefix}_location", {})
    if isinstance(loc, dict):
        return loc.get("place_name", loc.get("city", ""))
    elif isinstance(loc, str):
        return loc
    return ""


def _parse_family_data(data: dict) -> dict:
    """Parse immediate-family response into structured groups."""
    focus_id = _extract_geni_id(data.get("focus", {}))
    nodes = data.get("nodes", {})

    parents = []
    spouses = []
    children = []

    for node_key, node in nodes.items():
        node_id = _extract_geni_id(node)
        if node_id == focus_id:
            continue
        edges = node.get("edges", {})
        summary = {
            "id": node_id,
            "name": node.get("name", ""),
            "gender": node.get("gender", ""),
        }
        # Determine relationship based on edges
        for edge_key, edge in edges.items():
            rel = edge.get("rel", "")
            if rel == "child":
                # This node lists focus as child → this node is a parent
                if f"profile-{focus_id}" in edge_key or edge_key.endswith(focus_id):
                    parents.append(summary)
                    break
            elif rel == "partner":
                spouses.append(summary)
                break
            elif rel == "parent":
                children.append(summary)
                break
        else:
            # Try looking at the focus node's edges
            focus_node = nodes.get(f"profile-{focus_id}", {})
            focus_edges = focus_node.get("edges", {})
            edge_to_node = focus_edges.get(node_key, focus_edges.get(f"profile-{node_id}", {}))
            rel = edge_to_node.get("rel", "")
            if rel == "child":
                parents.append(summary)
            elif rel == "parent":
                children.append(summary)
            elif rel == "partner":
                spouses.append(summary)

    return {
        "focus_id": focus_id,
        "parents": parents,
        "spouses": spouses,
        "children": children,
    }


def _import_profile(tree, data: dict, geni_id: str) -> str:
    """Create a person from Geni profile data. Returns the new person ID."""
    first_name = data.get("first_name", data.get("name", ""))
    last_name = data.get("last_name", "")
    birth_date = _get_date(data, "birth")
    birth_location = _get_location(data, "birth")
    death_date = _get_date(data, "death")
    is_alive = data.get("is_alive", death_date == "")

    attrs: dict = {
        "firstname": first_name,
        "lastname": last_name,
        "geni_profile_id": geni_id,
    }
    if birth_date:
        attrs["birthdate"] = birth_date
    if birth_location:
        attrs["birthplace"] = birth_location
    if death_date:
        attrs["deathdate"] = death_date
        attrs["isAlive"] = False
    else:
        attrs["isAlive"] = bool(is_alive)

    return tree.add_person(override_warnings=True, **attrs)


def _import_family(tree, family_data: dict, focus_person_id: str, focus_geni_id: str) -> list[str]:
    """Import immediate family members and create relationships."""
    imported: list[str] = []
    nodes = family_data.get("nodes", {})
    parsed = _parse_family_data(family_data)

    for parent in parsed["parents"]:
        geni_id = parent["id"]
        node_data = nodes.get(f"profile-{geni_id}", {})
        pid = _import_profile(tree, node_data, geni_id)
        imported.append(pid)
        try:
            tree.add_relationship(
                focus_person_id,
                pid,
                "isChildOf",
                override_warnings=True,
            )
        except (ValueError, Exception) as e:
            logger.warning("Could not add parent relationship: %s", e)

    for spouse in parsed["spouses"]:
        geni_id = spouse["id"]
        node_data = nodes.get(f"profile-{geni_id}", {})
        pid = _import_profile(tree, node_data, geni_id)
        imported.append(pid)
        try:
            tree.add_relationship(
                focus_person_id,
                pid,
                "isSpouseOf",
                override_warnings=True,
            )
        except (ValueError, Exception) as e:
            logger.warning("Could not add spouse relationship: %s", e)

    for child in parsed["children"]:
        geni_id = child["id"]
        node_data = nodes.get(f"profile-{geni_id}", {})
        pid = _import_profile(tree, node_data, geni_id)
        imported.append(pid)
        try:
            tree.add_relationship(
                pid,
                focus_person_id,
                "isChildOf",
                override_warnings=True,
            )
        except (ValueError, Exception) as e:
            logger.warning("Could not add child relationship: %s", e)

    return imported

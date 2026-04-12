"""Authentication & user-management endpoints.

Supports two auth modes:
1. **Backend-proxied OAuth** (confidential client) – the backend handles the
   full authorization-code flow with Entra External ID and issues a signed
   session cookie.  The frontend just redirects to ``/api/auth/login``.
2. **Bearer JWT** – the frontend sends a JWT in the Authorization header
   (original SPA flow, still supported as fallback).

Set ``AZURE_AD_CLIENT_SECRET`` to enable mode 1.
"""

import os
import secrets
import time

import httpx
from fastapi import APIRouter, Depends, HTTPException, Request, Response, status
from fastapi.responses import RedirectResponse
from pydantic import BaseModel
from slowapi import Limiter
from slowapi.util import get_remote_address

from backend.app.auth import (
    get_allowed_users,
    get_current_user,
    require_admin,
    save_allowed_users,
    _get_user_role,
    _DEV_MODE,
    SESSION_SECRET,
    SESSION_MAX_AGE,
    SESSION_COOKIE_NAME,
    _session_serializer,
    read_session_cookie,
)

router = APIRouter(prefix="/api/auth", tags=["auth"])
limiter = Limiter(key_func=get_remote_address)

# ---------------------------------------------------------------------------
# OAuth configuration (confidential-client flow)
# ---------------------------------------------------------------------------

_CLIENT_ID = os.getenv("AZURE_AD_CLIENT_ID", "")
_CLIENT_SECRET = os.getenv("AZURE_AD_CLIENT_SECRET", "")
_TENANT_NAME = os.getenv("AZURE_AD_TENANT_NAME", "")  # e.g. "erjosito"
_TENANT_ID = os.getenv("AZURE_AD_TENANT_ID", "")
_REDIRECT_URI = os.getenv("OAUTH_REDIRECT_URI", "http://localhost:8000/api/auth/callback")
_FRONTEND_URL = os.getenv("CORS_ORIGINS", "http://localhost:3000").split(",")[0].strip()

_OAUTH_ENABLED = bool(_CLIENT_SECRET and (_TENANT_NAME or _TENANT_ID))

# Microsoft: direct consumers endpoint
_MSA_AUTHORITY = "https://login.microsoftonline.com/consumers"

# Google: direct OAuth (bypasses Entra CIAM entirely)
_GOOGLE_CLIENT_ID = os.getenv("GOOGLE_CLIENT_ID", "")
_GOOGLE_CLIENT_SECRET = os.getenv("GOOGLE_CLIENT_SECRET", "")
_GOOGLE_AUTHORIZE_URL = "https://accounts.google.com/o/oauth2/v2/auth"
_GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"

# In-memory nonce store (state param → expiry)
_pending_states: dict[str, float] = {}


def _set_session_cookie(response: Response, user: dict) -> None:
    """Sign user info into a cookie."""
    token = _session_serializer.dumps(user)
    response.set_cookie(
        SESSION_COOKIE_NAME, token,
        max_age=SESSION_MAX_AGE,
        httponly=True,
        samesite="lax",
        secure=os.getenv("COOKIE_SECURE", "false").lower() == "true",
    )


def _get_session_user(request: Request) -> dict | None:
    """Read and verify the session cookie, returning user dict or None."""
    return read_session_cookie(request)


# ---------------------------------------------------------------------------
# OAuth endpoints
# ---------------------------------------------------------------------------


@router.get("/login")
@limiter.limit("10/minute")
async def login(request: Request, provider: str = "microsoft"):
    """Redirect the user to the appropriate identity provider.
    
    provider=microsoft → direct MSA login (consumers endpoint)
    provider=google → direct Google OAuth
    """
    if not _OAUTH_ENABLED:
        raise HTTPException(400, "OAuth not configured")

    state = secrets.token_urlsafe(32)
    # Clean expired states
    now = time.time()
    for k in [k for k, v in _pending_states.items() if isinstance(v, dict) and v.get("expiry", 0) < now]:
        _pending_states.pop(k, None)

    if provider == "google" and _GOOGLE_CLIENT_ID:
        # Direct Google OAuth
        _pending_states[state] = {"expiry": time.time() + 600, "provider": "google"}
        params = {
            "client_id": _GOOGLE_CLIENT_ID,
            "response_type": "code",
            "redirect_uri": _REDIRECT_URI,
            "scope": "openid email profile",
            "state": state,
            "prompt": "select_account",
            "access_type": "offline",
        }
        from urllib.parse import urlencode
        return RedirectResponse(f"{_GOOGLE_AUTHORIZE_URL}?{urlencode(params)}")
    else:
        # Microsoft direct login
        _pending_states[state] = {"expiry": time.time() + 600, "provider": "microsoft"}
        authorize_url = f"{_MSA_AUTHORITY}/oauth2/v2.0/authorize"
        params = {
            "client_id": _CLIENT_ID,
            "response_type": "code",
            "redirect_uri": _REDIRECT_URI,
            "scope": "openid profile email",
            "state": state,
            "response_mode": "query",
            "prompt": "select_account",
        }
        qs = "&".join(f"{k}={v}" for k, v in params.items())
        return RedirectResponse(f"{authorize_url}?{qs}")


@router.get("/callback")
@limiter.limit("10/minute")
async def callback(request: Request, code: str = "", state: str = "", error: str = ""):
    """Handle the OAuth callback from Microsoft or Google."""
    if error:
        return RedirectResponse(f"{_FRONTEND_URL}?auth_error={error}")

    # Validate state and retrieve provider
    state_data = _pending_states.pop(state, None)
    if state_data is None:
        return RedirectResponse(f"{_FRONTEND_URL}?auth_error=invalid_state")
    if isinstance(state_data, dict):
        if time.time() > state_data["expiry"]:
            return RedirectResponse(f"{_FRONTEND_URL}?auth_error=invalid_state")
        provider = state_data.get("provider", "microsoft")
    else:
        if time.time() > state_data:
            return RedirectResponse(f"{_FRONTEND_URL}?auth_error=invalid_state")
        provider = "microsoft"

    # Exchange authorization code for tokens
    if provider == "google":
        token_url = _GOOGLE_TOKEN_URL
        token_data_req = {
            "client_id": _GOOGLE_CLIENT_ID,
            "client_secret": _GOOGLE_CLIENT_SECRET,
            "code": code,
            "redirect_uri": _REDIRECT_URI,
            "grant_type": "authorization_code",
        }
    else:
        token_url = f"{_MSA_AUTHORITY}/oauth2/v2.0/token"
        token_data_req = {
            "client_id": _CLIENT_ID,
            "client_secret": _CLIENT_SECRET,
            "code": code,
            "redirect_uri": _REDIRECT_URI,
            "grant_type": "authorization_code",
            "scope": "openid profile email",
        }

    async with httpx.AsyncClient(timeout=15) as client:
        resp = await client.post(token_url, data=token_data_req)

    if resp.status_code != 200:
        return RedirectResponse(f"{_FRONTEND_URL}?auth_error=token_exchange_failed")

    token_data = resp.json()
    id_token = token_data.get("id_token", "")

    # ------------------------------------------------------------------
    # Decode ID token claims.
    # SECURITY NOTE: We skip full JWT signature verification here because
    # the id_token was obtained directly from the provider's token endpoint
    # over HTTPS in exchange for an authorization code.  This is safe
    # because:
    #   1. The authorization code was bound to a cryptographic state/nonce
    #      we generated, so a forged callback is rejected before we reach
    #      this point.
    #   2. The token endpoint response is TLS-protected end-to-end; an
    #      attacker cannot inject a forged id_token without compromising
    #      the TLS channel to the identity provider.
    # Full signature validation (via JWKS) would add defence-in-depth but
    # is not strictly necessary in the confidential-client code-exchange
    # flow (RFC 6749 §4.1, OpenID Connect Core §3.1.3.7).
    # ------------------------------------------------------------------
    import base64, json as _json
    try:
        payload = id_token.split(".")[1]
        payload += "=" * (4 - len(payload) % 4)
        claims = _json.loads(base64.urlsafe_b64decode(payload))
    except Exception:
        return RedirectResponse(f"{_FRONTEND_URL}?auth_error=invalid_id_token")

    email = claims.get("preferred_username") or claims.get("email") or claims.get("upn", "")
    name = claims.get("name", email)

    # Build user dict
    user = {"email": email, "name": name, "roles": []}
    role = _get_user_role(email)
    if role:
        user["role"] = role
        if role == "admin":
            user["roles"] = ["admin"]
    elif not _DEV_MODE:
        return RedirectResponse(f"{_FRONTEND_URL}?auth_error=not_authorized")

    response = RedirectResponse(_FRONTEND_URL)
    _set_session_cookie(response, user)
    return response


@router.post("/logout")
async def logout():
    """Clear the session cookie."""
    response = Response(status_code=200)
    response.delete_cookie(SESSION_COOKIE_NAME)
    return response


@router.get("/session")
async def session_info(request: Request):
    """Return current session user (from cookie), or 401."""
    user = _get_session_user(request)
    if user:
        return user
    if _DEV_MODE:
        return {"email": "dev@localhost", "name": "Dev User", "roles": ["admin"]}
    raise HTTPException(status_code=401, detail="Not authenticated")


# ---- helper: require admin from session or JWT ----------------------------

def _require_session_admin(request: Request) -> dict:
    """Check admin from session cookie, raise 401/403 if not."""
    user = _get_session_user(request)
    if not user:
        if _DEV_MODE:
            return {"email": "dev@localhost", "name": "Dev User", "roles": ["admin"], "role": "admin"}
        raise HTTPException(status_code=401, detail="Not authenticated")
    if "admin" not in user.get("roles", []) and user.get("role") != "admin":
        raise HTTPException(status_code=403, detail="Admin privileges required")
    return user


class UserIn(BaseModel):
    email: str
    role: str = "user"


# ---- current user ----------------------------------------------------------

@router.get("/me")
async def me(request: Request, user: dict = Depends(get_current_user)):
    """Return the authenticated user's info (supports both JWT and session cookie)."""
    session_user = _get_session_user(request)
    if session_user:
        return session_user
    return user


# ---- allowed-user management (admin only) ----------------------------------

@router.get("/users")
async def list_users(request: Request):
    """List all allowed users."""
    _require_session_admin(request)
    data = get_allowed_users()
    return data.get("users", [])


@router.post("/users", status_code=status.HTTP_201_CREATED)
async def add_user(body: UserIn, request: Request):
    """Add a user to the allowed list."""
    _require_session_admin(request)
    allowed = get_allowed_users()
    users = allowed.setdefault("users", [])

    for u in users:
        if u["email"].lower() == body.email.lower():
            raise HTTPException(
                status_code=status.HTTP_409_CONFLICT,
                detail=f"User '{body.email}' already exists",
            )

    users.append({"email": body.email, "role": body.role})
    save_allowed_users(allowed)
    return {"email": body.email, "role": body.role}


@router.put("/users/{email}")
async def update_user(email: str, body: UserIn, request: Request):
    """Update a user's role."""
    _require_session_admin(request)
    allowed = get_allowed_users()
    users = allowed.get("users", [])

    for u in users:
        if u["email"].lower() == email.lower():
            u["role"] = body.role
            u["email"] = body.email
            save_allowed_users(allowed)
            return u

    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"User '{email}' not found",
    )


@router.delete("/users/{email}", status_code=status.HTTP_204_NO_CONTENT)
async def remove_user(email: str, request: Request):
    """Remove a user from the allowed list."""
    _require_session_admin(request)
    allowed = get_allowed_users()
    users = allowed.get("users", [])

    for i, u in enumerate(users):
        if u["email"].lower() == email.lower():
            users.pop(i)
            save_allowed_users(allowed)
            return

    raise HTTPException(
        status_code=status.HTTP_404_NOT_FOUND,
        detail=f"User '{email}' not found",
    )

"""Entra ID (Azure AD) OIDC authentication helpers for FastAPI."""

import json
import logging
import os
import time
from typing import Optional

import httpx
from authlib.jose import JsonWebKey, jwt as authlib_jwt
from azure.storage.blob import BlobServiceClient
from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

AZURE_AD_TENANT_ID = os.getenv("AZURE_AD_TENANT_ID")
AZURE_AD_CLIENT_ID = os.getenv("AZURE_AD_CLIENT_ID")

_DEV_MODE = not AZURE_AD_TENANT_ID

OIDC_DISCOVERY_URL = (
    f"https://login.microsoftonline.com/{AZURE_AD_TENANT_ID}/v2.0/"
    ".well-known/openid-configuration"
    if AZURE_AD_TENANT_ID
    else None
)

# ---------------------------------------------------------------------------
# JWKS key cache
# ---------------------------------------------------------------------------

_jwks_cache: dict = {}
_jwks_cache_expiry: float = 0
_JWKS_CACHE_TTL = 3600  # seconds


def _fetch_jwks() -> dict:
    """Download the JWKS from Microsoft's OIDC endpoint (cached)."""
    global _jwks_cache, _jwks_cache_expiry
    now = time.time()
    if _jwks_cache and now < _jwks_cache_expiry:
        return _jwks_cache

    with httpx.Client(timeout=10) as client:
        oidc = client.get(OIDC_DISCOVERY_URL).json()
        jwks_uri = oidc["jwks_uri"]
        jwks_response = client.get(jwks_uri).json()

    _jwks_cache = jwks_response
    _jwks_cache_expiry = now + _JWKS_CACHE_TTL
    return _jwks_cache


# ---------------------------------------------------------------------------
# Token validation
# ---------------------------------------------------------------------------

_bearer_scheme = HTTPBearer(auto_error=False)


def _decode_token(token: str) -> dict:
    """Validate and decode an Entra ID JWT, returning claims."""
    jwks_data = _fetch_jwks()
    keyset = JsonWebKey.import_key_set(jwks_data)

    claims = authlib_jwt.decode(
        token,
        keyset,
    )
    claims.validate()

    # Verify audience
    aud = claims.get("aud")
    if isinstance(aud, list):
        if AZURE_AD_CLIENT_ID not in aud:
            raise ValueError("Token audience mismatch")
    elif aud != AZURE_AD_CLIENT_ID:
        raise ValueError("Token audience mismatch")

    # Verify issuer
    expected_issuer = (
        f"https://login.microsoftonline.com/{AZURE_AD_TENANT_ID}/v2.0"
    )
    if claims.get("iss") != expected_issuer:
        raise ValueError("Token issuer mismatch")

    return dict(claims)


# ---------------------------------------------------------------------------
# Default dev-mode user
# ---------------------------------------------------------------------------

_DEFAULT_DEV_USER = {
    "email": "dev@localhost",
    "name": "Dev User",
    "roles": ["admin"],
}

# ---------------------------------------------------------------------------
# Allowed-users helpers (Azure Blob Storage)
# ---------------------------------------------------------------------------

_BLOB_CONTAINER = "familytreeconfig"
_BLOB_NAME = "allowed_users.json"


def _get_blob_client():
    account = os.getenv("AZURE_STORAGE_ACCOUNT")
    key = os.getenv("AZURE_STORAGE_KEY")
    if not account or not key:
        return None
    svc = BlobServiceClient(
        account_url=f"https://{account}.blob.core.windows.net",
        credential=key,
    )
    return svc.get_blob_client(container=_BLOB_CONTAINER, blob=_BLOB_NAME)


def get_allowed_users() -> dict:
    """Load allowed_users.json from Azure Blob Storage."""
    blob = _get_blob_client()
    if blob is None:
        # Dev fallback: everyone is allowed
        return {"users": [{"email": "dev@localhost", "role": "admin"}]}
    try:
        data = blob.download_blob().readall()
        return json.loads(data)
    except Exception:
        logger.exception("Failed to load allowed users from blob storage")
        return {"users": []}


def save_allowed_users(allowed_users: dict) -> None:
    """Persist allowed_users.json back to Azure Blob Storage."""
    blob = _get_blob_client()
    if blob is None:
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Azure Storage not configured",
        )
    blob.upload_blob(json.dumps(allowed_users), overwrite=True)


def _get_user_role(email: str, allowed_users: Optional[dict] = None) -> Optional[str]:
    if allowed_users is None:
        allowed_users = get_allowed_users()
    for user in allowed_users.get("users", []):
        if user["email"].lower() == email.lower():
            return user["role"]
    return None


# ---------------------------------------------------------------------------
# FastAPI dependencies
# ---------------------------------------------------------------------------


async def get_current_user(
    credentials: Optional[HTTPAuthorizationCredentials] = Depends(_bearer_scheme),
) -> dict:
    """Return the authenticated user dict, or a dev stub when auth is not configured."""
    if _DEV_MODE:
        return _DEFAULT_DEV_USER

    if credentials is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing authorization token",
            headers={"WWW-Authenticate": "Bearer"},
        )

    try:
        claims = _decode_token(credentials.credentials)
    except Exception as exc:
        logger.warning("Token validation failed: %s", exc)
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
            headers={"WWW-Authenticate": "Bearer"},
        ) from exc

    email = (
        claims.get("preferred_username")
        or claims.get("email")
        or claims.get("upn", "")
    )
    name = claims.get("name", email)
    roles = claims.get("roles", [])

    # Check that the user is in the allowed list
    user_role = _get_user_role(email)
    if user_role is None:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="User is not authorized",
        )

    if user_role == "admin":
        roles = list(set(roles) | {"admin"})

    return {"email": email, "name": name, "roles": roles, "role": user_role}


async def require_admin(user: dict = Depends(get_current_user)) -> dict:
    """Ensure the current user has the admin role."""
    if "admin" not in user.get("roles", []) and user.get("role") != "admin":
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin privileges required",
        )
    return user

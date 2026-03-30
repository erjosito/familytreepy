"""Authentication & user-management endpoints."""

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel

from backend.app.auth import (
    get_allowed_users,
    get_current_user,
    require_admin,
    save_allowed_users,
)

router = APIRouter(prefix="/api/auth", tags=["auth"])


class UserIn(BaseModel):
    email: str
    role: str = "user"


# ---- current user ----------------------------------------------------------

@router.get("/me")
async def me(user: dict = Depends(get_current_user)):
    """Return the authenticated user's info."""
    return user


# ---- allowed-user management (admin only) ----------------------------------

@router.get("/users")
async def list_users(admin: dict = Depends(require_admin)):
    """List all allowed users."""
    data = get_allowed_users()
    return data.get("users", [])


@router.post("/users", status_code=status.HTTP_201_CREATED)
async def add_user(body: UserIn, admin: dict = Depends(require_admin)):
    """Add a user to the allowed list."""
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


@router.delete("/users/{email}", status_code=status.HTTP_204_NO_CONTENT)
async def remove_user(email: str, admin: dict = Depends(require_admin)):
    """Remove a user from the allowed list."""
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

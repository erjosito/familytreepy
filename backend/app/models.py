"""Pydantic models for API requests and responses."""

from typing import Any
from pydantic import BaseModel


class PersonCreate(BaseModel):
    firstname: str | None = None
    lastname: str | None = None
    birthdate: str | None = None
    birthplace: str | None = None
    isAlive: bool | None = True
    deathdate: str | None = None
    profilepic: str | None = None
    pictures: list[str] | None = None
    extra: dict[str, Any] | None = None


class PersonUpdate(BaseModel):
    firstname: str | None = None
    lastname: str | None = None
    birthdate: str | None = None
    birthplace: str | None = None
    isAlive: bool | None = None
    deathdate: str | None = None
    profilepic: str | None = None
    pictures: list[str] | None = None
    extra: dict[str, Any] | None = None


class PersonResponse(BaseModel):
    id: str
    fullname: str
    firstname: str | None = None
    lastname: str | None = None
    birthdate: str | None = None
    birthplace: str | None = None
    isAlive: bool | None = None
    deathdate: str | None = None
    profilepic: str | None = None
    pictures: list[str] | None = None


class RelationshipCreate(BaseModel):
    source: str
    target: str
    type: str
    start_date: str | None = None


class RelationshipDeactivate(BaseModel):
    end_date: str | None = None


class RelationshipResponse(BaseModel):
    source: str
    target: str
    type: str
    is_active: bool | None = True
    start_date: str | None = None
    end_date: str | None = None


class GraphResponse(BaseModel):
    nodes: list[dict[str, Any]]
    edges: list[dict[str, Any]]


class ImportGmlRequest(BaseModel):
    azstorage_account: str | None = None
    azstorage_key: str | None = None
    azstorage_container: str
    azstorage_blob: str

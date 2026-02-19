"""Pydantic v2 schemas for request/response validation."""
import uuid
from datetime import datetime
from typing import Optional

from pydantic import BaseModel, EmailStr, field_validator


# ── User ──────────────────────────────────────────────────────────────────────

class UserSyncRequest(BaseModel):
    clerk_id: str
    email: str
    full_name: Optional[str] = None
    avatar_url: Optional[str] = None


class UserResponse(BaseModel):
    id: uuid.UUID
    clerk_id: str
    email: str
    full_name: Optional[str] = None
    avatar_url: Optional[str] = None
    created_at: datetime

    model_config = {"from_attributes": True}


# ── DB Connection ─────────────────────────────────────────────────────────────

class DBConnectionCreate(BaseModel):
    name: str
    connection_string: str  # raw — will be encrypted before storage


class DBConnectionResponse(BaseModel):
    id: uuid.UUID
    name: str
    pinecone_namespace: Optional[str] = None
    table_count: Optional[int] = None
    indexed_at: Optional[datetime] = None
    is_active: bool
    created_at: datetime

    model_config = {"from_attributes": True}


# ── Query ─────────────────────────────────────────────────────────────────────

class QueryRequest(BaseModel):
    connection_id: uuid.UUID
    nl_query: str


class QueryResult(BaseModel):
    columns: list[str]
    rows: list[list]
    exec_time_ms: int
    row_count: int


# ── Query Log ─────────────────────────────────────────────────────────────────

class QueryLogResponse(BaseModel):
    id: uuid.UUID
    connection_id: uuid.UUID
    nl_query: str
    generated_sql: Optional[str] = None
    row_count: Optional[int] = None
    exec_time_ms: Optional[int] = None
    status: str
    error_message: Optional[str] = None
    created_at: datetime

    model_config = {"from_attributes": True}

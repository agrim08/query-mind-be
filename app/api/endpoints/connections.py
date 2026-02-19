"""Connections endpoint — CRUD for user DB connections + schema indexing trigger."""
import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, Request, status
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.api.deps import get_current_user
from app.core.security import encrypt, decrypt
from app.db.session import get_db
from app.models.models import DBConnection, User
from app.schemas.schemas import DBConnectionCreate, DBConnectionResponse
from app.services.schema_indexer import index_schema

router = APIRouter()


def _to_asyncpg(conn_str: str) -> str:
    """Rewrite a plain postgresql(s):// or postgres:// URL to use the asyncpg driver
    and strip / translate psycopg2-style query params that asyncpg doesn't understand.

    Uses regex-based param stripping (not urlparse) because non-standard schemes
    like postgresql+asyncpg:// are not reliably parsed by Python's urlparse.

    Handled:
    - Scheme rewritten to postgresql+asyncpg://
    - sslmode removed entirely (asyncpg handles SSL via connect_args, not URL params)
    - All other psycopg2-only / libpq-only params are stripped
    """
    import re as _re

    # 1. Normalise scheme
    for old, new in [
        ("postgresql+psycopg2://", "postgresql+asyncpg://"),
        ("postgresql+psycopg://",  "postgresql+asyncpg://"),
        ("postgres://",            "postgresql+asyncpg://"),
        ("postgresql://",          "postgresql+asyncpg://"),
    ]:
        if conn_str.startswith(old):
            conn_str = new + conn_str[len(old):]
            break

    # 2. Params to strip entirely from the URL query string.
    #    These are psycopg2/libpq params that asyncpg will reject.
    _STRIP_PARAMS = {
        "sslmode", "channel_binding", "options", "application_name",
        "target_session_attrs", "connect_timeout", "fallback_application_name",
        "keepalives", "keepalives_idle", "keepalives_interval", "keepalives_count",
        "tcp_user_timeout", "gssencmode", "krbsrvname", "passfile",
    }

    # Strip each forbidden param and any trailing & or leading & left behind
    for param in _STRIP_PARAMS:
        # Matches: param=value at start/?/mid/end of query string
        conn_str = _re.sub(
            rf"([?&]){_re.escape(param)}=[^&]*(&?)",
            lambda m: (m.group(1) if m.group(2) else ""),
            conn_str,
        )

    # Clean up any trailing ? or & with nothing after it
    conn_str = _re.sub(r"[?&]$", "", conn_str)

    return conn_str



@router.get("/", response_model=list[DBConnectionResponse])
async def list_connections(
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> list[DBConnectionResponse]:
    result = await db.execute(
        select(DBConnection).where(DBConnection.user_id == current_user.id)
    )
    connections = result.scalars().all()
    return [DBConnectionResponse.model_validate(c) for c in connections]


class _TestRequest(BaseModel):
    conn_string: str


@router.post("/test")
async def test_connection(
    payload: _TestRequest,
    current_user: User = Depends(get_current_user),
) -> dict:
    """Quickly validate that a connection string is reachable (does not persist anything)."""
    async_conn_str = _to_asyncpg(payload.conn_string)
    try:
        from sqlalchemy.ext.asyncio import create_async_engine
        engine = create_async_engine(async_conn_str, pool_pre_ping=True)
        async with engine.connect():
            pass
        await engine.dispose()
        return {"ok": True}
    except Exception as exc:
        return {"ok": False, "error": str(exc)}



@router.post("/", response_model=DBConnectionResponse, status_code=status.HTTP_201_CREATED)
async def create_connection(
    payload: DBConnectionCreate,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> DBConnectionResponse:
    """Create a new DB connection. The connection string is Fernet-encrypted before storage."""

    # Normalise the scheme so the async engine always uses asyncpg.
    # Users typically paste postgresql:// or postgres:// — both need +asyncpg for async SQLAlchemy.
    async_conn_str = _to_asyncpg(payload.connection_string)

    # Test the connection before saving
    try:
        from sqlalchemy.ext.asyncio import create_async_engine
        test_engine = create_async_engine(async_conn_str, pool_pre_ping=True)
        async with test_engine.connect():
            pass
        await test_engine.dispose()
    except Exception as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Could not connect to database: {exc}",
        )

    namespace = f"user-{current_user.id}-conn-{uuid.uuid4().hex[:8]}"
    connection = DBConnection(
        user_id=current_user.id,
        name=payload.name,
        # Store the normalised async-compatible string so we can use it for indexing too
        encrypted_conn_string=encrypt(async_conn_str),
        pinecone_namespace=namespace,
    )
    db.add(connection)
    await db.commit()
    await db.refresh(connection)
    return DBConnectionResponse.model_validate(connection)



@router.delete("/{connection_id}", status_code=status.HTTP_204_NO_CONTENT)
async def delete_connection(
    connection_id: uuid.UUID,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> None:
    result = await db.execute(
        select(DBConnection).where(
            DBConnection.id == connection_id,
            DBConnection.user_id == current_user.id,
        )
    )
    connection = result.scalar_one_or_none()
    if not connection:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Connection not found")
    await db.delete(connection)
    await db.commit()


@router.post("/{connection_id}/index")
async def trigger_indexing(
    connection_id: uuid.UUID,
    current_user: User = Depends(get_current_user),
    db: AsyncSession = Depends(get_db),
) -> StreamingResponse:
    """Trigger schema indexing for a connection. Streams SSE progress events."""
    result = await db.execute(
        select(DBConnection).where(
            DBConnection.id == connection_id,
            DBConnection.user_id == current_user.id,
        )
    )
    connection = result.scalar_one_or_none()
    if not connection:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Connection not found")

    async def _stream():
        table_count = 0
        async for event in index_schema(
            connection.encrypted_conn_string, connection.pinecone_namespace
        ):
            yield event
            # Parse done event to update DB
            import json
            try:
                data = json.loads(event.removeprefix("data: ").strip())
                if data.get("type") == "done":
                    table_count = data.get("table_count", 0)
            except Exception:
                pass

        # Update connection metadata after indexing
        connection.table_count = table_count
        connection.indexed_at = datetime.now(timezone.utc)
        await db.commit()

    return StreamingResponse(
        _stream(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )

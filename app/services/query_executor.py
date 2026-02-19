"""Query Executor — runs a validated SELECT on the user's database.

Key safety guarantees:
- Fresh async connection per request (no shared pool with user DBs).
- SET statement_timeout enforced before every query.
- Hard cap of 500 rows returned.
- Connection is always closed after execution.
"""
import time
from dataclasses import dataclass

from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from app.core.security import decrypt

MAX_ROWS = 500
STATEMENT_TIMEOUT_MS = 10_000  # 10 seconds


@dataclass
class QueryResult:
    columns: list[str]
    rows: list[list]
    exec_time_ms: int
    row_count: int


async def execute_query(encrypted_conn_string: str, sql: str) -> QueryResult:
    """
    Execute a validated SELECT query on the user's database.

    Args:
        encrypted_conn_string: Fernet-encrypted connection string from DB.
        sql: Validated SELECT SQL to execute.

    Returns:
        QueryResult with columns, rows, timing, and row count.

    Raises:
        Exception: Propagates DB errors to the caller for SSE error events.
    """
    from app.api.endpoints.connections import _to_asyncpg  # avoid circular at module level

    conn_string = decrypt(encrypted_conn_string)
    # Re-normalise in case the string was stored before our scheme-fix was deployed
    conn_string = _to_asyncpg(conn_string)

    # Fresh engine per request — no persistent pool for user DBs.
    engine = create_async_engine(conn_string, pool_pre_ping=True)

    try:
        async with engine.connect() as conn:
            # Enforce a hard statement timeout.
            # PostgreSQL accepts: SET statement_timeout = <integer_ms>
            await conn.execute(text(f"SET statement_timeout = {STATEMENT_TIMEOUT_MS}"))

            start = time.perf_counter()
            result = await conn.execute(text(sql))
            elapsed_ms = int((time.perf_counter() - start) * 1000)

            columns = list(result.keys())
            rows = [list(row) for row in result.fetchmany(MAX_ROWS)]

            return QueryResult(
                columns=columns,
                rows=rows,
                exec_time_ms=elapsed_ms,
                row_count=len(rows),
            )
    finally:
        await engine.dispose()

"""SQL Generator — streams SQL from Gemini 2.5 Flash given schema context + NL query.

Yields raw text chunks as they arrive from the model.
"""
import asyncio
from typing import AsyncGenerator

from google import genai
from google.genai import types as genai_types

from app.core.config import settings
from app.services.schema_retriever import TableDoc

# Hard-coded per spec — no fallback
MODEL_NAME = "gemini-2.5-flash"

SYSTEM_PROMPT = """You are an expert PostgreSQL query writer.

Rules you MUST follow:
1. Return ONLY the raw SQL query — no markdown, no code blocks, no explanation.
2. Write only SELECT statements. Never use INSERT, UPDATE, DELETE, DROP, ALTER, TRUNCATE, or any DDL/DML.
3. Use proper PostgreSQL syntax.
4. CRITICAL: You may ONLY reference tables that are explicitly listed in the "Available tables" section of the prompt.
   Never infer, guess, or join tables that are not in that list — even if a column name implies a related table exists.
5. If the question cannot be answered using ONLY the available tables and their columns, return: -- Cannot answer: <reason>
6. Always qualify column names when joining tables to avoid ambiguity.
7. Use LIMIT 500 if the query could return many rows.
8. CRITICAL: Always wrap ALL table names and ALL column names in double quotes (e.g., "users", "screenConfig", "projectId").
9. JOIN LOGIC: Use explicit JOINs based on foreign keys described in the schema. If the user asks for email but a table doesn't have it, join with the "users" table.
10. ALIASING: If you assign an alias to a table (e.g., "table" AS "t"), you MUST use that alias for all column references (e.g., "t"."column"). Never use the original table name if an alias exists.
"""


def _build_prompt(nl_query: str, table_docs: list[TableDoc]) -> str:
    schema_section = "\n\n".join(doc.doc for doc in table_docs)
    # Explicitly list available tables so the model cannot claim ignorance
    available_tables = ", ".join(doc.table_name for doc in table_docs)
    return (
        f"Available tables (you may ONLY use these): {available_tables}\n\n"
        f"Database Schema:\n{schema_section}\n\n"
        f"Question: {nl_query}\n\n"
        f"SQL Query:"
    )


async def stream_sql(
    nl_query: str,
    table_docs: list[TableDoc],
) -> AsyncGenerator[str, None]:
    """
    Stream SQL tokens from Gemini 2.5 Flash.

    Yields text chunks as they arrive. The caller is responsible for
    assembling the full SQL string for validation.
    """
    client = genai.Client(api_key=settings.GOOGLE_API_KEY)
    prompt = _build_prompt(nl_query, table_docs)

    # Run the blocking streaming call in a thread executor
    loop = asyncio.get_event_loop()
    response_iter = await loop.run_in_executor(
        None,
        lambda: client.models.generate_content_stream(
            model=MODEL_NAME,
            contents=prompt,
            config=genai_types.GenerateContentConfig(
                system_instruction=SYSTEM_PROMPT,
                temperature=0.1,
                max_output_tokens=1024,
            ),
        ),
    )

    for chunk in response_iter:
        text = chunk.text if hasattr(chunk, "text") and chunk.text else ""
        if text:
            yield text

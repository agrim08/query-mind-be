"""Unit tests for sql_generator — mocks the Gemini API, tests prompt building.

Run with: pytest app/tests/test_sql_generator.py -v
"""
import sys
import os
import asyncio
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from app.services.sql_generator import _build_prompt, MODEL_NAME, SYSTEM_PROMPT
from app.services.schema_retriever import TableDoc


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture
def sample_table_docs():
    return [
        TableDoc(
            table_name="users",
            doc="Table: users\nColumns:\n  - id (INTEGER) NOT NULL\n  - email (VARCHAR) NOT NULL\n  - name (VARCHAR)",
            score=0.95,
        ),
        TableDoc(
            table_name="orders",
            doc="Table: orders\nColumns:\n  - id (INTEGER) NOT NULL\n  - user_id (INTEGER) NOT NULL\n  - total (NUMERIC)",
            score=0.88,
        ),
    ]


# ── Prompt Building ───────────────────────────────────────────────────────────

class TestBuildPrompt:
    def test_contains_nl_query(self, sample_table_docs):
        prompt = _build_prompt("How many users are there?", sample_table_docs)
        assert "How many users are there?" in prompt

    def test_contains_table_docs(self, sample_table_docs):
        prompt = _build_prompt("show me all users", sample_table_docs)
        assert "Table: users" in prompt
        assert "Table: orders" in prompt

    def test_prompt_ends_with_sql_query_marker(self, sample_table_docs):
        prompt = _build_prompt("show me all users", sample_table_docs)
        assert prompt.strip().endswith("SQL Query:")

    def test_empty_table_docs(self):
        prompt = _build_prompt("show me all users", [])
        assert "SQL Query:" in prompt
        assert "Database Schema:" in prompt

    def test_schema_section_separator(self, sample_table_docs):
        """Each table doc should be separated by double newline."""
        prompt = _build_prompt("test", sample_table_docs)
        assert "Table: users" in prompt
        assert "Table: orders" in prompt


# ── Model Config ──────────────────────────────────────────────────────────────

class TestModelConfig:
    def test_model_name_is_gemini_flash(self):
        """Hard rule: must use gemini-2.5-flash, no fallback."""
        assert MODEL_NAME == "gemini-2.5-flash"

    def test_system_prompt_forbids_mutations(self):
        """System prompt must explicitly forbid mutating statements."""
        forbidden = ["INSERT", "UPDATE", "DELETE", "DROP", "ALTER", "TRUNCATE"]
        for keyword in forbidden:
            assert keyword in SYSTEM_PROMPT, f"System prompt missing: {keyword}"

    def test_system_prompt_requires_select_only(self):
        assert "SELECT" in SYSTEM_PROMPT

    def test_system_prompt_requires_raw_sql(self):
        """Must instruct model to return raw SQL without markdown."""
        assert "raw SQL" in SYSTEM_PROMPT or "ONLY" in SYSTEM_PROMPT


# ── Streaming (mocked) ────────────────────────────────────────────────────────

class TestStreamSql:
    def test_stream_yields_chunks(self, sample_table_docs):
        """stream_sql should yield text chunks from the model response."""
        from app.services.sql_generator import stream_sql

        # Mock chunk objects returned by generate_content_stream
        mock_chunk_1 = MagicMock()
        mock_chunk_1.text = "SELECT COUNT(*)"
        mock_chunk_2 = MagicMock()
        mock_chunk_2.text = " FROM users"

        mock_client = MagicMock()
        mock_client.models.generate_content_stream.return_value = iter(
            [mock_chunk_1, mock_chunk_2]
        )

        with patch("app.services.sql_generator.genai") as mock_genai:
            mock_genai.Client.return_value = mock_client

            async def collect():
                chunks = []
                async for chunk in stream_sql("count users", sample_table_docs):
                    chunks.append(chunk)
                return chunks

            chunks = asyncio.run(collect())

        assert chunks == ["SELECT COUNT(*)", " FROM users"]
        full_sql = "".join(chunks)
        assert "SELECT" in full_sql

    def test_stream_skips_empty_chunks(self, sample_table_docs):
        """Chunks with empty text should not be yielded."""
        from app.services.sql_generator import stream_sql

        mock_chunk_empty = MagicMock()
        mock_chunk_empty.text = ""
        mock_chunk_real = MagicMock()
        mock_chunk_real.text = "SELECT 1"

        mock_client = MagicMock()
        mock_client.models.generate_content_stream.return_value = iter(
            [mock_chunk_empty, mock_chunk_real]
        )

        with patch("app.services.sql_generator.genai") as mock_genai:
            mock_genai.Client.return_value = mock_client

            async def collect():
                return [c async for c in stream_sql("test", sample_table_docs)]

            chunks = asyncio.run(collect())

        assert chunks == ["SELECT 1"]
        assert "" not in chunks

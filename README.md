# 🚀 QueryMind Backend: The RAG SQL Engine

The intelligence layer of QueryMind, built for scale, security, and precision. This FastAPI-based service orchestrates the complex flow from raw English text to validated PostgreSQL queries.

---

## 🛠 Technical Highlights

- **Asynchronous Pipeline**: Built on **Python 3.11** and **FastAPI**, leveraging `asyncio` for non-blocking I/O across database operations and AI streaming.
- **Smart Schema Indexing**: Uses **Pinecone Serverless** and **Gemini Embeddings** to semantically index database schemas. This allows the system to handle massive databases by only feeding relevant table context to the LLM.
- **Streaming SQL Generation**: Implements **Server-Sent Events (SSE)** to stream SQL tokens directly from **Gemini 2.5 Flash** as they are generated, providing a snappy, real-time user experience.
- **Production-Grade Security**:
  - **Fernet Encryption**: User database credentials are encrypted at rest using AES-128 via the `cryptography` library.
  - **SQL Guardrails**: Custom validation engine using `sqlparse` to block DDL/DML and enforce a strict keyword policy.
  - **Read-Only Enforcement**: Every user query is executed via a dedicated read-only connection.

---

## 🧬 RAG Pipeline Architecture

1. **Inspection**: SQLAlchemy `inspect` extracts tables, columns, types, and foreign keys.
2. **Vectorization**: Table metadata is flattened into "Table Docs" and embedded via `models/gemini-embedding-001`.
3. **Retrieval**: User queries are embedded to find the top-K relevant tables in Pinecone.
4. **Context Injection**: Retreived schema is injected into a specialized System Prompt.
5. **Generation & Validation**: SQL is generated, validated for structure/safety, and then executed.

---

## 📡 API Overview

- `POST /api/v1/users/sync`: Onboard and synchronize Clerk users.
- `GET /api/v1/connections`: List, test, and manage database connections.
- `POST /api/v1/connections/{id}/index`: Trigger the background schema indexing process.
- `POST /api/v1/query`: The core RAG endpoint (SSE) that transforms text to SQL.

---

## 💻 Tech Stack

- **FastAPI**: Modern Web Framework.
- **SQLAlchemy 2.0**: Next-gen ORM with full Async support.
- **Google GenAI SDK**: Direct integration with Gemini 2.5.
- **Pinecone**: Vector database for schema search.
- **jose**: JWT verification for secure Clerk authentication.

---

## 🛠 Local Setup

1. Install dependencies: `pip install -r requirements.txt`
2. Configure `.env` with your API keys.
3. Start the dev server: `fastapi dev app/main.py`

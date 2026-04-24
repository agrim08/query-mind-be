<div align="center">

<br />

<!-- Replace with actual logo once generated -->
<img src="public/logo-horizontal.svg" alt="QueryMind" height="40" />

<br />
<br />

**Your database, in plain English.**

Ask questions. Get SQL. See results. No syntax required.

<br />

[![Live Demo](https://img.shields.io/badge/Live%20Demo-querymind.app-c8f04d?style=flat-square&labelColor=080909&color=c8f04d)](https://querymind.app)
[![Next.js](https://img.shields.io/badge/Next.js-15-080909?style=flat-square&logo=nextdotjs&logoColor=white)](https://nextjs.org)
[![FastAPI](https://img.shields.io/badge/FastAPI-Python%203.11-080909?style=flat-square&logo=fastapi&logoColor=white)](https://fastapi.tiangolo.com)
[![Gemini](https://img.shields.io/badge/Gemini-2.5%20Flash-080909?style=flat-square&logo=google&logoColor=white)](https://deepmind.google/technologies/gemini)
[![Pinecone](https://img.shields.io/badge/Pinecone-Serverless-080909?style=flat-square)](https://pinecone.io)
[![Neon](https://img.shields.io/badge/Neon-PostgreSQL-080909?style=flat-square&logo=postgresql&logoColor=white)](https://neon.tech)

<br />

![QueryMind Demo](public/og-image.png)

</div>

---

## What is QueryMind?

QueryMind is a full-stack AI application that translates natural language into validated, executable SQL — and runs it against your PostgreSQL database in real time.

You connect a database. You type a question. QueryMind retrieves the relevant schema from Pinecone, generates precise SQL using Gemini 2.5 Flash via a streaming SSE pipeline, validates it for safety, executes it on your Neon DB, and returns the results — all in under two seconds.

It also ships a **Schema Designer**: describe your database in plain English and get a full ER diagram on an interactive React Flow canvas, with auto-drawn foreign key relationships and PDF/SQL export.

No BI tool setup. No SQL editor. No developer in the loop.

---

## RAG Pipeline

```
User question
     │
     ▼
Gemini text-embedding-004
(embed the question)
     │
     ▼
Pinecone vector search
(top-6 relevant table docs by cosine similarity)
     │
     ▼
Gemini 2.5 Flash
(schema context + question → SQL, streamed via SSE)
     │
     ▼
SQL Validator
(keyword blocklist + sqlparse + table existence check)
     │
     ▼
Query Executor
(read-only async connection → Neon DB → 500-row cap)
     │
     ▼
Results → Frontend
(streamed, rendered, exportable as CSV)
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| **Frontend** | Next.js 15 (App Router), TypeScript, Tailwind CSS v4, shadcn/ui |
| **Auth** | Clerk — JWT verified on every backend request |
| **Backend** | Python 3.11, FastAPI (async), SQLAlchemy 2.0, Alembic |
| **LLM** | Gemini 2.5 Flash (`gemini-2.5-flash`) |
| **Embeddings** | Gemini `models/text-embedding-004` |
| **Vector DB** | Pinecone Serverless — per-connection namespaces |
| **App DB** | Neon PostgreSQL via asyncpg |
| **Streaming** | Server-Sent Events (SSE) — FastAPI → Next.js |
| **State** | Zustand (client) + TanStack Query v5 (server) |
| **Canvas** | React Flow — Schema Designer feature |
| **Security** | Fernet encryption for connection strings, read-only DB roles |
| **Deployment** | Vercel (frontend) + Railway (backend) |

---

## Project Structure

```
querymind/
├── backend/
│   ├── app/
│   │   ├── api/
│   │   │   ├── deps.py                  # Clerk JWT auth, DB session
│   │   │   └── routes/
│   │   │       ├── query.py             # POST /api/query (SSE stream)
│   │   │       ├── schema.py            # Schema indexing + retrieval
│   │   │       ├── connections.py       # CRUD for DB connections
│   │   │       ├── history.py           # Query history
│   │   │       └── users.py             # Clerk user sync
│   │   ├── core/
│   │   │   ├── config.py               # Pydantic settings
│   │   │   ├── security.py             # Fernet + JWT verification
│   │   │   └── logging.py
│   │   ├── services/
│   │   │   ├── schema_indexer.py       # Introspect DB → embed → Pinecone
│   │   │   ├── schema_retriever.py     # Vector similarity search
│   │   │   ├── sql_generator.py        # Gemini 2.5 Flash streaming
│   │   │   ├── sql_validator.py        # Safety + syntax validation
│   │   │   └── query_executor.py       # Read-only async execution
│   │   ├── models/                     # SQLAlchemy models
│   │   ├── schemas/                    # Pydantic request/response
│   │   └── main.py
│   └── tests/
│       ├── unit/
│       └── integration/
│
└── frontend/
    └── src/
        ├── app/
        │   ├── dashboard/              # Query, history, connections, schema
        │   └── (auth)/                 # Clerk sign-in / sign-up
        ├── components/
        │   ├── query/                  # NL input, SQL display, results table
        │   ├── schema/                 # Schema explorer + React Flow canvas
        │   └── connections/            # Connection management
        ├── hooks/                      # useQueryStream, useConnections, useHistory
        └── store/                      # Zustand stores
```

---

## Key Engineering Decisions

**Why SSE over WebSockets?**
SSE is unidirectional and HTTP-native — no handshake overhead, simpler to deploy behind a reverse proxy, and exactly right for the use case (server pushing token chunks to client). WebSockets would add complexity with no benefit here.

**Why Pinecone namespaces per connection?**
Each user's database connection gets its own Pinecone namespace (`conn_{id}`). This gives clean isolation — deleting a connection deletes its namespace. No cross-contamination between user schemas.

**Why `text-embedding-004` separately from `gemini-2.5-flash`?**
Two different jobs: `text-embedding-004` converts schema documents and user questions into dense vectors for similarity search. `gemini-2.5-flash` handles generation. Mixing them would mean paying generation-level costs for embedding calls.

**Why read-only at the connection level, not just the validator?**
Defense in depth. The validator catches keyword-based attacks. The read-only Postgres connection (via `SET default_transaction_read_only = on`) catches anything that slips through, including indirect write operations.

**Why Fernet for connection string encryption?**
Symmetric, authenticated, Python-native. The encrypted string is useless without the `FERNET_SECRET_KEY`. Even a full DB breach exposes nothing useful.

---

## Local Setup

### Prerequisites

- Python 3.11+
- Node.js 18+
- A [Neon](https://neon.tech) PostgreSQL database
- A [Pinecone](https://pinecone.io) account (serverless, free tier)
- A [Google AI Studio](https://aistudio.google.com) API key (Gemini)
- A [Clerk](https://clerk.com) account

---

### 1. Clone Backend:-

```bash
git clone https://github.com/agrim08/query-mind-be
cd querymind
```

### 2. Backend

```bash
python -m venv venv
source venv/bin/activate        # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

Copy and fill the env file:

```bash
cp .env.example .env
```

```env
# .env
DATABASE_URL=postgresql+asyncpg://user:pass@host/db
GEMINI_API_KEY=your_key_here
PINECONE_API_KEY=your_key_here
PINECONE_INDEX_NAME=querymind-schema
CLERK_SECRET_KEY=sk_live_...
FERNET_SECRET_KEY=          # generate: python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())"
ENVIRONMENT=development
MAX_QUERY_ROWS=500
QUERY_TIMEOUT_SECONDS=10
```

Run migrations and start:

```bash
alembic upgrade head
uvicorn app.main:app --reload --port 8000
```

### 3. Clone Frontend

```bash
git clone https://github.com/agrim08/query-mind-fe
cd querymind
```

```bash
npm install
cp .env.local.example .env.local
```

```env
# .env.local
NEXT_PUBLIC_CLERK_PUBLISHABLE_KEY=pk_live_...
CLERK_SECRET_KEY=sk_live_...
NEXT_PUBLIC_CLERK_SIGN_IN_URL=/sign-in
NEXT_PUBLIC_CLERK_SIGN_UP_URL=/sign-up
NEXT_PUBLIC_CLERK_AFTER_SIGN_IN_URL=/dashboard
NEXT_PUBLIC_CLERK_AFTER_SIGN_UP_URL=/dashboard
NEXT_PUBLIC_API_URL=http://localhost:8000
```

```bash
npm run dev
```

App is now running at `http://localhost:3000`.

---

### 4. Docker (optional)

```bash
docker-compose up --build
```

This spins up the FastAPI backend + a local Postgres instance for development. Pinecone and Gemini are external services — configure via `.env`.

---

## Environment Variables Reference

### Backend

| Variable | Required | Description |
|---|---|---|
| `DATABASE_URL` | ✓ | Neon asyncpg connection string |
| `GEMINI_API_KEY` | ✓ | Google AI Studio key |
| `PINECONE_API_KEY` | ✓ | Pinecone API key |
| `PINECONE_INDEX_NAME` | ✓ | Pinecone index (create: `querymind-schema`) |
| `CLERK_SECRET_KEY` | ✓ | Clerk backend secret |
| `FERNET_SECRET_KEY` | ✓ | Generate with `Fernet.generate_key()` |
| `MAX_QUERY_ROWS` | — | Default: `500` |
| `QUERY_TIMEOUT_SECONDS` | — | Default: `10` |
| `ENVIRONMENT` | — | `development` or `production` |

### Frontend

| Variable | Required | Description |
|---|---|---|
| `NEXT_PUBLIC_CLERK_PUBLISHABLE_KEY` | ✓ | Clerk publishable key |
| `CLERK_SECRET_KEY` | ✓ | Clerk secret (server-side) |
| `NEXT_PUBLIC_API_URL` | ✓ | Backend base URL |
| `NEXT_PUBLIC_CLERK_SIGN_IN_URL` | ✓ | `/sign-in` |
| `NEXT_PUBLIC_CLERK_SIGN_UP_URL` | ✓ | `/sign-up` |

---

## Security Model

- **Read-only connections** — every user query runs against a read-only Postgres role. Writes are impossible at the transport level, not just the application layer.
- **Keyword blocklist** — `DROP`, `DELETE`, `INSERT`, `UPDATE`, `TRUNCATE`, `ALTER`, `CREATE`, `GRANT`, `REVOKE`, `EXEC` are rejected before reaching the DB.
- **Fernet encryption** — connection strings are encrypted before storage. The key never touches the database.
- **JWT verification** — every protected route verifies the Clerk JWT via JWKS. No session cookies, no custom auth logic.
- **Row cap** — all queries return a maximum of 500 rows regardless of what the SQL requests.
- **Execution timeout** — `SET statement_timeout = '10s'` on every connection. Long-running queries are killed automatically.

---

## License

MIT — see [LICENSE](LICENSE)

---

<div align="center">

Built by [Agrim](https://agrimdev.vercel.app) · [Portfolio](https://agrimdev.vercel.app) · [LinkedIn](https://linkedin.com/in/agrim-gupta08)

<br />

*QueryMind — Your database, in plain English.*

</div>
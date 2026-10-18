# 🚀 Data Pipeline & Analytics Platform for Database Monitoring (Backend)

###### MustaInf

This repository contains the **backend API** of a **Data Pipeline and Analytics Platform** designed to **manage, monitor, query, and transfer data across databases** in a secure, scalable, and auditable way.

The backend is built with **FastAPI** and provides a powerful abstraction layer that allows users to interact with databases **without writing raw SQL**, while still supporting advanced operations such as **data transfer, analytics, reporting, and auditing**.

---

## 🎯 Project Purpose

This backend was designed to demonstrate:

- Solid **backend architecture**
- Secure **data access and manipulation**
- Real-world **data pipeline workflows**
- Database **monitoring and analytics**
- Clean separation of concerns (routes, services, models, schemas)

It is suitable for:

- Technical portfolios
- Internal data platforms
- Admin panels for database operations
- Learning advanced FastAPI + SQLAlchemy patterns

---

## 🧩 Core Concepts

### 🗄️ Database Management Engine

Responsible for:

- CRUD operations across multiple databases
- Secure execution of parameterized queries
- Connection management (PostgreSQL, MySQL, MongoDB, etc.)
- Schema inspection (tables, columns, indexes)
- Logs, auditing, and query history
- Transaction-safe data transfers between databases

### 📈 Analytics & Monitoring Layer

- Database statistics and performance metrics
- Deadlock monitoring
- Table size and row count tracking
- Query execution history
- Streaming large query results

### 🛡️ Dry-Run & Impact Predictor (Governance & Performance Safeguard)

- **Pre-execution Impact Estimation**: Evaluates query cost, estimated rows, and scan types (Index Scan vs. Full Table/Sequential/Collection Scan) before running queries or transferring data.
- **Engine-specific Explainers**: PostgreSQL (`EXPLAIN JSON`), MySQL (`EXPLAIN FORMAT=JSON`), MongoDB (`explain("queryPlanner")`), SQLite (`EXPLAIN QUERY PLAN`).
- **Impact Guard**: Automatic safety interception for high-risk queries (Sequential Scan on large tables, `DELETE`/`UPDATE` without `WHERE`, destructive DDL).
- **Index Recommendations**: Detects missing indexes on filtered columns and generates executable `CREATE INDEX` statements.
- Complete documentation: [docs/DRY_RUN_IMPACT_PREDICTOR.md](./docs/DRY_RUN_IMPACT_PREDICTOR.md).

### 🗺️ Data Lineage & Flow (Visual DAG & Governance)

- **Interactive End-to-End Lineage**: Directed Acyclic Graph (DAG) powered by NetworkX visualizing data flow across heterogeneous databases (PostgreSQL ➔ Filter ➔ MongoDB).
- **Automatic Event Interception**: Captures data transfers, volumes, queries, and column mappings transparently.
- **Impact Analysis (Upstream & Downstream)**: Evaluates the ripple effects of schema changes and table deletions with automated risk scoring (`LOW`, `MEDIUM`, `HIGH`, `CRITICAL`).
- Complete documentation: [docs/DATA_LINEAGE_AND_FLOW.md](./docs/DATA_LINEAGE_AND_FLOW.md).


### 🗂️ Project & Task Management (Support Layer)

- Projects, tasks, sprints, and user assignments
- Execution tracking and audit trails
- Report generation (PDF and Excel)

---

## 🛠️ Tech Stack

### Backend

- **Python 3.11+**
- **FastAPI**
- **SQLAlchemy**
- **Pydantic**
- **Uvicorn**
- **PostgreSQL**
- **Alembic** (migrations)
- **JWT authentication**
- **CORS middleware**
- **dotenv** for environment configuration

### Reporting & Data Export

- **WeasyPrint** (PDF generation)
- **Excel report generators**
- Structured JSON responses for frontend consumption

---

## 🏗️ Backend Architecture

```
app/
├── config/       # Startup logic, cache, environment, reset logic
├── cruds/        # Database access layer (CRUD operations)
├── middleware/   # Request context and global exception handlers
├── models/       # SQLAlchemy models
├── schemas/      # Pydantic schemas (DTOs)
├── services/     # Business logic and pipeline execution
├── routes/       # API endpoints
├── ultils/       # Shared helpers, query builders, logging
├── relatorio/    # PDF and Excel report generation
├── static/       # Locally served Swagger / ReDoc assets
├── auth.py       # Authentication logic
├── database.py   # Database connection and session management
└── main.py       # Application entry point
```

This structure enforces:

- **Clear separation of concerns**
- **Scalable codebase**
- **Easy maintenance and testing**

---

## ⚙️ Installation & Setup

### 1. Clone the repository

```bash
git clone https://github.com/your-username/gestor-bd.git
cd gestor-bd
```

### 2. Create and activate a virtual environment

```bash
python -m venv venv
venv\Scripts\activate      # Windows
source venv/bin/activate   # Linux / macOS
```

### 3. Install dependencies

```bash
pip install -r requirements.txt
```

### 4. Environment configuration

Copy `.env.example` to `.env` and adjust the values:

```env
ENV=dev
HOST=0.0.0.0
PORT=8000

DATABASE_URL=postgresql://user:password@localhost:5432/gestor_db

SECRET_KEY=<generate-a-unique-key-per-environment>
ENCRYPTION_KEY=<generate-a-unique-master-key-per-environment>

BACKEND_CORS_ORIGINS=http://localhost:3000,http://127.0.0.1:3000
```

`.env.example` lists every supported variable with its default.

---

## ▶️ Running the API

```bash
uvicorn app.main:app --reload
```

API available at:

```
http://localhost:8000
```

Interactive documentation:

```
http://localhost:8000/docs     # Swagger UI
http://localhost:8000/redoc    # ReDoc
```

Both pages are served from local assets (`app/static/swagger/`), so they work
without internet access. Docs are enabled outside production; set
`ENABLE_DOCS=true` to force them on, or `false` to force them off.

### Health checks

| Endpoint | Purpose |
| --- | --- |
| `GET /health/live` | Liveness probe — the process is up and answering. |
| `GET /health/ready` | Readiness probe — returns `503` when the database is unreachable. |
| `GET /health` | Shortcut, equivalent to `/health/ready`. |

They are deliberately separate: the orchestrator should pull the container out
of the load balancer when the database is down, not restart the process.

---

## 📋 Logs & Observability

Logs are written to **two places**, and each one answers a different question.

### Audit table (`logs`)

Application events recorded in the database, with level, source, and user.

| Endpoint | Description |
| --- | --- |
| `GET /logs` | List logs, filterable by `level`, with `limit` (max 500). |
| `GET /logs/stats` | Counts grouped by level. |
| `GET /logs/{log_id}` | A single log entry. |

### Log file (`database_connector.log`)

The file on disk. It holds everything the audit table cannot — in particular
failures that happen **before** the database connection is available.

| Endpoint | Description |
| --- | --- |
| `GET /logs/file` | Reads the file. Query params: `limit` (1–5000, default 200), `level` (`debug`, `info`, `success`, `warning`, `error`, `critical`) and `search` (substring, case-insensitive). |
| `GET /logs/file/download` | Downloads the whole file as `text/plain`. |

```bash
curl -b cookies.txt "http://localhost:8000/logs/file?limit=100&level=error"
```

Response payload:

```json
{
  "success": true,
  "data": {
    "caminho": "/app/database_connector.log",
    "modificado_em": "2026-08-07T14:25:41+00:00",
    "tamanho_bytes": 169060,
    "linhas": ["2026-08-07 14:25:41,374 - ERROR - falha ao ligar à base de dados"],
    "total_retornado": 1,
    "linhas_analisadas": 340,
    "inicio_do_ficheiro": false,
    "filtros": { "limit": 100, "level": "error", "search": null }
  }
}
```

`linhas` comes back in chronological order. `inicio_do_ficheiro: false` means
the reader stopped before reaching the top of the file — raise `limit` to see
further back. The file is read backwards in 64 KB blocks, so a 500 MB log
costs the same as a 5 KB one.

**Where the file lives.** By default it is created in the process working
directory, which changes depending on who starts the application (uvicorn from
the repository root, Docker from `/app`, the PyInstaller `.exe` from wherever
it was launched). The endpoints do not guess: they ask the active `logging`
handler where it is writing, and only then fall back to the known locations.

To pin the file to a fixed path, set:

```env
DATABASE_CONNECTOR_LOG_FILE=/app/logs/database_connector.log
```

It accepts either a file or a directory. Note this is **not** the same as
`LOG_FILE`, which belongs to a separate logging configuration block.

To follow the log inside the container:

```bash
docker exec -it minha-api tail -f /app/database_connector.log
```

---

## 🔐 Authentication

The API authenticates by **cookie**. `POST /auth/login` issues `access_token`
and `refresh_token`; because Swagger UI is served by the API itself, the cookie
travels automatically on subsequent requests — nothing to fill in under
**Authorize**.

Clients without a cookie jar (Postman, curl, tests) can send
`Authorization: Bearer <access_token>` instead. When both are present, the
cookie wins.

---

## 🌐 Frontend Integration

Allowed origins come from `BACKEND_CORS_ORIGINS`, comma-separated:

```env
BACKEND_CORS_ORIGINS=http://localhost:3000,http://192.168.54.68:3000
```

⚠️ `*` is rejected in production and disables credentials elsewhere: browsers
forbid `Access-Control-Allow-Origin: *` when cookies travel with the request,
so the wildcard would break cookie login rather than loosen it. List the real
origins.

Designed to integrate seamlessly with **React / Next.js** dashboards.

---

## ✨ Key Features

- JWT authentication and role-based access
- Secure query execution (no raw SQL required by user)
- **Dry-Run & Impact Predictor** (pre-execution query risk assessment, scan detection, and index recommendations)
- **Multi-Database Data Transfer** (transactional migrations between SQL and NoSQL with custom queries and live preview)
- Query streaming for large datasets
- Database schema inspection
- Connection monitoring and statistics
- Query history and auditing
- PDF and Excel report generation
- Task and project execution tracking

---

## 🔄 Data Pipeline Capabilities

- Extract data from source databases
- Transform results via service layer
- Load data into target databases
- Export results to spreadsheets
- Generate formal reports (PDF / Excel)
- Maintain full audit trail of operations

This qualifies the system as a **Data Pipeline & Analytics Platform**, not just a CRUD API.

---

## 👨‍💻 Author

**Francemy Eduardo Sebastião**

Full Stack Developer — Luanda, Angola 🇦🇴

---

## 📄 License

MIT License — free to use, modify, and distribute.

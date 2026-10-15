![runtime](assets/runtime.png)

# Durable Agent Runtime

Crash-safe, replayable multi-step agent orchestration runtime.

Runs are created through an HTTP API and executed by a worker from an
append-only MySQL event log. All workflow state is derived by replaying that
log, so any worker can resume a run after a crash without repeating committed
side effects or LLM calls.

## Highlights

- **Event-sourced state** — the event log is the single source of truth;
  `WorkflowState` is a pure reduction over events.
- **Crash-safe execution** — workers claim runs with row-level leases
  (`SELECT ... FOR UPDATE SKIP LOCKED`) and can resume mid-workflow.
- **Exactly-once side effects** — tools run under stable idempotency keys;
  `tool_operations` / `tool_effects` guarantee a logical effect happens once.
- **Recorded LLM calls** — model responses are persisted and reused, so replay
  never issues new model calls.
- **Deterministic replay** — the replay endpoint reproduces recorded decisions
  without executing tools or calling the LLM.
- **Snapshots** — periodic state snapshots bound reconstruction cost.
- **Fault injection** — a test-only controller can crash or pause the engine at
  defined points to prove recovery behavior.

## Requirements

- Python 3.11 – 3.13 (the `asyncmy` driver does not yet ship wheels for 3.14)
- MySQL 8.0, installed locally or otherwise reachable from the application

## Quick start

```bash
# 1. Install dependencies
python -m venv .venv
.venv\Scripts\activate            # Windows
# source .venv/bin/activate       # macOS / Linux
pip install -e ".[dev]"

# 2. Create your local configuration
copy .env.example .env            # Windows
# cp .env.example .env            # macOS / Linux

# 3. Edit .env and set DATABASE_URL and TEST_DATABASE_URL to your local
#    MySQL username, password, host, and database names (see below).
```

Create the application and test databases and a MySQL user with access to them.
Run this SQL while connected to your local MySQL server as an administrator;
replace the example password with your own:

```sql
CREATE DATABASE durable_agent CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE DATABASE durable_agent_test CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

CREATE USER 'durable'@'localhost' IDENTIFIED BY 'replace-with-a-strong-password';
GRANT ALL PRIVILEGES ON durable_agent.* TO 'durable'@'localhost';
GRANT ALL PRIVILEGES ON durable_agent_test.* TO 'durable'@'localhost';
```

Set these values in `.env`, using the password you chose above:

```dotenv
DATABASE_URL=mysql+asyncmy://durable:replace-with-a-strong-password@localhost:3306/durable_agent
TEST_DATABASE_URL=mysql+asyncmy://durable:replace-with-a-strong-password@localhost:3306/durable_agent_test
```

If your password contains URL-reserved characters (such as `@`, `:`, `/`, or
`#`), percent-encode those characters in the URL. `DATABASE_SYNC_URL` and
`TEST_DATABASE_SYNC_URL` can be omitted; Alembic derives the synchronous
PyMySQL URLs from the async URLs.

With your local MySQL service running and `.env` configured, run:

```bash
# 4. Create / update the schema
alembic upgrade head

# 5. Run the API (spawns an embedded worker by default)
durable-api
# or: uvicorn app.main:app --host 127.0.0.1 --port 8000
```

No Docker commands are needed for this setup. `compose.yaml` remains available
as an optional way to run a separate MySQL instance instead. Keep
`durable_agent_test` separate from any data you want to keep: the integration
test setup clears tables in its configured test database.

Create and inspect a run:

```bash
curl -X POST http://127.0.0.1:8000/runs \
  -H "Content-Type: application/json" \
  -d '{"task": "Write a weekly ops report", "notify_channel": "email"}'

curl http://127.0.0.1:8000/runs/<run_id>
curl http://127.0.0.1:8000/runs/<run_id>/events
```

Set `EMBEDDED_WORKER=false` and run `durable-worker` to scale execution
separately from the API.

## Streamlit explorer

Install the optional UI dependencies in the project environment and launch the
interactive explorer:

```bash
pip install -e ".[ui]"
streamlit run app/ui.py
```

The sidebar defaults to `http://127.0.0.1:8000`; change it if the API is hosted
elsewhere. The UI walks through the report workflow, creates runs, inspects
reduced state and ordered events, retries failed runs, and previews replay
decisions. Its SSE viewer can resume from a `Last-Event-ID`. The API Playground
loads `/openapi.json` from the selected server and lets you call its operations
with editable path parameters, query parameters, and JSON bodies. Swagger UI
and ReDoc links are also provided.

The built-in LLM provider is deterministic and local. Report saving and
notifications are mock effects persisted in MySQL; the demo does not write
files or send real notifications.

## HTTP API

| Method | Path                     | Description                                            |
| ------ | ------------------------ | ------------------------------------------------------ |
| GET    | `/health`                | Liveness plus database connectivity.                   |
| POST   | `/runs`                  | Create a run (`task`, `notify_channel`, `workflow_name`). |
| GET    | `/runs/{run_id}`         | Current reduced state of a run.                        |
| GET    | `/runs/{run_id}/events`  | Full ordered event log.                                |
| POST   | `/runs/{run_id}/retry`   | Re-queue a failed run.                                 |
| POST   | `/runs/{run_id}/replay`  | Reproduce decisions without executing side effects.    |
| GET    | `/runs/{run_id}/stream`  | Server-Sent Events stream (resumable via `Last-Event-ID`). |

## Project structure

```
app/
  main.py            FastAPI app factory and lifespan (starts embedded worker)
  config.py          Pydantic settings loaded from environment / .env
  container.py       Dependency container wiring the engine, tools, storage
  observability.py   JSON logging configuration and helpers
  api/               HTTP layer
    health.py          /health endpoint
    runs.py            run lifecycle, events, retry, replay, SSE stream
    deps.py            shared request dependencies
  models/            Persistence model definitions
    enums.py           RunStatus, OperationStatus, EventType
    tables.py          SQLAlchemy ORM tables (runs, events, tools, snapshots)
  schemas/           Pydantic contracts
    api.py             request / response models
    events.py          typed event payloads + validation registry
  runtime/           Core orchestration logic
    engine.py          step decision-making and event emission
    workflow.py        workflow definition, deterministic compute helpers
    state.py           pure event reducer -> WorkflowState
    replay.py          read-only reconstruction and replay report
    idempotency.py     stable key derivation for tools and LLM calls
    retry.py           retry policy and retryable / permanent errors
    llm.py             LLM provider protocol + deterministic fake provider
    faults.py          test-only crash / pause injection points
  storage/           Persistence layer
    database.py        async engine / session factories, URL helpers
    event_store.py     append-only event log with sequencing
    repositories.py    run, tool, and snapshot repositories
  tools/             Tool system
    base.py            Tool protocol, context, registry
    executor.py        transactional idempotent tool execution
    save_report.py     persists a report effect
    mock_notification.py  mock notification effect (+ failure injection)
  workers/           Run execution
    worker.py          lease claim loop and processing
    main.py            standalone worker entry point
    __main__.py        python -m app.workers

migrations/          Alembic environment and schema versions
  env.py               sync URL + metadata configuration
  versions/            migration scripts
tests/
  unit/                reducer, schema, idempotency, replay tests
  integration/         API, persistence, crash recovery, concurrency (MySQL)
  conftest.py          fixtures: DB, container, ASGI client, worker
  helpers.py           run-creation and lease helpers
benchmarks/
  baseline.py        in-memory reference workflow for overhead comparison
scripts/
  init_mysql.sql     creates databases and the durable user on first boot
compose.yaml         MySQL 8.0 service
alembic.ini          Alembic configuration
pyproject.toml       packaging, dependencies, ruff and pytest config
```

## How a run executes

1. `POST /runs` writes the run row and a `RunCreated` event.
2. A worker leases the run (`claim_next`) and loops over `Engine.process_run`.
3. `Engine._decide` picks the next action from the reduced state; each action
   appends event(s) inside a transaction that re-checks the lease.
4. LLM responses are stored as `LlmCallRecorded`; tool side effects use
   idempotency keys recorded in `tool_operations` / `tool_effects`.
5. `finalize` writes `RunCompleted` and the final result. If a worker crashes,
   the lease expires and another worker replays the log to resume.

### Event types

`RunCreated`, `RunStarted`, `StepStarted`, `StepCompleted`, `LlmCallRecorded`,
`ToolCallRequested`, `ToolCallSucceeded`, `ToolCallFailed`, `RunCompleted`,
`RunFailed`, `RunCancelled`, `RunRetryRequested`.

## Configuration

All settings come from environment variables (see `.env.example`):

| Variable                       | Default | Purpose                                  |
| ------------------------------ | ------- | ---------------------------------------- |
| `DATABASE_URL`                 | mysql+asyncmy://... | Async SQLAlchemy URL          |
| `DATABASE_SYNC_URL`            | derived | Sync URL used by Alembic                 |
| `TEST_DATABASE_URL`            | ..._test | Database for integration tests          |
| `LEASE_SECONDS`                | 30      | Run lease duration                       |
| `WORKER_POLL_INTERVAL_SECONDS` | 0.4     | Idle poll interval                       |
| `MAX_TOOL_ATTEMPTS`            | 3       | Retry attempts per tool call             |
| `SNAPSHOT_EVERY_EVENTS`        | 50      | Snapshot cadence (0 disables)            |
| `EMBEDDED_WORKER`              | true    | Run a worker inside the API process      |
| `LOG_LEVEL` / `HOST` / `PORT`  | INFO / 127.0.0.1 / 8000 | Logging and bind address |

## Testing

```bash
ruff check .
pytest                    # integration tests skip if MySQL is unreachable
pytest -m mysql           # run only the MySQL-backed tests
```

Integration tests expect a running MySQL instance matching `TEST_DATABASE_URL`
(the test database is created automatically when reachable).

## Benchmarks

`benchmarks/baseline.py` runs the same workflow shape entirely in memory as a
reference point for measuring the durable runtime's overhead.

## Design notes

- The workflow is a fixed, deterministic step list; the LLM only produces
  recorded analysis and is never trusted to drive control flow.
- Tool effects and their `ToolOperation` rows are committed in one transaction,
  so a succeeded effect always has a matching operation record.
- Replay reconstructs decisions from the immutable log and makes no external
  calls; it deliberately does not promise determinism for arbitrary external
  systems beyond the recorded effects.

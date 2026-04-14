# LocalGCP — Claude Code Guide

## Project overview

LocalGCP is a local emulator for GCP services (Cloud Storage, Pub/Sub, Firestore, Secret Manager, Cloud Tasks). It is written in Python using FastAPI and runs every service as a separate uvicorn server, all started concurrently from a single entry point.

**Stack:** Python 3.12+, FastAPI, uvicorn, Pydantic v2, grpcio, uv (package manager)

## Running locally

```bash
uv sync           # install all deps (including dev)
uv run localgcp   # start all services
```

Or via Docker:

```bash
docker compose up
```

## Running tests

```bash
uv run pytest tests/
```

All 39 tests should pass. Tests use `pytest-asyncio` with `asyncio_mode = "auto"` (set in `pyproject.toml`). No external services required — each test file creates its own in-process test client.

## Project layout

```
localgcp/
  main.py                   Entry point — spawns all service servers concurrently
  config.py                 Settings dataclass, reads env vars
  core/
    store.py                NamespacedStore — shared in-memory (or file-backed) state
    auth.py                 Auth middleware (no-op for local dev)
    errors.py               Common HTTP error helpers
    middleware.py           Request middleware
  services/
    gcs/                    Cloud Storage (port 4443)
    pubsub/                 Pub/Sub gRPC (8085) + REST (8086)
    firestore/              Firestore (port 8080)
    secretmanager/          Secret Manager (port 8090)
    tasks/                  Cloud Tasks (port 8123)
  admin/                    Admin UI (port 8888)

tests/                      One file per service (test_gcs.py, test_pubsub.py, …)
sdk_compat/
  clients.py                Pre-configured GCP SDK client factories
  test_with_sdk.py          Live smoke tests (requires a running instance)
bin/
  gcloudlocal.py            gcloud-compatible CLI for the emulator
  *.sh                      Shell helper scripts
```

## Service pattern

Every service follows the same three-file layout:

```
localgcp/services/<name>/
    app.py      FastAPI application with all routes
    models.py   Pydantic v2 request/response models
    store.py    Thin wrapper around NamespacedStore
```

When adding a new service:
1. Create the directory and three files above.
2. Register the service in `localgcp/main.py` by adding it to `_SERVICES` and `apps` in `_build_configs()`.
3. Add a port setting to `localgcp/config.py` and `docker-compose.yml`.
4. Add a test file under `tests/`.

## Key environment variables

| Variable | Default | Purpose |
|---|---|---|
| `LOCALGCP_PROJECT` | `local-project` | Default project ID |
| `LOCALGCP_LOCATION` | `us-central1` | Default region |
| `LOCALGCP_DATA_DIR` | *(unset)* | Enables file-backed persistence |
| `LOCALGCP_LOG_LEVEL` | `info` | Log verbosity |
| Port variables (`LOCALGCP_*_PORT`) | see config.py | Per-service port overrides |

## Pub/Sub transport notes

Pub/Sub is the only service with two endpoints:
- **Port 8085** — gRPC server, compatible with `PUBSUB_EMULATOR_HOST=localhost:8085`
- **Port 8086** — HTTP/1.1 REST server, for `transport="rest"` SDK clients

Use `sdk_compat/clients.py` helpers to get correctly-configured SDK clients without manual setup.

## Persistence

By default all state is in-memory and lost on restart. Set `LOCALGCP_DATA_DIR` to a directory path to enable JSON file persistence. The `NamespacedStore` in `localgcp/core/store.py` handles both modes transparently.

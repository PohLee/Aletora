# Development

## Requirements

- Python 3.12, 3.13, or 3.14 (the supported verification versions)
- [uv](https://docs.astral.sh/uv/)
- An isolated PostgreSQL instance. CI uses `postgres:16`.
- A disposable Qdrant server for full integration coverage. CI uses `qdrant/qdrant:v1.13.4`.

The core package has no third-party runtime dependencies. Optional adapters are installed with
the `sqlalchemy`, `tortoise`, and `qdrant` extras. Development and CI install all extras so the
adapter tests are included. Local Qdrant tests use the client's `:memory:` mode; server-mode
payload-index tests run when `ALETORA_TEST_QDRANT_URL` is configured.

Package metadata currently declares `aletora==0.1.0` and Python >=3.12. The supported host
API and optional store and resolver protocols are described in [the V1 specification](V1-SPEC.md).

## Setup

From the repository root (use an explicit interpreter path if uv discovery is restricted):

```sh
uv sync --frozen --group dev --all-extras
```

For a specific verification version, add `--python 3.12`, `--python 3.13`, or `--python 3.14`
(an absolute interpreter path is also accepted). This uses the committed `uv.lock` and creates
a project `.venv`. To install only development tools and the core package, omit `--all-extras`.

## Checks

On Windows, `scripts/verify.ps1` defaults to Python 3.12, 3.13, and 3.14. For each interpreter
it runs the checks, builds the wheel and source distribution, and installs the wheel in a
fresh core-only environment. `-PostgresDsn` is required, and `-QdrantUrl` enables server-mode
tests. Both default to their corresponding `ALETORA_TEST_*` environment variables.

```powershell
./scripts/verify.ps1 -PostgresDsn "postgresql://user@127.0.0.1:55433/aletora_test" `
    -QdrantUrl "http://127.0.0.1:6333" -Cleanup
```

Use `-Python` to select versions or pass absolute interpreter paths. `-EnvironmentRoot`
defaults to `aletora-verify` under the system temporary directory and contains unique
per-run build outputs and temporary environments. `-Cleanup` removes the environments
and build artifacts created by the run. Start the test services
before invoking the script.

Enable the repository Git hooks once per clone:

```sh
git config core.hooksPath .githooks
```

- `pre-commit` runs `ruff format` on staged Python files and restages the result. It formats
  the staged content itself, so unstaged edits in a partially staged file stay unstaged.
- `pre-push` runs `ruff format --check .` on the working tree and blocks the push on failure.

Both hooks use `uv run --frozen ruff` (or `ruff` on `PATH` when `uv` is absent). Markdown files
are excluded from formatting in `pyproject.toml`. CI runs the same `ruff format --check .`.

Run the same gates as CI:

```sh
uv run --frozen ruff check .
uv run --frozen ruff format --check .
uv run --frozen mypy
uv run --frozen pytest -ra
uv run --frozen python -m build --no-isolation
```

Shared host-transaction scenarios use the `postgres_host` fixture from `tests/conftest.py`.
It runs each scenario against SQLAlchemy and Tortoise on real PostgreSQL. The test-host module
in `tests/postgres_host.py` owns isolated schema setup, host transactions, durable outbox/formal
writes, failure injection and independent committed-state observation. Governance and atomicity
assertions belong in the scenarios; adapter-specific transaction-mode and RLS checks keep their
own direct setup. Add a common guarantee once, then verify it against both adapters.

The pytest `postgres` and `qdrant_server` markers denote cases that require external isolated
test services. Without `ALETORA_TEST_POSTGRES_DSN` or `ALETORA_TEST_QDRANT_URL`, those cases are
skipped while the rest of the suite remains runnable. PostgreSQL adapter tests use a restricted
role provisioned by the test setup and must not use a production database. Set the environment
variables to dedicated disposable test services, for example:

```powershell
# PowerShell example; supply credentials appropriate for your local PostgreSQL instance.
$env:ALETORA_TEST_POSTGRES_DSN = "postgresql://aletora_test_admin:password@localhost:5432/aletora_test"
$env:ALETORA_TEST_QDRANT_URL = "http://localhost:6333"
uv run --frozen pytest -ra
```

The configured connection is for test provisioning only; tests create/use restricted roles for
the behavior under test. Never point it at a production database.

## Package smoke test

`python -m build --no-isolation` produces `aletora-0.1.0-py3-none-any.whl` and
`aletora-0.1.0.tar.gz`. Pass `--outdir` with a fresh directory to keep earlier builds.
Install the wheel with `python -m pip install <wheel-path>` in a fresh virtual environment
without requesting extras. Confirm `from aletora import Memory, Scope` works and check that
`importlib.util.find_spec` returns `None` for `sqlalchemy`, `tortoise`, and `qdrant_client`.

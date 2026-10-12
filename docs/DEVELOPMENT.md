# Development

## Requirements

- Python 3.12 or 3.14 (the supported verification versions)
- [uv](https://docs.astral.sh/uv/)
- An isolated PostgreSQL instance (for example a disposable Docker container or temporary cluster)

The core package has no third-party runtime dependencies. Optional adapters are installed with
the `sqlalchemy`, `tortoise`, and `qdrant` extras. Development and CI install all extras so the
adapter tests are included. Qdrant tests use the client's local mode; no Qdrant server is needed.

## Setup

From the repository root (use an explicit interpreter path if uv discovery is restricted):

```sh
uv sync --frozen --group dev --all-extras
```

For a specific verification version, add `--python 3.12` or `--python 3.14` (an absolute interpreter
path is also accepted). This uses the committed `uv.lock` and creates a project `.venv`. To install only development
tools and the core package, omit `--all-extras`.

## Checks

On Windows, `scripts/verify.ps1` runs all gates for both versions in separate temporary
environments, builds the distributions and checks a clean core-only install. It requires
the isolated PostgreSQL DSN, so a successful full verification cannot omit database tests:

```powershell
./scripts/verify.ps1 -PostgresDsn "postgresql://user@127.0.0.1:55433/aletora_test"
# Pass absolute interpreter paths via -Python @(path312, path314) when discovery is restricted.
```

The script leaves its uniquely named temporary environments for inspection; it does not
start or stop databases and never touches existing project virtual environments.

Run the same gates as CI:

```sh
uv run --frozen ruff check .
uv run --frozen ruff format --check src tests
uv run --frozen mypy
uv run --frozen pytest
uv run --frozen python -m build --no-isolation
```

The pytest `postgres` marker denotes cases that need the isolated test database. Without
`ALETORA_TEST_POSTGRES_DSN`, those cases are skipped; the rest of the suite remains runnable.
PostgreSQL adapter tests use a restricted role provisioned by the test setup and must not use a
production database. Set the environment variable to the dedicated disposable test database,
for example:

```sh
# PowerShell example; supply credentials appropriate for your local PostgreSQL instance.
$env:ALETORA_TEST_POSTGRES_DSN = "postgresql://aletora_test_admin:password@localhost:5432/aletora_test"
uv run --frozen pytest
```

The configured connection is for test provisioning only; tests should create/use restricted
roles for the behavior under test. Never point it at a production database.

## Package smoke test

Build artifacts with `uv run --frozen python -m build --no-isolation`, then install the wheel
without requesting optional extras into a clean environment and verify the public top-level import:

```sh
python -m venv .venv-core-smoke
```

Activate that environment, then:

```sh
python -m pip install dist/aletora-0.1.0-py3-none-any.whl
python -c "import aletora"
```

This verifies the core-only install path, not integration with any host application.

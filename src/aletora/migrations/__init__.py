"""Render packaged PostgreSQL migrations for host-managed application."""

from __future__ import annotations

from importlib.resources import files

from .._postgres import qualified_table

_MIGRATIONS = {"dedicated.sql", "shared.sql"}


def read_migration(name: str) -> str:
    """Read a packaged PostgreSQL migration without applying it."""
    if name not in _MIGRATIONS:
        raise ValueError(f"unknown Aletora migration {name!r}")
    return files(__package__).joinpath(name).read_text(encoding="utf-8")


def render_migration(name: str, *, schema: str | None = None) -> str:
    """Render a migration, safely quoting a validated optional schema identifier.

    ``schema=None`` leaves the table unqualified so the host's current
    ``search_path`` controls dedicated-schema placement.
    """
    template = read_migration(name)
    return template.replace("{{table}}", qualified_table(schema))


__all__ = ["read_migration", "render_migration"]

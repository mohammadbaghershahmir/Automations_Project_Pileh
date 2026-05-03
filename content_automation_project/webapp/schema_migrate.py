"""Lightweight SQLite schema additions for existing deployments."""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.engine import Engine


def apply_schema_migrations(engine: Engine) -> None:
    if engine.dialect.name != "sqlite":
        return
    with engine.connect() as conn:
        rows = conn.execute(text("PRAGMA table_info(jobs)")).fetchall()
        colnames = {r[1] for r in rows}
        if "cancel_requested" not in colnames:
            conn.execute(
                text(
                    "ALTER TABLE jobs ADD COLUMN cancel_requested INTEGER NOT NULL DEFAULT 0"
                )
            )
            conn.commit()

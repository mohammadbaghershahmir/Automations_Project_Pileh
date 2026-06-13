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

    _ensure_gemini_tts_rate_columns(engine)

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS system_prompt_defaults (
                    job_type VARCHAR(32) NOT NULL,
                    config_key VARCHAR(32) NOT NULL,
                    prompt_text TEXT NOT NULL,
                    updated_at DATETIME,
                    updated_by_id INTEGER,
                    PRIMARY KEY (job_type, config_key),
                    FOREIGN KEY(updated_by_id) REFERENCES users (id)
                )
                """
            )
        )


def _ensure_gemini_tts_rate_columns(engine: Engine) -> None:
    if engine.dialect.name != "sqlite":
        return
    additions = [
        ("rpm_limit", "INTEGER NOT NULL DEFAULT 3"),
        ("rpd_limit", "INTEGER NOT NULL DEFAULT 10"),
        ("requests_today", "INTEGER NOT NULL DEFAULT 0"),
        ("requests_in_minute", "INTEGER NOT NULL DEFAULT 0"),
        ("rpm_window_start", "DATETIME"),
        ("daily_quota_date", "VARCHAR(10)"),
    ]
    with engine.begin() as conn:
        rows = conn.execute(text("PRAGMA table_info(gemini_tts_api_keys)")).fetchall()
        if not rows:
            return
        colnames = {r[1] for r in rows}
        for col, typedef in additions:
            if col not in colnames:
                conn.execute(
                    text(f"ALTER TABLE gemini_tts_api_keys ADD COLUMN {col} {typedef}")
                )

"""Celery application: Redis broker, no result backend (job state lives in SQLite)."""

from __future__ import annotations

from celery import Celery

from webapp.config import REDIS_URL

# Fire-and-forget long jobs; status is in the DB (Job / JobPair), not Celery results.
celery_app = Celery(
    "content_automation",
    broker=REDIS_URL,
)

celery_app.conf.update(
    task_serializer="json",
    accept_content=["json"],
    result_serializer="json",
    timezone="UTC",
    enable_utc=True,
    task_track_started=True,
    task_ignore_result=True,
    result_backend=None,
    task_time_limit=72 * 3600,
    task_soft_time_limit=72 * 3600 - 120,
    broker_connection_retry_on_startup=True,
)

# Worker process does not run FastAPI startup: ensure tables + migrations exist.
import webapp.models  # noqa: F401, E402 — register models
from webapp.database import Base, engine  # noqa: E402
from webapp.schema_migrate import apply_schema_migrations  # noqa: E402

from webapp.database import SessionLocal  # noqa: E402
from webapp.bootstrap import bootstrap_admins, ensure_missing_env_admins  # noqa: E402
from webapp.system_prompt_defaults import seed_system_prompt_defaults  # noqa: E402

Base.metadata.create_all(bind=engine)
apply_schema_migrations(engine)
_worker_db = SessionLocal()
try:
    bootstrap_admins(_worker_db)
    ensure_missing_env_admins(_worker_db)
    seed_system_prompt_defaults(_worker_db)
finally:
    _worker_db.close()

celery_app.autodiscover_tasks(packages=["webapp"], related_name="celery_tasks", force=True)

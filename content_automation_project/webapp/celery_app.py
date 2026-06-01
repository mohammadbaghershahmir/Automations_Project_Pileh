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
    # Recycle worker children after each task to release LLM response memory (SIGKILL/OOM on regen).
    worker_max_tasks_per_child=1,
    worker_prefetch_multiplier=1,
)

# Worker process does not run FastAPI startup: ensure tables + migrations exist.
import webapp.models  # noqa: F401, E402 — register models
from webapp.database import Base, engine  # noqa: E402
from webapp.schema_migrate import apply_schema_migrations  # noqa: E402

from webapp.database import SessionLocal  # noqa: E402
from webapp.bootstrap import bootstrap_admins, ensure_missing_env_admins  # noqa: E402
from webapp.job_files import append_log  # noqa: E402
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


@celery_app.on_after_configure.connect
def _connect_task_failure_logger(**kwargs: object) -> None:
    from celery.signals import task_failure

    @task_failure.connect
    def _log_regenerate_task_failure(
        sender=None,
        task_id=None,
        exception=None,
        args=None,
        **kw: object,
    ) -> None:
        name = getattr(sender, "name", "") or ""
        if name != "webapp.run_regenerate_unit" or not args or len(args) < 3:
            return
        job_id, pair_index, unit_index = args[0], args[1], args[2]
        exc_s = str(exception or "")
        if "Worker exited prematurely" in exc_s or "SIGKILL" in exc_s:
            msg = (
                f"Regenerate unit {unit_index} was killed (signal 9 — usually a RAM spike, not a bad unit). "
                "Ensure only one LLM job runs at a time (worker --pool=solo), then retry."
            )
        else:
            msg = f"Regenerate unit {unit_index} failed: {exc_s}"
        try:
            from webapp.unit_repair.lock import _clear_lock, _lock_path

            _clear_lock(_lock_path(job_id, pair_index))
        except Exception:
            pass
        db = SessionLocal()
        try:
            append_log(db, job_id, msg, pair_index)
        except Exception:
            pass
        finally:
            db.close()

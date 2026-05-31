"""FastAPI admin app: Test Bank jobs, polling, auth."""

from __future__ import annotations

import inspect
import json
import os
from datetime import datetime
import shutil
import tempfile
import uuid
from pathlib import Path
from typing import Any, List, Optional

from dotenv import load_dotenv
from fastapi import Depends, FastAPI, File, Form, HTTPException, Query, Request, UploadFile
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, PlainTextResponse, RedirectResponse
from pydantic import BaseModel
from fastapi.templating import Jinja2Templates
try:
    from webapp.celery_tasks import run_full_pipeline_task, run_step1_task, run_step2_task

    HAS_CELERY = True
except ImportError:
    HAS_CELERY = False
from sqlalchemy import func
from sqlalchemy.orm import Session, joinedload
from webapp.auth_utils import COOKIE_NAME, create_access_token, verify_password
from webapp.bootstrap import (
    bootstrap_admins,
    ensure_missing_env_admins,
    sync_admin_password_from_env,
)
from webapp.config import (
    DEFAULT_TEST_BANK_MODEL,
    DEFAULT_TEST_BANK_PROVIDER,
    DEFAULT_VOICE_CLASS_CHARS_PER_SECOND,
    DEFAULT_VOICE_CLASS_MAX_SEGMENT_SECONDS,
    DEFAULT_VOICE_CLASS_TTS_MODEL,
    DEFAULT_VOICE_CLASS_TTS_VOICE,
    GEMINI_TTS_KEYS_SEED_DIR,
    JOBS_ROOT,
    PROJECT_ROOT,
    RUN_TASKS_INLINE,
    SONGS_DIR,
    TEST_BANK_OPENROUTER_MODEL_CHOICES,
    normalize_test_bank_model,
    normalize_test_bank_provider,
)
from webapp.database import Base, SessionLocal, engine, get_db
from webapp.schema_migrate import apply_schema_migrations
import webapp.models  # noqa: F401 — register models with metadata
from webapp.deps import CurrentUser, is_admin_user, require_admin
from webapp.datetime_jalali import format_tehran_shamsi
from webapp.job_files import (
    append_log,
    delete_job_completely,
    ensure_dirs,
    find_word_file_abs_for_basename,
    job_root,
    list_word_basenames_for_job,
    pair_inputs,
    register_input_artifact,
)
from webapp.job_runner_common import SINGLE_STAGE_JOB_TYPES
from webapp.job_prompts import (
    apply_submitted_prompts_to_cfg,
    build_prompt_editor_rows,
    job_type_has_editable_prompts,
)
from webapp.system_prompt_defaults import (
    NEW_JOB_PAGE_BY_TYPE,
    PromptNotConfiguredError,
    apply_submitted_system_prompts,
    get_system_prompt_default,
    resolve_prompt_for_job,
    seed_system_prompt_defaults,
)
from webapp.models import Artifact, GeminiTtsApiKey, InboxNotification, Job, JobLogLine, JobPair, User
from webapp.tasks_stage_v import run_full_pipeline_job, run_step1_job, run_step2_job
from stage_v_pairing import (
    attach_step1_combined_uploads_to_pairs,
    auto_pair_chapter_summary_files,
    auto_pair_flashcard_files,
    auto_pair_stage_v_files,
    auto_pair_voice_class_files,
)

try:
    import multipart  # noqa: F401 — python-multipart (required for large file uploads)

    HAS_MULTIPART = True
except ImportError:
    HAS_MULTIPART = False

load_dotenv(PROJECT_ROOT / ".env")


def _is_nonempty_file_upload(u: Any) -> bool:
    """Starlette/FastAPI may use a class that is not `isinstance(..., UploadFile)` in all envs; treat as upload if it looks like a file field."""
    if u is None or isinstance(u, (str, bytes)):
        return False
    fn = getattr(u, "filename", None)
    if not fn or not str(fn).strip():
        return False
    read_fn = getattr(u, "read", None)
    return callable(read_fn)


async def _read_upload_bytes(u: Any) -> bytes:
    read_fn = getattr(u, "read", None)
    if not callable(read_fn):
        return b""
    r = read_fn()
    if inspect.isawaitable(r):
        r = await r
    if isinstance(r, memoryview):
        return r.tobytes()
    if isinstance(r, bytes):
        return r
    if r is None:
        return b""
    return bytes(r)


def user_can_edit_job(job: Job, user: CurrentUser) -> bool:
    return job.created_by_id == user.id or is_admin_user(user)


def require_job_owner(job: Job, user: CurrentUser) -> None:
    if not user_can_edit_job(job, user):
        raise HTTPException(
            status_code=403,
            detail="Only the job creator or an admin can change pairing, run steps, regenerate units, or cancel.",
        )


templates = Jinja2Templates(directory=str(Path(__file__).parent / "templates"))
templates.env.filters["shamsi"] = format_tehran_shamsi


def _job_cfg_filter(job: Job) -> dict:
    return json.loads(job.config_json or "{}")


templates.env.filters["job_cfg"] = _job_cfg_filter


def tasks_use_celery_queue() -> bool:
    """True when tasks are sent to Celery (requires a running worker process)."""
    return bool(HAS_CELERY and not RUN_TASKS_INLINE)


def enqueue_task(name: str, job_id: str, pair_indices: Optional[List[int]]) -> None:
    if tasks_use_celery_queue():
        if name == "step1":
            run_step1_task.delay(job_id, pair_indices)
        elif name == "step2":
            run_step2_task.delay(job_id, pair_indices)
        elif name == "full":
            run_full_pipeline_task.delay(job_id, pair_indices)
        else:
            raise ValueError(name)
        return

    import threading

    def _run() -> None:
        if name == "step1":
            run_step1_job(job_id, pair_indices)
        elif name == "step2":
            run_step2_job(job_id, pair_indices)
        elif name == "full":
            run_full_pipeline_job(job_id, pair_indices)
        else:
            raise ValueError(name)

    threading.Thread(target=_run, daemon=True).start()


def enqueue_unit_repair(name: str, job_id: str, pair_index: int, unit_index: Optional[int] = None) -> None:
    try:
        from webapp.celery_tasks import regenerate_unit_task, renumber_pair_task
    except ImportError:
        regenerate_unit_task = None
        renumber_pair_task = None

    if tasks_use_celery_queue() and regenerate_unit_task and renumber_pair_task:
        if name == "regenerate" and unit_index is not None:
            regenerate_unit_task.delay(job_id, pair_index, unit_index)
        elif name == "renumber":
            renumber_pair_task.delay(job_id, pair_index)
        else:
            raise ValueError(name)
        return

    import threading
    from webapp.unit_repair.tasks import run_regenerate_unit_task, run_renumber_pair_task

    def _run() -> None:
        if name == "regenerate" and unit_index is not None:
            run_regenerate_unit_task(job_id, pair_index, unit_index)
        elif name == "renumber":
            run_renumber_pair_task(job_id, pair_index)
        else:
            raise ValueError(name)

    threading.Thread(target=_run, daemon=True).start()


def queued_task_log_suffix() -> str:
    if tasks_use_celery_queue():
        return (
            " Jobs run in Celery workers; without a worker Step 1 never runs: "
            "`celery -A webapp.celery_app worker --loglevel=info` (same env as the API), "
            "or Docker Compose `worker`, or `python -m webapp.run_worker`, "
            "or set WEBAPP_RUN_TASKS_INLINE=1."
        )
    if HAS_CELERY and RUN_TASKS_INLINE:
        return " Running in-process (WEBAPP_RUN_TASKS_INLINE=1)."
    return " Running in-process background thread (Celery unavailable or not installed)."


def parse_pair_indices(raw: Optional[str]) -> Optional[List[int]]:
    if not raw or not str(raw).strip():
        return None
    return [int(x.strip()) for x in str(raw).split(",") if x.strip()]


def _sorted_nonempty_paths(paths: List[str]) -> List[str]:
    return sorted([p for p in paths if p], key=lambda p: os.path.basename(p).lower())


# Labels align with main_gui.py tab titles (Content Automation - Pileh).
# Keys include likely Job.type values plus stage_* aliases for APIs / imports.
JOB_STAGE_LABELS = {
    # Document Processing (Stages 1–4 tab)
    "document_processing": "Document Processing",
    "stages_1_4": "Document Processing",
    # Image Notes Generation — Stage E
    "image_notes": "Image Notes Generation",
    "stage_e": "Image Notes Generation",
    # Table Notes Generation — Stage TA
    "table_notes": "Table Notes Generation",
    "stage_ta": "Table Notes Generation",
    # Image File Catalog — Stage F
    "image_file_catalog": "Image File Catalog",
    "stage_f": "Image File Catalog",
    # Importance & Type Tagging — Stage J
    "importance_type_tagging": "Importance & Type Tagging",
    "importance_type": "Importance & Type Tagging",
    "stage_j": "Importance & Type Tagging",
    # Flashcard Generation — Stage H
    "flashcard": "Flashcard Generation",
    "flashcard_generation": "Flashcard Generation",
    "stage_h": "Flashcard Generation",
    # Test Bank Generation — Stage V (web jobs default)
    "test_bank": "Test Bank Generation",
    "test_bank_1": "Test Bank 1",
    "test_bank_2": "Test Bank 2",
    "stage_v": "Test Bank Generation",
    # Topic List Extraction — Stage M
    "topic_list": "Topic List Extraction",
    "topic_list_extraction": "Topic List Extraction",
    "stage_m": "Topic List Extraction",
    # Chapter Summary — Stage L
    "chapter_summary": "Chapter Summary",
    "chapter_summary_json_to_csv": "Chapter Summary → CSV",
    "image_catalog_json_to_csv": "Image Catalog → CSV",
    "test_bank_2_json_to_csv": "Test Bank 2 → CSV",
    "flashcard_json_to_csv": "Flashcard → CSV",
    "table_notes_json_to_word": "Table Notes → Word",
    "document_processing_json_to_word": "Table Notes → Word",
    "stage_l": "Chapter Summary",
    "voice_class": "Voice / Speech Class",
    # Book Changes Detection — Stage X
    "book_changes": "Book Changes Detection",
    "book_changes_detection": "Book Changes Detection",
    "stage_x": "Book Changes Detection",
    # Deletion Detection — Stage Y
    "deletion_detection": "Deletion Detection",
    "stage_y": "Deletion Detection",
    # RichText Generation — Stage Z
    "rich_text": "RichText Generation",
    "richtext": "RichText Generation",
    "stage_z": "RichText Generation",
    # Web single-stage jobs (additional labels)
    "pre_ocr_topic": "Pre-OCR Topic Extraction",
    "ocr_extraction": "OCR Extraction",
}


def job_stage_label(job: Job) -> str:
    t = (job.type or "").strip() or "test_bank"
    return JOB_STAGE_LABELS.get(t, t.replace("_", " ").title())


def job_active_for_delete(job: Job, pairs: List[JobPair]) -> bool:
    """True while a worker may still be using this job (stop before delete)."""
    if (job.status or "") in ("queued", "running"):
        return True
    return effective_job_list_status(job, pairs) in ("running", "queued")


def effective_job_list_status(job: Job, pairs: List[JobPair]) -> str:
    """Jobs list row status: prefer pair outcomes so stale job.status does not show succeeded when pairs failed."""
    st = job.status or ""
    if st in ("running", "queued"):
        return st
    if st == "cancelled":
        return "cancelled"
    if st == "draft":
        return "draft"
    if not pairs:
        return st
    jt = (job.type or "").strip()
    if jt in SINGLE_STAGE_JOB_TYPES:
        if any(p.step1_status == "failed" for p in pairs):
            return "failed"
        if any(p.step1_status == "running" for p in pairs):
            return "running"
        if all(p.step1_status == "succeeded" for p in pairs):
            return "succeeded"
        return "pending"
    if any(p.step1_status == "failed" or p.step2_status == "failed" for p in pairs):
        return "failed"
    if any(p.step1_status == "running" or p.step2_status == "running" for p in pairs):
        return "running"
    if all(p.step1_status == "succeeded" and p.step2_status == "succeeded" for p in pairs):
        return "succeeded"
    return "pending"


STEP1_ARTIFACT_ROLES = frozenset({"step1_combined", "txt_dump", "llm_prompt_step1", "voice_script_json"})
STEP2_ARTIFACT_ROLES = frozenset(
    {
        "step2_topic",
        "final_b_json",
        "step2_failed_topics",
        "step2_prompt_input",
        "output",
        "llm_prompt_step2",
        "tts_segment",
        "final_mp3",
    }
)


def split_artifacts_for_steps(
    artifacts: List[Artifact],
    job_type: Optional[str] = None,
) -> tuple[List[Artifact], List[Artifact], List[Artifact]]:
    jt = (job_type or "").strip()
    s1: List[Artifact] = []
    s2: List[Artifact] = []
    other: List[Artifact] = []
    for a in artifacts:
        if a.role in STEP1_ARTIFACT_ROLES or (jt in SINGLE_STAGE_JOB_TYPES and a.role == "output"):
            s1.append(a)
        elif a.role in STEP2_ARTIFACT_ROLES and not (jt in SINGLE_STAGE_JOB_TYPES and a.role == "output"):
            s2.append(a)
        else:
            other.append(a)
    if jt == "test_bank_2":
        s1.extend(s2)
        s2.clear()
    return s1, s2, other


def all_pairs_step1_succeeded(pairs: List[JobPair]) -> bool:
    if not pairs:
        return False
    return all(p.step1_status == "succeeded" for p in pairs)


class LoginBody(BaseModel):
    email: str
    password: str


class InboxMarkReadBody(BaseModel):
    ids: Optional[List[int]] = None
    mark_all: bool = False


class RenumberBody(BaseModel):
    confirm: bool = False


def _iso_utc(dt: Optional[datetime]) -> Optional[str]:
    if dt is None:
        return None
    s = dt.isoformat()
    return s + "Z" if getattr(dt, "tzinfo", None) is None else s


def _system_prompt_for_ui(db: Session, job_type: str, config_key: str) -> str:
    try:
        return get_system_prompt_default(db, job_type, config_key)
    except PromptNotConfiguredError as exc:
        raise HTTPException(500, str(exc)) from exc


def _prompt_for_new_job(
    db: Session,
    job_type: str,
    form_value: str,
    config_key: str,
) -> str:
    try:
        return resolve_prompt_for_job(db, job_type, {config_key: form_value}, config_key)
    except PromptNotConfiguredError as exc:
        raise HTTPException(500, str(exc)) from exc


def create_app() -> FastAPI:
    app = FastAPI(title="Content Automation Admin", version="1.0")

    @app.on_event("startup")
    def _startup() -> None:
        os.makedirs(JOBS_ROOT, exist_ok=True)
        os.makedirs(PROJECT_ROOT / "data", exist_ok=True)
        Base.metadata.create_all(bind=engine)
        apply_schema_migrations(engine)
        db = SessionLocal()
        try:
            bootstrap_admins(db)
            ensure_missing_env_admins(db)
            sync_admin_password_from_env(db)
            seed_system_prompt_defaults(db)
            from webapp.gemini_tts_keys_service import seed_gemini_tts_keys_from_dir

            seed_gemini_tts_keys_from_dir(db, GEMINI_TTS_KEYS_SEED_DIR)
        finally:
            db.close()

    @app.get("/", response_class=HTMLResponse)
    def root() -> RedirectResponse:
        return RedirectResponse("/login", status_code=302)

    @app.get("/login", response_class=HTMLResponse)
    def login_page(request: Request) -> Any:
        return templates.TemplateResponse(request, "login.html")

    @app.post("/api/login")
    def api_login(body: LoginBody, db: Session = Depends(get_db)) -> JSONResponse:
        norm = body.email.strip().lower()
        # Match bootstrap normalization; SQLite TEXT compare is case-sensitive for == on stored casing
        user = db.query(User).filter(func.lower(User.email) == norm).first()
        raw_pw = body.password or ""
        pw_ok = False
        if user is not None:
            if verify_password(raw_pw, user.password_hash):
                pw_ok = True
            elif raw_pw != raw_pw.strip() and verify_password(
                raw_pw.strip(), user.password_hash
            ):
                pw_ok = True
        if not pw_ok:
            return JSONResponse({"ok": False, "error": "Invalid email or password"}, status_code=401)
        token = create_access_token(subject=user.email, user_id=user.id)
        resp = JSONResponse({"ok": True})
        resp.set_cookie(
            key=COOKIE_NAME,
            value=token,
            httponly=True,
            samesite="lax",
            max_age=60 * 60 * 24 * 7,
        )
        return resp

    @app.get("/logout")
    def logout() -> RedirectResponse:
        r = RedirectResponse("/login", status_code=302)
        r.delete_cookie(COOKIE_NAME)
        return r

    @app.get("/api/inbox")
    def api_inbox(
        user: CurrentUser,
        db: Session = Depends(get_db),
        limit: int = Query(40, ge=1, le=100),
    ) -> dict:
        rows = (
            db.query(InboxNotification)
            .filter(InboxNotification.user_id == user.id)
            .order_by(InboxNotification.created_at.desc())
            .limit(limit)
            .all()
        )
        unread = (
            db.query(func.count())
            .select_from(InboxNotification)
            .filter(InboxNotification.user_id == user.id, InboxNotification.read_at.is_(None))
            .scalar()
        )
        return {
            "items": [
                {
                    "id": n.id,
                    "job_id": n.job_id,
                    "kind": n.kind,
                    "title": n.title,
                    "body": n.body,
                    "read_at": _iso_utc(n.read_at),
                    "created_at": _iso_utc(n.created_at),
                }
                for n in rows
            ],
            "unread_count": int(unread or 0),
        }

    @app.post("/api/inbox/mark-read")
    def api_inbox_mark_read(
        body: InboxMarkReadBody,
        user: CurrentUser,
        db: Session = Depends(get_db),
    ) -> dict:
        now = datetime.utcnow()
        if body.mark_all:
            db.query(InboxNotification).filter(
                InboxNotification.user_id == user.id,
                InboxNotification.read_at.is_(None),
            ).update({InboxNotification.read_at: now}, synchronize_session=False)
            db.commit()
            return {"ok": True}
        if not body.ids:
            raise HTTPException(400, "Provide ids or mark_all")
        db.query(InboxNotification).filter(
            InboxNotification.user_id == user.id,
            InboxNotification.id.in_(body.ids),
        ).update({InboxNotification.read_at: now}, synchronize_session=False)
        db.commit()
        return {"ok": True}

    @app.get("/jobs", response_class=HTMLResponse)
    def jobs_list(request: Request, user: CurrentUser, db: Session = Depends(get_db)) -> Any:
        jobs = (
            db.query(Job)
            .options(joinedload(Job.pairs), joinedload(Job.created_by_user))
            .order_by(Job.created_at.desc())
            .limit(100)
            .all()
        )
        job_rows = [
            {
                "job": j,
                "list_status": effective_job_list_status(j, list(j.pairs)),
                "stage_label": job_stage_label(j),
                "can_delete": user_can_edit_job(j, user),
            }
            for j in jobs
        ]
        return templates.TemplateResponse(
            request,
            "jobs_list.html",
            {"user": user, "job_rows": job_rows, "is_admin": is_admin_user(user)},
        )

    @app.get("/test-bank/new", response_class=HTMLResponse)
    def test_bank_legacy_redirect() -> RedirectResponse:
        return RedirectResponse("/test-bank-1/new", status_code=302)

    @app.get("/test-bank-1/new", response_class=HTMLResponse)
    def test_bank_1_new(
        request: Request,
        user: CurrentUser,
        db: Session = Depends(get_db),
    ) -> Any:
        return templates.TemplateResponse(
            request,
            "test_bank_1_new.html",
            {
                "user": user,
                "is_admin": is_admin_user(user),
                "multipart_ok": HAS_MULTIPART,
                "default_prompt_1": _system_prompt_for_ui(db, "test_bank_1", "prompt_1"),
                "default_test_bank_model": DEFAULT_TEST_BANK_MODEL,
                "test_bank_model_choices": TEST_BANK_OPENROUTER_MODEL_CHOICES,
            },
        )

    @app.get("/test-bank-2/new", response_class=HTMLResponse)
    def test_bank_2_new(
        request: Request,
        user: CurrentUser,
        db: Session = Depends(get_db),
    ) -> Any:
        return templates.TemplateResponse(
            request,
            "test_bank_2_new.html",
            {
                "user": user,
                "is_admin": is_admin_user(user),
                "multipart_ok": HAS_MULTIPART,
                "default_prompt_2": _system_prompt_for_ui(db, "test_bank_2", "prompt_2"),
                "default_test_bank_model": DEFAULT_TEST_BANK_MODEL,
                "test_bank_model_choices": TEST_BANK_OPENROUTER_MODEL_CHOICES,
            },
        )

    @app.get("/pre-ocr/new", response_class=HTMLResponse)
    def pre_ocr_new(
        request: Request,
        user: CurrentUser,
        db: Session = Depends(get_db),
    ) -> Any:
        return templates.TemplateResponse(
            request,
            "pre_ocr_new.html",
            {
                "user": user,
                "is_admin": is_admin_user(user),
                "multipart_ok": HAS_MULTIPART,
                "default_prompt": _system_prompt_for_ui(db, "pre_ocr_topic", "prompt"),
            },
        )

    @app.get("/ocr-extraction/new", response_class=HTMLResponse)
    def ocr_extraction_new(
        request: Request,
        user: CurrentUser,
        db: Session = Depends(get_db),
    ) -> Any:
        return templates.TemplateResponse(
            request,
            "ocr_extraction_new.html",
            {
                "user": user,
                "is_admin": is_admin_user(user),
                "multipart_ok": HAS_MULTIPART,
                "default_prompt": _system_prompt_for_ui(db, "ocr_extraction", "prompt"),
            },
        )

    @app.get("/table-notes/json-to-word/new", response_class=HTMLResponse)
    def table_notes_json_to_word_new(
        request: Request,
        user: CurrentUser,
    ) -> Any:
        return templates.TemplateResponse(
            request,
            "json_to_word_new.html",
            {
                "user": user,
                "is_admin": is_admin_user(user),
                "multipart_ok": HAS_MULTIPART,
            },
        )

    @app.get("/document-processing/json-to-word/new", response_class=HTMLResponse)
    def document_processing_json_to_word_new_redirect(user: CurrentUser) -> RedirectResponse:
        return RedirectResponse("/table-notes/json-to-word/new", status_code=302)

    @app.get("/document-processing/new", response_class=HTMLResponse)
    def document_processing_new(
        request: Request,
        user: CurrentUser,
        db: Session = Depends(get_db),
    ) -> Any:
        return templates.TemplateResponse(
            request,
            "document_processing_new.html",
            {
                "user": user,
                "is_admin": is_admin_user(user),
                "multipart_ok": HAS_MULTIPART,
                "default_prompt": _system_prompt_for_ui(db, "document_processing", "prompt"),
            },
        )

    @app.get("/image-notes/new", response_class=HTMLResponse)
    def image_notes_new(
        request: Request,
        user: CurrentUser,
        db: Session = Depends(get_db),
    ) -> Any:
        return templates.TemplateResponse(
            request,
            "image_notes_new.html",
            {
                "user": user,
                "is_admin": is_admin_user(user),
                "multipart_ok": HAS_MULTIPART,
                "default_prompt": _system_prompt_for_ui(db, "image_notes", "prompt"),
            },
        )

    @app.get("/table-notes/new", response_class=HTMLResponse)
    def table_notes_new(
        request: Request,
        user: CurrentUser,
        db: Session = Depends(get_db),
    ) -> Any:
        return templates.TemplateResponse(
            request,
            "table_notes_new.html",
            {
                "user": user,
                "is_admin": is_admin_user(user),
                "multipart_ok": HAS_MULTIPART,
                "default_prompt": _system_prompt_for_ui(db, "table_notes", "prompt"),
            },
        )

    @app.get("/importance-type/new", response_class=HTMLResponse)
    def importance_type_new(
        request: Request,
        user: CurrentUser,
        db: Session = Depends(get_db),
    ) -> Any:
        return templates.TemplateResponse(
            request,
            "importance_type_new.html",
            {
                "user": user,
                "is_admin": is_admin_user(user),
                "multipart_ok": HAS_MULTIPART,
                "default_prompt": _system_prompt_for_ui(db, "importance_type", "prompt"),
            },
        )

    @app.get("/flashcard/new", response_class=HTMLResponse)
    def flashcard_new(
        request: Request,
        user: CurrentUser,
        db: Session = Depends(get_db),
    ) -> Any:
        return templates.TemplateResponse(
            request,
            "flashcard_new.html",
            {
                "user": user,
                "is_admin": is_admin_user(user),
                "multipart_ok": HAS_MULTIPART,
                "default_prompt": _system_prompt_for_ui(db, "flashcard", "prompt"),
            },
        )

    @app.get("/chapter-summary/new", response_class=HTMLResponse)
    def chapter_summary_new(
        request: Request,
        user: CurrentUser,
        db: Session = Depends(get_db),
    ) -> Any:
        return templates.TemplateResponse(
            request,
            "chapter_summary_new.html",
            {
                "user": user,
                "is_admin": is_admin_user(user),
                "multipart_ok": HAS_MULTIPART,
                "default_prompt": _system_prompt_for_ui(db, "chapter_summary", "prompt"),
                "default_test_bank_model": DEFAULT_TEST_BANK_MODEL,
                "test_bank_model_choices": TEST_BANK_OPENROUTER_MODEL_CHOICES,
            },
        )

    @app.get("/voice-class/new", response_class=HTMLResponse)
    def voice_class_new(
        request: Request,
        user: CurrentUser,
        db: Session = Depends(get_db),
    ) -> Any:
        from api_layer import APIConfig

        return templates.TemplateResponse(
            request,
            "voice_class_new.html",
            {
                "user": user,
                "is_admin": is_admin_user(user),
                "multipart_ok": HAS_MULTIPART,
                "default_prompt_1": _system_prompt_for_ui(db, "voice_class", "prompt_1"),
                "default_tts_instruction": _system_prompt_for_ui(db, "voice_class", "tts_instruction"),
                "default_test_bank_model": DEFAULT_TEST_BANK_MODEL,
                "test_bank_model_choices": TEST_BANK_OPENROUTER_MODEL_CHOICES,
                "default_tts_model": DEFAULT_VOICE_CLASS_TTS_MODEL,
                "default_tts_voice": DEFAULT_VOICE_CLASS_TTS_VOICE,
                "tts_model_choices": APIConfig.TTS_MODELS,
                "tts_voice_choices": APIConfig.TTS_VOICES,
            },
        )

    @app.post("/admin/prompt-defaults/{job_type}")
    async def post_admin_prompt_defaults(
        job_type: str,
        request: Request,
        user: CurrentUser,
        db: Session = Depends(get_db),
    ) -> RedirectResponse:
        require_admin(user)
        jt = (job_type or "").strip()
        if not job_type_has_editable_prompts(jt):
            raise HTTPException(400, "This job type has no editable LLM prompts.")
        form = await request.form()
        try:
            apply_submitted_system_prompts(db, jt, form, user.id)
        except ValueError as exc:
            raise HTTPException(400, str(exc)) from exc
        redirect_path = NEW_JOB_PAGE_BY_TYPE.get(jt, "/jobs")
        return RedirectResponse(f"{redirect_path}?prompt_saved=1", status_code=302)

    @app.get("/admin/gemini-tts-keys", response_class=HTMLResponse)
    def admin_gemini_tts_keys_page(
        request: Request,
        user: CurrentUser,
        db: Session = Depends(get_db),
    ) -> Any:
        require_admin(user)
        from webapp.gemini_tts_keys_service import list_keys_for_admin

        imported = request.query_params.get("imported")
        added = request.query_params.get("added")
        skipped = request.query_params.get("skipped")
        err = request.query_params.get("error")
        return templates.TemplateResponse(
            request,
            "admin_gemini_tts_keys.html",
            {
                "user": user,
                "keys": list_keys_for_admin(db),
                "imported": imported,
                "added": added,
                "skipped": skipped,
                "error": err,
            },
        )

    @app.post("/admin/gemini-tts-keys/add")
    async def admin_gemini_tts_keys_add(
        request: Request,
        user: CurrentUser,
        db: Session = Depends(get_db),
    ) -> RedirectResponse:
        require_admin(user)
        form = await request.form()
        from webapp.gemini_tts_keys_service import add_key_manual

        ok, msg = add_key_manual(
            db,
            account_name=str(form.get("account_name") or ""),
            project_name=str(form.get("project_name") or ""),
            api_key=str(form.get("api_key") or ""),
            admin_user_id=user.id,
        )
        if not ok:
            return RedirectResponse(f"/admin/gemini-tts-keys?error={msg}", status_code=302)
        return RedirectResponse("/admin/gemini-tts-keys?imported=1&added=1", status_code=302)

    @app.post("/admin/gemini-tts-keys/import")
    async def admin_gemini_tts_keys_import(
        user: CurrentUser,
        db: Session = Depends(get_db),
        import_file: UploadFile = File(...),
    ) -> RedirectResponse:
        require_admin(user)
        from webapp.gemini_tts_keys_service import import_keys_bytes

        content = await import_file.read()
        filename = import_file.filename or ""
        fallback = (
            filename.replace(".csv", "")
            .replace(".xlsx", "")
            .replace(".xlsm", "")
            .replace(" apikey", "")
            .strip()
        )
        added, skipped, errors = import_keys_bytes(
            db,
            content,
            filename=filename,
            fallback_account=fallback,
            admin_user_id=user.id,
        )
        if errors and added == 0:
            return RedirectResponse(
                f"/admin/gemini-tts-keys?error={errors[0][:200]}",
                status_code=302,
            )
        return RedirectResponse(
            f"/admin/gemini-tts-keys?imported=1&added={added}&skipped={skipped}",
            status_code=302,
        )

    @app.post("/admin/gemini-tts-keys/{key_id}/toggle")
    def admin_gemini_tts_keys_toggle(
        key_id: int,
        user: CurrentUser,
        db: Session = Depends(get_db),
        active: str = Form("1"),
    ) -> RedirectResponse:
        require_admin(user)
        from webapp.gemini_tts_keys_service import toggle_key_active

        toggle_key_active(db, key_id, active in ("1", "true", "on", "yes"))
        return RedirectResponse("/admin/gemini-tts-keys", status_code=302)

    @app.post("/admin/gemini-tts-keys/{key_id}/delete")
    def admin_gemini_tts_keys_delete(
        key_id: int,
        user: CurrentUser,
        db: Session = Depends(get_db),
    ) -> RedirectResponse:
        require_admin(user)
        from webapp.gemini_tts_keys_service import delete_key

        delete_key(db, key_id)
        return RedirectResponse("/admin/gemini-tts-keys", status_code=302)

    @app.get("/image-file-catalog/new", response_class=HTMLResponse)
    def image_file_catalog_new(request: Request, user: CurrentUser) -> Any:
        return templates.TemplateResponse(
            request,
            "image_file_catalog_new.html",
            {
                "user": user,
                "multipart_ok": HAS_MULTIPART,
            },
        )

    @app.get("/json-to-csv/new", response_class=HTMLResponse)
    def json_to_csv_new(request: Request, user: CurrentUser, type: Optional[str] = None) -> Any:
        from webapp.json_to_csv_jobs import JSON_TO_CSV_CONVERSION_ORDER, json_to_csv_page_context

        ctx = json_to_csv_page_context()
        t = (type or "").strip()
        if t in JSON_TO_CSV_CONVERSION_ORDER:
            ctx["default_conversion"] = t
        return templates.TemplateResponse(
            request,
            "json_to_csv_new.html",
            {
                "user": user,
                "multipart_ok": HAS_MULTIPART,
                **ctx,
            },
        )

    @app.get("/chapter-summary/json-to-csv/new", response_class=HTMLResponse)
    def chapter_summary_json_to_csv_new_redirect(user: CurrentUser) -> RedirectResponse:
        return RedirectResponse("/json-to-csv/new?type=chapter_summary", status_code=302)

    @app.get("/image-catalog/json-to-csv/new", response_class=HTMLResponse)
    def image_catalog_json_to_csv_new_redirect(user: CurrentUser) -> RedirectResponse:
        return RedirectResponse("/json-to-csv/new?type=image_catalog", status_code=302)

    @app.get("/test-bank-2/json-to-csv/new", response_class=HTMLResponse)
    def test_bank_2_json_to_csv_new_redirect(user: CurrentUser) -> RedirectResponse:
        return RedirectResponse("/json-to-csv/new?type=test_bank_2", status_code=302)

    @app.get("/flashcard/json-to-csv/new", response_class=HTMLResponse)
    def flashcard_json_to_csv_new_redirect(user: CurrentUser) -> RedirectResponse:
        return RedirectResponse("/json-to-csv/new?type=flashcard", status_code=302)

    if HAS_MULTIPART:

        @app.post("/jobs/test-bank-1")
        async def create_test_bank_1_job(
            user: CurrentUser,
            db: Session = Depends(get_db),
            stage_j_files: List[UploadFile] = File(...),
            word_files: List[UploadFile] = File(...),
            prompt_1: str = Form(""),
            provider_1: str = Form(DEFAULT_TEST_BANK_PROVIDER),
            model_1: str = Form(DEFAULT_TEST_BANK_MODEL),
            delay_seconds: str = Form("5"),
            job_name: str = Form(""),
        ) -> RedirectResponse:
            if not stage_j_files or not word_files:
                raise HTTPException(400, "Stage J and Word files required")
            name_stripped = (job_name or "").strip()
            if not name_stripped:
                raise HTTPException(400, "Job name is required (a short label to find this job in the list).")
            if len(name_stripped) > 200:
                raise HTTPException(400, "Job name must be at most 200 characters.")

            tmp = tempfile.mkdtemp(prefix="tb1_upload_")
            try:
                j_paths: List[str] = []
                for uf in stage_j_files:
                    if not uf.filename:
                        continue
                    dest = os.path.join(tmp, os.path.basename(uf.filename))
                    content = await uf.read()
                    with open(dest, "wb") as f:
                        f.write(content)
                    j_paths.append(dest)

                w_paths: List[str] = []
                for uf in word_files:
                    if not uf.filename:
                        continue
                    dest = os.path.join(tmp, os.path.basename(uf.filename))
                    content = await uf.read()
                    with open(dest, "wb") as f:
                        f.write(content)
                    w_paths.append(dest)

                pairs_spec = auto_pair_stage_v_files(j_paths, w_paths)
                if not pairs_spec:
                    raise HTTPException(400, "No pairable Stage J files (check PointId / filenames)")

                job_id = str(uuid.uuid4())
                root = job_root(job_id)
                os.makedirs(root, exist_ok=True)

                try:
                    delay_val = float(delay_seconds)
                except ValueError:
                    delay_val = 5.0

                display_name = name_stripped
                prompt_1_eff = _prompt_for_new_job(db, "test_bank_1", prompt_1, "prompt_1")
                model_1_eff = normalize_test_bank_model(model_1)
                if model_1_eff not in TEST_BANK_OPENROUTER_MODEL_CHOICES:
                    raise HTTPException(400, "Invalid OpenRouter model for Step 1.")
                provider_1_eff = normalize_test_bank_provider(provider_1)
                cfg = {
                    "display_name": display_name,
                    "prompt_1": prompt_1_eff,
                    "provider_1": provider_1_eff,
                    "model_1": model_1_eff,
                    "delay_seconds": delay_val,
                }

                job = Job(
                    id=job_id,
                    type="test_bank_1",
                    status="draft",
                    created_by_id=user.id,
                    config_json=json.dumps(cfg, ensure_ascii=False),
                )
                db.add(job)
                db.flush()

                for pair_index, p in enumerate(pairs_spec):
                    sj = p["stage_j_path"]
                    wj = p.get("word_path")
                    ensure_dirs(job_id, pair_index)
                    sj_name = os.path.basename(sj)
                    rel_j = f"pair_{pair_index}/inputs/{sj_name}"
                    shutil.copy2(sj, os.path.join(root, rel_j.replace("/", os.sep)))
                    rel_w = None
                    if wj:
                        w_name = os.path.basename(wj)
                        rel_w = f"pair_{pair_index}/inputs/{w_name}"
                        shutil.copy2(wj, os.path.join(root, rel_w.replace("/", os.sep)))

                    db.add(
                        JobPair(
                            job_id=job_id,
                            pair_index=pair_index,
                            stage_j_filename=sj_name,
                            word_filename=os.path.basename(wj) if wj else "",
                            stage_j_relpath=rel_j,
                            word_relpath=rel_w,
                            output_relpath=f"pair_{pair_index}/output",
                        )
                    )
                    register_input_artifact(db, job_id, pair_index, root, rel_j, "upload_stage_j")
                    if rel_w:
                        register_input_artifact(db, job_id, pair_index, root, rel_w, "upload_word")

                db.commit()
            finally:
                shutil.rmtree(tmp, ignore_errors=True)

            return RedirectResponse(f"/jobs/{job_id}", status_code=302)

        @app.post("/jobs/test-bank-2")
        async def create_test_bank_2_job(
            user: CurrentUser,
            db: Session = Depends(get_db),
            stage_j_files: List[UploadFile] = File(...),
            word_files: List[UploadFile] = File(...),
            step1_combined_files: List[UploadFile] = File(...),
            prompt_2: str = Form(""),
            provider_2: str = Form(DEFAULT_TEST_BANK_PROVIDER),
            model_2: str = Form(DEFAULT_TEST_BANK_MODEL),
            model_1: str = Form(DEFAULT_TEST_BANK_MODEL),
            delay_seconds: str = Form("5"),
            job_name: str = Form(""),
        ) -> RedirectResponse:
            if not stage_j_files or not word_files or not step1_combined_files:
                raise HTTPException(
                    400,
                    "Stage J JSON, Word files, and Test Bank 1 combined JSON (step1_combined_*.json) are required",
                )
            name_stripped = (job_name or "").strip()
            if not name_stripped:
                raise HTTPException(400, "Job name is required (a short label to find this job in the list).")
            if len(name_stripped) > 200:
                raise HTTPException(400, "Job name must be at most 200 characters.")

            tmp = tempfile.mkdtemp(prefix="tb2_upload_")
            try:
                j_paths: List[str] = []
                for uf in stage_j_files:
                    if not uf.filename:
                        continue
                    dest = os.path.join(tmp, os.path.basename(uf.filename))
                    content = await uf.read()
                    with open(dest, "wb") as f:
                        f.write(content)
                    j_paths.append(dest)

                w_paths: List[str] = []
                for uf in word_files:
                    if not uf.filename:
                        continue
                    dest = os.path.join(tmp, os.path.basename(uf.filename))
                    content = await uf.read()
                    with open(dest, "wb") as f:
                        f.write(content)
                    w_paths.append(dest)

                s1_paths: List[str] = []
                for uf in step1_combined_files:
                    if not uf.filename:
                        continue
                    dest = os.path.join(tmp, os.path.basename(uf.filename))
                    content = await uf.read()
                    with open(dest, "wb") as f:
                        f.write(content)
                    s1_paths.append(dest)

                if not s1_paths:
                    raise HTTPException(400, "Upload at least one Test Bank 1 combined JSON file.")

                pairs_spec = auto_pair_stage_v_files(j_paths, w_paths)
                if not pairs_spec:
                    raise HTTPException(400, "No pairable Stage J files (check PointId / filenames)")
                attach_step1_combined_uploads_to_pairs(pairs_spec, s1_paths)
                for i, p in enumerate(pairs_spec):
                    if not p.get("step1_combined_upload"):
                        raise HTTPException(
                            400,
                            f"Could not match a Step 1 combined JSON to pair {i} (book/chapter). "
                            "Upload one combined file per Stage J file or fix filenames.",
                        )

                job_id = str(uuid.uuid4())
                root = job_root(job_id)
                os.makedirs(root, exist_ok=True)

                try:
                    delay_val = float(delay_seconds)
                except ValueError:
                    delay_val = 5.0

                step1_rel_by_pair: dict[str, str] = {}
                prompt_2_eff = _prompt_for_new_job(db, "test_bank_2", prompt_2, "prompt_2")
                model_2_eff = normalize_test_bank_model(model_2)
                model_1_eff_tb2 = normalize_test_bank_model(model_1)
                if model_2_eff not in TEST_BANK_OPENROUTER_MODEL_CHOICES:
                    raise HTTPException(400, "Invalid OpenRouter model for Step 2.")
                if model_1_eff_tb2 not in TEST_BANK_OPENROUTER_MODEL_CHOICES:
                    raise HTTPException(400, "Invalid OpenRouter model for Step 1 reference.")
                provider_2_eff = normalize_test_bank_provider(provider_2)
                cfg = {
                    "display_name": name_stripped,
                    "prompt_2": prompt_2_eff,
                    "provider_2": provider_2_eff,
                    "model_2": model_2_eff,
                    "model_1": model_1_eff_tb2,
                    "delay_seconds": delay_val,
                    "step1_combined_relpaths": step1_rel_by_pair,
                }

                job = Job(
                    id=job_id,
                    type="test_bank_2",
                    status="draft",
                    created_by_id=user.id,
                    config_json=json.dumps(cfg, ensure_ascii=False),
                )
                db.add(job)
                db.flush()

                for pair_index, p in enumerate(pairs_spec):
                    sj = p["stage_j_path"]
                    wj = p.get("word_path")
                    s1_src = p.get("step1_combined_upload")
                    ensure_dirs(job_id, pair_index)
                    sj_name = os.path.basename(sj)
                    rel_j = f"pair_{pair_index}/inputs/{sj_name}"
                    shutil.copy2(sj, os.path.join(root, rel_j.replace("/", os.sep)))
                    rel_w = None
                    if wj:
                        w_name = os.path.basename(wj)
                        rel_w = f"pair_{pair_index}/inputs/{w_name}"
                        shutil.copy2(wj, os.path.join(root, rel_w.replace("/", os.sep)))
                    s1_name = os.path.basename(s1_src) if s1_src else "step1_combined.json"
                    rel_s1 = f"pair_{pair_index}/inputs/{s1_name}"
                    if s1_src:
                        shutil.copy2(s1_src, os.path.join(root, rel_s1.replace("/", os.sep)))
                    step1_rel_by_pair[str(pair_index)] = rel_s1

                    db.add(
                        JobPair(
                            job_id=job_id,
                            pair_index=pair_index,
                            stage_j_filename=sj_name,
                            word_filename=os.path.basename(wj) if wj else "",
                            stage_j_relpath=rel_j,
                            word_relpath=rel_w,
                            output_relpath=f"pair_{pair_index}/output",
                        )
                    )
                    register_input_artifact(db, job_id, pair_index, root, rel_j, "upload_stage_j")
                    if rel_w:
                        register_input_artifact(db, job_id, pair_index, root, rel_w, "upload_word")
                    register_input_artifact(db, job_id, pair_index, root, rel_s1, "upload_step1_combined_json")

                job.config_json = json.dumps(cfg, ensure_ascii=False)
                db.commit()
            finally:
                shutil.rmtree(tmp, ignore_errors=True)

            return RedirectResponse(f"/jobs/{job_id}", status_code=302)

        @app.post("/jobs/test-bank")
        def create_test_bank_legacy_redirect(user: CurrentUser) -> None:
            raise HTTPException(
                410,
                "Test Bank is split into Test Bank 1 and Test Bank 2. Use /test-bank-1/new or /test-bank-2/new.",
            )

        @app.post("/jobs/pre-ocr")
        async def create_pre_ocr_job(
            user: CurrentUser,
            db: Session = Depends(get_db),
            pdf_files: List[UploadFile] = File(...),
            prompt: str = Form(""),
            provider: str = Form("openrouter"),
            model: str = Form("z-ai/glm-5"),
            delay_seconds: str = Form("5"),
            job_name: str = Form(""),
        ) -> RedirectResponse:
            if not pdf_files:
                raise HTTPException(400, "At least one PDF is required")
            name_stripped = (job_name or "").strip()
            if not name_stripped:
                raise HTTPException(400, "Job name is required (a short label to find this job in the list).")
            if len(name_stripped) > 200:
                raise HTTPException(400, "Job name must be at most 200 characters.")

            tmp = tempfile.mkdtemp(prefix="preocr_upload_")
            try:
                pdf_paths: List[str] = []
                for uf in pdf_files:
                    if not uf.filename:
                        continue
                    low = uf.filename.lower()
                    if not low.endswith(".pdf"):
                        raise HTTPException(400, f"Not a PDF: {uf.filename}")
                    dest = os.path.join(tmp, os.path.basename(uf.filename))
                    content = await uf.read()
                    with open(dest, "wb") as f:
                        f.write(content)
                    pdf_paths.append(dest)
                if not pdf_paths:
                    raise HTTPException(400, "No PDF files uploaded")

                try:
                    delay_val = float(delay_seconds)
                except ValueError:
                    delay_val = 5.0

                prompt_eff = _prompt_for_new_job(db, "pre_ocr_topic", prompt, "prompt")
                cfg = {
                    "display_name": name_stripped,
                    "prompt": prompt_eff,
                    "provider": provider,
                    "model": model,
                    "delay_seconds": delay_val,
                }
                job_id = str(uuid.uuid4())
                root = job_root(job_id)
                os.makedirs(root, exist_ok=True)

                job = Job(
                    id=job_id,
                    type="pre_ocr_topic",
                    status="draft",
                    created_by_id=user.id,
                    config_json=json.dumps(cfg, ensure_ascii=False),
                )
                db.add(job)
                db.flush()

                sorted_pdfs = _sorted_nonempty_paths(pdf_paths)
                for pair_index, pdf_path in enumerate(sorted_pdfs):
                    ensure_dirs(job_id, pair_index)
                    base_name = os.path.basename(pdf_path)
                    rel_pdf = f"pair_{pair_index}/inputs/{base_name}"
                    shutil.copy2(pdf_path, os.path.join(root, rel_pdf.replace("/", os.sep)))
                    db.add(
                        JobPair(
                            job_id=job_id,
                            pair_index=pair_index,
                            stage_j_filename=base_name,
                            word_filename="",
                            stage_j_relpath=rel_pdf,
                            word_relpath=None,
                            output_relpath=f"pair_{pair_index}/output",
                        )
                    )
                    register_input_artifact(db, job_id, pair_index, root, rel_pdf, "upload_pdf")

                db.commit()
            finally:
                shutil.rmtree(tmp, ignore_errors=True)

            return RedirectResponse(f"/jobs/{job_id}", status_code=302)

        @app.post("/jobs/ocr-extraction")
        async def create_ocr_extraction_job(
            user: CurrentUser,
            db: Session = Depends(get_db),
            pdf_files: List[UploadFile] = File(...),
            topic_json_files: List[UploadFile] = File(...),
            prompt: str = Form(""),
            provider: str = Form("openrouter"),
            model: str = Form("z-ai/glm-5"),
            delay_seconds: str = Form("5"),
            job_name: str = Form(""),
        ) -> RedirectResponse:
            name_stripped = (job_name or "").strip()
            if not name_stripped:
                raise HTTPException(400, "Job name is required (a short label to find this job in the list).")
            if len(name_stripped) > 200:
                raise HTTPException(400, "Job name must be at most 200 characters.")

            tmp = tempfile.mkdtemp(prefix="ocr_ext_upload_")
            try:
                pdf_paths: List[str] = []
                for uf in pdf_files:
                    if not uf.filename:
                        continue
                    dest = os.path.join(tmp, "pdf", os.path.basename(uf.filename))
                    os.makedirs(os.path.dirname(dest), exist_ok=True)
                    content = await uf.read()
                    with open(dest, "wb") as f:
                        f.write(content)
                    pdf_paths.append(dest)

                json_paths: List[str] = []
                for uf in topic_json_files:
                    if not uf.filename:
                        continue
                    dest = os.path.join(tmp, "json", os.path.basename(uf.filename))
                    os.makedirs(os.path.dirname(dest), exist_ok=True)
                    content = await uf.read()
                    with open(dest, "wb") as f:
                        f.write(content)
                    json_paths.append(dest)

                pdfs = _sorted_nonempty_paths(pdf_paths)
                topics = _sorted_nonempty_paths(json_paths)
                if len(pdfs) != len(topics):
                    raise HTTPException(
                        400,
                        f"PDF count ({len(pdfs)}) must match topic JSON count ({len(topics)}). "
                        "Sort order is by filename within each group.",
                    )
                if not pdfs:
                    raise HTTPException(400, "Upload at least one PDF and one topic JSON")

                try:
                    delay_val = float(delay_seconds)
                except ValueError:
                    delay_val = 5.0

                prompt_eff = _prompt_for_new_job(db, "ocr_extraction", prompt, "prompt")
                cfg = {
                    "display_name": name_stripped,
                    "prompt": prompt_eff,
                    "provider": provider,
                    "model": model,
                    "delay_seconds": delay_val,
                }
                job_id = str(uuid.uuid4())
                root = job_root(job_id)
                os.makedirs(root, exist_ok=True)

                job = Job(
                    id=job_id,
                    type="ocr_extraction",
                    status="draft",
                    created_by_id=user.id,
                    config_json=json.dumps(cfg, ensure_ascii=False),
                )
                db.add(job)
                db.flush()

                for pair_index, (pdf_p, top_p) in enumerate(zip(pdfs, topics)):
                    ensure_dirs(job_id, pair_index)
                    pj = os.path.basename(pdf_p)
                    tj = os.path.basename(top_p)
                    rel_pdf = f"pair_{pair_index}/inputs/{pj}"
                    rel_topic = f"pair_{pair_index}/inputs/{tj}"
                    shutil.copy2(pdf_p, os.path.join(root, rel_pdf.replace("/", os.sep)))
                    shutil.copy2(top_p, os.path.join(root, rel_topic.replace("/", os.sep)))
                    db.add(
                        JobPair(
                            job_id=job_id,
                            pair_index=pair_index,
                            stage_j_filename=pj,
                            word_filename=tj,
                            stage_j_relpath=rel_pdf,
                            word_relpath=rel_topic,
                            output_relpath=f"pair_{pair_index}/output",
                        )
                    )
                    register_input_artifact(db, job_id, pair_index, root, rel_pdf, "upload_pdf")
                    register_input_artifact(db, job_id, pair_index, root, rel_topic, "upload_topic_json")

                db.commit()
            finally:
                shutil.rmtree(tmp, ignore_errors=True)

            return RedirectResponse(f"/jobs/{job_id}", status_code=302)

        @app.post("/jobs/document-processing")
        async def create_document_processing_job(
            user: CurrentUser,
            db: Session = Depends(get_db),
            ocr_json_files: List[UploadFile] = File(...),
            prompt: str = Form(""),
            provider: str = Form("openrouter"),
            model: str = Form("z-ai/glm-5"),
            delay_seconds: str = Form("5"),
            job_name: str = Form(""),
            start_pointid: str = Form("1050030001"),
            pointid_txt: Optional[UploadFile] = File(None),
        ) -> RedirectResponse:
            name_stripped = (job_name or "").strip()
            if not name_stripped:
                raise HTTPException(400, "Job name is required (a short label to find this job in the list).")
            if len(name_stripped) > 200:
                raise HTTPException(400, "Job name must be at most 200 characters.")
            start_pointid_str = (start_pointid or "").strip()
            if not (start_pointid_str.isdigit() and len(start_pointid_str) == 10):
                raise HTTPException(400, "Start PointId must be a 10-digit number, e.g. 1050030001.")
            book_id = int(start_pointid_str[0:3])
            chapter_id = int(start_pointid_str[3:6])
            start_point_index = int(start_pointid_str[6:10])

            tmp = tempfile.mkdtemp(prefix="docproc_upload_")
            try:
                json_paths: List[str] = []
                for uf in ocr_json_files:
                    if not uf.filename:
                        continue
                    dest = os.path.join(tmp, os.path.basename(uf.filename))
                    content = await uf.read()
                    with open(dest, "wb") as f:
                        f.write(content)
                    json_paths.append(dest)
                if not json_paths:
                    raise HTTPException(400, "At least one OCR JSON file is required")

                try:
                    delay_val = float(delay_seconds)
                except ValueError:
                    delay_val = 5.0

                prompt_eff = _prompt_for_new_job(db, "document_processing", prompt, "prompt")
                pointid_rel: Optional[str] = None
                job_id = str(uuid.uuid4())
                root = job_root(job_id)
                os.makedirs(root, exist_ok=True)

                if _is_nonempty_file_upload(pointid_txt):
                    pn = os.path.basename(pointid_txt.filename or "pointid_mapping.txt")
                    pointid_rel = f"mappings/{pn}"
                    dest_pi = os.path.join(root, pointid_rel.replace("/", os.sep))
                    os.makedirs(os.path.dirname(dest_pi), exist_ok=True)
                    data = await _read_upload_bytes(pointid_txt)
                    with open(dest_pi, "wb") as f:
                        f.write(data)

                cfg = {
                    "display_name": name_stripped,
                    "prompt": prompt_eff,
                    "provider": provider,
                    "model": model,
                    "delay_seconds": delay_val,
                    "book_id": book_id,
                    "chapter_id": chapter_id,
                    "start_point_index": start_point_index,
                    "start_pointid": start_pointid_str,
                }
                if pointid_rel:
                    cfg["pointid_mapping_relpath"] = pointid_rel.replace("\\", "/")

                job = Job(
                    id=job_id,
                    type="document_processing",
                    status="draft",
                    created_by_id=user.id,
                    config_json=json.dumps(cfg, ensure_ascii=False),
                )
                db.add(job)
                db.flush()

                sorted_json = _sorted_nonempty_paths(json_paths)
                for pair_index, jp in enumerate(sorted_json):
                    ensure_dirs(job_id, pair_index)
                    jn = os.path.basename(jp)
                    rel_j = f"pair_{pair_index}/inputs/{jn}"
                    shutil.copy2(jp, os.path.join(root, rel_j.replace("/", os.sep)))
                    db.add(
                        JobPair(
                            job_id=job_id,
                            pair_index=pair_index,
                            stage_j_filename=jn,
                            word_filename="",
                            stage_j_relpath=rel_j,
                            word_relpath=None,
                            output_relpath=f"pair_{pair_index}/output",
                        )
                    )
                    register_input_artifact(db, job_id, pair_index, root, rel_j, "upload_ocr_json")
                if pointid_rel:
                    register_input_artifact(db, job_id, 0, root, pointid_rel.replace("\\", "/"), "upload_pointid_txt")

                db.commit()
            finally:
                shutil.rmtree(tmp, ignore_errors=True)

            return RedirectResponse(f"/jobs/{job_id}", status_code=302)

        @app.post("/jobs/image-notes")
        async def create_image_notes_job(
            user: CurrentUser,
            db: Session = Depends(get_db),
            stage4_json_files: List[UploadFile] = File(...),
            ocr_json_files: List[UploadFile] = File(...),
            prompt: str = Form(""),
            provider: str = Form("openrouter"),
            model: str = Form("z-ai/glm-5"),
            delay_seconds: str = Form("5"),
            job_name: str = Form(""),
        ) -> RedirectResponse:
            name_stripped = (job_name or "").strip()
            if not name_stripped:
                raise HTTPException(400, "Job name is required (a short label to find this job in the list).")
            if len(name_stripped) > 200:
                raise HTTPException(400, "Job name must be at most 200 characters.")

            tmp = tempfile.mkdtemp(prefix="imgnotes_upload_")
            try:
                s4_paths: List[str] = []
                for uf in stage4_json_files:
                    if not uf.filename:
                        continue
                    dest = os.path.join(tmp, "s4", os.path.basename(uf.filename))
                    os.makedirs(os.path.dirname(dest), exist_ok=True)
                    content = await uf.read()
                    with open(dest, "wb") as f:
                        f.write(content)
                    s4_paths.append(dest)

                ocr_paths: List[str] = []
                for uf in ocr_json_files:
                    if not uf.filename:
                        continue
                    dest = os.path.join(tmp, "ocr", os.path.basename(uf.filename))
                    os.makedirs(os.path.dirname(dest), exist_ok=True)
                    content = await uf.read()
                    with open(dest, "wb") as f:
                        f.write(content)
                    ocr_paths.append(dest)

                s4_sorted = _sorted_nonempty_paths(s4_paths)
                ocr_sorted = _sorted_nonempty_paths(ocr_paths)
                if len(s4_sorted) != len(ocr_sorted):
                    raise HTTPException(
                        400,
                        f"Document processing JSON count ({len(s4_sorted)}) must match OCR extraction JSON count ({len(ocr_sorted)}). "
                        "Sort order is by filename within each group.",
                    )
                if not s4_sorted:
                    raise HTTPException(
                        400,
                        "Upload at least one Document processing (Stage 4) JSON and one OCR extraction JSON.",
                    )

                try:
                    delay_val = float(delay_seconds)
                except ValueError:
                    delay_val = 5.0

                prompt_eff = _prompt_for_new_job(db, "image_notes", prompt, "prompt")
                cfg = {
                    "display_name": name_stripped,
                    "prompt": prompt_eff,
                    "provider": provider,
                    "model": model,
                    "delay_seconds": delay_val,
                }
                job_id = str(uuid.uuid4())
                root = job_root(job_id)
                os.makedirs(root, exist_ok=True)

                job = Job(
                    id=job_id,
                    type="image_notes",
                    status="draft",
                    created_by_id=user.id,
                    config_json=json.dumps(cfg, ensure_ascii=False),
                )
                db.add(job)
                db.flush()

                for pair_index, (s4_p, ocr_p) in enumerate(zip(s4_sorted, ocr_sorted)):
                    ensure_dirs(job_id, pair_index)
                    s4n = os.path.basename(s4_p)
                    ocrn = os.path.basename(ocr_p)
                    rel_s4 = f"pair_{pair_index}/inputs/{s4n}"
                    rel_ocr = f"pair_{pair_index}/inputs/{ocrn}"
                    shutil.copy2(s4_p, os.path.join(root, rel_s4.replace("/", os.sep)))
                    shutil.copy2(ocr_p, os.path.join(root, rel_ocr.replace("/", os.sep)))
                    db.add(
                        JobPair(
                            job_id=job_id,
                            pair_index=pair_index,
                            stage_j_filename=s4n,
                            word_filename=ocrn,
                            stage_j_relpath=rel_s4,
                            word_relpath=rel_ocr,
                            output_relpath=f"pair_{pair_index}/output",
                        )
                    )
                    register_input_artifact(db, job_id, pair_index, root, rel_s4, "upload_stage4_json")
                    register_input_artifact(db, job_id, pair_index, root, rel_ocr, "upload_ocr_json")

                db.commit()
            finally:
                shutil.rmtree(tmp, ignore_errors=True)

            return RedirectResponse(f"/jobs/{job_id}", status_code=302)

        @app.post("/jobs/table-notes")
        async def create_table_notes_job(
            user: CurrentUser,
            db: Session = Depends(get_db),
            stage_e_json_files: List[UploadFile] = File(...),
            ocr_json_files: List[UploadFile] = File(...),
            prompt: str = Form(""),
            provider: str = Form("openrouter"),
            model: str = Form("z-ai/glm-5"),
            delay_seconds: str = Form("5"),
            job_name: str = Form(""),
        ) -> RedirectResponse:
            name_stripped = (job_name or "").strip()
            if not name_stripped:
                raise HTTPException(400, "Job name is required (a short label to find this job in the list).")
            if len(name_stripped) > 200:
                raise HTTPException(400, "Job name must be at most 200 characters.")

            tmp = tempfile.mkdtemp(prefix="tblnotes_upload_")
            try:
                se_paths: List[str] = []
                for uf in stage_e_json_files:
                    if not uf.filename:
                        continue
                    dest = os.path.join(tmp, "se", os.path.basename(uf.filename))
                    os.makedirs(os.path.dirname(dest), exist_ok=True)
                    content = await uf.read()
                    with open(dest, "wb") as f:
                        f.write(content)
                    se_paths.append(dest)

                ocr_paths: List[str] = []
                for uf in ocr_json_files:
                    if not uf.filename:
                        continue
                    dest = os.path.join(tmp, "ocr", os.path.basename(uf.filename))
                    os.makedirs(os.path.dirname(dest), exist_ok=True)
                    content = await uf.read()
                    with open(dest, "wb") as f:
                        f.write(content)
                    ocr_paths.append(dest)

                se_sorted = _sorted_nonempty_paths(se_paths)
                ocr_sorted = _sorted_nonempty_paths(ocr_paths)
                if len(se_sorted) != len(ocr_sorted):
                    raise HTTPException(
                        400,
                        f"Image notes (Stage E) JSON count ({len(se_sorted)}) must match OCR extraction JSON count ({len(ocr_sorted)}). "
                        "Sort order is by filename within each group.",
                    )
                if not se_sorted:
                    raise HTTPException(
                        400,
                        "Upload at least one Image notes (Stage E) JSON and one OCR extraction JSON.",
                    )

                try:
                    delay_val = float(delay_seconds)
                except ValueError:
                    delay_val = 5.0

                prompt_eff = _prompt_for_new_job(db, "table_notes", prompt, "prompt")
                cfg = {
                    "display_name": name_stripped,
                    "prompt": prompt_eff,
                    "provider": provider,
                    "model": model,
                    "delay_seconds": delay_val,
                }
                job_id = str(uuid.uuid4())
                root = job_root(job_id)
                os.makedirs(root, exist_ok=True)

                job = Job(
                    id=job_id,
                    type="table_notes",
                    status="draft",
                    created_by_id=user.id,
                    config_json=json.dumps(cfg, ensure_ascii=False),
                )
                db.add(job)
                db.flush()

                for pair_index, (se_p, ocr_p) in enumerate(zip(se_sorted, ocr_sorted)):
                    ensure_dirs(job_id, pair_index)
                    sen = os.path.basename(se_p)
                    ocrn = os.path.basename(ocr_p)
                    rel_se = f"pair_{pair_index}/inputs/{sen}"
                    rel_ocr = f"pair_{pair_index}/inputs/{ocrn}"
                    shutil.copy2(se_p, os.path.join(root, rel_se.replace("/", os.sep)))
                    shutil.copy2(ocr_p, os.path.join(root, rel_ocr.replace("/", os.sep)))
                    db.add(
                        JobPair(
                            job_id=job_id,
                            pair_index=pair_index,
                            stage_j_filename=sen,
                            word_filename=ocrn,
                            stage_j_relpath=rel_se,
                            word_relpath=rel_ocr,
                            output_relpath=f"pair_{pair_index}/output",
                        )
                    )
                    register_input_artifact(db, job_id, pair_index, root, rel_se, "upload_stage_e_json")
                    register_input_artifact(db, job_id, pair_index, root, rel_ocr, "upload_ocr_json")

                db.commit()
            finally:
                shutil.rmtree(tmp, ignore_errors=True)

            return RedirectResponse(f"/jobs/{job_id}", status_code=302)

        @app.post("/jobs/importance-type")
        async def create_importance_type_job(
            user: CurrentUser,
            db: Session = Depends(get_db),
            ta_json_files: List[UploadFile] = File(...),
            tablepic_json_files: List[UploadFile] = File(...),
            filepic_json_files: List[UploadFile] = File(...),
            step1_json_files: List[UploadFile] = File(...),
            prompt: str = Form(""),
            provider: str = Form("openrouter"),
            model: str = Form("z-ai/glm-5"),
            delay_seconds: str = Form("5"),
            job_name: str = Form(""),
        ) -> RedirectResponse:
            name_stripped = (job_name or "").strip()
            if not name_stripped:
                raise HTTPException(400, "Job name is required (a short label to find this job in the list).")
            if len(name_stripped) > 200:
                raise HTTPException(400, "Job name must be at most 200 characters.")

            tmp = tempfile.mkdtemp(prefix="imptype_upload_")
            try:

                async def _collect_uploads(files: List[UploadFile], subdir: str) -> List[str]:
                    paths: List[str] = []
                    for uf in files:
                        if not uf.filename:
                            continue
                        dest = os.path.join(tmp, subdir, os.path.basename(uf.filename))
                        os.makedirs(os.path.dirname(dest), exist_ok=True)
                        content = await uf.read()
                        with open(dest, "wb") as f:
                            f.write(content)
                        paths.append(dest)
                    return paths

                ta_paths = await _collect_uploads(ta_json_files, "ta")
                tp_paths = await _collect_uploads(tablepic_json_files, "tp")
                fp_paths = await _collect_uploads(filepic_json_files, "fp")
                s1_paths = await _collect_uploads(step1_json_files, "s1")

                ta_sorted = _sorted_nonempty_paths(ta_paths)
                tp_sorted = _sorted_nonempty_paths(tp_paths)
                fp_sorted = _sorted_nonempty_paths(fp_paths)
                s1_sorted = _sorted_nonempty_paths(s1_paths)
                n_ta, n_tp, n_fp, n_s1 = len(ta_sorted), len(tp_sorted), len(fp_sorted), len(s1_sorted)
                if not ta_sorted or not tp_sorted or not fp_sorted or not s1_sorted:
                    raise HTTPException(
                        400,
                        "Upload at least one file in each group: TA merged JSON, tablepic, filepic, Step 1 combined.",
                    )
                if n_ta != n_tp or n_ta != n_fp or n_ta != n_s1:
                    raise HTTPException(
                        400,
                        f"File counts must match: TA ({n_ta}), tablepic ({n_tp}), filepic ({n_fp}), Step 1 ({n_s1}). "
                        "Sort order is by filename within each group.",
                    )

                try:
                    delay_val = float(delay_seconds)
                except ValueError:
                    delay_val = 5.0

                prompt_eff = _prompt_for_new_job(db, "importance_type", prompt, "prompt")
                pair_media: dict = {}
                job_id = str(uuid.uuid4())
                root = job_root(job_id)
                os.makedirs(root, exist_ok=True)

                for pair_index, (ta_p, tp_p, fp_p, s1_p) in enumerate(
                    zip(ta_sorted, tp_sorted, fp_sorted, s1_sorted)
                ):
                    ensure_dirs(job_id, pair_index)
                    tan = os.path.basename(ta_p)
                    tpn = os.path.basename(tp_p)
                    fpn = os.path.basename(fp_p)
                    s1n = os.path.basename(s1_p)
                    rel_ta = f"pair_{pair_index}/inputs/{tan}"
                    rel_tp = f"pair_{pair_index}/inputs/{tpn}"
                    rel_fp = f"pair_{pair_index}/inputs/{fpn}"
                    rel_s1 = f"pair_{pair_index}/inputs/{s1n}"
                    shutil.copy2(ta_p, os.path.join(root, rel_ta.replace("/", os.sep)))
                    shutil.copy2(tp_p, os.path.join(root, rel_tp.replace("/", os.sep)))
                    shutil.copy2(fp_p, os.path.join(root, rel_fp.replace("/", os.sep)))
                    shutil.copy2(s1_p, os.path.join(root, rel_s1.replace("/", os.sep)))
                    pair_media[str(pair_index)] = {
                        "tablepic_relpath": rel_tp,
                        "filepic_relpath": rel_fp,
                        "tablepic_basename": tpn,
                        "filepic_basename": fpn,
                    }

                cfg = {
                    "display_name": name_stripped,
                    "prompt": prompt_eff,
                    "provider": provider,
                    "model": model,
                    "delay_seconds": delay_val,
                    "pair_media": pair_media,
                }
                job = Job(
                    id=job_id,
                    type="importance_type",
                    status="draft",
                    created_by_id=user.id,
                    config_json=json.dumps(cfg, ensure_ascii=False),
                )
                db.add(job)
                db.flush()

                for pair_index, (ta_p, tp_p, fp_p, s1_p) in enumerate(
                    zip(ta_sorted, tp_sorted, fp_sorted, s1_sorted)
                ):
                    tan = os.path.basename(ta_p)
                    tpn = os.path.basename(tp_p)
                    fpn = os.path.basename(fp_p)
                    s1n = os.path.basename(s1_p)
                    rel_ta = f"pair_{pair_index}/inputs/{tan}"
                    rel_tp = f"pair_{pair_index}/inputs/{tpn}"
                    rel_fp = f"pair_{pair_index}/inputs/{fpn}"
                    rel_s1 = f"pair_{pair_index}/inputs/{s1n}"
                    db.add(
                        JobPair(
                            job_id=job_id,
                            pair_index=pair_index,
                            stage_j_filename=tan,
                            word_filename=s1n,
                            stage_j_relpath=rel_ta,
                            word_relpath=rel_s1,
                            output_relpath=f"pair_{pair_index}/output",
                        )
                    )
                    register_input_artifact(db, job_id, pair_index, root, rel_ta, "upload_ta_merged_json")
                    register_input_artifact(db, job_id, pair_index, root, rel_tp, "upload_tablepic_json")
                    register_input_artifact(db, job_id, pair_index, root, rel_fp, "upload_filepic_json")
                    register_input_artifact(db, job_id, pair_index, root, rel_s1, "upload_step1_combined_json")

                db.commit()
            finally:
                shutil.rmtree(tmp, ignore_errors=True)

            return RedirectResponse(f"/jobs/{job_id}", status_code=302)

        @app.post("/jobs/flashcard")
        async def create_flashcard_job(
            user: CurrentUser,
            db: Session = Depends(get_db),
            tagged_json_files: List[UploadFile] = File(...),
            catalog_json_files: List[UploadFile] = File(...),
            prompt: str = Form(""),
            provider: str = Form("openrouter"),
            model: str = Form("z-ai/glm-5"),
            delay_seconds: str = Form("5"),
            job_name: str = Form(""),
        ) -> RedirectResponse:
            name_stripped = (job_name or "").strip()
            if not name_stripped:
                raise HTTPException(400, "Job name is required (a short label to find this job in the list).")
            if len(name_stripped) > 200:
                raise HTTPException(400, "Job name must be at most 200 characters.")

            tmp = tempfile.mkdtemp(prefix="flashcard_upload_")
            try:

                async def _collect(files: List[UploadFile], subdir: str) -> List[str]:
                    paths: List[str] = []
                    for uf in files:
                        if not uf.filename:
                            continue
                        dest = os.path.join(tmp, subdir, os.path.basename(uf.filename))
                        os.makedirs(os.path.dirname(dest), exist_ok=True)
                        content = await uf.read()
                        with open(dest, "wb") as f:
                            f.write(content)
                        paths.append(dest)
                    return paths

                tagged_paths = await _collect(tagged_json_files, "tagged")
                catalog_paths = await _collect(catalog_json_files, "catalog")
                if not tagged_paths:
                    raise HTTPException(400, "Upload at least one tagged JSON (a*.json from Importance & Type).")
                if not catalog_paths:
                    raise HTTPException(400, "Upload at least one image catalog JSON (f*.json from Image File Catalog).")

                pairs_spec = auto_pair_flashcard_files(tagged_paths, catalog_paths)
                if not pairs_spec:
                    raise HTTPException(400, "No pairable tagged JSON files (check PointId / filenames).")
                unpaired = [p for p in pairs_spec if not p.get("word_path")]
                if unpaired:
                    raise HTTPException(
                        400,
                        f"{len(unpaired)} tagged file(s) could not be matched to an image catalog JSON.",
                    )

                try:
                    delay_val = float(delay_seconds)
                except ValueError:
                    delay_val = 5.0

                prompt_eff = _prompt_for_new_job(db, "flashcard", prompt, "prompt")
                cfg = {
                    "display_name": name_stripped,
                    "prompt": prompt_eff,
                    "provider": provider,
                    "model": model,
                    "delay_seconds": delay_val,
                }
                job_id = str(uuid.uuid4())
                root = job_root(job_id)
                os.makedirs(root, exist_ok=True)

                job = Job(
                    id=job_id,
                    type="flashcard",
                    status="draft",
                    created_by_id=user.id,
                    config_json=json.dumps(cfg, ensure_ascii=False),
                )
                db.add(job)
                db.flush()

                for pair_index, p in enumerate(pairs_spec):
                    tagged = p["stage_j_path"]
                    catalog = p["word_path"]
                    ensure_dirs(job_id, pair_index)
                    tagged_name = os.path.basename(tagged)
                    catalog_name = os.path.basename(catalog)
                    rel_tagged = f"pair_{pair_index}/inputs/{tagged_name}"
                    rel_catalog = f"pair_{pair_index}/inputs/{catalog_name}"
                    shutil.copy2(tagged, os.path.join(root, rel_tagged.replace("/", os.sep)))
                    shutil.copy2(catalog, os.path.join(root, rel_catalog.replace("/", os.sep)))
                    db.add(
                        JobPair(
                            job_id=job_id,
                            pair_index=pair_index,
                            stage_j_filename=tagged_name,
                            word_filename=catalog_name,
                            stage_j_relpath=rel_tagged,
                            word_relpath=rel_catalog,
                            output_relpath=f"pair_{pair_index}/output",
                        )
                    )
                    register_input_artifact(db, job_id, pair_index, root, rel_tagged, "upload_tagged_json")
                    register_input_artifact(db, job_id, pair_index, root, rel_catalog, "upload_image_catalog_json")

                db.commit()
            finally:
                shutil.rmtree(tmp, ignore_errors=True)

            return RedirectResponse(f"/jobs/{job_id}", status_code=302)

        @app.post("/jobs/chapter-summary")
        async def create_chapter_summary_job(
            user: CurrentUser,
            db: Session = Depends(get_db),
            tagged_json_files: List[UploadFile] = File(...),
            step1_combined_files: List[UploadFile] = File(...),
            prompt: str = Form(""),
            provider_1: str = Form(DEFAULT_TEST_BANK_PROVIDER),
            model_1: str = Form(DEFAULT_TEST_BANK_MODEL),
            delay_seconds: str = Form("5"),
            job_name: str = Form(""),
        ) -> RedirectResponse:
            name_stripped = (job_name or "").strip()
            if not name_stripped:
                raise HTTPException(400, "Job name is required (a short label to find this job in the list).")
            if len(name_stripped) > 200:
                raise HTTPException(400, "Job name must be at most 200 characters.")

            tmp = tempfile.mkdtemp(prefix="chaptersummary_upload_")
            try:

                async def _collect(files: List[UploadFile], subdir: str) -> List[str]:
                    paths: List[str] = []
                    for uf in files:
                        if not uf.filename:
                            continue
                        dest = os.path.join(tmp, subdir, os.path.basename(uf.filename))
                        os.makedirs(os.path.dirname(dest), exist_ok=True)
                        content = await uf.read()
                        with open(dest, "wb") as f:
                            f.write(content)
                        paths.append(dest)
                    return paths

                tagged_paths = await _collect(tagged_json_files, "tagged")
                step1_paths = await _collect(step1_combined_files, "step1")
                if not tagged_paths:
                    raise HTTPException(400, "Upload at least one tagged JSON (a*.json from Importance & Type).")
                if not step1_paths:
                    raise HTTPException(
                        400,
                        "Upload at least one Test Bank 1 combined JSON (step1_combined_*.json).",
                    )

                pairs_spec = auto_pair_chapter_summary_files(tagged_paths, step1_paths)
                if not pairs_spec:
                    raise HTTPException(400, "No pairable tagged JSON files (check PointId / filenames).")
                unpaired = [p for p in pairs_spec if not p.get("word_path")]
                if unpaired:
                    raise HTTPException(
                        400,
                        f"{len(unpaired)} tagged file(s) could not be matched to a Test Bank 1 combined JSON.",
                    )

                try:
                    delay_val = float(delay_seconds)
                except ValueError:
                    delay_val = 5.0

                prompt_eff = _prompt_for_new_job(db, "chapter_summary", prompt, "prompt")
                model_1_eff = normalize_test_bank_model(model_1)
                if model_1_eff not in TEST_BANK_OPENROUTER_MODEL_CHOICES:
                    raise HTTPException(400, "Invalid OpenRouter model.")
                provider_1_eff = normalize_test_bank_provider(provider_1)
                cfg = {
                    "display_name": name_stripped,
                    "prompt": prompt_eff,
                    "provider_1": provider_1_eff,
                    "model_1": model_1_eff,
                    "delay_seconds": delay_val,
                }
                job_id = str(uuid.uuid4())
                root = job_root(job_id)
                os.makedirs(root, exist_ok=True)

                job = Job(
                    id=job_id,
                    type="chapter_summary",
                    status="draft",
                    created_by_id=user.id,
                    config_json=json.dumps(cfg, ensure_ascii=False),
                )
                db.add(job)
                db.flush()

                for pair_index, p in enumerate(pairs_spec):
                    tagged = p["stage_j_path"]
                    step1 = p["word_path"]
                    ensure_dirs(job_id, pair_index)
                    tagged_name = os.path.basename(tagged)
                    s1_name = os.path.basename(step1)
                    rel_tagged = f"pair_{pair_index}/inputs/{tagged_name}"
                    rel_s1 = f"pair_{pair_index}/inputs/{s1_name}"
                    shutil.copy2(tagged, os.path.join(root, rel_tagged.replace("/", os.sep)))
                    shutil.copy2(step1, os.path.join(root, rel_s1.replace("/", os.sep)))
                    db.add(
                        JobPair(
                            job_id=job_id,
                            pair_index=pair_index,
                            stage_j_filename=tagged_name,
                            word_filename=s1_name,
                            stage_j_relpath=rel_tagged,
                            word_relpath=rel_s1,
                            output_relpath=f"pair_{pair_index}/output",
                        )
                    )
                    register_input_artifact(db, job_id, pair_index, root, rel_tagged, "upload_tagged_json")
                    register_input_artifact(db, job_id, pair_index, root, rel_s1, "upload_step1_combined_json")

                db.commit()
            finally:
                shutil.rmtree(tmp, ignore_errors=True)

            return RedirectResponse(f"/jobs/{job_id}", status_code=302)

        @app.post("/jobs/voice-class")
        async def create_voice_class_job(
            user: CurrentUser,
            db: Session = Depends(get_db),
            tagged_json_files: List[UploadFile] = File(...),
            prompt_1: str = Form(""),
            tts_instruction: str = Form(""),
            provider_1: str = Form(DEFAULT_TEST_BANK_PROVIDER),
            model_1: str = Form(DEFAULT_TEST_BANK_MODEL),
            tts_model: str = Form(DEFAULT_VOICE_CLASS_TTS_MODEL),
            tts_voice: str = Form(DEFAULT_VOICE_CLASS_TTS_VOICE),
            max_segment_seconds: str = Form("60"),
            chars_per_second: str = Form("13"),
            delay_seconds: str = Form("5"),
            job_name: str = Form(""),
        ) -> RedirectResponse:
            name_stripped = (job_name or "").strip()
            if not name_stripped:
                raise HTTPException(400, "Job name is required (a short label to find this job in the list).")
            if len(name_stripped) > 200:
                raise HTTPException(400, "Job name must be at most 200 characters.")

            tmp = tempfile.mkdtemp(prefix="voiceclass_upload_")
            try:
                j_paths: List[str] = []
                for uf in tagged_json_files:
                    if not uf.filename:
                        continue
                    dest = os.path.join(tmp, os.path.basename(uf.filename))
                    content = await uf.read()
                    with open(dest, "wb") as f:
                        f.write(content)
                    j_paths.append(dest)

                if not j_paths:
                    raise HTTPException(400, "Upload at least one tagged JSON (a*.json from Importance & Type).")

                pairs_spec = auto_pair_voice_class_files(j_paths)
                if not pairs_spec:
                    raise HTTPException(400, "No pairable tagged JSON files (check PointId / filenames).")

                try:
                    delay_val = float(delay_seconds)
                except ValueError:
                    delay_val = 5.0
                try:
                    max_seg_val = float(max_segment_seconds)
                except ValueError:
                    max_seg_val = DEFAULT_VOICE_CLASS_MAX_SEGMENT_SECONDS
                try:
                    cps_val = float(chars_per_second)
                except ValueError:
                    cps_val = DEFAULT_VOICE_CLASS_CHARS_PER_SECOND

                prompt_1_eff = _prompt_for_new_job(db, "voice_class", prompt_1, "prompt_1")
                tts_instruction_eff = _prompt_for_new_job(db, "voice_class", tts_instruction, "tts_instruction")
                model_1_eff = normalize_test_bank_model(model_1)
                if model_1_eff not in TEST_BANK_OPENROUTER_MODEL_CHOICES:
                    raise HTTPException(400, "Invalid OpenRouter model.")
                provider_1_eff = normalize_test_bank_provider(provider_1)

                cfg = {
                    "display_name": name_stripped,
                    "prompt_1": prompt_1_eff,
                    "tts_instruction": tts_instruction_eff,
                    "provider_1": provider_1_eff,
                    "model_1": model_1_eff,
                    "tts_model": (tts_model or DEFAULT_VOICE_CLASS_TTS_MODEL).strip(),
                    "tts_voice": (tts_voice or DEFAULT_VOICE_CLASS_TTS_VOICE).strip(),
                    "max_segment_seconds": max_seg_val,
                    "chars_per_second": cps_val,
                    "delay_seconds": delay_val,
                }
                job_id = str(uuid.uuid4())
                root = job_root(job_id)
                os.makedirs(root, exist_ok=True)

                job = Job(
                    id=job_id,
                    type="voice_class",
                    status="draft",
                    created_by_id=user.id,
                    config_json=json.dumps(cfg, ensure_ascii=False),
                )
                db.add(job)
                db.flush()

                for pair_index, p in enumerate(pairs_spec):
                    tagged = p["stage_j_path"]
                    ensure_dirs(job_id, pair_index)
                    tagged_name = os.path.basename(tagged)
                    rel_tagged = f"pair_{pair_index}/inputs/{tagged_name}"
                    shutil.copy2(tagged, os.path.join(root, rel_tagged.replace("/", os.sep)))
                    db.add(
                        JobPair(
                            job_id=job_id,
                            pair_index=pair_index,
                            stage_j_filename=tagged_name,
                            word_filename=None,
                            stage_j_relpath=rel_tagged,
                            word_relpath=None,
                            output_relpath=f"pair_{pair_index}/output",
                        )
                    )
                    register_input_artifact(db, job_id, pair_index, root, rel_tagged, "upload_tagged_json")

                db.commit()
            finally:
                shutil.rmtree(tmp, ignore_errors=True)

            return RedirectResponse(f"/jobs/{job_id}", status_code=302)

        @app.post("/jobs/image-file-catalog")
        async def create_image_file_catalog_job(
            user: CurrentUser,
            db: Session = Depends(get_db),
            stage_ta_json_files: List[UploadFile] = File(...),
            filepic_json_files: List[UploadFile] = File(...),
            tablepic_json_files: List[UploadFile] = File(...),
            delay_seconds: str = Form("5"),
            job_name: str = Form(""),
        ) -> RedirectResponse:
            # Stage F builds f_*.json from Table Notes (TA merged: images + tables) plus filepic/tablepic captions.
            name_stripped = (job_name or "").strip()
            if not name_stripped:
                raise HTTPException(400, "Job name is required (a short label to find this job in the list).")
            if len(name_stripped) > 200:
                raise HTTPException(400, "Job name must be at most 200 characters.")

            tmp = tempfile.mkdtemp(prefix="imgfcat_upload_")
            try:
                ta_paths: List[str] = []
                for uf in stage_ta_json_files:
                    if not uf.filename:
                        continue
                    dest = os.path.join(tmp, "ta", os.path.basename(uf.filename))
                    os.makedirs(os.path.dirname(dest), exist_ok=True)
                    content = await uf.read()
                    with open(dest, "wb") as f:
                        f.write(content)
                    ta_paths.append(dest)

                fp_paths: List[str] = []
                for uf in filepic_json_files:
                    if not uf.filename:
                        continue
                    dest = os.path.join(tmp, "fp", os.path.basename(uf.filename))
                    os.makedirs(os.path.dirname(dest), exist_ok=True)
                    content = await uf.read()
                    with open(dest, "wb") as f:
                        f.write(content)
                    fp_paths.append(dest)

                tp_paths: List[str] = []
                for uf in tablepic_json_files:
                    if not uf.filename:
                        continue
                    dest = os.path.join(tmp, "tp", os.path.basename(uf.filename))
                    os.makedirs(os.path.dirname(dest), exist_ok=True)
                    content = await uf.read()
                    with open(dest, "wb") as f:
                        f.write(content)
                    tp_paths.append(dest)

                ta_sorted = _sorted_nonempty_paths(ta_paths)
                fp_sorted = _sorted_nonempty_paths(fp_paths)
                tp_sorted = _sorted_nonempty_paths(tp_paths)
                if not ta_sorted:
                    raise HTTPException(400, "Upload at least one Table notes (Stage TA) JSON.")
                if not fp_sorted or not tp_sorted:
                    raise HTTPException(400, "Upload matching filepic and tablepic JSON files for each chapter.")
                n_ta, n_fp, n_tp = len(ta_sorted), len(fp_sorted), len(tp_sorted)
                if n_ta != n_fp or n_ta != n_tp:
                    raise HTTPException(
                        400,
                        f"File counts must match: Table notes ({n_ta}), filepic ({n_fp}), tablepic ({n_tp}). "
                        "Sort order is by filename within each group.",
                    )

                try:
                    delay_val = float(delay_seconds)
                except ValueError:
                    delay_val = 5.0

                pair_media: dict = {}
                job_id = str(uuid.uuid4())
                root = job_root(job_id)
                os.makedirs(root, exist_ok=True)

                for pair_index, (ta_p, fp_p, tp_p) in enumerate(zip(ta_sorted, fp_sorted, tp_sorted)):
                    ensure_dirs(job_id, pair_index)
                    tan = os.path.basename(ta_p)
                    fpn = os.path.basename(fp_p)
                    tpn = os.path.basename(tp_p)
                    rel_ta = f"pair_{pair_index}/inputs/{tan}"
                    rel_fp = f"pair_{pair_index}/inputs/{fpn}"
                    rel_tp = f"pair_{pair_index}/inputs/{tpn}"
                    shutil.copy2(ta_p, os.path.join(root, rel_ta.replace("/", os.sep)))
                    shutil.copy2(fp_p, os.path.join(root, rel_fp.replace("/", os.sep)))
                    shutil.copy2(tp_p, os.path.join(root, rel_tp.replace("/", os.sep)))
                    pair_media[str(pair_index)] = {
                        "filepic_relpath": rel_fp,
                        "tablepic_relpath": rel_tp,
                        "filepic_basename": fpn,
                        "tablepic_basename": tpn,
                    }

                cfg = {
                    "display_name": name_stripped,
                    "delay_seconds": delay_val,
                    "pair_media": pair_media,
                }

                job = Job(
                    id=job_id,
                    type="image_file_catalog",
                    status="draft",
                    created_by_id=user.id,
                    config_json=json.dumps(cfg, ensure_ascii=False),
                )
                db.add(job)
                db.flush()

                for pair_index, (ta_p, fp_p, tp_p) in enumerate(zip(ta_sorted, fp_sorted, tp_sorted)):
                    tan = os.path.basename(ta_p)
                    rel_ta = f"pair_{pair_index}/inputs/{tan}"
                    db.add(
                        JobPair(
                            job_id=job_id,
                            pair_index=pair_index,
                            stage_j_filename=tan,
                            word_filename="",
                            stage_j_relpath=rel_ta,
                            word_relpath=None,
                            output_relpath=f"pair_{pair_index}/output",
                        )
                    )
                    register_input_artifact(db, job_id, pair_index, root, rel_ta, "upload_stage_ta_json")
                    register_input_artifact(
                        db, job_id, pair_index, root, pair_media[str(pair_index)]["filepic_relpath"], "upload_filepic_json"
                    )
                    register_input_artifact(
                        db, job_id, pair_index, root, pair_media[str(pair_index)]["tablepic_relpath"], "upload_tablepic_json"
                    )

                db.commit()
            finally:
                shutil.rmtree(tmp, ignore_errors=True)

            return RedirectResponse(f"/jobs/{job_id}", status_code=302)

        async def _create_json_to_csv_stage_job(
            user: CurrentUser,
            db: Session,
            job_type: str,
            json_files: List[UploadFile],
            job_name: str,
            delimiter: str = ",",
        ) -> RedirectResponse:
            from webapp.json_to_csv_jobs import JSON_TO_CSV_JOB_LABELS

            name_stripped = (job_name or "").strip()
            if not name_stripped:
                raise HTTPException(400, "Job name is required (a short label to find this job in the list).")
            if len(name_stripped) > 200:
                raise HTTPException(400, "Job name must be at most 200 characters.")

            delim = (delimiter or ",").strip() or ","
            jt = (job_type or "").strip()
            if jt not in JSON_TO_CSV_JOB_LABELS:
                raise HTTPException(400, f"Unknown JSON to CSV job type: {jt}")

            tmp = tempfile.mkdtemp(prefix="jsontocsv_upload_")
            try:
                json_paths: List[str] = []
                for uf in json_files:
                    if not uf.filename:
                        continue
                    dest = os.path.join(tmp, os.path.basename(uf.filename))
                    content = await uf.read()
                    with open(dest, "wb") as f:
                        f.write(content)
                    json_paths.append(dest)

                sorted_json = _sorted_nonempty_paths(json_paths)
                if not sorted_json:
                    raise HTTPException(400, "Upload at least one JSON file.")

                cfg = {
                    "display_name": name_stripped,
                    "delimiter": delim,
                }
                job_id = str(uuid.uuid4())
                root = job_root(job_id)
                os.makedirs(root, exist_ok=True)

                job = Job(
                    id=job_id,
                    type=jt,
                    status="draft",
                    created_by_id=user.id,
                    config_json=json.dumps(cfg, ensure_ascii=False),
                )
                db.add(job)
                db.flush()

                for pair_index, jp in enumerate(sorted_json):
                    ensure_dirs(job_id, pair_index)
                    jn = os.path.basename(jp)
                    rel_j = f"pair_{pair_index}/inputs/{jn}"
                    shutil.copy2(jp, os.path.join(root, rel_j.replace("/", os.sep)))
                    db.add(
                        JobPair(
                            job_id=job_id,
                            pair_index=pair_index,
                            stage_j_filename=jn,
                            word_filename="",
                            stage_j_relpath=rel_j,
                            word_relpath=None,
                            output_relpath=f"pair_{pair_index}/output",
                        )
                    )
                    register_input_artifact(db, job_id, pair_index, root, rel_j, "upload_json")

                db.commit()
            finally:
                shutil.rmtree(tmp, ignore_errors=True)

            return RedirectResponse(f"/jobs/{job_id}", status_code=302)

        @app.post("/jobs/json-to-csv")
        async def create_json_to_csv_job(
            user: CurrentUser,
            db: Session = Depends(get_db),
            conversion_type: str = Form(...),
            json_files: List[UploadFile] = File(...),
            delimiter: str = Form(","),
            job_name: str = Form(""),
        ) -> RedirectResponse:
            from webapp.json_to_csv_jobs import job_type_for_conversion_key

            job_type = job_type_for_conversion_key(conversion_type)
            if not job_type:
                raise HTTPException(400, "Invalid conversion type.")
            return await _create_json_to_csv_stage_job(
                user,
                db,
                job_type,
                json_files,
                job_name,
                delimiter,
            )

        async def _create_json_to_word_stage_job(
            user: CurrentUser,
            db: Session,
            json_files: List[UploadFile],
            job_name: str,
        ) -> RedirectResponse:
            from webapp.json_to_word_jobs import JSON_TO_WORD_JOB_TYPES

            name_stripped = (job_name or "").strip()
            if not name_stripped:
                raise HTTPException(400, "Job name is required (a short label to find this job in the list).")
            if len(name_stripped) > 200:
                raise HTTPException(400, "Job name must be at most 200 characters.")

            jt = "table_notes_json_to_word"
            if jt not in JSON_TO_WORD_JOB_TYPES:
                raise HTTPException(400, f"Unknown JSON to Word job type: {jt}")

            tmp = tempfile.mkdtemp(prefix="jsontoword_upload_")
            try:
                json_paths: List[str] = []
                for uf in json_files:
                    if not uf.filename:
                        continue
                    dest = os.path.join(tmp, os.path.basename(uf.filename))
                    content = await uf.read()
                    with open(dest, "wb") as f:
                        f.write(content)
                    json_paths.append(dest)

                sorted_json = _sorted_nonempty_paths(json_paths)
                if not sorted_json:
                    raise HTTPException(400, "Upload at least one JSON file.")

                cfg = {"display_name": name_stripped}
                job_id = str(uuid.uuid4())
                root = job_root(job_id)
                os.makedirs(root, exist_ok=True)

                job = Job(
                    id=job_id,
                    type=jt,
                    status="draft",
                    created_by_id=user.id,
                    config_json=json.dumps(cfg, ensure_ascii=False),
                )
                db.add(job)
                db.flush()

                for pair_index, jp in enumerate(sorted_json):
                    ensure_dirs(job_id, pair_index)
                    jn = os.path.basename(jp)
                    rel_j = f"pair_{pair_index}/inputs/{jn}"
                    shutil.copy2(jp, os.path.join(root, rel_j.replace("/", os.sep)))
                    db.add(
                        JobPair(
                            job_id=job_id,
                            pair_index=pair_index,
                            stage_j_filename=jn,
                            word_filename="",
                            stage_j_relpath=rel_j,
                            word_relpath=None,
                            output_relpath=f"pair_{pair_index}/output",
                        )
                    )
                    register_input_artifact(db, job_id, pair_index, root, rel_j, "upload_json")

                db.commit()
            finally:
                shutil.rmtree(tmp, ignore_errors=True)

            return RedirectResponse(f"/jobs/{job_id}", status_code=302)

        @app.post("/jobs/table-notes/json-to-word")
        async def create_table_notes_json_to_word_job(
            user: CurrentUser,
            db: Session = Depends(get_db),
            json_files: List[UploadFile] = File(...),
            job_name: str = Form(""),
        ) -> RedirectResponse:
            return await _create_json_to_word_stage_job(user, db, json_files, job_name)

        @app.post("/jobs/document-processing/json-to-word")
        async def create_document_processing_json_to_word_job(
            user: CurrentUser,
            db: Session = Depends(get_db),
            json_files: List[UploadFile] = File(...),
            job_name: str = Form(""),
        ) -> RedirectResponse:
            return await _create_json_to_word_stage_job(user, db, json_files, job_name)

    else:

        @app.post("/jobs/test-bank")
        def create_test_bank_legacy_stub(user: CurrentUser) -> None:
            raise HTTPException(
                410,
                "Test Bank is split into Test Bank 1 and Test Bank 2. Use /test-bank-1/new or /test-bank-2/new.",
            )

        @app.post("/jobs/test-bank-1")
        def create_test_bank_1_job_stub(user: CurrentUser) -> None:
            raise HTTPException(
                503,
                "Install python-multipart (pip install python-multipart) to enable Test Bank 1 file uploads.",
            )

        @app.post("/jobs/test-bank-2")
        def create_test_bank_2_job_stub(user: CurrentUser) -> None:
            raise HTTPException(
                503,
                "Install python-multipart (pip install python-multipart) to enable Test Bank 2 file uploads.",
            )

        @app.post("/jobs/pre-ocr")
        def create_pre_ocr_job_stub(user: CurrentUser) -> None:
            raise HTTPException(503, "Install python-multipart to enable file uploads.")

        @app.post("/jobs/ocr-extraction")
        def create_ocr_extraction_job_stub(user: CurrentUser) -> None:
            raise HTTPException(503, "Install python-multipart to enable file uploads.")

        @app.post("/jobs/document-processing")
        def create_document_processing_job_stub(user: CurrentUser) -> None:
            raise HTTPException(503, "Install python-multipart to enable file uploads.")

        @app.post("/jobs/image-notes")
        def create_image_notes_job_stub(user: CurrentUser) -> None:
            raise HTTPException(503, "Install python-multipart to enable file uploads.")

        @app.post("/jobs/table-notes")
        def create_table_notes_job_stub(user: CurrentUser) -> None:
            raise HTTPException(503, "Install python-multipart to enable file uploads.")

        @app.post("/jobs/importance-type")
        def create_importance_type_job_stub(user: CurrentUser) -> None:
            raise HTTPException(503, "Install python-multipart to enable file uploads.")

        @app.post("/jobs/flashcard")
        def create_flashcard_job_stub(user: CurrentUser) -> None:
            raise HTTPException(503, "Install python-multipart to enable file uploads.")

        @app.post("/jobs/chapter-summary")
        def create_chapter_summary_job_stub(user: CurrentUser) -> None:
            raise HTTPException(503, "Install python-multipart to enable file uploads.")

        @app.post("/jobs/voice-class")
        def create_voice_class_job_stub(user: CurrentUser) -> None:
            raise HTTPException(503, "Install python-multipart to enable file uploads.")

        @app.post("/jobs/image-file-catalog")
        def create_image_file_catalog_job_stub(user: CurrentUser) -> None:
            raise HTTPException(503, "Install python-multipart to enable file uploads.")

        @app.post("/jobs/json-to-csv")
        def create_json_to_csv_job_stub(user: CurrentUser) -> None:
            raise HTTPException(503, "Install python-multipart to enable file uploads.")

        @app.post("/jobs/table-notes/json-to-word")
        def create_table_notes_json_to_word_job_stub(user: CurrentUser) -> None:
            raise HTTPException(503, "Install python-multipart to enable file uploads.")

        @app.post("/jobs/document-processing/json-to-word")
        def create_document_processing_json_to_word_job_stub(user: CurrentUser) -> None:
            raise HTTPException(503, "Install python-multipart to enable file uploads.")

    @app.get("/jobs/{job_id}", response_class=HTMLResponse)
    def job_detail(
        request: Request,
        job_id: str,
        user: CurrentUser,
        db: Session = Depends(get_db),
    ) -> Any:
        job = (
            db.query(Job)
            .options(joinedload(Job.created_by_user))
            .filter(Job.id == job_id)
            .one_or_none()
        )
        if not job:
            raise HTTPException(404)
        pairs = db.query(JobPair).filter(JobPair.job_id == job_id).order_by(JobPair.pair_index).all()
        artifacts = db.query(Artifact).filter(Artifact.job_id == job_id).order_by(Artifact.id).all()
        word_choices = list_word_basenames_for_job(job_id)
        seen = set(word_choices)
        for p in pairs:
            if p.word_filename and p.word_filename not in seen:
                if find_word_file_abs_for_basename(job_id, p.word_filename):
                    word_choices.append(p.word_filename)
                    seen.add(p.word_filename)
        word_choices = sorted(word_choices, key=lambda s: s.lower())
        jt = (job.type or "").strip()
        single_stage = jt in SINGLE_STAGE_JOB_TYPES
        step1_artifacts, step2_artifacts, other_artifacts = split_artifacts_for_steps(artifacts, job.type)
        if single_stage:
            step2_enabled = False
        elif jt == "test_bank_1":
            step2_enabled = False
        elif jt in ("test_bank", "voice_class"):
            step2_enabled = all_pairs_step1_succeeded(pairs)
        else:
            step2_enabled = job.type != "test_bank" or all_pairs_step1_succeeded(pairs)
        show_step2_section = (not single_stage) and (jt != "test_bank_1")
        creator = job.created_by_user
        creator_label = creator.email if creator else "Unknown"
        is_job_owner = user_can_edit_job(job, user)
        if single_stage:
            if jt == "test_bank_2":
                step1_poll_roles_json = json.dumps(
                    [
                        "final_b_json",
                        "step2_topic",
                        "step2_failed_topics",
                        "step2_prompt_input",
                        "output",
                        "llm_prompt_step2",
                    ]
                )
            else:
                step1_poll_roles_json = json.dumps(
                    ["step1_combined", "txt_dump", "output", "llm_prompt_step1"]
                )
            step2_poll_roles_json = "[]"
        else:
            if jt == "voice_class":
                step1_poll_roles_json = json.dumps(["voice_script_json", "llm_prompt_step1"])
                step2_poll_roles_json = json.dumps(["tts_segment", "final_mp3"])
            else:
                step1_poll_roles_json = json.dumps(["step1_combined", "txt_dump", "llm_prompt_step1"])
                step2_poll_roles_json = json.dumps(
                    [
                        "step2_topic",
                        "final_b_json",
                        "step2_failed_topics",
                        "step2_prompt_input",
                        "output",
                        "llm_prompt_step2",
                    ]
                )
        stage_label = job_stage_label(job)
        cfg_dict = json.loads(job.config_json or "{}")
        prompt_editor_rows = build_prompt_editor_rows(db, jt, cfg_dict)
        from webapp.unit_repair.registry import job_supports_unit_repair, renumber_scheme_for_job

        supports_unit_repair = job_supports_unit_repair(jt)
        renumber_scheme = renumber_scheme_for_job(jt)
        return templates.TemplateResponse(
            request,
            "job_detail.html",
            {
                "user": user,
                "job": job,
                "pairs": pairs,
                "artifacts": artifacts,
                "step1_artifacts": step1_artifacts,
                "step2_artifacts": step2_artifacts,
                "other_artifacts": other_artifacts,
                "step2_enabled": step2_enabled,
                "cfg": cfg_dict,
                "word_choices": word_choices,
                "is_job_owner": is_job_owner,
                "creator_label": creator_label,
                "single_stage_job": single_stage,
                "show_step2_section": show_step2_section,
                "step1_poll_roles_json": step1_poll_roles_json,
                "step2_poll_roles_json": step2_poll_roles_json,
                "stage_label": stage_label,
                "prompt_editor_rows": prompt_editor_rows,
                "job_type_has_editable_prompts": job_type_has_editable_prompts(jt),
                "supports_unit_repair": supports_unit_repair,
                "renumber_scheme": renumber_scheme,
                "is_voice_class": jt == "voice_class",
                "is_admin": is_admin_user(user),
            },
        )

    @app.post("/jobs/{job_id}/pairing")
    async def post_job_pairing(
        job_id: str,
        request: Request,
        user: CurrentUser,
        db: Session = Depends(get_db),
    ) -> RedirectResponse:
        job = db.query(Job).filter(Job.id == job_id).one_or_none()
        if not job:
            raise HTTPException(404)
        require_job_owner(job, user)
        form = await request.form()
        root = job_root(job_id)
        pairs = db.query(JobPair).filter(JobPair.job_id == job_id).order_by(JobPair.pair_index).all()
        allowed = set(list_word_basenames_for_job(job_id))

        def _safe_word_basename(name: str) -> str:
            b = os.path.basename(str(name).replace("\\", "/"))
            if not b or b in (".", ".."):
                raise HTTPException(400, "Invalid Word filename")
            low = b.lower()
            if not (low.endswith(".docx") or low.endswith(".doc")):
                raise HTTPException(400, "Word file must be .doc or .docx")
            return b

        for pair in pairs:
            up_key = f"word_upload_{pair.pair_index}"
            sel_key = f"word_pair_{pair.pair_index}"
            upload = form.get(up_key)
            has_upload = _is_nonempty_file_upload(upload)
            raw = form.get(sel_key)
            choice = str(raw).strip() if raw is not None else ""

            if has_upload:
                _safe_word_basename(upload.filename or "")
            elif choice and choice not in allowed:
                raise HTTPException(400, f"Unknown Word file: {choice}")

            in_dir = pair_inputs(job_id, pair.pair_index)
            os.makedirs(in_dir, exist_ok=True)
            if os.path.isdir(in_dir):
                for fn in os.listdir(in_dir):
                    low = fn.lower()
                    if low.endswith(".docx") or low.endswith(".doc"):
                        try:
                            os.remove(os.path.join(in_dir, fn))
                        except OSError:
                            pass

            db.query(Artifact).filter(
                Artifact.job_id == job_id,
                Artifact.pair_index == pair.pair_index,
                Artifact.role == "upload_word",
            ).delete(synchronize_session=False)

            if has_upload:
                fn = _safe_word_basename(upload.filename or "")
                data = await _read_upload_bytes(upload)
                if not data:
                    raise HTTPException(400, f"Empty file: {fn}")
                dest = os.path.join(in_dir, fn)
                with open(dest, "wb") as f:
                    f.write(data)
                rel_w = f"pair_{pair.pair_index}/inputs/{fn}".replace("\\", "/")
                pair.word_filename = fn
                pair.word_relpath = rel_w
                pair.step1_status = "pending"
                pair.step2_status = "pending"
                pair.step1_error = None
                pair.step2_error = None
                register_input_artifact(db, job_id, pair.pair_index, root, rel_w, "upload_word")
                continue

            if not choice:
                pair.word_filename = ""
                pair.word_relpath = None
                pair.step1_status = "pending"
                pair.step2_status = "pending"
                pair.step1_error = None
                pair.step2_error = None
                continue

            src = find_word_file_abs_for_basename(job_id, choice)
            if not src or not os.path.isfile(src):
                raise HTTPException(400, f"Word file not found on disk: {choice}")

            dest = os.path.join(in_dir, os.path.basename(src))
            shutil.copy2(src, dest)
            rel_w = f"pair_{pair.pair_index}/inputs/{os.path.basename(dest)}"
            rel_w = rel_w.replace("\\", "/")
            pair.word_filename = os.path.basename(dest)
            pair.word_relpath = rel_w
            pair.step1_status = "pending"
            pair.step2_status = "pending"
            pair.step1_error = None
            pair.step2_error = None
            register_input_artifact(db, job_id, pair.pair_index, root, rel_w, "upload_word")

        db.commit()
        append_log(db, job_id, "Pairing updated from job page", None)
        return RedirectResponse(f"/jobs/{job_id}", status_code=302)

    @app.post("/jobs/{job_id}/prompts")
    async def post_job_prompts(
        job_id: str,
        request: Request,
        user: CurrentUser,
        db: Session = Depends(get_db),
    ) -> RedirectResponse:
        job = db.query(Job).filter(Job.id == job_id).one_or_none()
        if not job:
            raise HTTPException(404)
        require_job_owner(job, user)
        jt = (job.type or "").strip()
        if not job_type_has_editable_prompts(jt):
            raise HTTPException(400, "This job type has no editable LLM prompts in config.")
        form = await request.form()
        cfg = json.loads(job.config_json or "{}")
        apply_submitted_prompts_to_cfg(jt, cfg, form)
        job.config_json = json.dumps(cfg, ensure_ascii=False)
        db.commit()
        append_log(db, job_id, "Job prompts updated and saved to database (config_json).", None)
        return RedirectResponse(f"/jobs/{job_id}", status_code=302)

    @app.post("/jobs/{job_id}/enqueue-step1")
    def enqueue_step1(
        job_id: str,
        user: CurrentUser,
        db: Session = Depends(get_db),
        pair_indices: Optional[str] = Query(None),
    ) -> dict:
        job = db.query(Job).filter(Job.id == job_id).one_or_none()
        if not job:
            raise HTTPException(404)
        require_job_owner(job, user)
        job.status = "queued"
        job.cancel_requested = False
        append_log(db, job_id, "Queued Step 1." + queued_task_log_suffix(), None)
        db.commit()
        try:
            enqueue_task("step1", job_id, parse_pair_indices(pair_indices))
        except Exception as e:
            job.status = "failed"
            job.error_summary = str(e)
            db.commit()
            raise HTTPException(503, f"Queue unavailable: {e}") from e
        return {"ok": True, "job_id": job_id}

    @app.post("/jobs/{job_id}/enqueue-step2")
    def enqueue_step2(
        job_id: str,
        user: CurrentUser,
        db: Session = Depends(get_db),
        pair_indices: Optional[str] = Query(None),
    ) -> dict:
        job = db.query(Job).filter(Job.id == job_id).one_or_none()
        if not job:
            raise HTTPException(404)
        require_job_owner(job, user)
        if (job.type or "").strip() in SINGLE_STAGE_JOB_TYPES:
            raise HTTPException(400, "Step 2 is not used for this job type.")
        pairs_all = (
            db.query(JobPair)
            .filter(JobPair.job_id == job_id)
            .order_by(JobPair.pair_index)
            .all()
        )
        idx_filter = parse_pair_indices(pair_indices)
        scope = (
            pairs_all
            if idx_filter is None
            else [p for p in pairs_all if p.pair_index in set(idx_filter)]
        )
        if not scope:
            raise HTTPException(400, "No pairs match pair_indices filter.")
        if job.type == "test_bank_1":
            raise HTTPException(
                400,
                "Test Bank 1 has no Step 2 here. Create a Test Bank 2 job and upload your "
                "Test Bank 1 combined JSON (step1_combined_*.json) plus the same Stage J and Word pairing.",
            )
        if job.type == "test_bank":
            not_ready = [p.pair_index for p in scope if p.step1_status != "succeeded"]
            if not_ready:
                raise HTTPException(
                    400,
                    "Step 1 must succeed for all selected pairs before Step 2. "
                    f"Not ready (pair index): {sorted(not_ready)}",
                )
        job.status = "queued"
        job.cancel_requested = False
        append_log(db, job_id, "Queued Step 2." + queued_task_log_suffix(), None)
        db.commit()
        try:
            enqueue_task("step2", job_id, parse_pair_indices(pair_indices))
        except Exception as e:
            job.status = "failed"
            job.error_summary = str(e)
            db.commit()
            raise HTTPException(503, f"Queue unavailable: {e}") from e
        return {"ok": True, "job_id": job_id}

    @app.post("/jobs/{job_id}/enqueue-full")
    def enqueue_full(
        job_id: str,
        user: CurrentUser,
        db: Session = Depends(get_db),
        pair_indices: Optional[str] = Query(None),
    ) -> dict:
        job = db.query(Job).filter(Job.id == job_id).one_or_none()
        if not job:
            raise HTTPException(404)
        require_job_owner(job, user)
        job.status = "queued"
        job.cancel_requested = False
        append_log(
            db,
            job_id,
            "Queued full pipeline (Step 1 + Step 2)." + queued_task_log_suffix(),
            None,
        )
        db.commit()
        try:
            enqueue_task("full", job_id, parse_pair_indices(pair_indices))
        except Exception as e:
            job.status = "failed"
            job.error_summary = str(e)
            db.commit()
            raise HTTPException(503, f"Queue unavailable: {e}") from e
        return {"ok": True, "job_id": job_id}

    @app.post("/jobs/{job_id}/cancel")
    def cancel_job_run(
        job_id: str,
        user: CurrentUser,
        db: Session = Depends(get_db),
    ) -> dict:
        job = db.query(Job).filter(Job.id == job_id).one_or_none()
        if not job:
            raise HTTPException(404)
        require_job_owner(job, user)
        if job.status not in ("queued", "running"):
            raise HTTPException(
                400,
                "Nothing to stop: job is not queued or running.",
            )
        job.cancel_requested = True
        append_log(
            db,
            job_id,
            "Stop requested — OpenRouter runs use streaming when possible, so stop can take effect "
            "during generation; otherwise the worker exits after the current step or between pairs.",
            None,
        )
        db.commit()
        return {"ok": True, "job_id": job_id}

    @app.post("/jobs/{job_id}/delete")
    def delete_job_endpoint(
        job_id: str,
        user: CurrentUser,
        db: Session = Depends(get_db),
    ) -> dict:
        job = (
            db.query(Job)
            .options(joinedload(Job.pairs))
            .filter(Job.id == job_id)
            .one_or_none()
        )
        if not job:
            raise HTTPException(404)
        require_job_owner(job, user)
        pairs = list(job.pairs)
        if job_active_for_delete(job, pairs):
            raise HTTPException(
                409,
                "Job is still queued or running. Stop it and wait until it finishes stopping, then try again.",
            )
        delete_job_completely(db, job)
        return {"ok": True, "job_id": job_id}

    @app.get("/jobs/{job_id}/pairs/{pair_index}/units")
    def get_job_pair_units(
        job_id: str,
        pair_index: int,
        user: CurrentUser,
        db: Session = Depends(get_db),
    ) -> dict:
        job = db.query(Job).filter(Job.id == job_id).one_or_none()
        if not job:
            raise HTTPException(404)
        require_job_owner(job, user)
        jt = (job.type or "").strip()
        from webapp.unit_repair.registry import job_supports_unit_repair
        from webapp.unit_repair.service import get_units_payload
        from webapp.unit_repair.tasks import pair_repair_busy

        if not job_supports_unit_repair(jt):
            return {"supported": False, "units": [], "renumber": {}}
        try:
            payload = get_units_payload(job_id, pair_index, jt, db=db)
        except Exception:
            payload = {
                "units": [],
                "renumber": {"scheme": None, "ids_provisional": False},
                "supports_renumber": False,
            }
        for u in payload.get("units") or []:
            ui = u.get("unit_index")
            if ui is not None:
                u["preview_url"] = f"/jobs/{job_id}/pairs/{pair_index}/units/{ui}/preview"
            rel = (u.get("artifact_relpath") or "").strip()
            if rel:
                art = (
                    db.query(Artifact)
                    .filter(Artifact.job_id == job_id, Artifact.rel_path == rel)
                    .one_or_none()
                )
                if art:
                    u["artifact_id"] = art.id
        payload["supported"] = True
        payload["pair_repair_busy"] = pair_repair_busy(job_id, pair_index)
        payload["job_status"] = job.status
        return payload

    @app.get("/jobs/{job_id}/pairs/{pair_index}/units/{unit_index}/preview")
    def get_job_pair_unit_preview(
        job_id: str,
        pair_index: int,
        unit_index: int,
        user: CurrentUser,
        db: Session = Depends(get_db),
    ) -> dict:
        job = db.query(Job).filter(Job.id == job_id).one_or_none()
        if not job:
            raise HTTPException(404)
        require_job_owner(job, user)
        jt = (job.type or "").strip()
        from webapp.unit_repair.registry import job_supports_unit_repair
        from webapp.unit_repair.preview import get_unit_preview_payload

        if not job_supports_unit_repair(jt):
            raise HTTPException(400, "This job type does not support unit preview")
        try:
            payload = get_unit_preview_payload(job_id, pair_index, unit_index, jt)
        except ValueError as e:
            raise HTTPException(404, str(e)) from e
        except FileNotFoundError as e:
            raise HTTPException(404, str(e)) from e
        for sec in payload.get("sections") or []:
            rel = (sec.get("rel_path") or "").strip()
            if not rel:
                continue
            art = (
                db.query(Artifact)
                .filter(Artifact.job_id == job_id, Artifact.rel_path == rel)
                .one_or_none()
            )
            if art:
                sec["artifact_id"] = art.id
        return payload

    @app.get("/jobs/{job_id}/pairs/{pair_index}/voice-segments")
    def get_voice_segments(
        job_id: str,
        pair_index: int,
        user: CurrentUser,
        db: Session = Depends(get_db),
    ) -> dict:
        job = db.query(Job).filter(Job.id == job_id).one_or_none()
        if not job:
            raise HTTPException(404)
        require_job_owner(job, user)
        if (job.type or "").strip() != "voice_class":
            return {"supported": False, "segments": []}

        from webapp.tasks_voice_class import _find_voice_script

        base = job_root(job_id)
        script_path = _find_voice_script(base, pair_index)
        if not script_path:
            return {"supported": True, "segments": [], "script_missing": True}

        with open(script_path, encoding="utf-8") as f:
            data = json.load(f)
        segments = data.get("segments") or []
        meta = data.get("metadata") or {}
        out = []
        for seg in segments:
            sid = int(seg.get("segment_id") or 0)
            rel_wav = f"pair_{pair_index}/output/tts_segments/segment_{sid:03d}.wav"
            abs_wav = os.path.join(base, rel_wav.replace("/", os.sep))
            art = (
                db.query(Artifact)
                .filter(Artifact.job_id == job_id, Artifact.rel_path == rel_wav)
                .one_or_none()
            )
            out.append(
                {
                    "segment_id": sid,
                    "paragraph_count": seg.get("paragraph_count"),
                    "estimated_seconds": seg.get("estimated_seconds"),
                    "char_count": seg.get("char_count"),
                    "has_wav": os.path.isfile(abs_wav),
                    "artifact_id": art.id if art else None,
                    "rel_path": rel_wav if os.path.isfile(abs_wav) else None,
                }
            )
        return {
            "supported": True,
            "segments": out,
            "metadata": meta,
        }

    @app.post("/jobs/{job_id}/pairs/{pair_index}/voice-segments/{segment_id}/regenerate")
    def post_regenerate_voice_segment(
        job_id: str,
        pair_index: int,
        segment_id: int,
        user: CurrentUser,
        db: Session = Depends(get_db),
    ) -> dict:
        job = db.query(Job).filter(Job.id == job_id).one_or_none()
        if not job:
            raise HTTPException(404)
        require_job_owner(job, user)
        if (job.type or "").strip() != "voice_class":
            raise HTTPException(400, "Not a voice class job")
        if job.status in ("running", "queued"):
            raise HTTPException(409, "Job is still running")

        job.status = "queued"
        job.cancel_requested = False
        db.commit()

        def _run() -> None:
            from webapp.tasks_voice_class import run_voice_class_regenerate_segment

            run_voice_class_regenerate_segment(job_id, pair_index, segment_id)

        try:
            if tasks_use_celery_queue():
                import threading

                threading.Thread(target=_run, daemon=True).start()
            else:
                import threading

                threading.Thread(target=_run, daemon=True).start()
        except Exception as e:
            raise HTTPException(503, str(e)) from e
        return {"ok": True, "job_id": job_id, "segment_id": segment_id}

    @app.post("/jobs/{job_id}/pairs/{pair_index}/merge-voice")
    def post_merge_voice(
        job_id: str,
        pair_index: int,
        user: CurrentUser,
        db: Session = Depends(get_db),
    ) -> dict:
        job = db.query(Job).filter(Job.id == job_id).one_or_none()
        if not job:
            raise HTTPException(404)
        require_job_owner(job, user)
        if (job.type or "").strip() != "voice_class":
            raise HTTPException(400, "Not a voice class job")
        if job.status in ("running", "queued"):
            raise HTTPException(409, "Job is still running")

        from webapp.tasks_voice_class import run_voice_class_merge_only

        run_voice_class_merge_only(job_id, pair_index)
        return {"ok": True, "job_id": job_id, "pair_index": pair_index}

    @app.post("/jobs/{job_id}/pairs/{pair_index}/units/{unit_index}/regenerate")
    def post_regenerate_unit(
        job_id: str,
        pair_index: int,
        unit_index: int,
        user: CurrentUser,
        db: Session = Depends(get_db),
    ) -> dict:
        job = db.query(Job).filter(Job.id == job_id).one_or_none()
        if not job:
            raise HTTPException(404)
        require_job_owner(job, user)
        if job.status == "running":
            raise HTTPException(409, "Job is still running")
        from webapp.unit_repair.registry import job_supports_unit_repair
        from webapp.unit_repair.tasks import pair_repair_busy

        if not job_supports_unit_repair((job.type or "").strip()):
            raise HTTPException(400, "This job type does not support unit regenerate")
        if pair_repair_busy(job_id, pair_index):
            raise HTTPException(409, "Unit repair already in progress for this pair")
        try:
            enqueue_unit_repair("regenerate", job_id, pair_index, unit_index)
        except Exception as e:
            raise HTTPException(503, str(e)) from e
        append_log(db, job_id, f"Queued regenerate for unit {unit_index} (pair {pair_index}).", pair_index)
        return {"ok": True, "job_id": job_id, "pair_index": pair_index, "unit_index": unit_index}

    @app.post("/jobs/{job_id}/pairs/{pair_index}/renumber")
    def post_renumber_pair(
        job_id: str,
        pair_index: int,
        payload: RenumberBody,
        user: CurrentUser,
        db: Session = Depends(get_db),
    ) -> dict:
        job = db.query(Job).filter(Job.id == job_id).one_or_none()
        if not job:
            raise HTTPException(404)
        require_job_owner(job, user)
        if not payload.confirm:
            raise HTTPException(400, "confirm must be true")
        if job.status == "running":
            raise HTTPException(409, "Job is still running")
        from webapp.unit_repair.registry import supports_renumber
        from webapp.unit_repair.tasks import pair_repair_busy

        jt = (job.type or "").strip()
        if not supports_renumber(jt):
            raise HTTPException(400, "This job type does not support renumber")
        if pair_repair_busy(job_id, pair_index):
            raise HTTPException(409, "Unit repair already in progress for this pair")
        try:
            enqueue_unit_repair("renumber", job_id, pair_index)
        except Exception as e:
            raise HTTPException(503, str(e)) from e
        append_log(db, job_id, f"Queued renumber for pair {pair_index}.", pair_index)
        return {"ok": True, "job_id": job_id, "pair_index": pair_index}

    @app.get("/jobs/{job_id}/poll")
    def poll_job(
        job_id: str,
        user: CurrentUser,
        db: Session = Depends(get_db),
        after_seq: int = 0,
        limit: int = 500,
    ) -> dict:
        job = db.query(Job).filter(Job.id == job_id).one_or_none()
        if not job:
            raise HTTPException(404)

        lines = (
            db.query(JobLogLine)
            .filter(JobLogLine.job_id == job_id, JobLogLine.seq > after_seq)
            .order_by(JobLogLine.seq)
            .limit(limit)
            .all()
        )

        pairs = db.query(JobPair).filter(JobPair.job_id == job_id).order_by(JobPair.pair_index).all()
        arts = db.query(Artifact).filter(Artifact.job_id == job_id).order_by(Artifact.id).all()

        max_seq = db.query(func.max(JobLogLine.seq)).filter(JobLogLine.job_id == job_id).scalar() or 0

        return {
            "job_id": job_id,
            "job_type": job.type,
            "status": job.status,
            "cancel_requested": bool(getattr(job, "cancel_requested", False)),
            "error_summary": job.error_summary,
            "log_lines": [{"seq": ln.seq, "line": ln.line, "pair_index": ln.pair_index} for ln in lines],
            "max_seq": max_seq,
            "pairs": [
                {
                    "pair_index": p.pair_index,
                    "step1_status": p.step1_status,
                    "step2_status": p.step2_status,
                    "step1_error": p.step1_error,
                    "step2_error": p.step2_error,
                    "stage_j": p.stage_j_filename,
                    "word": p.word_filename or "",
                }
                for p in pairs
            ],
            "artifacts": [
                {
                    "id": a.id,
                    "pair_index": a.pair_index,
                    "rel_path": a.rel_path,
                    "role": a.role,
                    "byte_size": a.byte_size,
                }
                for a in arts
            ],
        }

    @app.get("/artifacts/{artifact_id}/download")
    def download_artifact(
        artifact_id: int,
        user: CurrentUser,
        db: Session = Depends(get_db),
    ) -> FileResponse:
        art = db.query(Artifact).filter(Artifact.id == artifact_id).one_or_none()
        if not art:
            raise HTTPException(404)
        path = os.path.join(job_root(art.job_id), art.rel_path.replace("/", os.sep))
        if not os.path.isfile(path):
            raise HTTPException(404, "File missing on disk")
        return FileResponse(path, filename=os.path.basename(path))

    @app.get("/artifacts/{artifact_id}/preview", response_class=PlainTextResponse)
    def preview_artifact(
        artifact_id: int,
        user: CurrentUser,
        db: Session = Depends(get_db),
        offset: int = Query(0, ge=0),
        limit: int = Query(524_288, ge=1, le=2_000_000),
    ) -> PlainTextResponse:
        art = db.query(Artifact).filter(Artifact.id == artifact_id).one_or_none()
        if not art:
            raise HTTPException(404)
        path = os.path.join(job_root(art.job_id), art.rel_path.replace("/", os.sep))
        if not os.path.isfile(path):
            raise HTTPException(404)
        total = os.path.getsize(path)
        with open(path, "rb") as f:
            f.seek(max(0, offset))
            chunk = f.read(limit)
        try:
            text = chunk.decode("utf-8")
        except UnicodeDecodeError:
            text = chunk.decode("utf-8", errors="replace")
        header = f"<!-- total_bytes={total} offset={offset} count={len(chunk)} -->\n"
        return PlainTextResponse(header + text, media_type="text/plain; charset=utf-8")

    @app.get("/api/prompts/names")
    def api_prompt_names(user: CurrentUser) -> dict:
        from prompt_manager import PromptManager

        pm = PromptManager(str(PROJECT_ROOT / "prompts.json"))
        return {"names": pm.get_prompt_names()}

    @app.get("/api/prompts/{name}")
    def api_prompt_body(name: str, user: CurrentUser) -> dict:
        from prompt_manager import PromptManager

        pm = PromptManager(str(PROJECT_ROOT / "prompts.json"))
        body = pm.get_prompt(name)
        if body is None:
            raise HTTPException(404)
        return {"name": name, "body": body}

    return app


app = create_app()

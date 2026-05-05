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
from webapp.default_prompts import get_default_step1_prompt, get_default_step2_prompt
from webapp.config import (
    DEFAULT_TEST_BANK_MODEL,
    DEFAULT_TEST_BANK_PROVIDER,
    JOBS_ROOT,
    PROJECT_ROOT,
    RUN_TASKS_INLINE,
)
from webapp.database import Base, SessionLocal, engine, get_db
from webapp.schema_migrate import apply_schema_migrations
import webapp.models  # noqa: F401 — register models with metadata
from webapp.deps import CurrentUser
from webapp.datetime_jalali import format_tehran_shamsi
from webapp.job_files import (
    append_log,
    ensure_dirs,
    find_word_file_abs_for_basename,
    job_root,
    list_word_basenames_for_job,
    pair_inputs,
    register_input_artifact,
)
from webapp.models import Artifact, InboxNotification, Job, JobLogLine, JobPair, User
from webapp.tasks_stage_v import run_full_pipeline_job, run_step1_job, run_step2_job
from stage_v_pairing import auto_pair_stage_v_files

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


def require_job_owner(job: Job, user: CurrentUser) -> None:
    if job.created_by_id != user.id:
        raise HTTPException(
            status_code=403,
            detail="Only the user who created this job can change pairing, run steps, or cancel.",
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
    "stage_j": "Importance & Type Tagging",
    # Flashcard Generation — Stage H
    "flashcard": "Flashcard Generation",
    "flashcard_generation": "Flashcard Generation",
    "stage_h": "Flashcard Generation",
    # Test Bank Generation — Stage V (web jobs default)
    "test_bank": "Test Bank Generation",
    "stage_v": "Test Bank Generation",
    # Topic List Extraction — Stage M
    "topic_list": "Topic List Extraction",
    "topic_list_extraction": "Topic List Extraction",
    "stage_m": "Topic List Extraction",
    # Chapter Summary — Stage L
    "chapter_summary": "Chapter Summary",
    "stage_l": "Chapter Summary",
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
}


def job_stage_label(job: Job) -> str:
    t = (job.type or "").strip() or "test_bank"
    return JOB_STAGE_LABELS.get(t, t.replace("_", " ").title())


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
    if any(p.step1_status == "failed" or p.step2_status == "failed" for p in pairs):
        return "failed"
    if any(p.step1_status == "running" or p.step2_status == "running" for p in pairs):
        return "running"
    if all(p.step1_status == "succeeded" and p.step2_status == "succeeded" for p in pairs):
        return "succeeded"
    return "pending"


STEP1_ARTIFACT_ROLES = frozenset({"step1_combined", "txt_dump"})
STEP2_ARTIFACT_ROLES = frozenset({"step2_topic", "final_b_json", "step2_failed_topics", "output"})


def split_artifacts_for_steps(
    artifacts: List[Artifact],
) -> tuple[List[Artifact], List[Artifact], List[Artifact]]:
    s1: List[Artifact] = []
    s2: List[Artifact] = []
    other: List[Artifact] = []
    for a in artifacts:
        if a.role in STEP1_ARTIFACT_ROLES:
            s1.append(a)
        elif a.role in STEP2_ARTIFACT_ROLES:
            s2.append(a)
        else:
            other.append(a)
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


def _iso_utc(dt: Optional[datetime]) -> Optional[str]:
    if dt is None:
        return None
    s = dt.isoformat()
    return s + "Z" if getattr(dt, "tzinfo", None) is None else s


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
            }
            for j in jobs
        ]
        return templates.TemplateResponse(
            request,
            "jobs_list.html",
            {"user": user, "job_rows": job_rows},
        )

    @app.get("/test-bank/new", response_class=HTMLResponse)
    def test_bank_new(request: Request, user: CurrentUser) -> Any:
        return templates.TemplateResponse(
            request,
            "test_bank_new.html",
            {
                "user": user,
                "multipart_ok": HAS_MULTIPART,
                "default_prompt_1": get_default_step1_prompt(),
                "default_prompt_2": get_default_step2_prompt(),
            },
        )

    if HAS_MULTIPART:

        @app.post("/jobs/test-bank")
        async def create_test_bank_job(
            user: CurrentUser,
            db: Session = Depends(get_db),
            stage_j_files: List[UploadFile] = File(...),
            word_files: List[UploadFile] = File(...),
            prompt_1: str = Form(""),
            prompt_2: str = Form(""),
            provider_1: str = Form(DEFAULT_TEST_BANK_PROVIDER),
            model_1: str = Form(DEFAULT_TEST_BANK_MODEL),
            provider_2: str = Form(DEFAULT_TEST_BANK_PROVIDER),
            model_2: str = Form(DEFAULT_TEST_BANK_MODEL),
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

            tmp = tempfile.mkdtemp(prefix="tb_upload_")
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

                j_named = sorted([p for p in j_paths if p], key=lambda x: os.path.basename(x).lower())
                display_name = name_stripped

                prompt_1_eff = prompt_1.strip() or get_default_step1_prompt()
                prompt_2_eff = prompt_2.strip() or get_default_step2_prompt()
                cfg = {
                    "display_name": display_name,
                    "prompt_1": prompt_1_eff,
                    "prompt_2": prompt_2_eff,
                    "provider_1": provider_1,
                    "model_1": model_1,
                    "provider_2": provider_2,
                    "model_2": model_2,
                    "delay_seconds": delay_val,
                }

                job = Job(
                    id=job_id,
                    type="test_bank",
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

    else:

        @app.post("/jobs/test-bank")
        def create_test_bank_job_stub(user: CurrentUser) -> None:
            raise HTTPException(
                503,
                "Install python-multipart (pip install python-multipart) to enable Test Bank file uploads.",
            )

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
        step1_artifacts, step2_artifacts, other_artifacts = split_artifacts_for_steps(artifacts)
        step2_enabled = job.type != "test_bank" or all_pairs_step1_succeeded(pairs)
        creator = job.created_by_user
        creator_label = creator.email if creator else "Unknown"
        is_job_owner = job.created_by_id == user.id
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
                "cfg": json.loads(job.config_json or "{}"),
                "word_choices": word_choices,
                "is_job_owner": is_job_owner,
                "creator_label": creator_label,
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

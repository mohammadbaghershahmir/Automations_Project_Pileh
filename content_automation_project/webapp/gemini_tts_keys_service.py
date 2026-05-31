"""Admin CRUD and CSV import for Gemini TTS API keys."""

from __future__ import annotations

import csv
import io
import logging
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy.orm import Session

from webapp.gemini_tts_key_manager import mask_api_key
from webapp.models import GeminiTtsApiKey

logger = logging.getLogger(__name__)


def list_keys_for_admin(db: Session) -> List[Dict[str, Any]]:
    rows = db.query(GeminiTtsApiKey).order_by(GeminiTtsApiKey.account_name, GeminiTtsApiKey.id).all()
    out: List[Dict[str, Any]] = []
    for r in rows:
        out.append(
            {
                "id": r.id,
                "account_name": r.account_name,
                "project_name": r.project_name or "",
                "masked_key": mask_api_key(r.api_key),
                "is_active": bool(r.is_active),
                "exhausted_until": r.exhausted_until,
                "last_error": r.last_error or "",
                "last_used_at": r.last_used_at,
                "created_at": r.created_at,
            }
        )
    return out


def _existing_key_set(db: Session) -> set[str]:
    return {r.api_key.strip() for r in db.query(GeminiTtsApiKey.api_key).all() if r.api_key}


def add_key_manual(
    db: Session,
    *,
    account_name: str,
    project_name: str,
    api_key: str,
    admin_user_id: Optional[int] = None,
) -> Tuple[bool, str]:
    key = (api_key or "").strip()
    if not key:
        return False, "API key is required"
    if key in _existing_key_set(db):
        return False, "This API key already exists"
    db.add(
        GeminiTtsApiKey(
            account_name=(account_name or "Manual").strip() or "Manual",
            project_name=(project_name or "").strip(),
            api_key=key,
            is_active=True,
            updated_by_id=admin_user_id,
        )
    )
    db.commit()
    return True, "Key added"


def toggle_key_active(db: Session, key_id: int, active: bool) -> bool:
    row = db.query(GeminiTtsApiKey).filter(GeminiTtsApiKey.id == key_id).one_or_none()
    if row is None:
        return False
    row.is_active = active
    db.commit()
    return True


def delete_key(db: Session, key_id: int) -> bool:
    row = db.query(GeminiTtsApiKey).filter(GeminiTtsApiKey.id == key_id).one_or_none()
    if row is None:
        return False
    db.delete(row)
    db.commit()
    return True


def _detect_delimiter(sample: str) -> str:
    if ";" in sample and sample.count(";") >= sample.count(","):
        return ";"
    return ","


def parse_csv_keys(content: bytes, fallback_account: str = "") -> List[Dict[str, str]]:
    text = content.decode("utf-8-sig", errors="replace")
    delim = _detect_delimiter(text[:1024])
    reader = csv.DictReader(io.StringIO(text), delimiter=delim)
    rows: List[Dict[str, str]] = []
    for row in reader:
        api_key = (
            row.get("api_key")
            or row.get("API_KEY")
            or row.get("Api_Key")
            or ""
        ).strip()
        if not api_key:
            continue
        account = (row.get("account") or row.get("Account") or "").strip()
        if not account:
            account = fallback_account.strip() or "imported"
        project = (row.get("project") or row.get("Project") or "").strip()
        rows.append({"account_name": account, "project_name": project, "api_key": api_key})
    return rows


def import_csv_bytes(
    db: Session,
    content: bytes,
    *,
    fallback_account: str = "",
    admin_user_id: Optional[int] = None,
) -> Tuple[int, int, List[str]]:
    """
    Import keys from CSV bytes. Returns (added, skipped_duplicates, errors).
    """
    parsed = parse_csv_keys(content, fallback_account=fallback_account)
    if not parsed:
        return 0, 0, ["No valid api_key rows found in CSV"]

    existing = _existing_key_set(db)
    added = 0
    skipped = 0
    errors: List[str] = []

    for item in parsed:
        key = item["api_key"]
        if key in existing:
            skipped += 1
            continue
        try:
            db.add(
                GeminiTtsApiKey(
                    account_name=item["account_name"],
                    project_name=item["project_name"],
                    api_key=key,
                    is_active=True,
                    updated_by_id=admin_user_id,
                )
            )
            db.flush()
            existing.add(key)
            added += 1
        except Exception as e:
            errors.append(str(e))

    if added:
        db.commit()
    else:
        db.rollback()
    return added, skipped, errors


def seed_gemini_tts_keys_from_dir(db: Session, directory: str) -> int:
    """Import all *.csv from directory if DB table is empty. Returns count added."""
    import os

    if db.query(GeminiTtsApiKey).count() > 0:
        return 0
    if not directory or not os.path.isdir(directory):
        return 0

    total = 0
    for name in sorted(os.listdir(directory)):
        if not name.lower().endswith(".csv"):
            continue
        path = os.path.join(directory, name)
        fallback = os.path.splitext(name)[0].replace(" apikey", "").replace(" freetier", "").strip()
        try:
            with open(path, "rb") as f:
                added, _, _ = import_csv_bytes(db, f.read(), fallback_account=fallback)
            total += added
        except Exception as e:
            logger.warning("Failed to seed keys from %s: %s", path, e)
    return total

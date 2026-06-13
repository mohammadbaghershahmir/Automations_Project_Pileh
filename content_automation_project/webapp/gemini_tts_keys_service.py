"""Admin CRUD and CSV/XLSX import for Gemini TTS API keys."""

from __future__ import annotations

import csv
import io
import logging
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy import func
from sqlalchemy.orm import Session

from webapp.config import GEMINI_TTS_DEFAULT_RPM, GEMINI_TTS_DEFAULT_RPD
from webapp.gemini_tts_key_manager import GeminiTtsKeyManager, mask_api_key
from webapp.models import GeminiTtsApiKey

logger = logging.getLogger(__name__)


def account_name_groups(db: Session) -> List[Dict[str, Any]]:
    """Distinct account names with key counts, highest count first."""
    rows = (
        db.query(GeminiTtsApiKey.account_name, func.count(GeminiTtsApiKey.id))
        .group_by(GeminiTtsApiKey.account_name)
        .order_by(func.count(GeminiTtsApiKey.id).desc(), GeminiTtsApiKey.account_name)
        .all()
    )
    return [{"account_name": name, "count": int(count)} for name, count in rows]


def list_keys_for_admin(db: Session) -> List[Dict[str, Any]]:
    from datetime import datetime

    counts = {
        g["account_name"]: g["count"]
        for g in account_name_groups(db)
    }
    rows = db.query(GeminiTtsApiKey).order_by(GeminiTtsApiKey.account_name, GeminiTtsApiKey.id).all()
    now = datetime.utcnow()
    out: List[Dict[str, Any]] = []
    for r in rows:
        GeminiTtsKeyManager._refresh_row_counters(r, now)
        rpm = int(r.rpm_limit or GEMINI_TTS_DEFAULT_RPM)
        rpd = int(r.rpd_limit or GEMINI_TTS_DEFAULT_RPD)
        out.append(
            {
                "id": r.id,
                "account_name": r.account_name,
                "account_count": counts.get(r.account_name, 1),
                "project_name": r.project_name or "",
                "masked_key": mask_api_key(r.api_key),
                "is_active": bool(r.is_active),
                "rpm_limit": rpm,
                "rpd_limit": rpd,
                "requests_today": int(r.requests_today or 0),
                "requests_in_minute": int(r.requests_in_minute or 0),
                "exhausted_until": r.exhausted_until,
                "last_error": r.last_error or "",
                "last_used_at": r.last_used_at,
                "created_at": r.created_at,
            }
        )
    db.commit()
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
            rpm_limit=GEMINI_TTS_DEFAULT_RPM,
            rpd_limit=GEMINI_TTS_DEFAULT_RPD,
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


def delete_all_keys(db: Session) -> int:
    """Remove every Gemini TTS key. Returns number of rows deleted."""
    n = db.query(GeminiTtsApiKey).count()
    if n == 0:
        return 0
    db.query(GeminiTtsApiKey).delete()
    db.commit()
    return n


def delete_keys_by_account_name(db: Session, account_name: str) -> int:
    """Remove all keys sharing the same account name. Returns number deleted."""
    name = (account_name or "").strip()
    if not name:
        return 0
    n = db.query(GeminiTtsApiKey).filter(GeminiTtsApiKey.account_name == name).count()
    if n == 0:
        return 0
    db.query(GeminiTtsApiKey).filter(GeminiTtsApiKey.account_name == name).delete()
    db.commit()
    return n


def _detect_delimiter(sample: str) -> str:
    if ";" in sample and sample.count(";") >= sample.count(","):
        return ";"
    return ","


def _cell_str(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _extract_key_from_row(row: Dict[str, Any], fallback_account: str) -> Optional[Dict[str, str]]:
    norm = {
        str(k).strip().lower(): _cell_str(v)
        for k, v in row.items()
        if k is not None and str(k).strip()
    }
    api_key = norm.get("api_key") or norm.get("api key") or ""
    if not api_key:
        return None
    account = norm.get("account") or fallback_account.strip() or "imported"
    project = norm.get("project") or ""
    return {"account_name": account, "project_name": project, "api_key": api_key}


def parse_csv_keys(content: bytes, fallback_account: str = "") -> List[Dict[str, str]]:
    text = content.decode("utf-8-sig", errors="replace")
    delim = _detect_delimiter(text[:1024])
    reader = csv.DictReader(io.StringIO(text), delimiter=delim)
    rows: List[Dict[str, str]] = []
    for row in reader:
        item = _extract_key_from_row(row, fallback_account)
        if item:
            rows.append(item)
    return rows


def parse_xlsx_keys(content: bytes, fallback_account: str = "") -> List[Dict[str, str]]:
    try:
        import openpyxl
    except ImportError as e:
        raise RuntimeError("openpyxl is required for XLSX import") from e

    wb = openpyxl.load_workbook(io.BytesIO(content), read_only=True, data_only=True)
    try:
        ws = wb.active
        row_iter = ws.iter_rows(values_only=True)
        header = next(row_iter, None)
        if not header:
            return []

        headers = [_cell_str(h).lower() for h in header]
        rows: List[Dict[str, str]] = []
        for values in row_iter:
            if not values or all(v is None or _cell_str(v) == "" for v in values):
                continue
            row_dict = {
                headers[i]: values[i] if i < len(values) else None
                for i in range(len(headers))
                if headers[i]
            }
            item = _extract_key_from_row(row_dict, fallback_account)
            if item:
                rows.append(item)
        return rows
    finally:
        wb.close()


def _is_xlsx_content(content: bytes, filename: str = "") -> bool:
    fn = (filename or "").lower()
    if fn.endswith((".xlsx", ".xlsm")):
        return True
    return content[:2] == b"PK"


def _parse_keys(content: bytes, *, filename: str = "", fallback_account: str = "") -> Tuple[List[Dict[str, str]], str]:
    if _is_xlsx_content(content, filename):
        return parse_xlsx_keys(content, fallback_account=fallback_account), "spreadsheet"
    return parse_csv_keys(content, fallback_account=fallback_account), "CSV"


def import_keys_bytes(
    db: Session,
    content: bytes,
    *,
    filename: str = "",
    fallback_account: str = "",
    admin_user_id: Optional[int] = None,
) -> Tuple[int, int, List[str]]:
    """
    Import keys from CSV or XLSX bytes. Returns (added, skipped_duplicates, errors).
    """
    try:
        parsed, kind = _parse_keys(content, filename=filename, fallback_account=fallback_account)
    except Exception as e:
        return 0, 0, [str(e)]

    if not parsed:
        return 0, 0, [f"No valid api_key rows found in {kind}"]

    return _import_parsed_keys(db, parsed, admin_user_id=admin_user_id)


def import_csv_bytes(
    db: Session,
    content: bytes,
    *,
    filename: str = "",
    fallback_account: str = "",
    admin_user_id: Optional[int] = None,
) -> Tuple[int, int, List[str]]:
    """Backward-compatible alias for import_keys_bytes."""
    return import_keys_bytes(
        db,
        content,
        filename=filename,
        fallback_account=fallback_account,
        admin_user_id=admin_user_id,
    )


def _import_parsed_keys(
    db: Session,
    parsed: List[Dict[str, str]],
    *,
    admin_user_id: Optional[int] = None,
) -> Tuple[int, int, List[str]]:

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
                    rpm_limit=GEMINI_TTS_DEFAULT_RPM,
                    rpd_limit=GEMINI_TTS_DEFAULT_RPD,
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
    """Import all *.csv / *.xlsx from directory if DB table is empty. Returns count added."""
    import os

    if db.query(GeminiTtsApiKey).count() > 0:
        return 0
    if not directory or not os.path.isdir(directory):
        return 0

    total = 0
    for name in sorted(os.listdir(directory)):
        lower = name.lower()
        if not (lower.endswith(".csv") or lower.endswith(".xlsx") or lower.endswith(".xlsm")):
            continue
        path = os.path.join(directory, name)
        fallback = os.path.splitext(name)[0].replace(" apikey", "").replace(" freetier", "").strip()
        try:
            with open(path, "rb") as f:
                added, _, _ = import_keys_bytes(
                    db,
                    f.read(),
                    filename=name,
                    fallback_account=fallback,
                )
            total += added
        except Exception as e:
            logger.warning("Failed to seed keys from %s: %s", path, e)
    return total

"""Load and rotate Gemini TTS API keys from the database."""

from __future__ import annotations

import logging
import re
from datetime import datetime, timedelta
from typing import List, Optional

from sqlalchemy.orm import Session

from api_layer import APIKeyManager
from webapp.models import GeminiTtsApiKey

logger = logging.getLogger(__name__)

_QUOTA_RE = re.compile(
    r"429|quota|rate.?limit|resource.?exhausted|exceeded",
    re.IGNORECASE,
)


def mask_api_key(key: str) -> str:
    k = (key or "").strip()
    if len(k) <= 10:
        return "***"
    return f"{k[:4]}…{k[-4:]}"


class GeminiTtsKeyManager:
    """Round-robin Gemini TTS keys with quota-aware cooldown."""

    def __init__(self, db: Session):
        self.db = db
        self._index = 0

    def max_attempts(self) -> int:
        n = (
            self.db.query(GeminiTtsApiKey)
            .filter(GeminiTtsApiKey.is_active.is_(True))
            .count()
        )
        return max(n, 1)

    def _active_rows(self) -> List[GeminiTtsApiKey]:
        now = datetime.utcnow()
        rows = (
            self.db.query(GeminiTtsApiKey)
            .filter(GeminiTtsApiKey.is_active.is_(True))
            .order_by(GeminiTtsApiKey.id)
            .all()
        )
        return [r for r in rows if r.exhausted_until is None or r.exhausted_until <= now]

    def get_next_available_key(self) -> Optional[GeminiTtsApiKey]:
        rows = self._active_rows()
        if not rows:
            return None
        if self._index >= len(rows):
            self._index = 0
        row = rows[self._index % len(rows)]
        self._index += 1
        return row

    def mark_success(self, row: GeminiTtsApiKey) -> None:
        row.last_used_at = datetime.utcnow()
        row.last_error = None
        self.db.commit()

    def mark_failure(self, row: GeminiTtsApiKey, error: str) -> None:
        sanitized = APIKeyManager.sanitize_error_message(error, row.api_key)
        row.last_error = sanitized[:500] if sanitized else None
        if _QUOTA_RE.search(error or ""):
            row.exhausted_until = datetime.utcnow() + timedelta(hours=1)
            logger.warning("Gemini TTS key %s marked exhausted for 1h", row.account_name)
        self.db.commit()

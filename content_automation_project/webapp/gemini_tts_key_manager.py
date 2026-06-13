"""Load and rotate Gemini TTS API keys from the database."""

from __future__ import annotations

import logging
import re
import time
from datetime import datetime, timedelta, timezone
from typing import Callable, Dict, List, Optional
from zoneinfo import ZoneInfo

from sqlalchemy.orm import Session

from api_layer import APIKeyManager
from webapp.config import (
    GEMINI_TTS_DAILY_RESET_TZ,
    GEMINI_TTS_DEFAULT_RPD,
    GEMINI_TTS_DEFAULT_RPM,
    GEMINI_TTS_MAX_ROTATION_ATTEMPTS,
)
from webapp.models import GeminiTtsApiKey

logger = logging.getLogger(__name__)

_QUOTA_RE = re.compile(
    r"429|quota|rate.?limit|resource.?exhausted|exceeded",
    re.IGNORECASE,
)
_DAILY_QUOTA_RE = re.compile(
    r"per.?day|requests?.?per.?day|daily|RPD|quota.*day",
    re.IGNORECASE,
)
_INVALID_KEY_RE = re.compile(
    r"API_KEY_INVALID|API key expired|API key not valid|API Key not found|invalid.*api.?key",
    re.IGNORECASE,
)


def mask_api_key(key: str) -> str:
    k = (key or "").strip()
    if len(k) <= 10:
        return "***"
    return f"{k[:4]}…{k[-4:]}"


def _pt_zone() -> ZoneInfo:
    try:
        return ZoneInfo(GEMINI_TTS_DAILY_RESET_TZ)
    except Exception:
        return ZoneInfo("America/Los_Angeles")


def _pt_today_iso() -> str:
    return datetime.now(_pt_zone()).date().isoformat()


def _next_midnight_pt_utc(now: Optional[datetime] = None) -> datetime:
    """Naive UTC datetime when the next Pacific midnight occurs."""
    now = now or datetime.utcnow()
    now_pt = datetime.now(_pt_zone())
    next_midnight_pt = (now_pt + timedelta(days=1)).replace(
        hour=0, minute=0, second=0, microsecond=0
    )
    return next_midnight_pt.astimezone(timezone.utc).replace(tzinfo=None)


class GeminiTtsKeyManager:
    """Round-robin Gemini TTS keys with proactive RPM/RPD budgets."""

    def __init__(self, db: Session):
        self.db = db
        self._index = 0

    @staticmethod
    def _limits(row: GeminiTtsApiKey) -> tuple[int, int]:
        rpm = int(row.rpm_limit or GEMINI_TTS_DEFAULT_RPM)
        rpd = int(row.rpd_limit or GEMINI_TTS_DEFAULT_RPD)
        return max(rpm, 1), max(rpd, 1)

    @staticmethod
    def _refresh_row_counters(row: GeminiTtsApiKey, now: datetime) -> None:
        today = _pt_today_iso()
        if row.daily_quota_date != today:
            row.daily_quota_date = today
            row.requests_today = 0

        if row.rpm_window_start is None or (now - row.rpm_window_start).total_seconds() >= 60:
            row.rpm_window_start = now
            row.requests_in_minute = 0

    def _has_budget(self, row: GeminiTtsApiKey, now: datetime) -> bool:
        if row.exhausted_until is not None and row.exhausted_until > now:
            return False
        self._refresh_row_counters(row, now)
        rpm, rpd = self._limits(row)
        return row.requests_in_minute < rpm and row.requests_today < rpd

    def _seconds_until_row_available(self, row: GeminiTtsApiKey, now: datetime) -> float:
        waits = [0.0]
        if row.exhausted_until is not None and row.exhausted_until > now:
            waits.append((row.exhausted_until - now).total_seconds())
        self._refresh_row_counters(row, now)
        rpm, rpd = self._limits(row)
        if row.requests_in_minute >= rpm and row.rpm_window_start is not None:
            window_end = row.rpm_window_start + timedelta(seconds=60)
            if window_end > now:
                waits.append((window_end - now).total_seconds())
        if row.requests_today >= rpd:
            midnight = _next_midnight_pt_utc(now)
            if midnight > now:
                waits.append((midnight - now).total_seconds())
        return max(waits)

    def _all_active_rows(self) -> List[GeminiTtsApiKey]:
        return (
            self.db.query(GeminiTtsApiKey)
            .filter(GeminiTtsApiKey.is_active.is_(True))
            .order_by(GeminiTtsApiKey.id)
            .all()
        )

    def max_attempts(self) -> int:
        n = len(self._all_active_rows())
        return min(GEMINI_TTS_MAX_ROTATION_ATTEMPTS, max(n, 1))

    def seconds_until_any_key_available(self) -> Optional[float]:
        now = datetime.utcnow()
        rows = self._all_active_rows()
        if not rows:
            return None
        earliest: Optional[float] = None
        for row in rows:
            delay = self._seconds_until_row_available(row, now)
            if delay <= 0:
                return 0.0
            if earliest is None or delay < earliest:
                earliest = delay
        return earliest

    def pool_stats(self) -> Dict[str, int]:
        """Remaining daily TTS budget across all active keys."""
        now = datetime.utcnow()
        rows = self._all_active_rows()
        remaining = 0
        for row in rows:
            self._refresh_row_counters(row, now)
            _, rpd = self._limits(row)
            remaining += max(0, rpd - int(row.requests_today or 0))
        self.db.commit()
        return {
            "active_keys": len(rows),
            "daily_budget_remaining": remaining,
            "default_rpm": GEMINI_TTS_DEFAULT_RPM,
            "default_rpd": GEMINI_TTS_DEFAULT_RPD,
        }

    def wait_for_available_key(
        self,
        *,
        progress_callback: Optional[Callable[[str], None]] = None,
        cancel_check: Optional[Callable[[], bool]] = None,
    ) -> Optional[GeminiTtsApiKey]:
        """Block until a key has RPM/RPD budget, or return None if cancelled / no keys."""
        while True:
            if cancel_check and cancel_check():
                return None
            key = self.get_next_available_key()
            if key is not None:
                return key

            delay = self.seconds_until_any_key_available()
            if delay is None:
                return None

            delay = max(1.0, min(delay, 300.0))
            msg = (
                f"Gemini TTS rate limit — waiting {delay:.0f}s "
                f"(pool RPM={GEMINI_TTS_DEFAULT_RPM}, RPD={GEMINI_TTS_DEFAULT_RPD})"
            )
            logger.info(msg)
            if progress_callback:
                progress_callback(msg)

            remaining = delay
            while remaining > 0:
                if cancel_check and cancel_check():
                    return None
                chunk = min(remaining, 5.0)
                time.sleep(chunk)
                remaining -= chunk
            self.db.expire_all()

    def get_next_available_key(self) -> Optional[GeminiTtsApiKey]:
        now = datetime.utcnow()
        rows = self._all_active_rows()
        eligible = [r for r in rows if self._has_budget(r, now)]
        if not eligible:
            self.db.commit()
            return None
        if self._index >= len(eligible):
            self._index = 0
        row = eligible[self._index % len(eligible)]
        self._index += 1
        self.db.commit()
        return row

    def _apply_success_budget(self, row: GeminiTtsApiKey, now: datetime) -> None:
        self._refresh_row_counters(row, now)
        row.requests_today = int(row.requests_today or 0) + 1
        row.requests_in_minute = int(row.requests_in_minute or 0) + 1
        rpm, rpd = self._limits(row)
        if row.requests_in_minute >= rpm and row.rpm_window_start is not None:
            window_end = row.rpm_window_start + timedelta(seconds=60)
            if window_end > now:
                row.exhausted_until = max(row.exhausted_until or now, window_end)
        if row.requests_today >= rpd:
            row.exhausted_until = max(row.exhausted_until or now, _next_midnight_pt_utc(now))

    def mark_success(self, row: GeminiTtsApiKey) -> None:
        now = datetime.utcnow()
        row.last_used_at = now
        row.last_error = None
        self._apply_success_budget(row, now)
        self.db.commit()

    def mark_failure(self, row: GeminiTtsApiKey, error: str) -> None:
        now = datetime.utcnow()
        sanitized = APIKeyManager.sanitize_error_message(error, row.api_key)
        row.last_error = sanitized[:500] if sanitized else None
        err = error or ""
        rpm, rpd = self._limits(row)
        if _INVALID_KEY_RE.search(err):
            row.is_active = False
            logger.warning(
                "Gemini TTS key %s deactivated (invalid/expired) — update keys in Admin",
                row.account_name,
            )
        elif _QUOTA_RE.search(err):
            self._refresh_row_counters(row, now)
            if _DAILY_QUOTA_RE.search(err):
                row.requests_today = rpd
                row.exhausted_until = max(row.exhausted_until or now, _next_midnight_pt_utc(now))
                logger.warning(
                    "Gemini TTS key %s hit daily quota (RPD=%s)",
                    row.account_name,
                    rpd,
                )
            else:
                row.requests_in_minute = rpm
                if row.rpm_window_start is None:
                    row.rpm_window_start = now
                row.exhausted_until = max(
                    row.exhausted_until or now,
                    row.rpm_window_start + timedelta(seconds=60),
                )
                logger.warning(
                    "Gemini TTS key %s hit RPM limit (%s/min)",
                    row.account_name,
                    rpm,
                )
        self.db.commit()

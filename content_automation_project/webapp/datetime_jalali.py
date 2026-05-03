"""Tehran (Shamsi) display for job timestamps."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

import jdatetime


def format_tehran_shamsi(dt: Optional[datetime]) -> str:
    """Format naive UTC (or aware) datetime as Jalali date/time in Asia/Tehran."""
    if dt is None:
        return "—"
    if dt.tzinfo is None:
        dt_utc = dt.replace(tzinfo=timezone.utc)
    else:
        dt_utc = dt.astimezone(timezone.utc)
    try:
        from zoneinfo import ZoneInfo

        local = dt_utc.astimezone(ZoneInfo("Asia/Tehran"))
    except Exception:
        local = dt_utc
    jd = jdatetime.datetime.fromgregorian(datetime=local)
    return jd.strftime("%Y/%m/%d %H:%M")

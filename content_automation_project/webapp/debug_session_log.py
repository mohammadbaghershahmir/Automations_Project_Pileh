"""NDJSON debug logs for Cursor debug sessions (worker + API)."""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any, Optional

from webapp.config import PROJECT_ROOT

_SESSION_ID = "5cc8d9"
_LOG_PATH = PROJECT_ROOT / ".cursor" / f"debug-{_SESSION_ID}.log"


def debug_log(
    hypothesis_id: str,
    location: str,
    message: str,
    data: Optional[dict[str, Any]] = None,
    *,
    run_id: str = "pre-fix",
) -> None:
    # #region agent log
    try:
        _LOG_PATH.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "sessionId": _SESSION_ID,
            "runId": run_id,
            "hypothesisId": hypothesis_id,
            "location": location,
            "message": message,
            "data": data or {},
            "timestamp": int(time.time() * 1000),
        }
        with open(_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps(payload, default=str) + "\n")
    except Exception:
        pass
    # #endregion

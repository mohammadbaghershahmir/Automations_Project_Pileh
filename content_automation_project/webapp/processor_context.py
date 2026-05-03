"""Build UnifiedAPIClient + StageVProcessor for worker and API (same as GUI)."""

from __future__ import annotations

import logging
import sys
from pathlib import Path

# Ensure project root on path when running as webapp.worker
_PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from api_layer import APIKeyManager  # noqa: E402
from stage_settings_manager import StageSettingsManager  # noqa: E402
from stage_v_processor import StageVProcessor  # noqa: E402
from unified_api_client import UnifiedAPIClient  # noqa: E402

logger = logging.getLogger(__name__)


def build_stage_v_processor():
    """Return (api_client, stage_settings_manager, stage_v_processor)."""
    km = APIKeyManager()
    km.load_from_env()
    ssm = StageSettingsManager()
    client = UnifiedAPIClient(
        google_api_key_manager=km,
        deepseek_api_key_manager=km,
        openrouter_api_key_manager=km,
        stage_settings_manager=ssm,
    )
    proc = StageVProcessor(client)
    return client, ssm, proc

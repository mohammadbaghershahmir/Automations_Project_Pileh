import os
from pathlib import Path

# Project root `content_automation_project/` (parent of `webapp/`)
PROJECT_ROOT = Path(__file__).resolve().parents[1]

JOBS_ROOT = os.environ.get("JOBS_ROOT", str(PROJECT_ROOT / "data" / "jobs"))
DATABASE_URL = os.environ.get("DATABASE_URL", f"sqlite:///{PROJECT_ROOT / 'data' / 'webapp.db'}")
REDIS_URL = os.environ.get("REDIS_URL", "redis://127.0.0.1:6379/0")
SECRET_KEY = os.environ.get("SECRET_KEY", "change-me-in-production-use-openssl-rand-hex-32")
ACCESS_TOKEN_EXPIRE_MINUTES = int(os.environ.get("ACCESS_TOKEN_EXPIRE_MINUTES", "10080"))  # 7 days
ALGORITHM = "HS256"

# When true, run Step 1/2/full pipeline in a background thread instead of Celery + Redis.
# Use when developing with only uvicorn (no separate Celery worker).
_run_inline = os.environ.get("WEBAPP_RUN_TASKS_INLINE", "").strip().lower()
RUN_TASKS_INLINE = _run_inline in ("1", "true", "yes", "on")

# Test Bank / Stage V API defaults (aligned with api_layer.APIConfig OpenRouter + GLM-5)
DEFAULT_TEST_BANK_PROVIDER = "openrouter"
DEFAULT_TEST_BANK_MODEL = "z-ai/glm-5"

# Web admin Test Bank model dropdown only. Does not change desktop api_layer model lists.
TEST_BANK_OPENROUTER_MODEL_CHOICES = (
    "z-ai/glm-5",
    "qwen/qwen3.6-plus",
    "qwen/qwen3.5-plus-20260420",
    "google/gemini-2.5-pro",
)


def normalize_nonempty(value: object, default: str) -> str:
    """Strip and fall back to default (handles empty JSON strings)."""
    if value is None:
        return default
    s = str(value).strip()
    return s if s else default


def normalize_test_bank_model(value: object, default: str = DEFAULT_TEST_BANK_MODEL) -> str:
    return normalize_nonempty(value, default)


def normalize_test_bank_provider(value: object, default: str = DEFAULT_TEST_BANK_PROVIDER) -> str:
    return normalize_nonempty(value, default)


# Admin bootstrap: ADMIN_EMAIL_1, ADMIN_PASSWORD_1, ... up to 3, or JSON in ADMIN_BOOTSTRAP

import os
from pathlib import Path

from openrouter_models import OPENROUTER_MODEL_CHOICE_IDS

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

# Web admin model dropdown for all job types. Shared with api_layer via openrouter_models.
TEST_BANK_OPENROUTER_MODEL_CHOICES = OPENROUTER_MODEL_CHOICE_IDS


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


SONGS_DIR = os.environ.get("SONGS_DIR", str(PROJECT_ROOT / "songs"))
VOICE_CLASS_INTRO_FILENAME = "a_int.mp3"
VOICE_CLASS_OUTRO_FILENAME = "a_out.mp3"
GEMINI_TTS_KEYS_SEED_DIR = os.environ.get(
    "GEMINI_TTS_KEYS_SEED_DIR",
    str(PROJECT_ROOT / "data" / "seed" / "gemini_tts_keys"),
)

DEFAULT_VOICE_CLASS_TTS_MODEL = "gemini-2.5-flash-preview-tts"
DEFAULT_VOICE_CLASS_TTS_VOICE = "Enceladus"
DEFAULT_VOICE_CLASS_MAX_SEGMENT_SECONDS = 60.0
DEFAULT_VOICE_CLASS_CHARS_PER_SECOND = 13.0


def get_voice_class_song_paths() -> tuple[str, str]:
    """Absolute paths to intro/outro MP3s under SONGS_DIR."""
    base = Path(SONGS_DIR).expanduser().resolve()
    return str(base / VOICE_CLASS_INTRO_FILENAME), str(base / VOICE_CLASS_OUTRO_FILENAME)


def voice_class_songs_status() -> dict:
    intro, outro = get_voice_class_song_paths()
    return {
        "songs_dir": str(Path(SONGS_DIR).expanduser().resolve()),
        "intro_path": intro,
        "outro_path": outro,
        "intro_ok": os.path.isfile(intro),
        "outro_ok": os.path.isfile(outro),
    }

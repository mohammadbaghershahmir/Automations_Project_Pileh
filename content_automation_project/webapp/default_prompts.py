"""Default Test Bank (Stage V) Step 1 / Step 2 prompts — canonical copy lives in `prompts.json`."""

from __future__ import annotations

import json
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parents[1]

_STEP1_KEY = "Test Bank Generation - Step 1 Prompt"
_STEP2_KEY = "Test Bank Generation - Step 2 Prompt"


def _read_prompts() -> dict:
    p = _PROJECT_ROOT / "prompts.json"
    return json.loads(p.read_text(encoding="utf-8"))


def get_default_step1_prompt() -> str:
    return _read_prompts()[_STEP1_KEY].strip()


def get_default_step2_prompt() -> str:
    return _read_prompts()[_STEP2_KEY].strip()


DEFAULT_TEST_BANK_STEP1_PROMPT = get_default_step1_prompt()
DEFAULT_TEST_BANK_STEP2_PROMPT = get_default_step2_prompt()

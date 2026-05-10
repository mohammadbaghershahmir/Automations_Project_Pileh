"""
Persist the exact user/system payload passed to UnifiedAPIClient.process_text for job debugging.

Writes UTF-8 files under job_root/pair_N/prompts/ and registers artifacts with role llm_prompt_step1 / llm_prompt_step2.
"""

from __future__ import annotations

import logging
import os
import re
from typing import Any, Optional

from sqlalchemy.orm import Session

from unified_api_client import UnifiedAPIClient
from webapp.job_files import job_root, register_input_artifact

logger = logging.getLogger(__name__)


def _safe_filename_part(s: str) -> str:
    t = re.sub(r"[^a-zA-Z0-9._-]+", "_", (s or "").strip())
    return (t[:80] if t else "job")


class PromptCapturingUnifiedClient:
    """Delegate to UnifiedAPIClient; before each process_text, dump the full request body to disk."""

    def __init__(
        self,
        inner: UnifiedAPIClient,
        db: Session,
        job_id: str,
        pair_index: int,
        job_type: str,
        pipeline_step: str,
    ):
        self._inner = inner
        self._db = db
        self._job_id = job_id
        self._pair_index = pair_index
        self._job_type = job_type or "unknown"
        self._pipeline_step = pipeline_step
        self._seq = 0

    def __getattr__(self, name: str) -> Any:
        return getattr(self._inner, name)

    def process_text(self, *args: Any, **kwargs: Any) -> Optional[str]:
        self._seq += 1
        text = kwargs.get("text")
        if text is None and args:
            text = args[0]
        system_prompt = kwargs.get("system_prompt")
        model_name = kwargs.get("model_name")
        temperature = kwargs.get("temperature")
        max_tokens = kwargs.get("max_tokens")
        timeout_s = kwargs.get("timeout_s")
        cancel_check = kwargs.get("cancel_check")

        header = (
            "=== LLM request capture (process_text) ===\n"
            f"job_id: {self._job_id}\n"
            f"pair_index: {self._pair_index}\n"
            f"job_type: {self._job_type}\n"
            f"pipeline_step: {self._pipeline_step}\n"
            f"call_sequence: {self._seq:04d}\n"
            f"model_name (argument): {model_name!r}\n"
            f"temperature: {temperature!r}\n"
            f"max_tokens: {max_tokens!r}\n"
            f"timeout_s: {timeout_s!r}\n"
            f"cancel_check_set: {cancel_check is not None}\n"
            "\n=== system_prompt ===\n"
            f"{system_prompt if system_prompt else '(none)'}\n"
            "\n=== user message text (full prompt sent to the API) ===\n"
        )
        body = text if isinstance(text, str) else repr(text)
        full_dump = header + body

        base = job_root(self._job_id)
        prompts_dir = os.path.join(base, f"pair_{self._pair_index}", "prompts")
        try:
            os.makedirs(prompts_dir, exist_ok=True)
            jt = _safe_filename_part(self._job_type)
            ps = _safe_filename_part(self._pipeline_step)
            fn = f"{self._seq:04d}_{ps}_{jt}.txt"
            abs_path = os.path.join(prompts_dir, fn)
            with open(abs_path, "w", encoding="utf-8") as f:
                f.write(full_dump)
            rel_path = os.path.relpath(abs_path, base).replace("\\", "/")
            role = f"llm_prompt_{self._pipeline_step}"
            register_input_artifact(self._db, self._job_id, self._pair_index, base, rel_path, role)
        except Exception as e:
            logger.warning("Failed to save LLM prompt capture: %s", e)

        return self._inner.process_text(*args, **kwargs)


def wrap_prompt_capture(
    client: UnifiedAPIClient,
    db: Session,
    job_id: str,
    pair_index: int,
    job_type: str,
    pipeline_step: str,
) -> PromptCapturingUnifiedClient:
    """pipeline_step: step1 | step2 (matches artifact role llm_prompt_step1 / llm_prompt_step2)."""
    return PromptCapturingUnifiedClient(
        client, db, job_id, pair_index, job_type, pipeline_step
    )

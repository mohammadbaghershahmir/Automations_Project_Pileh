"""
Prompt assembly for Voice Class Step 1 script generation.

Pure string building — no API calls, no filesystem access.
"""

from __future__ import annotations

import json
from typing import Any, Dict, List, Optional

from voice_class_captions import format_caption_block_for_prompt

_SCRIPT_JSON_SCHEMA = """
Return ONLY valid JSON with this structure:
{
  "paragraphs": [
    {
      "paragraph_id": 1,
      "chapter": "...",
      "subchapter": "...",
      "topic": "...",
      "text": "..."
    }
  ]
}
Each paragraph must be a self-contained spoken block in Farsi (complete sentences).
Use paragraph_id starting at 1 within this topic response.
Escape double quotes inside text with backslash (\\"). No markdown fences. No trailing commas."""

SCRIPT_JSON_RETRY_SUFFIX = (
    "\n\nCRITICAL: Reply with raw JSON only — no markdown code fences, no commentary. "
    "Escape every double quote inside string values with \\\". "
    "Use valid JSON commas between array elements."
)


def build_topic_scope_instruction(
    chapter: str,
    subchapter: str,
    topic: str,
    topic_index: int,
    total_topics: int,
) -> str:
    """Scope instruction telling the LLM which topic to write for."""
    return (
        f"\n\n[مبحث {topic_index}/{total_topics}: «{topic}» — "
        f"زیرفصل «{subchapter}» — فصل «{chapter}». "
        f"فقط از نکات همین مبحث در JSON زیر اسکریپت بنویس.]\n"
    )


def build_topic_script_prompt(
    prompt_body: str,
    chapter: str,
    subchapter: str,
    topic: str,
    topic_rows: List[Dict[str, Any]],
    topic_index: int,
    total_topics: int,
    *,
    topics_context: Optional[Dict[str, Any]] = None,
) -> str:
    """Assemble the full user prompt for one topic's script generation."""
    scope = build_topic_scope_instruction(
        chapter,
        subchapter,
        topic,
        topic_index,
        total_topics,
    )

    caption_block = ""
    if topics_context:
        caption_block = format_caption_block_for_prompt(topics_context)

    lesson_json = json.dumps(topic_rows, ensure_ascii=False, indent=2)

    return (
        f"{prompt_body}{scope}{caption_block}\n\n"
        f"Tagged lesson JSON for THIS TOPIC ONLY (Imp 1 and 2 points only):\n"
        f"{lesson_json}\n"
        f"{_SCRIPT_JSON_SCHEMA}"
    )

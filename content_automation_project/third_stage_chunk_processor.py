"""
Third stage chunked processor.

This module handles chunked third-stage processing for large JSON inputs,
according to the user's chunking prompt specification.

The model is instructed to return JSON objects with the following structure
for each chunk:

{
  "chunk_index": <int>,
  "is_last": <true|false>,
  "payload": {
    "numberchapter": "...",
    "chapter": "...",
    "content": [
      {
        "level_1": "...",
        "children": [ ... ]
      }
    ]
  },
  "next_cursor": {
    "next_page": <int>,
    "note": "..."
  }
}

We then loop over chunks, accumulate all payload.content items, and finally
return a single combined JSON of the form:

{
  "chapter": "...",
  "content": [ ... ]
}
"""

import json
import logging
from typing import Any, Dict, List, Optional, Callable


logger = logging.getLogger(__name__)


def run_third_stage_chunked(
    api_client: Any,
    json1_data: Dict[str, Any],
    json2_data: Dict[str, Any],
    base_prompt: str,
    chapter_name: str,
    model_name: str,
    progress_callback: Optional[Callable[[str], None]] = None,
    max_chunks: int = 50,
    stage_name: str = "third",
) -> Optional[Dict[str, Any]]:
    """
    Run third-stage processing in chunked mode using the given prompt
    and model, following the chunk JSON structure defined in the prompt.

    Args:
        api_client: Instance of GeminiAPIClient (must expose process_text).
        json1_data: Source JSON (Stage 1 output).
        json2_data: Incomplete output JSON (Stage 2 output).
        base_prompt: User's third-stage prompt (with {CHAPTER_NAME} already replaced).
        chapter_name: Chapter name string.
        model_name: Gemini model name.
        progress_callback: Optional callback for status messages.
        max_chunks: Safety limit on maximum allowed chunks.

    Returns:
        Combined hierarchical JSON:
        {
          "chapter": chapter_name,
          "content": [...]
        }
        or None on error.
    """

    def _status(msg: str) -> None:
        if progress_callback:
            progress_callback(msg)
        logger.info(msg)

    # Pre-render source and incomplete JSON once (they may be large)
    source_json_str = json.dumps(json1_data, ensure_ascii=False, indent=2)
    incomplete_json_str = json.dumps(json2_data, ensure_ascii=False, indent=2)

    all_content: List[Any] = []
    chunk_index: int = 1
    is_last: bool = False
    cursor: Optional[Dict[str, Any]] = None

    _status(f"Starting {stage_name}-stage chunked processing...")

    while not is_last and chunk_index <= max_chunks:
        _status(f"Processing {stage_name}-stage chunk {chunk_index}...")

        cursor_part = ""
        if cursor is not None:
            # Provide cursor to the model for continuation, as JSON
            cursor_part = (
                "\n\nPrevious cursor (for continuation – read-only, "
                "do NOT copy it verbatim into payload):\n"
                + json.dumps(cursor, ensure_ascii=False, indent=2)
            )

        # Build full prompt for this chunk.
        # NOTE: base_prompt already contains all detailed instructions
        # (chunk structure, rules, etc.) provided by the user.
        full_prompt = f"""{base_prompt}

==================================================
ورودی‌های این Chunk
==================================================

Source JSON (Source JSON کامل):
{source_json_str}

Incomplete Output JSON (خروجی ناقص فعلی – Incomplete Output):
{incomplete_json_str}

Chunk index (درخواستی): {chunk_index}
{cursor_part}
"""

        # Call the model
        try:
            response = api_client.process_text(
                text=full_prompt,
                model_name=model_name,
                temperature=0.7,
            )
        except Exception as e:
            logger.error(f"{stage_name.capitalize()}-stage chunk {chunk_index} call failed: {e}", exc_info=True)
            return None

        if not response:
            _status(f"❌ No response for chunk {chunk_index}")
            return None

        # Clean potential markdown fences
        cleaned = response.strip()
        if cleaned.startswith("```"):
            end_idx = cleaned.find("```", 3)
            if end_idx > 0:
                cleaned = cleaned[3:end_idx].strip()
                if cleaned.lower().startswith("json"):
                    cleaned = cleaned[4:].strip()

        # Parse chunk JSON
        try:
            chunk_obj = json.loads(cleaned)
        except json.JSONDecodeError as e:
            logger.error(f"Failed to parse JSON for {stage_name}-stage chunk {chunk_index}: {e}")
            logger.debug(f"Response text (first 500 chars): {cleaned[:500]}")
            return None

        if not isinstance(chunk_obj, Dict):
            logger.error(f"{stage_name.capitalize()}-stage chunk {chunk_index} JSON is not an object (type={type(chunk_obj).__name__})")
            return None

        # Validate required fields
        if "payload" not in chunk_obj:
            logger.error(f"{stage_name.capitalize()}-stage chunk {chunk_index} missing 'payload' field")
            return None

        # Read fields
        model_chunk_index = chunk_obj.get("chunk_index", chunk_index)
        try:
            model_chunk_index = int(model_chunk_index)
        except (ValueError, TypeError):
            model_chunk_index = chunk_index

        is_last = bool(chunk_obj.get("is_last", False))
        payload = chunk_obj.get("payload", {}) or {}
        next_cursor = chunk_obj.get("next_cursor")

        # Extract content from payload
        # Payload can be either:
        # - dict with a "content" field (preferred, Stage 3 style)
        # - or directly a list of content items (fallback / Stage 4 variations)
        if isinstance(payload, list):
            payload_content = payload
        elif isinstance(payload, dict):
            payload_content = payload.get("content", [])
        else:
            payload_content = []

        if isinstance(payload_content, list):
            all_content.extend(payload_content)
            _status(
                f"Chunk {model_chunk_index}: added {len(payload_content)} content item(s). "
                f"Total so far: {len(all_content)}"
            )
        elif payload_content:
            all_content.append(payload_content)
            _status(
                f"Chunk {model_chunk_index}: added 1 content item (non-list). "
                f"Total so far: {len(all_content)}"
            )
        else:
            _status(f"Chunk {model_chunk_index}: no content in payload.")

        # Prepare for next loop
        cursor = next_cursor if isinstance(next_cursor, dict) else None
        if is_last:
            _status(f"Received last chunk (index={model_chunk_index}).")
            break

        # Next chunk index (prefer model-provided index when valid)
        chunk_index = model_chunk_index + 1

    if not all_content:
        logger.error("No content collected from any chunk.")
        return None

    combined = {
        "chapter": chapter_name or json2_data.get("chapter", ""),
        "content": all_content,
    }

    _status(f"Third-stage chunked processing completed. Total content items: {len(all_content)}")
    return combined



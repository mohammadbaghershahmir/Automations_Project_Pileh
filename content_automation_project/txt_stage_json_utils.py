"""
Robust utilities for loading Stage 3 / Stage 4 TXT outputs as JSON.

These TXT files typically contain a markdown code block with JSON content:

    ```json
    { ... JSON ... }
    ```

However, in practice they may suffer from:
- Missing closing code fences
- Incomplete or truncated JSON (unterminated strings, missing braces/brackets)

This module provides a resilient loader that tries multiple strategies to
extract and repair the JSON content as much as possible.
"""

import json
import logging
import re
from pathlib import Path
from typing import Optional, Dict, Any

from third_stage_converter import ThirdStageConverter


logger = logging.getLogger(__name__)


def load_stage_txt_as_json(txt_path: str | Path) -> Optional[Dict[str, Any]]:
    """
    Robustly load a Stage 3/4 TXT file and return its JSON content as a dict.

    Strategy:
      1) Read the TXT file as UTF-8.
      2) Delegate JSON extraction/repair to ThirdStageConverter._extract_json_from_response,
         which already knows how to:
            - Handle markdown code fences (```json ... ```)
            - Extract inner JSON objects/arrays
            - Repair some forms of incomplete JSON
      3) If that fails, try a lightweight fallback:
            - Strip leading/trailing code fences
            - Take substring from first '{' to last '}'
            - Attempt json.loads

    Returns:
        Parsed JSON object (dict) or None on failure.
    """
    txt_path = Path(txt_path)

    if not txt_path.exists():
        logger.error("TXT file not found for JSON loading: %s", txt_path)
        return None

    try:
        text = txt_path.read_text(encoding="utf-8")
    except Exception as e:
        logger.error("Failed to read TXT file %s: %s", txt_path, e)
        return None

    if not text or not text.strip():
        logger.error("TXT file is empty or whitespace only: %s", txt_path)
        return None

    # Primary path: reuse robust JSON extraction from ThirdStageConverter
    try:
        converter = ThirdStageConverter()
        json_obj = converter._extract_json_from_response(text)
        if json_obj is not None:
            logger.info("Successfully extracted JSON from TXT using ThirdStageConverter for %s", txt_path)
            return json_obj
    except Exception as e:
        logger.warning("ThirdStageConverter JSON extraction failed for %s: %s", txt_path, e)

    # Fallback: manually strip code fences and parse inner JSON
    logger.info("Falling back to manual JSON extraction for %s", txt_path)

    # Remove ```json / ``` fences if present
    fence_pattern = re.compile(r"^```[a-zA-Z0-9_-]*\s*|\s*```$", re.MULTILINE)
    cleaned = fence_pattern.sub("", text).strip()

    # Try to extract complete objects from array (for incomplete JSON)
    array_start = cleaned.find("[")
    if array_start != -1:
        objects = []
        obj_start = -1
        brace_count = 0
        in_string = False
        escape_next = False
        
        for i, char in enumerate(cleaned[array_start:], start=array_start):
            if escape_next:
                escape_next = False
                continue
            if char == "\\":
                escape_next = True
                continue
            if char == '"' and not escape_next:
                in_string = not in_string
            if not in_string:
                if char == "{":
                    if brace_count == 0:
                        obj_start = i
                    brace_count += 1
                elif char == "}":
                    brace_count -= 1
                    if brace_count == 0 and obj_start != -1:
                        obj_str = cleaned[obj_start:i+1]
                        try:
                            obj = json.loads(obj_str)
                            objects.append(obj)
                        except Exception:
                            pass
                        obj_start = -1
        
        if objects:
            logger.info("Extracted %d complete objects from incomplete JSON array for %s", len(objects), txt_path)
            return {"data": objects}

    # Fallback: try to parse as single object
    start = cleaned.find("{")
    end = cleaned.rfind("}")
    if start == -1 or end == -1 or end <= start:
        logger.error("No JSON object boundaries found in TXT file: %s", txt_path)
        return None

    candidate = cleaned[start : end + 1]

    try:
        json_obj = json.loads(candidate)
        logger.info("Successfully parsed JSON from cleaned TXT content for %s", txt_path)
        return json_obj
    except json.JSONDecodeError as e:
        logger.error("Failed to parse JSON from cleaned TXT content for %s: %s", txt_path, e)
        return None





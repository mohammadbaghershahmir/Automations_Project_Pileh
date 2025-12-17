"""
Independent converter for third stage output files.
Converts the model response (with hierarchical JSON structure) to flat JSON with PointId.
"""

import json
import logging
import os
import re
from typing import Dict, Any, List, Optional
from datetime import datetime


class ThirdStageConverter:
    """Convert third stage output to flat JSON with PointId."""

    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def convert_third_stage_file(
        self,
        input_path: str,
        book_id: int = 1,
        chapter_id: int = 1,
        start_index: int = 1,
        output_path: Optional[str] = None
    ) -> Optional[str]:
        """
        Convert third stage output file to flat JSON with PointId.

        Args:
            input_path: Path to third stage output JSON file
            book_id: Book ID for PointId generation (default: 1)
            chapter_id: Chapter ID for PointId generation (default: 1)
            start_index: Starting index for PointId generation (default: 1)
            output_path: Optional output path. If None, auto-generates from input path.

        Returns:
            Path to converted JSON file, or None on error.
        """
        if not os.path.exists(input_path):
            self.logger.error(f"Input file not found: {input_path}")
            return None

        self.logger.info(f"Loading third stage file: {input_path}")
        try:
            with open(input_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            self.logger.error(f"Failed to load input file: {e}")
            return None

        # Extract response field
        response_text = data.get("response", "")
        if not response_text:
            self.logger.error("No 'response' field found in input file")
            return None

        self.logger.info("Extracting JSON from response field...")
        # Extract JSON from markdown code block
        json_data = self._extract_json_from_response(response_text)
        if not json_data:
            self.logger.error("Failed to extract JSON from response")
            return None

        self.logger.info("Flattening hierarchical structure to points...")
        # Flatten hierarchical structure to points
        flat_rows = self._flatten_to_points(json_data)

        if not flat_rows:
            self.logger.warning("No points extracted from JSON structure")
            return None

        self.logger.info(f"Extracted {len(flat_rows)} points. Generating PointIds...")
        # Generate PointId for each row
        current_index = start_index
        for row in flat_rows:
            point_id = f"{book_id:03d}{chapter_id:03d}{current_index:04d}"
            row["PointId"] = point_id
            current_index += 1

        # Extract chapter name from JSON if available
        chapter_name = ""
        try:
            with open(input_path, "r", encoding="utf-8") as f:
                input_data = json.load(f)
            response_text = input_data.get("response", "")
            json_data_temp = self._extract_json_from_response(response_text)
            if json_data_temp:
                # Support both English and Persian keys
                chapter_name = json_data_temp.get("chapter") or json_data_temp.get("فصل", "")
        except:
            pass

        # Build final output
        output_data = {
            "metadata": {
                "chapter": chapter_name,
                "book_id": book_id,
                "chapter_id": chapter_id,
                "start_point_index": start_index,
                "total_points": len(flat_rows),
                "processed_at": datetime.now().isoformat(),
                "source_file": os.path.basename(input_path),
            },
            "points": flat_rows,
        }

        # Determine output path
        if not output_path:
            base_dir = os.path.dirname(input_path) or os.getcwd()
            base_name, _ = os.path.splitext(os.path.basename(input_path))
            output_path = os.path.join(base_dir, f"{base_name}_converted.json")

        # Save output
        try:
            self.logger.info(f"Saving converted JSON to: {output_path}")
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(output_data, f, ensure_ascii=False, indent=2)
            self.logger.info(f"Successfully converted and saved: {output_path}")
            return output_path
        except Exception as e:
            self.logger.error(f"Failed to save converted JSON: {e}")
            return None

    def _extract_json_from_response(self, response_text: str) -> Optional[Dict[str, Any]]:
        """
        Extract JSON from response text (may contain markdown code blocks).

        Returns:
            Parsed JSON object or None on error.
        """
        if not response_text:
            return None

        # If response_text is already a JSON string (escaped), try to unescape it first
        try:
            # Check if it's a JSON-encoded string
            if response_text.startswith('"') and response_text.endswith('"'):
                unescaped = json.loads(response_text)
                if isinstance(unescaped, str):
                    response_text = unescaped
        except:
            pass

        # Try to find JSON in markdown code blocks
        # First, check if there's an opening ```json marker
        start_marker = '```json'
        start_idx = response_text.find(start_marker)
        
        if start_idx != -1:
            # Found opening marker, extract from after it
            json_start = start_idx + len(start_marker)
            # Look for closing marker
            end_marker = '```'
            end_idx = response_text.find(end_marker, json_start)
            
            if end_idx != -1:
                # Found closing marker
                json_str = response_text[json_start:end_idx].strip()
            else:
                # No closing marker - JSON is incomplete, extract everything after opening
                json_str = response_text[json_start:].strip()
                self.logger.warning("No closing markdown marker found, JSON may be incomplete")
        else:
            # Try without json identifier
            start_marker = '```'
            start_idx = response_text.find(start_marker)
            if start_idx != -1:
                json_start = start_idx + len(start_marker)
                end_idx = response_text.find('```', json_start)
                if end_idx != -1:
                    json_str = response_text[json_start:end_idx].strip()
                else:
                    json_str = response_text[json_start:].strip()
                    self.logger.warning("No closing markdown marker found, JSON may be incomplete")
            else:
                # No markdown markers, try direct JSON parse
                json_str = response_text.strip()

        # Try to parse JSON
        try:
            parsed = json.loads(json_str)
            self.logger.debug("Successfully parsed JSON directly")
            return parsed
        except json.JSONDecodeError as e:
            self.logger.warning(f"Direct JSON parse failed: {e}. Trying fallback extraction...")
            self.logger.debug(f"JSON string length: {len(json_str)}, first 200 chars: {json_str[:200]}")
            
            # Try to extract JSON object/array
            start_obj = json_str.find("{")
            start_arr = json_str.find("[")
            
            if start_obj != -1 and (start_arr == -1 or start_obj < start_arr):
                end_obj = json_str.rfind("}")
                if end_obj > start_obj:
                    candidate = json_str[start_obj:end_obj + 1]
                    self.logger.debug(f"Trying to parse extracted object (length: {len(candidate)})")
                    try:
                        parsed = json.loads(candidate)
                        self.logger.info("Successfully parsed JSON using object extraction")
                        return parsed
                    except json.JSONDecodeError as e2:
                        self.logger.warning(f"Object extraction parse failed: {e2}")
                        # Try progressive extraction - find the largest valid JSON prefix
                        try:
                            balanced = self._extract_largest_valid_json(candidate)
                            if balanced:
                                parsed = json.loads(balanced)
                                self.logger.info(f"Successfully parsed JSON using progressive extraction (extracted {len(balanced)} chars from {len(candidate)})")
                                return parsed
                        except Exception as e3:
                            self.logger.debug(f"Progressive extraction also failed: {e3}")
                        
                        # Last resort: try to extract valid JSON by finding last complete structure
                        try:
                            self.logger.info("Attempting to repair incomplete JSON...")
                            repaired = self._repair_incomplete_json(candidate)
                            if repaired:
                                parsed = json.loads(repaired)
                                self.logger.info("Successfully parsed JSON using repair method")
                                return parsed
                            else:
                                self.logger.warning("Repair method returned None")
                        except Exception as e4:
                            self.logger.warning(f"JSON repair also failed: {e4}", exc_info=True)
            elif start_arr != -1:
                end_arr = json_str.rfind("]")
                if end_arr > start_arr:
                    candidate = json_str[start_arr:end_arr + 1]
                    self.logger.debug(f"Trying to parse extracted array (length: {len(candidate)})")
                    try:
                        parsed = json.loads(candidate)
                        self.logger.info("Successfully parsed JSON using array extraction")
                        return parsed
                    except json.JSONDecodeError as e2:
                        self.logger.warning(f"Array extraction parse failed: {e2}")

        self.logger.error("All JSON extraction methods failed")
        return None

    def _extract_balanced_json(self, text: str, max_pos: int) -> Optional[str]:
        """Try to extract balanced JSON up to a certain position."""
        if not text or max_pos <= 0:
            return None
        
        # Find the opening brace
        start = text.find("{")
        if start == -1:
            return None
        
        # Try to find balanced braces up to max_pos
        depth = 0
        in_string = False
        escape_next = False
        last_balanced = start
        
        for i in range(start, min(max_pos, len(text))):
            char = text[i]
            
            if escape_next:
                escape_next = False
                continue
            
            if char == '\\':
                escape_next = True
                continue
            
            if char == '"' and not escape_next:
                in_string = not in_string
                continue
            
            if in_string:
                continue
            
            if char == '{':
                depth += 1
            elif char == '}':
                depth -= 1
                if depth == 0:
                    last_balanced = i
        
        if last_balanced > start:
            return text[start:last_balanced + 1]
        
        return None

    def _repair_incomplete_json(self, text: str) -> Optional[str]:
        """
        Try to repair incomplete JSON by finding the last complete string
        and closing any unclosed brackets/braces.
        """
        if not text:
            return None
        
        start = text.find("{")
        if start == -1:
            return None
        
        # Find the last complete value by scanning forward
        # We need to find the last complete array element or object property
        in_string = False
        escape_next = False
        last_string_end = start
        depth = 0
        array_depth = 0
        
        for i, char in enumerate(text[start:], start):
            if escape_next:
                escape_next = False
                continue
            
            if char == '\\':
                escape_next = True
                continue
            
            if char == '"' and not escape_next:
                if in_string:
                    # String closed - this could be end of a value
                    # Check if we're in a valid context (not in a key)
                    # A value ends with: ", " or ", \n" or "]" or "}"
                    # Look ahead to see what comes after this quote
                    if i + 1 < len(text):
                        next_chars = text[i+1:i+4].strip()
                        if next_chars.startswith(',') or next_chars.startswith(']') or next_chars.startswith('}'):
                            last_string_end = i
                in_string = not in_string
                continue
            
            if in_string:
                continue
            
            # We're outside a string
            if char == '{':
                depth += 1
            elif char == '}':
                depth -= 1
                if depth == 0:
                    last_string_end = i
            elif char == '[':
                array_depth += 1
            elif char == ']':
                array_depth -= 1
                if array_depth == 0 and depth == 0:
                    last_string_end = i
        
        if last_string_end > start:
            # Extract up to the last complete value
            # But we need to make sure we're at a valid boundary
            # Look for the last complete array element or object property
            extracted = text[start:last_string_end + 1]
            
            # Find the last ']' that's not inside a string
            last_array_end = -1
            in_str = False
            escape = False
            for i in range(len(extracted) - 1, -1, -1):
                if escape:
                    escape = False
                    continue
                if extracted[i] == '\\':
                    escape = True
                    continue
                if extracted[i] == '"':
                    in_str = not in_str
                    continue
                if not in_str and extracted[i] == ']':
                    last_array_end = i
                    break
            
            if last_array_end > 0:
                # Extract up to the last complete array
                extracted = extracted[:last_array_end + 1]
            
            # Count unclosed brackets/braces
            open_braces = extracted.count('{') - extracted.count('}')
            open_brackets = extracted.count('[') - extracted.count(']')
            
            self.logger.debug(f"Found last value at position {last_string_end}, open_braces={open_braces}, open_brackets={open_brackets}")
            
            # Close in reverse order (arrays first, then objects)
            if open_brackets > 0:
                extracted += ']' * open_brackets
            if open_braces > 0:
                extracted += '}' * open_braces
            
            try:
                parsed = json.loads(extracted)
                self.logger.info(f"Successfully repaired JSON (closed {open_brackets} brackets, {open_braces} braces)")
                return extracted
            except json.JSONDecodeError as e:
                self.logger.debug(f"Repair attempt failed: {e}")
                # Try to find a better cut point by looking for the last complete array/object
                pass
        
        # Fallback: find last complete structure by tracking depth
        depth = 0
        in_string = False
        escape_next = False
        last_valid_pos = start
        
        for i, char in enumerate(text[start:], start):
            if escape_next:
                escape_next = False
                continue
            
            if char == '\\':
                escape_next = True
                continue
            
            if char == '"' and not escape_next:
                in_string = not in_string
                continue
            
            if in_string:
                continue
            
            if char == '{':
                depth += 1
            elif char == '}':
                depth -= 1
                if depth == 0:
                    last_valid_pos = i
                elif depth < 0:
                    break
        
        # If we found a complete structure, use it
        if last_valid_pos > start:
            extracted = text[start:last_valid_pos + 1]
            open_braces = extracted.count('{') - extracted.count('}')
            open_brackets = extracted.count('[') - extracted.count(']')
            
            if open_brackets > 0:
                extracted += ']' * open_brackets
            if open_braces > 0:
                extracted += '}' * open_braces
            
            try:
                parsed = json.loads(extracted)
                return extracted
            except:
                pass
        
        return None

    def _extract_largest_valid_json(self, text: str) -> Optional[str]:
        """
        Extract the largest valid JSON from potentially incomplete text.
        Works backwards from the end, trying progressively shorter prefixes.
        """
        if not text:
            return None
        
        start = text.find("{")
        if start == -1:
            return None
        
        # Try progressively shorter prefixes
        # Start from end and work backwards
        max_length = len(text) - start
        min_length = 100  # Minimum reasonable JSON length
        
        # Binary search for the largest valid JSON
        left = min_length
        right = max_length
        
        best_result = None
        
        while left <= right:
            mid = (left + right) // 2
            candidate = text[start:start + mid]
            
            # Try to balance and parse
            try:
                # First, try to balance brackets
                balanced = self._balance_json_brackets(candidate)
                if balanced:
                    parsed = json.loads(balanced)
                    # Valid JSON found, try longer
                    best_result = balanced
                    left = mid + 1
                else:
                    # Invalid, try shorter
                    right = mid - 1
            except json.JSONDecodeError:
                # Invalid, try shorter
                right = mid - 1
        
        return best_result

    def _balance_json_brackets(self, text: str) -> Optional[str]:
        """
        Try to balance JSON brackets by finding the last complete structure.
        """
        if not text:
            return None
        
        depth = 0
        in_string = False
        escape_next = False
        last_balanced_pos = -1
        
        for i, char in enumerate(text):
            if escape_next:
                escape_next = False
                continue
            
            if char == '\\':
                escape_next = True
                continue
            
            if char == '"' and not escape_next:
                in_string = not in_string
                continue
            
            if in_string:
                continue
            
            if char == '{':
                depth += 1
            elif char == '}':
                depth -= 1
                if depth == 0:
                    last_balanced_pos = i
                elif depth < 0:
                    # Unbalanced closing brace
                    break
        
        if last_balanced_pos > 0:
            result = text[:last_balanced_pos + 1]
            # Try to close any unclosed arrays
            open_arrays = result.count('[') - result.count(']')
            if open_arrays > 0:
                result += ']' * open_arrays
            return result
        
        # If no balanced structure found, try to close manually
        open_braces = text.count('{') - text.count('}')
        if open_braces > 0:
            # Try adding closing braces
            candidate = text + '}' * open_braces
            try:
                json.loads(candidate)
                return candidate
            except:
                pass
        
        return None

    def _flatten_to_points(self, json_data: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Flatten hierarchical JSON structure to flat list of points.

        Expected structure:
        {
            "chapter": "...",
            "content": [
                {
                    "level_1": "...",
                    "children": [
                        {
                            "level_2": "...",
                            "children": [
                                {
                                    "level_3": "...",
                                    "children": [
                                        {
                                            "level_4": "...",
                                            "children": [
                                                {
                                                    "level_5": "...",
                                                    "points": ["...", "..."]
                                                }
                                            ]
                                        }
                                    ]
                                }
                            ]
                        }
                    ]
                }
            ]
        }

        Returns:
            List of flat point dictionaries.
        """
        flat_rows = []

        def walk(
            item: Dict[str, Any],
            level_1: Optional[str],
            level_2: Optional[str],
            level_3: Optional[str],
            level_4: Optional[str],
            level_5: Optional[str],
            chapter_name: str
        ):
            """Recursively walk the hierarchical structure."""
            if not isinstance(item, dict):
                return

            # Clean label function (remove prefixes)
            def clean_label(text: str) -> str:
                if not isinstance(text, str):
                    return ""
                prefixes = ["فصل:", "زیرفصل:", "مبحث:", "عنوان:", "زیرعنوان:"]
                for pref in prefixes:
                    if text.startswith(pref):
                        return text[len(pref):].strip()
                return text.strip()

            # Extract level values - support both English and Persian keys
            # English keys: level_1, level_2, etc.
            # Persian keys: فصل, زیرفصل, مبحث, عنوان, زیرعنوان
            level_1_val = item.get("level_1") or item.get("فصل") or level_1 or ""
            level_2_val = item.get("level_2") or item.get("زیرفصل") or level_2 or ""
            level_3_val = item.get("level_3") or item.get("مبحث") or level_3 or ""
            level_4_val = item.get("level_4") or item.get("عنوان") or level_4 or ""
            level_5_val = item.get("level_5") or item.get("زیرعنوان") or level_5 or ""
            
            # Clean extracted values
            current_level_1 = clean_label(level_1_val)
            current_level_2 = clean_label(level_2_val)
            current_level_3 = clean_label(level_3_val)
            current_level_4 = clean_label(level_4_val)
            current_level_5 = clean_label(level_5_val)

            # If this item has points, create rows for each point
            if "points" in item and isinstance(item["points"], list):
                for point_text in item["points"]:
                    if point_text and isinstance(point_text, str):
                        # Clean point text (remove bullet point if present)
                        cleaned_point = point_text.lstrip("•").strip()
                        
                        row = {
                            "chapter": chapter_name or "",
                            "subchapter": current_level_2,
                            "topic": current_level_3,
                            "subtopic": current_level_4,
                            "subsubtopic": current_level_5,
                            "points": cleaned_point,
                        }
                        flat_rows.append(row)

            # Recursively process children
            if "children" in item and isinstance(item["children"], list):
                for child in item["children"]:
                    walk(
                        child,
                        current_level_1,
                        current_level_2,
                        current_level_3,
                        current_level_4,
                        current_level_5,
                        chapter_name
                    )

        # Start processing - support both English and Persian structures
        # English: {"chapter": "...", "content": [...]}
        # Persian: {"فصل": "...", "children": [...]} or just {"children": [...]}
        chapter = json_data.get("chapter") or json_data.get("فصل", "")
        content = json_data.get("content") or json_data.get("children", [])

        # If content is empty but we have children at root level, use that
        if not content and "children" in json_data:
            content = json_data["children"]

        if not isinstance(content, list):
            self.logger.warning("Content/children is not a list, trying to process as single object")
            content = [content] if content else []

        self.logger.info(f"Processing chapter: '{chapter}' with {len(content)} top-level items")
        
        for item in content:
            walk(item, None, None, None, None, None, chapter)

        return flat_rows


def convert_third_stage_file(
    input_path: str,
    book_id: int = 1,
    chapter_id: int = 1,
    start_index: int = 1,
    output_path: Optional[str] = None
) -> Optional[str]:
    """
    Standalone function to convert third stage file.

    Args:
        input_path: Path to third stage output JSON file
        book_id: Book ID for PointId generation (default: 1)
        chapter_id: Chapter ID for PointId generation (default: 1)
        start_index: Starting index for PointId generation (default: 1)
        output_path: Optional output path. If None, auto-generates from input path.

    Returns:
        Path to converted JSON file, or None on error.
    """
    converter = ThirdStageConverter()
    return converter.convert_third_stage_file(
        input_path=input_path,
        book_id=book_id,
        chapter_id=chapter_id,
        start_index=start_index,
        output_path=output_path
    )


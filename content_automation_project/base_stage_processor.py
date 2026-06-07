"""
Base class for all stage processors.
Provides common functionality for JSON handling, file operations, and PointId management.
"""

import copy
import json
import logging
import os
import re
import time
from datetime import datetime
from typing import Optional, Dict, List, Any

# #region agent log
_AGENT_DEBUG_LOG_PATH = "/Users/mehrad/MyData/Code/Automations_Project_Pileh/content_automation_project/.cursor/debug-24d820.log"


def _agent_debug_log(location: str, message: str, data: dict, hypothesis_id: str, run_id: str = "pre-fix") -> None:
    try:
        payload = {
            "sessionId": "24d820",
            "runId": run_id,
            "hypothesisId": hypothesis_id,
            "location": location,
            "message": message,
            "data": data,
            "timestamp": int(time.time() * 1000),
        }
        with open(_AGENT_DEBUG_LOG_PATH, "a", encoding="utf-8") as _df:
            _df.write(json.dumps(payload, ensure_ascii=False) + "\n")
    except OSError:
        pass


def _agent_count_ocr_figures_in_slice(ocr_slice: Dict[str, Any]) -> dict:
    list_figs = 0
    dict_figs = 0
    topics_seen: List[str] = []
    for chapter_obj in ocr_slice.get("chapters", []) or []:
        if not isinstance(chapter_obj, dict):
            continue
        for sub in chapter_obj.get("subchapters", []) or []:
            if not isinstance(sub, dict):
                continue
            for topic_obj in sub.get("topics", []) or []:
                if not isinstance(topic_obj, dict):
                    continue
                tname = (topic_obj.get("topic") or "").strip()
                if tname:
                    topics_seen.append(tname)
                ex = topic_obj.get("extractions")
                if isinstance(ex, list):
                    for item in ex:
                        if isinstance(item, dict):
                            type_val = str(item.get("type") or item.get("Type") or "")
                            x = type_val.strip().lower().replace(" ", "")
                            if x in ("figure", "e-figure", "efigure", "fig", "image", "e-image", "eimage") or (
                                x.startswith("e-") and "fig" in x
                            ):
                                list_figs += 1
                elif isinstance(ex, dict):
                    for key in ("figs", "figures", "images"):
                        vals = ex.get(key)
                        if isinstance(vals, list):
                            dict_figs += len(vals)
    return {
        "list_figure_extractions": list_figs,
        "dict_figure_extractions": dict_figs,
        "ocr_topics_in_slice": topics_seen[:20],
        "subchapters_in_slice": len(
            [
                s
                for c in ocr_slice.get("chapters", []) or []
                if isinstance(c, dict)
                for s in c.get("subchapters", []) or []
                if isinstance(s, dict)
            ]
        ),
    }


# #endregion
from third_stage_converter import ThirdStageConverter
from txt_stage_json_utils import load_stage_txt_as_json


class BaseStageProcessor:
    """Base class providing common functionality for all stage processors"""

    @staticmethod
    def _normalize_topic_label(label: str) -> str:
        return re.sub(r"\s+", " ", (label or "").strip())

    @classmethod
    def _ocr_topic_labels_match(cls, ocr_topic: Any, stage4_topic: str) -> bool:
        """Match OCR topic objects to Stage 4 topic names (incl. empty / بدون مبحث)."""
        ocr_label = cls._normalize_topic_label(str(ocr_topic or ""))
        stage_label = cls._normalize_topic_label(stage4_topic or "")
        if not stage_label or stage_label == "(بدون مبحث)":
            return not ocr_label
        if ocr_label == stage_label:
            return True
        return re.sub(r"\s+", "", ocr_label) == re.sub(r"\s+", "", stage_label)

    @staticmethod
    def _ocr_extraction_item_type(item: Dict[str, Any]) -> str:
        if not isinstance(item, dict):
            return ""
        return str(item.get("type") or item.get("Type") or "")
    
    def __init__(self, api_client):
        """
        Initialize base processor.
        
        Args:
            api_client: GeminiAPIClient instance for API calls
        """
        self.api_client = api_client
        self.logger = logging.getLogger(self.__class__.__name__)
        self.converter = ThirdStageConverter()
    
    def extract_json_blocks_from_text(self, text: str) -> List[Dict[str, Any]]:
        """
        Extract JSON blocks from a single text response (same as document processing).
        Handles markdown code blocks, escaped JSON strings, and direct JSON.
        This is the same conversion method used in document processing.
        
        Args:
            text: Raw response text from model
            
        Returns:
            List of JSON objects extracted from the text
        """
        if not text:
            return []
        
        json_blocks = []
        
        # Strategy 1: Try to extract from markdown code blocks
        # First try: ```json ... ```
        json_block_patterns = [
            r'```json\s*(.*?)\s*```',  # ```json ... ```
            r'```\s*(.*?)\s*```',       # ``` ... ```
        ]
        
        matches = []
        for pattern in json_block_patterns:
            found_matches = re.findall(pattern, text, re.DOTALL)
            if found_matches:
                matches = found_matches
                self.logger.info(f"Found {len(matches)} JSON block(s) using pattern: {pattern}")
                self.logger.debug(f"First match content: {repr(matches[0][:200]) if matches else 'N/A'}")
                break
        
        # Strategy 2: If no code blocks found, try to extract JSON directly from text
        if not matches:
            # Try to find JSON object/array in the text
            start_obj = text.find('{')
            start_arr = text.find('[')
            if start_obj != -1 or start_arr != -1:
                start = start_obj if (start_obj != -1 and (start_arr == -1 or start_obj < start_arr)) else start_arr
                # Find matching closing bracket
                if start_obj != -1 and (start_arr == -1 or start_obj < start_arr):
                    end = text.rfind('}')
                    if end > start:
                        candidate = text[start:end + 1]
                        matches = [candidate]
                        self.logger.debug("Found JSON object in text (no code blocks)")
                elif start_arr != -1:
                    end = text.rfind(']')
                    if end > start:
                        candidate = text[start:end + 1]
                        matches = [candidate]
                        self.logger.debug("Found JSON array in text (no code blocks)")
        
        # Strategy 3: If text is an escaped JSON string (like in raw_responses), try to unescape it
        if not matches:
            try:
                # Check if it's a JSON-encoded string
                if text.startswith('"') and text.endswith('"'):
                    unescaped = json.loads(text)
                    if isinstance(unescaped, str):
                        # Recursively try to extract from unescaped string
                        return self.extract_json_blocks_from_text(unescaped)
            except:
                pass
        
        # Parse each match
        for match in matches:
            json_str = match.strip()
            if not json_str:
                continue
            
            # Try to parse JSON
            try:
                json_obj = json.loads(json_str)
                json_blocks.append(json_obj)
                self.logger.info(f"Successfully parsed JSON block (type: {type(json_obj).__name__})")
                if isinstance(json_obj, dict):
                    self.logger.info(f"  JSON object keys: {list(json_obj.keys())}")
                elif isinstance(json_obj, list):
                    self.logger.info(f"  JSON array length: {len(json_obj)}")
            except json.JSONDecodeError as e:
                self.logger.warning(f"JSON parse failed: {e}. Trying fallback extraction...")
                # Try fallback extraction - use balanced bracket matching
                json_obj = self._extract_json_with_balanced_brackets(json_str)
                if json_obj:
                    json_blocks.append(json_obj)
                    self.logger.info("Successfully parsed JSON using balanced bracket matching")
                else:
                    # Try simple fallback - find largest valid JSON object/array
                    start_obj = json_str.find("{")
                    start_arr = json_str.find("[")
                    
                    if start_obj != -1 and (start_arr == -1 or start_obj < start_arr):
                        end_obj = json_str.rfind("}")
                        if end_obj > start_obj:
                            try:
                                candidate = json_str[start_obj:end_obj + 1]
                                json_obj = json.loads(candidate)
                                json_blocks.append(json_obj)
                                self.logger.debug("Successfully parsed JSON using fallback extraction (object)")
                            except json.JSONDecodeError:
                                self.logger.warning(f"Fallback extraction failed for JSON object")
                    elif start_arr != -1:
                        end_arr = json_str.rfind("]")
                        if end_arr > start_arr:
                            try:
                                candidate = json_str[start_arr:end_arr + 1]
                                json_obj = json.loads(candidate)
                                json_blocks.append(json_obj)
                                self.logger.debug("Successfully parsed JSON using fallback extraction (array)")
                            except json.JSONDecodeError:
                                self.logger.warning(f"Fallback extraction failed for JSON array")
        
        if json_blocks:
            self.logger.info(f"Extracted {len(json_blocks)} JSON block(s) from response")
        else:
            self.logger.warning("No JSON blocks found in response")
        
        return json_blocks
    
    def _extract_json_with_balanced_brackets(self, text: str) -> Optional[Dict | List]:
        """
        Extract JSON using balanced bracket matching to handle incomplete JSON.
        This method finds the largest valid JSON object/array by matching brackets.
        Handles incomplete JSON by finding the last complete item in an array.
        
        Args:
            text: Text containing JSON (possibly incomplete)
            
        Returns:
            Parsed JSON object/array or None on error
        """
        if not text or not text.strip():
            return None
        
        # Find start of JSON (first { or [)
        start_obj = text.find('{')
        start_arr = text.find('[')
        
        if start_obj == -1 and start_arr == -1:
            return None
        
        # Determine which to use (prefer object if both exist and object comes first)
        if start_obj != -1 and (start_arr == -1 or start_obj < start_arr):
            # Extract JSON object
            end_pos = self._find_balanced_json_end(text, start_obj, '{', '}')
            if end_pos != -1:
                candidate = text[start_obj:end_pos + 1]
                try:
                    return json.loads(candidate)
                except json.JSONDecodeError:
                    # Try to fix common issues
                    # Remove trailing commas before closing braces/brackets
                    import re
                    candidate = re.sub(r',\s*([}\]])', r'\1', candidate)
                    # Fix unclosed strings (common in truncated JSON)
                    candidate = re.sub(r':\s*"([^"]*)$', r': ""', candidate)
                    candidate = re.sub(r':\s*"([^"]*)\n', r': ""', candidate)
                    try:
                        return json.loads(candidate)
                    except json.JSONDecodeError:
                        return None
        elif start_arr != -1:
            # Extract JSON array - handle incomplete arrays
            end_pos = self._find_balanced_json_end(text, start_arr, '[', ']')
            if end_pos != -1:
                candidate = text[start_arr:end_pos + 1]
                try:
                    return json.loads(candidate)
                except json.JSONDecodeError:
                    # Try to fix common issues
                    import re
                    candidate = re.sub(r',\s*([}\]])', r'\1', candidate)
                    # Fix unclosed strings
                    candidate = re.sub(r':\s*"([^"]*)$', r': ""', candidate)
                    candidate = re.sub(r':\s*"([^"]*)\n', r': ""', candidate)
                    try:
                        return json.loads(candidate)
                    except json.JSONDecodeError:
                        # If still fails, try to extract partial array (find last complete item)
                        # This handles cases where JSON is truncated mid-item
                        return self._extract_partial_json_array(text, start_arr)
        
        return None
    
    def _extract_partial_json_array(self, text: str, start_pos: int) -> Optional[List]:
        """
        Extract partial JSON array by finding the last complete item.
        Useful when JSON is truncated mid-item.
        
        Args:
            text: Text containing JSON array
            start_pos: Position where array starts
            
        Returns:
            List of complete items or None
        """
        if start_pos < 0 or start_pos >= len(text):
            return None
        
        # Find all complete objects in the array
        items = []
        i = start_pos + 1  # Skip opening [
        n = len(text)
        
        while i < n:
            # Skip whitespace
            while i < n and text[i] in ' \n\t\r':
                i += 1
            
            if i >= n or text[i] == ']':
                break
            
            # Try to find a complete object
            if text[i] == '{':
                obj_end = self._find_balanced_json_end(text, i, '{', '}')
                if obj_end != -1:
                    try:
                        obj_str = text[i:obj_end + 1]
                        obj = json.loads(obj_str)
                        items.append(obj)
                        i = obj_end + 1
                        # Skip comma
                        while i < n and text[i] in ' \n\t\r,':
                            i += 1
                        continue
                    except json.JSONDecodeError:
                        pass
            
            # If we can't find a complete object, break
            break
        
        if items:
            self.logger.info(f"Extracted {len(items)} complete items from partial JSON array")
            return items
        
        return None
    
    def _find_balanced_json_end(self, text: str, start: int, open_char: str, close_char: str) -> int:
        """
        Find the index of the matching closing bracket/brace, respecting strings and nesting.
        open_char is '{' or '[', close_char is '}' or ']'.
        Returns -1 if not found.
        """
        depth = 0
        i = start
        in_string = False
        escape_next = False
        quote_char = None
        n = len(text)
        
        while i < n:
            c = text[i]
            if escape_next:
                escape_next = False
                i += 1
                continue
            if c == '\\' and in_string:
                escape_next = True
                i += 1
                continue
            if c in ('"', "'") and not escape_next:
                if not in_string:
                    in_string = True
                    quote_char = c
                elif c == quote_char:
                    in_string = False
                i += 1
                continue
            if in_string:
                i += 1
                continue
            if c == open_char:
                depth += 1
            elif c == close_char:
                depth -= 1
                if depth == 0:
                    return i
            i += 1
        
        return -1
    
    def extract_json_from_response(self, response_text: str) -> Optional[Dict | List]:
        """
        Extract JSON from model response (for DeepSeek stages E-Y).
        Uses the same conversion method as document processing (extract_json_blocks_from_text).
        Returns first JSON block if multiple blocks found, or None if no blocks found.
        
        This method is for DeepSeek stages (E, F, J, H, V, M, L, X, Y, Z).
        For Google/Gemini stages (pre_ocr_topic, ocr_extraction), use extract_json_from_response_google().
        
        Args:
            response_text: Raw response text from model
            
        Returns:
            Parsed JSON dictionary, list, or None on error
        """
        if not response_text or not response_text.strip():
            self.logger.warning("Empty response text provided")
            return None
        
        # Use the same extraction method as document processing (ONLY this method, no fallback)
        json_blocks = self.extract_json_blocks_from_text(response_text)
        
        if json_blocks:
            # Return first block (or combine if multiple blocks)
            if len(json_blocks) == 1:
                self.logger.info("Successfully extracted JSON from response using document processing method (1 block)")
                return json_blocks[0]
            else:
                # Multiple blocks - return first block
                self.logger.info(f"Extracted {len(json_blocks)} JSON blocks from response using document processing method, returning first block")
                return json_blocks[0]
        
        # No JSON blocks found - try ThirdStageConverter as fallback
        self.logger.warning("Failed to extract JSON from response using document processing method, trying ThirdStageConverter fallback...")
        try:
            json_obj = self.converter._extract_json_from_response(response_text)
            if json_obj:
                self.logger.info("Successfully extracted JSON using ThirdStageConverter fallback")
                return json_obj
        except Exception as e:
            self.logger.warning(f"ThirdStageConverter fallback also failed: {e}")
        
        return None
    
    def extract_json_from_response_google(self, response_text: str) -> Optional[Dict | List]:
        """
        Extract JSON from Google/Gemini model response (original method).
        Uses ThirdStageConverter for robust extraction (original method).
        
        This method is for Google/Gemini stages (pre_ocr_topic, ocr_extraction).
        DeepSeek stages (E-Y) should use extract_json_from_response() instead.
        
        Args:
            response_text: Raw response text from model
            
        Returns:
            Parsed JSON dictionary, list, or None on error
        """
        if not response_text or not response_text.strip():
            self.logger.warning("Empty response text provided")
            return None
        
        # Use original ThirdStageConverter method (for Google/Gemini)
        try:
            json_obj = self.converter._extract_json_from_response(response_text)
            if json_obj is not None:
                self.logger.info("Successfully extracted JSON from response using Google/Gemini method")
                return json_obj
            else:
                self.logger.warning("Failed to extract JSON from response")
                return None
        except Exception as e:
            self.logger.error(f"Error extracting JSON from response: {e}")
            return None
    
    def load_json_file(self, file_path: str) -> Optional[Dict]:
        """
        Load JSON file with comprehensive error handling.
        
        Args:
            file_path: Path to JSON file
            
        Returns:
            Parsed JSON dictionary or None on error
        """
        if not file_path or not file_path.strip():
            self.logger.error("Empty file path provided")
            return None
        
        if not os.path.exists(file_path):
            self.logger.error(f"File not found: {file_path}")
            return None
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            self.logger.info(f"Successfully loaded JSON: {file_path}")
            return data
        except json.JSONDecodeError as e:
            self.logger.error(f"Invalid JSON in {file_path}: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Error loading {file_path}: {e}")
            return None

    def _filter_ocr_extraction_for_subchapter(
        self,
        ocr_extraction_data: Dict[str, Any],
        subchapter_name: str,
    ) -> Dict[str, Any]:
        """
        Build a minimal OCR Extraction JSON containing only the subtree for one subchapter.
        Sending the full multi-subchapter OCR blob on every API call often exceeds model context.
        """
        target = (subchapter_name or "").strip()
        if not target:
            return {"chapters": []}

        out_chapters: List[Dict[str, Any]] = []
        for chapter_obj in ocr_extraction_data.get("chapters", []) or []:
            if not isinstance(chapter_obj, dict):
                continue
            subs = chapter_obj.get("subchapters", []) or []
            matched = [
                s
                for s in subs
                if isinstance(s, dict) and (s.get("subchapter", "") or "").strip() == target
            ]
            if matched:
                out_chapters.append(
                    {"chapter": chapter_obj.get("chapter", ""), "subchapters": matched}
                )

        result: Dict[str, Any] = {"chapters": out_chapters}
        for key in ("metadata", "book", "title", "source_file"):
            if key in ocr_extraction_data:
                result[key] = ocr_extraction_data[key]
        if not out_chapters:
            self.logger.warning(
                "OCR slice has no chapters for subchapter %r — prompt will omit full OCR text for this name",
                subchapter_name,
            )
        return result

    def _filter_ocr_extraction_for_subchapter_topic(
        self,
        ocr_extraction_data: Dict[str, Any],
        subchapter_name: str,
        topic_name: str,
    ) -> Dict[str, Any]:
        """
        Build a minimal OCR Extraction JSON containing only one topic inside one subchapter.
        Used by context-window fallback paths to minimize input tokens.
        """
        sub_target = (subchapter_name or "").strip()
        if not sub_target:
            return {"chapters": []}
        topic_target = (topic_name or "").strip()

        out_chapters: List[Dict[str, Any]] = []
        for chapter_obj in ocr_extraction_data.get("chapters", []) or []:
            if not isinstance(chapter_obj, dict):
                continue

            matched_subs: List[Dict[str, Any]] = []
            for sub in chapter_obj.get("subchapters", []) or []:
                if not isinstance(sub, dict):
                    continue
                if (sub.get("subchapter", "") or "").strip() != sub_target:
                    continue

                topic_matches = [
                    t
                    for t in sub.get("topics", []) or []
                    if isinstance(t, dict)
                    and self._ocr_topic_labels_match(t.get("topic", ""), topic_target)
                ]
                if not topic_matches:
                    continue

                new_sub = dict(sub)
                new_sub["topics"] = topic_matches
                matched_subs.append(new_sub)

            if matched_subs:
                out_chapters.append(
                    {"chapter": chapter_obj.get("chapter", ""), "subchapters": matched_subs}
                )

        result: Dict[str, Any] = {"chapters": out_chapters}
        for key in ("metadata", "book", "title", "source_file"):
            if key in ocr_extraction_data:
                result[key] = ocr_extraction_data[key]

        if not out_chapters:
            self.logger.warning(
                "OCR topic slice has no matches for subchapter=%r topic=%r",
                subchapter_name,
                topic_name,
            )
        # #region agent log
        _agent_debug_log(
            "base_stage_processor.py:_filter_ocr_extraction_for_subchapter_topic",
            "OCR topic slice built",
            {
                "subchapter_name": sub_target,
                "topic_name": topic_target,
                "matched_chapters": len(out_chapters),
                **_agent_count_ocr_figures_in_slice(result),
            },
            "A",
        )
        # #endregion
        return result

    def _slim_ocr_for_stage_e_image_notes(self, ocr_slice: Dict[str, Any]) -> Dict[str, Any]:
        """
        Image-note (Stage E) prompts only need figure / e-figure captions from OCR.
        Tables belong to Table Notes (Stage TA). Bulk `text` extractions are dropped to save tokens.
        """
        slim = copy.deepcopy(ocr_slice)

        def _ocr_type_is_image_only(type_str: str) -> bool:
            x = (type_str or "").strip().lower().replace(" ", "")
            if not x or x == "text":
                return False
            if x in ("table", "etable", "e-table", "e_table"):
                return False
            if x in ("figure", "e-figure", "efigure", "fig", "image", "e-image", "eimage"):
                return True
            if x.startswith("e-") and "fig" in x:
                return True
            return False

        def _slim_extractions(extractions: Any) -> Any:
            if isinstance(extractions, list):
                return [
                    item
                    for item in extractions
                    if isinstance(item, dict)
                    and _ocr_type_is_image_only(BaseStageProcessor._ocr_extraction_item_type(item))
                ]
            if isinstance(extractions, dict):
                out: Dict[str, Any] = {}
                for key in ("figs", "figures", "images"):
                    if key in extractions:
                        out[key] = extractions[key]
                return out
            return extractions

        for chapter_obj in slim.get("chapters", []) or []:
            if not isinstance(chapter_obj, dict):
                continue
            new_subs: List[Dict[str, Any]] = []
            for sub in chapter_obj.get("subchapters", []) or []:
                if not isinstance(sub, dict):
                    continue
                new_topics: List[Dict[str, Any]] = []
                for topic_obj in sub.get("topics", []) or []:
                    if not isinstance(topic_obj, dict):
                        continue
                    ex = topic_obj.get("extractions")
                    slim_ex = _slim_extractions(ex)
                    if isinstance(slim_ex, list) and not slim_ex:
                        continue
                    if isinstance(slim_ex, dict) and not slim_ex:
                        continue
                    topic_obj = dict(topic_obj)
                    topic_obj["extractions"] = slim_ex
                    new_topics.append(topic_obj)
                if not new_topics:
                    continue
                sub = dict(sub)
                sub["topics"] = new_topics
                new_subs.append(sub)
            chapter_obj["subchapters"] = new_subs
        return slim

    @staticmethod
    def _ocr_slim_slice_has_figure_extractions(ocr_slice: Dict[str, Any]) -> bool:
        """True when a slimmed OCR slice still contains figure/image extractions."""
        for chapter_obj in ocr_slice.get("chapters", []) or []:
            if not isinstance(chapter_obj, dict):
                continue
            for sub in chapter_obj.get("subchapters", []) or []:
                if not isinstance(sub, dict):
                    continue
                for topic_obj in sub.get("topics", []) or []:
                    if not isinstance(topic_obj, dict):
                        continue
                    ex = topic_obj.get("extractions")
                    if isinstance(ex, list) and ex:
                        return True
                    if isinstance(ex, dict) and ex:
                        return True
        return False

    def _ocr_subchapter_has_figure_extractions(
        self,
        ocr_extraction_data: Dict[str, Any],
        subchapter_name: str,
    ) -> bool:
        """True when OCR Extraction JSON has at least one figure/image extraction in this subchapter."""
        ocr_slice = self._filter_ocr_extraction_for_subchapter(
            ocr_extraction_data, subchapter_name
        )
        slim = self._slim_ocr_for_stage_e_image_notes(ocr_slice)
        return self._ocr_slim_slice_has_figure_extractions(slim)

    def _ocr_figure_counts_by_topic_in_subchapter(
        self, ocr_extraction_data: Dict[str, Any], subchapter_name: str
    ) -> Dict[str, int]:
        """Count figure/e-figure extractions per OCR topic name within one subchapter."""
        counts: Dict[str, int] = {}
        ocr_slice = self._filter_ocr_extraction_for_subchapter(
            ocr_extraction_data, subchapter_name
        )
        for chapter_obj in ocr_slice.get("chapters", []) or []:
            if not isinstance(chapter_obj, dict):
                continue
            for sub in chapter_obj.get("subchapters", []) or []:
                if not isinstance(sub, dict):
                    continue
                for topic_obj in sub.get("topics", []) or []:
                    if not isinstance(topic_obj, dict):
                        continue
                    topic_key = (topic_obj.get("topic") or "").strip() or "(بدون مبحث)"
                    extractions = topic_obj.get("extractions")
                    n = 0
                    if isinstance(extractions, list):
                        for ex in extractions:
                            if not isinstance(ex, dict):
                                continue
                            type_val = self._ocr_extraction_item_type(ex)
                            x = type_val.strip().lower().replace(" ", "")
                            if not x or x == "text":
                                continue
                            if x in ("table", "etable", "e-table", "e_table"):
                                continue
                            if x in (
                                "figure",
                                "e-figure",
                                "efigure",
                                "fig",
                                "image",
                                "e-image",
                                "eimage",
                            ) or (x.startswith("e-") and "fig" in x):
                                n += 1
                    elif isinstance(extractions, dict):
                        for key in ("figs", "figures", "images"):
                            vals = extractions.get(key)
                            if isinstance(vals, list):
                                n += len(vals)
                    if n > 0:
                        counts[topic_key] = counts.get(topic_key, 0) + n
        return counts

    def _should_use_subchapter_figure_fallback(
        self,
        ocr_extraction_data: Dict[str, Any],
        subchapter_name: str,
        topic_name: str,
        stage4_topics_in_subchapter: Optional[set[str]] = None,
    ) -> bool:
        """
        Use all figures in a subchapter when the Stage 4 topic name does not match the
        OCR topic bucket that holds the figures (e.g. Stage 4 «مقدمه» vs OCR «ساختار پایه پوست»).
        """
        stage4_topic = (topic_name or "").strip() or "(بدون مبحث)"
        ocr_topic_filter = "" if stage4_topic == "(بدون مبحث)" else stage4_topic
        topic_slice = self._filter_ocr_extraction_for_subchapter_topic(
            ocr_extraction_data, subchapter_name, ocr_topic_filter
        )
        topic_slim = self._slim_ocr_for_stage_e_image_notes(topic_slice)
        if self._ocr_slim_slice_has_figure_extractions(topic_slim):
            return False

        sub_slice = self._filter_ocr_extraction_for_subchapter(
            ocr_extraction_data, subchapter_name
        )
        sub_slim = self._slim_ocr_for_stage_e_image_notes(sub_slice)
        if not self._ocr_slim_slice_has_figure_extractions(sub_slim):
            return False

        stage4_topics = stage4_topics_in_subchapter or set()
        fig_counts = self._ocr_figure_counts_by_topic_in_subchapter(
            ocr_extraction_data, subchapter_name
        )
        topic_norm = self._normalize_topic_label(stage4_topic)
        stage4_norm = {
            self._normalize_topic_label(t) for t in stage4_topics if (t or "").strip()
        }
        for ocr_topic, n in fig_counts.items():
            if n <= 0:
                continue
            ocr_norm = self._normalize_topic_label(ocr_topic)
            if ocr_norm == topic_norm:
                continue
            if ocr_norm in stage4_norm or ocr_topic in stage4_topics:
                return False
        return True

    def _ocr_image_slice_for_stage_e_topic(
        self,
        ocr_extraction_data: Dict[str, Any],
        subchapter_name: str,
        topic_name: str,
        stage4_topics_in_subchapter: Optional[set[str]] = None,
    ) -> tuple[Dict[str, Any], str]:
        """Return (slim OCR slice, mode) where mode is topic | subchapter_fallback | empty."""
        stage4_topic = (topic_name or "").strip() or "(بدون مبحث)"
        ocr_topic_filter = "" if stage4_topic == "(بدون مبحث)" else stage4_topic
        topic_slice = self._filter_ocr_extraction_for_subchapter_topic(
            ocr_extraction_data, subchapter_name, ocr_topic_filter
        )
        topic_slim = self._slim_ocr_for_stage_e_image_notes(topic_slice)
        if self._ocr_slim_slice_has_figure_extractions(topic_slim):
            return topic_slim, "topic"
        if self._should_use_subchapter_figure_fallback(
            ocr_extraction_data,
            subchapter_name,
            topic_name,
            stage4_topics_in_subchapter,
        ):
            sub_slice = self._filter_ocr_extraction_for_subchapter(
                ocr_extraction_data, subchapter_name
            )
            return self._slim_ocr_for_stage_e_image_notes(sub_slice), "subchapter_fallback"
        return topic_slim, "empty"

    def _ocr_topic_has_figure_extractions(
        self,
        ocr_extraction_data: Dict[str, Any],
        subchapter_name: str,
        topic_name: str,
        stage4_topics_in_subchapter: Optional[set[str]] = None,
    ) -> bool:
        """True when OCR has figure/image extractions for this Stage 4 topic bucket."""
        slim, mode = self._ocr_image_slice_for_stage_e_topic(
            ocr_extraction_data,
            subchapter_name,
            topic_name,
            stage4_topics_in_subchapter,
        )
        has_figs = mode != "empty" and self._ocr_slim_slice_has_figure_extractions(slim)
        # #region agent log
        _agent_debug_log(
            "base_stage_processor.py:_ocr_topic_has_figure_extractions",
            "OCR figure presence check",
            {
                "subchapter_name": (subchapter_name or "").strip(),
                "topic_name": (topic_name or "").strip() or "(بدون مبحث)",
                "has_figures_after_slim": has_figs,
                "ocr_slice_mode": mode,
                **_agent_count_ocr_figures_in_slice(slim),
            },
            "B",
        )
        # #endregion
        return has_figs

    @staticmethod
    def _ocr_extraction_type_is_table(type_str: str) -> bool:
        x = (type_str or "").strip().lower().replace(" ", "")
        return x in ("table", "etable", "e-table", "e_table")

    @staticmethod
    def _ocr_slice_has_table_extractions(ocr_slice: Dict[str, Any]) -> bool:
        """True when an OCR slice contains at least one table extraction."""
        for chapter_obj in ocr_slice.get("chapters", []) or []:
            if not isinstance(chapter_obj, dict):
                continue
            for sub in chapter_obj.get("subchapters", []) or []:
                if not isinstance(sub, dict):
                    continue
                for topic_obj in sub.get("topics", []) or []:
                    if not isinstance(topic_obj, dict):
                        continue
                    extractions = topic_obj.get("extractions", [])
                    if not isinstance(extractions, list):
                        continue
                    for extraction in extractions:
                        if not isinstance(extraction, dict):
                            continue
                        if BaseStageProcessor._ocr_extraction_type_is_table(
                            str(extraction.get("type", ""))
                        ):
                            return True
        return False

    def _ocr_subchapter_has_table_extractions(
        self,
        ocr_extraction_data: Dict[str, Any],
        subchapter_name: str,
    ) -> bool:
        ocr_slice = self._filter_ocr_extraction_for_subchapter(
            ocr_extraction_data, subchapter_name
        )
        return self._ocr_slice_has_table_extractions(ocr_slice)

    def _ocr_topic_has_table_extractions(
        self,
        ocr_extraction_data: Dict[str, Any],
        subchapter_name: str,
        topic_name: str,
    ) -> bool:
        stage4_topic = (topic_name or "").strip() or "(بدون مبحث)"
        ocr_topic_filter = "" if stage4_topic == "(بدون مبحث)" else stage4_topic
        topic_slice = self._filter_ocr_extraction_for_subchapter_topic(
            ocr_extraction_data, subchapter_name, ocr_topic_filter
        )
        return self._ocr_slice_has_table_extractions(topic_slice)

    def _mark_subchapter_topic_units_skipped(
        self,
        *,
        topic_names: List[str],
        persian_subchapter_name: str,
        filtered_points: List[Dict[str, Any]],
        unit_hooks: Optional[Any],
        topic_unit_map: Optional[Dict[str, Dict[str, Any]]],
    ) -> None:
        """Mark manifest units skipped when a subchapter/topic has no OCR pic content to process."""
        if not unit_hooks:
            return
        for topic_name in topic_names:
            pts = [
                p
                for p in filtered_points
                if isinstance(p, dict) and (p.get("topic") or "").strip() == topic_name
            ]
            ch = (pts[0].get("chapter") if pts else "") or ""
            from webapp.unit_repair.table_notes import resolve_topic_unit

            unit_info = resolve_topic_unit(
                topic_unit_map, str(ch or ""), persian_subchapter_name, topic_name
            )
            if not unit_info:
                continue
            ch = unit_info.get("chapter") or (pts[0].get("chapter") if pts else "")
            sub = unit_info.get("subchapter") or persian_subchapter_name
            unit_hooks.after_unit(
                int(unit_info["unit_index"]),
                str(ch or ""),
                str(sub or ""),
                topic_name,
                [],
                int(unit_info["unit_index"]),
                status="skipped",
            )

    def save_json_file(self, data: List[Dict], file_path: str, 
                      metadata: Dict, stage_name: str) -> bool:
        """
        Save JSON with standard structure: {metadata, data}
        
        Args:
            data: List of data records
            file_path: Output file path
            metadata: Additional metadata to include
            stage_name: Name of the stage (e.g., "E", "F", "J")
            
        Returns:
            True if successful, False otherwise
        """
        if not data:
            self.logger.warning("No data to save")
            data = []
        
        try:
            standard_json = {
                "metadata": {
                    "stage": stage_name,
                    "processed_at": datetime.now().isoformat(),
                    "total_records": len(data),
                    **metadata
                },
                "data": data
            }
            
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(file_path) or '.', exist_ok=True)
            
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(standard_json, f, ensure_ascii=False, indent=2)
            
            self.logger.info(f"Successfully saved JSON: {file_path} ({len(data)} records)")
            return True
        except Exception as e:
            self.logger.error(f"Error saving {file_path}: {e}")
            return False
    
    def generate_filename(self, prefix: str, book: int, chapter: int, 
                         base_dir: Optional[str] = None) -> str:
        """
        Generate filename: prefix + book + chapter
        Example: e105003.json
        
        Args:
            prefix: File prefix (e.g., "e", "f", "a")
            book: Book ID (3 digits)
            chapter: Chapter ID (3 digits)
            base_dir: Optional base directory
            
        Returns:
            Full file path
        """
        filename = f"{prefix}{book:03d}{chapter:03d}.json"
        if base_dir:
            return os.path.join(base_dir, filename)
        return filename
    
    def extract_book_chapter_from_pointid(self, point_id: str) -> tuple[int, int]:
        """
        Extract book and chapter from PointId.
        Format: BBBCCCPPPP (10 digits)
        - BBB (3 digits): Book ID
        - CCC (3 digits): Chapter ID
        - PPPP (4 digits): Point index
        
        Args:
            point_id: 10-digit PointId string
            
        Returns:
            Tuple of (book_id, chapter_id)
            
        Raises:
            ValueError: If PointId format is invalid
        """
        try:
            if not point_id or len(point_id) != 10 or not point_id.isdigit():
                raise ValueError(f"Invalid PointId format: {point_id} (must be 10 digits)")
            
            book = int(point_id[0:3])
            chapter = int(point_id[3:6])
            return book, chapter
        except (ValueError, IndexError) as e:
            self.logger.error(f"Error extracting book/chapter from PointId {point_id}: {e}")
            raise
    
    def get_data_from_json(self, json_data: Dict | List) -> List[Dict]:
        """
        Extract data array from JSON.
        Supports multiple structures: 
        - Direct array: [{...}, {...}]
        - Object with data: {metadata, data}, {metadata, points}, {rows}, {chapters}
        
        Args:
            json_data: JSON dictionary or list
            
        Returns:
            List of data records
        """
        # If json_data is already a list, return it directly
        if isinstance(json_data, list):
            return json_data
        
        # If json_data is a dict, look for data/points/rows/chapters keys
        if isinstance(json_data, dict):
            if "data" in json_data:
                return json_data["data"]
            elif "points" in json_data:
                return json_data["points"]
            elif "rows" in json_data:
                return json_data["rows"]
            elif "chapters" in json_data:
                # If chapters is a list, return it directly
                chapters = json_data["chapters"]
                if isinstance(chapters, list):
                    return chapters
                # If chapters is a dict, try to extract rows/data from it
                elif isinstance(chapters, dict):
                    if "rows" in chapters:
                        return chapters["rows"]
                    elif "data" in chapters:
                        return chapters["data"]
                    else:
                        # Return chapters dict as a single-item list
                        return [chapters]
                else:
                    return []
            else:
                self.logger.warning("No data/points/rows/chapters found in JSON, returning empty list")
                return []
        
        # Fallback: return empty list
        self.logger.warning(f"Unexpected JSON data type: {type(json_data)}, returning empty list")
        return []
    
    def get_metadata_from_json(self, json_data: Dict) -> Dict:
        """
        Extract metadata from JSON.
        
        Args:
            json_data: JSON dictionary
            
        Returns:
            Metadata dictionary (empty dict if not found)
        """
        return json_data.get("metadata", {})
    
    def get_first_pointid_from_json(self, json_data: Dict) -> Optional[str]:
        """
        Get the first PointId from JSON data.
        Useful for extracting book/chapter information.
        
        Args:
            json_data: JSON dictionary
            
        Returns:
            First PointId string or None
        """
        data = self.get_data_from_json(json_data)
        if not data:
            return None
        
        first_record = data[0]
        return first_record.get("PointId", None)
    
    def load_txt_as_json(self, txt_path: str) -> Optional[Dict | List]:
        """
        Load TXT file and extract JSON from it.
        Uses txt_stage_json_utils for robust extraction.
        
        Args:
            txt_path: Path to TXT file
            
        Returns:
            Parsed JSON (dict, list, or None on error).
            For Stage E, may return a direct array: [{...}]
            For other stages, may return an object: {"data": [...]}
        """
        if not os.path.exists(txt_path):
            self.logger.error(f"TXT file not found: {txt_path}")
            return None
        
        try:
            json_obj = load_stage_txt_as_json(txt_path)
            if json_obj:
                self.logger.info(f"Successfully loaded JSON from TXT: {txt_path}")
            return json_obj
        except Exception as e:
            self.logger.error(f"Error loading TXT as JSON {txt_path}: {e}")
            return None
    
    def load_txt_as_json_from_text(self, text: str) -> Optional[Dict | List]:
        """
        Extract JSON directly from text (without file) - for DeepSeek stages E-Y.
        Uses the same conversion method as document processing (extract_json_blocks_from_text).
        
        This method is for DeepSeek stages (E, F, J, H, V, M, L, X, Y, Z).
        For Google/Gemini stages (pre_ocr_topic, ocr_extraction), use load_txt_as_json_from_text_google().
        
        Args:
            text: Text content containing JSON
            
        Returns:
            Parsed JSON dictionary, list, or None on error
        """
        if not text or not text.strip():
            return None
        
        # Use the same extraction method as document processing (ONLY this method, no fallback)
        json_blocks = self.extract_json_blocks_from_text(text)
        
        if json_blocks:
            # Return first block (or combine if multiple blocks)
            if len(json_blocks) == 1:
                self.logger.info(f"Successfully extracted JSON from text using document processing method ({len(text)} chars, 1 block)")
                return json_blocks[0]
            else:
                # Multiple blocks - return first block
                self.logger.info(f"Extracted {len(json_blocks)} JSON blocks from text using document processing method, returning first block")
                return json_blocks[0]
        
        # No JSON blocks found - return None (no fallback to old method)
        self.logger.warning(f"Failed to extract JSON from text using document processing method ({len(text)} chars)")
        return None
    
    def load_txt_as_json_from_text_google(self, text: str) -> Optional[Dict | List]:
        """
        Extract JSON directly from text (without file) - Google/Gemini method.
        Uses ThirdStageConverter for robust extraction (original method).
        
        This method is for Google/Gemini stages (pre_ocr_topic, ocr_extraction).
        DeepSeek stages (E-Y) should use load_txt_as_json_from_text() instead.
        
        Args:
            text: Text content containing JSON
            
        Returns:
            Parsed JSON dictionary, list, or None on error
        """
        if not text or not text.strip():
            return None
        
        # Use original ThirdStageConverter method (for Google/Gemini)
        try:
            json_obj = self.converter._extract_json_from_response(text)
            if json_obj:
                self.logger.info(f"Successfully extracted JSON from text using Google/Gemini method ({len(text)} chars)")
            return json_obj
        except Exception as e:
            self.logger.error(f"Error extracting JSON from text: {e}")
            return None


"""
Base class for all stage processors.
Provides common functionality for JSON handling, file operations, and PointId management.
"""

import json
import logging
import os
import re
from datetime import datetime
from typing import Optional, Dict, List, Any
from third_stage_converter import ThirdStageConverter
from txt_stage_json_utils import load_stage_txt_as_json


class BaseStageProcessor:
    """Base class providing common functionality for all stage processors"""
    
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


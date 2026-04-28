"""
Secondary multi-part post-processor for taking an existing final_output.json
and running a second-stage prompt per Part, then combining all results.

NOTE:
- This module does NOT call the PDF endpoint. It only calls `process_text`
  on the existing JSON content per Part.
"""

import json
import logging
import os
import re
from typing import Dict, Any, List, Optional, Tuple


class MultiPartPostProcessor:
    """Post-process an existing final_output.json file part-by-part."""

    def __init__(self, api_client):
        """
        Args:
            api_client: GeminiAPIClient instance (must expose process_text)
        """
        self.api_client = api_client
        self.logger = logging.getLogger(__name__)
    
    def load_chapter_pointid_mapping(self, txt_path: str) -> List[str]:
        """
        Load PointId mapping from TXT file.
        Each line = start PointId for one chapter (in order)
        
        Supports multiple formats:
        - Exact 10-digit format: 1251330001
        - Numbers with spaces/text: "Chapter 1: 1251330001" or "1251330001 some text"
        - Numbers less than 10 digits: will be padded to 10 digits
        - Multiple numbers per line: first number is used
        
        Format examples:
        1251330001
        1151031001
        1451432001
        OR
        Chapter 1: 1251330001
        1251330001 (Chapter 2)
        
        Args:
            txt_path: Path to TXT file containing PointId mappings
            
        Returns:
            List of PointId strings (10 digits each)
        """
        pointids = []
        try:
            self.logger.info(f"Reading PointId mapping file: {txt_path}")
            if not os.path.exists(txt_path):
                self.logger.error(f"PointId mapping file does not exist: {txt_path}")
                return []
            
            # Try different encodings
            encodings_to_try = ['utf-8', 'utf-8-sig', 'utf-16', 'latin-1', 'cp1256']
            file_content = None
            used_encoding = None
            
            for encoding in encodings_to_try:
                try:
                    with open(txt_path, 'r', encoding=encoding) as f:
                        file_content = f.read()
                        used_encoding = encoding
                        self.logger.info(f"Successfully read file with encoding: {encoding}")
                        break
                except UnicodeDecodeError:
                    continue
            
            if file_content is None:
                self.logger.error(f"Failed to read file with any encoding: {txt_path}")
                return []
            
            self.logger.info(f"File size: {len(file_content)} bytes, Encoding: {used_encoding}")
            self.logger.info(f"File content preview (first 500 chars): {repr(file_content[:500])}")
            
            with open(txt_path, 'r', encoding=used_encoding) as f:
                total_lines = 0
                for line_num, line in enumerate(f, 1):
                    total_lines += 1
                    original_line = line
                    line = line.strip()
                    
                    # Log every line for debugging
                    self.logger.info(f"Line {line_num}: Raw content = {repr(original_line)}, Stripped = {repr(line)}, Length = {len(line)}")
                    
                    # Skip empty lines and comments
                    if not line or line.startswith('#'):
                        self.logger.info(f"Line {line_num}: Skipped (empty or comment): '{original_line.rstrip()}'")
                        continue
                    
                    # Try to extract numbers from the line
                    # First, try exact 10-digit format
                    if len(line) == 10 and line.isdigit():
                        pointids.append(line)
                        self.logger.info(f"Line {line_num}: Loaded PointId = {line} (Book: {line[0:3]}, Chapter: {line[3:6]}, Index: {line[6:10]})")
                    else:
                        # Extract all numbers from the line using regex
                        numbers = re.findall(r'\d+', line)
                        self.logger.info(f"Line {line_num}: Found numbers using regex: {numbers}")
                        if numbers:
                            # Use the first number found
                            first_number = numbers[0]
                            
                            # If number is less than 10 digits, pad it to 10 digits
                            if len(first_number) < 10:
                                # Pad with zeros at the end (right-pad)
                                padded_number = first_number.ljust(10, '0')
                                self.logger.info(f"Line {line_num}: Extracted number '{first_number}' (length: {len(first_number)}), padded to 10 digits: {padded_number}")
                                pointids.append(padded_number)
                            elif len(first_number) == 10:
                                pointids.append(first_number)
                                self.logger.info(f"Line {line_num}: Extracted PointId = {first_number} (Book: {first_number[0:3]}, Chapter: {first_number[3:6]}, Index: {first_number[6:10]})")
                            else:
                                # If number is more than 10 digits, take first 10 digits
                                truncated = first_number[:10]
                                pointids.append(truncated)
                                self.logger.warning(f"Line {line_num}: Extracted number '{first_number}' (length: {len(first_number)}), truncated to 10 digits: {truncated}")
                        else:
                            self.logger.warning(f"Line {line_num}: No numbers found in line: '{line}' (repr: {repr(line)})")
            
            self.logger.info(f"Total lines read: {total_lines}")
            self.logger.info(f"Loaded {len(pointids)} PointId mappings from {txt_path}")
            if pointids:
                self.logger.info(f"PointId list: {pointids}")
            else:
                self.logger.warning(f"No PointIds were extracted from file {txt_path}. Please check the file format.")
            return pointids
        except FileNotFoundError:
            self.logger.error(f"PointId mapping file not found: {txt_path}")
            return []
        except Exception as e:
            self.logger.error(f"Failed to load PointId mapping file {txt_path}: {e}", exc_info=True)
            return []


    def process_final_json_by_parts(
        self,
        json_path: str,
        user_prompt: str,
        model_name: str,
    ) -> Optional[str]:
        """
        Take final_output.json, split rows by Part, send each Part with the given prompt
        to the language model (via process_text), collect all JSON responses, and save
        a combined final JSON file.

        Args:
            json_path: Path to existing final_output.json
            user_prompt: Prompt/instruction for second-stage processing
            model_name: Gemini model name to use

        Returns:
            Path to combined final JSON file, or None on error.
        """
        if not os.path.exists(json_path):
            self.logger.error(f"JSON file not found: {json_path}")
            return None

        self.logger.info(f"Loading input JSON file: {json_path}")
        try:
            with open(json_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            self.logger.error(f"Failed to load JSON file {json_path}: {e}")
            return None

        rows = data.get("rows", [])
        if not isinstance(rows, list) or not rows:
            self.logger.error("Input JSON has no rows to process")
            return None

        self.logger.info(f"Found {len(rows)} rows in input JSON. Grouping by Part...")
        # Group rows by Part
        parts: Dict[int, List[Dict[str, Any]]] = {}
        for row in rows:
            if not isinstance(row, dict):
                continue
            part_value = row.get("Part", 0)
            try:
                part_num = int(part_value) if part_value not in (None, "") else 0
            except (ValueError, TypeError):
                part_num = 0

            parts.setdefault(part_num, []).append(row)

        if not parts:
            self.logger.error("No valid Part information found in rows")
            return None

        sorted_parts = sorted(parts.keys())
        self.logger.info(f"Processing {len(sorted_parts)} parts: {sorted_parts}")
        all_responses: List[str] = []

        for part_num in sorted_parts:
            part_rows = parts[part_num]
            if not part_rows:
                continue

            # Build text payload: just the JSON for this part
            part_json_text = json.dumps(part_rows, ensure_ascii=False, indent=2)

            # Use only user's prompt, no additions
            self.logger.info(f"Processing Part {part_num} ({len(part_rows)} rows) with second-stage prompt...")
            response_text = self.api_client.process_text(
                text=part_json_text,
                system_prompt=user_prompt,
                model_name=model_name,
            )

            if not response_text:
                self.logger.error(f"No response received for Part {part_num}, aborting post-process")
                return None

            self.logger.info(f"Part {part_num} processed successfully. Response length: {len(response_text)} characters")
            # Store response as-is (no parsing, no conversion)
            all_responses.append(response_text)

        if not all_responses:
            self.logger.error("No responses produced by second-stage processing")
            return None

        # Extract JSON blocks from each response separately, then combine
        self.logger.info(f"Extracting JSON blocks from {len(all_responses)} responses (one per part)...")
        all_json_blocks = []
        
        for part_num, response_text in zip(sorted_parts, all_responses):
            self.logger.debug(f"Part {part_num}: Extracting JSON from response ({len(response_text)} chars)...")
            # Extract JSON blocks from this response
            part_json_blocks = self._extract_json_blocks_from_text(response_text)
            if part_json_blocks:
                all_json_blocks.extend(part_json_blocks)
                self.logger.debug(f"Part {part_num}: Extracted {len(part_json_blocks)} JSON block(s)")
            else:
                self.logger.warning(f"Part {part_num}: No JSON blocks found in response")
        
        if not all_json_blocks:
            self.logger.error("No JSON blocks extracted from any response")
            return None

        self.logger.info(f"Total {len(all_json_blocks)} JSON block(s) extracted from all parts")
        
        # Combine all JSON blocks into final structure
        self.logger.info("Combining JSON blocks into final structure...")
        json_data = self._combine_json_blocks(all_json_blocks)
        if not json_data:
            self.logger.error("Failed to extract JSON from responses")
            return None

        # Save as JSON
        base_dir = os.path.dirname(json_path) or os.getcwd()
        input_name = os.path.basename(json_path)
        base_name, _ = os.path.splitext(input_name)
        # Stage 2 output naming: append explicit stage suffix
        json_filename = f"{base_name}_stage2.json"
        json_path_final = os.path.join(base_dir, json_filename)

        try:
            self.logger.info(f"Saving post-processed JSON to: {json_path_final}")
            with open(json_path_final, "w", encoding="utf-8") as f:
                json.dump(json_data, f, ensure_ascii=False, indent=2)
            self.logger.info(f"Post-processed JSON saved successfully: {json_path_final}")
            return json_path_final
        except Exception as e:
            self.logger.error(f"Failed to save post-processed JSON: {e}")
            return None

    def _extract_and_combine_json_blocks(self, text: str) -> Optional[Dict[str, Any]]:
        """
        Extract JSON blocks from text (between ```json and ```) and combine them.
        
        Returns:
            Combined JSON structure or None on error
        """
        if not text:
            return None

        json_blocks = []
        
        # Strategy 1: Find all JSON blocks between ```json and ``` or ``` and ```
        # Use a more robust pattern that handles multiple blocks
        json_block_patterns = [
            r'```json\s*(.*?)\s*```',  # ```json ... ```
            r'```\s*(.*?)\s*```',       # ``` ... ```
        ]
        
        matches = []
        for pattern in json_block_patterns:
            found_matches = re.findall(pattern, text, re.DOTALL)
            if found_matches:
                matches = found_matches
                self.logger.info(f"Found {len(matches)} JSON block(s) using pattern: {pattern[:20]}...")
                break
        
        # Strategy 2: If no code blocks found, try to extract JSON from each response separately
        # This handles cases where responses don't have code blocks
        if not matches:
            self.logger.warning("No code blocks found, trying to extract JSON from responses directly...")
            # Split by common separators and try to find JSON in each part
            parts = re.split(r'\n\n+', text)
            for part in parts:
                part = part.strip()
                if not part:
                    continue
                # Try to find JSON object/array in this part
                start_obj = part.find('{')
                start_arr = part.find('[')
                if start_obj != -1 or start_arr != -1:
                    start = start_obj if (start_obj != -1 and (start_arr == -1 or start_obj < start_arr)) else start_arr
                    # Find matching closing bracket
                    if start_obj != -1 and (start_arr == -1 or start_obj < start_arr):
                        end = part.rfind('}')
                        if end > start:
                            candidate = part[start:end + 1]
                            try:
                                json_obj = json.loads(candidate)
                                matches.append(candidate)
                            except json.JSONDecodeError:
                                pass
                    elif start_arr != -1:
                        end = part.rfind(']')
                        if end > start:
                            candidate = part[start:end + 1]
                            try:
                                json_obj = json.loads(candidate)
                                matches.append(candidate)
                            except json.JSONDecodeError:
                                pass
        
        self.logger.info(f"Found {len(matches)} JSON block(s) in text")
        for i, match in enumerate(matches, 1):
            json_str = match.strip()
            if not json_str:
                continue
            
            try:
                json_obj = json.loads(json_str)
                json_blocks.append(json_obj)
                self.logger.debug(f"Successfully parsed JSON block {i}/{len(matches)}")
            except json.JSONDecodeError as e:
                self.logger.warning(f"Failed to parse JSON block {i}/{len(matches)}: {str(e)}. Attempting fallback extraction...")
                # Try to find JSON object/array in the text
                start_obj = json_str.find("{")
                start_arr = json_str.find("[")
                
                if start_obj != -1 and (start_arr == -1 or start_obj < start_arr):
                    end_obj = json_str.rfind("}")
                    if end_obj > start_obj:
                        try:
                            candidate = json_str[start_obj:end_obj + 1]
                            json_obj = json.loads(candidate)
                            json_blocks.append(json_obj)
                            self.logger.debug(f"Successfully extracted JSON object from block {i} using fallback method")
                        except json.JSONDecodeError:
                            self.logger.warning(f"Fallback extraction failed for block {i}")
                            pass
                elif start_arr != -1:
                    end_arr = json_str.rfind("]")
                    if end_arr > start_arr:
                        try:
                            candidate = json_str[start_arr:end_arr + 1]
                            json_obj = json.loads(candidate)
                            json_blocks.append(json_obj)
                            self.logger.debug(f"Successfully extracted JSON array from block {i} using fallback method")
                        except json.JSONDecodeError:
                            self.logger.warning(f"Fallback extraction failed for block {i}")
                            pass
        
        if not json_blocks:
            self.logger.error("No valid JSON blocks found in responses")
            return None
        
        self.logger.info(f"Successfully extracted {len(json_blocks)} valid JSON block(s)")
        
        # Combine all JSON blocks
        self.logger.info("Combining JSON blocks into final structure...")
        # If all blocks have the same structure (chapter + content), combine content arrays
        combined_content = []
        combined_chapter = None
        
        for i, block in enumerate(json_blocks, 1):
            if isinstance(block, dict):
                content = block.get("content") if "content" in block else block.get("children")
                has_chapter = "chapter" in block or "فصل" in block
                if has_chapter and content is not None:
                    block_chapter = block.get("chapter") or block.get("فصل", "")
                    if combined_chapter is None:
                        combined_chapter = block_chapter
                        self.logger.debug(f"Block {i}: Using chapter '{combined_chapter}'")
                    elif combined_chapter != block_chapter:
                        self.logger.warning(f"Block {i}: Different chapter '{block_chapter}' (expected '{combined_chapter}'). Keeping both.")
                    
                    if isinstance(content, list):
                        combined_content.extend(content)
                        self.logger.debug(f"Block {i}: Added {len(content)} content items (total: {len(combined_content)})")
                    elif isinstance(content, dict):
                        combined_content.append(content)
                        self.logger.debug(f"Block {i}: Added 1 content object (content was dict, total: {len(combined_content)})")
                else:
                    combined_content.append(block)
                    self.logger.debug(f"Block {i}: Added as-is (different structure)")
            else:
                combined_content.append(block)
                self.logger.debug(f"Block {i}: Added as-is (not a dict)")
        
        # Build final JSON structure
        if combined_chapter:
            result = {
                "chapter": combined_chapter,
                "content": combined_content
            }
            self.logger.info(f"Final structure: chapter '{combined_chapter}' with {len(combined_content)} content items")
        else:
            if len(combined_content) == 1:
                result = combined_content[0]
                self.logger.info("Final structure: single object (no chapter structure)")
            else:
                result = combined_content
                self.logger.info(f"Final structure: array with {len(combined_content)} items (no chapter structure)")
        
        return result

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

    def _extract_json_blocks_from_text(self, text: str) -> List[Dict[str, Any]]:
        """
        Extract JSON blocks from a single text response.
        Handles: whole-text JSON, markdown code blocks (```json / ```), raw JSON by braces,
        escaped JSON strings, and fallback (ThirdStageConverter).

        Returns:
            List of JSON objects (dicts) extracted from the text
        """
        if not text:
            return []
        text = text.replace('\ufeff', '').strip()
        if not text:
            return []

        # Strategy 0: Escaped JSON string (e.g. from raw_responses)
        try:
            if text.startswith('"') and text.endswith('"'):
                unescaped = json.loads(text)
                if isinstance(unescaped, str):
                    return self._extract_json_blocks_from_text(unescaped)
        except (json.JSONDecodeError, TypeError):
            pass

        json_blocks: List[Dict[str, Any]] = []
        matches: List[str] = []

        # Strategy 1: Whole text is a single JSON object or array (raw model output, no markdown)
        stripped = text.strip()
        if stripped.startswith('{') or stripped.startswith('['):
            try:
                obj = json.loads(stripped)
                if isinstance(obj, dict):
                    json_blocks.append(obj)
                    self.logger.debug("Extracted single JSON object from whole response (no markdown)")
                    return json_blocks
                if isinstance(obj, list):
                    for item in obj:
                        if isinstance(item, dict):
                            json_blocks.append(item)
                    if json_blocks:
                        self.logger.debug("Extracted %d JSON object(s) from whole response array", len(json_blocks))
                        return json_blocks
            except json.JSONDecodeError:
                pass

        # Strategy 2: Markdown code blocks (case-insensitive ```json and plain ```)
        code_block_patterns = [
            r'```\s*[jJ][sS][oO][nN]\s*\n?(.*?)```',
            r'```\s*(.*?)```',
        ]
        for pattern in code_block_patterns:
            found = re.findall(pattern, text, re.DOTALL)
            if found:
                matches = [m.strip() for m in found if m and m.strip()]
                break

        # Strategy 3: No code blocks — extract by balanced braces
        if not matches:
            pos = 0
            while pos < len(text):
                start_obj = text.find('{', pos)
                start_arr = text.find('[', pos)
                if start_obj == -1 and start_arr == -1:
                    break
                start = start_obj if (start_arr == -1 or (start_obj != -1 and start_obj < start_arr)) else start_arr
                open_c, close_c = ('{', '}') if text[start] == '{' else ('[', ']')
                end = self._find_balanced_json_end(text, start, open_c, close_c)
                if end != -1:
                    candidate = text[start:end + 1].strip()
                    if candidate:
                        matches.append(candidate)
                    pos = end + 1
                else:
                    pos = start + 1

        for raw in matches:
            json_str = raw.strip()
            if not json_str:
                continue
            try:
                obj = json.loads(json_str)
                if isinstance(obj, dict):
                    json_blocks.append(obj)
                elif isinstance(obj, list):
                    for item in obj:
                        if isinstance(item, dict):
                            json_blocks.append(item)
            except json.JSONDecodeError:
                start_obj = json_str.find('{')
                start_arr = json_str.find('[')
                if start_obj != -1 or start_arr != -1:
                    start = start_obj if (start_arr == -1 or (start_obj != -1 and start_obj < start_arr)) else start_arr
                    open_c, close_c = ('{', '}') if json_str[start] == '{' else ('[', ']')
                    end = self._find_balanced_json_end(json_str, start, open_c, close_c)
                    if end != -1:
                        try:
                            obj = json.loads(json_str[start:end + 1])
                            if isinstance(obj, dict):
                                json_blocks.append(obj)
                            elif isinstance(obj, list):
                                for item in obj:
                                    if isinstance(item, dict):
                                        json_blocks.append(item)
                        except json.JSONDecodeError:
                            pass

        # Strategy 4: Fallback — ThirdStageConverter (repair, largest valid JSON)
        if not json_blocks:
            try:
                from third_stage_converter import ThirdStageConverter
                converter = ThirdStageConverter()
                parsed = converter._extract_json_from_response(text)
                if parsed is not None:
                    if isinstance(parsed, dict):
                        json_blocks.append(parsed)
                    elif isinstance(parsed, list):
                        for item in parsed:
                            if isinstance(item, dict):
                                json_blocks.append(item)
            except Exception as e:
                self.logger.debug(f"ThirdStageConverter fallback failed: {e}")

        return json_blocks

    def _combine_json_blocks(self, json_blocks: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """
        Combine multiple JSON blocks into a single structure.
        
        Args:
            json_blocks: List of JSON objects to combine
            
        Returns:
            Combined JSON structure or None on error
        """
        if not json_blocks:
            self.logger.error("No JSON blocks to combine")
            return None
        
        self.logger.info(f"Combining {len(json_blocks)} JSON block(s) into final structure...")
        
        # If all blocks have the same structure (chapter + content/children), combine content arrays
        combined_content = []
        combined_chapter = None
        
        for i, block in enumerate(json_blocks, 1):
            if isinstance(block, dict):
                # Content may be under "content" or "children" (model may return either)
                content = block.get("content") if "content" in block else block.get("children")
                has_chapter = "chapter" in block or "فصل" in block
                if has_chapter and content is not None:
                    block_chapter = block.get("chapter") or block.get("فصل", "")
                    if combined_chapter is None:
                        combined_chapter = block_chapter
                        self.logger.debug(f"Block {i}: Using chapter '{combined_chapter}'")
                    elif combined_chapter != block_chapter:
                        self.logger.warning(f"Block {i}: Different chapter '{block_chapter}' (expected '{combined_chapter}'). Keeping both.")
                    
                    if isinstance(content, list):
                        combined_content.extend(content)
                        self.logger.debug(f"Block {i}: Added {len(content)} content items (total: {len(combined_content)})")
                    elif isinstance(content, dict):
                        combined_content.append(content)
                        self.logger.debug(f"Block {i}: Added 1 content object (content was dict, total: {len(combined_content)})")
                else:
                    # Different structure, add as-is
                    combined_content.append(block)
                    self.logger.debug(f"Block {i}: Added as-is (different structure)")
            else:
                # Not a dict, add as-is
                combined_content.append(block)
                self.logger.debug(f"Block {i}: Added as-is (not a dict)")
        
        # Build final JSON structure
        if combined_chapter:
            result = {
                "chapter": combined_chapter,
                "content": combined_content
            }
            self.logger.info(f"Final structure: chapter '{combined_chapter}' with {len(combined_content)} content items")
        else:
            # If no chapter structure, return as array or object
            if len(combined_content) == 1:
                result = combined_content[0]
                self.logger.info("Final structure: single object (no chapter structure)")
            else:
                result = combined_content
                self.logger.info(f"Final structure: array with {len(combined_content)} items (no chapter structure)")
        
        return result

    def process_final_json_by_parts_with_responses(
        self,
        json_path: str,
        user_prompt: str,
        model_name: str,
    ) -> Tuple[Optional[str], Dict[int, str]]:
        """
        Take final_output.json, split rows by Part, send each Part with the given prompt
        to the language model (via process_text), collect all JSON responses, and save
        a combined final JSON file. Also returns individual responses per Part.

        Args:
            json_path: Path to existing final_output.json
            user_prompt: Prompt/instruction for second-stage processing
            model_name: Gemini model name to use

        Returns:
            Tuple of (path to combined final JSON file, dict of {part_num: response_text}),
            or (None, {}) on error.
        """
        if not os.path.exists(json_path):
            self.logger.error(f"JSON file not found: {json_path}")
            return None, {}

        self.logger.info(f"Loading input JSON file: {json_path}")
        try:
            with open(json_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            self.logger.error(f"Failed to load JSON file {json_path}: {e}")
            return None, {}

        rows = data.get("rows", [])
        if not isinstance(rows, list) or not rows:
            self.logger.error("Input JSON has no rows to process")
            return None, {}

        self.logger.info(f"Found {len(rows)} rows in input JSON. Grouping by Part...")
        # Group rows by Part
        parts: Dict[int, List[Dict[str, Any]]] = {}
        for row in rows:
            if not isinstance(row, dict):
                continue
            part_value = row.get("Part", 0)
            try:
                part_num = int(part_value) if part_value not in (None, "") else 0
            except (ValueError, TypeError):
                part_num = 0

            parts.setdefault(part_num, []).append(row)

        if not parts:
            self.logger.error("No valid Part information found in rows")
            return None, {}

        sorted_parts = sorted(parts.keys())
        self.logger.info(f"Processing {len(sorted_parts)} parts: {sorted_parts}")
        all_responses: List[str] = []
        part_responses: Dict[int, str] = {}

        for part_num in sorted_parts:
            part_rows = parts[part_num]
            if not part_rows:
                continue

            # Build text payload: just the JSON for this part
            part_json_text = json.dumps(part_rows, ensure_ascii=False, indent=2)

            # Use only user's prompt, no additions
            self.logger.info(f"Processing Part {part_num} ({len(part_rows)} rows) with second-stage prompt...")
            response_text = self.api_client.process_text(
                text=part_json_text,
                system_prompt=user_prompt,
                model_name=model_name,
            )

            if not response_text:
                self.logger.error(f"No response received for Part {part_num}, aborting post-process")
                return None, {}

            self.logger.info(f"Part {part_num} processed successfully. Response length: {len(response_text)} characters")
            # Store response as-is (no parsing, no conversion)
            all_responses.append(response_text)
            part_responses[part_num] = response_text

        if not all_responses:
            self.logger.error("No responses produced by second-stage processing")
            return None, {}

        # Extract JSON blocks from each response separately, then combine
        self.logger.info(f"Extracting JSON blocks from {len(all_responses)} responses (one per part)...")
        all_json_blocks = []
        
        for part_num, response_text in zip(sorted_parts, all_responses):
            self.logger.debug(f"Part {part_num}: Extracting JSON from response ({len(response_text)} chars)...")
            # Extract JSON blocks from this response
            part_json_blocks = self._extract_json_blocks_from_text(response_text)
            if part_json_blocks:
                all_json_blocks.extend(part_json_blocks)
                self.logger.debug(f"Part {part_num}: Extracted {len(part_json_blocks)} JSON block(s)")
            else:
                self.logger.warning(f"Part {part_num}: No JSON blocks found in response")
        
        if not all_json_blocks:
            self.logger.error("No JSON blocks extracted from any response")
            return None, {}
        
        self.logger.info(f"Total {len(all_json_blocks)} JSON block(s) extracted from all parts")
        
        # Combine all JSON blocks into final structure
        self.logger.info("Combining JSON blocks into final structure...")
        json_data = self._combine_json_blocks(all_json_blocks)
        if not json_data:
            self.logger.error("Failed to extract JSON from responses")
            return None, {}

        # Save as JSON
        base_dir = os.path.dirname(json_path) or os.getcwd()
        input_name = os.path.basename(json_path)
        base_name, _ = os.path.splitext(input_name)
        # Stage 2 output naming: append explicit stage suffix
        json_filename = f"{base_name}_stage2.json"
        json_path_final = os.path.join(base_dir, json_filename)

        try:
            self.logger.info(f"Saving post-processed JSON to: {json_path_final}")
            with open(json_path_final, "w", encoding="utf-8") as f:
                json.dump(json_data, f, ensure_ascii=False, indent=2)
            self.logger.info(f"Post-processed JSON saved successfully: {json_path_final}")
            return json_path_final, part_responses
        except Exception as e:
            self.logger.error(f"Failed to save post-processed JSON: {e}")
            return None, {}

    def process_final_json_by_subchapters(
        self,
        json_path: str,
        topic_file_path: str,
        user_prompt: str,
        model_name: str,
    ) -> Optional[Tuple[str, Dict[str, str]]]:
        """
        Take final_output.json, split rows by Subchapter (from topic file), send each Subchapter with the given prompt
        to the language model (via process_text), collect all JSON responses, and save
        a combined final JSON file.

        Args:
            json_path: Path to existing final_output.json (OCR Extraction JSON)
            topic_file_path: Path to topic file (t{book}{chapter}.json)
            user_prompt: Prompt/instruction for second-stage processing
            model_name: Gemini model name to use

        Returns:
            Tuple of (Path to combined final JSON file, Dict of subchapter_id -> response_text), or (None, {}) on error.
        """
        if not os.path.exists(json_path):
            self.logger.error(f"JSON file not found: {json_path}")
            return None, {}
        
        if not os.path.exists(topic_file_path):
            self.logger.error(f"Topic file not found: {topic_file_path}")
            return None, {}

        self.logger.info(f"Loading input JSON file: {json_path}")
        try:
            with open(json_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            self.logger.error(f"Failed to load JSON file {json_path}: {e}")
            return None, {}

        rows = data.get("rows", []) or data.get("data", []) or data.get("points", [])
        if not isinstance(rows, list) or not rows:
            self.logger.error("Input JSON has no rows to process")
            return None, {}

        # Load topic file
        self.logger.info(f"Loading topic file: {topic_file_path}")
        try:
            with open(topic_file_path, "r", encoding="utf-8") as f:
                topic_data = json.load(f)
        except Exception as e:
            self.logger.error(f"Failed to load topic file {topic_file_path}: {e}")
            return None, {}

        # Extract subchapters list from topic file
        topics_list = topic_data if isinstance(topic_data, list) else topic_data.get("data", topic_data.get("topics", []))
        if not topics_list:
            self.logger.error("Topic file has no subchapters")
            return None, {}

        # Extract subchapter names from topic file structure
        subchapter_names_from_file = set()
        for item in topics_list:
            if isinstance(item, dict):
                subchapter_name = item.get("Subchapter", "") or item.get("subchapter", "")
                if subchapter_name:
                    subchapter_names_from_file.add(subchapter_name.strip().lower())
        
        self.logger.info(f"Found {len(rows)} rows in input JSON. Starting division by Subchapter...")
        self.logger.info(f"Subchapters found in topic file: {len(subchapter_names_from_file)} - {sorted(subchapter_names_from_file)}")
        
        # Group rows by Subchapter
        # We'll match rows to subchapters based on subchapter field in rows
        subchapters: Dict[str, List[Dict[str, Any]]] = {}
        unmatched_rows = []
        
        # Log division statistics
        division_stats = {}
        
        for idx, row in enumerate(rows, 1):
            if not isinstance(row, dict):
                self.logger.warning(f"Row {idx} is not a dictionary, skipping")
                continue
            
            # Try to find subchapter in row
            subchapter_value = row.get("subchapter", "") or row.get("Subchapter", "")
            if subchapter_value:
                # Normalize subchapter value for matching
                subchapter_key = str(subchapter_value).strip()
                subchapter_key_lower = subchapter_key.lower()
                
                if subchapter_key_lower not in subchapters:
                    subchapters[subchapter_key_lower] = []
                    division_stats[subchapter_key_lower] = {
                        "name": subchapter_key,
                        "count": 0,
                        "first_row_index": idx
                    }
                
                subchapters[subchapter_key_lower].append(row)
                division_stats[subchapter_key_lower]["count"] += 1
                
                # Log first occurrence of each subchapter
                if division_stats[subchapter_key_lower]["count"] == 1:
                    self.logger.info(f"  [DIVISION] Subchapter '{subchapter_key}' - First row at index {idx}")
            else:
                unmatched_rows.append((idx, row))
        
        # Log division summary
        self.logger.info("=" * 80)
        self.logger.info("DIVISION BY SUBCHAPTER - SUMMARY")
        self.logger.info("=" * 80)
        for subchapter_key, stats in sorted(division_stats.items()):
            self.logger.info(f"  Subchapter: '{stats['name']}'")
            self.logger.info(f"    - Total rows: {stats['count']}")
            self.logger.info(f"    - First row index: {stats['first_row_index']}")
            self.logger.info(f"    - Last row index: {stats['first_row_index'] + stats['count'] - 1}")
        
        if unmatched_rows:
            self.logger.warning(f"  Found {len(unmatched_rows)} rows without subchapter field:")
            for row_idx, row in unmatched_rows[:10]:  # Log first 10
                self.logger.warning(f"    - Row {row_idx}: {str(row)[:100]}...")
            if len(unmatched_rows) > 10:
                self.logger.warning(f"    ... and {len(unmatched_rows) - 10} more rows")
            
            # Assign unmatched rows to a default subchapter
            if "default" not in subchapters:
                subchapters["default"] = []
                division_stats["default"] = {
                    "name": "default (unmatched)",
                    "count": 0,
                    "first_row_index": unmatched_rows[0][0] if unmatched_rows else 0
                }
            for row_idx, row in unmatched_rows:
                subchapters["default"].append(row)
                division_stats["default"]["count"] += 1
            
            self.logger.warning(f"  Assigned {len(unmatched_rows)} unmatched rows to 'default' subchapter")
        
        self.logger.info("=" * 80)
        self.logger.info(f"Total subchapters after division: {len(subchapters)}")
        self.logger.info(f"Total rows processed: {sum(len(rows) for rows in subchapters.values())}")
        self.logger.info("=" * 80)

        if not subchapters:
            self.logger.error("No valid Subchapter information found in rows")
            return None, {}

        sorted_subchapters = sorted(subchapters.keys())
        self.logger.info(f"Processing {len(sorted_subchapters)} subchapters: {[division_stats[k]['name'] for k in sorted_subchapters]}")
        all_responses: List[str] = []
        subchapter_responses: Dict[str, str] = {}

        for subchapter_idx, subchapter_id in enumerate(sorted_subchapters, 1):
            subchapter_rows = subchapters[subchapter_id]
            if not subchapter_rows:
                self.logger.warning(f"Subchapter '{division_stats[subchapter_id]['name']}' has no rows, skipping")
                continue

            subchapter_name = division_stats[subchapter_id]["name"]
            self.logger.info(f"[{subchapter_idx}/{len(sorted_subchapters)}] Processing Subchapter '{subchapter_name}' ({len(subchapter_rows)} rows)...")

            # Build text payload: just the JSON for this subchapter
            subchapter_json_text = json.dumps(subchapter_rows, ensure_ascii=False, indent=2)

            # Use only user's prompt, no additions
            response_text = self.api_client.process_text(
                text=subchapter_json_text,
                system_prompt=user_prompt,
                model_name=model_name,
            )

            if not response_text:
                self.logger.error(f"No response received for Subchapter '{subchapter_name}', aborting post-process")
                return None, {}

            self.logger.info(f"Subchapter '{subchapter_name}' processed successfully. Response length: {len(response_text)} characters")
            # Store response as-is (no parsing, no conversion)
            all_responses.append(response_text)
            subchapter_responses[subchapter_id] = response_text

        if not all_responses:
            self.logger.error("No responses produced by second-stage processing")
            return None, {}

        # Extract JSON blocks from each response separately, then combine
        self.logger.info(f"Extracting JSON blocks from {len(all_responses)} responses (one per subchapter)...")
        all_json_blocks = []
        
        for subchapter_id, response_text in zip(sorted_subchapters, all_responses):
            subchapter_name = division_stats[subchapter_id]["name"]
            self.logger.debug(f"Subchapter '{subchapter_name}': Extracting JSON from response ({len(response_text)} chars)...")
            # Extract JSON blocks from this response
            subchapter_json_blocks = self._extract_json_blocks_from_text(response_text)
            if subchapter_json_blocks:
                all_json_blocks.extend(subchapter_json_blocks)
                self.logger.debug(f"Subchapter '{subchapter_name}': Extracted {len(subchapter_json_blocks)} JSON block(s)")
            else:
                self.logger.warning(f"Subchapter '{subchapter_name}': No JSON blocks found in response")
        
        if not all_json_blocks:
            self.logger.error("No JSON blocks extracted from any response")
            return None, {}

        self.logger.info(f"Total {len(all_json_blocks)} JSON block(s) extracted from all subchapters")
        
        # Combine all JSON blocks into final structure
        self.logger.info("Combining JSON blocks into final structure...")
        json_data = self._combine_json_blocks(all_json_blocks)
        if not json_data:
            self.logger.error("Failed to extract JSON from responses")
            return None, {}

        # Save as JSON
        base_dir = os.path.dirname(json_path) or os.getcwd()
        input_name = os.path.basename(json_path)
        base_name, _ = os.path.splitext(input_name)
        # Stage 2 output naming: append explicit stage suffix
        json_filename = f"{base_name}_stage2.json"
        json_path_final = os.path.join(base_dir, json_filename)

        try:
            self.logger.info(f"Saving post-processed JSON to: {json_path_final}")
            with open(json_path_final, "w", encoding="utf-8") as f:
                json.dump(json_data, f, ensure_ascii=False, indent=2)
            self.logger.info(f"Post-processed JSON saved successfully: {json_path_final}")
            return json_path_final, subchapter_responses
        except Exception as e:
            self.logger.error(f"Failed to save post-processed JSON: {e}")
            return None, {}

    def process_document_processing_from_ocr_json(
        self,
        ocr_json_path: str,
        user_prompt: str,
        model_name: str,
        book_id: Optional[int] = None,
        chapter_id: Optional[int] = None,
        start_point_index: int = 1,
        pointid_mapping_txt: Optional[str] = None,
        progress_callback: Optional[Any] = None,
    ) -> Optional[str]:
        """
        Process Document Processing stage using OCR Extraction JSON as input.
        
        This method:
        1. Reads OCR Extraction JSON (with chapters->subchapters->topics/paragraphs structure)
        2. Extracts all paragraphs with their subchapter names (if paragraph field exists, otherwise uses topics)
        3. For each paragraph, replaces {Paragraph_NAME} and {Subchapter_Name} in the prompt
        4. Processes each paragraph with the replaced prompt
        5. Combines all outputs
        6. Flattens to points and assigns PointId
        
        If pointid_mapping_txt is provided:
        - Each line = start PointId for one chapter (in order)
        - Example: 1251330001, 1151031001, 1451432001
        - Chapters will use PointIds from this mapping
        
        Args:
            ocr_json_path: Path to OCR Extraction JSON file
            user_prompt: Prompt template with {Topic_NAME} and {Subchapter_Name} placeholders
            model_name: Gemini model name to use
            book_id: Book ID for PointId generation (used if pointid_mapping_txt not provided)
            chapter_id: Chapter ID for PointId generation (used if pointid_mapping_txt not provided)
            start_point_index: Starting index for PointId (default: 1, used if pointid_mapping_txt not provided)
            pointid_mapping_txt: Optional path to TXT file with PointId mappings (one per line, one per chapter)
            progress_callback: Optional callback for progress updates
            
        Returns:
            Path to final output JSON file, or None on error
        """
        if not os.path.exists(ocr_json_path):
            self.logger.error(f"OCR JSON file not found: {ocr_json_path}")
            return None
        
        # Load PointId mapping if provided
        chapter_pointids = []
        self.logger.info(f"PointId mapping file parameter: {pointid_mapping_txt}")
        self.logger.info(f"PointId mapping file exists check: {pointid_mapping_txt and os.path.exists(pointid_mapping_txt) if pointid_mapping_txt else False}")
        if pointid_mapping_txt:
            self.logger.info(f"PointId mapping file path: '{pointid_mapping_txt}'")
            self.logger.info(f"PointId mapping file absolute path: '{os.path.abspath(pointid_mapping_txt) if pointid_mapping_txt else None}'")
            self.logger.info(f"PointId mapping file exists: {os.path.exists(pointid_mapping_txt) if pointid_mapping_txt else False}")
        
        if pointid_mapping_txt and os.path.exists(pointid_mapping_txt):
            self.logger.info(f"Loading PointId mapping from file: {pointid_mapping_txt}")
            chapter_pointids = self.load_chapter_pointid_mapping(pointid_mapping_txt)
            if chapter_pointids:
                self.logger.info(f"Using PointId mapping from {pointid_mapping_txt}: {len(chapter_pointids)} entries")
                self.logger.info(f"PointId list from file: {chapter_pointids}")
            else:
                self.logger.warning(f"PointId mapping file is empty or invalid, falling back to start_point_index")
        else:
            if pointid_mapping_txt:
                self.logger.warning(f"PointId mapping file not found: {pointid_mapping_txt}")
                self.logger.warning(f"Absolute path checked: {os.path.abspath(pointid_mapping_txt)}")
            else:
                self.logger.info("No PointId mapping file provided")
        
        # Load OCR Extraction JSON
        self.logger.info(f"Loading OCR Extraction JSON: {ocr_json_path}")
        try:
            with open(ocr_json_path, "r", encoding="utf-8") as f:
                ocr_data = json.load(f)
        except Exception as e:
            self.logger.error(f"Failed to load OCR JSON file {ocr_json_path}: {e}")
            return None
        
        # Extract chapters structure
        chapters = ocr_data.get("chapters", [])
        if not chapters:
            self.logger.error("OCR JSON has no 'chapters' structure")
            return None
        
        # Validate PointId mapping count
        if chapter_pointids and len(chapter_pointids) < len(chapters):
            self.logger.warning(
                f"PointId mapping has {len(chapter_pointids)} entries but JSON has {len(chapters)} chapters. "
                f"Missing chapters will use fallback PointId."
            )
        
        # Build chapter info with PointId mappings
        chapters_info = []  # List of dicts: {chapter_name, chapter_index, start_pointid}
        
        self.logger.info("=" * 80)
        self.logger.info("BUILDING CHAPTER INFO WITH POINTID MAPPINGS")
        self.logger.info("=" * 80)
        self.logger.info(f"Number of chapters in OCR JSON: {len(chapters)}")
        self.logger.info(f"Number of PointIds from TXT file: {len(chapter_pointids)}")
        
        # Get the last PointId from file to use as base for auto-increment
        last_pointid_from_file = None
        if chapter_pointids:
            last_pointid_from_file = chapter_pointids[-1]
            self.logger.info(f"Last PointId from file: {last_pointid_from_file} (will be used as base for auto-increment if needed)")
        
        for chapter_idx, chapter in enumerate(chapters):
            if not isinstance(chapter, dict):
                continue
            
            chapter_name = chapter.get("chapter", "")
            if not chapter_name:
                chapter_name = f"Chapter_{chapter_idx + 1}"
            
            # Get PointId for this chapter - DIRECTLY use index from TXT file
            # Each line in TXT file corresponds to one chapter in order
            if chapter_idx < len(chapter_pointids):
                # Use PointId directly from TXT file (one-to-one mapping by index)
                start_pointid_str = chapter_pointids[chapter_idx]
                self.logger.info(f"Chapter {chapter_idx + 1} ('{chapter_name}'): Using PointId from TXT file (line {chapter_idx + 1}) = {start_pointid_str}")
                
                # Validate PointId format (must be 10 digits)
                if not (len(start_pointid_str) == 10 and start_pointid_str.isdigit()):
                    self.logger.error(f"Invalid PointId format for chapter {chapter_idx + 1}: {start_pointid_str} (length: {len(start_pointid_str)}, isdigit: {start_pointid_str.isdigit()})")
                    # Fallback: construct from book_id/chapter_id/start_point_index
                    if book_id and chapter_id:
                        start_pointid_str = f"{book_id:03d}{chapter_id:03d}{start_point_index:04d}"
                    else:
                        start_pointid_str = f"001001{start_point_index:04d}"
                    self.logger.warning(f"  Using fallback PointId: {start_pointid_str}")
                else:
                    self.logger.info(f"  ✓ Valid PointId format: {start_pointid_str}")
            elif last_pointid_from_file:
                # Auto-increment from last PointId in file
                # Use the whole number as base and increment it
                try:
                    # Convert last PointId to integer and increment
                    base_pointid_int = int(last_pointid_from_file)
                    # Calculate how many chapters after the last mapped chapter
                    chapters_after_mapped = chapter_idx - len(chapter_pointids) + 1
                    # Increment the whole number
                    new_pointid_int = base_pointid_int + chapters_after_mapped
                    # Convert back to 10-digit string
                    start_pointid_str = f"{new_pointid_int:010d}"
                    self.logger.info(f"Chapter {chapter_idx + 1} ('{chapter_name}'): Auto-incremented PointId = {start_pointid_str} (base: {last_pointid_from_file}, increment: +{chapters_after_mapped})")
                except (ValueError, OverflowError) as e:
                    self.logger.error(f"Failed to increment PointId {last_pointid_from_file}: {e}")
                    # Fallback
                    if book_id and chapter_id:
                        start_pointid_str = f"{book_id:03d}{chapter_id:03d}{start_point_index:04d}"
                    else:
                        start_pointid_str = f"001001{start_point_index:04d}"
                    self.logger.warning(f"  Using fallback PointId: {start_pointid_str}")
            else:
                # Fallback: use provided book_id/chapter_id/start_point_index
                self.logger.warning(
                    f"Chapter {chapter_idx + 1} ('{chapter_name}'): No PointId in mapping (index {chapter_idx} >= {len(chapter_pointids)}), "
                    f"using fallback"
                )
                if book_id and chapter_id:
                    start_pointid_str = f"{book_id:03d}{chapter_id:03d}{start_point_index:04d}"
                else:
                    start_pointid_str = f"001001{start_point_index:04d}"
                self.logger.info(f"Chapter {chapter_idx + 1} '{chapter_name}': Using fallback PointId: {start_pointid_str}")
            
            chapters_info.append({
                "chapter_name": chapter_name,
                "chapter_index": chapter_idx,
                "start_pointid": start_pointid_str,  # Store full PointId instead of separate parts
            })
        
        # Extract chapter name from JSON input (from metadata or first chapter)
        chapter_name_from_input = ocr_data.get("metadata", {}).get("chapter", "")
        if not chapter_name_from_input and chapters:
            chapter_name_from_input = chapters[0].get("chapter", "")
        
        if not chapter_name_from_input:
            self.logger.warning("No chapter name found in OCR JSON, will use empty string")
        
        self.logger.info(f"Chapter name from input JSON: {chapter_name_from_input}")
        
        # Detect input format: OCR Extraction Paragraph has topics with extractions as object { paragraphs, tables, figs }
        is_ocr_extraction_paragraph = False
        for _ch in chapters:
            if not isinstance(_ch, dict):
                continue
            for _sub in _ch.get("subchapters", []):
                if not isinstance(_sub, dict):
                    continue
                for _topic in _sub.get("topics", []):
                    if not isinstance(_topic, dict):
                        continue
                    ext = _topic.get("extractions")
                    if isinstance(ext, dict) and ("paragraphs" in ext or "tables" in ext or "figs" in ext):
                        is_ocr_extraction_paragraph = True
                        break
                if is_ocr_extraction_paragraph:
                    break
            if is_ocr_extraction_paragraph:
                break
        if is_ocr_extraction_paragraph:
            self.logger.info("Detected OCR Extraction Paragraph format (extractions as object with paragraphs/tables/figs)")
        
        # Collect items to process: either topics (OCR Extraction Paragraph) or paragraphs/topics (OCR Extraction)
        paragraphs_list = []  # List of (chapter_name, subchapter_name, paragraph_data) tuples
        topics_list = []  # List of (chapter_name, subchapter_name, topic_info_dict) for OCR Extraction Paragraph
        subchapters_dict = {}  # subchapter_name -> list of paragraph/topic names (for logging)
        subchapter_full_items: Dict[str, List[Dict[str, Any]]] = {}
        subchapter_has_paragraphs: Dict[str, bool] = {}
        total_removed_figures = 0
        
        if is_ocr_extraction_paragraph:
            # Build topics_list: one API call per topic; input JSON = chapters with one topic and extractions as array of { type, content }
            subchapter_topic_count = {}  # Track topics per subchapter
            subchapter_skipped_count = {}  # Track skipped topics per subchapter
            for chapter_idx, chapter in enumerate(chapters):
                if not isinstance(chapter, dict):
                    continue
                chapter_name = chapter.get("chapter", "") or f"Chapter_{chapter_idx + 1}"
                for subchapter in chapter.get("subchapters", []):
                    if not isinstance(subchapter, dict):
                        continue
                    subchapter_name = subchapter.get("subchapter", "")
                    if not subchapter_name:
                        continue
                    if subchapter_name not in subchapter_topic_count:
                        subchapter_topic_count[subchapter_name] = 0
                        subchapter_skipped_count[subchapter_name] = 0
                    
                    topics_in_subchapter = subchapter.get("topics", [])
                    if not topics_in_subchapter:
                        self.logger.warning(f"Subchapter '{subchapter_name}' has no topics - skipping")
                        continue
                    
                    for topic in topics_in_subchapter:
                        if not isinstance(topic, dict):
                            continue
                        topic_name = topic.get("topic", "") or topic.get("Topic", "")
                        if not topic_name:
                            continue
                        extras = topic.get("extractions", {})
                        if not isinstance(extras, dict):
                            self.logger.warning(f"Topic '{topic_name}' in subchapter '{subchapter_name}' has invalid extractions format - skipping")
                            subchapter_skipped_count[subchapter_name] += 1
                            continue
                        
                        # Log paragraph names for this topic
                        paragraphs_list_for_topic = extras.get("paragraphs", [])
                        paragraph_names = []
                        for p in paragraphs_list_for_topic:
                            if isinstance(p, dict):
                                # Try to get paragraph name from various possible fields
                                para_name = p.get("paragraph") or p.get("Paragraph") or p.get("name") or p.get("title") or ""
                                if para_name:
                                    paragraph_names.append(para_name)
                                elif p.get("content"):
                                    # If no name, use first 50 chars of content as identifier
                                    content_preview = p.get("content", "")[:50].replace("\n", " ")
                                    paragraph_names.append(f"[Content: {content_preview}...]")
                        
                        # Build extractions array as expected by prompt: [ { "type": "text", "content": "..." } ]
                        # Only include paragraphs (text content); exclude tables and figures
                        extractions_array = []
                        for p in paragraphs_list_for_topic:
                            if isinstance(p, dict) and p.get("content"):
                                extractions_array.append({"type": "text", "content": p.get("content", "")})
                        
                        # Log paragraph names for this topic
                        if paragraph_names:
                            self.logger.info(f"  Topic '{topic_name}' in subchapter '{subchapter_name}' has {len(paragraph_names)} paragraph(s):")
                            for para_idx, para_name in enumerate(paragraph_names, 1):
                                self.logger.info(f"    Paragraph {para_idx}: {para_name}")
                        else:
                            self.logger.warning(f"  Topic '{topic_name}' in subchapter '{subchapter_name}' has no named paragraphs")
                        
                        # Tables and figures are excluded from input to model
                        if not extractions_array:
                            self.logger.warning(f"  Skipping topic '{topic_name}' in subchapter '{subchapter_name}' (no text content after filtering)")
                            subchapter_skipped_count[subchapter_name] += 1
                            continue
                        
                        input_json = {
                            "chapters": [
                                {
                                    "chapter": chapter_name,
                                    "subchapters": [
                                        {
                                            "subchapter": subchapter_name,
                                            "topics": [{"topic": topic_name, "extractions": extractions_array}],
                                        }
                                    ],
                                }
                            ]
                        }
                        topics_list.append((chapter_name, subchapter_name, {"topic": topic_name, "input_json": input_json}))
                        subchapters_dict.setdefault(subchapter_name, []).append(topic_name)
                        subchapter_topic_count[subchapter_name] += 1
            
            # Log summary for subchapters
            self.logger.info("=" * 80)
            self.logger.info("SUBCHAPTER PROCESSING SUMMARY")
            self.logger.info("=" * 80)
            for subchapter_name in sorted(subchapter_topic_count.keys()):
                processed = subchapter_topic_count[subchapter_name]
                skipped = subchapter_skipped_count.get(subchapter_name, 0)
                total = processed + skipped
                if total == 0:
                    self.logger.warning(f"  Subchapter '{subchapter_name}': NO TOPICS FOUND - will be skipped")
                elif skipped > 0:
                    self.logger.warning(f"  Subchapter '{subchapter_name}': {processed} topics processed, {skipped} topics skipped (no content)")
                else:
                    self.logger.info(f"  Subchapter '{subchapter_name}': {processed} topics processed successfully")
            self.logger.info("=" * 80)
            
            self.logger.info(f"OCR Extraction Paragraph: built {len(topics_list)} topics to process")
        else:
            # Original: collect paragraphs/topics with extractions as array
            subchapter_item_count = {}  # Track items per subchapter
            subchapter_skipped_count = {}  # Track skipped items per subchapter
            for chapter_idx, chapter in enumerate(chapters):
                if not isinstance(chapter, dict):
                    continue
                chapter_name = chapter.get("chapter", "")
                if not chapter_name:
                    chapter_name = f"Chapter_{chapter_idx + 1}"
                subchapters = chapter.get("subchapters", [])
                for subchapter in subchapters:
                    if not isinstance(subchapter, dict):
                        continue
                    subchapter_name = subchapter.get("subchapter", "")
                    if not subchapter_name:
                        continue
                    if subchapter_name not in subchapters_dict:
                        subchapters_dict[subchapter_name] = []
                    if subchapter_name not in subchapter_full_items:
                        subchapter_full_items[subchapter_name] = []
                    if subchapter_name not in subchapter_item_count:
                        subchapter_item_count[subchapter_name] = 0
                        subchapter_skipped_count[subchapter_name] = 0
                    
                    paragraphs = subchapter.get("paragraphs", [])
                    topics = subchapter.get("topics", [])
                    items_to_process = paragraphs if paragraphs else topics
                    item_type = "paragraph" if paragraphs else "topic"
                    subchapter_has_paragraphs[subchapter_name] = bool(paragraphs)
                    
                    if not items_to_process:
                        self.logger.warning(f"Subchapter '{subchapter_name}' has no {item_type}s - skipping")
                        continue
                    
                    for item in items_to_process:
                        if not isinstance(item, dict):
                            continue
                        item_name = item.get("paragraph", "") or item.get("Paragraph", "") or item.get("topic", "") or item.get("Topic", "")
                        extractions = item.get("extractions", [])
                        if item_name and extractions:
                            original_count = len(extractions)
                            filtered_extractions = [
                                ext for ext in extractions
                                if isinstance(ext, dict) and ext.get("type") not in ["figure", "e-figure"]
                            ]
                            removed_count = original_count - len(filtered_extractions)
                            total_removed_figures += removed_count
                            if removed_count > 0:
                                self.logger.info(f"Removed {removed_count} figure record(s) from {item_type} '{item_name}' in subchapter '{subchapter_name}'")
                            
                            if not filtered_extractions:
                                self.logger.warning(f"  Skipping {item_type} '{item_name}' in subchapter '{subchapter_name}' (no content after filtering)")
                                subchapter_skipped_count[subchapter_name] += 1
                                continue
                            
                            item_data = dict(item)
                            item_data["extractions"] = filtered_extractions
                            if "paragraph" not in item_data and "Paragraph" not in item_data:
                                item_data["paragraph"] = item_name
                            paragraphs_list.append((chapter_name, subchapter_name, item_data))
                            subchapters_dict[subchapter_name].append(item_name)
                            subchapter_full_items[subchapter_name].append(item_data)
                            subchapter_item_count[subchapter_name] += 1
                        else:
                            if not item_name:
                                self.logger.warning(f"  Skipping unnamed {item_type} in subchapter '{subchapter_name}'")
                            elif not extractions:
                                self.logger.warning(f"  Skipping {item_type} '{item_name}' in subchapter '{subchapter_name}' (no extractions)")
                            subchapter_skipped_count[subchapter_name] += 1
            
            # Log summary for subchapters
            self.logger.info("=" * 80)
            self.logger.info("SUBCHAPTER PROCESSING SUMMARY")
            self.logger.info("=" * 80)
            for subchapter_name in sorted(subchapter_item_count.keys()):
                processed = subchapter_item_count[subchapter_name]
                skipped = subchapter_skipped_count.get(subchapter_name, 0)
                total = processed + skipped
                if total == 0:
                    self.logger.warning(f"  Subchapter '{subchapter_name}': NO ITEMS FOUND - will be skipped")
                elif skipped > 0:
                    self.logger.warning(f"  Subchapter '{subchapter_name}': {processed} items processed, {skipped} items skipped (no content)")
                else:
                    self.logger.info(f"  Subchapter '{subchapter_name}': {processed} items processed successfully")
            self.logger.info("=" * 80)
        
        # Log total removed records
        if total_removed_figures > 0:
            self.logger.info(f"Total removed {total_removed_figures} figure/e-figure record(s) from OCR JSON before processing")
        else:
            self.logger.info("No figure/e-figure records found in OCR JSON")
        
        process_list = topics_list if is_ocr_extraction_paragraph else paragraphs_list
        if not process_list:
            self.logger.error("No paragraphs/topics found in OCR JSON")
            return None
        
        total_items = len(process_list)
        item_label = "topics" if is_ocr_extraction_paragraph else "paragraphs"
        unique_subchapters = set(subchapter_name for _, subchapter_name, _ in process_list)
        
        self.logger.info("=" * 80)
        self.logger.info("DOCUMENT PROCESSING - PROCESSING BY " + item_label.upper())
        self.logger.info("=" * 80)
        self.logger.info(f"Found {total_items} {item_label} to process")
        self.logger.info(f"Found {len(unique_subchapters)} unique subchapters")
        self.logger.info("")
        
        # Log subchapters and their items
        subchapter_idx_map = {}
        # Also log subchapters that have no items (from original JSON)
        all_subchapters_in_json = set()
        for chapter in chapters:
            if isinstance(chapter, dict):
                for subchapter in chapter.get("subchapters", []):
                    if isinstance(subchapter, dict):
                        subchapter_name = subchapter.get("subchapter", "")
                        if subchapter_name:
                            all_subchapters_in_json.add(subchapter_name)
        
        # Log all subchapters (including those with no items)
        for idx, subchapter_name in enumerate(sorted(all_subchapters_in_json), 1):
            subchapter_idx_map[subchapter_name] = idx
            names = subchapters_dict.get(subchapter_name, [])
            if names:
                self.logger.info(f"  Subchapter {idx}: '{subchapter_name}' - {len(names)} {item_label}")
                for pi, name in enumerate(names, 1):
                    self.logger.info(f"    {name}")
            else:
                self.logger.warning(f"  Subchapter {idx}: '{subchapter_name}' - NO {item_label.upper()} FOUND (will be skipped)")
        self.logger.info("=" * 80)
        
        if progress_callback:
            progress_callback(f"Found {total_items} {item_label} in {len(unique_subchapters)} subchapters to process")
        
        # Prepare base directory and filename for JSON files
        base_dir = os.path.dirname(ocr_json_path) or os.getcwd()
        input_name = os.path.basename(ocr_json_path)
        base_name, _ = os.path.splitext(input_name)
        # Remove _final_output suffix if present
        if base_name.endswith("_final_output"):
            base_name = base_name[:-13]
        
        # Initialize output JSON file immediately (incremental writing)
        if not book_id:
            book_id = 1
        if not chapter_id:
            chapter_id = 1
        
        # Use input file name for unique output filename
        output_filename = f"Lesson_file_{base_name}.json"
        output_path = os.path.join(base_dir, output_filename)
        
        # Create initial structure with empty points
        from datetime import datetime
        initial_output = {
            "metadata": {
                "chapter": chapter_name_from_input,
                "book_id": book_id,
                "chapter_id": chapter_id,
                "total_points": 0,
                "start_point_index": start_point_index,
                "next_free_index": start_point_index,
                "processed_at": datetime.now().isoformat(),
                "source_file": os.path.basename(ocr_json_path),
                "model": model_name,
                "processing_status": "in_progress",
                "paragraphs_processed": 0,
                "total_paragraphs": total_items
            },
            "points": [],
            "raw_responses": []  # Store all raw responses to ensure nothing is lost
        }
        
        # Write initial file
        try:
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(initial_output, f, ensure_ascii=False, indent=2)
            self.logger.info(f"Initialized Document Processing JSON file: {output_path}")
        except Exception as e:
            self.logger.error(f"Failed to initialize output file: {e}")
            return None
        
        # Process each item (topic or paragraph) individually
        all_responses = []
        response_paragraph_info = []  # List of { chapter, subchapter, paragraph } per response
        
        for paragraph_idx, item in enumerate(process_list, 1):
            if is_ocr_extraction_paragraph:
                chapter_name_for_paragraph, subchapter_name, topic_info = item
                paragraph_name = topic_info["topic"]
                input_json = topic_info["input_json"]
                paragraph_prompt = user_prompt.replace("{Subchapter_Name}", subchapter_name).replace("[SUBCHAPTER_NAME]", subchapter_name)
                paragraph_prompt = paragraph_prompt.replace("{Paragraph_NAME}", paragraph_name).replace("[TOPIC_NAME]", paragraph_name)
                paragraph_prompt = paragraph_prompt.replace("{Topic_NAME}", paragraph_name)
                paragraph_json_text = json.dumps(input_json, ensure_ascii=False, indent=2)
                num_extractions = len(input_json["chapters"][0]["subchapters"][0]["topics"][0].get("extractions", []))
            else:
                chapter_name_for_paragraph, subchapter_name, paragraph_data = item
                paragraph_name = paragraph_data.get("paragraph", "") or paragraph_data.get("Paragraph", "") or paragraph_data.get("topic", "") or paragraph_data.get("Topic", "")
                extractions = paragraph_data.get("extractions", [])
                num_extractions = len(extractions)
                paragraph_prompt = user_prompt.replace("{Subchapter_Name}", subchapter_name).replace("[SUBCHAPTER_NAME]", subchapter_name)
                paragraph_prompt = paragraph_prompt.replace("{Paragraph_NAME}", paragraph_name).replace("[TOPIC_NAME]", paragraph_name)
                paragraph_prompt = paragraph_prompt.replace("{Topic_NAME}", paragraph_name)
                full_subchapter_items = subchapter_full_items.get(subchapter_name, []) or [paragraph_data]
                paragraph_json = {
                    "subchapter": subchapter_name,
                    "chapter": chapter_name_for_paragraph,
                    "paragraphs": full_subchapter_items if subchapter_has_paragraphs.get(subchapter_name, False) else None,
                    "topics": full_subchapter_items if not subchapter_has_paragraphs.get(subchapter_name, False) else None
                }
                paragraph_json = {k: v for k, v in paragraph_json.items() if v is not None}
                paragraph_json_text = json.dumps(paragraph_json, ensure_ascii=False, indent=2)
            
            self.logger.info("")
            self.logger.info("=" * 80)
            self.logger.info(f"PROCESSING {item_label[:-1].upper()} {paragraph_idx}/{total_items}")
            self.logger.info("=" * 80)
            self.logger.info(f"  Chapter: '{chapter_name_for_paragraph}'")
            self.logger.info(f"  Subchapter: '{subchapter_name}'")
            self.logger.info(f"  {'Topic' if is_ocr_extraction_paragraph else 'Paragraph'}: '{paragraph_name}'")
            self.logger.info(f"  Number of extractions: {num_extractions}")
            
            # Log paragraph names for OCR Extraction Paragraph format
            if is_ocr_extraction_paragraph:
                try:
                    # Extract paragraph names from input_json
                    input_json_data = topic_info.get("input_json", {})
                    topics_in_json = input_json_data.get("chapters", [{}])[0].get("subchapters", [{}])[0].get("topics", [{}])
                    if topics_in_json:
                        extractions_in_topic = topics_in_json[0].get("extractions", [])
                        if extractions_in_topic:
                            self.logger.info(f"  Paragraphs in this topic ({len(extractions_in_topic)}):")
                            for para_idx, ext in enumerate(extractions_in_topic, 1):
                                content_preview = ext.get("content", "")[:80].replace("\n", " ")
                                self.logger.info(f"    Paragraph {para_idx}: {content_preview}...")
                except Exception as e:
                    self.logger.debug(f"Could not extract paragraph names for logging: {e}")
            
            if progress_callback:
                progress_callback(f"Processing {paragraph_idx}/{total_items}: {chapter_name_for_paragraph} > {subchapter_name} > {paragraph_name}")
            
            json_size = len(paragraph_json_text.encode('utf-8'))
            self.logger.info(f"  JSON payload size: {json_size:,} bytes ({json_size/1024:.2f} KB)")
            if is_ocr_extraction_paragraph:
                self.logger.info(f"  Sending topic JSON to model (one topic, extractions as array)...")
            else:
                self.logger.info(f"  Sending paragraph JSON to model (full subchapter context with {len(full_subchapter_items) if not is_ocr_extraction_paragraph else 0} items)...")
            
            # Process with model (retry up to 3 times if response is empty)
            max_attempts = 3
            response_text = None
            for attempt in range(1, max_attempts + 1):
                self.logger.info(f"  Calling model API... (attempt {attempt}/{max_attempts})")
                response_text = self.api_client.process_text(
                    text=paragraph_json_text,
                    system_prompt=paragraph_prompt,
                    model_name=model_name,
                )
                if response_text and response_text.strip():
                    break
                if attempt < max_attempts:
                    self.logger.warning(f"  Empty response for paragraph '{paragraph_name}', retrying ({attempt}/{max_attempts})...")
                else:
                    self.logger.error(f"  ✗ No response received for paragraph after {max_attempts} attempts: {chapter_name_for_paragraph} > {subchapter_name} > {paragraph_name}")
                    response_text = ""
            
            response_size = len(response_text.encode('utf-8')) if response_text else 0
            self.logger.info(f"  ✓ Paragraph '{paragraph_name}' processed successfully")
            self.logger.info(f"  Response size: {response_size:,} bytes ({response_size/1024:.2f} KB)")
            self.logger.info(f"  Response length: {len(response_text):,} characters")
            
            # Store response and its corresponding paragraph info
            all_responses.append(response_text)
            paragraph_info = {
                "chapter": chapter_name_for_paragraph,
                "subchapter": subchapter_name,
                "paragraph": paragraph_name,
            }
            response_paragraph_info.append(paragraph_info)
            
            # Immediately convert response to points and add to file (incremental conversion)
            try:
                # Read current file
                with open(output_path, "r", encoding="utf-8") as f:
                    current_data = json.load(f)
                
                # Extract JSON blocks from this response
                blocks = self._extract_json_blocks_from_text(response_text or "")
                new_points = []  # Initialize to avoid NameError
                
                if blocks:
                    # Convert to points immediately using ThirdStageConverter
                    from third_stage_converter import ThirdStageConverter
                    converter = ThirdStageConverter()
                    
                    for block in blocks:
                        try:
                            flat_rows = converter._flatten_to_points(block)
                            if flat_rows:
                                # Add chapter/subchapter/topic info to each point
                                for row in flat_rows:
                                    row["chapter"] = chapter_name_for_paragraph
                                    row["subchapter"] = subchapter_name
                                    row["topic"] = paragraph_name
                                    new_points.append(row)
                        except Exception as e:
                            self.logger.warning(f"Failed to flatten block for paragraph '{paragraph_name}': {e}")
                    
                    if new_points:
                        # Get current next_free_index for PointId assignment
                        current_meta = current_data.get("metadata", {})
                        book_id = int(current_meta.get("book_id") or 1)
                        chapter_id = int(current_meta.get("chapter_id") or 1)
                        next_free_idx = int(current_meta.get("next_free_index") or current_meta.get("start_point_index") or 1)
                        
                        # Assign PointId to new points
                        for point in new_points:
                            point["PointId"] = f"{book_id:03d}{chapter_id:03d}{next_free_idx:04d}"
                            next_free_idx += 1
                        
                        # Add new points to existing points array
                        if "points" not in current_data:
                            current_data["points"] = []
                        current_data["points"].extend(new_points)
                        
                        # Update metadata
                        current_data["metadata"]["next_free_index"] = next_free_idx
                        current_data["metadata"]["total_points"] = len(current_data["points"])
                        self.logger.info(f"  ✓ Converted {len(new_points)} points from paragraph '{paragraph_name}' (total points: {len(current_data['points'])})")
                    else:
                        self.logger.warning(f"  No points extracted from paragraph '{paragraph_name}'")
                
                # Also save raw response for debugging/recovery (optional - can be removed later)
                raw_response_entry = {
                    "paragraph_index": paragraph_idx,
                    "subchapter": subchapter_name,
                    "paragraph": paragraph_name,
                    "response_text": response_text,
                    "response_size_bytes": response_size,
                    "processed_at": datetime.now().isoformat()
                }
                try:
                    if len(blocks) == 1:
                        raw_response_entry["response"] = blocks[0]
                    elif len(blocks) > 1:
                        raw_response_entry["response"] = blocks
                    else:
                        stripped = (response_text or "").strip()
                        if stripped.startswith("{") or stripped.startswith("["):
                            raw_response_entry["response"] = json.loads(stripped)
                        else:
                            raw_response_entry["response"] = None
                except (json.JSONDecodeError, TypeError, Exception):
                    raw_response_entry["response"] = None
                
                if "raw_responses" not in current_data:
                    current_data["raw_responses"] = []
                current_data["raw_responses"].append(raw_response_entry)
                
                # Update metadata
                current_data["metadata"]["paragraphs_processed"] = len(current_data["raw_responses"])
                current_data["metadata"]["processed_at"] = datetime.now().isoformat()
                
                # Write back immediately with converted points
                with open(output_path, "w", encoding="utf-8") as f:
                    json.dump(current_data, f, ensure_ascii=False, indent=2)
                
                self.logger.info(f"  ✓ Added {len(new_points) if blocks else 0} points and raw response for paragraph '{paragraph_name}' to JSON file immediately")
            except Exception as e:
                self.logger.error(f"Failed to convert and write response for paragraph '{paragraph_name}' to file: {e}", exc_info=True)
                # Continue processing other paragraphs
            
            self.logger.info(f"  ✓ Paragraph '{paragraph_name}' completed")
            self.logger.info("=" * 80)
        
        if not all_responses:
            self.logger.error("No responses produced by Document Processing")
            return None
        
        # Finalize: Read final file, update metadata, optionally remove raw_responses
        self.logger.info("=" * 80)
        self.logger.info("FINALIZING DOCUMENT PROCESSING OUTPUT")
        self.logger.info("=" * 80)
        try:
            with open(output_path, "r", encoding="utf-8") as f:
                final_data = json.load(f)
            
            final_meta = final_data.get("metadata", {})
            final_points = final_data.get("points", [])
            topics_processed = int(final_meta.get("paragraphs_processed") or final_meta.get("topics_processed") or len(final_data.get("raw_responses", [])))
            total_topics = int(final_meta.get("total_paragraphs") or final_meta.get("total_topics") or total_items)
            
            # Update metadata to match sample format
            final_meta["total_points"] = len(final_points)
            final_meta["next_free_index"] = final_meta.get("next_free_index") or (final_meta.get("start_point_index", 1) + len(final_points) - 1 if final_points else final_meta.get("start_point_index", 1))
            final_meta["processing_status"] = "completed"
            final_meta["topics_processed"] = topics_processed
            final_meta["total_topics"] = total_topics
            final_meta["processed_at"] = datetime.now().isoformat()
            # Remove old keys
            final_meta.pop("paragraphs_processed", None)
            final_meta.pop("total_paragraphs", None)
            
            # Remove raw_responses from final output (only keep converted points)
            if "raw_responses" in final_data:
                del final_data["raw_responses"]
                self.logger.info("Removed raw_responses from final output file")
            
            final_data["metadata"] = final_meta
            
            # Write finalized file
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(final_data, f, ensure_ascii=False, indent=2)
            
            self.logger.info(f"✓ Document Processing finalized: {len(final_points)} points from {topics_processed}/{total_topics} topics")
            if progress_callback:
                progress_callback(f"✓ Document Processing completed. {len(final_points)} points generated from {topics_processed} responses.")
        except Exception as e:
            self.logger.error(f"Failed to finalize output file: {e}", exc_info=True)
            return None
        
        return output_path


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
                # Check if it has chapter and content structure
                if "chapter" in block and "content" in block:
                    block_chapter = block.get("chapter")
                    if combined_chapter is None:
                        combined_chapter = block_chapter
                        self.logger.debug(f"Block {i}: Using chapter '{combined_chapter}'")
                    elif combined_chapter != block_chapter:
                        self.logger.warning(f"Block {i}: Different chapter '{block_chapter}' (expected '{combined_chapter}'). Keeping both.")
                    
                    content = block.get("content", [])
                    if isinstance(content, list):
                        combined_content.extend(content)
                        self.logger.debug(f"Block {i}: Added {len(content)} content items (total: {len(combined_content)})")
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

    def _extract_json_blocks_from_text(self, text: str) -> List[Dict[str, Any]]:
        """
        Extract JSON blocks from a single text response.
        
        Returns:
            List of JSON objects extracted from the text
        """
        if not text:
            return []
        
        json_blocks = []
        
        # Strategy 1: Find JSON blocks between ```json and ``` or ``` and ```
        json_block_patterns = [
            r'```json\s*(.*?)\s*```',  # ```json ... ```
            r'```\s*(.*?)\s*```',       # ``` ... ```
        ]
        
        matches = []
        for pattern in json_block_patterns:
            found_matches = re.findall(pattern, text, re.DOTALL)
            if found_matches:
                matches = found_matches
                break
        
        # Strategy 2: If no code blocks found, try to extract JSON directly
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
                elif start_arr != -1:
                    end = text.rfind(']')
                    if end > start:
                        candidate = text[start:end + 1]
                        matches = [candidate]
        
        # Parse each match
        for match in matches:
            json_str = match.strip()
            if not json_str:
                continue
            
            try:
                json_obj = json.loads(json_str)
                json_blocks.append(json_obj)
            except json.JSONDecodeError as e:
                # Try fallback extraction
                start_obj = json_str.find("{")
                start_arr = json_str.find("[")
                
                if start_obj != -1 and (start_arr == -1 or start_obj < start_arr):
                    end_obj = json_str.rfind("}")
                    if end_obj > start_obj:
                        try:
                            candidate = json_str[start_obj:end_obj + 1]
                            json_obj = json.loads(candidate)
                            json_blocks.append(json_obj)
                        except json.JSONDecodeError:
                            pass
                elif start_arr != -1:
                    end_arr = json_str.rfind("]")
                    if end_arr > start_arr:
                        try:
                            candidate = json_str[start_arr:end_arr + 1]
                            json_obj = json.loads(candidate)
                            json_blocks.append(json_obj)
                        except json.JSONDecodeError:
                            pass
        
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
        
        # If all blocks have the same structure (chapter + content), combine content arrays
        combined_content = []
        combined_chapter = None
        
        for i, block in enumerate(json_blocks, 1):
            if isinstance(block, dict):
                # Check if it has chapter and content structure
                if "chapter" in block and "content" in block:
                    block_chapter = block.get("chapter")
                    if combined_chapter is None:
                        combined_chapter = block_chapter
                        self.logger.debug(f"Block {i}: Using chapter '{combined_chapter}'")
                    elif combined_chapter != block_chapter:
                        self.logger.warning(f"Block {i}: Different chapter '{block_chapter}' (expected '{combined_chapter}'). Keeping both.")
                    
                    content = block.get("content", [])
                    if isinstance(content, list):
                        combined_content.extend(content)
                        self.logger.debug(f"Block {i}: Added {len(content)} content items (total: {len(combined_content)})")
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
        progress_callback: Optional[Any] = None,
    ) -> Optional[str]:
        """
        Process Document Processing stage using OCR Extraction JSON as input.
        
        This method:
        1. Reads OCR Extraction JSON (with chapters->subchapters->topics structure)
        2. Extracts all topics with their subchapter names
        3. For each topic, replaces {Topic_NAME} and {Subchapter_Name} in the prompt
        4. Processes each topic with the replaced prompt
        5. Combines all outputs
        6. Flattens to points and assigns PointId
        
        Args:
            ocr_json_path: Path to OCR Extraction JSON file
            user_prompt: Prompt template with {Topic_NAME} and {Subchapter_Name} placeholders
            model_name: Gemini model name to use
            book_id: Book ID for PointId generation
            chapter_id: Chapter ID for PointId generation
            start_point_index: Starting index for PointId (default: 1)
            progress_callback: Optional callback for progress updates
            
        Returns:
            Path to final output JSON file, or None on error
        """
        if not os.path.exists(ocr_json_path):
            self.logger.error(f"OCR JSON file not found: {ocr_json_path}")
            return None
        
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
        
        # Extract chapter name from JSON input (from metadata or first chapter)
        chapter_name_from_input = ocr_data.get("metadata", {}).get("chapter", "")
        if not chapter_name_from_input and chapters:
            chapter_name_from_input = chapters[0].get("chapter", "")
        
        if not chapter_name_from_input:
            self.logger.warning("No chapter name found in OCR JSON, will use empty string")
        
        self.logger.info(f"Chapter name from input JSON: {chapter_name_from_input}")
        
        # Collect all topics with their subchapter info
        # Process topic by topic, but for EACH topic send the FULL subchapter (all its topics) as context to the model.
        topics_list = []  # List of (subchapter_name, topic_data) tuples
        subchapters_dict = {}  # subchapter_name -> list of topic names (for logging / txt saving)
        # subchapter_name -> list of full topic dicts {"topic": ..., "extractions": [...]}
        subchapter_full_topics: Dict[str, List[Dict[str, Any]]] = {}
        
        for chapter in chapters:
            if not isinstance(chapter, dict):
                continue
            subchapters = chapter.get("subchapters", [])
            
            for subchapter in subchapters:
                if not isinstance(subchapter, dict):
                    continue
                subchapter_name = subchapter.get("subchapter", "")
                topics = subchapter.get("topics", [])
                
                if not subchapter_name:
                    continue
                
                # Initialize structures for this subchapter if not exists
                if subchapter_name not in subchapters_dict:
                    subchapters_dict[subchapter_name] = []
                if subchapter_name not in subchapter_full_topics:
                    subchapter_full_topics[subchapter_name] = []
                
                # Collect each topic individually and also build full subchapter topics list
                for topic in topics:
                    if not isinstance(topic, dict):
                        continue
                    topic_name = topic.get("topic", "")
                    extractions = topic.get("extractions", [])
                    
                    if topic_name and extractions:
                        topic_data = {
                            "topic": topic_name,
                            "extractions": extractions
                        }
                        # For per-topic processing
                        topics_list.append((subchapter_name, topic_data))
                        # For logging
                        subchapters_dict[subchapter_name].append(topic_name)
                        # For model context (FULL subchapter for every topic)
                        subchapter_full_topics[subchapter_name].append(topic_data)
        
        if not topics_list:
            self.logger.error("No topics found in OCR JSON")
            return None
        
        # Count unique subchapters
        unique_subchapters = set(subchapter_name for subchapter_name, _ in topics_list)
        
        self.logger.info("=" * 80)
        self.logger.info("DOCUMENT PROCESSING - PROCESSING BY TOPIC")
        self.logger.info("=" * 80)
        self.logger.info(f"Found {len(topics_list)} topics to process")
        self.logger.info(f"Found {len(unique_subchapters)} unique subchapters")
        self.logger.info("")
        
        # Log subchapters and their topics
        subchapter_idx_map = {}
        for idx, subchapter_name in enumerate(sorted(unique_subchapters), 1):
            subchapter_idx_map[subchapter_name] = idx
            topic_names = subchapters_dict[subchapter_name]
            self.logger.info(f"  Subchapter {idx}: '{subchapter_name}' - {len(topic_names)} topics")
            for topic_idx, topic_name in enumerate(topic_names, 1):
                self.logger.info(f"    Topic {topic_idx}: '{topic_name}'")
        self.logger.info("=" * 80)
        
        if progress_callback:
            progress_callback(f"Found {len(topics_list)} topics in {len(unique_subchapters)} subchapters to process")
        
        # Prepare base directory and filename for txt files and JSON files
        base_dir = os.path.dirname(ocr_json_path) or os.getcwd()
        input_name = os.path.basename(ocr_json_path)
        base_name, _ = os.path.splitext(input_name)
        
        # Process each topic individually
        all_responses = []
        # Store topic/subchapter info for each response to maintain correct mapping
        response_topic_info = []  # List of topic_info dicts, one per response
        # Group responses by subchapter for txt file saving
        subchapter_responses_dict: Dict[str, List[Dict[str, str]]] = {}  # subchapter_name -> list of {topic, response}
        
        for topic_idx, (subchapter_name, topic_data) in enumerate(topics_list, 1):
            topic_name = topic_data["topic"]
            extractions = topic_data["extractions"]
            
            self.logger.info("")
            self.logger.info("=" * 80)
            self.logger.info(f"PROCESSING TOPIC {topic_idx}/{len(topics_list)}")
            self.logger.info("=" * 80)
            self.logger.info(f"  Subchapter: '{subchapter_name}'")
            self.logger.info(f"  Topic: '{topic_name}'")
            self.logger.info(f"  Number of extractions: {len(extractions)}")
            
            if progress_callback:
                progress_callback(f"Processing topic {topic_idx}/{len(topics_list)}: {subchapter_name} > {topic_name}")
            
            # Replace placeholders in prompt with current topic and subchapter
            topic_prompt = user_prompt.replace("{Subchapter_Name}", subchapter_name)
            topic_prompt = topic_prompt.replace("{Topic_NAME}", topic_name)
            
            # Build JSON payload for this topic:
            # IMPORTANT: the model should see the FULL subchapter (all topics of this subchapter)
            # as context, but focus only on the current topic defined by {Topic_NAME} in the prompt.
            full_subchapter_topics = subchapter_full_topics.get(subchapter_name, [])
            if not full_subchapter_topics:
                # Fallback: at least send the current topic if for some reason subchapter list is empty
                full_subchapter_topics = [
                    {
                        "topic": topic_name,
                        "extractions": extractions
                    }
                ]
            
            topic_json = {
                "subchapter": subchapter_name,
                "chapter": chapter_name_from_input,
                # FULL subchapter context (all topics), not just the current topic
                "topics": full_subchapter_topics
            }
            topic_json_text = json.dumps(topic_json, ensure_ascii=False, indent=2)
            
            # Log JSON size before sending to model
            json_size = len(topic_json_text.encode('utf-8'))
            self.logger.info(f"  JSON payload size: {json_size:,} bytes ({json_size/1024:.2f} KB)")
            self.logger.info(f"  Sending topic JSON to model (full subchapter context with {len(full_subchapter_topics)} topics)...")
            
            # Process with model
            self.logger.info(f"  Calling model API...")
            response_text = self.api_client.process_text(
                text=topic_json_text,
                system_prompt=topic_prompt,
                model_name=model_name,
            )
            
            if not response_text:
                self.logger.error(f"  ✗ No response received for topic: {subchapter_name} > {topic_name}")
                continue
            
            response_size = len(response_text.encode('utf-8'))
            self.logger.info(f"  ✓ Topic '{topic_name}' processed successfully")
            self.logger.info(f"  Response size: {response_size:,} bytes ({response_size/1024:.2f} KB)")
            self.logger.info(f"  Response length: {len(response_text):,} characters")
            
            # Store response and its corresponding topic info
            all_responses.append(response_text)
            response_topic_info.append({
                "subchapter": subchapter_name,
                "topic": topic_name,
                "chapter": chapter_name_from_input
            })
            
            # Group response by subchapter for txt file saving
            if subchapter_name not in subchapter_responses_dict:
                subchapter_responses_dict[subchapter_name] = []
            subchapter_responses_dict[subchapter_name].append({
                "topic": topic_name,
                "response": response_text
            })
            
            self.logger.info(f"  ✓ Topic '{topic_name}' completed")
            self.logger.info("=" * 80)
        
        # Save txt files for each subchapter after all its topics are processed
        self.logger.info("")
        self.logger.info("=" * 80)
        self.logger.info("SAVING TXT FILES FOR EACH SUBCHAPTER")
        self.logger.info("=" * 80)
        
        for subchapter_name, topic_responses_list in subchapter_responses_dict.items():
            subchapter_idx = subchapter_idx_map.get(subchapter_name, 0)
            self._save_subchapter_txt_file(
                subchapter_name,
                topic_responses_list,
                base_dir,
                base_name,
                subchapter_idx,
                len(unique_subchapters),
                progress_callback
            )
        
        self.logger.info("=" * 80)
        
        if not all_responses:
            self.logger.error("No responses produced by Document Processing")
            return None
        
        # Extract JSON blocks from each response (one per topic)
        self.logger.info("=" * 80)
        self.logger.info("EXTRACTING JSON BLOCKS FROM RESPONSES")
        self.logger.info("=" * 80)
        self.logger.info(f"Extracting JSON blocks from {len(all_responses)} responses (one per topic)...")
        if progress_callback:
            progress_callback("Extracting JSON from responses...")
        
        all_json_blocks = []
        # Track number of blocks per topic to maintain correct mapping after flattening
        blocks_per_topic = []  # List of (topic_info, num_blocks) tuples
        
        for response_idx, (topic_info_stored, response_text) in enumerate(zip(response_topic_info, all_responses), 1):
            subchapter_name = topic_info_stored["subchapter"]
            topic_name = topic_info_stored["topic"]
            self.logger.info(f"  Topic {response_idx}/{len(all_responses)}: '{subchapter_name}' > '{topic_name}'")
            self.logger.debug(f"    Extracting JSON from response ({len(response_text):,} characters)...")
            
            topic_json_blocks = self._extract_json_blocks_from_text(response_text)
            if topic_json_blocks:
                all_json_blocks.extend(topic_json_blocks)
                blocks_per_topic.append((topic_info_stored, len(topic_json_blocks)))
                self.logger.info(f"    ✓ Extracted {len(topic_json_blocks)} JSON block(s)")
            else:
                blocks_per_topic.append((topic_info_stored, 0))
                self.logger.warning(f"    ✗ No JSON blocks found in response")
        
        self.logger.info(f"Total JSON blocks extracted: {len(all_json_blocks)}")
        self.logger.info("=" * 80)
        
        if not all_json_blocks:
            self.logger.error("No JSON blocks extracted from any response")
            return None
        
        # Group blocks by subchapter and calculate points per subchapter
        self.logger.info("=" * 80)
        self.logger.info("CALCULATING POINTS PER SUBCHAPTER")
        self.logger.info("=" * 80)
        from third_stage_converter import ThirdStageConverter
        converter = ThirdStageConverter()
        
        # Group blocks by subchapter
        blocks_by_subchapter = {}  # subchapter_name -> list of (topic_info, blocks)
        block_start_idx = 0
        
        for topic_info_stored, num_blocks in blocks_per_topic:
            subchapter_name = topic_info_stored["subchapter"]
            if subchapter_name not in blocks_by_subchapter:
                blocks_by_subchapter[subchapter_name] = {
                    "topics": [],
                    "blocks": []
                }
            
            # Get blocks for this topic
            topic_blocks = all_json_blocks[block_start_idx:block_start_idx + num_blocks]
            block_start_idx += num_blocks
            
            blocks_by_subchapter[subchapter_name]["topics"].append(topic_info_stored)
            blocks_by_subchapter[subchapter_name]["blocks"].extend(topic_blocks)
        
        # Track points count for each subchapter
        points_per_subchapter_list = []  # List of (subchapter_info, num_points) tuples
        
        for subchapter_name, subchapter_data in blocks_by_subchapter.items():
            subchapter_blocks = subchapter_data["blocks"]
            topics_info = subchapter_data["topics"]
            
            if not subchapter_blocks:
                # Create subchapter_info from first topic (all topics in same subchapter have same chapter/subchapter)
                first_topic_info = topics_info[0] if topics_info else {}
                subchapter_info = {
                    "subchapter": subchapter_name,
                    "chapter": first_topic_info.get("chapter", chapter_name_from_input),
                    "num_topics": len(topics_info),
                    "topic_names": [t.get("topic", "") for t in topics_info]
                }
                points_per_subchapter_list.append((subchapter_info, 0))
                self.logger.info(f"  Subchapter '{subchapter_name}': 0 points (no blocks)")
                continue
            
            # Combine blocks for this subchapter
            subchapter_json_data = self._combine_json_blocks(subchapter_blocks)
            if not subchapter_json_data:
                first_topic_info = topics_info[0] if topics_info else {}
                subchapter_info = {
                    "subchapter": subchapter_name,
                    "chapter": first_topic_info.get("chapter", chapter_name_from_input),
                    "num_topics": len(topics_info),
                    "topic_names": [t.get("topic", "") for t in topics_info]
                }
                points_per_subchapter_list.append((subchapter_info, 0))
                self.logger.warning(f"  Subchapter '{subchapter_name}': Failed to combine blocks")
                continue
            
            # Flatten to get point count for this subchapter
            try:
                subchapter_flat_rows = converter._flatten_to_points(subchapter_json_data)
                first_topic_info = topics_info[0] if topics_info else {}
                subchapter_info = {
                    "subchapter": subchapter_name,
                    "chapter": first_topic_info.get("chapter", chapter_name_from_input),
                    "num_topics": len(topics_info),
                    "topic_names": [t.get("topic", "") for t in topics_info]
                }
                points_per_subchapter_list.append((subchapter_info, len(subchapter_flat_rows)))
                self.logger.info(f"  Subchapter '{subchapter_name}': {len(subchapter_flat_rows)} points ({len(topics_info)} topics)")
            except Exception as e:
                first_topic_info = topics_info[0] if topics_info else {}
                subchapter_info = {
                    "subchapter": subchapter_name,
                    "chapter": first_topic_info.get("chapter", chapter_name_from_input),
                    "num_topics": len(topics_info),
                    "topic_names": [t.get("topic", "") for t in topics_info]
                }
                self.logger.warning(f"  Subchapter '{subchapter_name}': Failed to count points - {e}")
                points_per_subchapter_list.append((subchapter_info, 0))
        
        self.logger.info("=" * 80)
        
        # Combine all JSON blocks
        self.logger.info("Combining JSON blocks into final structure...")
        if progress_callback:
            progress_callback("Combining JSON blocks...")
        
        json_data = self._combine_json_blocks(all_json_blocks)
        if not json_data:
            self.logger.error("Failed to extract JSON from responses")
            return None
        
        # Flatten to points using ThirdStageConverter
        self.logger.info("Flattening hierarchical structure to points...")
        if progress_callback:
            progress_callback("Flattening to points...")
        
        # Chapter name comes from input JSON (already extracted above)
        
        try:
            flat_rows = converter._flatten_to_points(json_data)
            self.logger.info(f"Extracted {len(flat_rows)} points from Document Processing")
        except Exception as e:
            self.logger.error(f"Failed to flatten JSON to points: {e}")
            # Try to use json_data directly if it's already a list
            if isinstance(json_data, list):
                flat_rows = json_data
            elif isinstance(json_data, dict) and "content" in json_data:
                # Try to flatten content
                try:
                    flat_rows = converter._flatten_to_points(json_data)
                except:
                    flat_rows = []
            else:
                flat_rows = []
        
        if not flat_rows:
            self.logger.warning("No points extracted from Document Processing output")
            return None
        
        # Assign PointId and add chapter/subchapter/topic
        if not book_id:
            book_id = 1
        if not chapter_id:
            chapter_id = 1
        
        self.logger.info(f"Assigning PointId and chapter/subchapter/topic to {len(flat_rows)} points (starting from {start_point_index})...")
        if progress_callback:
            progress_callback(f"Assigning PointId to {len(flat_rows)} points...")
        
        # Use tracked points per subchapter to maintain correct mapping
        total_tracked_points = sum(count for _, count in points_per_subchapter_list)
        self.logger.info("=" * 80)
        self.logger.info("ASSIGNING POINT IDs AND SUBCHAPTER INFO")
        self.logger.info("=" * 80)
        self.logger.info(f"Using tracked points per subchapter for correct mapping:")
        for info, count in points_per_subchapter_list:
            self.logger.info(f"  Subchapter '{info['subchapter']}': {count} points")
        self.logger.info(f"Total tracked points: {total_tracked_points}, Actual flattened points: {len(flat_rows)}")
        
        if total_tracked_points != len(flat_rows):
            self.logger.warning(f"Point count mismatch: tracked {total_tracked_points} but got {len(flat_rows)}. This may affect subchapter mapping accuracy.")
        
        current_index = start_point_index
        point_idx = 0
        
        # Assign subchapter info based on tracked points per subchapter (maintains order)
        for subchapter_info_stored, num_points in points_per_subchapter_list:
            subchapter_name = subchapter_info_stored["subchapter"]
            self.logger.info(f"  Assigning PointIds for subchapter '{subchapter_name}' ({num_points} points)...")
            # Assign topic info to points in this range
            for i in range(num_points):
                if point_idx >= len(flat_rows):
                    self.logger.warning(f"More points tracked than available in flat_rows. Stopping assignment.")
                    break
                
                row = flat_rows[point_idx]
                point_id = f"{book_id:03d}{chapter_id:03d}{current_index:04d}"
                row["PointId"] = point_id
                
                # Chapter always comes from input JSON (not from model output)
                row["chapter"] = subchapter_info_stored["chapter"]
                
                # Use subchapter from model output, fallback to stored info only if missing
                if "subchapter" not in row or not row.get("subchapter"):
                    row["subchapter"] = subchapter_info_stored.get("subchapter", "")
                
                point_idx += 1
                current_index += 1
            
            self.logger.info(f"    ✓ Assigned {num_points} PointIds for subchapter '{subchapter_name}'")
            
            if point_idx >= len(flat_rows):
                break
        
        # Handle any remaining points (shouldn't happen, but safety check)
        if point_idx < len(flat_rows):
            self.logger.warning(f"Some points ({len(flat_rows) - point_idx}) were not assigned subchapter info. Using fallback.")
            for i in range(point_idx, len(flat_rows)):
                row = flat_rows[i]
                point_id = f"{book_id:03d}{chapter_id:03d}{current_index:04d}"
                row["PointId"] = point_id
                # Chapter always comes from input JSON
                row["chapter"] = chapter_name_from_input
                # Use subchapter from model output, fallback to stored info only if missing
                if "subchapter" not in row or not row.get("subchapter"):
                    if points_per_subchapter_list:
                        last_subchapter_info = points_per_subchapter_list[-1][0]
                        row["subchapter"] = last_subchapter_info.get("subchapter", "")
                current_index += 1
        
        self.logger.info("=" * 80)
        
        # Build final output
        from datetime import datetime
        base_dir = os.path.dirname(ocr_json_path) or os.getcwd()
        input_name = os.path.basename(ocr_json_path)
        base_name, _ = os.path.splitext(input_name)
        # Remove _final_output suffix if present
        if base_name.endswith("_final_output"):
            base_name = base_name[:-13]
        
        output_filename = f"Lesson_file_{book_id}_{chapter_id}.json"
        output_path = os.path.join(base_dir, output_filename)
        
        final_output = {
            "metadata": {
                "chapter": chapter_name_from_input,  # Chapter from input JSON
                "book_id": book_id,
                "chapter_id": chapter_id,
                "total_points": len(flat_rows),
                "start_point_index": start_point_index,
                "next_free_index": current_index,
                "processed_at": datetime.now().isoformat(),
                "source_file": os.path.basename(ocr_json_path),
                "model": model_name,
            },
            "points": flat_rows,
        }
        
        try:
            self.logger.info(f"Saving Document Processing output to: {output_path}")
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(final_output, f, ensure_ascii=False, indent=2)
            self.logger.info(f"Document Processing completed successfully: {output_path}")
            if progress_callback:
                progress_callback(f"✓ Document Processing completed. {len(flat_rows)} points generated.")
            return output_path
        except Exception as e:
            self.logger.error(f"Failed to save Document Processing output: {e}")
            return None

    def _save_subchapter_txt_file(
        self,
        subchapter_name: str,
        topic_responses_list: List[Dict[str, str]],
        base_dir: str,
        base_name: str,
        subchapter_idx: int,
        total_subchapters: int,
        progress_callback: Optional[Any] = None
    ):
        """
        Save a subchapter's responses to a txt file.
        
        Args:
            subchapter_name: Name of the subchapter
            topic_responses_list: List of dicts with 'topic' and 'response' keys
            base_dir: Base directory for saving files
            base_name: Base name for the file (without extension)
            subchapter_idx: Index of this subchapter (1-based)
            total_subchapters: Total number of subchapters
            progress_callback: Optional callback for progress updates
        """
        # Create safe filename from subchapter name
        safe_subchapter_name = subchapter_name.replace('/', '_').replace(' ', '_').replace('\\', '_')
        safe_subchapter_name = ''.join(c for c in safe_subchapter_name if c.isalnum() or c in ('_', '-', '.'))
        
        # Create txt filename
        txt_filename = f"{base_name}_subchapter_{subchapter_idx}_{safe_subchapter_name}.txt"
        txt_path = os.path.join(base_dir, txt_filename)
        
        try:
            # Combine all topic responses for this subchapter
            combined_text = f"=== Subchapter: {subchapter_name} ===\n\n"
            combined_text += f"Total topics in this subchapter: {len(topic_responses_list)}\n\n"
            combined_text += "=" * 80 + "\n\n"
            
            for topic_idx, topic_response_item in enumerate(topic_responses_list, 1):
                topic_name = topic_response_item["topic"]
                response_text = topic_response_item["response"]
                
                combined_text += f"--- Topic {topic_idx}: {topic_name} ---\n\n"
                combined_text += response_text
                combined_text += "\n\n" + "=" * 80 + "\n\n"
            
            # Save to file
            with open(txt_path, "w", encoding="utf-8") as f:
                f.write(combined_text)
            
            self.logger.info(f"Saved subchapter '{subchapter_name}' response to: {txt_path} ({len(topic_responses_list)} topics)")
            if progress_callback:
                progress_callback(f"Saved subchapter {subchapter_idx}/{total_subchapters}: {subchapter_name}")
        except Exception as e:
            self.logger.error(f"Failed to save txt file for subchapter '{subchapter_name}': {e}")




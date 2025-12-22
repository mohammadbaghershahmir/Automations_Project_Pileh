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




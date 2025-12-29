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

    def process_final_json_by_topics(
        self,
        json_path: str,
        topic_file_path: str,
        user_prompt: str,
        model_name: str,
    ) -> Optional[Tuple[str, Dict[str, str]]]:
        """
        Take final_output.json, split rows by Topic (from topic file), send each Topic with the given prompt
        to the language model (via process_text), collect all JSON responses, and save
        a combined final JSON file.

        Args:
            json_path: Path to existing final_output.json (OCR Extraction JSON)
            topic_file_path: Path to topic file (t{book}{chapter}.json)
            user_prompt: Prompt/instruction for second-stage processing
            model_name: Gemini model name to use

        Returns:
            Tuple of (Path to combined final JSON file, Dict of topic_id -> response_text), or (None, {}) on error.
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

        rows = data.get("rows", []) or data.get("data", [])
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

        # Extract topics list from topic file
        topics_list = topic_data if isinstance(topic_data, list) else topic_data.get("data", topic_data.get("topics", []))
        if not topics_list:
            self.logger.error("Topic file has no topics")
            return None, {}

        self.logger.info(f"Found {len(rows)} rows in input JSON. Grouping by Topic...")
        
        # Group rows by Topic
        # We'll match rows to topics based on topic field in rows
        topics: Dict[str, List[Dict[str, Any]]] = {}
        unmatched_rows = []
        
        for row in rows:
            if not isinstance(row, dict):
                continue
            # Try to find topic in row
            topic_value = row.get("topic", "") or row.get("Topic", "")
            if topic_value:
                # Normalize topic value for matching
                topic_key = str(topic_value).strip().lower()
                if topic_key not in topics:
                    topics[topic_key] = []
                topics[topic_key].append(row)
            else:
                unmatched_rows.append(row)
        
        # If we have unmatched rows, try to match them to topics from topic file
        if unmatched_rows:
            self.logger.warning(f"Found {len(unmatched_rows)} rows without topic field. Attempting to match...")
            # For now, we'll assign unmatched rows to a default topic
            if "default" not in topics:
                topics["default"] = []
            topics["default"].extend(unmatched_rows)

        if not topics:
            self.logger.error("No valid Topic information found in rows")
            return None, {}

        sorted_topics = sorted(topics.keys())
        self.logger.info(f"Processing {len(sorted_topics)} topics: {sorted_topics}")
        all_responses: List[str] = []
        topic_responses: Dict[str, str] = {}

        for topic_id in sorted_topics:
            topic_rows = topics[topic_id]
            if not topic_rows:
                continue

            # Build text payload: just the JSON for this topic
            topic_json_text = json.dumps(topic_rows, ensure_ascii=False, indent=2)

            # Use only user's prompt, no additions
            self.logger.info(f"Processing Topic {topic_id} ({len(topic_rows)} rows) with second-stage prompt...")
            response_text = self.api_client.process_text(
                text=topic_json_text,
                system_prompt=user_prompt,
                model_name=model_name,
            )

            if not response_text:
                self.logger.error(f"No response received for Topic {topic_id}, aborting post-process")
                return None, {}

            self.logger.info(f"Topic {topic_id} processed successfully. Response length: {len(response_text)} characters")
            # Store response as-is (no parsing, no conversion)
            all_responses.append(response_text)
            topic_responses[topic_id] = response_text

        if not all_responses:
            self.logger.error("No responses produced by second-stage processing")
            return None, {}

        # Extract JSON blocks from each response separately, then combine
        self.logger.info(f"Extracting JSON blocks from {len(all_responses)} responses (one per topic)...")
        all_json_blocks = []
        
        for topic_id, response_text in zip(sorted_topics, all_responses):
            self.logger.debug(f"Topic {topic_id}: Extracting JSON from response ({len(response_text)} chars)...")
            # Extract JSON blocks from this response
            topic_json_blocks = self._extract_json_blocks_from_text(response_text)
            if topic_json_blocks:
                all_json_blocks.extend(topic_json_blocks)
                self.logger.debug(f"Topic {topic_id}: Extracted {len(topic_json_blocks)} JSON block(s)")
            else:
                self.logger.warning(f"Topic {topic_id}: No JSON blocks found in response")
        
        if not all_json_blocks:
            self.logger.error("No JSON blocks extracted from any response")
            return None, {}

        self.logger.info(f"Total {len(all_json_blocks)} JSON block(s) extracted from all topics")
        
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
            return json_path_final, topic_responses
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
        
        # Collect all topics with their subchapter and chapter info
        topics_list = []
        for chapter in chapters:
            if not isinstance(chapter, dict):
                continue
            chapter_name = chapter.get("chapter", "")
            subchapters = chapter.get("subchapters", [])
            
            for subchapter in subchapters:
                if not isinstance(subchapter, dict):
                    continue
                subchapter_name = subchapter.get("subchapter", "")
                topics = subchapter.get("topics", [])
                
                for topic in topics:
                    if not isinstance(topic, dict):
                        continue
                    topic_name = topic.get("topic", "")
                    extractions = topic.get("extractions", [])
                    
                    if topic_name and extractions:
                        topics_list.append({
                            "chapter": chapter_name,
                            "subchapter": subchapter_name,
                            "topic": topic_name,
                            "extractions": extractions
                        })
        
        if not topics_list:
            self.logger.error("No topics found in OCR JSON")
            return None
        
        self.logger.info(f"Found {len(topics_list)} topics to process")
        if progress_callback:
            progress_callback(f"Found {len(topics_list)} topics to process")
        
        # Process each topic
        all_responses = []
        topic_responses = {}
        
        for idx, topic_info in enumerate(topics_list, 1):
            topic_name = topic_info["topic"]
            subchapter_name = topic_info["subchapter"]
            extractions = topic_info["extractions"]
            
            self.logger.info(f"Processing topic {idx}/{len(topics_list)}: {topic_name} (Subchapter: {subchapter_name})")
            if progress_callback:
                progress_callback(f"Processing topic {idx}/{len(topics_list)}: {topic_name}")
            
            # Replace placeholders in prompt
            topic_prompt = user_prompt.replace("{Topic_NAME}", topic_name)
            topic_prompt = topic_prompt.replace("{Subchapter_Name}", subchapter_name)
            
            # Build JSON payload for this topic
            topic_json = {
                "topic": topic_name,
                "subchapter": subchapter_name,
                "extractions": extractions
            }
            topic_json_text = json.dumps(topic_json, ensure_ascii=False, indent=2)
            
            # Process with model
            response_text = self.api_client.process_text(
                text=topic_json_text,
                system_prompt=topic_prompt,
                model_name=model_name,
            )
            
            if not response_text:
                self.logger.error(f"No response received for topic: {topic_name}")
                continue
            
            self.logger.info(f"Topic {topic_name} processed successfully. Response length: {len(response_text)} characters")
            all_responses.append(response_text)
            topic_responses[topic_name] = response_text
        
        if not all_responses:
            self.logger.error("No responses produced by Document Processing")
            return None
        
        # Extract JSON blocks from each response
        self.logger.info(f"Extracting JSON blocks from {len(all_responses)} responses...")
        if progress_callback:
            progress_callback("Extracting JSON from responses...")
        
        all_json_blocks = []
        for topic_name, response_text in zip([t["topic"] for t in topics_list], all_responses):
            self.logger.debug(f"Topic {topic_name}: Extracting JSON from response...")
            topic_json_blocks = self._extract_json_blocks_from_text(response_text)
            if topic_json_blocks:
                all_json_blocks.extend(topic_json_blocks)
                self.logger.debug(f"Topic {topic_name}: Extracted {len(topic_json_blocks)} JSON block(s)")
            else:
                self.logger.warning(f"Topic {topic_name}: No JSON blocks found in response")
        
        if not all_json_blocks:
            self.logger.error("No JSON blocks extracted from any response")
            return None
        
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
        
        from third_stage_converter import ThirdStageConverter
        converter = ThirdStageConverter()
        
        # Extract chapter name from first topic if available
        chapter_name = topics_list[0]["chapter"] if topics_list else ""
        
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
        
        # Calculate points distribution across topics
        total_topics = len(topics_list)
        points_per_topic = len(flat_rows) // total_topics if total_topics > 0 else 0
        if points_per_topic == 0:
            points_per_topic = 1
        
        self.logger.info(f"Distributing {len(flat_rows)} points across {total_topics} topics (~{points_per_topic} points per topic)")
        
        current_index = start_point_index
        for point_idx, row in enumerate(flat_rows):
            point_id = f"{book_id:03d}{chapter_id:03d}{current_index:04d}"
            row["PointId"] = point_id
            
            # Determine which topic this point belongs to based on index
            topic_idx = min(point_idx // points_per_topic, len(topics_list) - 1)
            topic_info = topics_list[topic_idx]
            
            # Add chapter/subchapter from OCR Extraction JSON
            # Note: topic field is kept from model output, not replaced
            row["chapter"] = topic_info.get("chapter", "")
            row["subchapter"] = topic_info.get("subchapter", "")
            # row["topic"] = topic_info.get("topic", "")  # Don't replace topic - keep model output
            
            current_index += 1
        
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
                "chapter": chapter_name,
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




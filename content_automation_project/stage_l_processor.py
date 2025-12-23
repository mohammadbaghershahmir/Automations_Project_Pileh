"""
Stage L Processor: Chapter Overview
Generates a chapter overview from Stage J (a file) and Stage V (b file) using the model.
"""

import json
import logging
import os
from typing import Optional, Dict, List, Any, Callable

from base_stage_processor import BaseStageProcessor
from api_layer import APIConfig


class StageLProcessor(BaseStageProcessor):
    """Process Stage L: Generate chapter overview from Stage J and Stage V data using the model."""

    def __init__(self, api_client):
        super().__init__(api_client)
        self.logger = logging.getLogger(__name__)

    def process_stage_l(
        self,
        stage_j_path: str,
        stage_v_path: str,
        prompt: str,
        model_name: str,
        output_dir: Optional[str] = None,
        progress_callback: Optional[Callable[[str], None]] = None
    ) -> Optional[str]:
        """
        Process Stage L: Generate chapter overview using Stage J, Stage V, and a prompt.

        Args:
            stage_j_path: Path to Stage J JSON file (a{book}{chapter}.json)
            stage_v_path: Path to Stage V JSON file (b{book}{chapter}.json)
            prompt: Chapter overview prompt from user
            model_name: Gemini model name
            output_dir: Output directory (defaults to stage_j_path directory)
            progress_callback: Optional callback for progress updates

        Returns:
            Path to output file (o{book}{chapter}.json) or None on error
        """

        def _progress(msg: str):
            if progress_callback:
                progress_callback(msg)
            self.logger.info(msg)

        _progress("Starting Stage L processing...")

        # Load Stage J (a file)
        _progress("Loading Stage J JSON...")
        stage_j_data = self.load_json_file(stage_j_path)
        if not stage_j_data:
            self.logger.error("Failed to load Stage J JSON")
            return None

        stage_j_records = self.get_data_from_json(stage_j_data)
        if not stage_j_records:
            self.logger.error("Stage J JSON has no data")
            return None

        _progress(f"Loaded {len(stage_j_records)} records from Stage J")

        # Extract book_id and chapter_id from first PointId
        first_point_id = stage_j_records[0].get("PointId") or stage_j_records[0].get("point_id")
        if not first_point_id:
            self.logger.error("No PointId found in Stage J data")
            return None

        try:
            book_id, chapter_id = self.extract_book_chapter_from_pointid(str(first_point_id))
        except ValueError as e:
            self.logger.error(f"Error extracting book/chapter: {e}")
            return None

        _progress(f"Detected Book ID: {book_id}, Chapter ID: {chapter_id}")

        # Load Stage V (b file)
        _progress("Loading Stage V JSON...")
        stage_v_data = self.load_json_file(stage_v_path)
        if not stage_v_data:
            self.logger.error("Failed to load Stage V JSON")
            return None

        stage_v_records = self.get_data_from_json(stage_v_data)
        if not stage_v_records:
            self.logger.warning("Stage V JSON has no data (no questions)")
            stage_v_records = []

        _progress(f"Loaded {len(stage_v_records)} records from Stage V")

        # Build compact overview context (stats per topic)
        _progress("Building overview context from Stage J and Stage V...")
        overview_context = self._build_overview_context(stage_j_records, stage_v_records)
        overview_json_str = json.dumps(overview_context, ensure_ascii=False, indent=2)

        # Build full prompt
        full_prompt = f"""{prompt}

Chapter Overview Context (JSON):
{overview_json_str}

Please analyze the chapter structure and the mapping between content (Stage J) and questions (Stage V),
and generate a JSON response with the chapter overview.

The JSON response MUST have the following structure:
{{
  "data": [
    {{
      "chapter": "chapter_name",
      "subchapter": "subchapter_name",
      "topic": "topic_name",
      "summary": "short textual overview of this topic (in Farsi)",
      "num_points": number_of_points_for_this_topic,
      "num_questions": number_of_questions_for_this_topic
    }},
    ...
  ]
}}

IMPORTANT:
- Use EXACT field names: chapter, subchapter, topic, summary, num_points, num_questions
- Response MUST be valid JSON. Do NOT include explanations outside JSON.
"""

        # Call model
        _progress("Calling model for Stage L overview...")
        max_retries = 3
        response_text = None
        for attempt in range(max_retries):
            try:
                response_text = self.api_client.process_text(
                    text=full_prompt,
                    system_prompt=None,
                    model_name=model_name,
                    temperature=APIConfig.DEFAULT_TEMPERATURE,
                    max_tokens=APIConfig.DEFAULT_MAX_TOKENS
                )
                if response_text:
                    _progress(f"Model response received ({len(response_text)} characters)")
                    break
            except Exception as e:
                self.logger.warning(f"Stage L model call attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    _progress(f"Retrying Stage L... (attempt {attempt + 2}/{max_retries})")
                else:
                    self.logger.error("All Stage L model call attempts failed")

        if not response_text:
            self.logger.error("No response from model in Stage L")
            return None

        # Prepare output directory
        if not output_dir:
            output_dir = os.path.dirname(stage_j_path) or os.getcwd()

        # Save raw response to TXT
        base_name, _ = os.path.splitext(os.path.basename(stage_j_path))
        txt_path = os.path.join(output_dir, f"{base_name}_stage_l.txt")
        try:
            with open(txt_path, "w", encoding="utf-8") as f:
                f.write(response_text)
            _progress(f"Saved raw model response to: {txt_path}")
        except Exception as e:
            self.logger.warning(f"Failed to save Stage L TXT file: {e}")

        # Extract JSON from response
        _progress("Extracting JSON from model response...")
        json_output = self.extract_json_from_response(response_text)
        if not json_output:
            _progress("Trying to extract JSON from text using fallback...")
            json_output = self.load_txt_as_json_from_text(response_text)

        if not json_output:
            self.logger.error("Failed to extract JSON from Stage L response")
            return None

        # Stage L output can have different structures:
        # 1. Standard format: {"data": [...]}
        # 2. Direct format: {"Chapter": "...", "Description": {...}}
        # 3. Other formats with data/points/rows keys
        overview_records = self.get_data_from_json(json_output)
        
        # If no standard data array found, check if it's a direct chapter overview structure
        if not overview_records:
            # Check if it's a direct chapter overview (e.g., {"Chapter": "...", "Description": {...}})
            if isinstance(json_output, dict) and ("Chapter" in json_output or "Description" in json_output):
                # Wrap the entire response as a single record
                overview_records = [json_output]
                _progress("Detected direct chapter overview structure, wrapping as single record")
            else:
                # Try to use the entire JSON as a single record
                overview_records = [json_output]
                _progress("Using entire JSON response as single overview record")
        
        if not overview_records:
            self.logger.error("No data found in Stage L JSON output")
            return None

        _progress(f"Extracted {len(overview_records)} overview record(s) from model")

        # Output filename: o{book}{chapter}.json
        output_path = self.generate_filename("o", book_id, chapter_id, output_dir)

        # Metadata
        output_metadata = {
            "book_id": book_id,
            "chapter_id": chapter_id,
            "source_stage_j": os.path.basename(stage_j_path),
            "source_stage_v": os.path.basename(stage_v_path),
            "stage_l_txt_file": os.path.basename(txt_path),
            "model_used": model_name,
            "total_records": len(overview_records),
        }

        _progress(f"Saving Stage L output to: {output_path}")
        success = self.save_json_file(overview_records, output_path, output_metadata, "L")

        if success:
            _progress(f"Stage L completed successfully: {output_path}")
            return output_path
        else:
            self.logger.error("Failed to save Stage L output")
            return None

    def _build_overview_context(
        self,
        stage_j_records: List[Dict[str, Any]],
        stage_v_records: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Build a compact overview context (stats per topic) to feed into the model."""
        topic_stats: Dict[Any, Dict[str, Any]] = {}

        # From Stage J
        for rec in stage_j_records:
            chapter = rec.get("chapter", "")
            subchapter = rec.get("subchapter", "")
            topic = rec.get("topic", "")
            key = (chapter, subchapter, topic)
            if key not in topic_stats:
                topic_stats[key] = {
                    "chapter": chapter,
                    "subchapter": subchapter,
                    "topic": topic,
                    "num_points": 0,
                    "num_questions": 0,
                }
            topic_stats[key]["num_points"] += 1

        # From Stage V
        for q in stage_v_records:
            chapter = q.get("Chapter", q.get("chapter", ""))
            subchapter = q.get("Subchapter", q.get("subchapter", ""))
            topic = q.get("Topic", q.get("topic", ""))
            key = (chapter, subchapter, topic)
            if key not in topic_stats:
                topic_stats[key] = {
                    "chapter": chapter,
                    "subchapter": subchapter,
                    "topic": topic,
                    "num_points": 0,
                    "num_questions": 0,
                }
            topic_stats[key]["num_questions"] += 1

        context_list = list(topic_stats.values())
        return {
            "topics": context_list,
            "total_topics": len(context_list),
            "total_points": sum(t["num_points"] for t in context_list),
            "total_questions": sum(t["num_questions"] for t in context_list),
        }




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
            stage_j_path: Path to Stage J JSON file (a{book}{chapter}+{chapter_name}.json)
            stage_v_path: Path to Stage V JSON file (b{book}{chapter}+{chapter_name}.json)
            prompt: Chapter overview prompt from user
            model_name: Gemini model name
            output_dir: Output directory (defaults to stage_j_path directory)
            progress_callback: Optional callback for progress updates

        Returns:
            Path to output file (o{book}{chapter}+{chapter_name}.json) or None on error
        """

        def _progress(msg: str):
            if progress_callback:
                progress_callback(msg)
            self.logger.info(msg)

        # Set stage if using UnifiedAPIClient (for API routing)
        if hasattr(self.api_client, 'set_stage'):
            self.api_client.set_stage("stage_l")

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

        # Extract chapter name from Stage J metadata or Stage V metadata
        chapter_name = ""
        stage_j_metadata = self.get_metadata_from_json(stage_j_data)
        chapter_name = (
            stage_j_metadata.get("chapter", "") or
            stage_j_metadata.get("Chapter", "") or
            stage_j_metadata.get("chapter_name", "") or
            stage_j_metadata.get("Chapter_Name", "") or
            ""
        )

        # If not found in Stage J metadata, try Stage V metadata
        if not chapter_name:
            stage_v_metadata = self.get_metadata_from_json(stage_v_data)
            chapter_name = (
                stage_v_metadata.get("chapter", "") or
                stage_v_metadata.get("Chapter", "") or
                stage_v_metadata.get("chapter_name", "") or
                stage_v_metadata.get("Chapter_Name", "") or
                ""
            )

        # If still not found, try to get from first record
        if not chapter_name and stage_j_records:
            chapter_name = stage_j_records[0].get("chapter", "")

        # If still not found, try to extract from Stage J filename (a{book}{chapter}+{chapter_name}.json)
        if not chapter_name:
            import re
            stage_j_basename = os.path.basename(stage_j_path)
            stage_j_name_without_ext = os.path.splitext(stage_j_basename)[0]
            # Try to extract chapter name from filename pattern: a{book}{chapter}+{chapter_name}
            match = re.match(r'^a\d{6}\+(.+)$', stage_j_name_without_ext)
            if match:
                chapter_name = match.group(1)
            else:
                # Try Stage V filename pattern: b{book}{chapter}+{chapter_name}.json
                stage_v_basename = os.path.basename(stage_v_path)
                stage_v_name_without_ext = os.path.splitext(stage_v_basename)[0]
                match = re.match(r'^b\d{6}\+(.+)$', stage_v_name_without_ext)
                if match:
                    chapter_name = match.group(1)

        # Clean chapter name for filename (remove invalid characters)
        if chapter_name:
            import re
            # Replace spaces and invalid filename characters with underscore
            chapter_name_clean = re.sub(r'[<>:"/\\|?*]', '_', chapter_name)
            chapter_name_clean = chapter_name_clean.replace(' ', '_')
            # Remove multiple underscores
            chapter_name_clean = re.sub(r'_+', '_', chapter_name_clean)
            # Remove leading/trailing underscores
            chapter_name_clean = chapter_name_clean.strip('_')
        else:
            chapter_name_clean = ""

        if chapter_name_clean:
            _progress(f"Detected Chapter Name: {chapter_name}")
        else:
            _progress("No chapter name found, will use timestamp in filename")

        # Generate output filename: o{book}{chapter}+{chapter_name}.json (matching Stage H/V pattern)
        # If chapter name is empty, use timestamp to avoid overwriting
        if chapter_name_clean:
            base_filename = f"o{book_id:03d}{chapter_id:03d}+{chapter_name_clean}.json"
        else:
            # Fallback if no chapter name: use timestamp to avoid overwriting
            from datetime import datetime
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            base_filename = f"o{book_id:03d}{chapter_id:03d}+{timestamp}.json"
            self.logger.warning(f"No chapter name found, using timestamp in filename: {timestamp}")

        output_path = os.path.join(output_dir, base_filename)

        # Check if file already exists and add counter if needed
        if os.path.exists(output_path) and chapter_name_clean:
            # If file exists and we have chapter name, add counter
            counter = 1
            while os.path.exists(output_path):
                base_filename = f"o{book_id:03d}{chapter_id:03d}+{chapter_name_clean}_{counter}.json"
                output_path = os.path.join(output_dir, base_filename)
                counter += 1
            if counter > 1:
                self.logger.info(f"File already exists, using counter: {counter - 1}")

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

    def _resolve_chapter_name(
        self,
        importance_type_path: str,
        importance_data: Any,
        importance_records: List[Dict[str, Any]],
    ) -> str:
        """Best-effort chapter title from metadata, records, or a*.json filename."""
        meta = self.get_metadata_from_json(importance_data)
        chapter_name = (
            meta.get("chapter", "")
            or meta.get("Chapter", "")
            or meta.get("chapter_name", "")
            or meta.get("Chapter_Name", "")
            or ""
        )
        if not chapter_name and importance_records:
            chapter_name = importance_records[0].get("chapter", "") or importance_records[0].get("Chapter", "")
        if not chapter_name:
            import re

            basename = os.path.basename(importance_type_path)
            name_without_ext = os.path.splitext(basename)[0]
            match = re.match(r"^a\d{6}\+(.+)$", name_without_ext)
            if match:
                chapter_name = match.group(1).replace("_", " ")
        return str(chapter_name or "").strip()

    def _normalize_chapter_summary_record(self, json_output: Any) -> Optional[Dict[str, str]]:
        """Map LLM JSON to exactly two fields: chapter_name, summary."""
        if isinstance(json_output, dict):
            if "data" in json_output and isinstance(json_output["data"], list) and json_output["data"]:
                first = json_output["data"][0]
                if isinstance(first, dict):
                    json_output = first
            chapter_name = (
                json_output.get("chapter_name")
                or json_output.get("chapter")
                or json_output.get("Chapter")
                or json_output.get("Chapter_Name")
                or ""
            )
            summary = (
                json_output.get("summary")
                or json_output.get("Summary")
                or json_output.get("overview")
                or json_output.get("Overview")
                or json_output.get("Description")
                or ""
            )
            if isinstance(summary, dict):
                summary = json.dumps(summary, ensure_ascii=False)
            chapter_name = str(chapter_name or "").strip()
            summary = str(summary or "").strip()
            if chapter_name and summary:
                return {"chapter_name": chapter_name, "summary": summary}
        return None

    def process_stage_l_web_chapter_summary(
        self,
        importance_type_path: str,
        step1_combined_path: str,
        prompt: str,
        model_name: str,
        output_dir: Optional[str] = None,
        progress_callback: Optional[Callable[[str], None]] = None,
    ) -> Optional[str]:
        """
        Web Chapter Summary: Importance & Type (a*) + Test Bank 1 step1_combined → o*.json
        with one row: chapter_name + summary.
        """
        def _progress(msg: str):
            if progress_callback:
                progress_callback(msg)
            self.logger.info(msg)

        if hasattr(self.api_client, "set_stage"):
            self.api_client.set_stage("stage_l")

        _progress("Starting Chapter Summary (web)...")

        _progress("Loading Importance & Type JSON...")
        imp_data = self.load_json_file(importance_type_path)
        if not imp_data:
            self.logger.error("Failed to load Importance & Type JSON")
            return None
        imp_records = self.get_data_from_json(imp_data)
        if not imp_records:
            self.logger.error("Importance & Type JSON has no data")
            return None
        _progress(f"Loaded {len(imp_records)} tagged lesson rows")

        first_point_id = imp_records[0].get("PointId") or imp_records[0].get("point_id")
        if not first_point_id:
            self.logger.error("No PointId found in Importance & Type data")
            return None
        try:
            book_id, chapter_id = self.extract_book_chapter_from_pointid(str(first_point_id))
        except ValueError as e:
            self.logger.error(f"Error extracting book/chapter: {e}")
            return None

        fallback_chapter_name = self._resolve_chapter_name(
            importance_type_path, imp_data, imp_records
        )

        _progress("Loading Test Bank 1 combined JSON...")
        step1_data = self.load_json_file(step1_combined_path)
        if not step1_data:
            self.logger.error("Failed to load Test Bank 1 combined JSON")
            return None
        step1_records = self.get_data_from_json(step1_data)
        if not step1_records:
            self.logger.warning("Test Bank 1 combined JSON has no questions")
            step1_records = []
        _progress(f"Loaded {len(step1_records)} Test Bank 1 question rows")

        _progress("Building overview context from tagged lesson + Test Bank 1...")
        overview_context = self._build_overview_context(imp_records, step1_records)
        overview_json_str = json.dumps(overview_context, ensure_ascii=False, indent=2)

        json_schema = """
The JSON response MUST be a single object with EXACTLY these two fields (no other top-level keys):
{
  "chapter_name": "chapter title in Farsi",
  "summary": "chapter overview text in Farsi (Big Picture only — no teaching, no topic-by-topic detail)"
}

Alternatively you may wrap it as: {"data": [{"chapter_name": "...", "summary": "..."}]} with exactly one element in data.

IMPORTANT:
- Output ONLY valid JSON. No markdown fences, no prose outside JSON.
- chapter_name: one short chapter title.
- summary: one cohesive chapter-level overview (not a list of per-topic summaries).
"""

        full_prompt = f"""{prompt}

{json_schema}

Importance & Type lesson context (compact stats from tagged a*.json):
{overview_json_str}

Test Bank 1 questions are included in the stats above (num_questions per topic). Use Shenasname / exam-year columns in the source tests when judging high-yield topics.

Suggested chapter_name (if helpful): {fallback_chapter_name or "(derive from inputs)"}
"""

        _progress("Calling model for Chapter Summary...")
        max_retries = 3
        response_text = None
        for attempt in range(max_retries):
            try:
                response_text = self.api_client.process_text(
                    text=full_prompt,
                    system_prompt=None,
                    model_name=model_name,
                    temperature=APIConfig.DEFAULT_TEMPERATURE,
                    max_tokens=APIConfig.DEFAULT_MAX_TOKENS,
                )
                if response_text:
                    _progress(f"Model response received ({len(response_text)} characters)")
                    break
            except Exception as e:
                self.logger.warning(f"Chapter Summary model call attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    _progress(f"Retrying... (attempt {attempt + 2}/{max_retries})")

        if not response_text:
            self.logger.error("No response from model in Chapter Summary")
            return None

        if not output_dir:
            output_dir = os.path.dirname(importance_type_path) or os.getcwd()

        base_name, _ = os.path.splitext(os.path.basename(importance_type_path))
        txt_path = os.path.join(output_dir, f"{base_name}_stage_l.txt")
        try:
            with open(txt_path, "w", encoding="utf-8") as f:
                f.write(response_text)
            _progress(f"Saved raw model response to: {txt_path}")
        except Exception as e:
            self.logger.warning(f"Failed to save Chapter Summary TXT file: {e}")

        _progress("Extracting JSON from model response...")
        json_output = self.extract_json_from_response(response_text)
        if not json_output:
            json_output = self.load_txt_as_json_from_text(response_text)

        normalized = self._normalize_chapter_summary_record(json_output) if json_output else None
        if not normalized:
            self.logger.error("Failed to parse chapter_name + summary from model response")
            return None

        if not normalized.get("chapter_name") and fallback_chapter_name:
            normalized["chapter_name"] = fallback_chapter_name

        if not normalized.get("chapter_name") or not normalized.get("summary"):
            self.logger.error("Chapter Summary output missing chapter_name or summary")
            return None

        chapter_name_clean = normalized["chapter_name"]
        import re

        filename_chapter = re.sub(r'[<>:"/\\|?*]', "_", chapter_name_clean)
        filename_chapter = filename_chapter.replace(" ", "_")
        filename_chapter = re.sub(r"_+", "_", filename_chapter).strip("_")

        if filename_chapter:
            base_filename = f"o{book_id:03d}{chapter_id:03d}+{filename_chapter}.json"
        else:
            from datetime import datetime

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            base_filename = f"o{book_id:03d}{chapter_id:03d}+{timestamp}.json"

        output_path = os.path.join(output_dir, base_filename)
        if os.path.exists(output_path) and filename_chapter:
            counter = 1
            while os.path.exists(output_path):
                base_filename = f"o{book_id:03d}{chapter_id:03d}+{filename_chapter}_{counter}.json"
                output_path = os.path.join(output_dir, base_filename)
                counter += 1

        output_records = [normalized]
        output_metadata = {
            "book_id": book_id,
            "chapter_id": chapter_id,
            "source_importance_type": os.path.basename(importance_type_path),
            "source_step1_combined": os.path.basename(step1_combined_path),
            "stage_l_txt_file": os.path.basename(txt_path),
            "model_used": model_name,
            "output_format": "chapter_name_summary",
        }

        _progress(f"Saving Chapter Summary output to: {output_path}")
        success = self.save_json_file(output_records, output_path, output_metadata, "L")
        if success:
            _progress(f"Chapter Summary completed: {output_path}")
            return output_path
        self.logger.error("Failed to save Chapter Summary output")
        return None




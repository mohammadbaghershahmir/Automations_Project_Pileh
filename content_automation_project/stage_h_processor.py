"""
Stage H Processor: Flashcard Generation
Generates flashcards from Stage J (without Imp column) and Stage F data.
"""

import json
import logging
import math
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Optional, Dict, List, Any, Callable

from base_stage_processor import BaseStageProcessor
from api_layer import APIConfig
from stage_j_processor import sj_web_is_context_limit_error

STAGE_H_WEB_LOG_PREFIX = "[stage_h_web]"

_SH_WEB_JSON_RETRY_SUFFIX = (
    "\n\nCRITICAL (retry): Reply with ONLY one JSON object {\"data\":[...]}. "
    "Each element: PointId, Qtext, Choice1, Choice2, Choice3, Choice4, Correct. "
    "No markdown, no chain-of-thought."
)


def sh_web_log(logger: logging.Logger, message: str) -> None:
    logger.info("%s %s", STAGE_H_WEB_LOG_PREFIX, message)


def sh_web_slim_lesson_row(record: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "PointId": str(record.get("PointId", "") or ""),
        "chapter": record.get("chapter", ""),
        "subchapter": record.get("subchapter", ""),
        "topic": record.get("topic", ""),
        "subtopic": record.get("subtopic", ""),
        "subsubtopic": record.get("subsubtopic", ""),
        "Points": record.get("Points", record.get("points", "")),
    }


def sh_web_build_user_prompt(
    prompt_body: str,
    part_num: int,
    total_parts: int,
    stage_f_json_str: str,
    slim_rows: List[Dict[str, Any]],
) -> str:
    sj = json.dumps(slim_rows, ensure_ascii=False, separators=(",", ":"))
    return f"""{prompt_body}

Stage F Data (Image files catalog):
{stage_f_json_str}

Structured inputs for this chunk (part {part_num} of {total_parts}):

Lesson rows (PointId + hierarchy + Points only — no Imp/Type in this block):
{sj}

Generate one flashcard per PointId in the lesson rows above. Reply with ONE JSON object per the Output rules (root key "data"). No markdown outside JSON."""


def sh_web_max_tokens_for_chunk(chunk_len: int) -> int:
    estimated = int(chunk_len * 250 * 1.5)
    cap = min(APIConfig.DEFAULT_MAX_TOKENS * 2, getattr(APIConfig, "DEFAULT_OPENROUTER_MAX_TOKENS", 65536))
    return max(4096, min(estimated, cap))


class StageHProcessor(BaseStageProcessor):
    """Process Stage H: Generate flashcards from Stage J and Stage F data"""

    STAGE_H_WEB_CHUNK_SIZE = 100
    STAGE_H_WEB_PARALLEL_CHUNKS = 6
    STAGE_H_WEB_LLM_PARSE_RETRIES = 3

    def __init__(self, api_client):
        super().__init__(api_client)
        self.logger = logging.getLogger(__name__)

    def _parse_flashcard_rows_from_llm_response(self, response_text: str) -> List[Dict[str, Any]]:
        if not (response_text or "").strip():
            return []
        part_output = self.extract_json_from_response(response_text)
        if not part_output:
            part_output = self.load_txt_as_json_from_text(response_text)
        if not part_output:
            return []
        rows = self.get_data_from_json(part_output)
        out: List[Dict[str, Any]] = []
        for x in rows or []:
            if not isinstance(x, dict):
                continue
            pid = str(x.get("PointId", "") or x.get("point_id", "") or "")
            if not pid:
                continue
            row = {
                "PointId": pid,
                "Qtext": str(x.get("Qtext", "") or ""),
                "Choice1": str(x.get("Choice1", "") or ""),
                "Choice2": str(x.get("Choice2", "") or ""),
                "Choice3": str(x.get("Choice3", "") or ""),
                "Choice4": str(x.get("Choice4", "") or ""),
                "Correct": str(x.get("Correct", "") or ""),
            }
            if row["Qtext"] or row["Correct"]:
                out.append(row)
        return out

    def _run_web_chunk_flashcards(
        self,
        chunk_rows: List[Dict[str, Any]],
        stage_f_json_str: str,
        part_num: int,
        total_parts: int,
        prompt_body: str,
        model_name: str,
        cancel_check: Optional[Callable[[], bool]],
    ) -> Optional[List[Dict[str, Any]]]:
        logger = self.logger
        if cancel_check and cancel_check():
            return None
        if not chunk_rows:
            return []

        slim_rows = [sh_web_slim_lesson_row(r) for r in chunk_rows if isinstance(r, dict)]
        full_prompt = sh_web_build_user_prompt(
            prompt_body, part_num, total_parts, stage_f_json_str, slim_rows
        )
        prompt_chars = len(full_prompt)
        max_out = sh_web_max_tokens_for_chunk(len(chunk_rows))
        sh_web_log(
            logger,
            "chunk_begin "
            f"part={part_num}/{total_parts} lesson_rows={len(chunk_rows)} "
            f"prompt_chars={prompt_chars} max_tokens={max_out}",
        )

        for attempt in range(1, self.STAGE_H_WEB_LLM_PARSE_RETRIES + 1):
            if cancel_check and cancel_check():
                return None

            attempt_prompt = full_prompt
            if attempt >= 2:
                attempt_prompt = full_prompt + _SH_WEB_JSON_RETRY_SUFFIX
            last_attempt = attempt >= self.STAGE_H_WEB_LLM_PARSE_RETRIES
            use_content_only = not last_attempt
            use_reasoning_none = not last_attempt

            try:
                resp = self.api_client.process_text(
                    text=attempt_prompt,
                    system_prompt=None,
                    model_name=model_name,
                    temperature=APIConfig.DEFAULT_TEMPERATURE,
                    max_tokens=max_out,
                    reasoning_effort_none=use_reasoning_none,
                    content_only=use_content_only,
                )
            except Exception as e:
                if sj_web_is_context_limit_error(e):
                    sh_web_log(
                        logger,
                        f"chunk_fail reason=context_limit part={part_num}/{total_parts} prompt_chars={prompt_chars}",
                    )
                    logger.error("%s context_limit_error: %s", STAGE_H_WEB_LOG_PREFIX, e)
                    return None
                sh_web_log(logger, f"chunk_fail reason=api_error part={part_num}/{total_parts} attempt={attempt}")
                logger.exception("%s api_error", STAGE_H_WEB_LOG_PREFIX)
                return None

            if not (resp or "").strip():
                sh_web_log(
                    logger,
                    f"chunk_retry reason=empty_response part={part_num}/{total_parts} attempt={attempt}",
                )
                continue

            rows = self._parse_flashcard_rows_from_llm_response(resp)
            min_rows = max(1, int(len(chunk_rows) * 0.5))
            if rows and len(rows) >= min_rows:
                sh_web_log(
                    logger,
                    f"chunk_ok part={part_num}/{total_parts} parsed_rows={len(rows)} attempt={attempt}",
                )
                return rows

            rlen = len(resp)
            tail = resp[-500:] if rlen > 500 else resp
            reason = "too_few_rows" if rows else "unparseable_json"
            sh_web_log(
                logger,
                f"chunk_retry reason={reason} part={part_num}/{total_parts} attempt={attempt} "
                f"parsed_rows={len(rows)} need>={min_rows}",
            )
            logger.warning("%s parse_fail_tail part=%s/%s: %r", STAGE_H_WEB_LOG_PREFIX, part_num, total_parts, tail)

        sh_web_log(
            logger,
            f"chunk_fail reason=parse_exhausted part={part_num}/{total_parts} retries={self.STAGE_H_WEB_LLM_PARSE_RETRIES}",
        )
        return None

    def process_stage_h_web_two_json(
        self,
        tagged_json_path: str,
        catalog_json_path: str,
        prompt: str,
        model_name: str,
        output_dir: Optional[str] = None,
        progress_callback: Optional[Callable[[str], None]] = None,
        cancel_check: Optional[Callable[[], bool]] = None,
        chunk_size: Optional[int] = None,
        max_parallel_chunks: Optional[int] = None,
    ) -> Optional[str]:
        """Web Stage H: tagged a*.json + image catalog f*.json → parallel chunks → ac*.json."""
        chunk_sz = chunk_size if chunk_size is not None else self.STAGE_H_WEB_CHUNK_SIZE
        max_par = max_parallel_chunks if max_parallel_chunks is not None else self.STAGE_H_WEB_PARALLEL_CHUNKS

        def _progress(msg: str) -> None:
            if progress_callback:
                progress_callback(msg)
            self.logger.info(msg)

        if hasattr(self.api_client, "set_stage"):
            self.api_client.set_stage("stage_h")

        if cancel_check and cancel_check():
            return None

        _progress("Starting Web Flashcard Generation (parallel chunks)...")
        sh_web_log(
            self.logger,
            f"job_begin model={model_name!r} chunk_size={chunk_sz} max_parallel={max_par}",
        )

        stage_j_data = self.load_json_file(tagged_json_path)
        if not stage_j_data:
            self.logger.error("Failed to load tagged JSON")
            return None
        stage_j_records = self.get_data_from_json(stage_j_data)
        if not stage_j_records:
            self.logger.error("Tagged JSON has no data")
            return None

        first_point_id = stage_j_records[0].get("PointId")
        if not first_point_id:
            self.logger.error("No PointId in tagged JSON")
            return None
        try:
            book_id, chapter_id = self.extract_book_chapter_from_pointid(first_point_id)
        except ValueError as e:
            self.logger.error("Error extracting book/chapter: %s", e)
            return None

        chapter_name = ""
        stage_j_metadata = self.get_metadata_from_json(stage_j_data)
        chapter_name = (
            stage_j_metadata.get("chapter", "")
            or stage_j_metadata.get("Chapter", "")
            or stage_j_metadata.get("chapter_name", "")
            or stage_j_metadata.get("Chapter_Name", "")
            or ""
        )
        if not chapter_name and stage_j_records:
            chapter_name = stage_j_records[0].get("chapter", "") or ""
        chapter_name_clean = ""
        if chapter_name:
            chapter_name_clean = re.sub(r'[<>:"/\\|?*]', "_", chapter_name)
            chapter_name_clean = chapter_name_clean.replace(" ", "_")
            chapter_name_clean = re.sub(r"_+", "_", chapter_name_clean).strip("_")

        stage_f_data = self.load_json_file(catalog_json_path)
        if not stage_f_data:
            self.logger.error("Failed to load image catalog JSON")
            return None
        stage_f_records = self.get_data_from_json(stage_f_data)
        if not stage_f_records:
            self.logger.warning("Image catalog JSON has no rows")
            stage_f_records = []
        stage_f_json_str = json.dumps(stage_f_records, ensure_ascii=False, separators=(",", ":"))

        _progress(f"Loaded tagged rows={len(stage_j_records)}, catalog rows={len(stage_f_records)}")

        n_chunks = max(1, math.ceil(len(stage_j_records) / chunk_sz))
        _progress(f"Chunk size={chunk_sz}, chunks={n_chunks}, parallel workers={min(max_par, n_chunks)}")

        chunks: List[List[Dict[str, Any]]] = []
        for i in range(0, len(stage_j_records), chunk_sz):
            chunks.append(stage_j_records[i : i + chunk_sz])

        nw = min(max_par, len(chunks)) if chunks else 1
        chunk_responses: Dict[int, Optional[List[Dict[str, Any]]]] = {}

        with ThreadPoolExecutor(max_workers=nw) as executor:
            future_to_idx: Dict[Any, int] = {}
            for idx, chunk in enumerate(chunks):
                if cancel_check and cancel_check():
                    _progress("Cancelled before scheduling chunks.")
                    break
                chunk_rows = [r for r in chunk if isinstance(r, dict)]
                fut = executor.submit(
                    self._run_web_chunk_flashcards,
                    chunk_rows,
                    stage_f_json_str,
                    idx + 1,
                    len(chunks),
                    prompt,
                    model_name,
                    cancel_check,
                )
                future_to_idx[fut] = idx

            for fut in as_completed(future_to_idx):
                idx = future_to_idx[fut]
                try:
                    chunk_responses[idx] = fut.result()
                except Exception as e:
                    sh_web_log(self.logger, f"chunk_worker_crash part_index={idx} err={e!r}")
                    self.logger.exception("%s chunk_worker_crash idx=%s", STAGE_H_WEB_LOG_PREFIX, idx)
                    chunk_responses[idx] = None

        if cancel_check and cancel_check():
            return None

        failed_parts = sorted(i + 1 for i, rows in chunk_responses.items() if rows is None)
        if failed_parts:
            msg = (
                f"Flashcard generation stopped: chunk(s) {failed_parts} of {len(chunks)} failed. "
                f"Search logs for {STAGE_H_WEB_LOG_PREFIX}."
            )
            sh_web_log(self.logger, f"job_fail failed_chunks={failed_parts} total_chunks={len(chunks)}")
            _progress(msg)
            self.logger.error("%s %s", STAGE_H_WEB_LOG_PREFIX, msg)
            return None

        combined_model_data: List[Dict[str, Any]] = []
        for idx in sorted(chunk_responses.keys()):
            part_rows = chunk_responses.get(idx)
            if part_rows:
                combined_model_data.extend(part_rows)
                _progress(f"Chunk {idx + 1}/{len(chunks)}: collected {len(part_rows)} flashcard row(s)")

        if not combined_model_data:
            sh_web_log(self.logger, "job_fail reason=no_rows_after_merge")
            return None

        pointid_to_flashcard: Dict[str, Dict[str, Any]] = {}
        for record in combined_model_data:
            point_id = str(record.get("PointId", "") or "")
            if point_id:
                pointid_to_flashcard[point_id] = record

        matched_count = 0
        flashcard_records: List[Dict[str, Any]] = []
        for record in stage_j_records:
            if not isinstance(record, dict):
                continue
            point_id = str(record.get("PointId", "") or "")
            fc = pointid_to_flashcard.get(point_id, {})
            flashcard_record = {
                "PointId": point_id,
                "chapter": record.get("chapter", ""),
                "subchapter": record.get("subchapter", ""),
                "topic": record.get("topic", ""),
                "subtopic": record.get("subtopic", ""),
                "subsubtopic": record.get("subsubtopic", ""),
                "Points": record.get("Points", record.get("points", "")),
                "Type": record.get("Type", ""),
                "Imp": record.get("Imp", ""),
                "Qtext": fc.get("Qtext", ""),
                "Choice1": fc.get("Choice1", ""),
                "Choice2": fc.get("Choice2", ""),
                "Choice3": fc.get("Choice3", ""),
                "Choice4": fc.get("Choice4", ""),
                "Correct": fc.get("Correct", ""),
                "Mainanswer": "زیرعنوان",
            }
            if fc.get("Qtext") or fc.get("Correct"):
                matched_count += 1
            flashcard_records.append(flashcard_record)

        _progress(f"Merged {len(flashcard_records)} records; matched flashcards for {matched_count}")

        out_dir = output_dir or os.path.dirname(tagged_json_path) or os.getcwd()
        if chapter_name_clean:
            base_filename = f"ac{book_id:03d}{chapter_id:03d}+{chapter_name_clean}.json"
        else:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            base_filename = f"ac{book_id:03d}{chapter_id:03d}+{timestamp}.json"
        output_path = os.path.join(out_dir, base_filename)
        if os.path.exists(output_path) and chapter_name_clean:
            counter = 1
            while os.path.exists(output_path):
                base_filename = f"ac{book_id:03d}{chapter_id:03d}+{chapter_name_clean}_{counter}.json"
                output_path = os.path.join(out_dir, base_filename)
                counter += 1

        output_metadata = {
            "book_id": book_id,
            "chapter_id": chapter_id,
            "source_stage_j": os.path.basename(tagged_json_path),
            "source_stage_f": os.path.basename(catalog_json_path),
            "model_used": model_name,
            "stage_h_call_mode": "parallel_chunks",
            "stage_h_web_strategy": "single_llm_call_per_chunk_parse_retries",
            "chunk_size": chunk_sz,
            "parallel_workers_used": nw,
            "total_records": len(flashcard_records),
            "records_with_flashcards": matched_count,
        }

        _progress(f"Saving flashcard output to: {output_path}")
        success = self.save_json_file(flashcard_records, output_path, output_metadata, "H")
        if success:
            sh_web_log(
                self.logger,
                f"job_ok path={os.path.basename(output_path)} chunks={len(chunks)} "
                f"flashcard_rows={len(combined_model_data)} merged={len(flashcard_records)}",
            )
            _progress(f"Web Flashcard Generation completed: {output_path}")
            return output_path
        sh_web_log(self.logger, f"job_fail reason=save_failed path={os.path.basename(output_path)}")
        return None

    def process_stage_h(
        self,
        stage_j_path: str,
        stage_f_path: str,
        prompt: str,
        model_name: str,
        output_dir: Optional[str] = None,
        progress_callback: Optional[Callable[[str], None]] = None
    ) -> Optional[str]:
        """
        Process Stage H: Generate flashcards from Stage J and Stage F data.
        
        Args:
            stage_j_path: Path to Stage J JSON file (a{book}{chapter}.json)
            stage_f_path: Path to Stage F JSON file (f.json)
            prompt: User prompt for flashcard generation
            model_name: Gemini model name
            output_dir: Output directory (defaults to stage_j_path directory)
            progress_callback: Optional callback for progress updates
            
        Returns:
            Path to output file (ac{book}{chapter}.json) or None on error
        """
        def _progress(msg: str):
            if progress_callback:
                progress_callback(msg)
            self.logger.info(msg)
        
        # Set stage if using UnifiedAPIClient (for API routing)
        if hasattr(self.api_client, 'set_stage'):
            self.api_client.set_stage("stage_h")
        
        _progress("Starting Stage H processing...")
        
        # Load Stage J JSON (without Imp column)
        _progress("Loading Stage J JSON...")
        stage_j_data = self.load_json_file(stage_j_path)
        if not stage_j_data:
            self.logger.error("Failed to load Stage J JSON")
            return None
        
        # Get data from Stage J
        stage_j_records = self.get_data_from_json(stage_j_data)
        if not stage_j_records:
            self.logger.error("Stage J JSON has no data")
            return None
        
        _progress(f"Loaded {len(stage_j_records)} records from Stage J")
        
        # Keep all columns from Stage J (we'll merge with model output later)
        # For prompt, we'll use a simplified version without Imp column
        stage_j_records_for_prompt = []
        for record in stage_j_records:
            clean_record = {
                "PointId": record.get("PointId", ""),
                "chapter": record.get("chapter", ""),
                "subchapter": record.get("subchapter", ""),
                "topic": record.get("topic", ""),
                "subtopic": record.get("subtopic", ""),
                "subsubtopic": record.get("subsubtopic", ""),
                "Points": record.get("Points", record.get("points", ""))
            }
            stage_j_records_for_prompt.append(clean_record)
        
        _progress(f"Prepared {len(stage_j_records_for_prompt)} records from Stage J for prompt (without Imp column)")
        
        # Extract book and chapter from first PointId
        first_point_id = stage_j_records[0].get("PointId")
        if not first_point_id:
            self.logger.error("No PointId found in Stage J data")
            return None
        
        try:
            book_id, chapter_id = self.extract_book_chapter_from_pointid(first_point_id)
        except ValueError as e:
            self.logger.error(f"Error extracting book/chapter: {e}")
            return None
        
        _progress(f"Detected Book ID: {book_id}, Chapter ID: {chapter_id}")
        
        # Extract chapter name from Stage J metadata
        chapter_name = ""
        stage_j_metadata = self.get_metadata_from_json(stage_j_data)
        chapter_name = (
            stage_j_metadata.get("chapter", "") or
            stage_j_metadata.get("Chapter", "") or
            stage_j_metadata.get("chapter_name", "") or
            stage_j_metadata.get("Chapter_Name", "") or
            ""
        )
        
        # If not found in metadata, try to get from first record
        if not chapter_name and stage_j_records:
            chapter_name = stage_j_records[0].get("chapter", "")
        
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
            _progress("No chapter name found, using empty string")
        
        # Load Stage F JSON
        _progress("Loading Stage F JSON...")
        stage_f_data = self.load_json_file(stage_f_path)
        if not stage_f_data:
            self.logger.error("Failed to load Stage F JSON")
            return None
        
        # Get data from Stage F
        stage_f_records = self.get_data_from_json(stage_f_data)
        if not stage_f_records:
            self.logger.warning("Stage F JSON has no data (no images)")
            stage_f_records = []
        
        _progress(f"Loaded {len(stage_f_records)} records from Stage F")
        
        # Divide Stage J data into parts (smaller size for Stage H due to large output per flashcard)
        # Each flashcard has 7 fields (Qtext, Choice1-4, Correct) so we need smaller parts
        PART_SIZE = 100  # Reduced from 100 to avoid max_tokens limit
        total_records = len(stage_j_records_for_prompt)
        num_parts = math.ceil(total_records / PART_SIZE)
        
        _progress(f"Dividing Stage J data into {num_parts} parts (max {PART_SIZE} records per part)")
        
        # Split Stage J data into parts
        stage_j_parts = []
        for i in range(num_parts):
            start_idx = i * PART_SIZE
            end_idx = min((i + 1) * PART_SIZE, total_records)
            part_data = stage_j_records_for_prompt[start_idx:end_idx]
            stage_j_parts.append(part_data)
            _progress(f"Part {i+1}: {len(part_data)} records (indices {start_idx} to {end_idx-1})")
        
        # Prepare Stage F JSON string
        stage_f_json_str = json.dumps(stage_f_records, ensure_ascii=False, indent=2)
        
        # Prepare base prompt template
        base_prompt_template = f"""{prompt}

Stage F Data (Image files):
{stage_f_json_str}

Please analyze the Stage J data and Stage F data, and generate flashcards with questions and multiple choice answers.

Generate a JSON response with the following structure:
{{
  "data": [
    {{
      "PointId": "point_id_string",
      "Qtext": "question_text",
      "Choice1": "option_1_text",
      "Choice2": "option_2_text",
      "Choice3": "option_3_text",
      "Choice4": "option_4_text",
      "Correct": "correct_choice_number_1_to_4"
    }},
    ...
  ]
}}

IMPORTANT:
- Use EXACT field names: PointId, Qtext, Choice1, Choice2, Choice3, Choice4, Correct
- PointId must be a STRING matching exactly the PointId from Stage J data
- Qtext: The flashcard question text
- Choice1, Choice2, Choice3, Choice4: Four multiple choice options
- Correct: The number (1, 2, 3, or 4) indicating which choice is correct
- Generate meaningful flashcard questions based on the Points content
- Use Stage F data (image files) as context if relevant

Return ONLY valid JSON, no additional text."""
        
        # Prepare paths: incremental parts file (like document processing) and raw TXT
        base_dir = os.path.dirname(stage_j_path) or os.getcwd()
        base_name, _ = os.path.splitext(os.path.basename(stage_j_path))
        parts_path = os.path.join(base_dir, f"{base_name}_stage_h_parts.json")
        txt_path = os.path.join(base_dir, f"{base_name}_stage_h.txt")
        
        # Initialize incremental parts file (write per-part immediately after each response)
        initial_parts_data = {
            "metadata": {
                "book_id": book_id,
                "chapter_id": chapter_id,
                "source_stage_j": os.path.basename(stage_j_path),
                "source_stage_f": os.path.basename(stage_f_path),
                "model_used": model_name,
                "num_parts": num_parts,
                "part_size": PART_SIZE,
                "processing_status": "in_progress",
            },
            "parts": [],
        }
        try:
            with open(parts_path, "w", encoding="utf-8") as f:
                json.dump(initial_parts_data, f, ensure_ascii=False, indent=2)
            _progress(f"Initialized incremental parts file: {os.path.basename(parts_path)}")
        except Exception as e:
            self.logger.error(f"Failed to create parts file: {e}")
            return None
        
        # Open TXT for appending raw responses (backup)
        txt_file_handle = None
        try:
            txt_file_handle = open(txt_path, "w", encoding="utf-8")
        except Exception as e:
            self.logger.warning(f"Could not open TXT file for raw responses: {e}")
        
        max_retries = 3
        
        for part_num, part_data in enumerate(stage_j_parts, 1):
            _progress("=" * 60)
            _progress(f"Processing Part {part_num}/{num_parts} ({len(part_data)} records)...")
            _progress("=" * 60)
            
            part_json_str = json.dumps(part_data, ensure_ascii=False, indent=2)
            part_prompt = f"""{base_prompt_template}

Stage J Data - Part {part_num}/{num_parts} (without Imp and Type columns):
{part_json_str}"""
            
            part_response = None
            for attempt in range(max_retries):
                try:
                    # Stage H needs more tokens per part because each flashcard has 7 fields
                    # Calculate max_tokens: PART_SIZE * avg_tokens_per_flashcard * safety_factor
                    # Each flashcard: ~200-300 tokens (Qtext + 4 choices + metadata)
                    # Safety factor: 1.5x to handle variations
                    estimated_tokens_per_flashcard = 250
                    stage_h_max_tokens = min(
                        int(len(part_data) * estimated_tokens_per_flashcard * 1.5),
                        APIConfig.DEFAULT_MAX_TOKENS * 2  # Cap at 2x default (32768 for gemini-2.5)
                    )
                    # Cap by provider: DeepSeek 4096, OpenRouter/GLM 65536 (GLM-5 supports ~131K)
                    if hasattr(self.api_client, 'get_client_for_stage'):
                        from deepseek_api_client import DeepSeekAPIClient
                        from openrouter_api_client import OpenRouterAPIClient
                        client = self.api_client.get_client_for_stage()
                        if isinstance(client, DeepSeekAPIClient):
                            stage_h_max_tokens = min(stage_h_max_tokens, APIConfig.DEFAULT_DEEPSEEK_MAX_TOKENS)
                        elif isinstance(client, OpenRouterAPIClient):
                            stage_h_max_tokens = min(stage_h_max_tokens, getattr(APIConfig, 'DEFAULT_OPENROUTER_MAX_TOKENS', 65536))
                    
                    self.logger.info(f"Part {part_num}: Using max_tokens={stage_h_max_tokens} for {len(part_data)} records")
                    
                    part_response = self.api_client.process_text(
                        text=part_prompt,
                        system_prompt=None,
                        model_name=model_name,
                        temperature=APIConfig.DEFAULT_TEMPERATURE,
                        max_tokens=stage_h_max_tokens
                    )
                    if part_response:
                        _progress(f"Part {part_num} response received ({len(part_response)} characters)")
                        break
                except Exception as e:
                    self.logger.warning(f"Part {part_num} model call attempt {attempt + 1} failed: {e}")
                    if attempt < max_retries - 1:
                        _progress(f"Retrying Part {part_num}... (attempt {attempt + 2}/{max_retries})")
                    else:
                        self.logger.error(f"All Part {part_num} model call attempts failed")
                        _progress(f"Error: Failed to get response for Part {part_num}. Skipping this part.")
                        part_response = None
            
            if not part_response:
                self.logger.error(f"No response from model for Part {part_num}")
                _progress(f"Error: No response for Part {part_num}. Skipping this part.")
                # Still append empty part to keep part numbers in sync
                try:
                    with open(parts_path, "r", encoding="utf-8") as f:
                        current_data = json.load(f)
                    current_data["parts"].append({"part_num": part_num, "response_text": "", "data": None})
                    with open(parts_path, "w", encoding="utf-8") as f:
                        json.dump(current_data, f, ensure_ascii=False, indent=2)
                except Exception as e:
                    self.logger.warning(f"Failed to append empty part to parts file: {e}")
                continue
            
            # Save raw response to TXT immediately
            if txt_file_handle:
                try:
                    if part_num > 1:
                        txt_file_handle.write("\n\n")
                    txt_file_handle.write(part_response)
                    txt_file_handle.flush()
                except Exception as e:
                    self.logger.warning(f"Failed to write Part {part_num} to TXT: {e}")
            
            # Convert to JSON immediately (like document processing) and add to parts file
            _progress(f"Extracting JSON from Part {part_num} response...")
            part_output = self.extract_json_from_response(part_response)
            if not part_output:
                _progress(f"Trying to extract Part {part_num} JSON from text...")
                part_output = self.load_txt_as_json_from_text(part_response)
            
            part_data_list = None
            if part_output:
                part_data_list = self.get_data_from_json(part_output)
                if part_data_list:
                    _progress(f"Part {part_num}: Extracted {len(part_data_list)} records")
                else:
                    self.logger.warning(f"Part {part_num}: No data extracted from JSON")
            else:
                self.logger.warning(f"Part {part_num}: Failed to extract JSON")
            
            # Append to incremental parts file immediately
            try:
                with open(parts_path, "r", encoding="utf-8") as f:
                    current_data = json.load(f)
                current_data["parts"].append({
                    "part_num": part_num,
                    "response_text": part_response,
                    "data": part_data_list,
                })
                current_data["metadata"]["parts_processed"] = len(current_data["parts"])
                current_data["metadata"]["processed_at"] = datetime.now().isoformat()
                with open(parts_path, "w", encoding="utf-8") as f:
                    json.dump(current_data, f, ensure_ascii=False, indent=2)
                self.logger.info(f"  ✓ Added Part {part_num} to parts file immediately ({len(part_data_list) if part_data_list else 0} records)")
            except Exception as e:
                self.logger.error(f"Failed to write Part {part_num} to parts file: {e}", exc_info=True)
        
        if txt_file_handle:
            try:
                txt_file_handle.close()
            except Exception:
                pass
            _progress(f"Saved raw model responses to: {txt_path}")
        
        # Build combined_model_data from parts file (no re-parsing at end)
        _progress("Combining JSON from parts file...")
        try:
            with open(parts_path, "r", encoding="utf-8") as f:
                parts_data = json.load(f)
        except Exception as e:
            self.logger.error(f"Failed to read parts file: {e}")
            return None
        
        combined_model_data = []
        part_counts = []
        for p in parts_data.get("parts", []):
            data = p.get("data")
            if data:
                combined_model_data.extend(data)
                part_counts.append(f"Part {p['part_num']}: {len(data)}")
        
        _progress(f"Extracted {len(combined_model_data)} records from parts file ({', '.join(part_counts)})")
        
        if not combined_model_data:
            self.logger.error("No JSON data extracted from any part")
            return None
        
        model_data = combined_model_data
        
        # Create a mapping from model output
        pointid_to_flashcard = {}
        for record in model_data:
            point_id = record.get("PointId", "") or record.get("point_id", "")
            if point_id:
                point_id = str(point_id)
                pointid_to_flashcard[point_id] = record
        
        _progress(f"Created mapping for {len(pointid_to_flashcard)} PointIds from model output")
        
        # Merge Stage J data with model output
        # Final output should have 16 columns:
        # From file a: PointId, chapter, subchapter, topic, subtopic, subsubtopic, Points, Type, Imp
        # From model: PointId, Qtext, Choice1, Choice2, Choice3, Choice4, Correct
        # New column: Mainanswer (default: "زیرعنوان")
        _progress("Merging Stage J data with model output...")
        matched_count = 0
        flashcard_records = []
        
        for record in stage_j_records:
            point_id = str(record.get("PointId", ""))
            flashcard_data = pointid_to_flashcard.get(point_id, {})
            
            flashcard_record = {
                "PointId": point_id,
                "chapter": record.get("chapter", ""),
                "subchapter": record.get("subchapter", ""),
                "topic": record.get("topic", ""),
                "subtopic": record.get("subtopic", ""),
                "subsubtopic": record.get("subsubtopic", ""),
                "Points": record.get("Points", record.get("points", "")),
                "Type": record.get("Type", ""),
                "Imp": record.get("Imp", ""),
                "Qtext": flashcard_data.get("Qtext", ""),
                "Choice1": flashcard_data.get("Choice1", ""),
                "Choice2": flashcard_data.get("Choice2", ""),
                "Choice3": flashcard_data.get("Choice3", ""),
                "Choice4": flashcard_data.get("Choice4", ""),
                "Correct": flashcard_data.get("Correct", ""),
                "Mainanswer": "زیرعنوان"
            }
            
            # Track matches
            if flashcard_data.get("Qtext") or flashcard_data.get("Correct"):
                matched_count += 1
            
            flashcard_records.append(flashcard_record)
        
        _progress(f"Generated {len(flashcard_records)} flashcard records")
        _progress(f"Matched {matched_count} records with flashcard data from model")
        if matched_count == 0:
            self.logger.warning("No records matched! Check PointId format in model output vs Stage J")
        
        # Prepare output directory
        if not output_dir:
            output_dir = os.path.dirname(stage_j_path) or os.getcwd()
        
        # Generate output filename: ac{book}{chapter}+namechapter.json
        # If chapter name is empty, use timestamp to avoid overwriting
        if chapter_name_clean:
            base_filename = f"ac{book_id:03d}{chapter_id:03d}+{chapter_name_clean}.json"
        else:
            # Fallback if no chapter name: use timestamp to avoid overwriting
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            base_filename = f"ac{book_id:03d}{chapter_id:03d}+{timestamp}.json"
            self.logger.warning(f"No chapter name found, using timestamp in filename: {timestamp}")
        
        output_path = os.path.join(output_dir, base_filename)
        
        # Check if file already exists and add counter if needed
        if os.path.exists(output_path) and chapter_name_clean:
            # If file exists and we have chapter name, add counter
            counter = 1
            while os.path.exists(output_path):
                base_filename = f"ac{book_id:03d}{chapter_id:03d}+{chapter_name_clean}_{counter}.json"
                output_path = os.path.join(output_dir, base_filename)
                counter += 1
            if counter > 1:
                self.logger.info(f"File already exists, using counter: {counter - 1}")
        
        # Prepare metadata
        output_metadata = {
            "book_id": book_id,
            "chapter_id": chapter_id,
            "source_stage_j": os.path.basename(stage_j_path),
            "source_stage_f": os.path.basename(stage_f_path),
            "model_used": model_name,
            "stage_h_txt_file": os.path.basename(txt_path),
            "stage_h_parts_file": os.path.basename(parts_path),
            "total_records": len(flashcard_records),
            "records_with_flashcards": matched_count,
            "num_parts": num_parts,
            "part_size": PART_SIZE,
            "division_method": "dynamic_by_part_size",
            "incremental_write": True,
        }
        
        # Save JSON
        _progress(f"Saving Stage H output to: {output_path}")
        success = self.save_json_file(flashcard_records, output_path, output_metadata, "H")
        
        if success:
            _progress(f"Stage H completed successfully: {output_path}")
            return output_path
        else:
            self.logger.error("Failed to save Stage H output")
            return None


"""
Stage TA Processor: Table Notes Processing
Takes Stage E JSON (with PointId) and OCR Extraction JSON, generates table notes,
and merges them with Stage E data.
"""

import json
import logging
import math
import os
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Optional, Dict, List, Any, Callable, Tuple
from base_stage_processor import BaseStageProcessor
from api_layer import APIConfig


class StageTAProcessor(BaseStageProcessor):
    """Process Stage TA: Generate table notes and merge with Stage E data"""

    SUBCHAPTER_MODEL_MAX_ATTEMPTS = 6
    SUBCHAPTER_MODEL_TIMEOUT_S = 900.0
    SUBCHAPTER_RETRY_BACKOFF_CAP_S = 120.0
    # Table-note JSON arrays are large; low completion caps truncate mid-object → unparseable JSON.
    SUBCHAPTER_MODEL_MAX_COMPLETION_TOKENS = 32768
    # Default: one LLM call per Stage-E topic bucket (with tables), batched like Stage E image notes.
    STAGE_TA_TOPIC_BATCH_SIZE = 10

    def __init__(self, api_client):
        super().__init__(api_client)
        self.logger = logging.getLogger(__name__)
        self._stage_ta_raw_lock = threading.Lock()
        self._stage_ta_api_lock = threading.Lock()

    def _tablepic_rows_from_parsed(self, tablepic_data: Any, persian_subchapter_name: str) -> List[Dict[str, Any]]:
        """Normalize model output (list or dict with data/rows/payload) to a list of row dicts."""
        self.logger.info(
            f"Processing tablepic_data for subchapter '{persian_subchapter_name}': type={type(tablepic_data)}"
        )
        if isinstance(tablepic_data, list):
            self.logger.info(f"tablepic_data is a list with {len(tablepic_data)} items")
            return tablepic_data
        if isinstance(tablepic_data, dict):
            self.logger.info(f"tablepic_data is a dict with keys: {list(tablepic_data.keys())}")
            rows = tablepic_data.get("data", tablepic_data.get("rows", tablepic_data.get("payload", [])))
            self.logger.info(
                f"Extracted records from dict: {len(rows) if isinstance(rows, list) else 'not a list'} items"
            )
            if isinstance(rows, list) and rows:
                return rows
            for key, value in tablepic_data.items():
                if isinstance(value, list) and value:
                    self.logger.info(f"  Found list in key '{key}' with {len(value)} items")
                    return value
            self.logger.warning("No list found in any dict value")
            return []
        self.logger.warning(f"Unexpected JSON structure from model: {type(tablepic_data)}")
        return []

    @staticmethod
    def _is_openrouter_context_limit_error(exc: BaseException) -> bool:
        """True when provider rejects prompt+completion budget as over context window."""
        msg = str(exc).lower()
        if "maximum context length" in msg:
            return True
        if "maximum context" in msg and "token" in msg:
            return True
        if "context length" in msg and ("exceed" in msg or "reduce" in msg):
            return True
        if "you requested about" in msg and "tokens" in msg:
            return True
        return False

    @staticmethod
    def _points_grouped_by_topic_in_order(
        points: List[Dict[str, Any]],
    ) -> List[Tuple[str, List[Dict[str, Any]]]]:
        """Stable first-seen topic grouping for fallback topic-by-topic calls."""
        order: List[str] = []
        buckets: Dict[str, List[Dict[str, Any]]] = {}
        for p in points:
            key = (p.get("topic") or "").strip() or "(بدون مبحث)"
            if key not in buckets:
                order.append(key)
                buckets[key] = []
            buckets[key].append(p)
        return [(k, buckets[k]) for k in order]

    @staticmethod
    def _tables_for_topic(
        tables: List[Dict[str, Any]], topic_name: str
    ) -> List[Dict[str, Any]]:
        return [
            t
            for t in tables
            if (t.get("topic") or "").strip() == topic_name
        ]

    def _build_stage_ta_prompt(
        self,
        *,
        prompt_with_subchapter: str,
        ocr_extraction_json_str: str,
        persian_subchapter_name: str,
        stage_e_points: List[Dict[str, Any]],
        subchapter_tables: List[Dict[str, Any]],
        scope_note: str = "",
    ) -> str:
        stage_e_json_str = json.dumps(stage_e_points, ensure_ascii=False, separators=(",", ":"))
        tables_json_str = json.dumps(subchapter_tables, ensure_ascii=False, indent=2)
        note = scope_note.strip()
        if note and not note.startswith("\n"):
            note = "\n\n" + note
        if note and not note.endswith("\n"):
            note = note + "\n"
        return f"""{prompt_with_subchapter}{note}

==================================================
فایل JSON متن درسی استخراج‌شده از کتاب درماتولوژی (OCR Extraction JSON — فقط این زیرفصل):
==================================================
{ocr_extraction_json_str}

==================================================
فایل JSON ساختار سلسله‌مراتبی درسنامه نهایی (Stage E JSON — زیرفصل '{persian_subchapter_name}' — {len(stage_e_points)} نقطه):
==================================================
{stage_e_json_str}

==================================================
جداول استخراج شده از OCR (Tables from OCR - {len(subchapter_tables)} tables):
==================================================
{tables_json_str}
"""

    def _append_stage_ta_raw_response_entry(
        self, output_path: str, entry: Dict[str, Any]
    ) -> None:
        """Append one raw model response to the Stage TA incremental JSON file (thread-safe)."""
        with self._stage_ta_raw_lock:
            with open(output_path, "r", encoding="utf-8") as f:
                current_data = json.load(f)
            current_data["raw_responses"].append(entry)
            current_data["metadata"]["subchapters_processed"] = len(current_data["raw_responses"])
            current_data["metadata"]["processed_at"] = datetime.now().isoformat()
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(current_data, f, ensure_ascii=False, indent=2)

    def _call_table_notes_llm_with_retries(
        self,
        *,
        full_prompt: str,
        model_name: str,
        attempt_label: str,
        _progress: Callable[[str], None],
    ) -> Tuple[Optional[str], Optional[Any], bool]:
        response_text: Optional[str] = None
        tablepic_data: Optional[Any] = None

        for attempt in range(1, self.SUBCHAPTER_MODEL_MAX_ATTEMPTS + 1):
            if attempt > 1:
                backoff = min(
                    8.0 * (2 ** (attempt - 2)),
                    self.SUBCHAPTER_RETRY_BACKOFF_CAP_S,
                )
                _progress(
                    f"Waiting {backoff:.0f}s before retry "
                    f"(attempt {attempt}/{self.SUBCHAPTER_MODEL_MAX_ATTEMPTS}) [{attempt_label}]..."
                )
                time.sleep(backoff)
                _progress(
                    f"Retrying model call (attempt {attempt}/{self.SUBCHAPTER_MODEL_MAX_ATTEMPTS}) "
                    f"[{attempt_label}]..."
                )

            try:
                with self._stage_ta_api_lock:
                    response_text = self.api_client.process_text(
                        text=full_prompt,
                        system_prompt=None,
                        model_name=model_name,
                        temperature=APIConfig.DEFAULT_TEMPERATURE,
                        max_tokens=self.SUBCHAPTER_MODEL_MAX_COMPLETION_TOKENS,
                        timeout_s=self.SUBCHAPTER_MODEL_TIMEOUT_S,
                    )
            except Exception as e:
                if self._is_openrouter_context_limit_error(e):
                    self.logger.warning(
                        "Context window limit (%s, attempt %s/%s); not retrying same prompt: %s",
                        attempt_label,
                        attempt,
                        self.SUBCHAPTER_MODEL_MAX_ATTEMPTS,
                        e,
                    )
                    return None, None, True
                self.logger.warning(f"Error calling model (attempt {attempt}): {e}")
                response_text = None
                continue

            if not response_text:
                continue

            _progress("Extracting JSON from model response...")
            self.logger.info(f"Response text length: {len(response_text)} chars")
            self.logger.debug(f"Response text preview: {response_text[:200]}")
            tablepic_data = self.extract_json_from_response(response_text)
            self.logger.info(
                f"Extracted tablepic_data type: {type(tablepic_data)}, value: {tablepic_data}"
            )
            if not tablepic_data:
                self.logger.warning(
                    "First extraction failed, trying load_txt_as_json_from_text..."
                )
                tablepic_data = self.load_txt_as_json_from_text(response_text)
                self.logger.info(
                    f"Second extraction result type: {type(tablepic_data)}, value: {tablepic_data}"
                )
            if not tablepic_data and response_text:
                tail = (
                    response_text[-800:]
                    if len(response_text) > 800
                    else response_text
                )
                self.logger.warning(
                    "Model returned non-parseable JSON (%d chars). Often truncation or unescaped "
                    "quotes in strings. Response tail: %r",
                    len(response_text),
                    tail,
                )

            if tablepic_data:
                _progress(f"Successfully extracted JSON (attempt {attempt}) [{attempt_label}]")
                self.logger.info(
                    f"Successfully extracted JSON: {type(tablepic_data)} with keys: "
                    f"{tablepic_data.keys() if isinstance(tablepic_data, dict) else 'N/A'}"
                )
                return response_text, tablepic_data, False

            _progress(f"JSON extraction failed (attempt {attempt}), retrying... [{attempt_label}]")
            self.logger.warning(
                f"Failed to extract JSON from response (attempt {attempt})"
            )

        return response_text, tablepic_data, False

    def _run_stage_ta_single_topic(
        self,
        topic_index: int,
        topic_name: str,
        pts: List[Dict[str, Any]],
        *,
        prompt_with_subchapter: str,
        persian_subchapter_name: str,
        ocr_extraction_data: Dict[str, Any],
        subchapter_tables: List[Dict[str, Any]],
        model_name: str,
        output_path: str,
        part_num: int,
        _progress: Callable[[str], None],
        raw_response_kind: str = "topic_parallel",
    ) -> Tuple[int, str, List[Dict[str, Any]], Optional[str]]:
        """
        One LLM call for a single Stage-E topic bucket (topic-scoped OCR + topic points + topic tables).

        Returns (topic_index, topic_name, tablepic_rows, error_message_or_None).
        """
        if not pts:
            return topic_index, topic_name, [], None

        topic_tables = self._tables_for_topic(subchapter_tables, topic_name)
        if not topic_tables:
            self.logger.info(
                "Skipping topic %r for TA: no OCR tables tagged with this topic.",
                topic_name,
            )
            return topic_index, topic_name, [], None

        topic_ocr_slice = self._filter_ocr_extraction_for_subchapter_topic(
            ocr_extraction_data,
            persian_subchapter_name,
            topic_name,
        )
        topic_ocr_extraction_json_str = json.dumps(
            topic_ocr_slice, ensure_ascii=False, separators=(",", ":")
        )
        scope = (
            f"[محدوده مرجع: فقط مبحث «{topic_name}» در همین زیرفصل. "
            f"تعداد نقاط Stage E در این درخواست: {len(pts)}. "
            f"تعداد جدول‌های OCR در این درخواست: {len(topic_tables)}.]"
        )
        full_prompt = self._build_stage_ta_prompt(
            prompt_with_subchapter=prompt_with_subchapter,
            ocr_extraction_json_str=topic_ocr_extraction_json_str,
            persian_subchapter_name=persian_subchapter_name,
            stage_e_points=pts,
            subchapter_tables=topic_tables,
            scope_note=scope,
        )
        label = (
            f"topic «{topic_name}» ({len(pts)} Stage E row(s), {len(topic_tables)} OCR table(s))"
        )
        response_text, tablepic_data, ctx_hit = self._call_table_notes_llm_with_retries(
            full_prompt=full_prompt,
            model_name=model_name,
            attempt_label=label,
            _progress=_progress,
        )

        if tablepic_data:
            rows = self._tablepic_rows_from_parsed(tablepic_data, persian_subchapter_name)
            try:
                entry: Dict[str, Any] = {
                    "subchapter_index": part_num,
                    "subchapter": persian_subchapter_name,
                    "topic": topic_name,
                    "topic_index": topic_index,
                    "stage_e_point_count": len(pts),
                    "table_count": len(topic_tables),
                    "call_kind": raw_response_kind,
                    "response_text": response_text,
                    "response_size_bytes": len((response_text or "").encode("utf-8")),
                    "processed_at": datetime.now().isoformat(),
                }
                self._append_stage_ta_raw_response_entry(output_path, entry)
                self.logger.info(
                    "  ✓ Raw response (%s) for subchapter %r topic %r",
                    raw_response_kind,
                    persian_subchapter_name,
                    topic_name,
                )
            except Exception as e:
                self.logger.error(
                    "Failed to write raw response for %r / %r: %s",
                    persian_subchapter_name,
                    topic_name,
                    e,
                    exc_info=True,
                )
            return topic_index, topic_name, rows, None

        if ctx_hit:
            msg = (
                f"topic «{topic_name}»: context window exceeded "
                f"({len(pts)} Stage E row(s)); no bisect configured"
            )
            self.logger.error("Stage TA topic call: %s", msg)
            return topic_index, topic_name, [], msg

        msg = (
            f"topic «{topic_name}» ({len(pts)} rows): no model response / JSON after retries"
        )
        self.logger.error("Stage TA topic call: %s", msg)
        return topic_index, topic_name, [], msg

    def _process_subchapter_table_topics_parallel(
        self,
        topic_groups: List[Tuple[str, List[Dict[str, Any]]]],
        *,
        prompt_with_subchapter: str,
        persian_subchapter_name: str,
        ocr_extraction_data: Dict[str, Any],
        subchapter_tables: List[Dict[str, Any]],
        model_name: str,
        output_path: str,
        part_num: int,
        _progress: Callable[[str], None],
    ) -> Tuple[List[Dict[str, Any]], List[str]]:
        """Run table-note topic calls in bounded parallel batches; preserve topic order in merged rows."""
        n_topics = len(topic_groups)
        if n_topics == 0:
            return [], []

        results_by_idx: Dict[int, Tuple[str, List[Dict[str, Any]], Optional[str]]] = {}
        topic_errors: List[str] = []

        def _progress_log_only(msg: str) -> None:
            """Workers must not touch the Celery task SQLAlchemy Session (append_log)."""
            self.logger.info("%s", msg)

        batch_size = self.STAGE_TA_TOPIC_BATCH_SIZE
        total_batches = (n_topics + batch_size - 1) // batch_size
        for batch_num, start in enumerate(range(0, n_topics, batch_size), 1):
            batch = topic_groups[start : start + batch_size]
            bw = len(batch)
            _progress(
                f"Stage TA topic batch {batch_num}/{total_batches}: {bw} topic(s) "
                f"(concurrency={bw}) for '{persian_subchapter_name}'..."
            )
            with ThreadPoolExecutor(max_workers=bw) as executor:
                future_to_idx: Dict[Any, int] = {}
                for rel_i, (topic_name, pts) in enumerate(batch):
                    tidx = start + rel_i
                    fut = executor.submit(
                        self._run_stage_ta_single_topic,
                        tidx,
                        topic_name,
                        pts,
                        prompt_with_subchapter=prompt_with_subchapter,
                        persian_subchapter_name=persian_subchapter_name,
                        ocr_extraction_data=ocr_extraction_data,
                        subchapter_tables=subchapter_tables,
                        model_name=model_name,
                        output_path=output_path,
                        part_num=part_num,
                        _progress=_progress_log_only,
                        raw_response_kind="topic_parallel",
                    )
                    future_to_idx[fut] = tidx

                for fut in as_completed(future_to_idx):
                    tidx = future_to_idx[fut]
                    try:
                        ti, tn, rows, err = fut.result()
                    except Exception as e:
                        self.logger.error(
                            "Stage TA topic worker crashed (idx=%s): %s",
                            tidx,
                            e,
                            exc_info=True,
                        )
                        topic_name_guess = topic_groups[tidx][0] if 0 <= tidx < n_topics else "?"
                        results_by_idx[tidx] = (topic_name_guess, [], str(e))
                        topic_errors.append(f"topic idx {tidx}: worker error: {e}")
                        _progress(
                            f"  ✗ Topic (topic_parallel) idx {tidx} «{topic_name_guess}»: "
                            f"worker crashed: {e}"
                        )
                        continue
                    results_by_idx[ti] = (tn, rows, err)
                    pts = topic_groups[ti][1]
                    tt_count = len(self._tables_for_topic(subchapter_tables, tn))
                    if err:
                        topic_errors.append(err)
                        _progress(f"  ✗ Topic (topic_parallel): «{tn}» — {err}")
                    elif tt_count == 0:
                        _progress(
                            f"  ○ Topic skipped (topic_parallel): «{tn}» — no OCR tables for this topic"
                        )
                    else:
                        _progress(
                            f"  ✓ Topic call (topic_parallel): {len(rows)} table row(s) for "
                            f"«{tn}» ({len(pts)} Stage E point(s), {tt_count} OCR table(s))"
                        )

        merged: List[Dict[str, Any]] = []
        for i in range(n_topics):
            _tn, rows, _e = results_by_idx[i]
            merged.extend(rows)
        return merged, topic_errors

    def process_stage_ta(
        self,
        stage_e_path: str,
        ocr_extraction_json_path: str,
        prompt: str,
        model_name: str,
        output_dir: Optional[str] = None,
        progress_callback: Optional[Callable[[str], None]] = None
    ) -> Optional[str]:
        """
        Process Stage TA: Generate table notes and merge with Stage E.
        
        Args:
            stage_e_path: Path to Stage E JSON file (with PointId) - to get last PointId
            ocr_extraction_json_path: Path to OCR Extraction JSON file (for subchapter names and tables)
            prompt: User prompt for table notes generation
            model_name: Model name
            output_dir: Output directory (defaults to stage_e_path directory)
            progress_callback: Optional callback for progress updates
            
        Returns:
            Path to output file (ta{book}{chapter}_{base_name}.json) or None on error
            Example: ta105003_Lesson_file_1_1.json
        """
        def _progress(msg: str):
            if progress_callback:
                progress_callback(msg)
            self.logger.info(msg)
        
        # Set stage if using UnifiedAPIClient (for API routing)
        if hasattr(self.api_client, 'set_stage'):
            self.api_client.set_stage("stage_ta")
        
        _progress("Starting Stage TA processing...")
        
        # Load OCR Extraction JSON (Stage 1) - contains table descriptions
        _progress("Loading OCR Extraction JSON (Stage 1)...")
        ocr_extraction_data = self.load_json_file(ocr_extraction_json_path)
        if not ocr_extraction_data:
            self.logger.error("Failed to load OCR Extraction JSON")
            _progress("Error: Failed to load OCR Extraction JSON")
            return None
        
        # Load Stage E JSON (to get last PointId)
        _progress("Loading Stage E JSON...")
        stage_e_data = self.load_json_file(stage_e_path)
        if not stage_e_data:
            self.logger.error("Failed to load Stage E JSON")
            return None
        
        # Extract data from Stage E
        stage_e_points = self.get_data_from_json(stage_e_data)
        
        if not stage_e_points:
            self.logger.error("Stage E JSON has no data/points")
            _progress("Error: Stage E JSON has no data/points")
            return None
        
        # Extract book and chapter from first PointId in Stage E
        first_point_id = stage_e_points[0].get("PointId")
        if not first_point_id:
            self.logger.error("No PointId found in Stage E data")
            return None
        
        try:
            book_id, chapter_id = self.extract_book_chapter_from_pointid(first_point_id)
        except ValueError as e:
            self.logger.error(f"Error extracting book/chapter: {e}")
            return None
        
        _progress(f"Detected Book ID: {book_id}, Chapter ID: {chapter_id}")
        
        # Get last PointId from Stage E to continue numbering (after images)
        last_point = stage_e_points[-1]
        last_point_id = last_point.get("PointId", "")
        if not last_point_id:
            self.logger.error("No PointId in last point of Stage E")
            return None
        
        # Extract current index from last PointId
        try:
            current_index = int(last_point_id[6:10]) + 1  # Next index after last point
        except (ValueError, IndexError):
            self.logger.error(f"Invalid PointId format: {last_point_id}")
            return None
        
        _progress(f"Last PointId in Stage E: {last_point_id}, Starting table notes from index: {current_index}")
        
        # Extract subchapters from OCR Extraction JSON (Persian names)
        _progress("Extracting subchapters from OCR Extraction JSON...")
        ocr_subchapters = self._extract_subchapters_from_ocr(ocr_extraction_data)
        
        if not ocr_subchapters:
            self.logger.error("No subchapters found in OCR Extraction JSON")
            _progress("Error: No subchapters found in OCR Extraction JSON")
            return None
        
        num_parts = len(ocr_subchapters)
        _progress(f"Found {num_parts} subchapters to process")
        
        # Prepare output directory
        base_dir = os.path.dirname(stage_e_path) or os.getcwd()
        base_name, _ = os.path.splitext(os.path.basename(stage_e_path))
        # Remove 'e' prefix if present (e.g., e105003_Lesson_file_1_1 -> 105003_Lesson_file_1_1)
        if base_name.startswith('e') and len(base_name) > 1 and base_name[1:4].isdigit():
            base_name = base_name[1:]
        
        # Generate output filename with base_name to ensure uniqueness per chapter
        # Format: ta{book}{chapter}_{base_name}.json (e.g., ta105003_Lesson_file_1_1.json)
        output_filename = f"ta{book_id:03d}{chapter_id:03d}_{base_name}.json"
        output_path = os.path.join(base_dir, output_filename)
        
        # Initialize output JSON file immediately (incremental writing)
        _progress("Initializing output JSON file...")
        initial_output = {
            "metadata": {
                "processing_status": "in_progress",
                "subchapters_processed": 0,
                "total_subchapters": num_parts,
                "book_id": book_id,
                "chapter_id": chapter_id,
                "source_stage_e": os.path.basename(stage_e_path),
                "source_ocr_extraction": os.path.basename(ocr_extraction_json_path),
                "model_used": model_name,
                "division_method": "ocr_extraction_by_subchapter",
                "ocr_subchapters": ocr_subchapters,
                "stage": "TA",
                "stage_ta_call_mode": "topic_parallel",
            },
            "data": [],  # Will contain merged points at the end
            "raw_responses": []  # All raw model responses
        }
        
        try:
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(initial_output, f, ensure_ascii=False, indent=2)
            self.logger.info(f"Initialized Stage TA JSON file: {output_path}")
        except Exception as e:
            self.logger.error(f"Failed to initialize output file: {e}")
            return None
        
        # Extract tables from OCR Extraction JSON (only type="table")
        _progress("Extracting tables from OCR Extraction JSON...")
        ocr_tables_data = self._extract_tables_from_ocr(ocr_extraction_data)
        
        if not ocr_tables_data:
            self.logger.warning("No tables found in OCR Extraction JSON")
            _progress("Warning: No tables found in OCR Extraction JSON")
        
        all_tablepic_records = []  # Collect all tablepic records from all subchapters
        subchapter_errors: List[str] = []  # Non-empty => abort — avoids marking job succeeded with partial output

        # Process each subchapter individually
        for part_num, persian_subchapter_name in enumerate(ocr_subchapters, 1):
            _progress("=" * 60)
            _progress(f"Processing Part {part_num}/{num_parts} - Subchapter: '{persian_subchapter_name}'...")
            _progress("=" * 60)
            
            # Filter Stage E points for this subchapter only
            _progress(f"Filtering Stage E points for subchapter '{persian_subchapter_name}'...")
            filtered_stage_e_points = [
                point for point in stage_e_points 
                if point.get("subchapter", "").strip() == persian_subchapter_name
            ]
            
            _progress(f"Found {len(filtered_stage_e_points)} points for subchapter '{persian_subchapter_name}' (out of {len(stage_e_points)} total)")
            
            if not filtered_stage_e_points:
                self.logger.warning(f"No Stage E points found for subchapter '{persian_subchapter_name}'. Skipping this subchapter.")
                _progress(f"Warning: No Stage E points found for subchapter '{persian_subchapter_name}'. Skipping...")
                continue
            
            # Filter tables for this subchapter
            subchapter_tables = self._filter_tables_by_subchapter(ocr_tables_data, persian_subchapter_name)
            _progress(f"Found {len(subchapter_tables)} tables for subchapter '{persian_subchapter_name}'")
            
            if not subchapter_tables:
                self.logger.warning(f"No tables found for subchapter '{persian_subchapter_name}'. Skipping this subchapter.")
                _progress(f"Warning: No tables found for subchapter '{persian_subchapter_name}'. Skipping...")
                continue

            prompt_with_subchapter = prompt.replace("{SUBCHAPTER_NAME}", persian_subchapter_name)
            _progress(f"Using Persian subchapter name in prompt: '{persian_subchapter_name}'")

            topic_groups = self._points_grouped_by_topic_in_order(filtered_stage_e_points)
            if not topic_groups:
                self.logger.warning("No topic buckets for subchapter %r", persian_subchapter_name)
                _progress(f"Warning: No topic buckets for '{persian_subchapter_name}'. Skipping...")
                continue

            _progress(
                f"Stage TA topic-parallel: {len(topic_groups)} topic(s) for subchapter "
                f"'{persian_subchapter_name}' (batch size {self.STAGE_TA_TOPIC_BATCH_SIZE})..."
            )
            subchapter_tablepic_records, topic_errs = self._process_subchapter_table_topics_parallel(
                topic_groups,
                prompt_with_subchapter=prompt_with_subchapter,
                persian_subchapter_name=persian_subchapter_name,
                ocr_extraction_data=ocr_extraction_data,
                subchapter_tables=subchapter_tables,
                model_name=model_name,
                output_path=output_path,
                part_num=part_num,
                _progress=_progress,
            )

            if not subchapter_tablepic_records:
                detail = "; ".join(topic_errs) if topic_errs else "no topic returned usable rows"
                subchapter_errors.append(
                    f"{persian_subchapter_name}: topic-parallel produced no rows ({detail})"
                )
                self.logger.error("No table rows for subchapter %r", persian_subchapter_name)
                _progress(
                    f"Error: No usable table rows for subchapter '{persian_subchapter_name}'."
                )
                continue

            if topic_errs:
                for fe in topic_errs:
                    self.logger.warning(
                        "Partial topic issue for subchapter %r: %s",
                        persian_subchapter_name,
                        fe,
                    )

            all_tablepic_records.extend(subchapter_tablepic_records)
            _progress(
                f"Extracted {len(subchapter_tablepic_records)} table note records from "
                f"subchapter '{persian_subchapter_name}'"
            )
            
            # Add delay between parts to avoid rate limiting (429 errors)
            if part_num < num_parts:  # Don't delay after the last part
                delay_seconds = 5  # 5 seconds delay between parts
                _progress(f"Waiting {delay_seconds} seconds before processing next subchapter...")
                time.sleep(delay_seconds)

        if subchapter_errors:
            summary = "; ".join(subchapter_errors)
            self.logger.error("Stage TA aborted due to subchapter failure(s): %s", summary)
            _progress(
                "Error: Table notes incomplete — one or more subchapters failed (job not saved as success). "
                f"Failures: {summary}"
            )
            return None
        
        if not all_tablepic_records:
            self.logger.error("No tablepic records found from any subchapter")
            _progress("Error: No records found in model responses.")
            return None
        
        _progress(f"Total extracted {len(all_tablepic_records)} table note records from all subchapters")
        
        # Process tablepic records:
        # 1. Remove caption column
        # 2. Replace point_text with points from Stage E
        # 3. Assign PointId sequentially
        
        _progress("Processing table notes...")
        processed_table_notes = []
        first_table_point_id = None
        
        for idx, record in enumerate(all_tablepic_records):
            if not isinstance(record, dict):
                continue
            
            # Remove caption and prepare new record
            processed_record = {}
            
            # Copy all fields except caption and point_text
            # This includes topic, chapter, subchapter, subtopic, subsubtopic from model output
            for k, v in record.items():
                if k not in ["caption", "point_text"]:
                    processed_record[k] = v
            
            # Convert point_text to Points
            # Get point_text value from the record (this is the table reference like "جدول 30:3")
            point_text_value = record.get("point_text", "")
            
            # Set Points column with the point_text value
            processed_record["Points"] = point_text_value
            
            # Assign PointId
            point_id = f"{book_id:03d}{chapter_id:03d}{current_index:04d}"
            processed_record["PointId"] = point_id
            
            if first_table_point_id is None:
                first_table_point_id = point_id
            
            processed_table_notes.append(processed_record)
            current_index += 1
        
        _progress(f"Processed {len(processed_table_notes)} table notes")
        _progress(f"First table PointId: {first_table_point_id}")
        
        # Save tablepic JSON (with caption) for use in future stages
        # IMPORTANT: Save the ORIGINAL all_tablepic_records (with caption) before processing
        _progress("Saving tablepic JSON for future stages...")
        tablepic_json_path = os.path.join(base_dir, f"{base_name}_tablepic.json")
        tablepic_saved = False
        
        # Verify that all_tablepic_records contain caption before saving
        caption_count = 0
        for record in all_tablepic_records:
            if isinstance(record, dict) and record.get("caption"):
                caption_count += 1
        
        _progress(f"Found {caption_count} records with caption out of {len(all_tablepic_records)}")
        
        try:
            tablepic_metadata = {
                "book_id": book_id,
                "chapter_id": chapter_id,
                "total_records": len(all_tablepic_records),
                "records_with_caption": caption_count,
                "division_method": "ocr_extraction_by_subchapter",
                "ocr_subchapters": ocr_subchapters
            }
            # Save original all_tablepic_records with caption (before processing)
            success = self.save_json_file(all_tablepic_records, tablepic_json_path, tablepic_metadata, "tablepic")
            if success:
                _progress(f"Tablepic saved to: {tablepic_json_path}")
                tablepic_saved = True
                # Verify that caption exists in saved file
                try:
                    with open(tablepic_json_path, 'r', encoding='utf-8') as f:
                        saved_data = json.load(f)
                        saved_records = self.get_data_from_json(saved_data)
                        if saved_records:
                            first_record = saved_records[0]
                            if "caption" in first_record:
                                caption_value = first_record.get("caption", "")
                                _progress(f"Verified: tablepic contains caption field (first caption: {len(caption_value)} chars)")
                            else:
                                self.logger.warning(f"tablepic saved but caption not found. Keys: {list(first_record.keys())}")
                except Exception as e:
                    self.logger.warning(f"Could not verify tablepic JSON: {e}")
            else:
                self.logger.error("Failed to save tablepic JSON")
        except Exception as e:
            self.logger.error(f"Error saving tablepic JSON: {e}", exc_info=True)
            # Continue anyway
        
        # Merge Stage E points with table notes
        _progress("Merging Stage E points with table notes...")
        merged_points = stage_e_points + processed_table_notes
        
        # Final update: Add processed data and remove raw_responses
        try:
            with open(output_path, "r", encoding="utf-8") as f:
                current_data = json.load(f)
            
            current_data["data"] = merged_points
            current_data["metadata"]["processing_status"] = "completed"
            current_data["metadata"]["stage_e_total_points"] = len(stage_e_points)
            current_data["metadata"]["table_notes_count"] = len(processed_table_notes)
            current_data["metadata"]["first_table_point_id"] = first_table_point_id
            current_data["metadata"]["last_point_id"] = f"{book_id:03d}{chapter_id:03d}{(current_index - 1):04d}"
            current_data["metadata"]["tablepic_json_file"] = os.path.basename(tablepic_json_path) if tablepic_saved else None
            current_data["metadata"]["tablepic_json_path"] = tablepic_json_path if tablepic_saved else None
            current_data["metadata"]["processed_at"] = datetime.now().isoformat()
            
            # Get additional metadata from Stage E
            stage_e_metadata = self.get_metadata_from_json(stage_e_data)
            for k, v in stage_e_metadata.items():
                if k not in ["stage", "processed_at", "total_records"]:
                    current_data["metadata"][k] = v
            
            # Remove raw_responses from final output
            if "raw_responses" in current_data:
                del current_data["raw_responses"]
                self.logger.info("Removed raw_responses from final output file")
            
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(current_data, f, ensure_ascii=False, indent=2)
            
            _progress(f"Stage TA completed successfully: {output_path}")
            return output_path
        except Exception as e:
            self.logger.error(f"Failed to finalize Stage TA output: {e}", exc_info=True)
            return None
    
    def _extract_subchapters_from_ocr(self, ocr_extraction_data: Dict) -> List[str]:
        """
        Extract subchapter names (Persian) from OCR Extraction JSON.
        
        OCR Extraction JSON structure:
        {
          "chapters": [
            {
              "chapter": "...",
              "subchapters": [
                {
                  "subchapter": "...",  # Persian name
                  "topics": [...]
                }
              ]
            }
          ]
        }
        
        Returns:
            List of subchapter names (Persian) in order
        """
        subchapters = []
        chapters = ocr_extraction_data.get("chapters", [])
        
        for chapter_obj in chapters:
            if not isinstance(chapter_obj, dict):
                continue
            
            subchapters_list = chapter_obj.get("subchapters", [])
            for subchapter_obj in subchapters_list:
                if not isinstance(subchapter_obj, dict):
                    continue
                
                subchapter_name = subchapter_obj.get("subchapter", "").strip()
                if subchapter_name and subchapter_name not in subchapters:
                    subchapters.append(subchapter_name)
        
        return subchapters
    
    def _extract_tables_from_ocr(self, ocr_extraction_data: Dict) -> List[Dict[str, Any]]:
        """
        Extract all tables (type="table") from OCR Extraction JSON.
        
        OCR Extraction JSON structure:
        {
          "chapters": [
            {
              "chapter": "...",
              "subchapters": [
                {
                  "subchapter": "...",
                  "topics": [
                    {
                      "topic": "...",
                      "extractions": [
                        {
                          "type": "table",
                          "content": "..."
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
            List of table dictionaries with metadata (subchapter, topic, content)
        """
        tables = []
        chapters = ocr_extraction_data.get("chapters", [])
        
        for chapter_obj in chapters:
            if not isinstance(chapter_obj, dict):
                continue
            
            chapter_name = chapter_obj.get("chapter", "")
            subchapters = chapter_obj.get("subchapters", [])
            
            for subchapter_obj in subchapters:
                if not isinstance(subchapter_obj, dict):
                    continue
                
                subchapter_name = subchapter_obj.get("subchapter", "")
                topics = subchapter_obj.get("topics", [])
                
                for topic_obj in topics:
                    if not isinstance(topic_obj, dict):
                        continue
                    
                    topic_name = topic_obj.get("topic", "")
                    extractions = topic_obj.get("extractions", [])
                    
                    if not isinstance(extractions, list):
                        continue
                    
                    for extraction in extractions:
                        if not isinstance(extraction, dict):
                            continue
                        
                        # Only extract tables (type="table")
                        extraction_type = extraction.get("type", "").lower()
                        if extraction_type == "table":
                            table_data = {
                                "chapter": chapter_name,
                                "subchapter": subchapter_name,
                                "topic": topic_name,
                                "type": extraction.get("type", "table"),
                                "content": extraction.get("content", extraction.get("Content", "")),
                                # Also include other fields if present
                                "extraction": extraction.get("Extraction", extraction.get("extraction", ""))
                            }
                            tables.append(table_data)
        
        return tables
    
    def _filter_tables_by_subchapter(self, tables: List[Dict[str, Any]], subchapter_name: str) -> List[Dict[str, Any]]:
        """
        Filter tables by subchapter name.
        
        Args:
            tables: List of table dictionaries
            subchapter_name: Subchapter name to filter by
            
        Returns:
            Filtered list of tables
        """
        return [
            table for table in tables
            if table.get("subchapter", "").strip() == subchapter_name
        ]

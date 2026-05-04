"""
Stage V Processor: Test File Generation
Generates test files from Stage J data and Word document in two steps:
Step 1: Generate initial test questions ONCE with full Stage J JSON + full test file + Step 1 prompt.
Step 2: Refine questions and add QId (per topic, using filtered Step 1 output).
"""

import json
import logging
import os
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime
from typing import Optional, Dict, List, Any, Callable, Tuple
from base_stage_processor import BaseStageProcessor
from word_file_processor import WordFileProcessor
from api_layer import APIConfig
from openrouter_api_client import OpenRouterRequestAborted


@dataclass
class StageVProcessingContext:
    """Shared context for Stage V Step 1 / Step 2 (built from Stage J + Word paths)."""

    stage_j_path: str
    word_file_path: str
    output_dir_final: str
    book_id: int
    chapter_id: int
    stage_j_records: List[Any]
    stage_j_records_for_prompt: List[Dict[str, Any]]
    topics_list: List[Tuple[str, str, str]]
    full_stage_j_json: str


class StageVProcessor(BaseStageProcessor):
    """Process Stage V: Generate test files from Stage J and Word document"""
    STEP2_BATCH_SIZE = 10
    # Step 1/2 return large JSON arrays; use OpenRouter-style ceiling (~131K) so long test banks are not cut off.
    _STAGE_V_OUTPUT_MAX_TOKENS = APIConfig.OPENROUTER_OUTPUT_TOKEN_CEILING

    def __init__(self, api_client):
        super().__init__(api_client)
        self.logger = logging.getLogger(__name__)
        self.word_processor = WordFileProcessor()

    def _extract_stage_v_question_rows_from_model_text(self, text: str) -> List[Dict[str, Any]]:
        """
        Collect question rows from a model response. Merges every JSON block (models often emit
        multiple ```json``` chunks); falls back to extract_json_from_response / load_txt_as_json_from_text.
        """
        if not (text or "").strip():
            return []
        blocks = self.extract_json_blocks_from_text(text)
        merged: List[Dict[str, Any]] = []
        for block in blocks:
            if isinstance(block, list):
                merged.extend(b for b in block if isinstance(b, dict))
            elif isinstance(block, dict):
                merged.extend(self.get_data_from_json(block) or [])
        if merged:
            return merged
        part_output = self.extract_json_from_response(text)
        if not part_output:
            part_output = self.load_txt_as_json_from_text(text)
        if not part_output:
            return []
        if isinstance(part_output, list):
            return [x for x in part_output if isinstance(x, dict)]
        if isinstance(part_output, dict):
            return self.get_data_from_json(part_output) or []
        return []

    def _normalize_key_part(self, value: Any) -> str:
        """Normalize chapter/subchapter/topic values for robust matching."""
        if value is None:
            return ""
        return str(value).strip().casefold()

    def _build_topic_key(self, chapter_name: Any, subchapter_name: Any, topic_name: Any) -> Tuple[str, str, str]:
        """Build normalized composite key: chapter + subchapter + topic."""
        return (
            self._normalize_key_part(chapter_name),
            self._normalize_key_part(subchapter_name),
            self._normalize_key_part(topic_name),
        )

    def _build_stage_v_processing_context(
        self,
        stage_j_path: str,
        word_file_path: str,
        output_dir: Optional[str],
        progress_callback: Optional[Callable[[str], None]],
    ) -> Optional[StageVProcessingContext]:
        """Load Stage J, derive topics and cleaned records for Step 1 / Step 2."""
        def _progress(msg: str) -> None:
            if progress_callback:
                progress_callback(msg)
            self.logger.info(msg)

        stage_j_data = self.load_json_file(stage_j_path)
        if not stage_j_data:
            self.logger.error("Failed to load Stage J JSON")
            return None

        stage_j_records = self.get_data_from_json(stage_j_data)
        if not stage_j_records:
            self.logger.error("Stage J JSON has no data")
            return None

        first_point_id = stage_j_records[0].get("PointId")
        if not first_point_id:
            self.logger.error("No PointId found in Stage J data")
            return None

        try:
            book_id, chapter_id = self.extract_book_chapter_from_pointid(first_point_id)
        except ValueError as e:
            self.logger.error(f"Error extracting book/chapter: {e}")
            return None

        output_dir_final = output_dir or os.path.dirname(stage_j_path) or os.getcwd()

        _progress(f"Detected Book ID: {book_id}, Chapter ID: {chapter_id}")
        _progress("Extracting topic structure directly from Stage J...")

        topics_list: List[Tuple[str, str, str]] = []
        subchapters_dict: Dict[str, List[str]] = {}
        seen_topic_keys = set()

        for record in stage_j_records:
            if not isinstance(record, dict):
                continue
            chapter_name = record.get("chapter", "")
            subchapter_name = record.get("subchapter", "")
            topic_name = record.get("topic", "")
            if not topic_name:
                continue
            if subchapter_name not in subchapters_dict:
                subchapters_dict[subchapter_name] = []
            topic_key = self._build_topic_key(chapter_name, subchapter_name, topic_name)
            if topic_key in seen_topic_keys:
                continue
            seen_topic_keys.add(topic_key)
            topics_list.append((chapter_name, subchapter_name, topic_name))
            subchapters_dict[subchapter_name].append(topic_name)

        if not topics_list:
            self.logger.error("No topics found in Stage J JSON")
            return None

        unique_subchapters = set(subchapter_name for _, subchapter_name, _ in topics_list)

        self.logger.info("=" * 80)
        self.logger.info("DOCUMENT PROCESSING - PROCESSING BY TOPIC")
        self.logger.info("=" * 80)
        self.logger.info(f"Found {len(topics_list)} topics to process")
        self.logger.info(f"Found {len(unique_subchapters)} unique subchapters")
        self.logger.info("")

        for idx, subchapter_name in enumerate(sorted(unique_subchapters), 1):
            topic_names = subchapters_dict[subchapter_name]
            self.logger.info(f"  Subchapter {idx}: '{subchapter_name}' - {len(topic_names)} topics")
            for topic_idx, topic_name in enumerate(topic_names, 1):
                self.logger.info(f"    Topic {topic_idx}: '{topic_name}'")
        self.logger.info("=" * 80)

        if progress_callback:
            progress_callback(f"Found {len(topics_list)} topics in {len(unique_subchapters)} subchapters to process")

        stage_j_records_for_prompt: List[Dict[str, Any]] = []
        for record in stage_j_records:
            clean_record = {
                "PointId": record.get("PointId", ""),
                "chapter": record.get("chapter", ""),
                "subchapter": record.get("subchapter", ""),
                "topic": record.get("topic", ""),
                "subtopic": record.get("subtopic", ""),
                "subsubtopic": record.get("subsubtopic", ""),
                "Points": record.get("Points", record.get("points", "")),
                "Imp": record.get("Imp", ""),
            }
            stage_j_records_for_prompt.append(clean_record)

        full_stage_j_json = json.dumps(stage_j_records_for_prompt, ensure_ascii=False, indent=2)

        return StageVProcessingContext(
            stage_j_path=stage_j_path,
            word_file_path=word_file_path,
            output_dir_final=output_dir_final,
            book_id=book_id,
            chapter_id=chapter_id,
            stage_j_records=stage_j_records,
            stage_j_records_for_prompt=stage_j_records_for_prompt,
            topics_list=topics_list,
            full_stage_j_json=full_stage_j_json,
        )

    def _execute_stage_v_step2_and_finalize(
        self,
        ctx: StageVProcessingContext,
        step1_combined_path: str,
        prompt_2: str,
        model_name_2: str,
        provider_2: str,
        model_name_1: str,
        stage_settings_manager: Optional[Any],
        progress_callback: Optional[Callable[[str], None]],
        delete_step1_combined_after_success: bool,
        cancel_check: Optional[Callable[[], bool]] = None,
    ) -> Optional[str]:
        """Run Step 2 batches, merge QIds, save final b*.json; optionally remove step1 combined file."""
        stage_j_path = ctx.stage_j_path
        word_file_path = ctx.word_file_path
        output_dir = ctx.output_dir_final
        book_id = ctx.book_id
        chapter_id = ctx.chapter_id
        stage_j_records = ctx.stage_j_records
        stage_j_records_for_prompt = ctx.stage_j_records_for_prompt
        topics_list = ctx.topics_list

        def _progress(msg: str) -> None:
            if progress_callback:
                progress_callback(msg)
            self.logger.info(msg)

        if stage_settings_manager:
            stage_settings_manager.set_stage_provider("stage_v", provider_2)
            stage_settings_manager.set_stage_model("stage_v", model_name_2)
            self.logger.info(f"Stage V Step 2: Using provider={provider_2}, model={model_name_2}")

        _progress("=" * 60)
        _progress("STEP 2: Refining questions and adding QId (each topic: filtered Stage J + full Step 1 file + Step 2 prompt)...")
        _progress("=" * 60)

        step2_topic_outputs: Dict[int, Tuple[str, str, str, str]] = {}

        step2_tasks: List[Dict[str, Any]] = []
        for topic_idx, (chapter_name, subchapter_name, topic_name) in enumerate(topics_list, 1):
            topic_key = self._build_topic_key(chapter_name, subchapter_name, topic_name)
            filtered_stage_j_records = [
                rec
                for rec in stage_j_records_for_prompt
                if self._build_topic_key(rec.get("chapter", ""), rec.get("subchapter", ""), rec.get("topic", "")) == topic_key
            ]
            if not filtered_stage_j_records:
                self.logger.warning(
                    f"Skipping Step 2 for Topic '{topic_name}' (chapter='{chapter_name}', subchapter='{subchapter_name}') because no Stage J rows were found."
                )
                continue
            step2_tasks.append(
                {
                    "topic_idx": topic_idx,
                    "chapter_name": chapter_name,
                    "subchapter_name": subchapter_name,
                    "topic_name": topic_name,
                    "topic_stage_j_json": json.dumps(filtered_stage_j_records, ensure_ascii=False, indent=2),
                    "filtered_rows_count": len(filtered_stage_j_records),
                }
            )

        if not step2_tasks:
            self.logger.error("No valid Step 2 tasks found")
            return None

        total_batches = (len(step2_tasks) + self.STEP2_BATCH_SIZE - 1) // self.STEP2_BATCH_SIZE
        for batch_idx, start in enumerate(range(0, len(step2_tasks), self.STEP2_BATCH_SIZE), 1):
            batch_tasks = step2_tasks[start : start + self.STEP2_BATCH_SIZE]
            _progress(
                f"STEP 2 batch {batch_idx}/{total_batches}: processing {len(batch_tasks)} topic(s) "
                f"with concurrency={self.STEP2_BATCH_SIZE}"
            )

            executor = ThreadPoolExecutor(max_workers=self.STEP2_BATCH_SIZE)
            future_to_task = {}
            try:
                for task in batch_tasks:
                    topic_idx = task["topic_idx"]
                    chapter_name = task["chapter_name"]
                    subchapter_name = task["subchapter_name"]
                    topic_name = task["topic_name"]
                    self.logger.info("")
                    self.logger.info("=" * 80)
                    self.logger.info(f"QUEUE TOPIC {topic_idx}/{len(topics_list)}: '{topic_name}'")
                    self.logger.info("=" * 80)
                    self.logger.info(f"  Chapter: '{chapter_name}'")
                    self.logger.info(f"  Subchapter: '{subchapter_name}'")
                    self.logger.info(f"  Topic: '{topic_name}'")
                    self.logger.info("  Input: filtered Stage J + full Step 1 file + Step 2 prompt")
                    self.logger.info(f"  Batch: {batch_idx}/{total_batches}")
                    self.logger.info(f"  Stage J rows: {task['filtered_rows_count']}")

                    future = executor.submit(
                        self._step2_refine_questions_and_add_qid,
                        stage_j_path=stage_j_path,
                        word_file_path=word_file_path,
                        full_stage_j_json=task["topic_stage_j_json"],
                        current_topic_name=topic_name,
                        current_topic_subchapter=subchapter_name,
                        step1_output_path=step1_combined_path,
                        prompt=prompt_2,
                        model_name=model_name_2,
                        book_id=book_id,
                        chapter_id=chapter_id,
                        topic_idx=topic_idx,
                        total_topics=len(topics_list),
                        qid_start_counter=1,
                        output_dir=output_dir,
                        progress_callback=progress_callback,
                        assign_qid=False,
                        cancel_check=cancel_check,
                    )
                    future_to_task[future] = task

                for future in as_completed(future_to_task):
                    task = future_to_task[future]
                    topic_idx = task["topic_idx"]
                    chapter_name = task["chapter_name"]
                    subchapter_name = task["subchapter_name"]
                    topic_name = task["topic_name"]
                    try:
                        topic_step2_output, num_questions = future.result()
                        if topic_step2_output:
                            step2_topic_outputs[topic_idx] = (chapter_name, subchapter_name, topic_name, topic_step2_output)
                            _progress(f"Step 2 completed for Topic '{topic_name}': {topic_step2_output}")
                            self.logger.info(
                                f"  ✓ Step 2 completed for Topic '{topic_name}' in batch {batch_idx}/{total_batches} "
                                f"({num_questions} questions)"
                            )
                        else:
                            self.logger.warning(
                                f"  ✗ Step 2 failed for Topic '{topic_name}' in batch {batch_idx}/{total_batches}, skipping..."
                            )
                    except OpenRouterRequestAborted:
                        raise
                    except Exception as e:
                        self.logger.warning(
                            f"  ✗ Step 2 raised exception for Topic '{topic_name}' in batch {batch_idx}/{total_batches}: {e}"
                        )
            finally:
                executor.shutdown(wait=False, cancel_futures=True)

        if not step2_topic_outputs:
            self.logger.error("Step 2 failed for all topics")
            return None

        _progress("Combining Step 2 outputs from all topics...")
        self.logger.info(f"Combining Step 2 outputs from {len(step2_topic_outputs)} topics...")
        step2_combined_data: List[Any] = []
        global_qid_counter = 1
        for topic_idx in sorted(step2_topic_outputs.keys()):
            chapter_name, subchapter_name, topic_name, topic_step2_path = step2_topic_outputs[topic_idx]
            topic_step2_data = self.load_json_file(topic_step2_path)
            if topic_step2_data:
                topic_records = self.get_data_from_json(topic_step2_data)
                if topic_records:
                    for question in topic_records:
                        qid = f"{book_id:03d}{chapter_id:03d}{global_qid_counter:04d}"
                        question["QId"] = qid
                        global_qid_counter += 1
                    step2_combined_data.extend(topic_records)
                    self.logger.info(
                        f"  Added {len(topic_records)} questions from Topic '{topic_name}' "
                        f"(Chapter: '{chapter_name}', Subchapter: '{subchapter_name}', topic_idx={topic_idx})"
                    )
            try:
                if os.path.exists(topic_step2_path):
                    os.remove(topic_step2_path)
            except Exception:
                pass

        if delete_step1_combined_after_success:
            try:
                if os.path.exists(step1_combined_path):
                    os.remove(step1_combined_path)
            except Exception:
                pass

        if not step2_combined_data:
            self.logger.error("Failed to combine Step 2 outputs")
            return None

        chapter_name_out = ""
        if stage_j_records:
            first_record = stage_j_records[0] if isinstance(stage_j_records[0], dict) else {}
            chapter_name_out = first_record.get("chapter", "") if isinstance(first_record, dict) else ""

        if chapter_name_out:
            chapter_name_clean = re.sub(r'[<>:"/\\|?*]', "_", chapter_name_out)
            chapter_name_clean = chapter_name_clean.replace(" ", "_")
            chapter_name_clean = re.sub(r"_+", "_", chapter_name_clean)
            chapter_name_clean = chapter_name_clean.strip("_")
        else:
            chapter_name_clean = ""

        if chapter_name_clean:
            _progress(f"Detected Chapter Name: {chapter_name_out}")
        else:
            _progress("No chapter name found, using empty string")

        output_dir_final = ctx.output_dir_final
        if chapter_name_clean:
            base_filename = f"b{book_id:03d}{chapter_id:03d}+{chapter_name_clean}.json"
        else:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            base_filename = f"b{book_id:03d}{chapter_id:03d}+{timestamp}.json"
            self.logger.warning(f"No chapter name found, using timestamp in filename: {timestamp}")

        output_path = os.path.join(output_dir_final, base_filename)

        if os.path.exists(output_path) and chapter_name_clean:
            counter = 1
            while os.path.exists(output_path):
                base_filename = f"b{book_id:03d}{chapter_id:03d}+{chapter_name_clean}_{counter}.json"
                output_path = os.path.join(output_dir_final, base_filename)
                counter += 1
            if counter > 1:
                self.logger.info(f"File already exists, using counter: {counter - 1}")

        output_metadata = {
            "book_id": book_id,
            "chapter_id": chapter_id,
            "source_stage_j": os.path.basename(stage_j_path),
            "model_step1": model_name_1,
            "model_step2": model_name_2,
            "total_topics": len(step2_topic_outputs),
            "total_questions": len(step2_combined_data),
        }

        success = self.save_json_file(step2_combined_data, output_path, output_metadata, "V")
        if success:
            _progress(f"Final output saved to: {output_path}")
            return output_path
        self.logger.error("Failed to save final output")
        return None

    def process_stage_v_step1(
        self,
        stage_j_path: str,
        word_file_path: str,
        prompt_1: str,
        model_name_1: str,
        provider_1: str = "deepseek",
        stage_settings_manager: Optional[Any] = None,
        output_dir: Optional[str] = None,
        progress_callback: Optional[Callable[[str], None]] = None,
        cancel_check: Optional[Callable[[], bool]] = None,
    ) -> Optional[str]:
        """
        Run only Step 1: writes step1_combined_*.json under output_dir (does not run Step 2).
        """
        if hasattr(self.api_client, "set_stage"):
            self.api_client.set_stage("stage_v")

        ctx = self._build_stage_v_processing_context(stage_j_path, word_file_path, output_dir, progress_callback)
        if not ctx:
            return None

        if stage_settings_manager:
            stage_settings_manager.set_stage_provider("stage_v", provider_1)
            stage_settings_manager.set_stage_model("stage_v", model_name_1)
            self.logger.info(f"Stage V Step 1: Using provider={provider_1}, model={model_name_1}")

        def _progress(msg: str) -> None:
            if progress_callback:
                progress_callback(msg)
            self.logger.info(msg)

        _progress("=" * 60)
        _progress("STEP 1: Generating initial test questions (single run: full Stage J + full test file + Step 1 prompt)...")
        _progress("=" * 60)

        return self._step1_run_once(
            stage_j_path=ctx.stage_j_path,
            word_file_path=ctx.word_file_path,
            full_stage_j_json=ctx.full_stage_j_json,
            prompt=prompt_1,
            model_name=model_name_1,
            book_id=ctx.book_id,
            chapter_id=ctx.chapter_id,
            output_dir=ctx.output_dir_final,
            progress_callback=progress_callback,
            cancel_check=cancel_check,
        )

    def process_stage_v_step2(
        self,
        stage_j_path: str,
        word_file_path: str,
        prompt_2: str,
        model_name_2: str,
        provider_2: str = "deepseek",
        step1_combined_path: Optional[str] = None,
        model_name_1: str = "",
        stage_settings_manager: Optional[Any] = None,
        output_dir: Optional[str] = None,
        progress_callback: Optional[Callable[[str], None]] = None,
        delete_step1_combined_after_success: bool = True,
        cancel_check: Optional[Callable[[], bool]] = None,
    ) -> Optional[str]:
        """
        Run Step 2 + final merge using an existing Step 1 combined JSON.
        If step1_combined_path is None, uses step1_combined_{book}{chapter}.json under output_dir.
        """
        if hasattr(self.api_client, "set_stage"):
            self.api_client.set_stage("stage_v")

        ctx = self._build_stage_v_processing_context(stage_j_path, word_file_path, output_dir, progress_callback)
        if not ctx:
            return None

        resolved_step1 = step1_combined_path or os.path.join(
            ctx.output_dir_final, f"step1_combined_{ctx.book_id}{ctx.chapter_id:03d}.json"
        )
        if not os.path.isfile(resolved_step1):
            self.logger.error(f"Step 1 combined file not found: {resolved_step1}")
            return None

        return self._execute_stage_v_step2_and_finalize(
            ctx,
            resolved_step1,
            prompt_2,
            model_name_2,
            provider_2,
            model_name_1,
            stage_settings_manager,
            progress_callback,
            delete_step1_combined_after_success,
            cancel_check=cancel_check,
        )

    def process_stage_v(
        self,
        stage_j_path: str,
        word_file_path: str,
        prompt_1: str,
        model_name_1: str,
        provider_1: str = "deepseek",
        prompt_2: str = "",
        model_name_2: str = "deepseek-reasoner",
        provider_2: str = "deepseek",
        stage_settings_manager: Optional[Any] = None,
        output_dir: Optional[str] = None,
        progress_callback: Optional[Callable[[str], None]] = None,
    ) -> Optional[str]:
        """
        Process Stage V: Generate test files from Stage J and Word document (Step 1 then Step 2).

        Returns:
            Path to output file (b.json) or None on error
        """
        def _progress(msg: str) -> None:
            if progress_callback:
                progress_callback(msg)
            self.logger.info(msg)

        if hasattr(self.api_client, "set_stage"):
            self.api_client.set_stage("stage_v")

        _progress("Starting Stage V processing...")

        ctx = self._build_stage_v_processing_context(stage_j_path, word_file_path, output_dir, progress_callback)
        if not ctx:
            return None

        if stage_settings_manager:
            stage_settings_manager.set_stage_provider("stage_v", provider_1)
            stage_settings_manager.set_stage_model("stage_v", model_name_1)
            self.logger.info(f"Stage V Step 1: Using provider={provider_1}, model={model_name_1}")

        _progress("=" * 60)
        _progress("STEP 1: Generating initial test questions (single run: full Stage J + full test file + Step 1 prompt)...")
        _progress("=" * 60)

        step1_combined_path = self._step1_run_once(
            stage_j_path=ctx.stage_j_path,
            word_file_path=ctx.word_file_path,
            full_stage_j_json=ctx.full_stage_j_json,
            prompt=prompt_1,
            model_name=model_name_1,
            book_id=ctx.book_id,
            chapter_id=ctx.chapter_id,
            output_dir=ctx.output_dir_final,
            progress_callback=progress_callback,
            cancel_check=None,
        )

        if not step1_combined_path:
            self.logger.error("Step 1 failed")
            return None

        _progress(f"Step 1 completed. Output: {step1_combined_path}")

        return self._execute_stage_v_step2_and_finalize(
            ctx,
            step1_combined_path,
            prompt_2,
            model_name_2,
            provider_2,
            model_name_1,
            stage_settings_manager,
            progress_callback,
            delete_step1_combined_after_success=True,
            cancel_check=None,
        )

    def _step1_run_once(
        self,
        stage_j_path: str,
        word_file_path: str,
        full_stage_j_json: str,
        prompt: str,
        model_name: str,
        book_id: int,
        chapter_id: int,
        output_dir: Optional[str] = None,
        progress_callback: Optional[Callable[[str], None]] = None,
        cancel_check: Optional[Callable[[], bool]] = None,
    ) -> Optional[str]:
        """
        Step 1 (single run): Generate initial test questions from full Stage J JSON + full test file + Step 1 prompt.
        No per-topic split; one API call with entire inputs.
        
        Args:
            stage_j_path: Path to Stage J JSON file
            word_file_path: Path to Word test file
            full_stage_j_json: Full Stage J JSON string (all records)
            prompt: Step 1 prompt (no topic placeholders)
            model_name: Model name for API
            book_id: Book ID for output metadata
            chapter_id: Chapter ID for output metadata
            output_dir: Output directory
            progress_callback: Optional progress callback
            
        Returns:
            Path to Step 1 combined output JSON, or None on error
        """
        def _progress(msg: str):
            if progress_callback:
                progress_callback(msg)
            self.logger.info(msg)
        
        _progress("Loading Word document for Step 1...")
        word_content = self.word_processor.read_word_file(word_file_path)
        if not word_content:
            self.logger.error("Failed to read Word file")
            return None
        
        word_content_formatted = self.word_processor.prepare_word_for_model(
            word_content,
            context="Test Questions"
        )
        
        full_prompt = f"""{prompt}

Word Document (Test Questions):
{word_content_formatted}

Stage J Data (FULL file - all records, without Type column):
{full_stage_j_json}"""
        
        _progress("Processing Stage V - Step 1 (single run: full Stage J + full test file)...")
        if cancel_check:
            _progress("Sending request to the LLM API (streaming — stop can take effect during generation).")
        else:
            _progress("Sending request to the LLM API (next log line is after the response returns).")
        part_response = None
        for attempt in range(3):
            try:
                part_response = self.api_client.process_text(
                    text=full_prompt,
                    system_prompt=None,
                    model_name=model_name,
                    temperature=APIConfig.DEFAULT_TEMPERATURE,
                    max_tokens=self._STAGE_V_OUTPUT_MAX_TOKENS,
                    cancel_check=cancel_check,
                )
                if part_response:
                    _progress(f"Step 1 response received ({len(part_response)} characters)")
                    break
            except OpenRouterRequestAborted:
                raise
            except Exception as e:
                self.logger.warning(f"Step 1 attempt {attempt + 1} failed: {e}")
                if attempt < 2:
                    _progress(f"Retrying Step 1... (attempt {attempt + 2}/3)")
                else:
                    self.logger.error("All Step 1 attempts failed")
        
        if not part_response:
            self.logger.error("No response from model in Step 1")
            return None
        
        base_dir = os.path.dirname(stage_j_path) or os.getcwd()
        base_name, _ = os.path.splitext(os.path.basename(stage_j_path))
        txt_path = os.path.join(base_dir, f"{base_name}_stage_v_step1_full.txt")
        try:
            with open(txt_path, 'w', encoding='utf-8') as f:
                f.write(part_response)
            _progress(f"Saved raw model response to: {txt_path}")
        except Exception as e:
            self.logger.warning(f"Failed to save TXT file: {e}")
        
        all_questions = self._extract_stage_v_question_rows_from_model_text(part_response)
        if not all_questions:
            try:
                with open(txt_path, "r", encoding="utf-8") as f:
                    all_questions = self._extract_stage_v_question_rows_from_model_text(f.read())
            except OSError as e:
                self.logger.warning(f"Step 1 TXT re-read for extraction failed: {e}")
        if not all_questions:
            model_output = self.load_txt_as_json(txt_path)
            if model_output:
                if isinstance(model_output, list):
                    all_questions = [x for x in model_output if isinstance(x, dict)]
                elif isinstance(model_output, dict):
                    all_questions = self.get_data_from_json(model_output) or []
        
        if not all_questions:
            self.logger.error("No questions extracted from Step 1 response")
            return None
        
        _progress(f"Step 1 extracted {len(all_questions)} questions")
        out_dir = output_dir or base_dir
        step1_combined_path = os.path.join(out_dir, f"step1_combined_{book_id}{chapter_id:03d}.json")
        step1_metadata = {
            "book_id": book_id,
            "chapter_id": chapter_id,
            "source": "Stage V - Step 1 (single run: full Stage J + full test file + Step 1 prompt)",
            "total_questions": len(all_questions),
            "source_stage_j": os.path.basename(stage_j_path),
            "source_word_file": os.path.basename(word_file_path),
            "model_used": model_name
        }
        success = self.save_json_file(all_questions, step1_combined_path, step1_metadata, "V-Step1")
        if success:
            _progress(f"Step 1 output saved to: {step1_combined_path}")
            return step1_combined_path
        self.logger.error("Failed to save Step 1 output")
        return None
    
    def _step1_generate_initial_questions(
        self,
        stage_j_path: str,
        word_file_path: str,
        full_stage_j_json: str,
        current_topic_name: str,
        current_topic_subchapter: str,
        prompt: str,
        model_name: str,
        topic_idx: int,
        total_topics: int,
        output_dir: Optional[str] = None,
        progress_callback: Optional[Callable[[str], None]] = None
    ) -> Optional[str]:
        """
        Step 1 (legacy, per-topic): Generate initial test questions from Stage J and Word document.
        Uses FULL Stage J data but focuses on current topic. Prefer _step1_run_once for new flow.
        
        Args:
            stage_j_path: Path to Stage J JSON file
            word_file_path: Path to Word file
            full_stage_j_json: Full Stage J JSON string (all records)
            current_topic_name: Current topic name to focus on
            current_topic_subchapter: Current topic's subchapter
            prompt: Prompt for initial question generation (should contain {Topic_NAME} and {Subchapter_Name})
            model_name: Gemini model name
            topic_idx: Current topic index (1-based)
            total_topics: Total number of topics
            output_dir: Output directory
            progress_callback: Optional callback for progress updates
            
        Returns:
            Path to Step 1 output file or None on error
        """
        def _progress(msg: str):
            if progress_callback:
                progress_callback(msg)
            self.logger.info(msg)
        
        _progress(f"Loading Word document for Topic {topic_idx}/{total_topics}...")
        
        # Load Word file
        word_content = self.word_processor.read_word_file(word_file_path)
        if not word_content:
            self.logger.error("Failed to read Word file")
            return None
        
        word_content_formatted = self.word_processor.prepare_word_for_model(
            word_content,
            context="Test Questions"
        )
        
        # Replace placeholders in prompt with current topic and subchapter
        topic_prompt = prompt.replace("{Topic_NAME}", current_topic_name)
        topic_prompt = topic_prompt.replace("{Subchapter_Name}", current_topic_subchapter)
        topic_prompt = topic_prompt.replace("{TOPIC_NAME}", current_topic_name)
        topic_prompt = topic_prompt.replace("{SUBCHAPTER_NAME}", current_topic_subchapter)

        # Prepare full prompt with FULL Stage J data
        full_prompt = f"""{topic_prompt}

Word Document (Test Questions):
{word_content_formatted}

Stage J Data (FULL file - all records, without Type column):
{full_stage_j_json}

IMPORTANT: Focus on generating test questions for the topic: "{current_topic_name}" (Subchapter: "{current_topic_subchapter}")."""
        
        # Call model once and collect raw response
        all_raw_responses = []
        max_retries = 3
        
        _progress(f"Processing Stage V - Step 1 for Topic {topic_idx}/{total_topics} (full file)...")
        part_response = None
        for attempt in range(max_retries):
            try:
                part_response = self.api_client.process_text(
                    text=full_prompt,
                    system_prompt=None,
                    model_name=model_name,
                    temperature=APIConfig.DEFAULT_TEMPERATURE,
                    max_tokens=self._STAGE_V_OUTPUT_MAX_TOKENS,
                )
                if part_response:
                    _progress(f"Step 1 response received for Topic {topic_idx} ({len(part_response)} characters)")
                    break
            except Exception as e:
                self.logger.warning(f"Step 1 attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    _progress(f"Retrying Step 1... (attempt {attempt + 2}/{max_retries})")
                else:
                    self.logger.error("All Step 1 attempts failed")
        
        if not part_response:
            self.logger.error("No response from model in Step 1")
            return None
        
        # Store raw response as single part
        all_raw_responses.append(part_response)
        
        if not all_raw_responses:
            self.logger.error("No responses received in Step 1")
            return None
        
        # Save all raw responses to TXT file
        base_dir = os.path.dirname(stage_j_path) or os.getcwd()
        base_name, _ = os.path.splitext(os.path.basename(stage_j_path))
        safe_topic_name = current_topic_name.replace('/', '_').replace(' ', '_').replace('\\', '_')
        safe_topic_name = ''.join(c for c in safe_topic_name if c.isalnum() or c in ('_', '-', '.'))
        txt_path = os.path.join(base_dir, f"{base_name}_stage_v_step1_topic_{topic_idx}_{safe_topic_name}.txt")
        
        try:
            with open(txt_path, 'w', encoding='utf-8') as f:
                for idx, response in enumerate(all_raw_responses, 1):
                    f.write(f"=== PART {idx} RESPONSE ===\n")
                    f.write(response)
                    f.write("\n\n")
            _progress(f"Saved raw model responses to: {txt_path}")
        except Exception as e:
            self.logger.warning(f"Failed to save TXT file: {e}")
        
        # Extract JSON from all responses
        all_questions = []
        for part_idx, part_response in enumerate(all_raw_responses, 1):
            _progress(f"Extracting JSON from Part {part_idx} response...")
            part_questions = self._extract_stage_v_question_rows_from_model_text(part_response)
            if part_questions:
                all_questions.extend(part_questions)
                _progress(f"Extracted {len(part_questions)} questions from Part {part_idx}")
        
        if not all_questions:
            self.logger.error("Failed to extract JSON from model responses")
            # Try loading from TXT file as fallback (whole TXT → JSON → list)
            _progress("Trying to load JSON from TXT file as fallback...")
            model_output = self.load_txt_as_json(txt_path)
            if model_output:
                if isinstance(model_output, list):
                    all_questions = model_output
                elif isinstance(model_output, dict):
                    all_questions = self.get_data_from_json(model_output)
        
        if not all_questions:
            self.logger.error(f"No questions generated in Step 1 for Topic {topic_idx}")
            return None
        
        _progress(f"Total questions generated in Step 1 for Topic {topic_idx}: {len(all_questions)}")
        
        # Save Step 1 output
        if not output_dir:
            output_dir = os.path.dirname(stage_j_path) or os.getcwd()
        
        base_name, _ = os.path.splitext(os.path.basename(stage_j_path))
        step1_output_path = os.path.join(output_dir, f"{base_name}_stage_v_step1_topic_{topic_idx}_{safe_topic_name}.json")
        
        step1_metadata = {
            "step": 1,
            "topic": current_topic_name,
            "subchapter": current_topic_subchapter,
            "topic_idx": topic_idx,
            "total_topics": total_topics,
            "source_stage_j": os.path.basename(stage_j_path),
            "source_word_file": os.path.basename(word_file_path),
            "model_used": model_name,
            "total_questions": len(all_questions)
        }
        
        success = self.save_json_file(all_questions, step1_output_path, step1_metadata, "V-Step1")
        if success:
            _progress(f"Step 1 output saved to: {step1_output_path}")
            return step1_output_path
        else:
            self.logger.error("Failed to save Step 1 output")
            return None
    
    def _step2_refine_questions_and_add_qid(
        self,
        stage_j_path: str,
        word_file_path: str,
        full_stage_j_json: str,
        current_topic_name: str,
        current_topic_subchapter: str,
        step1_output_path: str,
        prompt: str,
        model_name: str,
        book_id: int,
        chapter_id: int,
        topic_idx: int,
        total_topics: int,
        qid_start_counter: int,
        output_dir: Optional[str] = None,
        progress_callback: Optional[Callable[[str], None]] = None,
        assign_qid: bool = True,
        cancel_check: Optional[Callable[[], bool]] = None,
    ) -> tuple[Optional[str], int]:
        """
        Step 2: Refine questions and add QId mapping.
        Uses FULL Stage J data but focuses on current topic.
        
        Args:
            stage_j_path: Path to Stage J JSON file
            word_file_path: Path to Word file
            full_stage_j_json: Full Stage J JSON string (all records)
            current_topic_name: Current topic name to focus on
            current_topic_subchapter: Current topic's subchapter
            step1_output_path: Path to Step 1 output file for this topic
            prompt: Prompt for refinement (should contain {Topic_NAME} and {Subchapter_Name})
            model_name: Gemini model name
            book_id: Book ID
            chapter_id: Chapter ID
            topic_idx: Current topic index (1-based)
            total_topics: Total number of topics
            qid_start_counter: Starting QId counter for this topic (used only when assign_qid=True)
            output_dir: Output directory
            progress_callback: Optional callback for progress updates
            assign_qid: Whether to add QId in this function (for concurrent mode this is False and QId is assigned later)
            
        Returns:
            Tuple of (Path to Step 2 output file or None on error, number of questions processed)
        """
        def _progress(msg: str):
            if progress_callback:
                progress_callback(msg)
            self.logger.info(msg)
        
        _progress(f"Loading Word document for Topic {topic_idx}/{total_topics}...")
        
        # Load Word file
        word_content = self.word_processor.read_word_file(word_file_path)
        if not word_content:
            self.logger.error("Failed to read Word file")
            return None
        
        word_content_formatted = self.word_processor.prepare_word_for_model(
            word_content,
            context="Test Questions"
        )
        
        # Load Step 1 output for this topic
        _progress("Loading Step 1 output...")
        step1_data = self.load_json_file(step1_output_path)
        step1_questions = []
        if step1_data:
            step1_questions = self.get_data_from_json(step1_data) or []
        _progress(f"Loaded {len(step1_questions)} questions from Step 1")
        
        # Replace placeholders in prompt with current topic and subchapter
        topic_prompt = prompt.replace("{Topic_NAME}", current_topic_name)
        topic_prompt = topic_prompt.replace("{Subchapter_Name}", current_topic_subchapter)
        topic_prompt = topic_prompt.replace("{TOPIC_NAME}", current_topic_name)
        topic_prompt = topic_prompt.replace("{SUBCHAPTER_NAME}", current_topic_subchapter)

        # Prepare full prompt with FULL Stage J data
        full_prompt = f"""{topic_prompt}

Word Document (Test Questions):
{word_content_formatted}

Stage J Data (FULL file - all records, without Type column):
{full_stage_j_json}

IMPORTANT: Focus on refining test questions for the topic: "{current_topic_name}" (Subchapter: "{current_topic_subchapter}")."""
        
        # Call model once and collect raw response
        all_raw_responses = []
        max_retries = 3
        
        _progress(f"Processing Stage V - Step 2 for Topic {topic_idx}/{total_topics} (full file)...")
        part_response = None
        for attempt in range(max_retries):
            try:
                part_response = self.api_client.process_text(
                    text=full_prompt,
                    system_prompt=None,
                    model_name=model_name,
                    temperature=APIConfig.DEFAULT_TEMPERATURE,
                    max_tokens=self._STAGE_V_OUTPUT_MAX_TOKENS,
                    cancel_check=cancel_check,
                )
                if part_response:
                    _progress(f"Step 2 response received for Topic {topic_idx} ({len(part_response)} characters)")
                    break
            except OpenRouterRequestAborted:
                raise
            except Exception as e:
                self.logger.warning(f"Step 2 attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    _progress(f"Retrying Step 2... (attempt {attempt + 2}/{max_retries})")
                else:
                    self.logger.error("All Step 2 attempts failed")
        
        if not part_response:
            self.logger.error("No response from model in Step 2")
            return None
        
        # Store raw response as single part
        all_raw_responses.append(part_response)
        
        if not all_raw_responses:
            self.logger.error("No responses received in Step 2")
            return None
        
        # Save all raw responses to TXT file
        base_dir = os.path.dirname(stage_j_path) or os.getcwd()
        base_name, _ = os.path.splitext(os.path.basename(stage_j_path))
        safe_topic_name = current_topic_name.replace('/', '_').replace(' ', '_').replace('\\', '_')
        safe_topic_name = ''.join(c for c in safe_topic_name if c.isalnum() or c in ('_', '-', '.'))
        txt_path = os.path.join(base_dir, f"{base_name}_stage_v_step2_topic_{topic_idx}_{safe_topic_name}.txt")
        
        try:
            with open(txt_path, 'w', encoding='utf-8') as f:
                for idx, response in enumerate(all_raw_responses, 1):
                    f.write(f"=== PART {idx} RESPONSE ===\n")
                    f.write(response)
                    f.write("\n\n")
            _progress(f"Saved raw model responses to: {txt_path}")
        except Exception as e:
            self.logger.warning(f"Failed to save TXT file: {e}")
        
        # Extract JSON from all responses
        all_refined_questions = []
        for part_idx, part_response in enumerate(all_raw_responses, 1):
            _progress(f"Extracting JSON from Part {part_idx} response...")
            part_refined = self._extract_stage_v_question_rows_from_model_text(part_response)
            if part_refined:
                all_refined_questions.extend(part_refined)
                _progress(f"Extracted {len(part_refined)} refined questions from Part {part_idx}")
        
        if not all_refined_questions:
            self.logger.error("Failed to extract JSON from model responses")
            # Try loading from TXT file as fallback (whole TXT → JSON → list)
            _progress("Trying to load JSON from TXT file as fallback...")
            model_output = self.load_txt_as_json(txt_path)
            if model_output:
                if isinstance(model_output, list):
                    all_refined_questions = model_output
                elif isinstance(model_output, dict):
                    all_refined_questions = self.get_data_from_json(model_output)
        
        if not all_refined_questions:
            self.logger.error(f"No refined questions generated in Step 2 for Topic {topic_idx}")
            return None, 0
        
        _progress(f"Total refined questions from Step 2 for Topic {topic_idx}: {len(all_refined_questions)}")
        
        # Combine Step 1 and Step 2 questions
        all_combined_questions = step1_questions + all_refined_questions
        _progress(f"Total combined questions (Step 1 + Step 2) for Topic {topic_idx}: {len(all_combined_questions)}")
        
        # Add QId to all questions after receiving model response
        num_questions = len(all_combined_questions)
        for idx, question in enumerate(all_combined_questions, 1):
            # Reassign TestID sequentially (local to this topic)
            question["TestID"] = idx
        if assign_qid:
            _progress(f"Adding QId to all questions for Topic {topic_idx} (starting from QId counter {qid_start_counter})...")
            qid_counter = qid_start_counter
            for question in all_combined_questions:
                # Generate QId: BBBCCCPPPP format (global sequence across all topics)
                qid = f"{book_id:03d}{chapter_id:03d}{qid_counter:04d}"
                question["QId"] = qid
                qid_counter += 1
            _progress(f"Added QId to {num_questions} questions for Topic {topic_idx} (QId range: {qid_start_counter} to {qid_counter - 1})")
        
        # Save Step 2 output
        if not output_dir:
            output_dir = os.path.dirname(stage_j_path) or os.getcwd()
        
        base_name, _ = os.path.splitext(os.path.basename(stage_j_path))
        step2_output_path = os.path.join(output_dir, f"{base_name}_stage_v_step2_topic_{topic_idx}_{safe_topic_name}.json")
        
        step2_metadata = {
            "step": 2,
            "topic": current_topic_name,
            "subchapter": current_topic_subchapter,
            "topic_idx": topic_idx,
            "total_topics": total_topics,
            "source_stage_j": os.path.basename(stage_j_path),
            "source_word_file": os.path.basename(word_file_path),
            "source_step1": os.path.basename(step1_output_path),
            "model_used": model_name,
            "book_id": book_id,
            "chapter_id": chapter_id,
            "total_questions_step1": len(step1_questions),
            "total_questions_step2": len(all_refined_questions),
            "total_questions_combined": len(all_combined_questions)
        }
        
        success = self.save_json_file(all_combined_questions, step2_output_path, step2_metadata, "V-Step2")
        if success:
            _progress(f"Step 2 output saved to: {step2_output_path}")
            return step2_output_path, num_questions
        else:
            self.logger.error("Failed to save Step 2 output")
            return None, 0
    


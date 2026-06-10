"""
Voice Class processor: OpenRouter script (Step 1) and Gemini TTS + merge (Step 2).
"""

from __future__ import annotations

import json
import logging
import os
import time
from typing import Any, Callable, Dict, List, Optional, Tuple

from api_layer import APIConfig, GeminiAPIClient
from base_stage_processor import BaseStageProcessor
from voice_class_captions import (
    TopicCaptionIndexes,
    build_topic_caption_context,
    load_topic_caption_indexes,
)
from voice_class_prompts import SCRIPT_JSON_RETRY_SUFFIX, build_topic_script_prompt
from webapp.audio_merge import merge_voice_tracks, wav_duration_seconds

logger = logging.getLogger(__name__)

MAX_TTS_CHARS = 4096
DEFAULT_CHARS_PER_SECOND_ESTIMATE = 13.0
IMP_LEVELS_FOR_VOICE_SCRIPT = ("1", "2")
EMPTY_TOPIC_LABEL = "(بدون مبحث)"
VOICE_SCRIPT_PARSE_RETRIES = 3


class StageVoiceProcessor(BaseStageProcessor):
    """Generate voice-class script JSON and TTS audio for web jobs."""

    def __init__(self, api_client, gemini_tts_key_manager=None):
        super().__init__(api_client)
        self.logger = logging.getLogger(__name__)
        self._gemini_keys = gemini_tts_key_manager

    @staticmethod
    def pack_segments(
        paragraphs: List[Dict[str, Any]],
        *,
        max_segment_seconds: float = 60.0,
        chars_per_second: float = 13.0,
        max_chars: int = MAX_TTS_CHARS,
    ) -> List[Dict[str, Any]]:
        """Greedy bin-pack consecutive paragraphs into ≤1-minute TTS segments."""
        segments: List[Dict[str, Any]] = []
        current_ids: List[int] = []
        current_texts: List[str] = []
        current_chars = 0
        current_seconds = 0.0
        segment_id = 0

        def _flush() -> None:
            nonlocal segment_id, current_ids, current_texts, current_chars, current_seconds
            if not current_ids:
                return
            segment_id += 1
            combined = "\n\n".join(current_texts)
            segments.append(
                {
                    "segment_id": segment_id,
                    "paragraph_ids": list(current_ids),
                    "paragraph_count": len(current_ids),
                    "combined_text": combined,
                    "char_count": len(combined),
                    "estimated_seconds": round(current_seconds, 2),
                }
            )
            current_ids = []
            current_texts = []
            current_chars = 0
            current_seconds = 0.0

        for para in paragraphs:
            pid = int(para["paragraph_id"])
            text = (para.get("text") or "").strip()
            if not text:
                continue
            char_count = len(text)
            est_sec = char_count / chars_per_second if chars_per_second > 0 else 0.0

            would_exceed_time = current_seconds + est_sec > max_segment_seconds and current_ids
            would_exceed_chars = current_chars + char_count + (2 if current_texts else 0) > max_chars and current_ids

            if would_exceed_time or would_exceed_chars:
                _flush()

            if current_texts:
                current_chars += 2
                current_seconds += 2 / chars_per_second if chars_per_second > 0 else 0
            current_ids.append(pid)
            current_texts.append(text)
            current_chars += char_count
            current_seconds += est_sec

        _flush()
        return segments

    def _build_lesson_context(self, records: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Compact Imp 1–2 lesson rows for the script LLM."""
        lesson_rows: List[Dict[str, Any]] = []
        for record in records:
            importance = str(record.get("Imp") or record.get("imp") or "").strip()
            if importance not in IMP_LEVELS_FOR_VOICE_SCRIPT:
                continue
            lesson_rows.append(
                {
                    "PointId": record.get("PointId") or record.get("point_id"),
                    "chapter": record.get("chapter", ""),
                    "subchapter": record.get("subchapter", ""),
                    "topic": record.get("topic", ""),
                    "subtopic": record.get("subtopic", ""),
                    "subsubtopic": record.get("subsubtopic", ""),
                    "Points": record.get("Points") or record.get("points", ""),
                    "Imp": importance,
                    "Type": record.get("Type") or record.get("type", ""),
                }
            )
        return lesson_rows

    @staticmethod
    def _topic_key(chapter: Any, subchapter: Any, topic: Any) -> Tuple[str, str, str]:
        chapter_name = str(chapter or "").strip()
        subchapter_name = str(subchapter or "").strip()
        topic_name = str(topic or "").strip() or EMPTY_TOPIC_LABEL
        return chapter_name, subchapter_name, topic_name

    def _group_lesson_context_by_topic(
        self, lesson_rows: List[Dict[str, Any]]
    ) -> List[Tuple[str, str, str, List[Dict[str, Any]]]]:
        """Stable first-seen grouping by chapter + subchapter + topic."""
        order: List[Tuple[str, str, str]] = []
        buckets: Dict[Tuple[str, str, str], List[Dict[str, Any]]] = {}
        for row in lesson_rows:
            key = self._topic_key(row.get("chapter"), row.get("subchapter"), row.get("topic"))
            if key not in buckets:
                order.append(key)
                buckets[key] = []
            buckets[key].append(row)
        return [(k[0], k[1], k[2], buckets[k]) for k in order]

    def _load_tagged_records(self, tagged_json_path: str) -> Optional[List[Dict[str, Any]]]:
        tagged_data = self.load_json_file(tagged_json_path)
        if not tagged_data:
            self.logger.error("Failed to load tagged JSON")
            return None

        records = self.get_data_from_json(tagged_data)
        if not records:
            self.logger.error("Tagged JSON has no data rows")
            return None
        return records

    def _extract_chapter_metadata(
        self,
        records: List[Dict[str, Any]],
    ) -> Optional[Tuple[int, int, str]]:
        first_point_id = records[0].get("PointId") or records[0].get("point_id")
        if not first_point_id:
            self.logger.error("No PointId in tagged JSON")
            return None

        book_id, chapter_id = self.extract_book_chapter_from_pointid(str(first_point_id))
        chapter_name = records[0].get("chapter") or records[0].get("Chapter") or ""
        return book_id, chapter_id, chapter_name

    def _load_caption_indexes(
        self,
        filepic_json_path: Optional[str],
        tablepic_json_path: Optional[str],
    ) -> TopicCaptionIndexes:
        return load_topic_caption_indexes(
            filepic_json_path,
            tablepic_json_path,
            load_json=self.load_json_file,
            extract_rows=self.get_data_from_json,
        )

    def _call_llm_for_topic_script(
        self,
        *,
        prompt: str,
        chapter_name: str,
        subchapter_name: str,
        topic_name: str,
        topic_rows: List[Dict[str, Any]],
        topic_index: int,
        total_topics: int,
        caption_indexes: TopicCaptionIndexes,
        model_name: str,
        cancel_check: Optional[Callable[[], bool]],
    ) -> Optional[List[Dict[str, Any]]]:
        topics_context = build_topic_caption_context(
            chapter_name,
            subchapter_name,
            topic_name,
            caption_indexes,
        )
        full_prompt = build_topic_script_prompt(
            prompt,
            chapter_name,
            subchapter_name,
            topic_name,
            topic_rows,
            topic_index,
            total_topics,
            topics_context=topics_context,
        )
        last_error: Optional[str] = None
        for attempt in range(1, VOICE_SCRIPT_PARSE_RETRIES + 1):
            if cancel_check and cancel_check():
                return None

            attempt_prompt = full_prompt
            if attempt >= 2:
                attempt_prompt = full_prompt + SCRIPT_JSON_RETRY_SUFFIX
            last_attempt = attempt >= VOICE_SCRIPT_PARSE_RETRIES
            use_content_only = not last_attempt
            use_reasoning_none = not last_attempt

            try:
                response = self.api_client.process_text(
                    text=attempt_prompt,
                    model_name=model_name,
                    temperature=0.7,
                    max_tokens=APIConfig.DEFAULT_OPENROUTER_MAX_TOKENS,
                    cancel_check=cancel_check,
                    reasoning_effort_none=use_reasoning_none,
                    content_only=use_content_only,
                )
            except Exception as e:
                last_error = str(e)
                self.logger.warning(
                    "Voice script LLM error topic %s attempt %s/%s: %s",
                    topic_name,
                    attempt,
                    VOICE_SCRIPT_PARSE_RETRIES,
                    e,
                )
                continue

            if not response:
                last_error = "empty response"
                self.logger.warning(
                    "Empty response for topic %s attempt %s/%s",
                    topic_name,
                    attempt,
                    VOICE_SCRIPT_PARSE_RETRIES,
                )
                continue

            topic_paragraphs = self._parse_script_paragraphs_from_response(response)
            if topic_paragraphs:
                return topic_paragraphs

            last_error = "JSON parse produced no paragraphs"
            self.logger.warning(
                "No usable paragraphs for topic %s attempt %s/%s",
                topic_name,
                attempt,
                VOICE_SCRIPT_PARSE_RETRIES,
            )

        self.logger.error(
            "Voice script failed for topic %s after %s attempts: %s",
            topic_name,
            VOICE_SCRIPT_PARSE_RETRIES,
            last_error,
        )
        return None

    def _paragraph_unit_key(self, paragraph: Dict[str, Any]) -> Tuple[str, str, str]:
        return self._topic_key(
            paragraph.get("chapter"),
            paragraph.get("subchapter"),
            paragraph.get("topic"),
        )

    def _remove_unit_paragraphs(
        self,
        paragraphs: List[Dict[str, Any]],
        chapter_name: str,
        subchapter_name: str,
        topic_name: str,
    ) -> List[Dict[str, Any]]:
        target = self._topic_key(chapter_name, subchapter_name, topic_name)
        return [p for p in paragraphs if self._paragraph_unit_key(p) != target]

    def _insert_index_for_unit(
        self,
        paragraphs: List[Dict[str, Any]],
        chapter_name: str,
        subchapter_name: str,
        topic_name: str,
        units: List[Dict[str, Any]],
        unit_index: int,
    ) -> int:
        ordered_keys = [
            self._topic_key(u.get("chapter"), u.get("subchapter"), u.get("topic"))
            for u in sorted(units, key=lambda u: int(u.get("unit_index") or 0))
        ]
        target = self._topic_key(chapter_name, subchapter_name, topic_name)
        try:
            target_pos = ordered_keys.index(target)
        except ValueError:
            return len(paragraphs)
        prior_keys = set(ordered_keys[:target_pos])
        insert_at = 0
        for i, paragraph in enumerate(paragraphs):
            if self._paragraph_unit_key(paragraph) in prior_keys:
                insert_at = i + 1
        return insert_at

    def _renumber_all_paragraphs(
        self,
        paragraphs: List[Dict[str, Any]],
        *,
        chars_per_second: float,
    ) -> List[Dict[str, Any]]:
        next_id = 1
        for paragraph in paragraphs:
            text = (paragraph.get("text") or "").strip()
            paragraph["paragraph_id"] = next_id
            paragraph["char_count"] = len(text)
            paragraph["estimated_seconds"] = (
                round(len(text) / chars_per_second, 2) if chars_per_second and text else 0.0
            )
            next_id += 1
        return paragraphs

    def _filter_paragraphs_for_unit(
        self,
        paragraphs: List[Dict[str, Any]],
        chapter_name: str,
        subchapter_name: str,
        topic_name: str,
    ) -> List[Dict[str, Any]]:
        target = self._topic_key(chapter_name, subchapter_name, topic_name)
        return [p for p in paragraphs if self._paragraph_unit_key(p) == target]

    @staticmethod
    def _renumber_topic_paragraphs(
        topic_paragraphs: List[Dict[str, Any]],
        *,
        chapter_name: str,
        subchapter_name: str,
        topic_name: str,
        start_paragraph_id: int,
        chars_per_second: float,
    ) -> Tuple[List[Dict[str, Any]], int]:
        next_id = start_paragraph_id
        for paragraph in topic_paragraphs:
            paragraph["paragraph_id"] = next_id
            paragraph["chapter"] = (paragraph.get("chapter") or chapter_name or "").strip()
            paragraph["subchapter"] = (paragraph.get("subchapter") or subchapter_name or "").strip()
            paragraph["topic"] = (paragraph.get("topic") or topic_name or "").strip()
            paragraph["estimated_seconds"] = (
                round(paragraph["char_count"] / chars_per_second, 2) if chars_per_second else 0.0
            )
            next_id += 1
        return topic_paragraphs, next_id

    def _save_voice_script_json(
        self,
        *,
        output_dir: str,
        book_id: int,
        chapter_id: int,
        chapter_name: str,
        tagged_json_path: str,
        filepic_json_path: Optional[str],
        tablepic_json_path: Optional[str],
        paragraphs: List[Dict[str, Any]],
        segments: List[Dict[str, Any]],
        total_topics: int,
        max_segment_seconds: float,
        chars_per_second: float,
    ) -> str:
        output_payload = {
            "metadata": {
                "book_id": book_id,
                "chapter_id": chapter_id,
                "chapter_name": chapter_name,
                "source_tagged_json": os.path.basename(tagged_json_path),
                "source_filepic_json": os.path.basename(filepic_json_path) if filepic_json_path else None,
                "source_tablepic_json": os.path.basename(tablepic_json_path) if tablepic_json_path else None,
                "total_paragraphs": len(paragraphs),
                "total_segments": len(segments),
                "total_topics": total_topics,
                "script_mode": "topic_by_topic",
                "max_segment_seconds": max_segment_seconds,
                "chars_per_second": chars_per_second,
            },
            "paragraphs": paragraphs,
            "segments": segments,
        }

        os.makedirs(output_dir, exist_ok=True)
        output_name = f"voice_script_{book_id:03d}{chapter_id:03d}.json"
        output_path = os.path.join(output_dir, output_name)
        with open(output_path, "w", encoding="utf-8") as file_handle:
            json.dump(output_payload, file_handle, ensure_ascii=False, indent=2)
        return output_path

    def _parse_script_paragraphs_from_response(self, response: str) -> List[Dict[str, Any]]:
        parsed = self.extract_json_from_response(response)
        if parsed is None:
            return []
        if isinstance(parsed, dict):
            raw_paragraphs = parsed.get("paragraphs") or parsed.get("data")
        elif isinstance(parsed, list):
            raw_paragraphs = parsed
        else:
            raw_paragraphs = []
        return self._normalize_paragraphs(raw_paragraphs)

    def _normalize_paragraphs(self, raw_paragraphs: Any) -> List[Dict[str, Any]]:
        if not isinstance(raw_paragraphs, list):
            return []
        paragraphs: List[Dict[str, Any]] = []
        for i, item in enumerate(raw_paragraphs, start=1):
            if not isinstance(item, dict):
                continue
            text = (item.get("text") or item.get("Text") or "").strip()
            if not text:
                continue
            pid = item.get("paragraph_id") or item.get("Paragraph") or i
            try:
                pid = int(pid)
            except (TypeError, ValueError):
                pid = i
            char_count = len(text)
            paragraphs.append(
                {
                    "paragraph_id": pid,
                    "chapter": item.get("chapter") or item.get("Chapter") or "",
                    "subchapter": item.get("subchapter") or item.get("Subchapter") or "",
                    "topic": item.get("topic") or item.get("Topic") or "",
                    "text": text,
                    "char_count": char_count,
                    "estimated_seconds": round(char_count / DEFAULT_CHARS_PER_SECOND_ESTIMATE, 2),
                }
            )
        paragraphs.sort(key=lambda p: p["paragraph_id"])
        return paragraphs

    def process_voice_class_step1(
        self,
        tagged_json_path: str,
        prompt: str,
        model_name: str,
        output_dir: str,
        *,
        filepic_json_path: Optional[str] = None,
        tablepic_json_path: Optional[str] = None,
        max_segment_seconds: float = 60.0,
        chars_per_second: float = 13.0,
        delay_seconds: float = 0.0,
        progress_callback: Optional[Callable[[str], None]] = None,
        cancel_check: Optional[Callable[[], bool]] = None,
        unit_hooks: Optional[Any] = None,
    ) -> Optional[str]:
        def report_progress(message: str) -> None:
            if progress_callback:
                progress_callback(message)
            self.logger.info(message)

        if cancel_check and cancel_check():
            return None

        if hasattr(self.api_client, "set_stage"):
            self.api_client.set_stage("voice_class")

        report_progress("Loading Importance & Type JSON...")
        records = self._load_tagged_records(tagged_json_path)
        if not records:
            return None

        chapter_metadata = self._extract_chapter_metadata(records)
        if chapter_metadata is None:
            return None
        book_id, chapter_id, chapter_name = chapter_metadata

        caption_indexes = self._load_caption_indexes(filepic_json_path, tablepic_json_path)
        report_progress(
            f"Loaded captions: filepic topics={caption_indexes.image_topic_count}, "
            f"tablepic topics={caption_indexes.table_topic_count}"
        )

        lesson_context = self._build_lesson_context(records)
        topic_groups = self._group_lesson_context_by_topic(lesson_context)
        if not topic_groups:
            self.logger.error("No Imp 1–2 rows to process")
            return None

        report_progress(
            f"Built lesson context: {len(lesson_context)} Imp 1–2 rows "
            f"across {len(topic_groups)} topic(s)"
        )

        all_paragraphs: List[Dict[str, Any]] = []
        next_paragraph_id = 1
        total_topics = len(topic_groups)
        prompt_seq = 0
        failed_topics = 0

        for topic_index, (chapter_name, subchapter_name, topic_name, topic_rows) in enumerate(
            topic_groups, start=1
        ):
            if cancel_check and cancel_check():
                return None

            topic_label = f"«{topic_name}» ({len(topic_rows)} row(s))"
            if unit_hooks is not None:
                unit_hooks.before_unit(
                    topic_index, chapter_name, subchapter_name, topic_name, prompt_seq + 1
                )
            report_progress(
                f"Topic {topic_index}/{total_topics}: {topic_label} — "
                f"calling OpenRouter ({model_name})..."
            )

            topic_paragraphs = self._call_llm_for_topic_script(
                prompt=prompt,
                chapter_name=chapter_name,
                subchapter_name=subchapter_name,
                topic_name=topic_name,
                topic_rows=topic_rows,
                topic_index=topic_index,
                total_topics=total_topics,
                caption_indexes=caption_indexes,
                model_name=model_name,
                cancel_check=cancel_check,
            )
            prompt_seq += 1
            if topic_paragraphs is None:
                report_progress(
                    f"ERROR: Topic {topic_index}/{total_topics} {topic_label} returned no paragraphs"
                )
                if unit_hooks is not None:
                    unit_hooks.after_unit(
                        topic_index,
                        chapter_name,
                        subchapter_name,
                        topic_name,
                        [],
                        prompt_seq,
                        status="failed",
                    )
                    failed_topics += 1
                    if topic_index < total_topics and delay_seconds > 0:
                        time.sleep(delay_seconds)
                    continue
                return None

            renumbered, next_paragraph_id = self._renumber_topic_paragraphs(
                topic_paragraphs,
                chapter_name=chapter_name,
                subchapter_name=subchapter_name,
                topic_name=topic_name,
                start_paragraph_id=next_paragraph_id,
                chars_per_second=chars_per_second,
            )
            all_paragraphs.extend(renumbered)
            if unit_hooks is not None:
                unit_hooks.after_unit(
                    topic_index,
                    chapter_name,
                    subchapter_name,
                    topic_name,
                    list(renumbered),
                    prompt_seq,
                    status="succeeded",
                )
            report_progress(
                f"Topic {topic_index}/{total_topics} {topic_label}: "
                f"{len(renumbered)} paragraph(s)"
            )

            if topic_index < total_topics and delay_seconds > 0:
                time.sleep(delay_seconds)

        if not all_paragraphs:
            self.logger.error("LLM returned no usable paragraphs")
            return None

        if failed_topics:
            report_progress(
                f"WARNING: {failed_topics} topic(s) failed — partial script saved; "
                f"regenerate failed units from the job page."
            )

        segments = self.pack_segments(
            all_paragraphs,
            max_segment_seconds=max_segment_seconds,
            chars_per_second=chars_per_second,
        )
        if not segments:
            self.logger.error("Segment packing produced no segments")
            return None

        report_progress(
            f"Script: {len(all_paragraphs)} paragraphs → {len(segments)} TTS segments "
            f"(≤{max_segment_seconds}s each)"
        )

        output_path = self._save_voice_script_json(
            output_dir=output_dir,
            book_id=book_id,
            chapter_id=chapter_id,
            chapter_name=chapter_name,
            tagged_json_path=tagged_json_path,
            filepic_json_path=filepic_json_path,
            tablepic_json_path=tablepic_json_path,
            paragraphs=all_paragraphs,
            segments=segments,
            total_topics=total_topics,
            max_segment_seconds=max_segment_seconds,
            chars_per_second=chars_per_second,
        )
        if unit_hooks and hasattr(unit_hooks, "set_output_relpath") and hasattr(unit_hooks, "job_id"):
            from webapp.job_files import job_root

            rel_out = os.path.relpath(output_path, job_root(unit_hooks.job_id)).replace("\\", "/")
            unit_hooks.set_output_relpath(rel_out)
        report_progress(f"Saved voice script: {os.path.basename(output_path)}")
        return output_path

    def _generate_tts_with_rotation(
        self,
        text: str,
        output_wav: str,
        *,
        voice: str,
        model: str,
        instruction: Optional[str],
        progress_callback: Optional[Callable[[str], None]] = None,
    ) -> bool:
        if not self._gemini_keys:
            self.logger.error("No Gemini TTS key manager configured")
            return False

        from webapp.gemini_tts_key_manager import GeminiTtsKeyManager

        mgr: GeminiTtsKeyManager = self._gemini_keys
        attempts = mgr.max_attempts()
        from api_layer import APIKeyManager

        client = GeminiAPIClient(api_key_manager=APIKeyManager(load_env=False))

        for attempt in range(attempts):
            key_row = mgr.get_next_available_key()
            if key_row is None:
                if progress_callback:
                    progress_callback("No Gemini TTS API keys available")
                return False

            try:
                ok = client.generate_tts(
                    text=text,
                    output_file=output_wav,
                    voice=voice,
                    model=model,
                    api_key=key_row.api_key,
                    instruction=instruction,
                )
                if ok:
                    mgr.mark_success(key_row)
                    return True
                mgr.mark_failure(key_row, "TTS returned false")
            except Exception as e:
                err = str(e)
                if progress_callback:
                    progress_callback(f"TTS key {key_row.account_name} failed: {err[:120]}")
                mgr.mark_failure(key_row, err)

        return False

    def process_voice_class_step2(
        self,
        script_json_path: str,
        output_dir: str,
        *,
        intro_mp3: str,
        outro_mp3: str,
        tts_model: str,
        tts_voice: str,
        tts_instruction: Optional[str] = None,
        segment_indices: Optional[List[int]] = None,
        skip_merge: bool = False,
        progress_callback: Optional[Callable[[str], None]] = None,
        cancel_check: Optional[Callable[[], bool]] = None,
    ) -> Optional[str]:
        def _progress(msg: str) -> None:
            if progress_callback:
                progress_callback(msg)
            self.logger.info(msg)

        if cancel_check and cancel_check():
            return None

        script_data = self.load_json_file(script_json_path)
        if not script_data:
            return None

        segments = script_data.get("segments") or []
        meta = script_data.get("metadata") or {}
        book_id = int(meta.get("book_id") or 0)
        chapter_id = int(meta.get("chapter_id") or 0)

        tts_dir = os.path.join(output_dir, "tts_segments")
        os.makedirs(tts_dir, exist_ok=True)

        wanted = set(segment_indices) if segment_indices is not None else None

        # #region agent log
        from webapp.debug_session_log import debug_log

        existing_wavs = sum(
            1
            for seg in segments
            if os.path.isfile(
                os.path.join(tts_dir, f"segment_{int(seg.get('segment_id') or 0):03d}.wav")
            )
        )
        debug_log(
            "H3",
            "stage_voice_processor.py:process_voice_class_step2:entry",
            "step2_tts_loop_start",
            {
                "total_segments": len(segments),
                "existing_wavs": existing_wavs,
                "skip_merge": skip_merge,
                "segment_indices_filter": segment_indices,
            },
        )
        # #endregion

        for seg in segments:
            sid = int(seg.get("segment_id") or 0)
            if wanted is not None and sid not in wanted:
                continue
            if cancel_check and cancel_check():
                return None

            combined = (seg.get("combined_text") or "").strip()
            if not combined:
                continue

            wav_name = f"segment_{sid:03d}.wav"
            wav_path = os.path.join(tts_dir, wav_name)
            _progress(f"TTS segment {sid}/{len(segments)} ({seg.get('paragraph_count', '?')} paragraphs)...")

            ok = self._generate_tts_with_rotation(
                combined,
                wav_path,
                voice=tts_voice,
                model=tts_model,
                instruction=tts_instruction,
                progress_callback=progress_callback,
            )
            if not ok:
                self.logger.error("TTS failed for segment %s", sid)
                return None

            dur = wav_duration_seconds(wav_path)
            if dur is not None:
                est = float(seg.get("estimated_seconds") or 0)
                _progress(f"Segment {sid} audio: {dur:.1f}s (estimated {est:.1f}s)")

        if skip_merge:
            return script_json_path

        segment_wavs = []
        for seg in sorted(segments, key=lambda s: int(s.get("segment_id") or 0)):
            sid = int(seg.get("segment_id") or 0)
            wav_path = os.path.join(tts_dir, f"segment_{sid:03d}.wav")
            if not os.path.isfile(wav_path):
                self.logger.error("Missing WAV for merge: %s", wav_path)
                return None
            segment_wavs.append(wav_path)

        final_name = f"final_voice_{book_id:03d}{chapter_id:03d}.mp3"
        final_path = os.path.join(output_dir, final_name)
        if not intro_mp3 or not os.path.isfile(intro_mp3):
            self.logger.error("Intro MP3 missing: %s", intro_mp3)
            _progress(f"ERROR: Intro song not found at {intro_mp3}")
            return None
        if not outro_mp3 or not os.path.isfile(outro_mp3):
            self.logger.error("Outro MP3 missing: %s", outro_mp3)
            _progress(f"ERROR: Outro song not found at {outro_mp3}")
            return None
        _progress(f"Merging intro ({os.path.basename(intro_mp3)}) + {len(segment_wavs)} segment(s) + outro ({os.path.basename(outro_mp3)})…")
        if not merge_voice_tracks(intro_mp3, segment_wavs, outro_mp3, final_path):
            _progress("ERROR: Merge failed — check worker log (ffmpeg / pydub / song files).")
            return None
        _progress(f"Saved final MP3: {final_name}")
        return final_path

    def merge_existing_segments(
        self,
        script_json_path: str,
        output_dir: str,
        intro_mp3: str,
        outro_mp3: str,
        progress_callback: Optional[Callable[[str], None]] = None,
        cancel_check: Optional[Callable[[], bool]] = None,
    ) -> Optional[str]:
        script_data = self.load_json_file(script_json_path)
        if not script_data:
            return None
        meta = script_data.get("metadata") or {}
        book_id = int(meta.get("book_id") or 0)
        chapter_id = int(meta.get("chapter_id") or 0)
        segments = script_data.get("segments") or []
        tts_dir = os.path.join(output_dir, "tts_segments")
        segment_wavs = []
        for seg in sorted(segments, key=lambda s: int(s.get("segment_id") or 0)):
            sid = int(seg.get("segment_id") or 0)
            wav_path = os.path.join(tts_dir, f"segment_{sid:03d}.wav")
            if not os.path.isfile(wav_path):
                self.logger.error("Missing segment WAV: %s", wav_path)
                return None
            segment_wavs.append(wav_path)
        final_name = f"final_voice_{book_id:03d}{chapter_id:03d}.mp3"
        final_path = os.path.join(output_dir, final_name)
        if not intro_mp3 or not os.path.isfile(intro_mp3):
            self.logger.error("Intro MP3 missing: %s", intro_mp3)
            if progress_callback:
                progress_callback(f"ERROR: Intro song not found at {intro_mp3}")
            return None
        if not outro_mp3 or not os.path.isfile(outro_mp3):
            self.logger.error("Outro MP3 missing: %s", outro_mp3)
            if progress_callback:
                progress_callback(f"ERROR: Outro song not found at {outro_mp3}")
            return None
        if progress_callback:
            progress_callback(
                f"Re-merging intro ({os.path.basename(intro_mp3)}) + {len(segment_wavs)} segment(s) + outro ({os.path.basename(outro_mp3)})…"
            )
        if not merge_voice_tracks(
            intro_mp3,
            segment_wavs,
            outro_mp3,
            final_path,
            cancel_check=cancel_check,
            progress_callback=progress_callback,
        ):
            return None
        return final_path

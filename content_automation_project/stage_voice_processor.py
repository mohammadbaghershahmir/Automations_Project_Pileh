"""
Voice Class processor: OpenRouter script (Step 1) and Gemini TTS + merge (Step 2).
"""

from __future__ import annotations

import json
import logging
import os
from typing import Any, Callable, Dict, List, Optional

from api_layer import APIConfig, GeminiAPIClient
from base_stage_processor import BaseStageProcessor
from webapp.audio_merge import merge_voice_tracks, wav_duration_seconds

logger = logging.getLogger(__name__)

MAX_TTS_CHARS = 4096


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
        out: List[Dict[str, Any]] = []
        for rec in records:
            imp = str(rec.get("Imp") or rec.get("imp") or "").strip()
            if imp not in ("1", "2"):
                continue
            out.append(
                {
                    "PointId": rec.get("PointId") or rec.get("point_id"),
                    "chapter": rec.get("chapter", ""),
                    "subchapter": rec.get("subchapter", ""),
                    "topic": rec.get("topic", ""),
                    "subtopic": rec.get("subtopic", ""),
                    "subsubtopic": rec.get("subsubtopic", ""),
                    "Points": rec.get("Points") or rec.get("points", ""),
                    "Imp": imp,
                    "Type": rec.get("Type") or rec.get("type", ""),
                }
            )
        return out

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
                    "estimated_seconds": round(char_count / 13.0, 2),
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
        max_segment_seconds: float = 60.0,
        chars_per_second: float = 13.0,
        progress_callback: Optional[Callable[[str], None]] = None,
        cancel_check: Optional[Callable[[], bool]] = None,
    ) -> Optional[str]:
        def _progress(msg: str) -> None:
            if progress_callback:
                progress_callback(msg)
            self.logger.info(msg)

        if cancel_check and cancel_check():
            return None

        if hasattr(self.api_client, "set_stage"):
            self.api_client.set_stage("voice_class")

        _progress("Loading Importance & Type JSON...")
        tagged_data = self.load_json_file(tagged_json_path)
        if not tagged_data:
            self.logger.error("Failed to load tagged JSON")
            return None

        records = self.get_data_from_json(tagged_data)
        if not records:
            self.logger.error("Tagged JSON has no data rows")
            return None

        first_pid = records[0].get("PointId") or records[0].get("point_id")
        if not first_pid:
            self.logger.error("No PointId in tagged JSON")
            return None
        book_id, chapter_id = self.extract_book_chapter_from_pointid(str(first_pid))
        chapter_name = records[0].get("chapter") or records[0].get("Chapter") or ""

        lesson_context = self._build_lesson_context(records)
        _progress(f"Built lesson context: {len(lesson_context)} Imp 1–2 rows")

        full_prompt = f"""{prompt}

Tagged lesson JSON (Imp 1 and 2 points only):
{json.dumps(lesson_context, ensure_ascii=False, indent=2)}

Return ONLY valid JSON with this structure:
{{
  "paragraphs": [
    {{
      "paragraph_id": 1,
      "chapter": "...",
      "subchapter": "...",
      "topic": "...",
      "text": "..."
    }}
  ]
}}
Each paragraph must be a self-contained spoken block in Farsi (complete sentences)."""

        _progress(f"Calling OpenRouter ({model_name}) for voice script...")
        response = self.api_client.process_text(
            text=full_prompt,
            model_name=model_name,
            temperature=0.7,
            max_tokens=APIConfig.DEFAULT_OPENROUTER_MAX_TOKENS,
            cancel_check=cancel_check,
        )
        if not response:
            self.logger.error("Empty response from OpenRouter")
            return None

        parsed = self.extract_json_from_response(response)
        if parsed is None:
            self.logger.error("Could not parse script JSON from LLM response")
            return None

        if isinstance(parsed, dict):
            raw_paragraphs = parsed.get("paragraphs") or parsed.get("data")
        elif isinstance(parsed, list):
            raw_paragraphs = parsed
        else:
            raw_paragraphs = []

        paragraphs = self._normalize_paragraphs(raw_paragraphs)
        if not paragraphs:
            self.logger.error("LLM returned no usable paragraphs")
            return None

        for p in paragraphs:
            p["estimated_seconds"] = round(p["char_count"] / chars_per_second, 2) if chars_per_second else 0.0

        segments = self.pack_segments(
            paragraphs,
            max_segment_seconds=max_segment_seconds,
            chars_per_second=chars_per_second,
        )
        if not segments:
            self.logger.error("Segment packing produced no segments")
            return None

        _progress(f"Script: {len(paragraphs)} paragraphs → {len(segments)} TTS segments (≤{max_segment_seconds}s each)")

        output_payload = {
            "metadata": {
                "book_id": book_id,
                "chapter_id": chapter_id,
                "chapter_name": chapter_name,
                "source_tagged_json": os.path.basename(tagged_json_path),
                "total_paragraphs": len(paragraphs),
                "total_segments": len(segments),
                "max_segment_seconds": max_segment_seconds,
                "chars_per_second": chars_per_second,
            },
            "paragraphs": paragraphs,
            "segments": segments,
        }

        os.makedirs(output_dir, exist_ok=True)
        out_name = f"voice_script_{book_id:03d}{chapter_id:03d}.json"
        out_path = os.path.join(output_dir, out_name)
        with open(out_path, "w", encoding="utf-8") as f:
            json.dump(output_payload, f, ensure_ascii=False, indent=2)
        _progress(f"Saved voice script: {out_name}")
        return out_path

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

        for attempt in range(attempts):
            key_row = mgr.get_next_available_key()
            if key_row is None:
                if progress_callback:
                    progress_callback("No Gemini TTS API keys available")
                return False

            client = GeminiAPIClient()
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
        _progress("Merging intro + segments + outro into MP3...")
        if not merge_voice_tracks(intro_mp3, segment_wavs, outro_mp3, final_path):
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
        if progress_callback:
            progress_callback("Re-merging intro + segments + outro...")
        if not merge_voice_tracks(intro_mp3, segment_wavs, outro_mp3, final_path):
            return None
        return final_path

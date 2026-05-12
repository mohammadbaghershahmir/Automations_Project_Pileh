"""
Stage E Processor: Image Notes Processing
Takes Stage 4 JSON (with PointId) and Stage 1 JSON, generates image notes,
and merges them with Stage 4 data.
"""

import json
import logging
import math
import os
import re
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Optional, Dict, List, Any, Callable, Tuple
from base_stage_processor import BaseStageProcessor
from api_layer import APIConfig


class StageEProcessor(BaseStageProcessor):
    """Process Stage E: Generate image notes (topic-parallel LLM calls) and merge with Stage 4."""

    SUBCHAPTER_MODEL_MAX_ATTEMPTS = 6
    SUBCHAPTER_MODEL_TIMEOUT_S = 900.0
    SUBCHAPTER_RETRY_BACKOFF_CAP_S = 120.0
    # Subchapter JSON arrays are large; low completion caps truncate mid-object → unparseable JSON.
    SUBCHAPTER_MODEL_MAX_COMPLETION_TOKENS = 32768
    # Default path: one LLM call per Stage-4 topic group, batched like Stage V Step 2.
    STAGE_E_TOPIC_BATCH_SIZE = 10
    STAGE_E_HIERARCHY_REPAIR_MAX_ATTEMPTS = 2
    STAGE_E_HIERARCHY_REPAIR_MAX_TOKENS = 16384

    _HIERARCHY_REPAIR_INSTRUCTION = """تو یک اصلاحگر JSON هستی.

ورودی: (۱) بخش کوچک OCR همان مبحث، (۲) نقاط Stage 4 همان مبحث، (۳) لیست ردیف‌های خروجی تصویر که فقط فیلدهای subtopic یا subsubtopic خالی دارند.

وظیفه: فقط برای همان ردیف‌ها مقدار subtopic و subsubtopic را از سلسله‌مراتب Stage 4 (همان مبحث) پر کن. از Stage 4 کپی کن؛ حدس نزن مگر مجبور شوی.

قوانین سخت:
- خروجی فقط یک JSON معتبر باشد؛ بدون markdown و بدون متن بیرون JSON.
- همان تعداد ردیف ورودی ناقص؛ هیچ ردیف جدید/حذف/تکرار نکن.
- point_text و caption را عیناً برنگردان مگر در خروجی برای تطبیق لازم باشد؛ در payload فقط subtopic و subsubtopic و point_text برای کلید باشد.
- ساختار خروجی دقیق:
{"payload":[{"point_text":"...","subtopic":"...","subsubtopic":"..."}]}
- اگر برای یک point_text جای دقیق در Stage 4 نبود، نزدیک‌ترین subtopic/subsubtopic همان topic را بگذار (یک مقدار ثابت برای همان topic).

همهٔ تصاویر/ردیف‌های ناقص را در یک payload برگردان."""

    def __init__(self, api_client):
        super().__init__(api_client)
        self.logger = logging.getLogger(__name__)
        self._stage_e_raw_lock = threading.Lock()
        self._stage_e_api_lock = threading.Lock()

    def _coerce_filepic_rows(self, filepic_data: Any, persian_subchapter_name: str) -> List[Dict[str, Any]]:
        """Normalize model output (list or dict with data/rows/payload) to row dicts."""
        self.logger.info(
            f"Processing filepic_data for subchapter '{persian_subchapter_name}': type={type(filepic_data)}"
        )
        if isinstance(filepic_data, list):
            self.logger.info(f"filepic_data is a list with {len(filepic_data)} items")
            return filepic_data
        if isinstance(filepic_data, dict):
            self.logger.info(f"filepic_data is a dict with keys: {list(filepic_data.keys())}")
            rows = filepic_data.get("data", filepic_data.get("rows", filepic_data.get("payload", [])))
            self.logger.info(
                f"Extracted records from dict: {len(rows) if isinstance(rows, list) else 'not a list'} items"
            )
            if isinstance(rows, list) and rows:
                return rows
            for key, value in filepic_data.items():
                if isinstance(value, list) and value:
                    self.logger.info(f"  Found list in key '{key}' with {len(value)} items")
                    return value
            self.logger.warning("No list found in any dict value")
            return []
        self.logger.warning(f"Unexpected JSON structure from model: {type(filepic_data)}")
        return []

    _STAGE4_TEXT_KEYS = (
        "Points",
        "point_text",
        "points",
        "PointText",
        "text",
        "content",
        "point",
        "narrative",
    )
    _FIG_REF_RE = re.compile(
        r"(تصویر(\s+الکترونیکی)?|fig\.?\s*\d|figure\s+\d|e[-\s]?fig\w*|electronic\s+figure)",
        re.IGNORECASE | re.UNICODE,
    )

    @classmethod
    def _stage4_row_primary_text(cls, point: Dict[str, Any]) -> str:
        """Best-effort main lesson line — schemas vary (Points vs point_text vs nested)."""
        for key in cls._STAGE4_TEXT_KEYS:
            v = point.get(key)
            if isinstance(v, str) and v.strip():
                return v.strip()
        return ""

    @staticmethod
    def _stage4_text_is_table_anchor(text: str) -> bool:
        s = (text or "").strip()
        if not s:
            return False
        if s.startswith("جدول"):
            return True
        ls = s.lower()
        if ls.startswith("table ") or ls.startswith("table:") or ls.startswith("table\n"):
            return True
        if ls == "table":
            return True
        return False

    @classmethod
    def _stage4_text_looks_like_figure_ref(cls, text: str) -> bool:
        """True when the row text clearly references a figure (not used for table-only rows)."""
        t = (text or "").strip()
        if not t or cls._stage4_text_is_table_anchor(t):
            return False
        if t.startswith("تصویر"):
            return True
        low = t.lower()
        if low.startswith(("fig.", "figure", "fig ")):
            return True
        return cls._FIG_REF_RE.search(t) is not None

    @classmethod
    def _stage4_point_is_image_anchor(cls, point: Dict[str, Any]) -> bool:
        """True if this lesson row clearly tags a figure (تصویر / Fig …), not a table row."""
        return cls._stage4_text_looks_like_figure_ref(cls._stage4_row_primary_text(point))

    def _filepic_rows_images_only(
        self, rows: List[Dict[str, Any]], persian_subchapter_name: str
    ) -> List[Dict[str, Any]]:
        """Drop table rows the model may still emit; image notes must not create table points."""
        kept: List[Dict[str, Any]] = []
        dropped = 0
        for r in rows:
            if not isinstance(r, dict):
                continue
            pt = (r.get("point_text") or r.get("Points") or "").strip()
            if self._stage4_text_is_table_anchor(pt):
                dropped += 1
                continue
            kept.append(r)
        if dropped:
            self.logger.info(
                f"Removed {dropped} table-like row(s) from model output for subchapter "
                f"'{persian_subchapter_name}' (image notes are figures only)."
            )
        return kept

    @staticmethod
    def _is_openrouter_context_limit_error(exc: BaseException) -> bool:
        """True when the provider rejected the request because prompt + max_tokens exceed context."""
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
    def _stage4_points_grouped_by_topic_in_order(
        points: List[Dict[str, Any]],
    ) -> List[Tuple[str, List[Dict[str, Any]]]]:
        """Stable topic order (first-seen); empty/missing topic → single bucket."""
        order: List[str] = []
        buckets: Dict[str, List[Dict[str, Any]]] = {}
        for p in points:
            key = (p.get("topic") or "").strip() or "(بدون مبحث)"
            if key not in buckets:
                order.append(key)
                buckets[key] = []
            buckets[key].append(p)
        return [(k, buckets[k]) for k in order]

    def _build_image_notes_stage_e_prompt(
        self,
        prompt_with_subchapter: str,
        ocr_extraction_json_str: str,
        persian_subchapter_name: str,
        stage4_points: List[Dict[str, Any]],
        scope_note: str = "",
    ) -> str:
        """Assemble Stage E user prompt: optional scope note + OCR slice + Stage 4 JSON."""
        stage4_json_str = json.dumps(stage4_points, ensure_ascii=False, separators=(",", ":"))
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
فایل JSON ساختار سلسله‌مراتبی درسنامه نهایی (Stage 4 JSON — زیرفصل '{persian_subchapter_name}' — {len(stage4_points)} نقطه):
==================================================
{stage4_json_str}
"""

    def _append_stage_e_raw_response_entry(
        self, output_path: str, entry: Dict[str, Any]
    ) -> None:
        """Append one raw model response to the Stage E incremental JSON file (thread-safe)."""
        with self._stage_e_raw_lock:
            with open(output_path, "r", encoding="utf-8") as f:
                current_data = json.load(f)
            current_data["raw_responses"].append(entry)
            current_data["metadata"]["subchapters_processed"] = len(current_data["raw_responses"])
            current_data["metadata"]["processed_at"] = datetime.now().isoformat()
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(current_data, f, ensure_ascii=False, indent=2)

    def _call_image_notes_llm_with_retries(
        self,
        full_prompt: str,
        model_name: str,
        max_tokens: int,
        max_attempts: int,
        attempt_label: str,
        _progress: Callable[[str], None],
    ) -> Tuple[Optional[str], Optional[Any], bool]:
        """
        Call the model and parse filepic JSON. Retries on transient failures.

        Returns:
            (response_text, filepic_data, context_window_exceeded)
            If context_window_exceeded is True, the prompt must be shrunk — do not retry as-is.
        """
        response_text: Optional[str] = None
        filepic_data: Optional[Any] = None

        for attempt in range(1, max_attempts + 1):
            if attempt > 1:
                backoff = min(
                    8.0 * (2 ** (attempt - 2)),
                    self.SUBCHAPTER_RETRY_BACKOFF_CAP_S,
                )
                _progress(
                    f"Waiting {backoff:.0f}s before retry "
                    f"(attempt {attempt}/{max_attempts}) [{attempt_label}]..."
                )
                time.sleep(backoff)
                _progress(f"Retrying model call (attempt {attempt}/{max_attempts}) [{attempt_label}]...")

            try:
                with self._stage_e_api_lock:
                    response_text = self.api_client.process_text(
                        text=full_prompt,
                        system_prompt=None,
                        model_name=model_name,
                        temperature=APIConfig.DEFAULT_TEMPERATURE,
                        max_tokens=max_tokens,
                        timeout_s=self.SUBCHAPTER_MODEL_TIMEOUT_S,
                    )
            except Exception as e:
                if self._is_openrouter_context_limit_error(e):
                    self.logger.warning(
                        "Context window limit (%s, attempt %s/%s); not retrying same prompt: %s",
                        attempt_label,
                        attempt,
                        max_attempts,
                        e,
                    )
                    return None, None, True
                self.logger.warning(
                    "Error calling model (%s, attempt %s/%s): %s",
                    attempt_label,
                    attempt,
                    max_attempts,
                    e,
                )
                response_text = None
                continue

            if not response_text:
                continue

            _progress("Extracting JSON from model response...")
            self.logger.info(f"Response text length: {len(response_text)} chars")
            self.logger.debug(f"Response text preview: {response_text[:200]}")
            filepic_data = self.extract_json_from_response(response_text)
            self.logger.info(
                f"Extracted filepic_data type: {type(filepic_data)}, value: {filepic_data}"
            )
            if not filepic_data:
                self.logger.warning(
                    "First extraction failed, trying load_txt_as_json_from_text..."
                )
                filepic_data = self.load_txt_as_json_from_text(response_text)
                self.logger.info(
                    f"Second extraction result type: {type(filepic_data)}, value: {filepic_data}"
                )
            if not filepic_data and response_text:
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

            if filepic_data:
                _progress(f"Successfully extracted JSON (attempt {attempt}) [{attempt_label}]")
                self.logger.info(
                    f"Successfully extracted JSON: {type(filepic_data)} with keys: "
                    f"{filepic_data.keys() if isinstance(filepic_data, dict) else 'N/A'}"
                )
                return response_text, filepic_data, False

            _progress(f"JSON extraction failed (attempt {attempt}), retrying... [{attempt_label}]")
            self.logger.warning(f"Failed to extract JSON from response (attempt {attempt})")

        return response_text, filepic_data, False

    @staticmethod
    def _normalize_point_text_key(s: str) -> str:
        t = (s or "").strip()
        return re.sub(r"\s+", " ", t)

    @classmethod
    def _image_note_row_needs_hierarchy(cls, row: Dict[str, Any]) -> bool:
        st = (row.get("subtopic") or "").strip()
        sst = (row.get("subsubtopic") or "").strip()
        return (not st) or (not sst)

    def _topic_ocr_json_for_stage_e(
        self,
        ocr_extraction_data: Dict[str, Any],
        persian_subchapter_name: str,
        stage4_topic_bucket: str,
    ) -> str:
        # Stage 4 uses "(بدون مبحث)" for missing topic; OCR JSON often uses "" on topic objects.
        ocr_topic_filter = "" if stage4_topic_bucket == "(بدون مبحث)" else stage4_topic_bucket
        topic_ocr_slice = self._filter_ocr_extraction_for_subchapter_topic(
            ocr_extraction_data,
            persian_subchapter_name,
            ocr_topic_filter,
        )
        topic_ocr_slice = self._slim_ocr_for_stage_e_image_notes(topic_ocr_slice)
        return json.dumps(topic_ocr_slice, ensure_ascii=False, separators=(",", ":"))

    def _run_stage_e_single_topic(
        self,
        topic_index: int,
        topic_name: str,
        pts: List[Dict[str, Any]],
        *,
        prompt_with_subchapter: str,
        persian_subchapter_name: str,
        ocr_extraction_data: Dict[str, Any],
        model_name: str,
        output_path: str,
        part_num: int,
        _progress: Callable[[str], None],
        raw_response_kind: str = "topic_parallel",
    ) -> Tuple[int, str, List[Dict[str, Any]], Optional[str]]:
        """
        One LLM call for a single Stage-4 topic bucket (topic-scoped OCR + topic points).

        Returns (topic_index, topic_name, image_rows, error_message_or_None).
        """
        if not pts:
            return topic_index, topic_name, [], None

        topic_ocr_extraction_json_str = self._topic_ocr_json_for_stage_e(
            ocr_extraction_data, persian_subchapter_name, topic_name
        )
        scope = (
            f"[محدوده مرجع: فقط مبحث «{topic_name}» در همین زیرفصل. "
            f"تعداد نقاط Stage 4 در این درخواست: {len(pts)}. "
            "خروجی JSON را فقط برای تصاویر/مواردی که در این فهرست نقاط هستند تولید کن.]"
        )
        full_prompt = self._build_image_notes_stage_e_prompt(
            prompt_with_subchapter,
            topic_ocr_extraction_json_str,
            persian_subchapter_name,
            pts,
            scope_note=scope,
        )
        label = f"topic «{topic_name}» ({len(pts)} Stage 4 row(s))"
        response_text, filepic_data, ctx_hit = self._call_image_notes_llm_with_retries(
            full_prompt,
            model_name,
            self.SUBCHAPTER_MODEL_MAX_COMPLETION_TOKENS,
            self.SUBCHAPTER_MODEL_MAX_ATTEMPTS,
            label,
            _progress,
        )

        if filepic_data:
            rows = self._filepic_rows_images_only(
                self._coerce_filepic_rows(filepic_data, persian_subchapter_name),
                persian_subchapter_name,
            )
            try:
                entry: Dict[str, Any] = {
                    "subchapter_index": part_num,
                    "subchapter": persian_subchapter_name,
                    "stage4_point_count": len(pts),
                    "topic": topic_name,
                    "topic_index": topic_index,
                    "call_kind": raw_response_kind,
                    "response_text": response_text,
                    "response_size_bytes": len((response_text or "").encode("utf-8")),
                    "processed_at": datetime.now().isoformat(),
                }
                self._append_stage_e_raw_response_entry(output_path, entry)
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
                f"({len(pts)} Stage 4 row(s)); no bisect configured"
            )
            self.logger.error("Stage E topic call: %s", msg)
            return topic_index, topic_name, [], msg

        msg = f"topic «{topic_name}» ({len(pts)} rows): no model response / JSON after retries"
        self.logger.error("Stage E topic call: %s", msg)
        return topic_index, topic_name, [], msg

    def _process_subchapter_image_topics_parallel(
        self,
        topic_groups: List[Tuple[str, List[Dict[str, Any]]]],
        *,
        prompt_with_subchapter: str,
        persian_subchapter_name: str,
        ocr_extraction_data: Dict[str, Any],
        model_name: str,
        output_path: str,
        part_num: int,
        _progress: Callable[[str], None],
    ) -> Tuple[List[Dict[str, Any]], List[str]]:
        """Run topic calls in bounded parallel batches; preserve topic order in merged rows."""
        n_topics = len(topic_groups)
        if n_topics == 0:
            return [], []

        results_by_idx: Dict[int, Tuple[str, List[Dict[str, Any]], Optional[str]]] = {}
        topic_errors: List[str] = []

        def _progress_log_only(msg: str) -> None:
            """Workers must not touch the task's SQLAlchemy Session (see append_log / pair ORM)."""
            self.logger.info("%s", msg)

        batch_size = self.STAGE_E_TOPIC_BATCH_SIZE
        total_batches = (n_topics + batch_size - 1) // batch_size
        for batch_num, start in enumerate(range(0, n_topics, batch_size), 1):
            batch = topic_groups[start : start + batch_size]
            bw = len(batch)
            _progress(
                f"Stage E topic batch {batch_num}/{total_batches}: {bw} topic(s) "
                f"(concurrency={bw}) for '{persian_subchapter_name}'..."
            )
            with ThreadPoolExecutor(max_workers=bw) as executor:
                future_to_idx: Dict[Any, int] = {}
                for rel_i, (topic_name, pts) in enumerate(batch):
                    tidx = start + rel_i
                    fut = executor.submit(
                        self._run_stage_e_single_topic,
                        tidx,
                        topic_name,
                        pts,
                        prompt_with_subchapter=prompt_with_subchapter,
                        persian_subchapter_name=persian_subchapter_name,
                        ocr_extraction_data=ocr_extraction_data,
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
                            "Stage E topic worker crashed (idx=%s): %s", tidx, e, exc_info=True
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
                    if err:
                        topic_errors.append(err)
                        _progress(f"  ✗ Topic (topic_parallel): «{tn}» — {err}")
                    else:
                        _progress(
                            f"  ✓ Topic call (topic_parallel): {len(rows)} image row(s) for "
                            f"«{tn}» ({len(pts)} Stage 4 point(s))"
                        )

        merged: List[Dict[str, Any]] = []
        for i in range(n_topics):
            _tn, rows, _e = results_by_idx[i]
            merged.extend(rows)
        return merged, topic_errors

    def _fill_row_hierarchy_from_stage4_point_text(
        self, row: Dict[str, Any], topic_pts: List[Dict[str, Any]]
    ) -> bool:
        """If Stage 4 has a row whose primary text matches this image point_text, copy hierarchy."""
        pk = self._normalize_point_text_key(
            str(row.get("point_text") or row.get("Points") or "")
        )
        if not pk:
            return False
        for p in topic_pts:
            s4 = self._normalize_point_text_key(self._stage4_row_primary_text(p))
            if not s4 or s4 != pk:
                continue
            if not (row.get("subtopic") or "").strip() and (p.get("subtopic") or "").strip():
                row["subtopic"] = str(p.get("subtopic")).strip()
            if not (row.get("subsubtopic") or "").strip() and (p.get("subsubtopic") or "").strip():
                row["subsubtopic"] = str(p.get("subsubtopic")).strip()
            return True
        return False

    def _fill_row_hierarchy_from_first_stage4_row(
        self, row: Dict[str, Any], topic_pts: List[Dict[str, Any]]
    ) -> None:
        if not topic_pts:
            return
        first = topic_pts[0]
        st = (first.get("subtopic") or "").strip()
        sst = (first.get("subsubtopic") or "").strip()
        if not (row.get("subtopic") or "").strip() and st:
            row["subtopic"] = st
        if not (row.get("subsubtopic") or "").strip() and sst:
            row["subsubtopic"] = sst

    def _coerce_hierarchy_repair_patches(self, data: Any) -> Dict[str, Dict[str, str]]:
        """Map normalized point_text -> {subtopic, subsubtopic}."""
        out: Dict[str, Dict[str, str]] = {}
        rows: List[Dict[str, Any]] = []
        if isinstance(data, dict):
            rows = self._coerce_filepic_rows(data, "")
        elif isinstance(data, list):
            rows = [x for x in data if isinstance(x, dict)]
        for r in rows:
            pt = self._normalize_point_text_key(
                str(r.get("point_text") or r.get("Points") or "")
            )
            if not pt:
                continue
            st = (r.get("subtopic") or "").strip()
            sst = (r.get("subsubtopic") or "").strip()
            if st or sst:
                out[pt] = {"subtopic": st, "subsubtopic": sst}
        return out

    def _repair_image_hierarchy_per_topic(
        self,
        subchapter_rows: List[Dict[str, Any]],
        *,
        filtered_stage4_points: List[Dict[str, Any]],
        persian_subchapter_name: str,
        ocr_extraction_data: Dict[str, Any],
        model_name: str,
        output_path: str,
        part_num: int,
        _progress: Callable[[str], None],
    ) -> None:
        """Fill missing subtopic/subsubtopic: Stage4 match, then first-row fallback, then LLM per topic."""
        by_topic: Dict[str, List[Dict[str, Any]]] = {}
        for row in subchapter_rows:
            if not isinstance(row, dict):
                continue
            if not self._image_note_row_needs_hierarchy(row):
                continue
            tk = (row.get("topic") or "").strip() or "(بدون مبحث)"
            by_topic.setdefault(tk, []).append(row)

        for topic_key, bad_rows in by_topic.items():
            topic_pts = [
                p
                for p in filtered_stage4_points
                if (p.get("subchapter") or "").strip() == persian_subchapter_name
                and ((p.get("topic") or "").strip() or "(بدون مبحث)") == topic_key
            ]
            for r in bad_rows:
                self._fill_row_hierarchy_from_stage4_point_text(r, topic_pts)
            for r in bad_rows:
                if self._image_note_row_needs_hierarchy(r):
                    self._fill_row_hierarchy_from_first_stage4_row(r, topic_pts)

            still_bad = [r for r in bad_rows if self._image_note_row_needs_hierarchy(r)]
            if not still_bad:
                continue

            minimal = []
            for r in still_bad:
                minimal.append(
                    {
                        "point_text": r.get("point_text") or r.get("Points") or "",
                        "chapter": r.get("chapter", ""),
                        "subchapter": r.get("subchapter", ""),
                        "topic": r.get("topic", ""),
                        "subtopic": r.get("subtopic", ""),
                        "subsubtopic": r.get("subsubtopic", ""),
                    }
                )
            topic_ocr_json = self._topic_ocr_json_for_stage_e(
                ocr_extraction_data, persian_subchapter_name, topic_key
            )
            stage4_json = json.dumps(topic_pts, ensure_ascii=False, separators=(",", ":"))
            incomplete_json = json.dumps(minimal, ensure_ascii=False, separators=(",", ":"))
            repair_prompt = f"""{self._HIERARCHY_REPAIR_INSTRUCTION}

زیرفصل: {persian_subchapter_name}
مبحث: {topic_key}

=== OCR (همان مبحث، فقط شکل‌ها) ===
{topic_ocr_json}

=== Stage 4 (همان مبحث) ===
{stage4_json}

=== ردیف‌های ناقص (subtopic یا subsubtopic خالی) ===
{incomplete_json}
"""
            label = f"hierarchy repair «{topic_key}» ({len(still_bad)} row(s))"
            resp, parsed, _ctx = self._call_image_notes_llm_with_retries(
                repair_prompt,
                model_name,
                self.STAGE_E_HIERARCHY_REPAIR_MAX_TOKENS,
                self.STAGE_E_HIERARCHY_REPAIR_MAX_ATTEMPTS,
                label,
                _progress,
            )
            if parsed:
                try:
                    self._append_stage_e_raw_response_entry(
                        output_path,
                        {
                            "subchapter_index": part_num,
                            "subchapter": persian_subchapter_name,
                            "topic": topic_key,
                            "call_kind": "hierarchy_repair",
                            "response_text": resp,
                            "response_size_bytes": len((resp or "").encode("utf-8")),
                            "processed_at": datetime.now().isoformat(),
                        },
                    )
                except Exception as e:
                    self.logger.warning("Could not append hierarchy repair raw: %s", e)

            patches = self._coerce_hierarchy_repair_patches(parsed) if parsed else {}
            if not patches:
                self.logger.warning(
                    "Hierarchy repair produced no patches for subchapter=%r topic=%r",
                    persian_subchapter_name,
                    topic_key,
                )
                continue
            for r in still_bad:
                pk = self._normalize_point_text_key(
                    str(r.get("point_text") or r.get("Points") or "")
                )
                patch = patches.get(pk)
                if not patch:
                    continue
                if not (r.get("subtopic") or "").strip() and patch.get("subtopic"):
                    r["subtopic"] = patch["subtopic"]
                if not (r.get("subsubtopic") or "").strip() and patch.get("subsubtopic"):
                    r["subsubtopic"] = patch["subsubtopic"]

    def process_stage_e(
        self,
        stage4_path: str,
        ocr_extraction_json_path: str,
        prompt: str,
        model_name: str,
        output_dir: Optional[str] = None,
        progress_callback: Optional[Callable[[str], None]] = None
    ) -> Optional[str]:
        """
        Process Stage E: Generate image notes and merge with Stage 4.
        
        Args:
            stage4_path: Path to Stage 4 JSON file (with PointId)
            ocr_extraction_json_path: Path to OCR Extraction JSON file (for subchapter names)
            prompt: User prompt for image notes generation
            model_name: Gemini model name
            output_dir: Output directory (defaults to stage4_path directory)
            progress_callback: Optional callback for progress updates
            
        Returns:
            Path to output file (e{book}{chapter}_{base_name}.json) or None on error
            Example: e105003_Lesson_file_1_1.json
        """
        def _progress(msg: str):
            if progress_callback:
                progress_callback(msg)
            self.logger.info(msg)
        
        # Set stage if using UnifiedAPIClient (for API routing)
        if hasattr(self.api_client, 'set_stage'):
            self.api_client.set_stage("stage_e")
        
        _progress("Starting Stage E processing...")
        
        # Load OCR Extraction JSON (Stage 1) - contains figure and table descriptions
        _progress("Loading OCR Extraction JSON (Stage 1)...")
        ocr_extraction_data = self.load_json_file(ocr_extraction_json_path)
        if not ocr_extraction_data:
            self.logger.error("Failed to load OCR Extraction JSON")
            _progress("Error: Failed to load OCR Extraction JSON")
            return None
        
        # Load Stage 4 JSON
        _progress("Loading Stage 4 JSON...")
        stage4_data = self.load_json_file(stage4_path)
        if not stage4_data:
            self.logger.error("Failed to load Stage 4 JSON")
            return None
        
        # Extract data from Stage 4
        stage4_points = self.get_data_from_json(stage4_data)
        
        if not stage4_points:
            self.logger.error("Stage 4 JSON has no data/points")
            _progress("Error: Stage 4 JSON has no data/points")
            return None
        
        # Extract book and chapter from first PointId in Stage 4
        first_point_id = stage4_points[0].get("PointId")
        if not first_point_id:
            self.logger.error("No PointId found in Stage 4 data")
            return None
        
        try:
            book_id, chapter_id = self.extract_book_chapter_from_pointid(first_point_id)
        except ValueError as e:
            self.logger.error(f"Error extracting book/chapter: {e}")
            return None
        
        _progress(f"Detected Book ID: {book_id}, Chapter ID: {chapter_id}")
        
        # Get last PointId from Stage 4 to continue numbering
        last_point = stage4_points[-1]
        last_point_id = last_point.get("PointId", "")
        if not last_point_id:
            self.logger.error("No PointId in last point of Stage 4")
            return None
        
        # Extract current index from last PointId
        try:
            current_index = int(last_point_id[6:10]) + 1  # Next index after last point
        except (ValueError, IndexError):
            self.logger.error(f"Invalid PointId format: {last_point_id}")
            return None
        
        _progress(f"Last PointId in Stage 4: {last_point_id}, Starting image notes from index: {current_index}")
        
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
        base_dir = os.path.dirname(stage4_path) or os.getcwd()
        base_name, _ = os.path.splitext(os.path.basename(stage4_path))
        
        # Generate output filename with base_name to ensure uniqueness per chapter
        # Format: e{book}{chapter}_{base_name}.json (e.g., e105003_Lesson_file_1_1.json)
        output_filename = f"e{book_id:03d}{chapter_id:03d}_{base_name}.json"
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
                "source_stage4": os.path.basename(stage4_path),
                "source_ocr_extraction": os.path.basename(ocr_extraction_json_path),
                "model_used": model_name,
                "division_method": "ocr_extraction_by_subchapter",
                "stage_e_call_mode": "topic_parallel",
                "ocr_subchapters": ocr_subchapters,
                "stage": "E",
            },
            "data": [],  # Will contain merged points at the end
            "raw_responses": []  # All raw model responses
        }
        
        try:
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(initial_output, f, ensure_ascii=False, indent=2)
            self.logger.info(f"Initialized Stage E JSON file: {output_path}")
        except Exception as e:
            self.logger.error(f"Failed to initialize output file: {e}")
            return None
        
        all_filepic_records = []  # Collect all filepic records from all subchapters
        subchapter_errors: List[str] = []  # Non-empty => abort — avoids marking job succeeded with partial output

        # Process each subchapter individually
        for part_num, persian_subchapter_name in enumerate(ocr_subchapters, 1):
            _progress("=" * 60)
            _progress(f"Processing Part {part_num}/{num_parts} - Subchapter: '{persian_subchapter_name}'...")
            _progress("=" * 60)
            
            # Filter Stage 4 points for this subchapter only
            _progress(f"Filtering Stage 4 points for subchapter '{persian_subchapter_name}'...")
            filtered_stage4_points = [
                point for point in stage4_points 
                if point.get("subchapter", "").strip() == persian_subchapter_name
            ]
            
            _progress(f"Found {len(filtered_stage4_points)} points for subchapter '{persian_subchapter_name}' (out of {len(stage4_points)} total)")
            
            if not filtered_stage4_points:
                self.logger.warning(f"No Stage 4 points found for subchapter '{persian_subchapter_name}'. Skipping this subchapter.")
                _progress(f"Warning: No Stage 4 points found for subchapter '{persian_subchapter_name}'. Skipping...")
                continue

            image_stage4_points = [
                p for p in filtered_stage4_points if self._stage4_point_is_image_anchor(p)
            ]
            if not image_stage4_points:
                # Some Stage 4 exports use only plain bullets (no "تصویر" prefix) — fall back to
                # all subchapter rows except obvious table-placeholder lines; model + post-filter
                # still enforce figures-only output.
                non_table = [
                    p
                    for p in filtered_stage4_points
                    if not self._stage4_text_is_table_anchor(self._stage4_row_primary_text(p))
                ]
                if not non_table:
                    self.logger.warning(
                        f"No usable Stage 4 rows for subchapter '{persian_subchapter_name}' "
                        f"(all rows look like table anchors)."
                    )
                    _progress(
                        f"Warning: No non-table Stage 4 rows for '{persian_subchapter_name}'. Skipping..."
                    )
                    continue
                self.logger.warning(
                    "No explicit figure-reference rows (تصویر / Fig…) in Stage 4 for subchapter %r; "
                    "using %s non-table Stage 4 row(s) as context (schema without image prefixes).",
                    persian_subchapter_name,
                    len(non_table),
                )
                _progress(
                    f"Note: No dedicated image-reference rows in Points; sending {len(non_table)} "
                    f"Stage 4 row(s) for this subchapter (table-style rows excluded). "
                    "Model output is still filtered to drop any جدول rows."
                )
                image_stage4_points = non_table
            elif len(image_stage4_points) < len(filtered_stage4_points):
                _progress(
                    f"Using {len(image_stage4_points)} image-anchor Stage 4 row(s) for this call "
                    f"(excluded {len(filtered_stage4_points) - len(image_stage4_points)} other rows including جدول)."
                )

            prompt_with_subchapter = prompt.replace("{SUBCHAPTER_NAME}", persian_subchapter_name)
            _progress(f"Using Persian subchapter name in prompt: '{persian_subchapter_name}'")

            topic_groups = self._stage4_points_grouped_by_topic_in_order(image_stage4_points)
            if not topic_groups:
                self.logger.warning("No topic buckets for subchapter %r", persian_subchapter_name)
                _progress(f"Warning: No topic buckets for '{persian_subchapter_name}'. Skipping...")
                continue

            _progress(
                f"Stage E topic-parallel: {len(topic_groups)} topic(s) for subchapter "
                f"'{persian_subchapter_name}' (batch size {self.STAGE_E_TOPIC_BATCH_SIZE})..."
            )
            subchapter_filepic_records, topic_errs = self._process_subchapter_image_topics_parallel(
                topic_groups,
                prompt_with_subchapter=prompt_with_subchapter,
                persian_subchapter_name=persian_subchapter_name,
                ocr_extraction_data=ocr_extraction_data,
                model_name=model_name,
                output_path=output_path,
                part_num=part_num,
                _progress=_progress,
            )

            if not subchapter_filepic_records:
                detail = "; ".join(topic_errs) if topic_errs else "no topic returned usable rows"
                subchapter_errors.append(
                    f"{persian_subchapter_name}: topic-parallel produced no rows ({detail})"
                )
                self.logger.error("No image rows for subchapter %r", persian_subchapter_name)
                _progress(
                    f"Error: No usable image rows for subchapter '{persian_subchapter_name}'."
                )
                continue

            self._repair_image_hierarchy_per_topic(
                subchapter_filepic_records,
                filtered_stage4_points=filtered_stage4_points,
                persian_subchapter_name=persian_subchapter_name,
                ocr_extraction_data=ocr_extraction_data,
                model_name=model_name,
                output_path=output_path,
                part_num=part_num,
                _progress=_progress,
            )

            if topic_errs:
                for fe in topic_errs:
                    self.logger.warning(
                        "Partial topic issue for subchapter %r: %s",
                        persian_subchapter_name,
                        fe,
                    )

            if subchapter_filepic_records:
                all_filepic_records.extend(subchapter_filepic_records)
                _progress(
                    f"Extracted {len(subchapter_filepic_records)} image note records from "
                    f"subchapter '{persian_subchapter_name}'"
                )
            else:
                self.logger.warning(
                    f"No records found in filepic data for subchapter '{persian_subchapter_name}'"
                )
            
            # Add delay between parts to avoid rate limiting (429 errors)
            if part_num < num_parts:  # Don't delay after the last part
                delay_seconds = 5  # 2 seconds delay between parts
                _progress(f"Waiting {delay_seconds} seconds before processing next subchapter...")
                time.sleep(delay_seconds)

        if subchapter_errors:
            summary = "; ".join(subchapter_errors)
            self.logger.error("Stage E aborted due to subchapter failure(s): %s", summary)
            _progress(
                "Error: Stage E incomplete — one or more subchapters failed (job not saved as success). "
                f"Failures: {summary}"
            )
            return None
        
        if not all_filepic_records:
            self.logger.error("No filepic records found from any subchapter")
            _progress("Error: No records found in model responses.")
            return None
        
        _progress(f"Total extracted {len(all_filepic_records)} image note records from all subchapters")
        
        # Process filepic records:
        # 1. Remove caption column
        # 2. Replace point_text with points from Stage 4
        # 3. Assign PointId sequentially
        
        _progress("Processing image notes...")
        processed_image_notes = []
        first_image_point_id = None
        
        for idx, record in enumerate(all_filepic_records):
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
            # Get point_text value from the record (this is the image reference like "تصویر e30:18")
            point_text_value = record.get("point_text", "")
            
            # Set Points column with the point_text value
            processed_record["Points"] = point_text_value
            
            # Assign PointId
            point_id = f"{book_id:03d}{chapter_id:03d}{current_index:04d}"
            processed_record["PointId"] = point_id
            
            if first_image_point_id is None:
                first_image_point_id = point_id
            
            processed_image_notes.append(processed_record)
            current_index += 1
        
        _progress(f"Processed {len(processed_image_notes)} image notes")
        _progress(f"First image PointId: {first_image_point_id}")
        
        # Save filepic JSON (with caption) for use in Stage F
        # IMPORTANT: Save the ORIGINAL all_filepic_records (with caption) before processing
        _progress("Saving filepic JSON for Stage F...")
        filepic_json_path = os.path.join(base_dir, f"{base_name}_filepic.json")
        filepic_saved = False
        
        # Verify that all_filepic_records contain caption before saving
        caption_count = 0
        for record in all_filepic_records:
            if isinstance(record, dict) and record.get("caption"):
                caption_count += 1
        
        _progress(f"Found {caption_count} records with caption out of {len(all_filepic_records)}")
        
        try:
            filepic_metadata = {
                "book_id": book_id,
                "chapter_id": chapter_id,
                "total_records": len(all_filepic_records),
                "records_with_caption": caption_count,
                "division_method": "ocr_extraction_by_subchapter",
                "stage_e_call_mode": "topic_parallel",
                "ocr_subchapters": ocr_subchapters
            }
            # Save original all_filepic_records with caption (before processing)
            success = self.save_json_file(all_filepic_records, filepic_json_path, filepic_metadata, "filepic")
            if success:
                _progress(f"Filepic saved to: {filepic_json_path}")
                filepic_saved = True
                # Verify that caption exists in saved file
                try:
                    with open(filepic_json_path, 'r', encoding='utf-8') as f:
                        saved_data = json.load(f)
                        saved_records = self.get_data_from_json(saved_data)
                        if saved_records:
                            first_record = saved_records[0]
                            if "caption" in first_record:
                                caption_value = first_record.get("caption", "")
                                _progress(f"Verified: filepic contains caption field (first caption: {len(caption_value)} chars)")
                            else:
                                self.logger.warning(f"filepic saved but caption not found. Keys: {list(first_record.keys())}")
                except Exception as e:
                    self.logger.warning(f"Could not verify filepic JSON: {e}")
            else:
                self.logger.error("Failed to save filepic JSON")
        except Exception as e:
            self.logger.error(f"Error saving filepic JSON: {e}", exc_info=True)
            # Continue anyway, Stage F can try to load from JSON
        
        # Merge Stage 4 points with image notes
        _progress("Merging Stage 4 points with image notes...")
        merged_points = stage4_points + processed_image_notes
        
        # Final update: Add processed data and remove raw_responses
        try:
            with open(output_path, "r", encoding="utf-8") as f:
                current_data = json.load(f)
            
            current_data["data"] = merged_points
            current_data["metadata"]["processing_status"] = "completed"
            current_data["metadata"]["stage4_total_points"] = len(stage4_points)
            current_data["metadata"]["image_notes_count"] = len(processed_image_notes)
            current_data["metadata"]["first_image_point_id"] = first_image_point_id
            current_data["metadata"]["last_point_id"] = f"{book_id:03d}{chapter_id:03d}{(current_index - 1):04d}"
            current_data["metadata"]["filepic_json_file"] = os.path.basename(filepic_json_path) if filepic_saved else None
            current_data["metadata"]["filepic_json_path"] = filepic_json_path if filepic_saved else None
            current_data["metadata"]["processed_at"] = datetime.now().isoformat()
            
            # Get additional metadata from Stage 4
            stage4_metadata = self.get_metadata_from_json(stage4_data)
            for k, v in stage4_metadata.items():
                if k not in ["stage", "processed_at", "total_records"]:
                    current_data["metadata"][k] = v
            current_data["metadata"]["stage_e_call_mode"] = "topic_parallel"

            # Remove raw_responses from final output
            if "raw_responses" in current_data:
                del current_data["raw_responses"]
                self.logger.info("Removed raw_responses from final output file")
            
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(current_data, f, ensure_ascii=False, indent=2)
            
            _progress(f"Stage E completed successfully: {output_path}")
            return output_path
        except Exception as e:
            self.logger.error(f"Failed to finalize Stage E output: {e}", exc_info=True)
            return None
    
    def _convert_chapters_to_rows(self, chapters: List[Dict]) -> List[Dict]:
        """
        Convert chapters structure to rows structure.
        
        Args:
            chapters: List of chapter objects with subchapters
            
        Returns:
            List of row objects
        """
        rows = []
        for chapter_obj in chapters:
            chapter_name = chapter_obj.get("chapter", "")
            subchapters = chapter_obj.get("subchapters", [])
            
            if isinstance(subchapters, list):
                for subchapter_obj in subchapters:
                    # Create a row from subchapter data
                    row = {
                        "chapter": chapter_name,
                        **subchapter_obj  # Include all subchapter fields
                    }
                    rows.append(row)
            elif isinstance(subchapters, dict):
                # If subchapters is a dict, treat it as a single row
                row = {
                    "chapter": chapter_name,
                    **subchapters
                }
                rows.append(row)
        
        return rows
    
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
    
    def _extract_chapter_subchapter_topic_mapping(self, ocr_extraction_data: Dict) -> List[Dict]:
        """
        Extract chapter/subchapter/topic mapping from OCR Extraction JSON.
        
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
                      "extractions": [...]
                    }
                  ]
                }
              ]
            }
          ]
        }
        
        Returns a flat list of {chapter, subchapter, topic} for each extraction point.
        Note: The topic field in the mapping is not used - topic comes from model output instead.
        """
        mapping = []
        
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
                    
                    # For each extraction, add a mapping entry
                    # If extractions is a list, create one mapping per extraction
                    if isinstance(extractions, list):
                        for extraction in extractions:
                            mapping.append({
                                "chapter": chapter_name,
                                "subchapter": subchapter_name,
                                "topic": topic_name
                            })
                    else:
                        # If extractions is not a list, create one mapping
                        mapping.append({
                            "chapter": chapter_name,
                            "subchapter": subchapter_name,
                            "topic": topic_name
                        })
        
        return mapping
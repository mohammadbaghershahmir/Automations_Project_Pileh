---
name: Stage E topic threads repair
overview: Refactor Stage E to call the LLM topic-by-topic by default (with topic-scoped OCR), run topic calls in ThreadPoolExecutor batches like Stage V Step 2, and add a post-merge repair LLM pass per topic (only topics with bad rows)—never one whole-subchapter repair—to avoid overlap/duplicates; keep raw JSON writes safe under concurrency.
todos:
  - id: extract-topic-worker
    content: Extract `_run_stage_e_single_topic` from `_stage_e_topic_fallback` logic; build topic-scoped OCR JSON per topic
    status: completed
  - id: parallel-batches
    content: Replace full-subchapter default call with ThreadPoolExecutor batches (size const ~10), preserve topic order via topic_idx dict
    status: completed
  - id: locks-io-http
    content: Add threading.Lock for raw_responses append; add lock (or per-request client) around process_text for Stage E parallel calls
    status: completed
  - id: repair-pass
    content: After per-subchapter merge, group rows missing subtopic/subsubtopic by topic; run one compact repair LLM per affected topic only (topic Stage4 + incomplete rows); merge patches; 1–2 parse retries per repair call—no whole-subchapter repair
    status: completed
  - id: cleanup-metadata
    content: Remove dead full-subchapter path / duplicate fallback; update metadata + progress strings
    status: completed
isProject: false
---

# Stage E: default topic-by-topic + threading + hierarchy repair

## Scope

- **[stage_e_processor.py](content_automation_project/stage_e_processor.py) only** (per your choice). Stage TA unchanged in this plan.

## Current behavior (baseline)

- Subchapter loop builds one **full-subchapter** prompt (`_build_image_notes_stage_e_prompt` + full `image_stage4_points`) and calls `_call_image_notes_llm_with_retries` ([~645–662](content_automation_project/stage_e_processor.py)).
- On OpenRouter **context limit**, `_stage_e_topic_fallback` runs **sequential** per-topic calls with **topic-scoped OCR** via `_filter_ocr_extraction_for_subchapter_topic` + `_slim_ocr_for_stage_e_image_notes` ([base](content_automation_project/base_stage_processor.py) helper added earlier).
- Final row fields like `subtopic`/`subsubtopic` are copied **from the model output** only ([~800–804](content_automation_project/stage_e_processor.py)); empty strings from the LLM persist.

## Target behavior

1. **Default = topic-by-topic** for each OCR subchapter: no “full subchapter first” call in the happy path.
2. **Parallelism** modeled after Test Bank Step 2: `ThreadPoolExecutor` + batched `submit` + `as_completed`, with a configurable `max_workers` (default align with Stage V’s `STEP2_BATCH_SIZE = 10` in [stage_v_processor.py](content_automation_project/stage_v_processor.py) lines ~39, 365–406).
3. **Post-parse validation + repair (per topic, not per subchapter)**: after all topic results for a subchapter are merged, scan rows for empty `subtopic` or `subsubtopic`. **Group incomplete rows by `topic`**. For each topic bucket that has at least one bad row, run **one** compact repair `process_text` for **that topic only** (same scope as the main topic call: topic-scoped Stage 4 + topic-scoped OCR slice + only the incomplete payload rows). Merge returned `subtopic`/`subsubtopic` into matching rows (match by `point_text` / normalized figure ref). **Do not** send one repair prompt for the whole subchapter—avoids re-emitting rows for other topics and prevents overlap/duplicate payloads.

## Implementation details

### A) Extract a single-topic worker function

- Factor the body of `_stage_e_topic_fallback`’s per-topic loop into something like `_run_stage_e_single_topic(...)` that returns:
  - `(topic_name, rows, raw_meta, error_string|None)`
- Inputs per topic:
  - `prompt_with_subchapter`, `persian_subchapter_name`
  - `topic_points` (Stage 4 slice)
  - `ocr_extraction_data` (build `topic_ocr_json_str` exactly as fallback does today)
  - `model_name`, `output_path`, `part_num`, attempt label

### B) Replace subchapter primary call with batched executor

Inside `process_stage_e`’s per-subchapter block:

- Build `topic_groups = _stage4_points_grouped_by_topic_in_order(image_stage4_points)`.
- Create batches of size `STAGE_E_TOPIC_BATCH_SIZE` (new class constant, default 10).
- For each batch:
  - `ThreadPoolExecutor(max_workers=batch_size)` + `as_completed` (same structural pattern as [stage_v_processor.py](content_automation_project/stage_v_processor.py) ~358–408).
  - Collect results into `dict[topic_index] -> rows` to preserve **stable topic order** when flattening (Step 2 does `topic_idx` ordering similarly).

### C) Concurrency safety (must-have)

Two shared resources will break if unsynchronized:

1. **`raw_responses` incremental JSON writes** (`_append_stage_e_raw_response_entry`): concurrent `read/modify/write` will corrupt the file.
   - Add a `threading.Lock` owned by `StageEProcessor` and wrap `_append_stage_e_raw_response_entry` (or only the file IO section) so only one thread writes at a time.

2. **HTTP client thread safety**: `requests.Session` is not guaranteed safe for concurrent use from multiple threads.
   - Add a **process-wide lock** around `self.api_client.process_text(...)` for Stage E parallel batches (simplest, matches “correctness first”), or alternatively instantiate a dedicated `OpenRouterAPIClient` per worker task (heavier).
   - Default plan: **single lock around `process_text` in Stage E topic worker** (minimal code surface, avoids subtle session races).

### D) Repair pass (validation + per-topic retry)

After merging per-subchapter topic rows into `subchapter_filepic_records` (pre-`extend` to `all_filepic_records`):

- Define helper e.g. `_image_note_row_needs_hierarchy(row)` true when `subtopic` or `subsubtopic` is missing or whitespace-empty.
- Partition bad rows by **`topic` string** (same key used for topic grouping; use `(بدون مبحث)` bucket if needed for consistency).
- For **each** `topic_key` that has ≥1 bad row (sequential or small parallel batch—default **sequential** for repair to keep rate limits predictable):
  - Build repair prompt: “fill only `subtopic` and `subsubtopic` for these rows; output JSON only; do not add/remove rows; do not change `point_text` or `caption`.”
  - Inputs: **only** `topic_stage4_points` for that `(subchapter, topic)` + **topic-scoped OCR** (same helpers as main topic call) + JSON list of incomplete rows (minimal fields: `point_text`, `chapter`, `subchapter`, `topic`, plus empty fields).
  - Expected output: mapping `point_text` → `{subtopic, subsubtopic}` or a minimal `payload` list keyed by `point_text`.
  - Merge into **existing** rows in `subchapter_filepic_records` by matching `point_text` (normalize spacing / `:` vs `.` if needed); only fill when still empty.
  - 1–2 JSON parse retries per repair call; log per-topic repair counts.
- **Explicit non-goal**: no single “repair whole subchapter” call (user concern: overlap and duplicate figure rows across topics).

### E) Remove / simplify old paths

- Remove the default **full-subchapter** `_call_image_notes_llm_with_retries` block for the happy path.
- Keep `_stage_e_topic_fallback` only if you still want a secondary strategy; otherwise delete it and route everything through the new executor path to avoid duplicated logic.

### F) Metadata / UX

- Update `metadata.division_method` or add `metadata.stage_e_call_mode = "topic_parallel"` for traceability.
- Progress logs: per batch “processing topics X–Y with concurrency N”.

## Risks / tradeoffs (explicit)

- **OpenRouter rate limits**: higher concurrency can increase `429`; keep batch size configurable and consider small inter-batch sleep (optional, off by default).
- **Repair pass cost**: up to **one extra API call per topic that has bad rows** (often zero); never one giant subchapter repair.
- **Ordering**: flatten topic results using stored indices to avoid nondeterministic ordering from `as_completed`.

## Verification

- Run `py_compile` on touched files.
- Manual smoke: a subchapter with multiple topics completes; `raw_responses` remains valid JSON after parallel writes; forced-empty hierarchy triggers repair merge.

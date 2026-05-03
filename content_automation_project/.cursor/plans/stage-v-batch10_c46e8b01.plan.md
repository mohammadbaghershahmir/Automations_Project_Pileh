---
name: stage-v-batch10
overview: Add concurrent batch processing for Stage V Step 2 with fixed batch size 10, preserving deterministic output order and QId assignment.
todos:
  - id: refactor-step2-batching
    content: Refactor Stage V Step 2 loop to concurrent chunked execution with max_workers=10.
    status: completed
  - id: preserve-order-qid
    content: Ensure deterministic output merge order by topic_idx and preserve sequential global QId assignment.
    status: completed
  - id: progress-and-errors
    content: Update progress/log messages for batch context and keep per-topic failure tolerance.
    status: completed
  - id: verify-runtime
    content: Run compile/lint checks and confirm behavior with a dry run on test docs.
    status: completed
isProject: false
---

# Implement Stage V Step 2 Batch Size 10

## Goal
Change Test Bank Generation Step 2 from sequential topic processing to batched concurrent processing with fixed batch size `10`, while keeping output behavior deterministic and compatible with current UI/pipeline.

## Scope
- Update Stage V processor execution flow to run Step 2 requests in concurrent batches of 10 topics.
- Keep Step 1 behavior unchanged (single full-input run).
- Preserve final output ordering and QId consistency.

## Files to Update
- [/Users/mehrad/MyData/Code/Automations_Project_Pileh/content_automation_project/stage_v_processor.py](/Users/mehrad/MyData/Code/Automations_Project_Pileh/content_automation_project/stage_v_processor.py)
- [/Users/mehrad/MyData/Code/Automations_Project_Pileh/content_automation_project/main_gui.py](/Users/mehrad/MyData/Code/Automations_Project_Pileh/content_automation_project/main_gui.py) (optional status text update only)

## Implementation Approach
- Refactor Step 2 loop in `process_stage_v()`:
  - Build Step 2 tasks for all valid topics (each task keeps `topic_idx`, chapter/subchapter/topic, and filtered Stage J JSON).
  - Process tasks in chunks of 10 using `concurrent.futures.ThreadPoolExecutor(max_workers=10)`.
  - Submit one future per topic task in the current chunk.
  - Wait for chunk completion, collect per-topic outputs/errors, then move to next chunk.
- Keep deterministic merge/QId behavior:
  - Store successful results keyed by `topic_idx`.
  - Combine outputs in sorted `topic_idx` order, not completion order.
  - Continue incrementing global QId based on ordered successful topic outputs.
- Error handling:
  - Per-topic failures should be logged and skipped (same current tolerance behavior).
  - If all topics fail, return failure as today.
- Progress reporting:
  - Update callback/log to show `batch current/total` and per-topic status inside each batch.

## Validation
- Run syntax checks for updated files.
- Dry run on existing test inputs to verify:
  - Step 1 still runs once.
  - Step 2 starts in groups of up to 10.
  - Output file is generated successfully.
  - `QId` values remain sequential and stable.

## Notes
- This plan intentionally uses fixed batch size 10 (no UI setting) per your request.
- If OpenRouter rate limits appear, next iteration can add retry/backoff without changing the batch-size contract.
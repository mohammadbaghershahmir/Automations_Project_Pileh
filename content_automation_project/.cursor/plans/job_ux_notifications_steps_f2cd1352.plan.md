---
name: Job UX notifications steps
overview: Improve Test Bank job UX by adding poll-driven step visibility and notifications, hiding the full-pipeline control for `test_bank`, gating Step 2 behind completed Step 1 (with optional server validation), and structuring the job page into clear Step 1 vs Step 2 sections with artifact grouping.
todos:
  - id: hide-full-pipeline
    content: Conditionally hide Run full pipeline for test_bank in job_detail.html
    status: completed
  - id: progress-poll-ui
    content: Add Step 1/Step 2 progress card + poll-driven aggregate/per-pair badges and banners/toasts
    status: completed
  - id: split-artifacts-ux
    content: Split artifact lists under Step 1 vs Step 2 sections (filter by role/path)
    status: completed
  - id: gate-step2
    content: Disable Step 2 UI until all pairs step1 succeeded; add enqueue_step2 server validation for test_bank
    status: completed
  - id: jobs-list-emphasis
    content: "Optional: highlight running/queued rows on jobs_list.html"
    status: completed
isProject: false
---

# Job page and job list UX improvements

## Context

- [`webapp/templates/job_detail.html`](webapp/templates/job_detail.html): Single card with Step 1 / Step 2 buttons, **Run full pipeline**, Word pairing, flat Artifacts table, log-only polling (pair statuses removed earlier).
- [`webapp/main.py`](webapp/main.py) `GET /jobs/{job_id}/poll` already returns `pairs` (with `step1_status` / `step2_status`), `status`, `error_summary`, `artifacts` — enough to drive live step UI and transition detection without a new DB column.
- [`webapp/main.py`](webapp/main.py) `POST .../enqueue-step2` does not yet verify Step 1 completion (UX-only disable is not enough for API safety).

## 1. Success / failure notifications (and Step 1 → Step 2 nudge)

**Job detail (primary):** In the existing poll script, keep a small **client-side snapshot** of aggregate step state (e.g. any pair `step1` running/succeeded/failed, same for `step2`, plus `job.status` when relevant). On each poll, **detect transitions** and show a **dismissible in-page banner** (or stack of toasts) in a fixed area under the title or above the step cards, for example:

- Step 1 overall: `running → terminal (all succeeded)` → message like “Step 1 finished — review Step 1 outputs below, then run Step 2 when ready.”
- Step 1 overall: `any pair failed` → short failure banner with pointer to log/errors.
- Step 2: analogous messages when Step 2 completes or fails.

Avoid relying only on `window.alert` for happy paths; use non-blocking UI (reuse [`base.html`](webapp/templates/base.html) styles or add minimal `.toast` / `.banner` CSS).

**Jobs list (lighter touch):** Rows already show aggregate status. Optionally add **visual emphasis** for rows in `queued` / `running` (e.g. existing `.badge.run` + subtle row highlight via a class on `<tr>`). Full cross-job toast notifications would require either polling a small summary endpoint or fetching poll per job — treat as **phase 2** unless you want it in scope now.

**Optional:** If you want notifications when the tab is in the background, gate **Browser Notifications API** behind a single “Enable notifications” control and permission prompt; fire only on terminal transitions (avoid spam).

## 2. Hide “Run full pipeline” for Test Bank

In [`job_detail.html`](webapp/templates/job_detail.html), wrap the full-pipeline block (`#btn-full` and its container) in `{% if job.type != 'test_bank' %} ... {% endif %}`. Test Bank jobs keep Step 1 + Step 2 only.

If other job types gain the same page later, mirror the same rule or use a small allowlist (e.g. show full pipeline only for `type in multi_step_full_pipeline_types`).

## 3. Visible Step 1 / Step 2 running and outcome status

Add a **“Progress”** card (or two subsections) **above** or **inside** the step actions that updates from poll:

- **Step 1 row:** Aggregate label derived from `pairs`: e.g. “Running” if any `step1_status == running`; “Failed” if any `failed` (and not still running); “Succeeded” if all pairs have `step1_status == succeeded`; “Pending” / “Not started” otherwise. Optionally show a **compact per-pair line** (index + badge) so operators see which pair is stuck without opening the log.
- **Step 2 row:** Same pattern using `step2_status`.

Wire the existing poll callback to update DOM elements (`textContent` + badge classes matching [`base.html`](webapp/templates/base.html) `.badge.ok|fail|run`).

## 4. Separate Step 1 and Step 2 UX (review outputs before Step 2)

**Layout**

1. Word pairing (unchanged position or slightly above Step 1 block).
2. **Step 1 section** (card): title, short description, **aggregate Step 1 status** (from §3), pair summary, **Run Step 1**, pair indices field scoped to this section if desired.
3. **Sub-block “Step 1 outputs”:** Filter [`artifacts`](webapp/models.py) in the template by `role` / path heuristics consistent with [`webapp/job_files.py`](webapp/job_files.py) (`step1_combined`, etc.) so users see Step 1 outputs **before** acting on Step 2.
4. **Step 2 section** (card, visually below Step 1): title, **aggregate Step 2 status**, **Run Step 2** button **disabled** when Step 1 is not complete for **all** pairs (every pair has `word_relpath` / pairing valid and `step1_status == succeeded`). Show a short muted hint when disabled (“Finish Step 1 for every pair and review outputs above.”).
5. **Step 2 outputs:** separate artifact list for `final_b_json` / `step2_topic` / final outputs as registered today.

You may keep one combined “Artifacts” card as fallback but **duplicating filtered tables** under each step reduces confusion.

**Backend guard (recommended):** In `enqueue_step2`, for `job.type == 'test_bank'`, `raise HTTPException(400, ...)` if any pair lacks `step1_status == 'succeeded'` (respect `pair_indices` the same way tasks do). Prevents bypass via API.

## 5. Optional API tweak

Pass `job_type` (or full `type`) in the poll JSON response for simpler JS branching without embedding extra globals — optional if template already passes `job.type` only for SSR.

## Files to touch

| File | Changes |
|------|--------|
| [`webapp/templates/job_detail.html`](webapp/templates/job_detail.html) | Restructure sections; conditional full pipeline; Progress UI + poll-driven updates; banners/toasts; gated Step 2 button |
| [`webapp/templates/base.html`](webapp/templates/base.html) | Minor styles for banner/toast if needed |
| [`webapp/main.py`](webapp/main.py) | Validate Step 2 enqueue for test_bank; optional `job.type` in poll JSON |
| [`webapp/templates/jobs_list.html`](webapp/templates/jobs_list.html) | Optional: row class for running/queued |

## Testing (manual)

- Create Test Bank job → confirm **no** full pipeline button.
- Queue Step 1 → Progress shows running; on completion, banner + Step 1 outputs visible; Step 2 enables only when all pairs Step 1 succeeded.
- Try Step 2 before Step 1 complete → button disabled; POST enqueue-step2 returns 400.
- Induce Step 1 failure on one pair → failure banner; Step 2 remains disabled or clearly explained.

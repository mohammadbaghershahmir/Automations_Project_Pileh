"""Job-type capabilities for unit repair."""

from __future__ import annotations

from typing import FrozenSet, Optional

MULTI_UNIT_JOB_TYPES: FrozenSet[str] = frozenset(
    {
        "document_processing",
        "test_bank",
        "test_bank_2",
        "image_notes",
        "table_notes",
        "ocr_extraction",
    }
)

POINTID_RENUMBER_TYPES: FrozenSet[str] = frozenset(
    {
        "document_processing",
        "image_notes",
        "table_notes",
    }
)

QID_RENUMBER_TYPES: FrozenSet[str] = frozenset(
    {
        "test_bank",
        "test_bank_2",
    }
)


def job_supports_unit_repair(job_type: str) -> bool:
    return (job_type or "").strip() in MULTI_UNIT_JOB_TYPES


def renumber_scheme_for_job(job_type: str) -> Optional[str]:
    jt = (job_type or "").strip()
    if jt in POINTID_RENUMBER_TYPES:
        return "pointid"
    if jt in QID_RENUMBER_TYPES:
        return "qid"
    return None


def supports_renumber(job_type: str) -> bool:
    return renumber_scheme_for_job(job_type) is not None

"""Per-LLM-unit regenerate and manual renumber for web jobs.

Avoid importing manifest/job_files here so lightweight imports (e.g. renumber tests)
do not require SQLAlchemy. Use webapp.unit_repair.manifest for manifest helpers.
"""

from webapp.unit_repair.registry import (
    job_supports_unit_repair,
    renumber_scheme_for_job,
    supports_renumber,
)

__all__ = [
    "job_supports_unit_repair",
    "renumber_scheme_for_job",
    "supports_renumber",
]

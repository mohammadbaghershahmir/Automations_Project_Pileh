"""Tests for Test Bank Step 2 unit repair helpers."""

import unittest

from webapp.unit_repair.testbank import TestBankStep2UnitHooks, _safe_topic_artifact_suffix


class TestTestBankUnitHelpers(unittest.TestCase):
    def test_safe_topic_artifact_suffix_keeps_persian(self) -> None:
        suffix = _safe_topic_artifact_suffix("تکامل اپیدرم، اهمیت بالینی")
        self.assertIn("تکامل", suffix)
        self.assertNotEqual(suffix, "topic")

    def test_finalize_stale_units_marks_pending_as_failed(self) -> None:
        hooks = TestBankStep2UnitHooks("job-x", 0, "test_bank_2", 101, 2)
        hooks._manifest = {
            "job_type": "test_bank_2",
            "units": [
                {"unit_index": 1, "status": "succeeded"},
                {"unit_index": 2, "status": "pending"},
            ],
            "renumber": {},
            "output_relpath": None,
        }
        n = hooks.finalize_stale_units("failed")
        self.assertEqual(n, 1)
        units = {int(u["unit_index"]): u["status"] for u in hooks._manifest["units"]}
        self.assertEqual(units[1], "succeeded")
        self.assertEqual(units[2], "failed")


if __name__ == "__main__":
    unittest.main()

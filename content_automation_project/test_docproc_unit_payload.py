"""Tests for document-processing per-unit LLM JSON scope."""

import unittest

from multi_part_post_processor import MultiPartPostProcessor


class TestDocprocUnitPayload(unittest.TestCase):
    def test_unit_json_payload_one_topic_only(self) -> None:
        item_a = {
            "topic": "مقدمه",
            "extractions": [{"type": "text", "content": "intro only"}],
            "paragraph": "مقدمه",
        }
        item_b = {
            "topic": "سن",
            "extractions": [{"type": "text", "content": "age section"}],
            "paragraph": "سن",
        }
        payload = MultiPartPostProcessor._docproc_unit_json_payload(
            "Chapter 1",
            "تاریخچه",
            item_a,
            use_paragraphs_key=False,
        )
        self.assertEqual(payload["chapter"], "Chapter 1")
        self.assertEqual(payload["subchapter"], "تاریخچه")
        self.assertIn("topics", payload)
        self.assertNotIn("paragraphs", payload)
        self.assertEqual(len(payload["topics"]), 1)
        self.assertEqual(payload["topics"][0]["topic"], "مقدمه")
        self.assertNotEqual(payload["topics"][0], item_b)

    def test_unit_json_payload_one_paragraph_only(self) -> None:
        item = {
            "paragraph": "P1",
            "extractions": [{"type": "text", "content": "x"}],
        }
        payload = MultiPartPostProcessor._docproc_unit_json_payload(
            "C",
            "S",
            item,
            use_paragraphs_key=True,
        )
        self.assertIn("paragraphs", payload)
        self.assertNotIn("topics", payload)
        self.assertEqual(len(payload["paragraphs"]), 1)


if __name__ == "__main__":
    unittest.main()

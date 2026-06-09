"""Tests for voice class unit merge helpers."""

import unittest

from stage_voice_processor import StageVoiceProcessor


class VoiceClassUnitHelpersTests(unittest.TestCase):
    def test_remove_and_insert_unit_paragraphs(self) -> None:
        processor = StageVoiceProcessor(None)
        units = [
            {"unit_index": 1, "chapter": "A", "subchapter": "S1", "topic": "T1"},
            {"unit_index": 2, "chapter": "A", "subchapter": "S1", "topic": "T2"},
            {"unit_index": 3, "chapter": "A", "subchapter": "S1", "topic": "T3"},
        ]
        paragraphs = [
            {"paragraph_id": 1, "chapter": "A", "subchapter": "S1", "topic": "T1", "text": "a"},
            {"paragraph_id": 2, "chapter": "A", "subchapter": "S1", "topic": "T2", "text": "b"},
            {"paragraph_id": 3, "chapter": "A", "subchapter": "S1", "topic": "T3", "text": "c"},
        ]
        kept = processor._remove_unit_paragraphs(paragraphs, "A", "S1", "T2")
        self.assertEqual(len(kept), 2)
        self.assertEqual(kept[0]["topic"], "T1")
        self.assertEqual(kept[1]["topic"], "T3")

        insert_at = processor._insert_index_for_unit(kept, "A", "S1", "T2", units, 2)
        self.assertEqual(insert_at, 1)

        replacement = [
            {"text": "new b", "chapter": "A", "subchapter": "S1", "topic": "T2", "char_count": 5}
        ]
        renumbered, _ = processor._renumber_topic_paragraphs(
            replacement,
            chapter_name="A",
            subchapter_name="S1",
            topic_name="T2",
            start_paragraph_id=1,
            chars_per_second=13.0,
        )
        merged = list(kept[:insert_at]) + renumbered + list(kept[insert_at:])
        merged = processor._renumber_all_paragraphs(merged, chars_per_second=13.0)
        self.assertEqual([p["paragraph_id"] for p in merged], [1, 2, 3])
        self.assertEqual(merged[1]["text"], "new b")

    def test_filter_paragraphs_for_unit(self) -> None:
        processor = StageVoiceProcessor(None)
        paragraphs = [
            {"chapter": "A", "subchapter": "S", "topic": "X", "text": "one"},
            {"chapter": "A", "subchapter": "S", "topic": "Y", "text": "two"},
        ]
        matched = processor._filter_paragraphs_for_unit(paragraphs, "A", "S", "Y")
        self.assertEqual(len(matched), 1)
        self.assertEqual(matched[0]["text"], "two")


if __name__ == "__main__":
    unittest.main()

"""Tests for flashcard unit grouping and output merge."""

import unittest

from stage_h_processor import StageHProcessor
from webapp.unit_repair.flashcard import _apply_llm_flashcards_to_rows
from webapp.unit_repair.table_notes import filter_points_for_unit, topic_units_from_points


class TestFlashcardUnits(unittest.TestCase):
    def test_topic_units_from_tagged_rows(self) -> None:
        rows = [
            {"chapter": "C", "subchapter": "S1", "topic": "T1", "PointId": "0010010001"},
            {"chapter": "C", "subchapter": "S2", "topic": "T1", "PointId": "0010010002"},
        ]
        units = topic_units_from_points(rows)
        self.assertEqual(len(units), 2)
        self.assertEqual(units[0]["subchapter"], "S1")
        self.assertEqual(units[1]["subchapter"], "S2")

    def test_topic_unit_row_groups(self) -> None:
        proc = StageHProcessor(object())
        rows = [
            {"chapter": "C", "subchapter": "S", "topic": "A", "PointId": "1"},
            {"chapter": "C", "subchapter": "S", "topic": "B", "PointId": "2"},
        ]
        groups = proc._topic_unit_row_groups(rows)
        self.assertEqual(len(groups), 2)
        self.assertEqual(len(groups[0][1]), 1)
        self.assertEqual(groups[0][1][0]["topic"], "A")

    def test_apply_llm_flashcards_patches_unit_only(self) -> None:
        output = [
            {"PointId": "1", "topic": "A", "Qtext": ""},
            {"PointId": "2", "topic": "B", "Qtext": ""},
        ]
        llm = [{"PointId": "1", "Qtext": "Q?", "Choice1": "a", "Correct": "1"}]
        n = _apply_llm_flashcards_to_rows(output, llm, {"1"})
        self.assertEqual(n, 1)
        self.assertEqual(output[0]["Qtext"], "Q?")
        self.assertEqual(output[1]["Qtext"], "")

    def test_filter_points_for_unit_scopes_subchapter(self) -> None:
        unit = {"chapter": "C", "subchapter": "S2", "topic": "T"}
        rows = [
            {"chapter": "C", "subchapter": "S1", "topic": "T", "PointId": "1"},
            {"chapter": "C", "subchapter": "S2", "topic": "T", "PointId": "2"},
        ]
        scoped = filter_points_for_unit(rows, unit)
        self.assertEqual(len(scoped), 1)
        self.assertEqual(scoped[0]["PointId"], "2")


if __name__ == "__main__":
    unittest.main()

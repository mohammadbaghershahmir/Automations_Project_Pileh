"""Tests for unit repair renumber helpers."""

import unittest

from webapp.unit_repair.renumber import (
    format_pointid,
    renumber_points_in_rows,
    renumber_qids_in_rows,
)


class TestUnitRepairRenumber(unittest.TestCase):
    def test_renumber_pointids_sequential(self) -> None:
        manifest = {
            "units": [
                {"unit_index": 1, "chapter": "C1", "subchapter": "S1", "topic": "T1"},
                {"unit_index": 2, "chapter": "C1", "subchapter": "S1", "topic": "T2"},
            ]
        }
        rows = [
            {"chapter": "C1", "subchapter": "S1", "topic": "T2", "PointId": "9999999999"},
            {"chapter": "C1", "subchapter": "S1", "topic": "T1", "PointId": "1111111111"},
        ]
        n = renumber_points_in_rows(rows, manifest, "1050030001")
        self.assertEqual(n, 2)
        self.assertEqual(rows[0]["PointId"], format_pointid(105, 3, 1))
        self.assertEqual(rows[1]["PointId"], format_pointid(105, 3, 2))

    def test_renumber_qids_and_testid(self) -> None:
        manifest = {
            "units": [
                {
                    "unit_index": 1,
                    "chapter_name": "Ch",
                    "subchapter_name": "Sub",
                    "topic_name": "A",
                },
            ]
        }
        rows = [
            {"Chapter": "Ch", "Subchapter": "Sub", "Topic": "A", "QId": "x", "TestID": 9},
            {"Chapter": "Ch", "Subchapter": "Sub", "Topic": "A", "QId": "y", "TestID": 8},
        ]
        renumber_qids_in_rows(rows, manifest, 105, 3)
        self.assertEqual(rows[0]["QId"], "1050030001")
        self.assertEqual(rows[1]["QId"], "1050030002")
        self.assertEqual(rows[0]["TestID"], 1)
        self.assertEqual(rows[1]["TestID"], 2)


if __name__ == "__main__":
    unittest.main()

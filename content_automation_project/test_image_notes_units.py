"""Tests for image-notes unit grouping and OCR figure detection."""

import json
import tempfile
import unittest

from base_stage_processor import BaseStageProcessor
from stage_e_processor import StageEProcessor
from webapp.unit_repair.pic_sidecar import replace_topic_rows
from webapp.unit_repair.table_notes import (
    filter_points_for_unit,
    topic_unit_key,
    topic_units_from_points,
)


class FakeClient:
    def process_text(self, *args, **kwargs):
        return '{"payload":[{"chapter":"C","subchapter":"S2","topic":"مقدمه","point_text":"تصویر 1:1","caption":"c"}]}'


class TestImageNotesUnits(unittest.TestCase):
    def test_topic_units_are_per_subchapter_not_topic_only(self) -> None:
        points = [
            {"chapter": "C", "subchapter": "S1", "topic": "مقدمه", "points": "a"},
            {"chapter": "C", "subchapter": "S2", "topic": "مقدمه", "points": "b"},
            {"chapter": "C", "subchapter": "S2", "topic": "T2", "points": "c"},
        ]
        units = topic_units_from_points(points)
        self.assertEqual(len(units), 3)
        self.assertEqual(units[0]["subchapter"], "S1")
        self.assertEqual(units[1]["subchapter"], "S2")
        self.assertEqual(units[1]["topic"], "مقدمه")
        self.assertNotEqual(
            topic_unit_key(units[0]["chapter"], units[0]["subchapter"], units[0]["topic"]),
            topic_unit_key(units[1]["chapter"], units[1]["subchapter"], units[1]["topic"]),
        )

    def test_filter_points_for_unit_scopes_subchapter(self) -> None:
        unit = {"chapter": "C", "subchapter": "S2", "topic": "مقدمه"}
        points = [
            {"chapter": "C", "subchapter": "S1", "topic": "مقدمه", "points": "a"},
            {"chapter": "C", "subchapter": "S2", "topic": "مقدمه", "points": "b"},
        ]
        scoped = filter_points_for_unit(points, unit)
        self.assertEqual(len(scoped), 1)
        self.assertEqual(scoped[0]["points"], "b")

    def test_ocr_type_field_capital_t_is_figure(self) -> None:
        proc = BaseStageProcessor(FakeClient())
        ocr = {
            "chapters": [
                {
                    "chapter": "C",
                    "subchapters": [
                        {
                            "subchapter": "S2",
                            "topics": [
                                {
                                    "topic": "محافظت از DNA",
                                    "extractions": [
                                        {"Type": "Figure", "Extraction": "fig body"}
                                    ],
                                }
                            ],
                        }
                    ],
                }
            ]
        }
        self.assertTrue(
            proc._ocr_topic_has_figure_extractions(ocr, "S2", "محافظت از DNA")
        )
        topic_slice = proc._filter_ocr_extraction_for_subchapter_topic(
            ocr, "S2", "محافظت از DNA"
        )
        slim = proc._slim_ocr_for_stage_e_image_notes(topic_slice)
        self.assertTrue(proc._ocr_slim_slice_has_figure_extractions(slim))

    def test_regenerate_uses_manifest_subchapter_for_ocr(self) -> None:
        ocr = {
            "chapters": [
                {
                    "chapter": "آناتومی",
                    "subchapters": [
                        {
                            "subchapter": "S1",
                            "topics": [
                                {"topic": "مقدمه", "extractions": [{"type": "text", "content": "x"}]}
                            ],
                        },
                        {
                            "subchapter": "S2",
                            "topics": [
                                {
                                    "topic": "مقدمه",
                                    "extractions": [{"type": "figure", "content": "fig"}],
                                }
                            ],
                        },
                    ],
                }
            ]
        }
        stage4_pts = [
            {"chapter": "آناتومی", "subchapter": "S1", "topic": "مقدمه", "subtopic": "a", "subsubtopic": "b", "points": "p1"},
            {"chapter": "آناتومی", "subchapter": "S2", "topic": "مقدمه", "subtopic": "c", "subsubtopic": "d", "points": "p2"},
        ]
        unit = {"chapter": "آناتومی", "subchapter": "S2", "topic": "مقدمه"}
        pts = filter_points_for_unit(stage4_pts, unit)
        proc = StageEProcessor(FakeClient())
        with tempfile.NamedTemporaryFile(mode="w+", suffix=".json", delete=False) as tf:
            json.dump({"metadata": {}, "data": [], "raw_responses": []}, tf)
            out_path = tf.name
        _ti, _tn, rows, err = proc._run_stage_e_single_topic(
            0,
            "مقدمه",
            pts,
            prompt_with_subchapter="test",
            persian_subchapter_name=unit["subchapter"],
            ocr_extraction_data=ocr,
            model_name="test",
            output_path=out_path,
            part_num=1,
            _progress=lambda _m: None,
        )
        self.assertIsNone(err)
        self.assertEqual(len(rows), 1)


    def test_subchapter_fallback_when_ocr_topic_differs_from_stage4(self) -> None:
        proc = BaseStageProcessor(FakeClient())
        ocr = {
            "chapters": [
                {
                    "chapter": "C",
                    "subchapters": [
                        {
                            "subchapter": "S1",
                            "topics": [
                                {
                                    "topic": "مقدمه",
                                    "extractions": [{"type": "text", "content": "intro"}],
                                },
                                {
                                    "topic": "ساختار پایه پوست",
                                    "extractions": [
                                        {"type": "figure", "content": "Fig 1.1"},
                                        {"type": "e-figure", "content": "eFig 1.1"},
                                    ],
                                },
                            ],
                        }
                    ],
                }
            ]
        }
        stage4_topics = {"مقدمه"}
        self.assertTrue(
            proc._should_use_subchapter_figure_fallback(
                ocr, "S1", "مقدمه", stage4_topics
            )
        )
        slim, mode = proc._ocr_image_slice_for_stage_e_topic(
            ocr, "S1", "مقدمه", stage4_topics
        )
        self.assertEqual(mode, "subchapter_fallback")
        self.assertTrue(proc._ocr_slim_slice_has_figure_extractions(slim))

    def test_replace_topic_rows_scoped_by_subchapter(self) -> None:
        existing = [
            {"chapter": "C", "subchapter": "S1", "topic": "T", "point_text": "old1"},
            {"chapter": "C", "subchapter": "S2", "topic": "T", "point_text": "keep"},
        ]
        new_rows = [{"chapter": "C", "subchapter": "S1", "topic": "T", "point_text": "new1"}]
        out = replace_topic_rows(
            existing, "T", new_rows, chapter="C", subchapter="S1"
        )
        self.assertEqual(len(out), 2)
        self.assertEqual(out[0]["point_text"], "new1")
        self.assertEqual(out[1]["point_text"], "keep")

    def test_no_fallback_when_figures_belong_to_other_stage4_topic(self) -> None:
        proc = BaseStageProcessor(FakeClient())
        ocr = {
            "chapters": [
                {
                    "chapter": "C",
                    "subchapters": [
                        {
                            "subchapter": "S2",
                            "topics": [
                                {
                                    "topic": "محافظت از DNA",
                                    "extractions": [{"type": "text", "content": "dna"}],
                                },
                                {
                                    "topic": "محافظت ایمونولوژیک",
                                    "extractions": [{"type": "figure", "content": "Fig 1.7"}],
                                },
                            ],
                        }
                    ],
                }
            ]
        }
        stage4_topics = {"محافظت از DNA", "محافظت ایمونولوژیک"}
        self.assertFalse(
            proc._should_use_subchapter_figure_fallback(
                ocr, "S2", "محافظت از DNA", stage4_topics
            )
        )
        self.assertFalse(
            proc._ocr_topic_has_figure_extractions(
                ocr, "S2", "محافظت از DNA", stage4_topics
            )
        )


if __name__ == "__main__":
    unittest.main()

import json
import os
import tempfile
import threading
import time
import unittest

from stage_v_processor import StageVProcessor


class _DummyApiClient:
    def set_stage(self, _stage_name: str):
        return None


class _ThreadingTestStageVProcessor(StageVProcessor):
    def __init__(self):
        super().__init__(_DummyApiClient())
        self._lock = threading.Lock()
        self.current_concurrency = 0
        self.max_concurrency = 0
        self.step2_calls = 0

    def _step1_run_once(
        self,
        stage_j_path: str,
        word_file_path: str,
        full_stage_j_json: str,
        prompt: str,
        model_name: str,
        book_id: int,
        chapter_id: int,
        output_dir=None,
        progress_callback=None,
    ):
        # Fake Step 1 output file so Step 2 can continue.
        out_dir = output_dir or os.path.dirname(stage_j_path)
        os.makedirs(out_dir, exist_ok=True)
        path = os.path.join(out_dir, "fake_step1.json")
        self.save_json_file(
            data=[{"TestID": 1, "Qtext": "seed"}],
            file_path=path,
            metadata={"book_id": book_id, "chapter_id": chapter_id, "source": "test"},
            stage_name="V-Step1-Test",
        )
        return path

    def _step2_refine_questions_and_add_qid(
        self,
        stage_j_path: str,
        word_file_path: str,
        full_stage_j_json: str,
        current_topic_name: str,
        current_topic_subchapter: str,
        step1_output_path: str,
        prompt: str,
        model_name: str,
        book_id: int,
        chapter_id: int,
        topic_idx: int,
        total_topics: int,
        qid_start_counter: int,
        output_dir=None,
        progress_callback=None,
        assign_qid: bool = True,
    ):
        with self._lock:
            self.current_concurrency += 1
            self.max_concurrency = max(self.max_concurrency, self.current_concurrency)
            self.step2_calls += 1

        try:
            # Keep threads alive briefly to expose real overlap.
            time.sleep(0.08)
            out_dir = output_dir or os.path.dirname(stage_j_path)
            os.makedirs(out_dir, exist_ok=True)
            path = os.path.join(out_dir, f"step2_topic_{topic_idx}.json")
            self.save_json_file(
                data=[{"TestID": 1, "Qtext": f"q_{topic_idx}", "Topic": current_topic_name}],
                file_path=path,
                metadata={"topic_idx": topic_idx},
                stage_name="V-Step2-Test",
            )
            return path, 1
        finally:
            with self._lock:
                self.current_concurrency -= 1


class StageVThreadingTest(unittest.TestCase):
    def test_step2_uses_threaded_batches_without_real_api(self):
        with tempfile.TemporaryDirectory() as tmp:
            stage_j_path = os.path.join(tmp, "a105030.json")
            word_file_path = os.path.join(tmp, "doc.docx")

            # Create minimal word placeholder file (not used by fake Step 1).
            with open(word_file_path, "w", encoding="utf-8") as f:
                f.write("word-placeholder")

            # 20 topics => two batches with batch size 10.
            rows = []
            for i in range(1, 21):
                rows.append(
                    {
                        "PointId": f"105030{i:04d}",
                        "chapter": "Chapter 30",
                        "subchapter": f"Sub {i%3}",
                        "topic": f"Topic {i}",
                        "subtopic": "",
                        "subsubtopic": "",
                        "Points": f"Point {i}",
                        "Imp": "M",
                    }
                )
            with open(stage_j_path, "w", encoding="utf-8") as f:
                json.dump({"metadata": {"stage": "J"}, "data": rows}, f, ensure_ascii=False, indent=2)

            processor = _ThreadingTestStageVProcessor()
            output_path = processor.process_stage_v(
                stage_j_path=stage_j_path,
                word_file_path=word_file_path,
                prompt_1="p1",
                model_name_1="m1",
                prompt_2="p2 {Topic_NAME} {Subchapter_Name}",
                model_name_2="m2",
                output_dir=tmp,
            )

            self.assertIsNotNone(output_path)
            self.assertTrue(os.path.exists(output_path))
            self.assertEqual(processor.step2_calls, 20)
            self.assertGreater(processor.max_concurrency, 1)
            self.assertLessEqual(processor.max_concurrency, 10)

            with open(output_path, "r", encoding="utf-8") as f:
                result = json.load(f)
            data = result.get("data", [])
            self.assertEqual(len(data), 20)

            # QId must remain deterministic and globally sequential.
            expected_qids = [f"105030{i:04d}" for i in range(1, 21)]
            actual_qids = [row.get("QId") for row in data]
            self.assertEqual(actual_qids, expected_qids)


if __name__ == "__main__":
    unittest.main()

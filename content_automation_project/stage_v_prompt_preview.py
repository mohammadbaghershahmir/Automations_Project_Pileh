"""
Stage V prompt preview tool (no API calls).

Builds Step 1 and Step 2 prompts for Test Bank Generation topic-by-topic,
using the same input composition logic as StageVProcessor, but without
sending requests to AI providers.
"""

import argparse
import json
import logging
import os
import re
from typing import Any, Dict, List, Optional, Tuple

from base_stage_processor import BaseStageProcessor
from word_file_processor import WordFileProcessor

# Step 1 default matches `prompts.json` → "Test Bank Generation - Step 1 Prompt"
def _default_step1_from_repo() -> str:
    import json
    from pathlib import Path

    root = Path(__file__).resolve().parent
    data = json.loads((root / "prompts.json").read_text(encoding="utf-8"))
    return data["Test Bank Generation - Step 1 Prompt"].strip()


DEFAULT_STEP1_PROMPT = _default_step1_from_repo()

DEFAULT_STEP2_PROMPT = """تو یک دستیار تهیه محتوای آموزشی هستی.

برای تاپیک {Topic_NAME} از زیرفصل {Subchapter_Name}:
1) تست‌های مرتبط را از JSON ورودی نگه دار.
2) در صورت نیاز تست تک‌نکته و جمع‌بندی اضافه کن.
3) پاسخ تشریحی کامل تولید کن.
خروجی را به صورت JSON و با همان ساختار ستون‌ها برگردان.
"""


class StageVPromptPreview(BaseStageProcessor):
    """Generate Stage V prompts without calling any AI API."""

    def __init__(self):
        super().__init__(api_client=None)
        self.word_processor = WordFileProcessor()
        self.logger = logging.getLogger(__name__)

    @staticmethod
    def _normalize_key_part(value: Any) -> str:
        if value is None:
            return ""
        return str(value).strip().casefold()

    def _build_topic_key(self, chapter_name: Any, subchapter_name: Any, topic_name: Any) -> Tuple[str, str, str]:
        return (
            self._normalize_key_part(chapter_name),
            self._normalize_key_part(subchapter_name),
            self._normalize_key_part(topic_name),
        )

    @staticmethod
    def _safe_name(value: str, default: str = "topic") -> str:
        clean = (value or "").strip()
        clean = clean.replace("/", "_").replace("\\", "_").replace(" ", "_")
        clean = re.sub(r"[^0-9A-Za-z_\-.]+", "_", clean)
        clean = re.sub(r"_+", "_", clean).strip("_")
        return clean or default

    def _load_json(self, path: str, label: str) -> Dict[str, Any]:
        data = self.load_json_file(path)
        if not data:
            raise ValueError(f"Failed to load {label}: {path}")
        return data

    def _load_text(self, path: str, label: str) -> str:
        if not path or not os.path.exists(path):
            raise ValueError(f"{label} file not found: {path}")
        with open(path, "r", encoding="utf-8") as f:
            text = f.read().strip()
        if not text:
            raise ValueError(f"{label} file is empty: {path}")
        return text

    def build_prompts(
        self,
        stage_j_path: str,
        word_file_path: str,
        prompt_1: str,
        prompt_2: str,
        output_dir: Optional[str] = None,
    ) -> str:
        stage_j_data = self._load_json(stage_j_path, "Stage J JSON")
        stage_j_records = self.get_data_from_json(stage_j_data)
        if not stage_j_records:
            raise ValueError("Stage J JSON has no records")

        topics_list: List[Tuple[str, str, str]] = []
        seen_topic_keys = set()
        for record in stage_j_records:
            if not isinstance(record, dict):
                continue
            chapter_name = record.get("chapter", "")
            subchapter_name = record.get("subchapter", "")
            topic_name = record.get("topic", "")
            if not topic_name:
                continue
            key = self._build_topic_key(chapter_name, subchapter_name, topic_name)
            if key in seen_topic_keys:
                continue
            seen_topic_keys.add(key)
            topics_list.append((chapter_name, subchapter_name, topic_name))

        if not topics_list:
            raise ValueError("No topics found in Stage J JSON")

        word_content = self.word_processor.read_word_file(word_file_path)
        if not word_content:
            raise ValueError(f"Failed to read Word file: {word_file_path}")
        word_content_formatted = self.word_processor.prepare_word_for_model(word_content, context="Test Questions")

        cleaned_records = []
        for record in stage_j_records:
            cleaned_records.append(
                {
                    "PointId": record.get("PointId", ""),
                    "chapter": record.get("chapter", ""),
                    "subchapter": record.get("subchapter", ""),
                    "topic": record.get("topic", ""),
                    "subtopic": record.get("subtopic", ""),
                    "subsubtopic": record.get("subsubtopic", ""),
                    "Points": record.get("Points", record.get("points", "")),
                    "Imp": record.get("Imp", ""),
                }
            )

        final_output_dir = output_dir or os.path.join(os.path.dirname(stage_j_path) or os.getcwd(), "stage_v_prompt_preview")
        os.makedirs(final_output_dir, exist_ok=True)

        manifest: Dict[str, Any] = {
            "source_stage_j": os.path.abspath(stage_j_path),
            "source_word_file": os.path.abspath(word_file_path),
            "total_topics": len(topics_list),
            "step1_mode": "single_full_input",
            "topics": [],
        }

        full_stage_j_json = json.dumps(cleaned_records, ensure_ascii=False, indent=2)
        full_prompt_1 = f"""{prompt_1}

Word Document (Test Questions):
{word_content_formatted}

Stage J Data (FULL file - all records, without Type column):
{full_stage_j_json}"""

        step1_path = os.path.join(final_output_dir, "step1_full_input.txt")
        with open(step1_path, "w", encoding="utf-8") as f:
            f.write(full_prompt_1)
        manifest["step1_prompt_file"] = os.path.abspath(step1_path)

        for topic_idx, (chapter_name, subchapter_name, topic_name) in enumerate(topics_list, 1):
            topic_key = self._build_topic_key(chapter_name, subchapter_name, topic_name)
            filtered_stage_j = [
                rec
                for rec in cleaned_records
                if self._build_topic_key(rec.get("chapter", ""), rec.get("subchapter", ""), rec.get("topic", "")) == topic_key
            ]
            if not filtered_stage_j:
                continue

            stage_j_json = json.dumps(filtered_stage_j, ensure_ascii=False, indent=2)

            topic_prompt_2 = prompt_2.replace("{Topic_NAME}", topic_name)
            topic_prompt_2 = topic_prompt_2.replace("{Subchapter_Name}", subchapter_name)
            full_prompt_2 = f"""{topic_prompt_2}

Word Document (Test Questions):
{word_content_formatted}

Stage J Data (FULL file - all records, without Type column):
{stage_j_json}

IMPORTANT: Focus on refining test questions for the topic: "{topic_name}" (Subchapter: "{subchapter_name}")."""

            safe_topic = self._safe_name(topic_name, default=f"topic_{topic_idx}")
            step2_path = os.path.join(final_output_dir, f"step2_topic_{topic_idx:03d}_{safe_topic}.txt")

            with open(step2_path, "w", encoding="utf-8") as f:
                f.write(full_prompt_2)

            manifest["topics"].append(
                {
                    "topic_index": topic_idx,
                    "chapter": chapter_name,
                    "subchapter": subchapter_name,
                    "topic": topic_name,
                    "stage_j_rows_in_prompt": len(filtered_stage_j),
                    "step2_prompt_file": os.path.abspath(step2_path),
                }
            )

        if not manifest["topics"]:
            raise ValueError("No prompts generated (no matching Stage J rows found for topics)")

        manifest_path = os.path.join(final_output_dir, "prompt_manifest.json")
        with open(manifest_path, "w", encoding="utf-8") as f:
            json.dump(manifest, f, ensure_ascii=False, indent=2)

        return manifest_path


def _configure_logging(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s - %(levelname)s - %(message)s")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Preview Stage V prompts without AI calls.")
    parser.add_argument("--stage-j", required=True, help="Path to Stage J JSON file")
    parser.add_argument("--word-file", required=True, help="Path to Word file used in Stage V")
    parser.add_argument("--prompt1-file", default=None, help="Optional: override Step 1 prompt from text file")
    parser.add_argument("--prompt2-file", default=None, help="Optional: override Step 2 prompt from text file")
    parser.add_argument("--output-dir", default=None, help="Output directory for generated prompt files")
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    _configure_logging(verbose=args.verbose)

    preview = StageVPromptPreview()
    try:
        prompt_1 = DEFAULT_STEP1_PROMPT
        prompt_2 = DEFAULT_STEP2_PROMPT
        if args.prompt1_file:
            prompt_1 = preview._load_text(args.prompt1_file, "Step 1 prompt")
        if args.prompt2_file:
            prompt_2 = preview._load_text(args.prompt2_file, "Step 2 prompt")
        manifest_path = preview.build_prompts(
            stage_j_path=args.stage_j,
            word_file_path=args.word_file,
            prompt_1=prompt_1,
            prompt_2=prompt_2,
            output_dir=args.output_dir,
        )
    except Exception as exc:
        logging.error(str(exc))
        return 1

    logging.info("Prompt preview completed successfully.")
    logging.info("Manifest file: %s", manifest_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

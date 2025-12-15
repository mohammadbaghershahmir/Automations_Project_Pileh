"""
Secondary multi-part post-processor for taking an existing final_output.json
and running a second-stage prompt per Part, then combining all results.

NOTE:
- This module does NOT call the PDF endpoint. It only calls `process_text`
  on the existing JSON content per Part.
"""

import json
import logging
import os
from datetime import datetime
from typing import Dict, Any, List, Optional


class MultiPartPostProcessor:
    """Post-process an existing final_output.json file part-by-part."""

    def __init__(self, api_client):
        """
        Args:
            api_client: GeminiAPIClient instance (must expose process_text)
        """
        self.api_client = api_client
        self.logger = logging.getLogger(__name__)

    def _clean_json_text(self, text: str) -> str:
        """Remove markdown fences and return raw JSON text candidate."""
        if not text:
            return ""

        cleaned = text.strip()
        if cleaned.startswith("```"):
            lines = cleaned.split("\n")
            if len(lines) > 1:
                # drop first line (``` or ```json)
                cleaned = "\n".join(lines[1:])
                # drop trailing ``` if present
                if cleaned.endswith("```"):
                    cleaned = cleaned[:-3].strip()
        return cleaned

    def _parse_json_response(self, response_text: str) -> Optional[List[Dict[str, Any]]]:
        """
        Parse the model response which is expected to be JSON with six columns per row.

        Returns a list of dict rows, or None on failure.
        """
        if not response_text:
            return None

        cleaned = self._clean_json_text(response_text)
        try:
            data = json.loads(cleaned)
        except json.JSONDecodeError:
            self.logger.error("Failed to parse JSON response in post-processor")
            self.logger.debug(f"Response text (first 500 chars): {cleaned[:500]}")
            return None

        # Normalize to list of dicts
        if isinstance(data, dict):
            return [data]
        if isinstance(data, list):
            rows: List[Dict[str, Any]] = []
            for item in data:
                if isinstance(item, dict):
                    rows.append(item)
                else:
                    rows.append({"value": item})
            return rows

        # Fallback: wrap scalar
        return [{"value": data}]

    def process_final_json_by_parts(
        self,
        json_path: str,
        user_prompt: str,
        model_name: str,
    ) -> Optional[str]:
        """
        Take final_output.json, split rows by Part, send each Part with the given prompt
        to the language model (via process_text), collect all JSON responses, and save
        a combined final JSON file.

        Args:
            json_path: Path to existing final_output.json
            user_prompt: Prompt/instruction for second-stage processing
            model_name: Gemini model name to use

        Returns:
            Path to combined final JSON file, or None on error.
        """
        if not os.path.exists(json_path):
            self.logger.error(f"JSON file not found: {json_path}")
            return None

        try:
            with open(json_path, "r", encoding="utf-8") as f:
                data = json.load(f)
        except Exception as e:
            self.logger.error(f"Failed to load JSON file {json_path}: {e}")
            return None

        rows = data.get("rows", [])
        if not isinstance(rows, list) or not rows:
            self.logger.error("Input JSON has no rows to process")
            return None

        # Group rows by Part
        parts: Dict[int, List[Dict[str, Any]]] = {}
        for row in rows:
            if not isinstance(row, dict):
                continue
            part_value = row.get("Part", 0)
            try:
                part_num = int(part_value) if part_value not in (None, "") else 0
            except (ValueError, TypeError):
                part_num = 0

            parts.setdefault(part_num, []).append(row)

        if not parts:
            self.logger.error("No valid Part information found in rows")
            return None

        sorted_parts = sorted(parts.keys())
        all_output_rows: List[Dict[str, Any]] = []

        for part_num in sorted_parts:
            part_rows = parts[part_num]
            if not part_rows:
                continue

            # Build text payload: just the JSON for this part
            part_json_text = json.dumps(part_rows, ensure_ascii=False, indent=2)

            # System prompt includes user's instructions plus part context
            system_prompt = (
                f"{user_prompt}\n\n"
                f"داده‌های زیر مربوط به Part {part_num} هستند. "
                f"خروجی را فقط به صورت JSON آرایه‌ای از ردیف‌ها برگردان، "
                f"شامل شش ستون مورد نظر."
            )

            self.logger.info(f"Processing Part {part_num} with second-stage prompt...")
            response_text = self.api_client.process_text(
                text=part_json_text,
                system_prompt=system_prompt,
                model_name=model_name,
            )

            if not response_text:
                self.logger.error(f"No response for Part {part_num}, aborting post-process")
                return None

            parsed_rows = self._parse_json_response(response_text)
            if not parsed_rows:
                self.logger.error(f"Failed to parse JSON for Part {part_num}, aborting")
                return None

            # Attach Part info back if not present
            for r in parsed_rows:
                if "Part" not in r:
                    r["Part"] = part_num
                all_output_rows.append(r)

        if not all_output_rows:
            self.logger.error("No rows produced by second-stage processing")
            return None

        # Build final combined JSON
        timestamp = datetime.now().isoformat()
        input_name = os.path.basename(json_path)
        final_output: Dict[str, Any] = {
            "metadata": {
                "source_json": input_name,
                "total_parts": len(sorted_parts),
                "total_rows": len(all_output_rows),
                "processed_at": timestamp,
                "model": model_name,
            },
            "rows": all_output_rows,
        }

        base_dir = os.path.dirname(json_path) or os.getcwd()
        base_name, _ = os.path.splitext(input_name)
        final_filename = f"{base_name}_post_processed.json"
        final_path = os.path.join(base_dir, final_filename)

        try:
            with open(final_path, "w", encoding="utf-8") as f:
                json.dump(final_output, f, ensure_ascii=False, indent=2)
            self.logger.info(f"Post-processed JSON saved to: {final_path}")
            return final_path
        except Exception as e:
            self.logger.error(f"Failed to save post-processed JSON: {e}")
            return None



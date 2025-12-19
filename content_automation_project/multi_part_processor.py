"""
Multi-part processor for handling large PDF outputs by splitting into chunks.
Processes PDF in batches and combines results into a single final JSON file.
"""

import json
import logging
import os
import csv
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional, Callable


class MultiPartProcessor:
    """Process PDF files in multiple parts/chunks and combine results."""

    def __init__(self, api_client, output_dir: Optional[str] = None):
        """
        Args:
            api_client: GeminiAPIClient instance (must expose process_pdf_with_prompt_batch)
            output_dir: Directory to save output files (defaults to current directory)
        """
        self.api_client = api_client
        self.logger = logging.getLogger(__name__)
        self.output_dir = Path(output_dir) if output_dir else Path.cwd()

    def _sort_key(self, row: Dict[str, Any]) -> float:
        """Sort key function: convert Number to float for proper sorting."""
        try:
            number_val = row.get("Number", 0)
            if isinstance(number_val, (int, float)):
                return float(number_val)
            if isinstance(number_val, str):
                return float(number_val.replace(",", ""))
            return 0.0
        except (ValueError, TypeError):
            return 0.0

    def process_multi_part(
        self,
        pdf_path: str,
        base_prompt: str,
        model_name: str,
        temperature: float = 0.7,
        resume: bool = False,
        progress_callback: Optional[Callable[[str], None]] = None,
    ) -> Optional[str]:
        """
        Process PDF file in multiple parts/chunks and combine into final JSON.

        Args:
            pdf_path: Path to PDF file
            base_prompt: User's prompt (will be used as-is)
            model_name: Gemini model name
            temperature: Temperature for generation
            resume: Whether to resume from saved parts (currently disabled)
            progress_callback: Optional callback for progress updates

        Returns:
            Path to final combined JSON file, or None on error
        """
        if not os.path.exists(pdf_path):
            self.logger.error(f"PDF file not found: {pdf_path}")
            return None

        if progress_callback:
            progress_callback("Starting multi-part PDF processing...")

        # Use the batch processing method from API client
        # This handles splitting into parts automatically
        result_path = self.api_client.process_pdf_with_prompt_batch(
            pdf_path=pdf_path,
            prompt=base_prompt,
            model_name=model_name,
            temperature=temperature,
            progress_callback=progress_callback,
        )

        if not result_path or not os.path.exists(result_path):
            self.logger.error("Batch processing failed or returned no output")
            return None

        # Load the batch-processed JSON
        try:
            with open(result_path, "r", encoding="utf-8") as f:
                batch_data = json.load(f)
        except Exception as e:
            self.logger.error(f"Failed to load batch-processed JSON: {e}")
            return None

        # Extract rows from batch output
        rows = batch_data.get("rows", [])
        if not isinstance(rows, list):
            self.logger.error("Batch output does not contain valid rows")
            return None

        # Deduplicate rows based on content
        seen = set()
        unique_rows = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            # Create a hash of row content for deduplication
            row_str = json.dumps(row, sort_keys=True, ensure_ascii=False)
            if row_str not in seen:
                seen.add(row_str)
                unique_rows.append(row)

        # Sort rows by Number (as float)
        unique_rows.sort(key=self._sort_key)

        # Build final output structure
        base_name = Path(pdf_path).stem
        timestamp = datetime.now().isoformat()

        final_output = {
            "metadata": {
                "source_pdf": os.path.basename(pdf_path),
                "total_parts": batch_data.get("metadata", {}).get("total_parts", 1),
                "total_rows": len(unique_rows),
                "processed_at": timestamp,
                "model": model_name,
            },
            "rows": unique_rows,
        }

        # Save final JSON to current directory
        final_json_filename = f"{base_name}_final_output.json"
        final_json_path = self.output_dir / final_json_filename

        try:
            with open(final_json_path, "w", encoding="utf-8") as f:
                json.dump(final_output, f, ensure_ascii=False, indent=2)
            self.logger.info(f"Final JSON saved to: {final_json_path}")
        except Exception as e:
            self.logger.error(f"Failed to save final JSON: {e}")
            return None

        # Generate CSV file sorted by Number (as float)
        csv_filename = f"{base_name}_final_output.csv"
        csv_path = self.output_dir / csv_filename

        try:
            if unique_rows:
                # Get all unique keys from rows as headers
                headers = set()
                for row in unique_rows:
                    headers.update(row.keys())
                headers = sorted(headers)

                with open(csv_path, "w", encoding="utf-8", newline="") as f:
                    writer = csv.DictWriter(f, fieldnames=headers, extrasaction="ignore")
                    writer.writeheader()
                    writer.writerows(unique_rows)
                self.logger.info(f"CSV file saved to: {csv_path}")
        except Exception as e:
            self.logger.warning(f"Failed to save CSV file: {e}")

        if progress_callback:
            progress_callback(f"✓ Processing completed. {len(unique_rows)} rows extracted.")

        return str(final_json_path)

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
from base_stage_processor import BaseStageProcessor


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
        # Create a BaseStageProcessor instance for JSON extraction methods
        self.base_processor = BaseStageProcessor(api_client)

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
        Process PDF file in 2 parts: save each part as TXT, convert to JSON, then merge.

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
            progress_callback("Starting 2-part PDF processing...")

        # Get PDF page count using PDFProcessor
        try:
            from pdf_processor import PDFProcessor
            pdf_proc = PDFProcessor()
            total_pages = pdf_proc.count_pages(pdf_path)
            if total_pages == 0:
                self.logger.error("PDF has no pages or failed to count pages")
                return None
        except Exception as e:
            self.logger.error(f"Failed to get PDF page count: {e}")
            return None

        # Divide PDF into 2 parts
        mid_page = total_pages // 2
        part1_end = mid_page
        part2_start = mid_page + 1

        if progress_callback:
            progress_callback(f"PDF has {total_pages} pages. Part 1: pages 1-{part1_end}, Part 2: pages {part2_start}-{total_pages}")

        # Initialize model
        if not self.api_client.initialize_text_client(model_name):
            self.logger.error("Failed to initialize text client")
            return None

        # Load PDF file
        try:
            import google.generativeai as genai
            pdf_file = genai.upload_file(path=pdf_path)
        except Exception as e:
            self.logger.error(f"Failed to upload PDF file: {e}")
            return None

        base_name = Path(pdf_path).stem
        all_txt_responses = []
        all_json_rows = []

        # Process Part 1
        if progress_callback:
            progress_callback(f"Processing Part 1 (pages 1-{part1_end})...")
        
        part1_prompt = f"{base_prompt}\n\nIMPORTANT: Process ONLY pages 1 to {part1_end} of the PDF. Output JSON format."
        part1_response = self._process_part(
            pdf_file, part1_prompt, model_name, temperature, 1, part1_end, progress_callback
        )

        if part1_response:
            # Save Part 1 TXT FIRST (before JSON extraction)
            txt_filename_1 = f"{base_name}_part1.txt"
            txt_path_1 = self.output_dir / txt_filename_1
            try:
                with open(txt_path_1, 'w', encoding='utf-8') as f:
                    f.write(f"=== PART 1 (Pages 1-{part1_end}) ===\n\n")
                    f.write(part1_response)
                if progress_callback:
                    progress_callback(f"Saved Part 1 TXT: {txt_filename_1}")
                self.logger.info(f"Saved Part 1 raw response to: {txt_path_1}")
            except Exception as e:
                self.logger.warning(f"Failed to save Part 1 TXT: {e}")

            all_txt_responses.append(part1_response)

            # Extract JSON from Part 1 TXT file (like Stage V)
            if progress_callback:
                progress_callback("Extracting JSON from Part 1 TXT...")
            # First try: extract from response text
            part1_json = self.base_processor.extract_json_from_response(part1_response)
            if not part1_json:
                if progress_callback:
                    progress_callback("Trying to extract Part 1 JSON from text using fallback...")
                # Second try: extract from text using converter
                part1_json = self.base_processor.load_txt_as_json_from_text(part1_response)
            if not part1_json:
                if progress_callback:
                    progress_callback("Trying to load Part 1 JSON from TXT file...")
                # Third try: load from saved TXT file
                part1_json = self.base_processor.load_txt_as_json(str(txt_path_1))
            
            if part1_json:
                # Handle both list and dict JSON structures (like Stage V)
                if isinstance(part1_json, list):
                    all_json_rows.extend(part1_json)
                    if progress_callback:
                        progress_callback(f"Part 1: Extracted {len(part1_json)} rows")
                elif isinstance(part1_json, dict):
                    rows = part1_json.get("rows", part1_json.get("data", []))
                    if isinstance(rows, list):
                        all_json_rows.extend(rows)
                        if progress_callback:
                            progress_callback(f"Part 1: Extracted {len(rows)} rows")
                    else:
                        all_json_rows.append(part1_json)
                        if progress_callback:
                            progress_callback("Part 1: Extracted 1 row")
        else:
            self.logger.warning("Part 1 returned no response")

        # Process Part 2
        if progress_callback:
            progress_callback(f"Processing Part 2 (pages {part2_start}-{total_pages})...")
        
        part2_prompt = f"{base_prompt}\n\nIMPORTANT: Process ONLY pages {part2_start} to {total_pages} of the PDF. Output JSON format."
        part2_response = self._process_part(
            pdf_file, part2_prompt, model_name, temperature, part2_start, total_pages, progress_callback
        )

        if part2_response:
            # Save Part 2 TXT FIRST (before JSON extraction)
            txt_filename_2 = f"{base_name}_part2.txt"
            txt_path_2 = self.output_dir / txt_filename_2
            try:
                with open(txt_path_2, 'w', encoding='utf-8') as f:
                    f.write(f"=== PART 2 (Pages {part2_start}-{total_pages}) ===\n\n")
                    f.write(part2_response)
                if progress_callback:
                    progress_callback(f"Saved Part 2 TXT: {txt_filename_2}")
                self.logger.info(f"Saved Part 2 raw response to: {txt_path_2}")
            except Exception as e:
                self.logger.warning(f"Failed to save Part 2 TXT: {e}")

            all_txt_responses.append(part2_response)

            # Extract JSON from Part 2 TXT file (like Stage V)
            if progress_callback:
                progress_callback("Extracting JSON from Part 2 TXT...")
            # First try: extract from response text
            part2_json = self.base_processor.extract_json_from_response(part2_response)
            if not part2_json:
                if progress_callback:
                    progress_callback("Trying to extract Part 2 JSON from text using fallback...")
                # Second try: extract from text using converter
                part2_json = self.base_processor.load_txt_as_json_from_text(part2_response)
            if not part2_json:
                if progress_callback:
                    progress_callback("Trying to load Part 2 JSON from TXT file...")
                # Third try: load from saved TXT file
                part2_json = self.base_processor.load_txt_as_json(str(txt_path_2))
            
            if part2_json:
                # Handle both list and dict JSON structures (like Stage V)
                if isinstance(part2_json, list):
                    all_json_rows.extend(part2_json)
                    if progress_callback:
                        progress_callback(f"Part 2: Extracted {len(part2_json)} rows")
                elif isinstance(part2_json, dict):
                    rows = part2_json.get("rows", part2_json.get("data", []))
                    if isinstance(rows, list):
                        all_json_rows.extend(rows)
                        if progress_callback:
                            progress_callback(f"Part 2: Extracted {len(rows)} rows")
                    else:
                        all_json_rows.append(part2_json)
                        if progress_callback:
                            progress_callback("Part 2: Extracted 1 row")
        else:
            self.logger.warning("Part 2 returned no response")

        # Clean up uploaded file
        try:
            genai.delete_file(pdf_file.name)
        except:
            pass

        if not all_json_rows:
            self.logger.error("No JSON data extracted from any part")
            return None

        # Save combined TXT file
        combined_txt_path = self.output_dir / f"{base_name}_all_parts.txt"
        try:
            with open(combined_txt_path, 'w', encoding='utf-8') as f:
                for idx, response in enumerate(all_txt_responses, 1):
                    f.write(f"=== PART {idx} RESPONSE ===\n")
                    f.write(response)
                    f.write("\n\n")
            if progress_callback:
                progress_callback(f"Saved combined TXT: {os.path.basename(combined_txt_path)}")
        except Exception as e:
            self.logger.warning(f"Failed to save combined TXT: {e}")

        # Deduplicate rows
        seen = set()
        unique_rows = []
        for row in all_json_rows:
            if not isinstance(row, dict):
                continue
            row_str = json.dumps(row, sort_keys=True, ensure_ascii=False)
            if row_str not in seen:
                seen.add(row_str)
                unique_rows.append(row)

        # Sort rows by Number (as float)
        unique_rows.sort(key=self._sort_key)

        # Build final output structure
        timestamp = datetime.now().isoformat()
        final_output = {
            "metadata": {
                "source_pdf": os.path.basename(pdf_path),
                "total_parts": 2,
                "total_rows": len(unique_rows),
                "processed_at": timestamp,
                "model": model_name,
                "txt_file": str(combined_txt_path)
            },
            "rows": unique_rows,
        }

        # Save final JSON
        final_json_filename = f"{base_name}_final_output.json"
        final_json_path = self.output_dir / final_json_filename

        try:
            with open(final_json_path, "w", encoding="utf-8") as f:
                json.dump(final_output, f, ensure_ascii=False, indent=2)
            self.logger.info(f"Final JSON saved to: {final_json_path}")
        except Exception as e:
            self.logger.error(f"Failed to save final JSON: {e}")
            return None

        # Generate CSV file
        csv_filename = f"{base_name}_final_output.csv"
        csv_path = self.output_dir / csv_filename

        try:
            if unique_rows:
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

    def _process_part(
        self, pdf_file, prompt: str, model_name: str, temperature: float,
        start_page: int, end_page: int, progress_callback: Optional[Callable[[str], None]] = None
    ) -> Optional[str]:
        """Process a single part of PDF and return raw response text."""
        try:
            import google.generativeai as genai
            
            # Determine maximum tokens based on model (same logic as api_layer.py)
            # gemini-2.5-pro: up to 32768 tokens
            # gemini-2.5-flash: up to 32768 tokens
            # gemini-2.0-flash: up to 32768 tokens
            # gemini-1.5-pro: up to 8192 tokens
            # gemini-1.5-flash: up to 8192 tokens
            if '2.5' in model_name or '2.0' in model_name:
                # Newer models support up to 32768 tokens
                model_max_tokens = 32768
            elif '1.5' in model_name:
                # Older models support up to 8192 tokens
                model_max_tokens = 8192
            else:
                # Default to maximum for safety
                model_max_tokens = 32768
            
            self.logger.info(f"Model: {model_name}, Max tokens for model: {model_max_tokens}")
            
            generation_config = genai.types.GenerationConfig(
                temperature=temperature,
                max_output_tokens=model_max_tokens,  # Use model-specific maximum instead of fixed 8192
            )

            content_parts = [prompt, pdf_file]
            response = self.api_client.text_client.generate_content(
                content_parts,
                generation_config=generation_config,
                stream=False
            )

            return response.text if hasattr(response, 'text') and response.text else None
        except Exception as e:
            self.logger.error(f"Part processing (pages {start_page}-{end_page}) failed: {e}")
            return None


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
        output_dir: Optional[str] = None,
    ) -> Optional[str]:
        """
        Process entire PDF file at once: send full PDF to model and save output.

        Args:
            pdf_path: Path to PDF file
            base_prompt: User's prompt (will be used as-is)
            model_name: Gemini model name
            temperature: Temperature for generation
            resume: Whether to resume from saved parts (not used in single-part mode)
            progress_callback: Optional callback for progress updates
            output_dir: Optional output directory (overrides instance output_dir if provided)

        Returns:
            Path to final JSON file, or None on error
        """
        if not os.path.exists(pdf_path):
            self.logger.error(f"PDF file not found: {pdf_path}")
            return None

        # Update output_dir if provided
        if output_dir:
            self.output_dir = Path(output_dir)
            self.output_dir.mkdir(parents=True, exist_ok=True)
            self.logger.info(f"Using output directory: {self.output_dir}")

        if progress_callback:
            progress_callback("Starting PDF processing (full document)...")

        # Get PDF page count using PDFProcessor (for info only)
        total_pages = None
        try:
            from pdf_processor import PDFProcessor
            pdf_proc = PDFProcessor()
            total_pages = pdf_proc.count_pages(pdf_path)
            if total_pages == 0:
                self.logger.warning("PDF has no pages or failed to count pages")
            elif progress_callback:
                progress_callback(f"PDF has {total_pages} pages. Processing entire document...")
        except Exception as e:
            self.logger.warning(f"Failed to get PDF page count: {e}")

        # Initialize model
        if not self.api_client.initialize_text_client(model_name):
            self.logger.error("Failed to initialize text client")
            return None

        # Extract text from PDF instead of uploading file
        if progress_callback:
            progress_callback("Extracting text from PDF...")
        
        from pdf_processor import PDFProcessor
        pdf_proc = PDFProcessor()
        extracted_text = pdf_proc.extract_text(pdf_path)
        
        if not extracted_text:
            self.logger.error("Failed to extract text from PDF")
            return None
        
        char_count = len(extracted_text)
        self.logger.info(f"Extracted {char_count} characters from PDF")
        if progress_callback:
            progress_callback(f"Extracted {char_count:,} characters from PDF. Processing with model...")

        base_name = Path(pdf_path).stem

        # Process extracted text
        if progress_callback:
            progress_callback("Processing PDF text with model...")
        
        # Use base prompt with extracted text
        full_response = self._process_part_with_text(
            extracted_text, base_prompt, model_name, temperature, 1, total_pages if total_pages else 999, progress_callback
        )

        if not full_response:
            self.logger.error("No response received from model")
            return None

        # Save full response TXT
        txt_filename = f"{base_name}_full.txt"
        txt_path = self.output_dir / txt_filename
        try:
            with open(txt_path, 'w', encoding='utf-8') as f:
                f.write(full_response)
            if progress_callback:
                progress_callback(f"Saved full response TXT: {txt_filename}")
            self.logger.info(f"Saved full response to: {txt_path}")
        except Exception as e:
            self.logger.warning(f"Failed to save full TXT: {e}")

        # Extract JSON from response
        if progress_callback:
            progress_callback("Extracting JSON from response...")
        
        # First try: extract from response text
        json_data = self.base_processor.extract_json_from_response(full_response)
        if not json_data:
            if progress_callback:
                progress_callback("Trying to extract JSON from text using fallback...")
            # Second try: extract from text using converter
            json_data = self.base_processor.load_txt_as_json_from_text(full_response)
        if not json_data:
            if progress_callback:
                progress_callback("Trying to load JSON from TXT file...")
            # Third try: load from saved TXT file
            json_data = self.base_processor.load_txt_as_json(str(txt_path))

        if not json_data:
            self.logger.error("No JSON data extracted from response")
            return None

        # Build final output structure
        timestamp = datetime.now().isoformat()
        
        # Check if we have the new "chapters" structure
        if isinstance(json_data, dict) and "chapters" in json_data:
            # New structure: {"chapters": [...]}
            chapters = json_data.get("chapters", [])
            final_output = {
                "metadata": {
                    "source_pdf": os.path.basename(pdf_path),
                    "total_pages": total_pages,
                    "total_chapters": len(chapters),
                    "processed_at": timestamp,
                    "model": model_name,
                    "txt_file": str(txt_path)
                },
                "chapters": chapters,
            }
            if progress_callback:
                progress_callback(f"Extracted chapters structure ({len(chapters)} chapters)")
        elif isinstance(json_data, dict) and ("rows" in json_data or "data" in json_data):
            # Old structure: {"rows": [...]} or {"data": [...]}
            rows = json_data.get("rows", json_data.get("data", []))
            if isinstance(rows, list):
                # Deduplicate rows
                seen = set()
                unique_rows = []
                for row in rows:
                    if not isinstance(row, dict):
                        continue
                    row_str = json.dumps(row, sort_keys=True, ensure_ascii=False)
                    if row_str not in seen:
                        seen.add(row_str)
                        unique_rows.append(row)
                
                # Sort rows by Number (as float)
                unique_rows.sort(key=self._sort_key)
                
                final_output = {
                    "metadata": {
                        "source_pdf": os.path.basename(pdf_path),
                        "total_pages": total_pages,
                        "total_rows": len(unique_rows),
                        "processed_at": timestamp,
                        "model": model_name,
                        "txt_file": str(txt_path)
                    },
                    "rows": unique_rows,
                }
                if progress_callback:
                    progress_callback(f"Extracted {len(unique_rows)} rows")
            else:
                # Single object
                final_output = {
                    "metadata": {
                        "source_pdf": os.path.basename(pdf_path),
                        "total_pages": total_pages,
                        "processed_at": timestamp,
                        "model": model_name,
                        "txt_file": str(txt_path)
                    },
                    "data": json_data,
                }
                if progress_callback:
                    progress_callback("Extracted single JSON object")
        elif isinstance(json_data, list):
            # List structure
            final_output = {
                "metadata": {
                    "source_pdf": os.path.basename(pdf_path),
                    "total_pages": total_pages,
                    "total_items": len(json_data),
                    "processed_at": timestamp,
                    "model": model_name,
                    "txt_file": str(txt_path)
                },
                "data": json_data,
            }
            if progress_callback:
                progress_callback(f"Extracted {len(json_data)} items")
        else:
            # Unknown structure, wrap it
            final_output = {
                "metadata": {
                    "source_pdf": os.path.basename(pdf_path),
                    "total_pages": total_pages,
                    "processed_at": timestamp,
                    "model": model_name,
                    "txt_file": str(txt_path)
                },
                "data": json_data,
            }
            if progress_callback:
                progress_callback("Extracted JSON data")

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

        # Generate CSV file (only if we have rows structure)
        csv_filename = f"{base_name}_final_output.csv"
        csv_path = self.output_dir / csv_filename

        try:
            if "rows" in final_output:
                unique_rows = final_output["rows"]
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
            if "chapters" in final_output:
                chapters = final_output["chapters"]
                progress_callback(f"✓ Processing completed. {len(chapters)} chapters extracted.")
            elif "rows" in final_output:
                unique_rows = final_output["rows"]
                progress_callback(f"✓ Processing completed. {len(unique_rows)} rows extracted.")
            elif "data" in final_output:
                if isinstance(final_output["data"], list):
                    progress_callback(f"✓ Processing completed. {len(final_output['data'])} items extracted.")
                else:
                    progress_callback("✓ Processing completed. JSON data extracted.")

        return str(final_json_path)

    def _process_part(
        self, pdf_file, prompt: str, model_name: str, temperature: float,
        start_page: int, end_page: int, progress_callback: Optional[Callable[[str], None]] = None
    ) -> Optional[str]:
        """
        Legacy method for processing PDF file (kept for backward compatibility).
        This method is deprecated - use _process_part_with_text instead.
        """
        self.logger.warning("_process_part with PDF file is deprecated. Use _process_part_with_text instead.")
        return None
    
    def _process_part_with_text(
        self, extracted_text: str, prompt: str, model_name: str, temperature: float,
        start_page: int, end_page: int, progress_callback: Optional[Callable[[str], None]] = None
    ) -> Optional[str]:
        """
        Process extracted PDF text and return raw response text.
        
        Args:
            extracted_text: Text extracted from PDF
            prompt: User prompt
            model_name: Model name
            temperature: Temperature for generation
            start_page: Start page (for logging)
            end_page: End page (for logging)
            progress_callback: Optional progress callback
            
        Returns:
            Response text or None if failed
        """
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
                max_output_tokens=model_max_tokens,
            )

            # Combine prompt with extracted text
            full_content = f"{prompt}\n\n--- PDF Content ---\n{extracted_text}"
            
            response = self.api_client.text_client.generate_content(
                full_content,
                generation_config=generation_config,
                stream=False
            )

            return response.text if hasattr(response, 'text') and response.text else None
        except Exception as e:
            self.logger.error(f"Text processing (pages {start_page}-{end_page}) failed: {e}")
            return None




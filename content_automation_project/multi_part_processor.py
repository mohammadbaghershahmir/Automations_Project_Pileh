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

    def process_ocr_extraction_with_topics(
        self,
        pdf_path: str,
        topic_file_path: str,
        base_prompt: str,
        model_name: str,
        temperature: float = 0.7,
        progress_callback: Optional[Callable[[str], None]] = None,
        output_dir: Optional[str] = None,
    ) -> Optional[str]:
        """
        Process OCR Extraction: For each Subchapter, send PDF + prompt (with topics list) to model.
        
        Process:
        1. Load Pre-OCR Topic file
        2. For each Subchapter:
           - Replace {SUBCHAPTER_NAME} in prompt
           - Replace {TOPIC_NAME} with list of Topics for this Subchapter
           - Send PDF + prompt to model
           - Extract JSON response with topics and extractions
        3. Combine all outputs by Subchapter order
        4. Save as "OCR Extraction.json"
        
        Args:
            pdf_path: Path to PDF file
            topic_file_path: Path to Pre-OCR Topic file (t{book}{chapter}.json)
            base_prompt: Prompt template with {SUBCHAPTER_NAME} and {TOPIC_NAME} placeholders
            model_name: Gemini model name
            temperature: Temperature for generation
            progress_callback: Optional callback for progress updates
            output_dir: Optional output directory
            
        Returns:
            Path to final JSON file, or None on error
        """
        if not os.path.exists(pdf_path):
            self.logger.error(f"PDF file not found: {pdf_path}")
            return None
        
        if not os.path.exists(topic_file_path):
            self.logger.error(f"Topic file not found: {topic_file_path}")
            return None
        
        # Update output_dir if provided
        if output_dir:
            self.output_dir = Path(output_dir)
            self.output_dir.mkdir(parents=True, exist_ok=True)
            self.logger.info(f"Using output directory: {self.output_dir}")
        
        if progress_callback:
            progress_callback("Starting OCR Extraction with topics...")
        
        # Load Pre-OCR Topic file
        try:
            with open(topic_file_path, 'r', encoding='utf-8') as f:
                topic_data = json.load(f)
        except Exception as e:
            self.logger.error(f"Failed to load topic file: {e}")
            return None
        
        # Extract data from topic file
        topics_list = topic_data.get("data", [])
        if not topics_list:
            self.logger.error("Topic file has no data")
            return None
        
        metadata = topic_data.get("metadata", {})
        book_id = metadata.get("book_id", 105)
        chapter_id = metadata.get("chapter_id", 3)
        # Try different field names for chapter
        chapter_name = (
            metadata.get("chapter", "") or 
            metadata.get("Chapter", "") or 
            metadata.get("chapter_name", "") or
            metadata.get("Chapter_Name", "") or
            ""
        )
        
        # Extract text from PDF once (will be reused for each subchapter)
        if progress_callback:
            progress_callback("Extracting text from PDF...")
        
        from pdf_processor import PDFProcessor
        pdf_proc = PDFProcessor()
        extracted_text = pdf_proc.extract_text(pdf_path)
        
        if not extracted_text:
            self.logger.error("Failed to extract text from PDF")
            return None
        
        total_pages = pdf_proc.count_pages(pdf_path)
        
        # Initialize model
        if not self.api_client.initialize_text_client(model_name):
            self.logger.error("Failed to initialize text client")
            return None
        
        # Process each Subchapter - فقط TXT را ذخیره می‌کنیم
        total_subchapters = len(topics_list)
        
        for subchapter_idx, subchapter_item in enumerate(topics_list, 1):
            subchapter_name = subchapter_item.get("Subchapter", "")
            topics = subchapter_item.get("Topics", [])
            
            if not subchapter_name:
                self.logger.warning(f"Subchapter item {subchapter_idx} has no Subchapter name, skipping")
                continue
            
            if not topics or not isinstance(topics, list):
                self.logger.warning(f"Subchapter '{subchapter_name}' has no Topics, skipping")
                continue
            
            if progress_callback:
                progress_callback(f"Processing Subchapter {subchapter_idx}/{total_subchapters}: {subchapter_name} ({len(topics)} topics)")
            
            # Replace {SUBCHAPTER_NAME} in prompt
            subchapter_prompt = base_prompt.replace("{SUBCHAPTER_NAME}", subchapter_name)
            
            # Replace {TOPIC_NAME} with list of Topics for this Subchapter
            # Format: "Topic1, Topic2, Topic3, ..."
            topics_str = ", ".join(topics)
            subchapter_prompt = subchapter_prompt.replace("{TOPIC_NAME}", topics_str)
            
            # Process PDF text with replaced prompt
            full_content = f"{subchapter_prompt}\n\n--- PDF Content ---\n{extracted_text}"
            
            try:
                import google.generativeai as genai
                
                # Determine max tokens based on model
                if '2.5' in model_name or '2.0' in model_name:
                    model_max_tokens = 32768
                elif '1.5' in model_name:
                    model_max_tokens = 8192
                else:
                    model_max_tokens = 32768
                
                generation_config = genai.types.GenerationConfig(
                    temperature=temperature,
                    max_output_tokens=model_max_tokens,
                )
                
                if progress_callback:
                    progress_callback(f"Calling model for Subchapter: {subchapter_name}...")
                
                response = self.api_client.text_client.generate_content(
                    full_content,
                    generation_config=generation_config,
                    stream=False
                )
                
                response_text = response.text if hasattr(response, 'text') and response.text else None
                
                if not response_text:
                    self.logger.warning(f"No response for subchapter: {subchapter_name}")
                    continue
                
                # Save raw response to TXT file
                base_name = Path(pdf_path).stem
                safe_subchapter_name = subchapter_name.replace(' ', '_').replace('/', '_')
                txt_filename = f"{base_name}_subchapter_{subchapter_idx}_{safe_subchapter_name}.txt"
                txt_path = self.output_dir / txt_filename
                try:
                    with open(txt_path, 'w', encoding='utf-8') as f:
                        f.write(response_text)
                    self.logger.info(f"Saved response for {subchapter_name} to: {txt_path}")
                except Exception as e:
                    self.logger.warning(f"Failed to save TXT file: {e}")
                    
            except Exception as e:
                self.logger.error(f"Error processing subchapter {subchapter_name}: {e}", exc_info=True)
                continue
        
        # استخراج JSON از همه فایل‌های TXT و ترکیب آن‌ها
        if progress_callback:
            progress_callback("Extracting JSON from all TXT files...")
        
        all_subchapters = []
        for subchapter_idx, subchapter_item in enumerate(topics_list, 1):
            subchapter_name = subchapter_item.get("Subchapter", "")
            base_name = Path(pdf_path).stem
            safe_subchapter_name = subchapter_name.replace(' ', '_').replace('/', '_')
            txt_filename = f"{base_name}_subchapter_{subchapter_idx}_{safe_subchapter_name}.txt"
            txt_path = self.output_dir / txt_filename
            
            if not txt_path.exists():
                self.logger.warning(f"TXT file not found: {txt_path}")
                continue
            
            try:
                # خواندن فایل TXT
                with open(txt_path, 'r', encoding='utf-8') as f:
                    txt_content = f.read()
                
                # استخراج JSON از محتوای TXT - بدون هیچ تبدیل
                subchapter_json = self.base_processor.extract_json_from_response(txt_content)
                if not subchapter_json:
                    subchapter_json = self.base_processor.load_txt_as_json_from_text(txt_content)
                if not subchapter_json:
                    subchapter_json = self._extract_json_from_persian_text(txt_content)
                
                if not subchapter_json:
                    self.logger.warning(f"Failed to extract JSON from TXT file: {txt_path}")
                    continue
                
                # استفاده مستقیم از JSON بدون هیچ تبدیل یا mapping
                # اگر JSON یک subchapter است، مستقیماً اضافه می‌کنیم
                # اگر JSON یک ساختار بزرگتر است (مثلاً chapters یا subchapters)، subchapter را استخراج می‌کنیم
                extracted_subchapter = self._get_subchapter_from_json(subchapter_json, subchapter_name)
                
                if extracted_subchapter:
                    all_subchapters.append(extracted_subchapter)
                    self.logger.info(f"Successfully extracted subchapter '{subchapter_name}' from TXT file")
                else:
                    # اگر subchapter پیدا نشد، کل JSON را به عنوان subchapter اضافه می‌کنیم
                    if isinstance(subchapter_json, dict):
                        all_subchapters.append(subchapter_json)
                        self.logger.info(f"Added JSON as-is for subchapter '{subchapter_name}'")
                    else:
                        self.logger.warning(f"Failed to extract subchapter '{subchapter_name}' from JSON")
                    
            except Exception as e:
                self.logger.error(f"Error processing TXT file {txt_path}: {e}", exc_info=True)
                continue
        
        if not all_subchapters:
            self.logger.error("No subchapters processed successfully")
            return None
        
        # Try to extract chapter name from first TXT file if not found in topic file
        if not chapter_name and topics_list:
            # Only check the first TXT file (subchapter_idx = 1)
            first_subchapter_item = topics_list[0]
            subchapter_name = first_subchapter_item.get("Subchapter", "")
            base_name = Path(pdf_path).stem
            safe_subchapter_name = subchapter_name.replace(' ', '_').replace('/', '_')
            txt_filename = f"{base_name}_subchapter_1_{safe_subchapter_name}.txt"
            txt_path = self.output_dir / txt_filename
            
            if txt_path.exists():
                try:
                    with open(txt_path, 'r', encoding='utf-8') as f:
                        txt_content = f.read()
                    
                    # Extract JSON from TXT
                    subchapter_json = self.base_processor.extract_json_from_response(txt_content)
                    if not subchapter_json:
                        subchapter_json = self.base_processor.load_txt_as_json_from_text(txt_content)
                    if not subchapter_json:
                        subchapter_json = self._extract_json_from_persian_text(txt_content)
                    
                    if subchapter_json:
                        # Try to extract chapter from JSON structure
                        if isinstance(subchapter_json, dict):
                            # Check for chapters structure
                            if "chapters" in subchapter_json:
                                chapters = subchapter_json.get("chapters", [])
                                if chapters and len(chapters) > 0:
                                    extracted_chapter = chapters[0].get("chapter", "")
                                    if extracted_chapter:
                                        chapter_name = extracted_chapter
                                        self.logger.info(f"Extracted chapter name from first TXT file: {chapter_name}")
                            # Check if it's a single chapter structure
                            elif "chapter" in subchapter_json:
                                extracted_chapter = subchapter_json.get("chapter", "")
                                if extracted_chapter:
                                    chapter_name = extracted_chapter
                                    self.logger.info(f"Extracted chapter name from first TXT file: {chapter_name}")
                except Exception as e:
                    self.logger.debug(f"Could not extract chapter from first TXT file {txt_path}: {e}")
        
        # Build final structure - combine all JSON outputs as-is
        timestamp = datetime.now().isoformat()
        
        # Calculate total_topics safely (only if topics field exists)
        total_topics = 0
        for sub in all_subchapters:
            if isinstance(sub, dict) and "topics" in sub:
                if isinstance(sub["topics"], list):
                    total_topics += len(sub["topics"])
        
        final_output = {
            "metadata": {
                "source_pdf": os.path.basename(pdf_path),
                "source_topic_file": os.path.basename(topic_file_path),
                "total_pages": total_pages,
                "total_subchapters": len(all_subchapters),
                "total_topics": total_topics,
                "processed_at": timestamp,
                "model": model_name,
                "book_id": book_id,
                "chapter_id": chapter_id,
                "chapter": chapter_name
            },
            "chapters": [{
                "chapter": chapter_name,
                "subchapters": all_subchapters
            }]
        }
        
        # Save with name "OCR Extraction.json"
        output_filename = "OCR Extraction.json"
        final_json_path = self.output_dir / output_filename
        
        try:
            with open(final_json_path, "w", encoding="utf-8") as f:
                json.dump(final_output, f, ensure_ascii=False, indent=2)
            self.logger.info(f"OCR Extraction JSON saved to: {final_json_path}")
            
            if progress_callback:
                progress_callback(f"✓ OCR Extraction completed. {len(all_subchapters)} subchapters processed.")
            
            return str(final_json_path)
        except Exception as e:
            self.logger.error(f"Failed to save OCR Extraction JSON: {e}")
            return None

    def _extract_json_from_persian_text(self, text: str) -> Optional[Dict | List]:
        """
        Extract JSON from Persian text response.
        Handles Persian keys and values in JSON.
        
        Args:
            text: Text containing JSON (may be in Persian)
            
        Returns:
            Parsed JSON object or None on error
        """
        if not text or not text.strip():
            return None
        
        try:
            # سعی کن JSON را مستقیماً parse کنی
            # ابتدا سعی کن کل متن را parse کنی
            parsed = json.loads(text)
            self.logger.info("Successfully parsed JSON directly from text")
            return parsed
        except json.JSONDecodeError:
            pass
        
        # سعی کن JSON را از markdown code block استخراج کنی
        import re
        
        # جستجوی ```json ... ```
        json_pattern = r'```json\s*(.*?)\s*```'
        match = re.search(json_pattern, text, re.DOTALL)
        if match:
            json_str = match.group(1).strip()
            try:
                parsed = json.loads(json_str)
                self.logger.info("Successfully extracted JSON from markdown code block")
                return parsed
            except json.JSONDecodeError:
                pass
        
        # جستجوی ``` ... ``` (بدون json)
        code_pattern = r'```\s*(.*?)\s*```'
        match = re.search(code_pattern, text, re.DOTALL)
        if match:
            json_str = match.group(1).strip()
            # اگر با { یا [ شروع می‌شود، احتمالاً JSON است
            if json_str.startswith(('{', '[')):
                try:
                    parsed = json.loads(json_str)
                    self.logger.info("Successfully extracted JSON from code block")
                    return parsed
                except json.JSONDecodeError:
                    pass
        
        # سعی کن بزرگترین JSON object یا array را پیدا کنی
        # پیدا کردن اولین {
        start_obj = text.find('{')
        start_arr = text.find('[')
        
        if start_obj != -1 and (start_arr == -1 or start_obj < start_arr):
            # پیدا کردن آخرین }
            end_obj = text.rfind('}')
            if end_obj > start_obj:
                candidate = text[start_obj:end_obj + 1]
                try:
                    parsed = json.loads(candidate)
                    self.logger.info("Successfully extracted JSON object from text")
                    return parsed
                except json.JSONDecodeError:
                    # سعی کن با تعادل براکت‌ها parse کنی
                    try:
                        balanced = self._balance_json_string(candidate)
                        if balanced:
                            parsed = json.loads(balanced)
                            self.logger.info("Successfully parsed JSON after balancing")
                            return parsed
                    except:
                        pass
        
        elif start_arr != -1:
            # پیدا کردن آخرین ]
            end_arr = text.rfind(']')
            if end_arr > start_arr:
                candidate = text[start_arr:end_arr + 1]
                try:
                    parsed = json.loads(candidate)
                    self.logger.info("Successfully extracted JSON array from text")
                    return parsed
                except json.JSONDecodeError:
                    pass
        
        self.logger.warning("Failed to extract JSON from Persian text")
        return None
    
    def _balance_json_string(self, text: str) -> Optional[str]:
        """
        Try to balance JSON brackets/braces.
        
        Args:
            text: Potentially unbalanced JSON string
            
        Returns:
            Balanced JSON string or None
        """
        if not text:
            return None
        
        # شمارش براکت‌ها و بریس‌ها
        open_braces = text.count('{') - text.count('}')
        open_brackets = text.count('[') - text.count(']')
        
        if open_braces == 0 and open_brackets == 0:
            return text
        
        # بستن براکت‌ها و بریس‌ها
        balanced = text
        if open_brackets > 0:
            balanced += ']' * open_brackets
        if open_braces > 0:
            balanced += '}' * open_braces
        
        try:
            # تست کن که آیا valid است
            json.loads(balanced)
            return balanced
        except json.JSONDecodeError:
            return None
    
    def _get_subchapter_from_json(
        self,
        json_data: Dict | List,
        subchapter_name: str
    ) -> Optional[Dict]:
        """
        Get subchapter data directly from JSON without any transformation.
        Returns JSON as-is from model output.
        
        Args:
            json_data: JSON data extracted from TXT file (raw model output)
            subchapter_name: Name of subchapter (for logging only)
            
        Returns:
            Dictionary with subchapter structure (as-is from JSON) or None
        """
        if isinstance(json_data, dict):
            # Check for chapters structure
            if "chapters" in json_data:
                chapters = json_data.get("chapters", [])
                if chapters and len(chapters) > 0:
                    subchapters = chapters[0].get("subchapters", [])
                    if subchapters and len(subchapters) > 0:
                        # Return first subchapter as-is
                        return subchapters[0]
            
            # Check for subchapters structure
            elif "subchapters" in json_data:
                subchapters = json_data.get("subchapters", [])
                if subchapters and len(subchapters) > 0:
                    # Return first subchapter as-is
                    return subchapters[0]
            
            # Check if it's a single subchapter
            elif "subchapter" in json_data:
                return json_data
            
            # If it's a dict but doesn't match above structures, return as-is
            return json_data
        
        # If it's a list, return None (we expect dict structure)
        return None






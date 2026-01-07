"""
Stage Y Processor: Deletion Detection
Detects deleted content by comparing old book PDF extraction with OCR Extraction JSON.

Step 1: Extract PDF from old reference (divided into 2 parts)
Step 2: Compare OCR Extraction JSON with extracted PDF and detect deletions
"""

import json
import logging
import os
from typing import Optional, Dict, List, Any, Callable

from base_stage_processor import BaseStageProcessor
from api_layer import APIConfig


class StageYProcessor(BaseStageProcessor):
    """Process Stage Y: Detect deleted content"""
    
    def __init__(self, api_client):
        super().__init__(api_client)
        self.logger = logging.getLogger(__name__)
    
    def process_stage_y(
        self,
        old_book_pdf_path: str,  # Old reference PDF file
        ocr_extraction_prompt: str,  # Prompt for OCR extraction from PDF
        ocr_extraction_model: str,  # Model for OCR extraction
        ocr_extraction_json_path: str,  # JSON from OCR Extraction stage
        deletion_detection_prompt: str,  # Prompt for deletion detection
        deletion_detection_model: str,  # Model for deletion detection
        output_dir: Optional[str] = None,
        progress_callback: Optional[Callable[[str], None]] = None,
        step1_output_path: Optional[str] = None  # Optional: use pre-extracted Step 1 output
    ) -> Optional[str]:
        """
        Process Stage Y: Detect deleted content.
        
        Step 1: Extract PDF from old reference (divided into 2 parts)
        Step 2: Compare OCR Extraction JSON with extracted PDF and detect deletions
        
        Args:
            old_book_pdf_path: Path to old reference PDF file
            ocr_extraction_prompt: Prompt for OCR extraction from PDF
            ocr_extraction_model: Model for OCR extraction
            ocr_extraction_json_path: Path to OCR Extraction JSON file (from OCR Extraction stage)
            deletion_detection_prompt: Prompt for deletion detection
            deletion_detection_model: Model for deletion detection
            output_dir: Output directory (defaults to ocr_extraction_json_path directory)
            progress_callback: Optional callback for progress updates
            
        Returns:
            Path to output file (y{book}{chapter}+{chapter_name}.json) or None on error
        """
        def _progress(msg: str):
            if progress_callback:
                progress_callback(msg)
            self.logger.info(msg)
        
        # Set stage if using UnifiedAPIClient (for API routing)
        if hasattr(self.api_client, 'set_stage'):
            self.api_client.set_stage("stage_y")
        
        _progress("Starting Stage Y processing...")
        
        # Determine output directory
        if not output_dir:
            output_dir = os.path.dirname(ocr_extraction_json_path) or os.getcwd()
        os.makedirs(output_dir, exist_ok=True)
        
        # Extract book and chapter from OCR Extraction JSON
        ocr_extraction_data = self.load_json_file(ocr_extraction_json_path)
        if not ocr_extraction_data:
            self.logger.error("Failed to load OCR Extraction JSON")
            return None
        
        metadata = self.get_metadata_from_json(ocr_extraction_data)
        book_id = metadata.get("book_id", 105)
        chapter_id = metadata.get("chapter_id", 3)
        
        # ========== STEP 1: Extract PDF from Old Reference (2 parts) ==========
        # Use pre-extracted Step 1 output if provided, otherwise extract
        if step1_output_path and os.path.exists(step1_output_path):
            _progress(f"STEP 1: Using pre-extracted PDF: {os.path.basename(step1_output_path)}")
            step1_output = step1_output_path
        else:
            _progress("=" * 60)
            _progress("STEP 1: Extracting PDF from old reference (2 parts)...")
            _progress("=" * 60)
            
            step1_output = self._step1_extract_pdf(
                pdf_path=old_book_pdf_path,
                prompt=ocr_extraction_prompt,
                model_name=ocr_extraction_model,
                output_dir=output_dir,
                book_id=book_id,
                chapter_id=chapter_id,
                progress_callback=progress_callback
            )
            
            if not step1_output:
                self.logger.error("Step 1 failed")
                return None
            
            _progress(f"Step 1 completed. Output saved to: {step1_output}")
        
        # ========== STEP 2: Detect Deletions ==========
        _progress("=" * 60)
        _progress("STEP 2: Detecting deletions...")
        _progress("=" * 60)
        
        step2_output = self._step2_detect_deletions(
            step1_output_path=step1_output,
            ocr_extraction_json_path=ocr_extraction_json_path,
            prompt=deletion_detection_prompt,
            model_name=deletion_detection_model,
            output_dir=output_dir,
            book_id=book_id,
            chapter_id=chapter_id,
            progress_callback=progress_callback
        )
        
        if not step2_output:
            self.logger.error("Step 2 failed")
            return None
        
        _progress(f"Stage Y completed successfully: {step2_output}")
        return step2_output
    
    def _step1_extract_pdf(
        self,
        pdf_path: str,
        prompt: str,
        model_name: str,
        output_dir: str,
        book_id: int,
        chapter_id: int,
        progress_callback: Optional[Callable[[str], None]] = None
    ) -> Optional[str]:
        """
        Step 1: Extract PDF from old reference, divided into 2 parts.
        
        Args:
            pdf_path: Path to old reference PDF file
            prompt: Prompt for OCR extraction
            model_name: Model name
            output_dir: Output directory
            book_id: Book ID
            chapter_id: Chapter ID
            progress_callback: Optional progress callback
            
        Returns:
            Path to combined JSON file or None on error
        """
        def _progress(msg: str):
            if progress_callback:
                progress_callback(msg)
            self.logger.info(msg)
        
        # Get PDF page count
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
        
        _progress(f"PDF has {total_pages} pages. Part 1: pages 1-{part1_end}, Part 2: pages {part2_start}-{total_pages}")
        
        # Initialize model
        if not self.api_client.initialize_text_client(model_name):
            self.logger.error("Failed to initialize text client")
            return None
        
        # Extract text from PDF parts
        from pdf_processor import PDFProcessor
        pdf_proc = PDFProcessor()
        
        all_json_rows = []
        base_name = os.path.splitext(os.path.basename(pdf_path))[0]
        
        # Process Part 1
        _progress(f"Extracting text from Part 1 (pages 1-{part1_end})...")
        part1_text = pdf_proc.extract_text_range(pdf_path, 1, part1_end)
        if not part1_text:
            self.logger.error(f"Failed to extract text from pages 1-{part1_end}")
            return None
        
        _progress(f"Processing Part 1 with model...")
        part1_prompt = f"{prompt}\n\nIMPORTANT: Process ONLY pages 1 to {part1_end} of the PDF. Output JSON format."
        part1_response = self._process_part_with_text(
            part1_text, part1_prompt, model_name, 0.7, 1, part1_end, progress_callback
        )
        
        if part1_response:
            # Save Part 1 TXT
            txt_filename_1 = f"{base_name}_stage_y_step1_part1.txt"
            txt_path_1 = os.path.join(output_dir, txt_filename_1)
            try:
                with open(txt_path_1, 'w', encoding='utf-8') as f:
                    f.write(f"=== STEP 1 - PART 1 (Pages 1-{part1_end}) ===\n\n")
                    f.write(part1_response)
                _progress(f"Saved Part 1 TXT: {txt_filename_1}")
            except Exception as e:
                self.logger.warning(f"Failed to save Part 1 TXT: {e}")
            
            # Extract JSON from Part 1
            _progress("Extracting JSON from Part 1...")
            part1_json = self.extract_json_from_response(part1_response)
            if not part1_json:
                _progress("Trying to extract JSON from Part 1 using fallback...")
                part1_json = self.load_txt_as_json_from_text(part1_response)
            if not part1_json:
                _progress("Trying to load JSON from TXT file...")
                part1_json = self.load_txt_as_json(txt_path_1)
            
            if part1_json:
                part1_rows = self.get_data_from_json(part1_json) if isinstance(part1_json, dict) else (part1_json if isinstance(part1_json, list) else [])
                all_json_rows.extend(part1_rows)
                _progress(f"Part 1: Extracted {len(part1_rows)} rows")
        else:
            self.logger.warning("Part 1 returned no response")
        
        # Process Part 2
        _progress(f"Extracting text from Part 2 (pages {part2_start}-{total_pages})...")
        part2_text = pdf_proc.extract_text_range(pdf_path, part2_start, total_pages)
        if not part2_text:
            self.logger.error(f"Failed to extract text from pages {part2_start}-{total_pages}")
            return None
        
        _progress(f"Processing Part 2 with model...")
        part2_prompt = f"{prompt}\n\nIMPORTANT: Process ONLY pages {part2_start} to {total_pages} of the PDF. Output JSON format."
        part2_response = self._process_part_with_text(
            part2_text, part2_prompt, model_name, 0.7, part2_start, total_pages, progress_callback
        )
        
        if part2_response:
            # Save Part 2 TXT
            txt_filename_2 = f"{base_name}_stage_y_step1_part2.txt"
            txt_path_2 = os.path.join(output_dir, txt_filename_2)
            try:
                with open(txt_path_2, 'w', encoding='utf-8') as f:
                    f.write(f"=== STEP 1 - PART 2 (Pages {part2_start}-{total_pages}) ===\n\n")
                    f.write(part2_response)
                _progress(f"Saved Part 2 TXT: {txt_filename_2}")
            except Exception as e:
                self.logger.warning(f"Failed to save Part 2 TXT: {e}")
            
            # Extract JSON from Part 2
            _progress("Extracting JSON from Part 2...")
            part2_json = self.extract_json_from_response(part2_response)
            if not part2_json:
                _progress("Trying to extract JSON from Part 2 using fallback...")
                part2_json = self.load_txt_as_json_from_text(part2_response)
            if not part2_json:
                _progress("Trying to load JSON from TXT file...")
                part2_json = self.load_txt_as_json(txt_path_2)
            
            if part2_json:
                part2_rows = self.get_data_from_json(part2_json) if isinstance(part2_json, dict) else (part2_json if isinstance(part2_json, list) else [])
                all_json_rows.extend(part2_rows)
                _progress(f"Part 2: Extracted {len(part2_rows)} rows")
        else:
            self.logger.warning("Part 2 returned no response")
        
        if not all_json_rows:
            self.logger.error("Failed to extract JSON from both parts")
            return None
        
        # Save combined JSON
        output_filename = f"stage_y_step1_{book_id}{chapter_id:03d}.json"
        output_path = os.path.join(output_dir, output_filename)
        
        output_data = {
            "metadata": {
                "book_id": book_id,
                "chapter_id": chapter_id,
                "source": "Stage Y - Step 1 (PDF Extraction from Old Reference)",
                "total_parts": 2,
                "total_rows": len(all_json_rows)
            },
            "rows": all_json_rows
        }
        
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, ensure_ascii=False, indent=2)
            _progress(f"Step 1 combined output saved to: {output_path}")
            return output_path
        except Exception as e:
            self.logger.error(f"Failed to save Step 1 output: {e}")
            return None
    
    def _step2_detect_deletions(
        self,
        step1_output_path: str,
        ocr_extraction_json_path: str,
        prompt: str,
        model_name: str,
        output_dir: str,
        book_id: int,
        chapter_id: int,
        progress_callback: Optional[Callable[[str], None]] = None
    ) -> Optional[str]:
        """
        Step 2: Detect deletions by comparing OCR Extraction JSON with Step 1 output.
        
        Args:
            step1_output_path: Path to Step 1 output JSON
            ocr_extraction_json_path: Path to OCR Extraction JSON
            prompt: Prompt for deletion detection
            model_name: Model name
            output_dir: Output directory
            book_id: Book ID
            chapter_id: Chapter ID
            progress_callback: Optional progress callback
            
        Returns:
            Path to output JSON file or None on error
        """
        def _progress(msg: str):
            if progress_callback:
                progress_callback(msg)
            self.logger.info(msg)
        
        # Load Step 1 output (PDF extracted from old reference)
        step1_data = self.load_json_file(step1_output_path)
        if not step1_data:
            self.logger.error("Failed to load Step 1 output")
            return None
        
        pdf_extracted_rows = step1_data.get("rows", [])
        _progress(f"Loaded {len(pdf_extracted_rows)} rows from Step 1 output")
        
        # Load OCR Extraction JSON
        ocr_extraction_data = self.load_json_file(ocr_extraction_json_path)
        if not ocr_extraction_data:
            self.logger.error("Failed to load OCR Extraction JSON")
            return None
        
        ocr_extraction_records = self.get_data_from_json(ocr_extraction_data)
        if not ocr_extraction_records:
            self.logger.error("OCR Extraction JSON has no data")
            return None
        
        _progress(f"Loaded {len(ocr_extraction_records)} records from OCR Extraction JSON")
        
        # Prepare data for model
        comparison_data = {
            "current_data": ocr_extraction_records,
            "old_book_data": pdf_extracted_rows
        }
        comparison_text = json.dumps(comparison_data, ensure_ascii=False, indent=2)
        
        # Call model
        _progress("Calling model for deletion detection...")
        response = self.api_client.process_text(
            text=comparison_text,
            system_prompt=prompt,
            model_name=model_name
        )
        
        if not response:
            self.logger.error("Model returned no response")
            return None
        
        # Save raw response
        base_name = os.path.splitext(os.path.basename(ocr_extraction_json_path))[0]
        txt_path = os.path.join(output_dir, f"{base_name}_stage_y_step2.txt")
        try:
            with open(txt_path, 'w', encoding='utf-8') as f:
                f.write("=== STAGE Y - STEP 2 (Deletion Detection) RESPONSE ===\n\n")
                f.write(response)
            _progress(f"Saved Step 2 raw response to: {os.path.basename(txt_path)}")
        except Exception as e:
            self.logger.warning(f"Failed to save Step 2 TXT: {e}")
        
        # Extract JSON from response
        _progress("Extracting JSON from Step 2 response...")
        deletions_json = self.extract_json_from_response(response)
        if not deletions_json:
            _progress("Trying to extract JSON from text using fallback...")
            deletions_json = self.load_txt_as_json_from_text(response)
        if not deletions_json:
            _progress("Trying to load JSON from TXT file...")
            deletions_json = self.load_txt_as_json(txt_path)
        
        if not deletions_json:
            self.logger.error("Failed to extract JSON from model response")
            return None
        
        # Extract deletions list
        if isinstance(deletions_json, list):
            all_deletions = deletions_json
        elif isinstance(deletions_json, dict):
            all_deletions = deletions_json.get("deletions", deletions_json.get("data", []))
            if not isinstance(all_deletions, list):
                all_deletions = [deletions_json]
        else:
            all_deletions = [deletions_json]
        
        if not all_deletions:
            self.logger.error("No deletions found in model response")
            return None
        
        # Validate structure: should have Number and Sentence
        validated_deletions = []
        for idx, deletion in enumerate(all_deletions, start=1):
            if isinstance(deletion, dict):
                # Extract Sentence from model response
                sentence = None
                for key in ["Sentence", "sentence", "text", "Text", "content", "Content"]:
                    if key in deletion:
                        sentence = deletion[key]
                        break
                if sentence is None:
                    sentence = ""
                else:
                    sentence = str(sentence)
                
                # Auto-assign Number sequentially
                validated_deletion = {
                    "Number": str(idx),
                    "Sentence": sentence
                }
                validated_deletions.append(validated_deletion)
        
        _progress(f"Total validated deletions: {len(validated_deletions)}")
        
        # Extract chapter name from OCR Extraction JSON metadata or filename
        chapter_name = ""
        ocr_extraction_metadata = self.get_metadata_from_json(ocr_extraction_data)
        chapter_name = (
            ocr_extraction_metadata.get("chapter", "") or
            ocr_extraction_metadata.get("Chapter", "") or
            ocr_extraction_metadata.get("chapter_name", "") or
            ocr_extraction_metadata.get("Chapter_Name", "") or
            ""
        )
        
        # If not found in metadata, try to get from first record
        if not chapter_name and ocr_extraction_records:
            chapter_name = ocr_extraction_records[0].get("chapter", "")
        
        # If still not found, try to extract from OCR Extraction filename
        if not chapter_name:
            import re
            ocr_basename = os.path.basename(ocr_extraction_json_path)
            ocr_name_without_ext = os.path.splitext(ocr_basename)[0]
            # Try to extract chapter name from filename (various patterns)
            # Pattern: {prefix}{book}{chapter}+{chapter_name} or {prefix}{book}{chapter}
            match = re.match(r'^[a-z]?\d{6}\+(.+)$', ocr_name_without_ext)
            if match:
                chapter_name = match.group(1)
        
        # Clean chapter name for filename (remove invalid characters)
        if chapter_name:
            import re
            # Replace spaces and invalid filename characters with underscore
            chapter_name_clean = re.sub(r'[<>:"/\\|?*]', '_', chapter_name)
            chapter_name_clean = chapter_name_clean.replace(' ', '_')
            # Remove multiple underscores
            chapter_name_clean = re.sub(r'_+', '_', chapter_name_clean)
            # Remove leading/trailing underscores
            chapter_name_clean = chapter_name_clean.strip('_')
        else:
            chapter_name_clean = ""
        
        if chapter_name_clean:
            _progress(f"Detected Chapter Name: {chapter_name}")
        else:
            _progress("No chapter name found, will use timestamp in filename")
        
        # Generate output filename: y{book}{chapter}+{chapter_name}.json (matching Stage X pattern)
        # If chapter name is empty, use timestamp to avoid overwriting
        if chapter_name_clean:
            base_filename = f"y{book_id:03d}{chapter_id:03d}+{chapter_name_clean}.json"
        else:
            # Fallback if no chapter name: use timestamp to avoid overwriting
            from datetime import datetime
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            base_filename = f"y{book_id:03d}{chapter_id:03d}+{timestamp}.json"
            self.logger.warning(f"No chapter name found, using timestamp in filename: {timestamp}")
        
        output_path = os.path.join(output_dir, base_filename)
        
        # Check if file already exists and add counter if needed
        if os.path.exists(output_path) and chapter_name_clean:
            # If file exists and we have chapter name, add counter
            counter = 1
            while os.path.exists(output_path):
                base_filename = f"y{book_id:03d}{chapter_id:03d}+{chapter_name_clean}_{counter}.json"
                output_path = os.path.join(output_dir, base_filename)
                counter += 1
            if counter > 1:
                self.logger.info(f"File already exists, using counter: {counter - 1}")
        
        output_metadata = {
            "book_id": book_id,
            "chapter_id": chapter_id,
            "processed_at": self._get_timestamp(),
            "model": model_name,
            "ocr_extraction_path": ocr_extraction_json_path,
            "step1_output_path": step1_output_path
        }
        
        success = self.save_json_file(validated_deletions, output_path, output_metadata, "Y")
        if not success:
            self.logger.error("Failed to save Stage Y output")
            return None
        
        _progress(f"Stage Y completed: {output_filename}")
        return output_path
    
    def _process_part(
        self,
        pdf_file,
        prompt: str,
        model_name: str,
        temperature: float,
        start_page: int,
        end_page: int,
        progress_callback: Optional[Callable[[str], None]] = None
    ) -> Optional[str]:
        """
        Legacy method for processing PDF file (kept for backward compatibility).
        This method is deprecated - use _process_part_with_text instead.
        """
        self.logger.warning("_process_part with PDF file is deprecated. Use _process_part_with_text instead.")
        return None
    
    def _process_part_with_text(
        self,
        extracted_text: str,
        prompt: str,
        model_name: str,
        temperature: float,
        start_page: int,
        end_page: int,
        progress_callback: Optional[Callable[[str], None]] = None
    ) -> Optional[str]:
        """
        Process extracted PDF text (pages start_page to end_page).
        Uses api_client.process_text which has retry logic and key rotation for 429 errors.
        
        Args:
            extracted_text: Text extracted from PDF page range
            prompt: Prompt for processing
            model_name: Model name
            temperature: Temperature
            start_page: Start page number
            end_page: End page number
            progress_callback: Optional progress callback
            
        Returns:
            Response text or None on error
        """
        def _progress(msg: str):
            if progress_callback:
                progress_callback(msg)
            self.logger.info(msg)
        
        # Use api_client.process_text which has built-in retry logic and key rotation
        # This handles 429 errors automatically
        try:
            _progress(f"Processing pages {start_page}-{end_page} with model (handling retries automatically)...")
            
            # Combine prompt with extracted text
            full_prompt = f"{prompt}\n\n--- PDF Content (Pages {start_page}-{end_page}) ---\n{extracted_text}"
            
            # Use process_text which has retry logic and key rotation
            response_text = self.api_client.process_text(
                text=full_prompt,
                system_prompt="",  # No system prompt, full prompt is in text
                model_name=model_name,
                temperature=temperature
            )
            
            if response_text:
                _progress(f"Received response for pages {start_page}-{end_page} ({len(response_text)} characters)")
                return response_text
            else:
                self.logger.warning(f"No response received for pages {start_page}-{end_page}")
                return None
        except Exception as e:
            error_str = str(e)
            # Check if it's a quota error
            if '429' in error_str or 'quota' in error_str.lower() or 'resource has been exhausted' in error_str.lower():
                self.logger.error(f"Quota exhausted (429) for pages {start_page}-{end_page}. The API client should have tried all keys. Error: {e}")
                _progress(f"Error: Quota exhausted. Please wait or add more API keys.")
            else:
                self.logger.error(f"Error processing pages {start_page}-{end_page}: {e}")
            return None
    
    def _get_timestamp(self) -> str:
        """Get current timestamp"""
        from datetime import datetime
        return datetime.now().isoformat()


"""
Stage X Processor: Book Changes Detection
Detects changes between old book PDF and current Stage A data.
Has two parts:
1. Extract text from old book PDF
2. Compare and detect changes
"""

import json
import logging
import os
from typing import Optional, Dict, List, Any, Callable

from base_stage_processor import BaseStageProcessor
from multi_part_processor import MultiPartProcessor
from api_layer import APIConfig


class StageXProcessor(BaseStageProcessor):
    """Process Stage X: Detect changes between old book PDF and current Stage A data"""
    
    def __init__(self, api_client):
        super().__init__(api_client)
        self.logger = logging.getLogger(__name__)
    
    def process_stage_x(
        self,
        # Part 1: PDF Extraction
        old_book_pdf_path: str,
        pdf_extraction_prompt: str,
        pdf_extraction_model: str,
        
        # Part 2: Change Detection
        stage_a_path: str,  # File a without Imp column
        changes_prompt: str,
        changes_model: str,
        
        output_dir: Optional[str] = None,
        progress_callback: Optional[Callable[[str], None]] = None
    ) -> Optional[str]:
        """
        Process Stage X: Detect changes between old book and current data.
        
        Part 1: Extract text from old book PDF
        Part 2: Compare and detect changes
        
        Args:
            old_book_pdf_path: Path to old book PDF file
            pdf_extraction_prompt: Prompt for PDF text extraction
            pdf_extraction_model: Model for PDF extraction
            stage_a_path: Path to Stage A JSON file (a{book}{chapter}.json) - will remove Imp column
            changes_prompt: Prompt for change detection
            changes_model: Model for change detection
            output_dir: Output directory (defaults to stage_a_path directory)
            progress_callback: Optional callback for progress updates
            
        Returns:
            Path to output file (x{book}{chapter}.json) or None on error
        """
        def _progress(msg: str):
            if progress_callback:
                progress_callback(msg)
            self.logger.info(msg)
        
        _progress("Starting Stage X processing...")
        
        # Determine output directory
        if not output_dir:
            output_dir = os.path.dirname(stage_a_path) or os.getcwd()
        os.makedirs(output_dir, exist_ok=True)
        
        # Extract book and chapter from Stage A file
        stage_a_data = self.load_json_file(stage_a_path)
        if not stage_a_data:
            self.logger.error("Failed to load Stage A JSON")
            return None
        
        metadata = self.get_metadata_from_json(stage_a_data)
        book_id = metadata.get("book_id", 105)
        chapter_id = metadata.get("chapter_id", 3)
        
        # ========== PART 1: Extract PDF ==========
        _progress("Part 1: Extracting text from old book PDF...")
        
        # Process PDF in batches, save each batch as TXT, then convert to JSON
        pdf_extracted_path = self._extract_pdf_with_txt_saving(
            pdf_path=old_book_pdf_path,
            prompt=pdf_extraction_prompt,
            model_name=pdf_extraction_model,
            output_dir=output_dir,
            progress_callback=progress_callback
        )
        
        if not pdf_extracted_path or not os.path.exists(pdf_extracted_path):
            self.logger.error("Part 1: PDF extraction failed")
            return None
        
        _progress(f"Part 1 completed: {os.path.basename(pdf_extracted_path)}")
        
        # ========== PART 2: Detect Changes ==========
        _progress("Part 2: Detecting changes...")
        
        # Load Stage A data and remove Imp column
        stage_a_records = self.get_data_from_json(stage_a_data)
        if not stage_a_records:
            self.logger.error("Stage A JSON has no data")
            return None
        
        # Remove Imp column from each record
        stage_a_without_imp = []
        for record in stage_a_records:
            record_copy = record.copy()
            record_copy.pop("Imp", None)  # Remove Imp column if exists
            stage_a_without_imp.append(record_copy)
        
        _progress(f"Loaded {len(stage_a_without_imp)} records from Stage A (Imp column removed)")
        
        # Load extracted PDF JSON
        pdf_extracted_data = self.load_json_file(pdf_extracted_path)
        if not pdf_extracted_data:
            self.logger.error("Failed to load extracted PDF JSON")
            return None
        
        pdf_extracted_rows = pdf_extracted_data.get("rows", [])
        _progress(f"Loaded {len(pdf_extracted_rows)} rows from extracted PDF")
        
        # Divide Stage A into 3 parts
        total_records = len(stage_a_without_imp)
        part_size = total_records // 3
        part1_records = stage_a_without_imp[:part_size]
        part2_records = stage_a_without_imp[part_size:2*part_size]
        part3_records = stage_a_without_imp[2*part_size:]
        
        _progress(f"Divided Stage A into 3 parts: Part 1 ({len(part1_records)} records), Part 2 ({len(part2_records)} records), Part 3 ({len(part3_records)} records)")
        
        all_changes = []
        all_txt_responses = []
        base_name = os.path.splitext(os.path.basename(stage_a_path))[0]
        
        # ========== Process Part 1 of Stage A ==========
        _progress("=" * 60)
        _progress("Processing Part 1 of Stage A for change detection...")
        _progress("=" * 60)
        
        part1_data = {
            "current_data": part1_records,
            "old_book_data": pdf_extracted_rows
        }
        part1_text = json.dumps(part1_data, ensure_ascii=False, indent=2)
        
        part1_response = self.api_client.process_text(
            text=part1_text,
            system_prompt=changes_prompt,
            model_name=changes_model
        )
        
        if part1_response:
            # Save Part 1 TXT
            txt_path_1 = os.path.join(output_dir, f"{base_name}_stage_x_part2_part1.txt")
            try:
                with open(txt_path_1, 'w', encoding='utf-8') as f:
                    f.write("=== STAGE X PART 2 - PART 1 (Change Detection) RESPONSE ===\n\n")
                    f.write(part1_response)
                _progress(f"Saved Part 1 raw response to: {os.path.basename(txt_path_1)}")
                self.logger.info(f"Saved Stage X Part 2 Part 1 raw response to: {txt_path_1}")
            except Exception as e:
                self.logger.warning(f"Failed to save Part 1 TXT: {e}")
            
            all_txt_responses.append(part1_response)
            
            # Extract JSON from Part 1 (like Stage V)
            _progress("Extracting JSON from Part 1 response...")
            part1_json = self.extract_json_from_response(part1_response)
            if not part1_json:
                _progress("Trying to extract Part 1 JSON from text using fallback...")
                part1_json = self.load_txt_as_json_from_text(part1_response)
            if not part1_json:
                _progress("Trying to load Part 1 JSON from TXT file...")
                part1_json = self.load_txt_as_json(txt_path_1)
            
            if part1_json:
                # Handle both list and dict JSON structures (like Stage V)
                if isinstance(part1_json, list):
                    all_changes.extend(part1_json)
                    _progress(f"Part 1: Extracted {len(part1_json)} changes")
                elif isinstance(part1_json, dict):
                    changes = part1_json.get("changes", part1_json.get("data", []))
                    if isinstance(changes, list):
                        all_changes.extend(changes)
                        _progress(f"Part 1: Extracted {len(changes)} changes")
                    else:
                        all_changes.append(part1_json)
                        _progress("Part 1: Extracted 1 change")
        else:
            self.logger.warning("Part 1 returned no response")
        
        # ========== Process Part 2 of Stage A ==========
        _progress("=" * 60)
        _progress("Processing Part 2 of Stage A for change detection...")
        _progress("=" * 60)
        
        part2_data = {
            "current_data": part2_records,
            "old_book_data": pdf_extracted_rows
        }
        part2_text = json.dumps(part2_data, ensure_ascii=False, indent=2)
        
        part2_response = self.api_client.process_text(
            text=part2_text,
            system_prompt=changes_prompt,
            model_name=changes_model
        )
        
        if part2_response:
            # Save Part 2 TXT
            txt_path_2 = os.path.join(output_dir, f"{base_name}_stage_x_part2_part2.txt")
            try:
                with open(txt_path_2, 'w', encoding='utf-8') as f:
                    f.write("=== STAGE X PART 2 - PART 2 (Change Detection) RESPONSE ===\n\n")
                    f.write(part2_response)
                _progress(f"Saved Part 2 raw response to: {os.path.basename(txt_path_2)}")
                self.logger.info(f"Saved Stage X Part 2 Part 2 raw response to: {txt_path_2}")
            except Exception as e:
                self.logger.warning(f"Failed to save Part 2 TXT: {e}")
            
            all_txt_responses.append(part2_response)
            
            # Extract JSON from Part 2 (like Stage V)
            _progress("Extracting JSON from Part 2 response...")
            part2_json = self.extract_json_from_response(part2_response)
            if not part2_json:
                _progress("Trying to extract Part 2 JSON from text using fallback...")
                part2_json = self.load_txt_as_json_from_text(part2_response)
            if not part2_json:
                _progress("Trying to load Part 2 JSON from TXT file...")
                part2_json = self.load_txt_as_json(txt_path_2)
            
            if part2_json:
                # Handle both list and dict JSON structures (like Stage V)
                if isinstance(part2_json, list):
                    all_changes.extend(part2_json)
                    _progress(f"Part 2: Extracted {len(part2_json)} changes")
                elif isinstance(part2_json, dict):
                    changes = part2_json.get("changes", part2_json.get("data", []))
                    if isinstance(changes, list):
                        all_changes.extend(changes)
                        _progress(f"Part 2: Extracted {len(changes)} changes")
                    else:
                        all_changes.append(part2_json)
                        _progress("Part 2: Extracted 1 change")
        else:
            self.logger.warning("Part 2 returned no response")
        
        # ========== Process Part 3 of Stage A ==========
        _progress("=" * 60)
        _progress("Processing Part 3 of Stage A for change detection...")
        _progress("=" * 60)
        
        part3_data = {
            "current_data": part3_records,
            "old_book_data": pdf_extracted_rows
        }
        part3_text = json.dumps(part3_data, ensure_ascii=False, indent=2)
        
        part3_response = self.api_client.process_text(
            text=part3_text,
            system_prompt=changes_prompt,
            model_name=changes_model
        )
        
        if part3_response:
            # Save Part 3 TXT
            txt_path_3 = os.path.join(output_dir, f"{base_name}_stage_x_part2_part3.txt")
            try:
                with open(txt_path_3, 'w', encoding='utf-8') as f:
                    f.write("=== STAGE X PART 2 - PART 3 (Change Detection) RESPONSE ===\n\n")
                    f.write(part3_response)
                _progress(f"Saved Part 3 raw response to: {os.path.basename(txt_path_3)}")
                self.logger.info(f"Saved Stage X Part 2 Part 3 raw response to: {txt_path_3}")
            except Exception as e:
                self.logger.warning(f"Failed to save Part 3 TXT: {e}")
            
            all_txt_responses.append(part3_response)
            
            # Extract JSON from Part 3 (like Stage V)
            _progress("Extracting JSON from Part 3 response...")
            part3_json = self.extract_json_from_response(part3_response)
            if not part3_json:
                _progress("Trying to extract Part 3 JSON from text using fallback...")
                part3_json = self.load_txt_as_json_from_text(part3_response)
            if not part3_json:
                _progress("Trying to load Part 3 JSON from TXT file...")
                part3_json = self.load_txt_as_json(txt_path_3)
            
            if part3_json:
                # Handle both list and dict JSON structures (like Stage V)
                if isinstance(part3_json, list):
                    all_changes.extend(part3_json)
                    _progress(f"Part 3: Extracted {len(part3_json)} changes")
                elif isinstance(part3_json, dict):
                    changes = part3_json.get("changes", part3_json.get("data", []))
                    if isinstance(changes, list):
                        all_changes.extend(changes)
                        _progress(f"Part 3: Extracted {len(changes)} changes")
                    else:
                        all_changes.append(part3_json)
                        _progress("Part 3: Extracted 1 change")
        else:
            self.logger.warning("Part 3 returned no response")
        
        # Save combined TXT file
        combined_txt_path = os.path.join(output_dir, f"{base_name}_stage_x_part2_all_parts.txt")
        try:
            with open(combined_txt_path, 'w', encoding='utf-8') as f:
                for idx, response in enumerate(all_txt_responses, 1):
                    f.write(f"=== PART {idx} RESPONSE ===\n")
                    f.write(response)
                    f.write("\n\n")
            _progress(f"Saved combined TXT: {os.path.basename(combined_txt_path)}")
        except Exception as e:
            self.logger.warning(f"Failed to save combined TXT: {e}")
        
        if not all_changes:
            self.logger.error("Failed to extract JSON from model responses")
            return None
        
        # Validate structure: should have POINTID, Change Description, Change Type
        validated_changes = []
        for change in all_changes:
            if isinstance(change, dict):
                # Try multiple variations of POINTID field name (case-insensitive)
                pointid = None
                for key in ["POINTID", "PointId", "pointID", "pointid", "PointID"]:
                    if key in change:
                        pointid = change[key]
                        break
                if pointid is None:
                    pointid = ""
                # Convert to string if not empty
                if pointid:
                    pointid = str(pointid)
                
                # Try multiple variations of Change Description field name
                # Use 'in' check instead of 'or' to handle 0 correctly
                change_desc = None
                for key in ["Change Description", "change description", "ChangeDescription", "description", "Description"]:
                    if key in change:
                        change_desc = change[key]
                        break
                if change_desc is None:
                    change_desc = ""
                else:
                    # Convert to string, preserve 0 as "0" (not empty string)
                    change_desc = str(change_desc)
                
                # Try multiple variations of Change Type field name
                # Use 'in' check instead of 'or' to handle 0 correctly
                change_type = None
                for key in ["Change Type", "change type", "ChangeType", "type", "Type"]:
                    if key in change:
                        change_type = change[key]
                        break
                if change_type is None:
                    change_type = ""
                else:
                    # Convert to string, preserve 0 as "0" (not empty string)
                    change_type = str(change_type)
                
                validated_change = {
                    "POINTID": pointid,
                    "Change Description": change_desc,
                    "Change Type": change_type
                }
                validated_changes.append(validated_change)
        
        _progress(f"Total validated changes: {len(validated_changes)}")
        
        # Save output (fix save_json_file call)
        output_filename = self.generate_filename("x", book_id, chapter_id, output_dir)
        output_path = os.path.join(output_dir, output_filename)
        
        output_metadata = {
            "book_id": book_id,
            "chapter_id": chapter_id,
            "processed_at": self._get_timestamp(),
            "model": changes_model,
            "pdf_extracted_path": pdf_extracted_path,
            "stage_a_path": stage_a_path,
            "old_book_pdf_path": old_book_pdf_path  # Store old PDF path for Stage Y
        }
        
        success = self.save_json_file(validated_changes, output_path, output_metadata, "X")
        if not success:
            self.logger.error("Failed to save Stage X output")
            return None
        
        _progress(f"Stage X completed: {output_filename}")
        
        return output_path
    
    def _extract_pdf_with_txt_saving(
        self,
        pdf_path: str,
        prompt: str,
        model_name: str,
        output_dir: str,
        progress_callback: Optional[Callable[[str], None]] = None
    ) -> Optional[str]:
        """
        Extract PDF in batches, save each batch response as TXT, then convert to JSON.
        Similar to Stage V approach.
        
        Args:
            pdf_path: Path to PDF file
            prompt: Prompt for PDF extraction
            model_name: Model name
            output_dir: Output directory
            progress_callback: Optional progress callback
            
        Returns:
            Path to final JSON file or None on error
        """
        def _progress(msg: str):
            if progress_callback:
                progress_callback(msg)
            self.logger.info(msg)
        
        # Use MultiPartProcessor but modify to save TXT files
        # We'll use process_pdf_with_prompt_batch and intercept the batch processing
        # For now, let's use a simpler approach: use the existing batch processor
        # but save TXT files by modifying the batch processing callback
        
        base_name = os.path.splitext(os.path.basename(pdf_path))[0]
        all_txt_responses = []
        all_json_rows = []
        
        # We need to intercept batch responses, so let's use a custom batch processor
        # that saves TXT files. We'll use the API client's batch processing but
        # modify it to save TXT files.
        
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
        
        _progress(f"PDF has {total_pages} pages. Part 1: pages 1-{part1_end}, Part 2: pages {part2_start}-{total_pages}")
        
        # Initialize model
        if not self.api_client.initialize_text_client(model_name):
            self.logger.error("Failed to initialize text client")
            return None
        
        # Extract text from PDF parts
        from pdf_processor import PDFProcessor
        pdf_proc = PDFProcessor()
        
        # Process Part 1
        _progress(f"Extracting text from Part 1 (pages 1-{part1_end})...")
        part1_text = pdf_proc.extract_text_range(pdf_path, 1, part1_end)
        if not part1_text:
            self.logger.error(f"Failed to extract text from pages 1-{part1_end}")
            return None
        
        _progress(f"Processing Part 1 with model...")
        part1_prompt = f"{prompt}\n\nIMPORTANT: Process ONLY pages 1 to {part1_end} of the PDF. Output JSON format.\n\n--- PDF Content (Pages 1-{part1_end}) ---\n{part1_text}"
        part1_response = self._process_part_with_text(
            part1_text, part1_prompt, model_name, 0.7, 1, part1_end, progress_callback
        )

        if part1_response:
            # Save Part 1 TXT
            txt_filename_1 = f"{base_name}_stage_x_part1_part1.txt"
            txt_path_1 = os.path.join(output_dir, txt_filename_1)
            try:
                with open(txt_path_1, 'w', encoding='utf-8') as f:
                    f.write(f"=== PART 1 (Pages 1-{part1_end}) ===\n\n")
                    f.write(part1_response)
                _progress(f"Saved Part 1 TXT: {txt_filename_1}")
            except Exception as e:
                self.logger.warning(f"Failed to save Part 1 TXT: {e}")

            all_txt_responses.append(part1_response)

            # Extract JSON from Part 1
            _progress("Extracting JSON from Part 1...")
            part1_json = self.extract_json_from_response(part1_response)
            if not part1_json:
                _progress("Trying to extract JSON from Part 1 using fallback...")
                part1_json = self.load_txt_as_json_from_text(part1_response)

            if part1_json:
                # Use get_data_from_json to handle different JSON structures (rows/data/chapters)
                part1_rows = self.get_data_from_json(part1_json) if isinstance(part1_json, dict) else (part1_json if isinstance(part1_json, list) else [])
                if part1_rows:
                    all_json_rows.extend(part1_rows)
                    _progress(f"Part 1: Extracted {len(part1_rows)} rows")
                else:
                    # If no data found, add the whole JSON as a row
                    all_json_rows.append(part1_json)
                    _progress("Part 1: Extracted 1 row (whole JSON)")
        else:
            self.logger.warning("Part 1 returned no response")

        # Process Part 2
        _progress(f"Extracting text from Part 2 (pages {part2_start}-{total_pages})...")
        part2_text = pdf_proc.extract_text_range(pdf_path, part2_start, total_pages)
        if not part2_text:
            self.logger.error(f"Failed to extract text from pages {part2_start}-{total_pages}")
            return None
        
        _progress(f"Processing Part 2 with model...")
        part2_prompt = f"{prompt}\n\nIMPORTANT: Process ONLY pages {part2_start} to {total_pages} of the PDF. Output JSON format.\n\n--- PDF Content (Pages {part2_start}-{total_pages}) ---\n{part2_text}"
        part2_response = self._process_part_with_text(
            part2_text, part2_prompt, model_name, 0.7, part2_start, total_pages, progress_callback
        )

        if part2_response:
            # Save Part 2 TXT
            txt_filename_2 = f"{base_name}_stage_x_part1_part2.txt"
            txt_path_2 = os.path.join(output_dir, txt_filename_2)
            try:
                with open(txt_path_2, 'w', encoding='utf-8') as f:
                    f.write(f"=== PART 2 (Pages {part2_start}-{total_pages}) ===\n\n")
                    f.write(part2_response)
                _progress(f"Saved Part 2 TXT: {txt_filename_2}")
            except Exception as e:
                self.logger.warning(f"Failed to save Part 2 TXT: {e}")

            all_txt_responses.append(part2_response)

            # Extract JSON from Part 2
            _progress("Extracting JSON from Part 2...")
            part2_json = self.extract_json_from_response(part2_response)
            if not part2_json:
                _progress("Trying to extract JSON from Part 2 using fallback...")
                part2_json = self.load_txt_as_json_from_text(part2_response)

            if part2_json:
                # Use get_data_from_json to handle different JSON structures (rows/data/chapters)
                part2_rows = self.get_data_from_json(part2_json) if isinstance(part2_json, dict) else (part2_json if isinstance(part2_json, list) else [])
                if part2_rows:
                    all_json_rows.extend(part2_rows)
                    _progress(f"Part 2: Extracted {len(part2_rows)} rows")
                else:
                    # If no data found, add the whole JSON as a row
                    all_json_rows.append(part2_json)
                    _progress("Part 2: Extracted 1 row (whole JSON)")
        else:
            self.logger.warning("Part 2 returned no response")
        
        if not all_json_rows:
            self.logger.error("No JSON data extracted from any batch")
            return None
        
        # Save combined TXT file
        combined_txt_path = os.path.join(output_dir, f"{base_name}_stage_x_part1_all_parts.txt")
        try:
            with open(combined_txt_path, 'w', encoding='utf-8') as f:
                for idx, response in enumerate(all_txt_responses, 1):
                    f.write(f"=== PART {idx} RESPONSE ===\n")
                    f.write(response)
                    f.write("\n\n")
            _progress(f"Saved combined TXT file: {os.path.basename(combined_txt_path)}")
        except Exception as e:
            self.logger.warning(f"Failed to save combined TXT file: {e}")
        
        # Save final JSON
        final_json_path = os.path.join(output_dir, f"{base_name}_stage_x_part1_extracted.json")
        final_output = {
            "metadata": {
                "source_pdf": os.path.basename(pdf_path),
                "total_pages": total_pages,
                "total_parts": 2,
                "total_rows": len(all_json_rows),
                "processed_at": self._get_timestamp(),
                "model": model_name,
                "txt_file": combined_txt_path
            },
            "rows": all_json_rows
        }
        
        try:
            with open(final_json_path, 'w', encoding='utf-8') as f:
                json.dump(final_output, f, ensure_ascii=False, indent=2)
            _progress(f"Saved extracted JSON: {os.path.basename(final_json_path)}")
            return final_json_path
        except Exception as e:
            self.logger.error(f"Failed to save final JSON: {e}")
            return None
    
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
        """Process extracted PDF text and return raw response text."""
        try:
            import google.generativeai as genai
            
            # Ensure text_client is using the correct model
            # Recreate client if model changed
            if (not hasattr(self.api_client, '_current_model_name') or 
                self.api_client._current_model_name != model_name):
                # Get API key
                key = self.api_client.key_manager.get_next_key()
                if not key:
                    self.logger.error("No API key available")
                    return None
                genai.configure(api_key=key)
                self.api_client.text_client = genai.GenerativeModel(model_name)
                self.api_client._current_model_name = model_name
                self.logger.info(f"Recreated text_client with model: {model_name}")
            
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

            response = self.api_client.text_client.generate_content(
                prompt,
                generation_config=generation_config,
                stream=False
            )

            return response.text if hasattr(response, 'text') and response.text else None
        except Exception as e:
            self.logger.error(f"Text processing (pages {start_page}-{end_page}) failed: {e}")
            return None

    def _get_timestamp(self) -> str:
        """Get current timestamp"""
        from datetime import datetime
        return datetime.now().isoformat()


"""
Stage X Processor: Book Changes Detection
Detects changes between old book PDF and current Stage A data.
Has two parts:
1. Extract text from old book PDF
2. Compare and detect changes
"""

import json
import logging
import math
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
        progress_callback: Optional[Callable[[str], None]] = None,
        pdf_extracted_path: Optional[str] = None  # Optional: use pre-extracted PDF JSON
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
            Path to output file (x{book}{chapter}+{chapter_name}.json) or None on error
        """
        def _progress(msg: str):
            if progress_callback:
                progress_callback(msg)
            self.logger.info(msg)
        
        # Set stage if using UnifiedAPIClient (for API routing)
        if hasattr(self.api_client, 'set_stage'):
            self.api_client.set_stage("stage_x")
        
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
        # Use pre-extracted path if provided, otherwise extract
        if pdf_extracted_path and os.path.exists(pdf_extracted_path):
            _progress(f"Part 1: Using pre-extracted PDF: {os.path.basename(pdf_extracted_path)}")
        else:
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
        
        # Divide Stage A into parts of 200 records each
        PART_SIZE = 200
        total_records = len(stage_a_without_imp)
        num_parts = math.ceil(total_records / PART_SIZE)
        
        _progress(f"Dividing Stage A into {num_parts} parts (max {PART_SIZE} records per part)")
        
        # Split Stage A data into parts
        stage_a_parts = []
        for i in range(num_parts):
            start_idx = i * PART_SIZE
            end_idx = min((i + 1) * PART_SIZE, total_records)
            part_data = stage_a_without_imp[start_idx:end_idx]
            stage_a_parts.append(part_data)
            _progress(f"Part {i+1}: {len(part_data)} records (indices {start_idx} to {end_idx-1})")
        
        all_changes = []
        all_txt_responses = []
        base_name = os.path.splitext(os.path.basename(stage_a_path))[0]
        
        # Process each part separately
        for part_num, part_records in enumerate(stage_a_parts, 1):
            _progress("=" * 60)
            _progress(f"Processing Part {part_num}/{num_parts} of Stage A for change detection ({len(part_records)} records)...")
            _progress("=" * 60)
            
            part_data = {
                "current_data": part_records,
                "old_book_data": pdf_extracted_rows
            }
            part_text = json.dumps(part_data, ensure_ascii=False, indent=2)
            
            part_response = self.api_client.process_text(
                text=part_text,
                system_prompt=changes_prompt,
                model_name=changes_model
            )
            
            if part_response:
                # Save Part TXT
                txt_path = os.path.join(output_dir, f"{base_name}_stage_x_part2_part{part_num}.txt")
                try:
                    with open(txt_path, 'w', encoding='utf-8') as f:
                        f.write(f"=== STAGE X PART 2 - PART {part_num} (Change Detection) RESPONSE ===\n\n")
                        f.write(part_response)
                    _progress(f"Saved Part {part_num} raw response to: {os.path.basename(txt_path)}")
                    self.logger.info(f"Saved Stage X Part 2 Part {part_num} raw response to: {txt_path}")
                except Exception as e:
                    self.logger.warning(f"Failed to save Part {part_num} TXT: {e}")
                
                all_txt_responses.append(part_response)
                
                # Extract JSON from Part
                _progress(f"Extracting JSON from Part {part_num} response...")
                part_json = self.extract_json_from_response(part_response)
                if not part_json:
                    _progress(f"Trying to extract Part {part_num} JSON from text using fallback...")
                    part_json = self.load_txt_as_json_from_text(part_response)
                if not part_json:
                    _progress(f"Trying to load Part {part_num} JSON from TXT file...")
                    part_json = self.load_txt_as_json(txt_path)
                
                if part_json:
                    # Handle both list and dict JSON structures
                    if isinstance(part_json, list):
                        all_changes.extend(part_json)
                        _progress(f"Part {part_num}: Extracted {len(part_json)} changes")
                    elif isinstance(part_json, dict):
                        changes = part_json.get("changes", part_json.get("data", []))
                        if isinstance(changes, list):
                            all_changes.extend(changes)
                            _progress(f"Part {part_num}: Extracted {len(changes)} changes")
                        else:
                            all_changes.append(part_json)
                            _progress(f"Part {part_num}: Extracted 1 change")
            else:
                self.logger.warning(f"Part {part_num} returned no response")
                _progress(f"Warning: Part {part_num} returned no response. Continuing...")
        
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
        
        # Extract chapter name from Stage A metadata or filename
        chapter_name = ""
        stage_a_metadata = self.get_metadata_from_json(stage_a_data)
        chapter_name = (
            stage_a_metadata.get("chapter", "") or
            stage_a_metadata.get("Chapter", "") or
            stage_a_metadata.get("chapter_name", "") or
            stage_a_metadata.get("Chapter_Name", "") or
            ""
        )
        
        # If not found in metadata, try to get from first record
        if not chapter_name and stage_a_records:
            chapter_name = stage_a_records[0].get("chapter", "")
        
        # If still not found, try to extract from Stage A filename (a{book}{chapter}+{chapter_name}.json)
        if not chapter_name:
            import re
            stage_a_basename = os.path.basename(stage_a_path)
            stage_a_name_without_ext = os.path.splitext(stage_a_basename)[0]
            # Try to extract chapter name from filename pattern: a{book}{chapter}+{chapter_name}
            match = re.match(r'^a\d{6}\+(.+)$', stage_a_name_without_ext)
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
        
        # Generate output filename: x{book}{chapter}+{chapter_name}.json (matching Stage H/V/L pattern)
        # If chapter name is empty, use timestamp to avoid overwriting
        if chapter_name_clean:
            base_filename = f"x{book_id:03d}{chapter_id:03d}+{chapter_name_clean}.json"
        else:
            # Fallback if no chapter name: use timestamp to avoid overwriting
            from datetime import datetime
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            base_filename = f"x{book_id:03d}{chapter_id:03d}+{timestamp}.json"
            self.logger.warning(f"No chapter name found, using timestamp in filename: {timestamp}")
        
        output_path = os.path.join(output_dir, base_filename)
        
        # Check if file already exists and add counter if needed
        if os.path.exists(output_path) and chapter_name_clean:
            # If file exists and we have chapter name, add counter
            counter = 1
            while os.path.exists(output_path):
                base_filename = f"x{book_id:03d}{chapter_id:03d}+{chapter_name_clean}_{counter}.json"
                output_path = os.path.join(output_dir, base_filename)
                counter += 1
            if counter > 1:
                self.logger.info(f"File already exists, using counter: {counter - 1}")
        
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


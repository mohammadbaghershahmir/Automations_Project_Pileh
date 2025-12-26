"""
Stage Y Processor: Deletion Detection
Detects deleted content by comparing old book PDF extraction with current Stage A data.
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
        stage_a_path: str,  # File a without Imp column
        pdf_extracted_json_path: str,  # JSON extracted from PDF (Part 1 of Stage X)
        stage_x_output_path: str,  # Output from Stage X
        prompt: str,
        model_name: str,
        output_dir: Optional[str] = None,
        progress_callback: Optional[Callable[[str], None]] = None
    ) -> Optional[str]:
        """
        Process Stage Y: Detect deleted content.
        
        Args:
            stage_a_path: Path to Stage A JSON file (a{book}{chapter}.json) - will remove Imp column
            pdf_extracted_json_path: Path to JSON extracted from old book PDF (Part 1 of Stage X)
            stage_x_output_path: Path to Stage X output JSON
            prompt: Prompt for deletion detection
            model_name: Gemini model name
            output_dir: Output directory (defaults to stage_a_path directory)
            progress_callback: Optional callback for progress updates
            
        Returns:
            Path to output file (y{book}{chapter}.json) or None on error
        """
        def _progress(msg: str):
            if progress_callback:
                progress_callback(msg)
            self.logger.info(msg)
        
        _progress("Starting Stage Y processing...")
        
        # Determine output directory
        if not output_dir:
            output_dir = os.path.dirname(stage_a_path) or os.getcwd()
        os.makedirs(output_dir, exist_ok=True)
        
        # Load Stage A data and remove Imp column
        stage_a_data = self.load_json_file(stage_a_path)
        if not stage_a_data:
            self.logger.error("Failed to load Stage A JSON")
            return None
        
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
        
        # Load PDF extracted JSON
        pdf_extracted_data = self.load_json_file(pdf_extracted_json_path)
        if not pdf_extracted_data:
            self.logger.error("Failed to load PDF extracted JSON")
            return None
        
        pdf_extracted_rows = pdf_extracted_data.get("rows", [])
        _progress(f"Loaded {len(pdf_extracted_rows)} rows from PDF extracted JSON")
        
        # Load Stage X output
        stage_x_data = self.load_json_file(stage_x_output_path)
        if not stage_x_data:
            self.logger.error("Failed to load Stage X output")
            return None
        
        stage_x_changes = self.get_data_from_json(stage_x_data)
        _progress(f"Loaded {len(stage_x_changes)} changes from Stage X")
        
        # Divide Stage A into 3 parts
        total_records = len(stage_a_without_imp)
        part_size = total_records // 3
        part1_records = stage_a_without_imp[:part_size]
        part2_records = stage_a_without_imp[part_size:2*part_size]
        part3_records = stage_a_without_imp[2*part_size:]
        
        _progress(f"Divided Stage A into 3 parts: Part 1 ({len(part1_records)} records), Part 2 ({len(part2_records)} records), Part 3 ({len(part3_records)} records)")
        
        all_deletions = []
        all_txt_responses = []
        base_name = os.path.splitext(os.path.basename(stage_a_path))[0]
        
        # ========== Process Part 1 of Stage A ==========
        _progress("=" * 60)
        _progress("Processing Part 1 of Stage A for deletion detection...")
        _progress("=" * 60)
        
        part1_data = {
            "current_data": part1_records,
            "old_book_data": pdf_extracted_rows,
            "detected_changes": stage_x_changes
        }
        part1_text = json.dumps(part1_data, ensure_ascii=False, indent=2)
        
        part1_response = self.api_client.process_text(
            text=part1_text,
            system_prompt=prompt,
            model_name=model_name
        )
        
        if part1_response:
            # Save Part 1 TXT
            txt_path_1 = os.path.join(output_dir, f"{base_name}_stage_y_part1.txt")
            try:
                with open(txt_path_1, 'w', encoding='utf-8') as f:
                    f.write("=== STAGE Y - PART 1 (Deletion Detection) RESPONSE ===\n\n")
                    f.write(part1_response)
                _progress(f"Saved Part 1 raw response to: {os.path.basename(txt_path_1)}")
                self.logger.info(f"Saved Stage Y Part 1 raw response to: {txt_path_1}")
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
                    all_deletions.extend(part1_json)
                    _progress(f"Part 1: Extracted {len(part1_json)} deletions")
                elif isinstance(part1_json, dict):
                    deletions = part1_json.get("deletions", part1_json.get("data", []))
                    if isinstance(deletions, list):
                        all_deletions.extend(deletions)
                        _progress(f"Part 1: Extracted {len(deletions)} deletions")
                    else:
                        all_deletions.append(part1_json)
                        _progress("Part 1: Extracted 1 deletion")
        else:
            self.logger.warning("Part 1 returned no response")
        
        # ========== Process Part 2 of Stage A ==========
        _progress("=" * 60)
        _progress("Processing Part 2 of Stage A for deletion detection...")
        _progress("=" * 60)
        
        part2_data = {
            "current_data": part2_records,
            "old_book_data": pdf_extracted_rows,
            "detected_changes": stage_x_changes
        }
        part2_text = json.dumps(part2_data, ensure_ascii=False, indent=2)
        
        part2_response = self.api_client.process_text(
            text=part2_text,
            system_prompt=prompt,
            model_name=model_name
        )
        
        if part2_response:
            # Save Part 2 TXT
            txt_path_2 = os.path.join(output_dir, f"{base_name}_stage_y_part2.txt")
            try:
                with open(txt_path_2, 'w', encoding='utf-8') as f:
                    f.write("=== STAGE Y - PART 2 (Deletion Detection) RESPONSE ===\n\n")
                    f.write(part2_response)
                _progress(f"Saved Part 2 raw response to: {os.path.basename(txt_path_2)}")
                self.logger.info(f"Saved Stage Y Part 2 raw response to: {txt_path_2}")
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
                    all_deletions.extend(part2_json)
                    _progress(f"Part 2: Extracted {len(part2_json)} deletions")
                elif isinstance(part2_json, dict):
                    deletions = part2_json.get("deletions", part2_json.get("data", []))
                    if isinstance(deletions, list):
                        all_deletions.extend(deletions)
                        _progress(f"Part 2: Extracted {len(deletions)} deletions")
                    else:
                        all_deletions.append(part2_json)
                        _progress("Part 2: Extracted 1 deletion")
        else:
            self.logger.warning("Part 2 returned no response")
        
        # ========== Process Part 3 of Stage A ==========
        _progress("=" * 60)
        _progress("Processing Part 3 of Stage A for deletion detection...")
        _progress("=" * 60)
        
        part3_data = {
            "current_data": part3_records,
            "old_book_data": pdf_extracted_rows,
            "detected_changes": stage_x_changes
        }
        part3_text = json.dumps(part3_data, ensure_ascii=False, indent=2)
        
        part3_response = self.api_client.process_text(
            text=part3_text,
            system_prompt=prompt,
            model_name=model_name
        )
        
        if part3_response:
            # Save Part 3 TXT
            txt_path_3 = os.path.join(output_dir, f"{base_name}_stage_y_part3.txt")
            try:
                with open(txt_path_3, 'w', encoding='utf-8') as f:
                    f.write("=== STAGE Y - PART 3 (Deletion Detection) RESPONSE ===\n\n")
                    f.write(part3_response)
                _progress(f"Saved Part 3 raw response to: {os.path.basename(txt_path_3)}")
                self.logger.info(f"Saved Stage Y Part 3 raw response to: {txt_path_3}")
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
                    all_deletions.extend(part3_json)
                    _progress(f"Part 3: Extracted {len(part3_json)} deletions")
                elif isinstance(part3_json, dict):
                    deletions = part3_json.get("deletions", part3_json.get("data", []))
                    if isinstance(deletions, list):
                        all_deletions.extend(deletions)
                        _progress(f"Part 3: Extracted {len(deletions)} deletions")
                    else:
                        all_deletions.append(part3_json)
                        _progress("Part 3: Extracted 1 deletion")
        else:
            self.logger.warning("Part 3 returned no response")
        
        # Save combined TXT file
        combined_txt_path = os.path.join(output_dir, f"{base_name}_stage_y_all_parts.txt")
        try:
            with open(combined_txt_path, 'w', encoding='utf-8') as f:
                for idx, response in enumerate(all_txt_responses, 1):
                    f.write(f"=== PART {idx} RESPONSE ===\n")
                    f.write(response)
                    f.write("\n\n")
            _progress(f"Saved combined TXT: {os.path.basename(combined_txt_path)}")
        except Exception as e:
            self.logger.warning(f"Failed to save combined TXT: {e}")
        
        if not all_deletions:
            self.logger.error("Failed to extract JSON from model responses")
            return None
        
        # Validate structure: should have Number and Sentence
        # Number will be auto-assigned sequentially, not taken from model
        validated_deletions = []
        for idx, deletion in enumerate(all_deletions, start=1):
            if isinstance(deletion, dict):
                # Extract Sentence from model response (try multiple field name variations)
                sentence = None
                for key in ["Sentence", "sentence", "text", "Text", "content", "Content"]:
                    if key in deletion:
                        sentence = deletion[key]
                        break
                if sentence is None:
                    sentence = ""
                else:
                    sentence = str(sentence)
                
                # Auto-assign Number sequentially (starting from 1)
                validated_deletion = {
                    "Number": str(idx),
                    "Sentence": sentence
                }
                validated_deletions.append(validated_deletion)
        
        _progress(f"Total validated deletions: {len(validated_deletions)} (Numbers auto-assigned 1-{len(validated_deletions)})")
        
        # Extract book and chapter from Stage A
        metadata = self.get_metadata_from_json(stage_a_data)
        book_id = metadata.get("book_id", 105)
        chapter_id = metadata.get("chapter_id", 3)
        
        # Save output (fix save_json_file call)
        output_filename = self.generate_filename("y", book_id, chapter_id, output_dir)
        output_path = os.path.join(output_dir, output_filename)
        
        output_metadata = {
            "book_id": book_id,
            "chapter_id": chapter_id,
            "processed_at": self._get_timestamp(),
            "model": model_name,
            "stage_a_path": stage_a_path,
            "pdf_extracted_path": pdf_extracted_json_path,
            "stage_x_path": stage_x_output_path
        }
        
        success = self.save_json_file(validated_deletions, output_path, output_metadata, "Y")
        if not success:
            self.logger.error("Failed to save Stage Y output")
            return None
        
        _progress(f"Stage Y completed: {output_filename}")
        
        return output_path
    
    def _get_timestamp(self) -> str:
        """Get current timestamp"""
        from datetime import datetime
        return datetime.now().isoformat()


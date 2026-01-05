"""
Stage E Processor: Image Notes Processing
Takes Stage 4 JSON (with PointId) and Stage 1 JSON, generates image notes,
and merges them with Stage 4 data.
"""

import json
import logging
import math
import os
import time
from datetime import datetime
from typing import Optional, Dict, List, Any, Callable
from base_stage_processor import BaseStageProcessor
from api_layer import APIConfig


class StageEProcessor(BaseStageProcessor):
    """Process Stage E: Generate image notes and merge with Stage 4 data"""
    
    def __init__(self, api_client):
        super().__init__(api_client)
        self.logger = logging.getLogger(__name__)
    
    def process_stage_e(
        self,
        stage4_path: str,
        ocr_extraction_json_path: str,
        prompt: str,
        model_name: str,
        output_dir: Optional[str] = None,
        progress_callback: Optional[Callable[[str], None]] = None
    ) -> Optional[str]:
        """
        Process Stage E: Generate image notes and merge with Stage 4.
        
        Args:
            stage4_path: Path to Stage 4 JSON file (with PointId)
            ocr_extraction_json_path: Path to OCR Extraction JSON file (for subchapter names)
            prompt: User prompt for image notes generation
            model_name: Gemini model name
            output_dir: Output directory (defaults to stage4_path directory)
            progress_callback: Optional callback for progress updates
            
        Returns:
            Path to output file (e{book}{chapter}.json) or None on error
        """
        def _progress(msg: str):
            if progress_callback:
                progress_callback(msg)
            self.logger.info(msg)
        
        _progress("Starting Stage E processing...")
        
        # Load OCR Extraction JSON (Stage 1) - contains figure and table descriptions
        _progress("Loading OCR Extraction JSON (Stage 1)...")
        ocr_extraction_data = self.load_json_file(ocr_extraction_json_path)
        if not ocr_extraction_data:
            self.logger.error("Failed to load OCR Extraction JSON")
            _progress("Error: Failed to load OCR Extraction JSON")
            return None
        
        # Load Stage 4 JSON
        _progress("Loading Stage 4 JSON...")
        stage4_data = self.load_json_file(stage4_path)
        if not stage4_data:
            self.logger.error("Failed to load Stage 4 JSON")
            return None
        
        # Extract data from Stage 4
        stage4_points = self.get_data_from_json(stage4_data)
        
        if not stage4_points:
            self.logger.error("Stage 4 JSON has no data/points")
            _progress("Error: Stage 4 JSON has no data/points")
            return None
        
        # Extract book and chapter from first PointId in Stage 4
        first_point_id = stage4_points[0].get("PointId")
        if not first_point_id:
            self.logger.error("No PointId found in Stage 4 data")
            return None
        
        try:
            book_id, chapter_id = self.extract_book_chapter_from_pointid(first_point_id)
        except ValueError as e:
            self.logger.error(f"Error extracting book/chapter: {e}")
            return None
        
        _progress(f"Detected Book ID: {book_id}, Chapter ID: {chapter_id}")
        
        # Get last PointId from Stage 4 to continue numbering
        last_point = stage4_points[-1]
        last_point_id = last_point.get("PointId", "")
        if not last_point_id:
            self.logger.error("No PointId in last point of Stage 4")
            return None
        
        # Extract current index from last PointId
        try:
            current_index = int(last_point_id[6:10]) + 1  # Next index after last point
        except (ValueError, IndexError):
            self.logger.error(f"Invalid PointId format: {last_point_id}")
            return None
        
        _progress(f"Last PointId in Stage 4: {last_point_id}, Starting image notes from index: {current_index}")
        
        # Extract subchapters from OCR Extraction JSON (Persian names)
        _progress("Extracting subchapters from OCR Extraction JSON...")
        ocr_subchapters = self._extract_subchapters_from_ocr(ocr_extraction_data)
        
        if not ocr_subchapters:
            self.logger.error("No subchapters found in OCR Extraction JSON")
            _progress("Error: No subchapters found in OCR Extraction JSON")
            return None
        
        num_parts = len(ocr_subchapters)
        _progress(f"Found {num_parts} subchapters to process")
        
        # Prepare output directory
        base_dir = os.path.dirname(stage4_path) or os.getcwd()
        base_name, _ = os.path.splitext(os.path.basename(stage4_path))
        
        # Generate output filename
        output_filename = self.generate_filename("e", book_id, chapter_id)
        output_path = os.path.join(base_dir, output_filename)
        
        # Initialize output JSON file immediately (incremental writing)
        _progress("Initializing output JSON file...")
        initial_output = {
            "metadata": {
                "processing_status": "in_progress",
                "subchapters_processed": 0,
                "total_subchapters": num_parts,
                "book_id": book_id,
                "chapter_id": chapter_id,
                "source_stage4": os.path.basename(stage4_path),
                "source_ocr_extraction": os.path.basename(ocr_extraction_json_path),
                "model_used": model_name,
                "division_method": "ocr_extraction_by_subchapter",
                "ocr_subchapters": ocr_subchapters,
                "stage": "E",
            },
            "data": [],  # Will contain merged points at the end
            "raw_responses": []  # All raw model responses
        }
        
        try:
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(initial_output, f, ensure_ascii=False, indent=2)
            self.logger.info(f"Initialized Stage E JSON file: {output_path}")
        except Exception as e:
            self.logger.error(f"Failed to initialize output file: {e}")
            return None
        
        # Prepare OCR Extraction JSON string (complete - for context)
        ocr_extraction_json_str = json.dumps(ocr_extraction_data, ensure_ascii=False, indent=2)
        
        max_retries = 2
        all_filepic_records = []  # Collect all filepic records from all subchapters
        
        # Process each subchapter individually
        for part_num, persian_subchapter_name in enumerate(ocr_subchapters, 1):
            _progress("=" * 60)
            _progress(f"Processing Part {part_num}/{num_parts} - Subchapter: '{persian_subchapter_name}'...")
            _progress("=" * 60)
            
            # Filter Stage 4 points for this subchapter only
            _progress(f"Filtering Stage 4 points for subchapter '{persian_subchapter_name}'...")
            filtered_stage4_points = [
                point for point in stage4_points 
                if point.get("subchapter", "").strip() == persian_subchapter_name
            ]
            
            _progress(f"Found {len(filtered_stage4_points)} points for subchapter '{persian_subchapter_name}' (out of {len(stage4_points)} total)")
            
            if not filtered_stage4_points:
                self.logger.warning(f"No Stage 4 points found for subchapter '{persian_subchapter_name}'. Skipping this subchapter.")
                _progress(f"Warning: No Stage 4 points found for subchapter '{persian_subchapter_name}'. Skipping...")
                continue
            
            # Convert filtered points to JSON string
            stage4_json_str = json.dumps(filtered_stage4_points, ensure_ascii=False, indent=2)
            
            # Replace {SUBCHAPTER_NAME} placeholder in prompt with Persian subchapter name from OCR Extraction JSON
            prompt_with_subchapter = prompt.replace("{SUBCHAPTER_NAME}", persian_subchapter_name)
            
            _progress(f"Using Persian subchapter name in prompt: '{persian_subchapter_name}'")
            
            # Prepare full prompt with filtered Stage 4 JSON for this subchapter
            full_prompt = f"""{prompt_with_subchapter}

==================================================
فایل JSON متن درسی استخراج‌شده از کتاب درماتولوژی (OCR Extraction JSON):
==================================================
{ocr_extraction_json_str}

==================================================
فایل JSON ساختار سلسله‌مراتبی درسنامه نهایی (Stage 4 JSON - Filtered for subchapter '{persian_subchapter_name}' - {len(filtered_stage4_points)} points):
==================================================
{stage4_json_str}
"""
            
            # Call model with retry mechanism
            _progress(f"Calling model {model_name} for subchapter '{persian_subchapter_name}'...")
            response_text = None
            filepic_data = None
            
            for attempt in range(1, max_retries + 2):  # Initial attempt + max retries
                if attempt > 1:
                    _progress(f"Retrying model call (attempt {attempt}/{max_retries + 1})...")
                
                try:
                    response_text = self.api_client.process_text(
                        text=full_prompt,
                        system_prompt=None,
                        model_name=model_name,
                        temperature=APIConfig.DEFAULT_TEMPERATURE,
                        max_tokens=APIConfig.DEFAULT_MAX_TOKENS
                    )
                    
                    if not response_text:
                        continue
                    
                    # Try to extract JSON directly from response
                    _progress("Extracting JSON from model response...")
                    filepic_data = self.extract_json_from_response(response_text)
                    if not filepic_data:
                        filepic_data = self.load_txt_as_json_from_text(response_text)
                    
                    if filepic_data:
                        _progress(f"Successfully extracted JSON (attempt {attempt})")
                        break
                    else:
                        _progress(f"JSON extraction failed (attempt {attempt}), retrying...")
                        self.logger.warning(f"Failed to extract JSON from response (attempt {attempt})")
                        
                except Exception as e:
                    self.logger.warning(f"Error calling model (attempt {attempt}): {e}")
                    response_text = None
            
            if not response_text:
                self.logger.error(f"No response from model after retries for subchapter '{persian_subchapter_name}'")
                _progress(f"Error: Failed to get response from model for subchapter '{persian_subchapter_name}'.")
                continue
            
            if not filepic_data:
                self.logger.error(f"Failed to extract JSON from model response for subchapter '{persian_subchapter_name}'")
                _progress(f"Error: Failed to extract JSON from model response for subchapter '{persian_subchapter_name}'.")
                continue
            
            # Immediately add raw response to JSON file (incremental write)
            try:
                with open(output_path, "r", encoding="utf-8") as f:
                    current_data = json.load(f)
                
                raw_response_entry = {
                    "subchapter_index": part_num,
                    "subchapter": persian_subchapter_name,
                    "response_text": response_text,
                    "response_size_bytes": len(response_text.encode('utf-8')),
                    "processed_at": datetime.now().isoformat()
                }
                current_data["raw_responses"].append(raw_response_entry)
                
                current_data["metadata"]["subchapters_processed"] = len(current_data["raw_responses"])
                current_data["metadata"]["processed_at"] = datetime.now().isoformat()
                
                with open(output_path, "w", encoding="utf-8") as f:
                    json.dump(current_data, f, ensure_ascii=False, indent=2)
                
                self.logger.info(f"  ✓ Added raw response for subchapter '{persian_subchapter_name}' to JSON file immediately")
            except Exception as e:
                self.logger.error(f"Failed to write raw response for subchapter '{persian_subchapter_name}' to file: {e}", exc_info=True)
            
            # Handle different JSON structures
            if isinstance(filepic_data, list):
                subchapter_filepic_records = filepic_data
            elif isinstance(filepic_data, dict):
                # Try common keys: data, rows, payload
                subchapter_filepic_records = filepic_data.get("data", filepic_data.get("rows", filepic_data.get("payload", [])))
                if not subchapter_filepic_records:
                    # Try to extract from nested structure - get first list value
                    for value in filepic_data.values():
                        if isinstance(value, list):
                            subchapter_filepic_records = value
                            break
                    if not subchapter_filepic_records:
                        subchapter_filepic_records = []
            else:
                self.logger.warning(f"Unexpected JSON structure from model for subchapter '{persian_subchapter_name}': {type(filepic_data)}")
                subchapter_filepic_records = []
            
            if subchapter_filepic_records:
                all_filepic_records.extend(subchapter_filepic_records)
                _progress(f"Extracted {len(subchapter_filepic_records)} image note records from subchapter '{persian_subchapter_name}'")
            else:
                self.logger.warning(f"No records found in filepic data for subchapter '{persian_subchapter_name}'")
            
            # Add delay between parts to avoid rate limiting (429 errors)
            if part_num < num_parts:  # Don't delay after the last part
                delay_seconds = 5  # 2 seconds delay between parts
                _progress(f"Waiting {delay_seconds} seconds before processing next subchapter...")
                time.sleep(delay_seconds)
        
        if not all_filepic_records:
            self.logger.error("No filepic records found from any subchapter")
            _progress("Error: No records found in model responses.")
            return None
        
        _progress(f"Total extracted {len(all_filepic_records)} image note records from all subchapters")
        
        # Process filepic records:
        # 1. Remove caption column
        # 2. Replace point_text with points from Stage 4
        # 3. Assign PointId sequentially
        
        _progress("Processing image notes...")
        processed_image_notes = []
        first_image_point_id = None
        
        for idx, record in enumerate(all_filepic_records):
            if not isinstance(record, dict):
                continue
            
            # Remove caption and prepare new record
            processed_record = {}
            
            # Copy all fields except caption and point_text
            # This includes topic, chapter, subchapter, subtopic, subsubtopic from model output
            for k, v in record.items():
                if k not in ["caption", "point_text"]:
                    processed_record[k] = v
            
            # Convert point_text to Points
            # Get point_text value from the record (this is the image reference like "تصویر e30:18")
            point_text_value = record.get("point_text", "")
            
            # Set Points column with the point_text value
            processed_record["Points"] = point_text_value
            
            # Assign PointId
            point_id = f"{book_id:03d}{chapter_id:03d}{current_index:04d}"
            processed_record["PointId"] = point_id
            
            if first_image_point_id is None:
                first_image_point_id = point_id
            
            processed_image_notes.append(processed_record)
            current_index += 1
        
        _progress(f"Processed {len(processed_image_notes)} image notes")
        _progress(f"First image PointId: {first_image_point_id}")
        
        # Save filepic JSON (with caption) for use in Stage F
        # IMPORTANT: Save the ORIGINAL all_filepic_records (with caption) before processing
        _progress("Saving filepic JSON for Stage F...")
        filepic_json_path = os.path.join(base_dir, f"{base_name}_filepic.json")
        filepic_saved = False
        
        # Verify that all_filepic_records contain caption before saving
        caption_count = 0
        for record in all_filepic_records:
            if isinstance(record, dict) and record.get("caption"):
                caption_count += 1
        
        _progress(f"Found {caption_count} records with caption out of {len(all_filepic_records)}")
        
        try:
            filepic_metadata = {
                "book_id": book_id,
                "chapter_id": chapter_id,
                "total_records": len(all_filepic_records),
                "records_with_caption": caption_count,
                "division_method": "ocr_extraction_by_subchapter",
                "ocr_subchapters": ocr_subchapters
            }
            # Save original all_filepic_records with caption (before processing)
            success = self.save_json_file(all_filepic_records, filepic_json_path, filepic_metadata, "filepic")
            if success:
                _progress(f"Filepic saved to: {filepic_json_path}")
                filepic_saved = True
                # Verify that caption exists in saved file
                try:
                    with open(filepic_json_path, 'r', encoding='utf-8') as f:
                        saved_data = json.load(f)
                        saved_records = self.get_data_from_json(saved_data)
                        if saved_records:
                            first_record = saved_records[0]
                            if "caption" in first_record:
                                caption_value = first_record.get("caption", "")
                                _progress(f"Verified: filepic contains caption field (first caption: {len(caption_value)} chars)")
                            else:
                                self.logger.warning(f"filepic saved but caption not found. Keys: {list(first_record.keys())}")
                except Exception as e:
                    self.logger.warning(f"Could not verify filepic JSON: {e}")
            else:
                self.logger.error("Failed to save filepic JSON")
        except Exception as e:
            self.logger.error(f"Error saving filepic JSON: {e}", exc_info=True)
            # Continue anyway, Stage F can try to load from JSON
        
        # Merge Stage 4 points with image notes
        _progress("Merging Stage 4 points with image notes...")
        merged_points = stage4_points + processed_image_notes
        
        # Final update: Add processed data and remove raw_responses
        try:
            with open(output_path, "r", encoding="utf-8") as f:
                current_data = json.load(f)
            
            current_data["data"] = merged_points
            current_data["metadata"]["processing_status"] = "completed"
            current_data["metadata"]["stage4_total_points"] = len(stage4_points)
            current_data["metadata"]["image_notes_count"] = len(processed_image_notes)
            current_data["metadata"]["first_image_point_id"] = first_image_point_id
            current_data["metadata"]["last_point_id"] = f"{book_id:03d}{chapter_id:03d}{(current_index - 1):04d}"
            current_data["metadata"]["filepic_json_file"] = os.path.basename(filepic_json_path) if filepic_saved else None
            current_data["metadata"]["filepic_json_path"] = filepic_json_path if filepic_saved else None
            current_data["metadata"]["processed_at"] = datetime.now().isoformat()
            
            # Get additional metadata from Stage 4
            stage4_metadata = self.get_metadata_from_json(stage4_data)
            for k, v in stage4_metadata.items():
                if k not in ["stage", "processed_at", "total_records"]:
                    current_data["metadata"][k] = v
            
            # Remove raw_responses from final output
            if "raw_responses" in current_data:
                del current_data["raw_responses"]
                self.logger.info("Removed raw_responses from final output file")
            
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(current_data, f, ensure_ascii=False, indent=2)
            
            _progress(f"Stage E completed successfully: {output_path}")
            return output_path
        except Exception as e:
            self.logger.error(f"Failed to finalize Stage E output: {e}", exc_info=True)
            return None
    
    def _convert_chapters_to_rows(self, chapters: List[Dict]) -> List[Dict]:
        """
        Convert chapters structure to rows structure.
        
        Args:
            chapters: List of chapter objects with subchapters
            
        Returns:
            List of row objects
        """
        rows = []
        for chapter_obj in chapters:
            chapter_name = chapter_obj.get("chapter", "")
            subchapters = chapter_obj.get("subchapters", [])
            
            if isinstance(subchapters, list):
                for subchapter_obj in subchapters:
                    # Create a row from subchapter data
                    row = {
                        "chapter": chapter_name,
                        **subchapter_obj  # Include all subchapter fields
                    }
                    rows.append(row)
            elif isinstance(subchapters, dict):
                # If subchapters is a dict, treat it as a single row
                row = {
                    "chapter": chapter_name,
                    **subchapters
                }
                rows.append(row)
        
        return rows
    
    def _extract_subchapters_from_ocr(self, ocr_extraction_data: Dict) -> List[str]:
        """
        Extract subchapter names (Persian) from OCR Extraction JSON.
        
        OCR Extraction JSON structure:
        {
          "chapters": [
            {
              "chapter": "...",
              "subchapters": [
                {
                  "subchapter": "...",  # Persian name
                  "topics": [...]
                }
              ]
            }
          ]
        }
        
        Returns:
            List of subchapter names (Persian) in order
        """
        subchapters = []
        chapters = ocr_extraction_data.get("chapters", [])
        
        for chapter_obj in chapters:
            if not isinstance(chapter_obj, dict):
                continue
            
            subchapters_list = chapter_obj.get("subchapters", [])
            for subchapter_obj in subchapters_list:
                if not isinstance(subchapter_obj, dict):
                    continue
                
                subchapter_name = subchapter_obj.get("subchapter", "").strip()
                if subchapter_name and subchapter_name not in subchapters:
                    subchapters.append(subchapter_name)
        
        return subchapters
    
    def _extract_chapter_subchapter_topic_mapping(self, ocr_extraction_data: Dict) -> List[Dict]:
        """
        Extract chapter/subchapter/topic mapping from OCR Extraction JSON.
        
        OCR Extraction JSON structure:
        {
          "chapters": [
            {
              "chapter": "...",
              "subchapters": [
                {
                  "subchapter": "...",
                  "topics": [
                    {
                      "topic": "...",
                      "extractions": [...]
                    }
                  ]
                }
              ]
            }
          ]
        }
        
        Returns a flat list of {chapter, subchapter, topic} for each extraction point.
        Note: The topic field in the mapping is not used - topic comes from model output instead.
        """
        mapping = []
        
        chapters = ocr_extraction_data.get("chapters", [])
        
        for chapter_obj in chapters:
            if not isinstance(chapter_obj, dict):
                continue
            
            chapter_name = chapter_obj.get("chapter", "")
            subchapters = chapter_obj.get("subchapters", [])
            
            for subchapter_obj in subchapters:
                if not isinstance(subchapter_obj, dict):
                    continue
                
                subchapter_name = subchapter_obj.get("subchapter", "")
                topics = subchapter_obj.get("topics", [])
                
                for topic_obj in topics:
                    if not isinstance(topic_obj, dict):
                        continue
                    
                    topic_name = topic_obj.get("topic", "")
                    extractions = topic_obj.get("extractions", [])
                    
                    # For each extraction, add a mapping entry
                    # If extractions is a list, create one mapping per extraction
                    if isinstance(extractions, list):
                        for extraction in extractions:
                            mapping.append({
                                "chapter": chapter_name,
                                "subchapter": subchapter_name,
                                "topic": topic_name
                            })
                    else:
                        # If extractions is not a list, create one mapping
                        mapping.append({
                            "chapter": chapter_name,
                            "subchapter": subchapter_name,
                            "topic": topic_name
                        })
        
        return mapping
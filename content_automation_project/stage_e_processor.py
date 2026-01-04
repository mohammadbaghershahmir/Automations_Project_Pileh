"""
Stage E Processor: Image Notes Processing
Takes Stage 4 JSON (with PointId) and Stage 1 JSON, generates image notes,
and merges them with Stage 4 data.
"""

import json
import logging
import math
import os
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
        
        # Prepare OCR Extraction JSON string (Stage 1) - contains figure and table descriptions
        _progress("Preparing OCR Extraction JSON (Stage 1)...")
        ocr_extraction_json_str = json.dumps(ocr_extraction_data, ensure_ascii=False, indent=2)
        
        # Prepare Stage 4 JSON string (complete - send full file to model)
        _progress("Preparing Stage 4 JSON (complete)...")
        stage4_json_str = json.dumps(stage4_points, ensure_ascii=False, indent=2)
        
        # Process Stage 4 as a single unit (no subchapter division)
        base_dir = os.path.dirname(stage4_path) or os.getcwd()
        base_name, _ = os.path.splitext(os.path.basename(stage4_path))
        max_retries = 2
        
        _progress("=" * 60)
        _progress("Processing Stage 4 as complete file (no subchapter division)...")
        _progress("=" * 60)
        
        # Prepare full prompt with both OCR Extraction JSON (Stage 1) and Stage 4 JSON
        full_prompt = f"""{prompt}

==================================================
فایل JSON متن درسی استخراج‌شده از کتاب درماتولوژی (OCR Extraction JSON):
==================================================
{ocr_extraction_json_str}

==================================================
فایل JSON ساختار سلسله‌مراتبی درسنامه نهایی (Stage 4 JSON - Complete - with PointId):
==================================================
{stage4_json_str}
"""
        
        # Call model with retry mechanism
        _progress(f"Calling model {model_name} for complete Stage 4...")
        response_text = None
        filepic_data = None
        
        txt_filename = f"{base_name}_stage_e.txt"
        txt_path = os.path.join(base_dir, txt_filename)
        all_txt_files = []  # Track TXT files created
        
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
                
                # Save response as TXT file first
                _progress("Saving model response as TXT file...")
                try:
                    with open(txt_path, "w", encoding="utf-8") as f:
                        f.write(response_text)
                    self.logger.info(f"Stage E raw text saved: {txt_path}")
                    all_txt_files.append(os.path.basename(txt_path))
                except Exception as e:
                    self.logger.error(f"Error saving TXT file: {e}")
                    continue
                
                # Try to extract JSON
                _progress("Converting TXT to JSON...")
                filepic_data = self.load_txt_as_json(txt_path)
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
            self.logger.error("No response from model after retries")
            _progress("Error: Failed to get response from model.")
            return None
        
        if not filepic_data:
            self.logger.error("Failed to extract JSON from TXT file after retries")
            _progress("Error: Failed to extract JSON from model response.")
            return None
        
        # Handle different JSON structures
        # Model might return array directly or object with data/rows/payload
        if isinstance(filepic_data, list):
            filepic_records = filepic_data
        elif isinstance(filepic_data, dict):
            # Try common keys: data, rows, payload
            filepic_records = filepic_data.get("data", filepic_data.get("rows", filepic_data.get("payload", [])))
            if not filepic_records:
                # Try to extract from nested structure - get first list value
                for value in filepic_data.values():
                    if isinstance(value, list):
                        filepic_records = value
                        break
                if not filepic_records:
                    filepic_records = []
            
            # Log the structure found
            if filepic_records:
                self.logger.info(f"Extracted {len(filepic_records)} records from JSON structure")
            else:
                self.logger.warning(f"JSON structure keys: {list(filepic_data.keys())}, but no records found")
        else:
            self.logger.error(f"Unexpected JSON structure from model: {type(filepic_data)}")
            _progress("Error: Unexpected JSON structure from model.")
            return None
        
        if not filepic_records:
            self.logger.error("No records found in filepic data")
            _progress("Error: No records found in model response.")
            return None
        
        _progress(f"Extracted {len(filepic_records)} image note records")
        
        _progress(f"Extracted {len(filepic_records)} image note records")
        
        # Process filepic records:
        # 1. Remove caption column
        # 2. Replace point_text with points from Stage 4
        # 3. Assign PointId sequentially
        
        _progress("Processing image notes...")
        processed_image_notes = []
        first_image_point_id = None
        
        # Create a mapping from Stage 4 points by their order/index
        # We'll match image notes to Stage 4 points by order
        stage4_points_by_index = {idx: point for idx, point in enumerate(stage4_points)}
        
        for idx, record in enumerate(filepic_records):
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
        
        # Prepare output directory (needed for filepic JSON)
        if not output_dir:
            output_dir = os.path.dirname(stage4_path) or os.getcwd()
        
        base_name, _ = os.path.splitext(os.path.basename(stage4_path))
        
        # Save filepic JSON (with caption) for use in Stage F
        # IMPORTANT: Save the ORIGINAL filepic_records (with caption) before processing
        _progress("Saving filepic JSON for Stage F...")
        filepic_json_path = os.path.join(output_dir, f"{base_name}_filepic.json")
        filepic_saved = False
        
        # Verify that filepic_records contain caption before saving
        caption_count = 0
        for record in filepic_records:
            if isinstance(record, dict) and record.get("caption"):
                caption_count += 1
        
        _progress(f"Found {caption_count} records with caption out of {len(filepic_records)}")
        
        try:
            filepic_metadata = {
                "book_id": book_id,
                "chapter_id": chapter_id,
                "total_records": len(filepic_records),
                "records_with_caption": caption_count,
                "source_txt": all_txt_files if all_txt_files else [],
                "division_method": "complete_file_no_division"
            }
            # Save original filepic_records with caption (before processing)
            success = self.save_json_file(filepic_records, filepic_json_path, filepic_metadata, "filepic")
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
                                # Log first record for debugging
                                self.logger.debug(f"First record: {json.dumps(first_record, ensure_ascii=False)[:200]}")
                except Exception as e:
                    self.logger.warning(f"Could not verify filepic JSON: {e}")
            else:
                self.logger.error("Failed to save filepic JSON")
        except Exception as e:
            self.logger.error(f"Error saving filepic JSON: {e}", exc_info=True)
            # Continue anyway, Stage F can try to load from TXT
        
        # Note: Stage 4 points already have chapter/subchapter fields, no need to add them
        # Note: topic for image notes is already extracted from model output (filepic_records)
        _progress("Stage 4 points already contain chapter/subchapter fields")
        
        # Merge Stage 4 points with image notes
        _progress("Merging Stage 4 points with image notes...")
        merged_points = stage4_points + processed_image_notes
        
        # Generate output filename
        output_filename = self.generate_filename("e", book_id, chapter_id)
        output_path = os.path.join(output_dir, output_filename)
        
        # Prepare metadata
        stage4_metadata = self.get_metadata_from_json(stage4_data)
        metadata = {
            "book_id": book_id,
            "chapter_id": chapter_id,
            "stage4_total_points": len(stage4_points),
            "image_notes_count": len(processed_image_notes),
            "first_image_point_id": first_image_point_id,
            "last_point_id": f"{book_id:03d}{chapter_id:03d}{(current_index - 1):04d}",
            "source_stage4": os.path.basename(stage4_path),
            "source_ocr_extraction": os.path.basename(ocr_extraction_json_path),
            "stage_e_txt_files": all_txt_files if all_txt_files else [],
            "total_stage4_records": len(stage4_points),
            "division_method": "complete_file_no_division",
            "filepic_json_file": os.path.basename(filepic_json_path) if filepic_saved else None,
            "filepic_json_path": filepic_json_path if filepic_saved else None,
            "model_used": model_name,
            **{k: v for k, v in stage4_metadata.items() if k not in ["stage", "processed_at", "total_records"]}
        }
        
        # Save merged JSON
        _progress(f"Saving merged JSON to: {output_path}")
        success = self.save_json_file(merged_points, output_path, metadata, "E")
        
        if success:
            _progress(f"Stage E completed successfully: {output_path}")
            return output_path
        else:
            self.logger.error("Failed to save Stage E output")
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
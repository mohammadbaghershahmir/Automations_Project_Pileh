"""
Stage F Processor: Image File Generation
Creates JSON file for images from Stage E data.
"""

import json
import logging
import os
import re
from typing import Optional, Dict, List, Any, Callable
from base_stage_processor import BaseStageProcessor


class StageFProcessor(BaseStageProcessor):
    """Process Stage F: Generate image file JSON"""
    
    def __init__(self, api_client):
        super().__init__(api_client)
        self.logger = logging.getLogger(__name__)
    
    def determine_image_type(self, file_name: str) -> int:
        """
        Determine image type from file name.
        Check for "Fig" or "Table" in the file name (including prefixes).
        - 2 if "Fig" or "تصویر" is in the name
        - 3 if "Table" or "جدول" is in the name
        - Default: 2
        
        Args:
            file_name: File name string (e.g., "تصویر e30:18" or "Table 5:2")
            
        Returns:
            Image type (2 or 3)
        """
        if not file_name:
            return 2
        
        file_name_lower = file_name.lower()
        # Check for Table first (more specific)
        if "table" in file_name_lower or "جدول" in file_name:
            return 3
        elif "fig" in file_name_lower or "تصویر" in file_name:
            return 2
        else:
            # Default to 2 (Figure)
            return 2
    
    def extract_file_name_from_points(self, points_value: str) -> str:
        """
        Extract and transform file name from Points column value.
        Transformations:
        - If "تصویر" is in the name: replace with "Fig" and ":" with "_"
        - If "جدول" is in the name: replace with "Table" and ":" with "_"
        - Remove all spaces and extra characters, keeping only prefix, numbers, and "_"
        Examples:
        - "تصویر 30:19" → "Fig30_19"
        - "تصویر e30:18" → "Fig30_18"
        - "جدول 30:3" → "Table30_3"
        
        Args:
            points_value: Value from Points column (e.g., "تصویر 30:19" or "جدول 30:3")
            
        Returns:
            Transformed file name (e.g., "Fig30_19" or "Table30_3")
        """
        if not points_value:
            return ""
        
        file_name = points_value.strip()
        prefix = ""
        
        # Check for "تصویر" (Persian for "Figure")
        if "تصویر" in file_name:
            prefix = "Fig"
            # Remove "تصویر" from the string
            file_name = file_name.replace("تصویر", "")
        
        # Check for "جدول" (Persian for "Table")
        elif "جدول" in file_name:
            prefix = "Table"
            # Remove "جدول" from the string
            file_name = file_name.replace("جدول", "")
        
        # Check if already has English prefix
        elif "Fig" in file_name or "fig" in file_name.lower():
            prefix = "Fig"
            # Remove "Fig" or "fig" from the string
            file_name = re.sub(r'[Ff]ig\s*', '', file_name)
        
        elif "Table" in file_name or "table" in file_name.lower():
            prefix = "Table"
            # Remove "Table" or "table" from the string
            file_name = re.sub(r'[Tt]able\s*', '', file_name)
        
        # If no prefix found, default to "Fig"
        if not prefix:
            prefix = "Fig"
        
        # Replace ":" with "_"
        file_name = file_name.replace(":", "_")
        
        # Extract only numbers and "_" (remove all other characters including spaces, letters like "e")
        # Keep the pattern: numbers_numbers (e.g., "30_19" or "30_3")
        file_name = re.sub(r'[^\d_]', '', file_name)
        
        # Combine prefix with cleaned numbers
        result = prefix + file_name
        
        return result
    
    def process_stage_f(
        self,
        stage_e_path: str,
        output_dir: Optional[str] = None,
        progress_callback: Optional[Callable[[str], None]] = None
    ) -> Optional[str]:
        """
        Process Stage F: Generate image file JSON from Stage E.
        
        Args:
            stage_e_path: Path to Stage E JSON file
            output_dir: Output directory (defaults to stage_e_path directory)
            progress_callback: Optional callback for progress updates
            
        Returns:
            Path to output file (f.json) or None on error
        """
        def _progress(msg: str):
            if progress_callback:
                progress_callback(msg)
            self.logger.info(msg)
        
        _progress("Starting Stage F processing...")
        
        # Load Stage E JSON
        _progress("Loading Stage E JSON...")
        stage_e_data = self.load_json_file(stage_e_path)
        if not stage_e_data:
            self.logger.error("Failed to load Stage E JSON")
            return None
        
        # Get metadata and data
        metadata = self.get_metadata_from_json(stage_e_data)
        stage_e_points = self.get_data_from_json(stage_e_data)
        
        if not stage_e_points:
            self.logger.error("Stage E JSON has no data/points")
            return None
        
        # Get first_image_point_id from metadata
        first_image_point_id = metadata.get("first_image_point_id")
        if not first_image_point_id:
            self.logger.error("No first_image_point_id found in Stage E metadata")
            return None
        
        _progress(f"First image PointId: {first_image_point_id}")
        
        # Find the index where images start
        image_start_index = None
        for idx, point in enumerate(stage_e_points):
            if point.get("PointId") == first_image_point_id:
                image_start_index = idx
                break
        
        if image_start_index is None:
            self.logger.error(f"Could not find point with PointId {first_image_point_id} in Stage E data")
            return None
        
        _progress(f"Found {len(stage_e_points) - image_start_index} image records starting from index {image_start_index}")
        
        # Load filepic JSON to get descriptions (from caption column)
        base_dir = os.path.dirname(stage_e_path) or os.getcwd()
        base_name, _ = os.path.splitext(os.path.basename(stage_e_path))
        
        # Try to get filepic path from metadata first
        filepic_json_path = None
        filepic_json_file = metadata.get("filepic_json_file")
        filepic_json_path_from_meta = metadata.get("filepic_json_path")
        
        if filepic_json_path_from_meta and os.path.exists(filepic_json_path_from_meta):
            filepic_json_path = filepic_json_path_from_meta
            _progress(f"Using filepic path from metadata: {filepic_json_path}")
        elif filepic_json_file:
            # Try relative to base_dir
            filepic_json_path = os.path.join(base_dir, filepic_json_file)
            if not os.path.exists(filepic_json_path):
                # Try with base_name prefix
                filepic_json_path = os.path.join(base_dir, f"{base_name}_filepic.json")
        else:
            # Fallback: construct from base_name
            filepic_json_path = os.path.join(base_dir, f"{base_name}_filepic.json")
        
        # Try to load from filepic JSON first (preferred)
        descriptions = []
        if filepic_json_path and os.path.exists(filepic_json_path):
            _progress("Loading descriptions from filepic JSON...")
            filepic_data = self.load_json_file(filepic_json_path)
            if filepic_data:
                filepic_records = self.get_data_from_json(filepic_data)
                _progress(f"Found {len(filepic_records)} records in filepic JSON")
                
                # Extract captions as descriptions (in order)
                for idx, record in enumerate(filepic_records):
                    if isinstance(record, dict):
                        # Get caption value - try different possible keys
                        caption = record.get("caption", "") or record.get("Caption", "") or record.get("CAPTION", "")
                        if caption:
                            descriptions.append(caption)
                            self.logger.debug(f"Record {idx}: Found caption ({len(caption)} chars)")
                        else:
                            # If no caption, add empty string to maintain order
                            descriptions.append("")
                            self.logger.debug(f"Record {idx}: No caption found. Keys: {list(record.keys())}")
                    else:
                        descriptions.append("")
                        self.logger.warning(f"Record {idx} is not a dict")
                
                _progress(f"Loaded {len(descriptions)} descriptions from filepic JSON")
                non_empty_count = sum(1 for d in descriptions if d)
                if non_empty_count > 0:
                    _progress(f"Found {non_empty_count} non-empty captions out of {len(descriptions)}")
                else:
                    self.logger.warning("All captions are empty in filepic JSON")
                    _progress("Warning: All captions are empty in filepic JSON")
        
        # Fallback: Try to load from TXT file if JSON doesn't exist or all descriptions are empty
        if not descriptions or not any(desc for desc in descriptions):
            _progress("Trying fallback: Loading from TXT file...")
            stage_e_txt_file = metadata.get("stage_e_txt_file")
            if stage_e_txt_file:
                stage_e_txt_path = os.path.join(base_dir, stage_e_txt_file)
                if not os.path.exists(stage_e_txt_path):
                    # Try to find TXT file in parent directory or current directory
                    possible_paths = [
                        stage_e_txt_path,
                        os.path.join(os.path.dirname(base_dir), stage_e_txt_file),
                        os.path.join(os.getcwd(), stage_e_txt_file)
                    ]
                    for path in possible_paths:
                        if os.path.exists(path):
                            stage_e_txt_path = path
                            break
                
                if os.path.exists(stage_e_txt_path):
                    _progress(f"Loading descriptions from Stage E TXT file: {os.path.basename(stage_e_txt_path)}")
                    filepic_data = self.load_txt_as_json(stage_e_txt_path)
                    if filepic_data:
                        # Extract records from filepic
                        if isinstance(filepic_data, list):
                            filepic_records = filepic_data
                        elif isinstance(filepic_data, dict):
                            filepic_records = filepic_data.get("data", filepic_data.get("rows", []))
                        else:
                            filepic_records = []
                        
                        _progress(f"Found {len(filepic_records)} records in filepic TXT")
                        
                        # Extract captions as descriptions (in order)
                        for idx, record in enumerate(filepic_records):
                            if isinstance(record, dict):
                                # Get caption value - try different possible keys
                                caption = record.get("caption", "") or record.get("Caption", "") or record.get("CAPTION", "")
                                if caption:
                                    descriptions.append(caption)
                                    self.logger.debug(f"Record {idx}: Found caption ({len(caption)} chars)")
                                else:
                                    # If no caption, add empty string to maintain order
                                    descriptions.append("")
                                    self.logger.debug(f"Record {idx}: No caption found. Keys: {list(record.keys())}")
                            else:
                                descriptions.append("")
                                self.logger.warning(f"Record {idx} is not a dict")
                        
                        _progress(f"Loaded {len(descriptions)} descriptions from TXT file")
                        non_empty_count = sum(1 for d in descriptions if d)
                        if non_empty_count > 0:
                            _progress(f"Found {non_empty_count} non-empty captions out of {len(descriptions)}")
                        else:
                            self.logger.warning("All captions are empty in filepic TXT")
                            _progress("Warning: All captions are empty in filepic TXT")
        
        if not descriptions:
            self.logger.warning("No descriptions found in filepic. Descriptions will be empty.")
        
        # Process image points
        _progress("Processing image records...")
        image_records = []
        
        for idx, point in enumerate(stage_e_points[image_start_index:], start=image_start_index):
            point_id = point.get("PointId", "")
            points_value = point.get("Points", point.get("points", ""))
            
            if not point_id or not points_value:
                continue
            
            # Extract file name from Points
            file_name = self.extract_file_name_from_points(points_value)
            
            # Determine image type
            image_type = self.determine_image_type(file_name)
            
            # Get description (in order from filepic)
            description = ""
            desc_index = idx - image_start_index
            if desc_index < len(descriptions):
                description = descriptions[desc_index]
            
            # Create image record
            image_record = {
                "point_id": point_id,
                "file_name": file_name,
                "image_type": image_type,
                "display_level": 6,
                "description": description,
                "question": "",  # Empty as specified
                "is_title": ""   # Empty as specified
            }
            
            image_records.append(image_record)
        
        if not image_records:
            self.logger.error("No image records generated")
            return None
        
        _progress(f"Generated {len(image_records)} image records")
        
        # Prepare output directory
        if not output_dir:
            output_dir = os.path.dirname(stage_e_path) or os.getcwd()
        
        # Generate output filename
        output_filename = "f.json"
        output_path = os.path.join(output_dir, output_filename)
        
        # Prepare metadata
        output_metadata = {
            "source_stage_e": os.path.basename(stage_e_path),
            "first_image_point_id": first_image_point_id,
            "total_images": len(image_records),
            "image_start_index": image_start_index
        }
        
        # Save JSON
        _progress(f"Saving image file JSON to: {output_path}")
        success = self.save_json_file(image_records, output_path, output_metadata, "F")
        
        if success:
            _progress(f"Stage F completed successfully: {output_path}")
            return output_path
        else:
            self.logger.error("Failed to save Stage F output")
            return None


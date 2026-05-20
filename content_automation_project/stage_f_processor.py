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
    
    def _is_media_points(self, points_value: str) -> bool:
        """True when Points looks like an image or table reference."""
        if not points_value:
            return False
        pv = points_value.strip()
        pl = pv.lower()
        return any(
            token in pv or token in pl
            for token in ("تصویر", "جدول", "fig", "table", "الکترونیکی")
        )

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
        - If "تصویر" is in the name: replace with "Fig" and ":" with "-"
        - If "جدول" is in the name: replace with "Table" and ":" with "-"
        - Keep space between prefix and numbers
        - Preserve prefix letters (like "e") before numbers
        Examples:
        - "تصویر 30:19" → "Fig 30-19"
        - "تصویر e30:15" → "eFig 30-15"
        - "جدول 30:3" → "Table 30-3"
        - "جدول e30:3" → "eTable 30-3"
        
        Args:
            points_value: Value from Points column (e.g., "تصویر 30:19" or "جدول e30:3")
            
        Returns:
            Transformed file name (e.g., "Fig 30-19" or "eFig 30-15")
        """
        if not points_value:
            return ""
        
        file_name = points_value.strip()
        prefix = ""
        letter_prefix = ""  # For letters like "e" before numbers
        
        # Check for "تصویر" (Persian for "Figure")
        if "تصویر" in file_name:
            prefix = "Fig"
            # Remove "تصویر" from the string
            file_name = file_name.replace("تصویر", "").strip()
        
        # Check for "جدول" (Persian for "Table")
        elif "جدول" in file_name:
            prefix = "Table"
            # Remove "جدول" from the string
            file_name = file_name.replace("جدول", "").strip()
        
        # Check if already has English prefix
        elif "Fig" in file_name or "fig" in file_name.lower():
            prefix = "Fig"
            # Remove "Fig" or "fig" from the string
            file_name = re.sub(r'[Ff]ig\s*', '', file_name).strip()
        
        elif "Table" in file_name or "table" in file_name.lower():
            prefix = "Table"
            # Remove "Table" or "table" from the string
            file_name = re.sub(r'[Tt]able\s*', '', file_name).strip()
        
        # If no prefix found, default to "Fig"
        if not prefix:
            prefix = "Fig"
        
        # Check for letter prefix (like "e" before numbers)
        # Pattern: letter(s) followed by number (e.g., "e30:15" or "e 30:15")
        letter_match = re.match(r'^([a-zA-Z]+)\s*(\d)', file_name)
        if letter_match:
            letter_prefix = letter_match.group(1)
            file_name = file_name[len(letter_prefix):].strip()
        
        # Replace all colon-like and point/dot characters with "-" (ASCII ":", fullwidth "：", ".", etc.)
        # Otherwise a fullwidth colon or dot gets stripped by the next regex and "13：44" or "13.44" becomes "1344"
        for char in (":", "：", "︰", "﹕", ".", "．", "۔", "·"):
            file_name = file_name.replace(char, "-")
        
        # Extract numbers and "-" (remove all other characters except spaces)
        # Keep the pattern: numbers-numbers (e.g., "30-19" or "30-3")
        # First, replace multiple spaces with single space
        file_name = re.sub(r'\s+', ' ', file_name)
        # Extract numbers, dashes, and spaces
        file_name = re.sub(r'[^\d\s-]', '', file_name)
        # Clean up: remove leading/trailing spaces and normalize dashes
        file_name = file_name.strip()
        # Replace multiple dashes with single dash
        file_name = re.sub(r'-+', '-', file_name)
        
        # Combine: letter_prefix + prefix + space + numbers
        if letter_prefix:
            result = f"{letter_prefix}{prefix} {file_name}"
        else:
            result = f"{prefix} {file_name}"
        
        return result

    def _resolve_sidecar_json_path(
        self,
        base_dir: str,
        base_name: str,
        metadata: Dict[str, Any],
        *,
        file_meta_key: str,
        path_meta_key: str,
        default_suffix: str,
        override_path: Optional[str] = None,
    ) -> Optional[str]:
        if override_path and os.path.isfile(override_path):
            return override_path

        path_from_meta = metadata.get(path_meta_key)
        if path_from_meta and os.path.isfile(path_from_meta):
            return path_from_meta

        file_from_meta = metadata.get(file_meta_key)
        if file_from_meta:
            candidate = os.path.join(base_dir, file_from_meta)
            if os.path.isfile(candidate):
                return candidate

        candidate = os.path.join(base_dir, f"{base_name}{default_suffix}")
        if os.path.isfile(candidate):
            return candidate
        return None

    def _load_pic_captions(
        self,
        pic_json_path: Optional[str],
        label: str,
        _progress: Callable[[str], None],
    ) -> List[str]:
        descriptions: List[str] = []
        if not pic_json_path or not os.path.isfile(pic_json_path):
            return descriptions

        _progress(f"Loading {label} captions from {os.path.basename(pic_json_path)}...")
        pic_data = self.load_json_file(pic_json_path)
        if not pic_data:
            self.logger.warning("Failed to load %s JSON: %s", label, pic_json_path)
            return descriptions

        pic_records = self.get_data_from_json(pic_data)
        _progress(f"Found {len(pic_records)} records in {label} JSON")

        for idx, record in enumerate(pic_records):
            if not isinstance(record, dict):
                descriptions.append("")
                continue
            caption = (
                record.get("caption", "")
                or record.get("Caption", "")
                or record.get("CAPTION", "")
            )
            descriptions.append(str(caption or ""))
            if caption:
                self.logger.debug("%s record %s: caption (%s chars)", label, idx, len(caption))

        non_empty = sum(1 for d in descriptions if d)
        _progress(f"Loaded {len(descriptions)} {label} captions ({non_empty} non-empty)")
        return descriptions
    
    def process_stage_f(
        self,
        stage_e_path: str,
        output_dir: Optional[str] = None,
        progress_callback: Optional[Callable[[str], None]] = None,
        filepic_json_path: Optional[str] = None,
        tablepic_json_path: Optional[str] = None,
    ) -> Optional[str]:
        """
        Process Stage F: Generate catalog JSON from merged notes (Stage TA) or Stage E.
        
        Args:
            stage_e_path: Path to Stage TA merged JSON (preferred) or Stage E JSON file
            output_dir: Output directory (defaults to stage_e_path directory)
            progress_callback: Optional callback for progress updates
            filepic_json_path: Optional override for image captions sidecar JSON
            tablepic_json_path: Optional override for table captions sidecar JSON
            
        Returns:
            Path to output file (f.json) or None on error
        """
        def _progress(msg: str):
            if progress_callback:
                progress_callback(msg)
            self.logger.info(msg)
        
        # Set stage if using UnifiedAPIClient (for API routing)
        if hasattr(self.api_client, 'set_stage'):
            self.api_client.set_stage("stage_f")
        
        _progress("Starting Stage F processing...")
        
        _progress("Loading notes JSON (Stage TA merged or Stage E)...")
        notes_data = self.load_json_file(stage_e_path)
        if not notes_data:
            self.logger.error("Failed to load notes JSON")
            return None
        
        metadata = self.get_metadata_from_json(notes_data)
        all_points = self.get_data_from_json(notes_data)
        
        if not all_points:
            self.logger.error("Notes JSON has no data/points")
            return None
        
        first_image_point_id = metadata.get("first_image_point_id")
        if not first_image_point_id:
            self.logger.error("No first_image_point_id found in notes metadata")
            return None
        
        _progress(f"First image PointId: {first_image_point_id}")
        if metadata.get("first_table_point_id"):
            _progress(f"First table PointId: {metadata.get('first_table_point_id')}")
        
        image_start_index = None
        for idx, point in enumerate(all_points):
            if point.get("PointId") == first_image_point_id:
                image_start_index = idx
                break
        
        if image_start_index is None:
            self.logger.error(f"Could not find point with PointId {first_image_point_id} in notes data")
            return None
        
        base_dir = os.path.dirname(stage_e_path) or os.getcwd()
        base_name, _ = os.path.splitext(os.path.basename(stage_e_path))
        
        resolved_filepic = self._resolve_sidecar_json_path(
            base_dir,
            base_name,
            metadata,
            file_meta_key="filepic_json_file",
            path_meta_key="filepic_json_path",
            default_suffix="_filepic.json",
            override_path=filepic_json_path,
        )
        resolved_tablepic = self._resolve_sidecar_json_path(
            base_dir,
            base_name,
            metadata,
            file_meta_key="tablepic_json_file",
            path_meta_key="tablepic_json_path",
            default_suffix="_tablepic.json",
            override_path=tablepic_json_path,
        )

        image_descriptions = self._load_pic_captions(resolved_filepic, "filepic", _progress)
        table_descriptions = self._load_pic_captions(resolved_tablepic, "tablepic", _progress)

        if not image_descriptions and not table_descriptions:
            stage_e_txt_file = metadata.get("stage_e_txt_file")
            if stage_e_txt_file:
                stage_e_txt_path = os.path.join(base_dir, stage_e_txt_file)
                if os.path.isfile(stage_e_txt_path):
                    _progress(f"Trying fallback captions from TXT: {os.path.basename(stage_e_txt_path)}")
                    filepic_data = self.load_txt_as_json(stage_e_txt_path)
                    if isinstance(filepic_data, list):
                        image_descriptions = [
                            str(r.get("caption", "") or r.get("Caption", "") or "")
                            if isinstance(r, dict)
                            else ""
                            for r in filepic_data
                        ]
        
        _progress("Processing image and table records...")
        image_records = []
        image_desc_index = 0
        table_desc_index = 0
        
        for point in all_points[image_start_index:]:
            point_id = point.get("PointId", "")
            points_value = point.get("Points", point.get("points", ""))
            
            if not point_id or not points_value or not self._is_media_points(points_value):
                continue
            
            file_name = self.extract_file_name_from_points(points_value)
            image_type = self.determine_image_type(points_value)
            if image_type == 3:
                description = ""
                if table_desc_index < len(table_descriptions):
                    description = table_descriptions[table_desc_index]
                table_desc_index += 1
            else:
                description = ""
                if image_desc_index < len(image_descriptions):
                    description = image_descriptions[image_desc_index]
                image_desc_index += 1
            
            image_records.append(
                {
                    "point_id": point_id,
                    "file_name": file_name,
                    "image_type": image_type,
                    "display_level": 6,
                    "description": description,
                    "question": "",
                    "is_title": "",
                }
            )
        
        if not image_records:
            self.logger.error("No image/table records generated")
            return None
        
        _progress(
            f"Generated {len(image_records)} catalog records "
            f"({image_desc_index} images, {table_desc_index} tables)"
        )
        
        # Prepare output directory
        if not output_dir:
            output_dir = os.path.dirname(stage_e_path) or os.getcwd()
        
        # Generate unique output filename based on Stage E filename
        stage_e_basename = os.path.basename(stage_e_path)
        stage_e_name_without_ext = os.path.splitext(stage_e_basename)[0]
        # Create unique filename: f_{stage_e_name}.json
        # Example: e105001.json -> f_e105001.json
        output_filename = f"f_{stage_e_name_without_ext}.json"
        output_path = os.path.join(output_dir, output_filename)
        
        # Prepare metadata
        output_metadata = {
            "source_notes_json": os.path.basename(stage_e_path),
            "source_stage_e": os.path.basename(stage_e_path),
            "first_image_point_id": first_image_point_id,
            "first_table_point_id": metadata.get("first_table_point_id"),
            "total_images": len(image_records),
            "image_start_index": image_start_index,
            "image_caption_count": image_desc_index,
            "table_caption_count": table_desc_index,
            "filepic_json_file": os.path.basename(resolved_filepic) if resolved_filepic else None,
            "tablepic_json_file": os.path.basename(resolved_tablepic) if resolved_tablepic else None,
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


"""
Stage J Processor: Add Imp & Type
Adds two new columns (Imp and Type) to Stage E data based on Word test file and prompt.
"""

import json
import logging
import math
import os
import tempfile
from typing import Optional, Dict, List, Any, Callable
from base_stage_processor import BaseStageProcessor
from word_file_processor import WordFileProcessor
from api_layer import APIConfig


class StageJProcessor(BaseStageProcessor):
    """Process Stage J: Add Imp and Type columns to Stage E data"""
    
    def __init__(self, api_client):
        super().__init__(api_client)
        self.logger = logging.getLogger(__name__)
        self.word_processor = WordFileProcessor()
    
    def process_stage_j(
        self,
        stage_e_path: str,
        word_file_path: str,
        stage_f_path: Optional[str] = None,
        prompt: str = "",
        model_name: str = "",
        output_dir: Optional[str] = None,
        progress_callback: Optional[Callable[[str], None]] = None
    ) -> Optional[str]:
        """
        Process Stage J: Add Imp and Type columns to Stage E data.
        
        Args:
            stage_e_path: Path to Stage E JSON file (e{book}{chapter}.json)
            word_file_path: Path to Word file containing tests
            stage_f_path: Optional path to Stage F JSON file (f.json)
            prompt: User prompt for Imp and Type generation
            model_name: Gemini model name
            output_dir: Output directory (defaults to stage_e_path directory)
            progress_callback: Optional callback for progress updates
            
        Returns:
            Path to output file (a{book}{chapter}.json) or None on error
        """
        def _progress(msg: str):
            if progress_callback:
                progress_callback(msg)
            self.logger.info(msg)
        
        _progress("Starting Stage J processing...")
        
        # Load Stage E JSON
        _progress("Loading Stage E JSON...")
        stage_e_data = self.load_json_file(stage_e_path)
        if not stage_e_data:
            self.logger.error("Failed to load Stage E JSON")
            return None
        
        # Get data from Stage E
        stage_e_records = self.get_data_from_json(stage_e_data)
        if not stage_e_records:
            self.logger.error("Stage E JSON has no data")
            return None
        
        _progress(f"Loaded {len(stage_e_records)} records from Stage E")
        
        # Extract book and chapter from first PointId
        first_point_id = stage_e_records[0].get("PointId")
        if not first_point_id:
            self.logger.error("No PointId found in Stage E data")
            return None
        
        try:
            book_id, chapter_id = self.extract_book_chapter_from_pointid(first_point_id)
        except ValueError as e:
            self.logger.error(f"Error extracting book/chapter: {e}")
            return None
        
        _progress(f"Detected Book ID: {book_id}, Chapter ID: {chapter_id}")
        
        # Read Word file
        _progress("Reading Word file...")
        word_content = self.word_processor.read_word_file(word_file_path)
        if not word_content:
            self.logger.error("Failed to read Word file")
            return None
        
        _progress(f"Read Word file: {len(word_content)} characters")
        
        # Load Stage F JSON if provided
        stage_f_data = None
        stage_f_records = []
        if stage_f_path and os.path.exists(stage_f_path):
            _progress("Loading Stage F JSON...")
            stage_f_data = self.load_json_file(stage_f_path)
            if stage_f_data:
                stage_f_records = self.get_data_from_json(stage_f_data)
                _progress(f"Loaded {len(stage_f_records)} records from Stage F")
            else:
                self.logger.warning("Failed to load Stage F JSON, continuing without it")
        else:
            _progress("No Stage F JSON provided, continuing without it")
        
        # Prepare data for model
        # Create a simplified version of Stage E data for model input
        # Include only necessary columns: PointId and the 6 columns from filepic
        model_input_data = []
        for record in stage_e_records:
            model_record = {
                "PointId": record.get("PointId", ""),
                "chapter": record.get("chapter", ""),
                "subchapter": record.get("subchapter", ""),
                "topic": record.get("topic", ""),
                "subtopic": record.get("subtopic", ""),
                "subsubtopic": record.get("subsubtopic", ""),
                "Points": record.get("Points", record.get("points", ""))
            }
            model_input_data.append(model_record)
        
        # Divide Stage E data into parts of 200 records each
        PART_SIZE = 200
        total_records = len(model_input_data)
        num_parts = math.ceil(total_records / PART_SIZE)
        
        _progress(f"Dividing Stage E data into {num_parts} parts (max {PART_SIZE} records per part)")
        
        # Split Stage E data into parts
        model_input_parts = []
        for i in range(num_parts):
            start_idx = i * PART_SIZE
            end_idx = min((i + 1) * PART_SIZE, total_records)
            part_data = model_input_data[start_idx:end_idx]
            model_input_parts.append(part_data)
            _progress(f"Part {i+1}: {len(part_data)} records (indices {start_idx} to {end_idx-1})")
        
        word_content_formatted = self.word_processor.prepare_word_for_model(
            word_content, 
            context="Test Questions"
        )
        
        # Prepare Stage F JSON string if available
        stage_f_json_str = ""
        if stage_f_records:
            stage_f_json_str = json.dumps(stage_f_records, ensure_ascii=False, indent=2)
        
        # Prepare base prompt template
        base_prompt_template = f"""{prompt}

Word Document (Test Questions):
{word_content_formatted}
"""
        
        # Add Stage F data to prompt if available
        if stage_f_json_str:
            base_prompt_template += f"""
Stage F Data (Image Files JSON):
{stage_f_json_str}

"""
        
        base_prompt_template += """Please analyze the Stage E data and the Word document"""
        if stage_f_json_str:
            base_prompt_template += ", and Stage F data (image files)"
        base_prompt_template += """, and generate a JSON response with the following structure:
{{
  "data": [
    {{
      "PointId": "point_id_string",
      "Imp": "importance_value",
      "Type": "type_value"
    }},
    ...
  ]
}}

IMPORTANT: Use EXACT field names: "PointId" (not "point_id"), "Imp" (not "importance_level"), "Type" (not "point_type").
PointId must be a STRING matching exactly the PointId from Stage E data.

For each PointId in the Stage E data, provide:
- Imp: Importance level (based on the test questions and content analysis)
- Type: Type classification (based on the content and test questions)

Return ONLY valid JSON, no additional text."""
        
        # Process each part separately
        all_part_responses = []
        max_retries = 3
        
        for part_num, part_data in enumerate(model_input_parts, 1):
            _progress("=" * 60)
            _progress(f"Processing Part {part_num}/{num_parts} ({len(part_data)} records)...")
            _progress("=" * 60)
            
            part_json_str = json.dumps(part_data, ensure_ascii=False, indent=2)
            part_prompt = f"""{base_prompt_template}

Stage E Data - Part {part_num}/{num_parts} (JSON):
{part_json_str}"""
            
            part_response = None
            for attempt in range(max_retries):
                try:
                    part_response = self.api_client.process_text(
                        text=part_prompt,
                        system_prompt=None,
                        model_name=model_name,
                        temperature=APIConfig.DEFAULT_TEMPERATURE,
                        max_tokens=APIConfig.DEFAULT_MAX_TOKENS
                    )
                    if part_response:
                        _progress(f"Part {part_num} response received ({len(part_response)} characters)")
                        break
                except Exception as e:
                    self.logger.warning(f"Part {part_num} model call attempt {attempt + 1} failed: {e}")
                    if attempt < max_retries - 1:
                        _progress(f"Retrying Part {part_num}... (attempt {attempt + 2}/{max_retries})")
                    else:
                        self.logger.error(f"All Part {part_num} model call attempts failed")
                        _progress(f"Error: Failed to get response for Part {part_num}. Skipping this part.")
                        part_response = None
            
            if not part_response:
                self.logger.error(f"No response from model for Part {part_num}")
                _progress(f"Error: No response for Part {part_num}. Skipping this part.")
                continue
            
            all_part_responses.append({
                "part_num": part_num,
                "response": part_response
            })
        
        if not all_part_responses:
            self.logger.error("No responses received from any part")
            return None
        
        # Combine all responses into one TXT file
        _progress("Combining responses from all parts...")
        combined_response_parts = []
        for part_info in all_part_responses:
            combined_response_parts.append(f"=== PART {part_info['part_num']} RESPONSE ===\n{part_info['response']}")
        combined_response = "\n\n".join(combined_response_parts)
        
        # Save combined response to TXT file
        base_dir = os.path.dirname(stage_e_path) or os.getcwd()
        base_name, _ = os.path.splitext(os.path.basename(stage_e_path))
        txt_path = os.path.join(base_dir, f"{base_name}_stage_j.txt")
        
        try:
            with open(txt_path, 'w', encoding='utf-8') as f:
                f.write(combined_response)
            _progress(f"Saved combined response to: {txt_path}")
        except Exception as e:
            self.logger.warning(f"Failed to save TXT file: {e}")
        
        # Extract JSON from all responses
        all_part_data_lists = []
        for part_info in all_part_responses:
            part_num = part_info['part_num']
            part_response = part_info['response']
            
            _progress(f"Extracting JSON from Part {part_num} response...")
            part_output = self.extract_json_from_response(part_response)
            if not part_output:
                # Try loading from text directly
                _progress(f"Trying to extract Part {part_num} JSON from text...")
                part_output = self.load_txt_as_json_from_text(part_response)
            
            if part_output:
                part_data_list = self.get_data_from_json(part_output)
                if part_data_list:
                    all_part_data_lists.append({
                        "part_num": part_num,
                        "data": part_data_list,
                        "count": len(part_data_list)
                    })
                    _progress(f"Part {part_num}: Extracted {len(part_data_list)} records")
                else:
                    self.logger.warning(f"Part {part_num}: No data extracted from JSON")
            else:
                self.logger.warning(f"Part {part_num}: Failed to extract JSON")
        
        # Combine all outputs
        _progress("Combining JSON from all parts...")
        combined_model_data = []
        for part_info in all_part_data_lists:
            combined_model_data.extend(part_info['data'])
        
        part_counts = [f"Part {p['part_num']}: {p['count']}" for p in all_part_data_lists]
        _progress(f"Extracted {len(combined_model_data)} records from combined model output ({', '.join(part_counts)})")
        
        if not combined_model_data:
            self.logger.error("Failed to extract JSON from model responses")
            # Try loading from TXT file as fallback - split by parts and extract complete objects
            _progress("Trying to load JSON from TXT file as fallback (splitting by parts)...")
            try:
                with open(txt_path, 'r', encoding='utf-8') as f:
                    txt_content = f.read()
                
                # Split by double newline to get individual parts
                parts = txt_content.split('\n\n')
                self.logger.info(f"Found {len(parts)} parts in TXT file")
                
                all_parts_data = []
                for part_idx, part_text in enumerate(parts, 1):
                    if not part_text.strip():
                        continue
                    
                    _progress(f"Extracting JSON from TXT Part {part_idx}...")
                    # First try extract_json_from_response
                    part_json = self.extract_json_from_response(part_text)
                    if not part_json:
                        # Then try load_txt_as_json_from_text
                        part_json = self.load_txt_as_json_from_text(part_text)
                    
                    if part_json:
                        part_data = self.get_data_from_json(part_json)
                        if part_data:
                            all_parts_data.extend(part_data)
                            self.logger.info(f"TXT Part {part_idx}: Extracted {len(part_data)} records")
                        else:
                            self.logger.warning(f"TXT Part {part_idx}: No data extracted from JSON")
                    else:
                        # Last resort: use txt_stage_json_utils which can extract complete objects from incomplete JSON
                        _progress(f"Trying robust extraction for TXT Part {part_idx}...")
                        from txt_stage_json_utils import load_stage_txt_as_json
                        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False, encoding='utf-8') as tmp_file:
                            tmp_file.write(part_text)
                            tmp_path = tmp_file.name
                        
                        try:
                            part_json = load_stage_txt_as_json(tmp_path)
                            if part_json:
                                part_data = self.get_data_from_json(part_json)
                                if part_data:
                                    all_parts_data.extend(part_data)
                                    self.logger.info(f"TXT Part {part_idx}: Extracted {len(part_data)} records using robust extraction")
                        finally:
                            try:
                                os.unlink(tmp_path)
                            except:
                                pass
                
                if all_parts_data:
                    combined_model_data = all_parts_data
                    _progress(f"Extracted {len(combined_model_data)} records from TXT file fallback")
            except Exception as e:
                self.logger.error(f"Error loading from TXT file: {e}")
        
        if not combined_model_data:
            self.logger.error("Failed to extract JSON from model responses")
            return None
        
        model_data = combined_model_data
        
        
        # Create a mapping of PointId to Imp and Type
        pointid_to_imp_type = {}
        for record in model_data:
            # Try different field names for PointId (PointId, point_id)
            point_id = record.get("PointId", "") or record.get("point_id", "")
            
            # Convert to string if it's a number
            if point_id:
                point_id = str(point_id)
                
                # Try different field names for Imp (Imp, importance_level)
                imp_value = record.get("Imp", "") or record.get("importance_level", "")
                # Convert to string if it's a number
                if imp_value:
                    imp_value = str(imp_value)
                
                # Try different field names for Type (Type, point_type)
                type_value = record.get("Type", "") or record.get("point_type", "")
                # Convert to string if it's a number
                if type_value:
                    type_value = str(type_value)
                
                pointid_to_imp_type[point_id] = {
                    "Imp": imp_value,
                    "Type": type_value
                }
        
        _progress(f"Created mapping for {len(pointid_to_imp_type)} PointIds")
        if len(pointid_to_imp_type) > 0:
            # Log first few mappings for debugging
            sample_keys = list(pointid_to_imp_type.keys())[:3]
            for key in sample_keys:
                self.logger.debug(f"Mapping: {key} -> Imp: {pointid_to_imp_type[key].get('Imp')}, Type: {pointid_to_imp_type[key].get('Type')}")
        
        # Merge Stage E data with Imp and Type columns
        _progress("Merging Stage E data with Imp and Type columns...")
        merged_records = []
        
        matched_count = 0
        for record in stage_e_records:
            point_id = record.get("PointId", "")
            # Convert to string to ensure matching
            if point_id:
                point_id = str(point_id)
            
            # Get Imp and Type from model output
            imp_type_data = pointid_to_imp_type.get(point_id, {"Imp": "", "Type": ""})
            
            # Track matches for debugging
            if imp_type_data.get("Imp") or imp_type_data.get("Type"):
                matched_count += 1
            
            # Create merged record with 8 columns (6 from Stage E + 2 new)
            merged_record = {
                "PointId": point_id,
                "chapter": record.get("chapter", ""),
                "subchapter": record.get("subchapter", ""),
                "topic": record.get("topic", ""),
                "subtopic": record.get("subtopic", ""),
                "subsubtopic": record.get("subsubtopic", ""),
                "Points": record.get("Points", record.get("points", "")),
                "Imp": imp_type_data.get("Imp", ""),
                "Type": imp_type_data.get("Type", "")
            }
            
            merged_records.append(merged_record)
        
        _progress(f"Merged {len(merged_records)} records")
        _progress(f"Matched {matched_count} records with Imp/Type data")
        if matched_count == 0:
            self.logger.warning("No records matched! Check PointId format in model output vs Stage E")
        
        # Prepare output directory
        if not output_dir:
            output_dir = os.path.dirname(stage_e_path) or os.getcwd()
        
        # Generate output filename: a{book}{chapter}.json
        output_path = self.generate_filename("a", book_id, chapter_id, output_dir)
        
        # Prepare metadata
        output_metadata = {
            "book_id": book_id,
            "chapter_id": chapter_id,
            "source_stage_e": os.path.basename(stage_e_path),
            "word_file": os.path.basename(word_file_path),
            "source_stage_f": os.path.basename(stage_f_path) if stage_f_path else None,
            "model_used": model_name,
            "stage_j_txt_file": os.path.basename(txt_path),
            "total_records": len(merged_records),
            "records_with_imp_type": len([r for r in merged_records if r.get("Imp") or r.get("Type")])
        }
        
        # Save JSON
        _progress(f"Saving Stage J output to: {output_path}")
        success = self.save_json_file(merged_records, output_path, output_metadata, "J")
        
        if success:
            _progress(f"Stage J completed successfully: {output_path}")
            return output_path
        else:
            self.logger.error("Failed to save Stage J output")
            return None



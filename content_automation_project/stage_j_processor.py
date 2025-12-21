"""
Stage J Processor: Add Imp & Type
Adds two new columns (Imp and Type) to Stage E data based on Word test file and prompt.
"""

import json
import logging
import os
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
        
        # Split Stage E data into two parts
        total_records = len(model_input_data)
        mid_point = total_records // 2
        part1_data = model_input_data[:mid_point]
        part2_data = model_input_data[mid_point:]
        
        _progress(f"Splitting Stage E data into 2 parts: Part 1 ({len(part1_data)} records), Part 2 ({len(part2_data)} records)")
        
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
        
        # Process Part 1
        _progress("Processing Part 1...")
        part1_json_str = json.dumps(part1_data, ensure_ascii=False, indent=2)
        part1_prompt = f"""{base_prompt_template}

Stage E Data - Part 1 (JSON):
{part1_json_str}"""
        
        part1_response = None
        max_retries = 3
        for attempt in range(max_retries):
            try:
                part1_response = self.api_client.process_text(
                    text=part1_prompt,
                    system_prompt=None,
                    model_name=model_name,
                    temperature=APIConfig.DEFAULT_TEMPERATURE,
                    max_tokens=APIConfig.DEFAULT_MAX_TOKENS
                )
                if part1_response:
                    _progress(f"Part 1 response received ({len(part1_response)} characters)")
                    break
            except Exception as e:
                self.logger.warning(f"Part 1 model call attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    _progress(f"Retrying Part 1... (attempt {attempt + 2}/{max_retries})")
                else:
                    self.logger.error("All Part 1 model call attempts failed")
                    return None
        
        if not part1_response:
            self.logger.error("No response from model for Part 1")
            return None
        
        # Process Part 2
        _progress("Processing Part 2...")
        part2_json_str = json.dumps(part2_data, ensure_ascii=False, indent=2)
        part2_prompt = f"""{base_prompt_template}

Stage E Data - Part 2 (JSON):
{part2_json_str}"""
        
        part2_response = None
        for attempt in range(max_retries):
            try:
                part2_response = self.api_client.process_text(
                    text=part2_prompt,
                    system_prompt=None,
                    model_name=model_name,
                    temperature=APIConfig.DEFAULT_TEMPERATURE,
                    max_tokens=APIConfig.DEFAULT_MAX_TOKENS
                )
                if part2_response:
                    _progress(f"Part 2 response received ({len(part2_response)} characters)")
                    break
            except Exception as e:
                self.logger.warning(f"Part 2 model call attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    _progress(f"Retrying Part 2... (attempt {attempt + 2}/{max_retries})")
                else:
                    self.logger.error("All Part 2 model call attempts failed")
                    return None
        
        if not part2_response:
            self.logger.error("No response from model for Part 2")
            return None
        
        # Combine both responses into one TXT file
        _progress("Combining responses from both parts...")
        combined_response = f"""=== PART 1 RESPONSE ===
{part1_response}

=== PART 2 RESPONSE ===
{part2_response}"""
        
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
        
        # Extract JSON from both responses
        _progress("Extracting JSON from Part 1 response...")
        part1_output = self.extract_json_from_response(part1_response)
        if not part1_output:
            # Try loading from text directly
            _progress("Trying to extract Part 1 JSON from text...")
            part1_output = self.load_txt_as_json_from_text(part1_response)
        
        _progress("Extracting JSON from Part 2 response...")
        part2_output = self.extract_json_from_response(part2_response)
        if not part2_output:
            # Try loading from text directly
            _progress("Trying to extract Part 2 JSON from text...")
            part2_output = self.load_txt_as_json_from_text(part2_response)
        
        # Combine both outputs
        _progress("Combining JSON from both parts...")
        part1_data_list = self.get_data_from_json(part1_output) if part1_output else []
        part2_data_list = self.get_data_from_json(part2_output) if part2_output else []
        
        # Create combined model output
        combined_model_data = part1_data_list + part2_data_list
        
        if not combined_model_data:
            self.logger.error("Failed to extract JSON from model responses")
            # Try loading from TXT file as fallback
            _progress("Trying to load JSON from TXT file as fallback...")
            model_output = self.load_txt_as_json(txt_path)
            if model_output:
                combined_model_data = self.get_data_from_json(model_output)
        
        if not combined_model_data:
            self.logger.error("Failed to extract JSON from model responses")
            return None
        
        model_data = combined_model_data
        
        _progress(f"Extracted {len(model_data)} records from combined model output (Part 1: {len(part1_data_list)}, Part 2: {len(part2_data_list)})")
        
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



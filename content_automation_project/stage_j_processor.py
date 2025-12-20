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
        prompt: str,
        model_name: str,
        output_dir: Optional[str] = None,
        progress_callback: Optional[Callable[[str], None]] = None
    ) -> Optional[str]:
        """
        Process Stage J: Add Imp and Type columns to Stage E data.
        
        Args:
            stage_e_path: Path to Stage E JSON file (e{book}{chapter}.json)
            word_file_path: Path to Word file containing tests
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
        
        # Prepare prompt for model
        stage_e_json_str = json.dumps(model_input_data, ensure_ascii=False, indent=2)
        word_content_formatted = self.word_processor.prepare_word_for_model(
            word_content, 
            context="Test Questions"
        )
        
        full_prompt = f"""{prompt}

Stage E Data (JSON):
{stage_e_json_str}

Word Document (Test Questions):
{word_content_formatted}

Please analyze the Stage E data and the Word document, and generate a JSON response with the following structure:
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

For each PointId in the Stage E data, provide:
- Imp: Importance level (based on the test questions and content analysis)
- Type: Type classification (based on the content and test questions)

Return ONLY valid JSON, no additional text."""
        
        # Call model
        _progress(f"Calling model: {model_name}...")
        max_retries = 3
        response_text = None
        
        for attempt in range(max_retries):
            try:
                response_text = self.api_client.generate_content(
                    full_prompt,
                    model_name=model_name
                )
                if response_text:
                    break
            except Exception as e:
                self.logger.warning(f"Model call attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    _progress(f"Retrying... (attempt {attempt + 2}/{max_retries})")
                else:
                    self.logger.error("All model call attempts failed")
                    return None
        
        if not response_text:
            self.logger.error("No response from model")
            return None
        
        _progress(f"Received response from model ({len(response_text)} characters)")
        
        # Save raw response to TXT file
        base_dir = os.path.dirname(stage_e_path) or os.getcwd()
        base_name, _ = os.path.splitext(os.path.basename(stage_e_path))
        txt_path = os.path.join(base_dir, f"{base_name}_stage_j.txt")
        
        try:
            with open(txt_path, 'w', encoding='utf-8') as f:
                f.write(response_text)
            _progress(f"Saved raw response to: {txt_path}")
        except Exception as e:
            self.logger.warning(f"Failed to save TXT file: {e}")
        
        # Extract JSON from response
        _progress("Extracting JSON from model response...")
        model_output = self.extract_json_from_response(response_text)
        
        if not model_output:
            # Try loading from TXT file as fallback
            _progress("Trying to load JSON from TXT file...")
            model_output = self.load_txt_as_json(txt_path)
        
        if not model_output:
            self.logger.error("Failed to extract JSON from model response")
            return None
        
        # Get data from model output
        model_data = self.get_data_from_json(model_output)
        if not model_data:
            self.logger.error("Model output has no data")
            return None
        
        _progress(f"Extracted {len(model_data)} records from model output")
        
        # Create a mapping of PointId to Imp and Type
        pointid_to_imp_type = {}
        for record in model_data:
            point_id = record.get("PointId", "")
            if point_id:
                pointid_to_imp_type[point_id] = {
                    "Imp": record.get("Imp", ""),
                    "Type": record.get("Type", "")
                }
        
        _progress(f"Created mapping for {len(pointid_to_imp_type)} PointIds")
        
        # Merge Stage E data with Imp and Type columns
        _progress("Merging Stage E data with Imp and Type columns...")
        merged_records = []
        
        for record in stage_e_records:
            point_id = record.get("PointId", "")
            
            # Get Imp and Type from model output
            imp_type_data = pointid_to_imp_type.get(point_id, {"Imp": "", "Type": ""})
            
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


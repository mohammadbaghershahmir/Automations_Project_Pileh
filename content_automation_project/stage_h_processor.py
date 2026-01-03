"""
Stage H Processor: Flashcard Generation
Generates flashcards from Stage J (without Imp column) and Stage F data.
"""

import json
import logging
import os
from typing import Optional, Dict, List, Any, Callable
from base_stage_processor import BaseStageProcessor
from api_layer import APIConfig


class StageHProcessor(BaseStageProcessor):
    """Process Stage H: Generate flashcards from Stage J and Stage F data"""
    
    def __init__(self, api_client):
        super().__init__(api_client)
        self.logger = logging.getLogger(__name__)
    
    def process_stage_h(
        self,
        stage_j_path: str,
        stage_f_path: str,
        prompt: str,
        model_name: str,
        output_dir: Optional[str] = None,
        progress_callback: Optional[Callable[[str], None]] = None
    ) -> Optional[str]:
        """
        Process Stage H: Generate flashcards from Stage J and Stage F data.
        
        Args:
            stage_j_path: Path to Stage J JSON file (a{book}{chapter}.json)
            stage_f_path: Path to Stage F JSON file (f.json)
            prompt: User prompt for flashcard generation
            model_name: Gemini model name
            output_dir: Output directory (defaults to stage_j_path directory)
            progress_callback: Optional callback for progress updates
            
        Returns:
            Path to output file (ac{book}{chapter}.json) or None on error
        """
        def _progress(msg: str):
            if progress_callback:
                progress_callback(msg)
            self.logger.info(msg)
        
        _progress("Starting Stage H processing...")
        
        # Load Stage J JSON (without Imp column)
        _progress("Loading Stage J JSON...")
        stage_j_data = self.load_json_file(stage_j_path)
        if not stage_j_data:
            self.logger.error("Failed to load Stage J JSON")
            return None
        
        # Get data from Stage J
        stage_j_records = self.get_data_from_json(stage_j_data)
        if not stage_j_records:
            self.logger.error("Stage J JSON has no data")
            return None
        
        _progress(f"Loaded {len(stage_j_records)} records from Stage J")
        
        # Keep all columns from Stage J (we'll merge with model output later)
        # For prompt, we'll use a simplified version without Imp column
        stage_j_records_for_prompt = []
        for record in stage_j_records:
            clean_record = {
                "PointId": record.get("PointId", ""),
                "chapter": record.get("chapter", ""),
                "subchapter": record.get("subchapter", ""),
                "topic": record.get("topic", ""),
                "subtopic": record.get("subtopic", ""),
                "subsubtopic": record.get("subsubtopic", ""),
                "Points": record.get("Points", record.get("points", ""))
            }
            stage_j_records_for_prompt.append(clean_record)
        
        _progress(f"Prepared {len(stage_j_records_for_prompt)} records from Stage J for prompt (without Imp column)")
        
        # Extract book and chapter from first PointId
        first_point_id = stage_j_records[0].get("PointId")
        if not first_point_id:
            self.logger.error("No PointId found in Stage J data")
            return None
        
        try:
            book_id, chapter_id = self.extract_book_chapter_from_pointid(first_point_id)
        except ValueError as e:
            self.logger.error(f"Error extracting book/chapter: {e}")
            return None
        
        _progress(f"Detected Book ID: {book_id}, Chapter ID: {chapter_id}")
        
        # Load Stage F JSON
        _progress("Loading Stage F JSON...")
        stage_f_data = self.load_json_file(stage_f_path)
        if not stage_f_data:
            self.logger.error("Failed to load Stage F JSON")
            return None
        
        # Get data from Stage F
        stage_f_records = self.get_data_from_json(stage_f_data)
        if not stage_f_records:
            self.logger.warning("Stage F JSON has no data (no images)")
            stage_f_records = []
        
        _progress(f"Loaded {len(stage_f_records)} records from Stage F")
        
        # Prepare Stage F JSON string
        stage_f_json_str = json.dumps(stage_f_records, ensure_ascii=False, indent=2)
        
        # Prepare base prompt template
        base_prompt_template = f"""{prompt}

Stage F Data (Image files):
{stage_f_json_str}

Please analyze the Stage J data and Stage F data, and generate flashcards with questions and multiple choice answers.

Generate a JSON response with the following structure:
{{
  "data": [
    {{
      "PointId": "point_id_string",
      "Qtext": "question_text",
      "Choice1": "option_1_text",
      "Choice2": "option_2_text",
      "Choice3": "option_3_text",
      "Choice4": "option_4_text",
      "Correct": "correct_choice_number_1_to_4"
    }},
    ...
  ]
}}

IMPORTANT:
- Use EXACT field names: PointId, Qtext, Choice1, Choice2, Choice3, Choice4, Correct
- PointId must be a STRING matching exactly the PointId from Stage J data
- Qtext: The flashcard question text
- Choice1, Choice2, Choice3, Choice4: Four multiple choice options
- Correct: The number (1, 2, 3, or 4) indicating which choice is correct
- Generate meaningful flashcard questions based on the Points content
- Use Stage F data (image files) as context if relevant

Return ONLY valid JSON, no additional text."""
        
        # Split Stage J data dynamically into parts of 160 records each
        total_records = len(stage_j_records_for_prompt)
        part_size = 160  # Fixed size per part
        num_parts = (total_records + part_size - 1) // part_size  # Ceiling division
        
        _progress(f"Splitting Stage J data into {num_parts} parts (160 records per part, total: {total_records} records)")
        
        # Prepare output files
        base_dir = os.path.dirname(stage_j_path) or os.getcwd()
        base_name, _ = os.path.splitext(os.path.basename(stage_j_path))
        txt_path = os.path.join(base_dir, f"{base_name}_stage_h.txt")
        output_path = self.generate_filename("ac", book_id, chapter_id, output_dir or base_dir)
        
        # Initialize output files
        # Delete existing TXT file if exists
        if os.path.exists(txt_path):
            os.remove(txt_path)
        
        # Initialize lists for collecting data
        all_flashcard_records = []
        matched_count = 0
        pointid_to_flashcard_global = {}  # Global mapping
        
        # Process each part
        for part_num in range(1, num_parts + 1):
            start_idx = (part_num - 1) * part_size
            end_idx = min(start_idx + part_size, total_records)
            part_data = stage_j_records_for_prompt[start_idx:end_idx]
            
            _progress(f"Processing Part {part_num}/{num_parts} ({len(part_data)} records, indices {start_idx} to {end_idx-1})...")
            
            # Prepare prompt for this part
            part_json_str = json.dumps(part_data, ensure_ascii=False, indent=2)
            part_prompt = f"""{base_prompt_template}

Stage J Data - Part {part_num}/{num_parts} (without Imp and Type columns, {len(part_data)} records):
{part_json_str}"""
            
            # Call model with retry mechanism
            part_response = None
            max_retries = 3
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
                        return None
            
            if not part_response:
                self.logger.error(f"No response from model for Part {part_num}")
                return None
            
            # 1. Save to TXT file immediately (append mode)
            try:
                with open(txt_path, 'a', encoding='utf-8') as f:
                    if part_num > 1:
                        f.write("\n\n")
                    f.write(f"=== PART {part_num}/{num_parts} RESPONSE ===\n")
                    f.write(part_response)
                _progress(f"Part {part_num} saved to TXT file")
            except Exception as e:
                self.logger.warning(f"Failed to save Part {part_num} to TXT: {e}")
            
            # 2. Extract JSON from response immediately
            _progress(f"Extracting JSON from Part {part_num} response...")
            part_output = self.extract_json_from_response(part_response)
            if not part_output:
                _progress(f"Trying to extract Part {part_num} JSON from text...")
                part_output = self.load_txt_as_json_from_text(part_response)
            
            if not part_output:
                self.logger.warning(f"Failed to extract JSON from Part {part_num}, continuing...")
                continue
            
            # Extract data from JSON
            part_data_list = self.get_data_from_json(part_output) if part_output else []
            if not part_data_list:
                self.logger.warning(f"No data extracted from Part {part_num}, continuing...")
                continue
            
            _progress(f"Extracted {len(part_data_list)} records from Part {part_num}")
            
            # 3. Add to global mapping (collect flashcard data incrementally)
            for record in part_data_list:
                point_id = record.get("PointId", "") or record.get("point_id", "")
                if point_id:
                    point_id = str(point_id)
                    pointid_to_flashcard_global[point_id] = record
            
            _progress(f"Part {part_num} processed: {len(part_data_list)} flashcards extracted and added to mapping")
        
        _progress(f"Total flashcard data collected: {len(pointid_to_flashcard_global)} PointIds")
        
        # 4. Create flashcard records for ALL Stage J records (using collected flashcard data)
        _progress("Merging Stage J data with collected flashcard data...")
        for record in stage_j_records:
            point_id = str(record.get("PointId", ""))
            flashcard_data = pointid_to_flashcard_global.get(point_id, {})
            
            flashcard_record = {
                "PointId": point_id,
                "chapter": record.get("chapter", ""),
                "subchapter": record.get("subchapter", ""),
                "topic": record.get("topic", ""),
                "subtopic": record.get("subtopic", ""),
                "subsubtopic": record.get("subsubtopic", ""),
                "Points": record.get("Points", record.get("points", "")),
                "Type": record.get("Type", ""),
                "Imp": record.get("Imp", ""),
                "Qtext": flashcard_data.get("Qtext", ""),
                "Choice1": flashcard_data.get("Choice1", ""),
                "Choice2": flashcard_data.get("Choice2", ""),
                "Choice3": flashcard_data.get("Choice3", ""),
                "Choice4": flashcard_data.get("Choice4", ""),
                "Correct": flashcard_data.get("Correct", ""),
                "Mainanswer": "زیرعنوان"
            }
            
            # Track matches
            if flashcard_data.get("Qtext") or flashcard_data.get("Correct"):
                matched_count += 1
            
            all_flashcard_records.append(flashcard_record)
        
        _progress(f"Generated {len(all_flashcard_records)} flashcard records")
        _progress(f"Matched {matched_count} records with flashcard data from model")
        if matched_count == 0:
            self.logger.warning("No records matched! Check PointId format in model output vs Stage J")
        
        # Prepare output directory
        if not output_dir:
            output_dir = os.path.dirname(stage_j_path) or os.getcwd()
        
        # Generate output filename: ac{book}{chapter}.json
        output_path = self.generate_filename("ac", book_id, chapter_id, output_dir)
        
        # Prepare metadata
        output_metadata = {
            "book_id": book_id,
            "chapter_id": chapter_id,
            "source_stage_j": os.path.basename(stage_j_path),
            "source_stage_f": os.path.basename(stage_f_path),
            "model_used": model_name,
            "stage_h_txt_file": os.path.basename(txt_path),
            "total_records": len(all_flashcard_records),
            "records_with_flashcards": matched_count,
            "num_parts": num_parts,
            "part_size": part_size,
            "division_method": "dynamic_by_part_size"
        }
        
        # Save JSON
        _progress(f"Saving Stage H output to: {output_path}")
        success = self.save_json_file(all_flashcard_records, output_path, output_metadata, "H")
        
        if success:
            _progress(f"Stage H completed successfully: {output_path}")
            return output_path
        else:
            self.logger.error("Failed to save Stage H output")
            return None


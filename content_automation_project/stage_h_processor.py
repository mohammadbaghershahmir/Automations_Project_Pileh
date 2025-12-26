"""
Stage H Processor: Flashcard Generation
Generates flashcards from Stage J (without Imp column) and Stage F data.
"""

import json
import logging
import os
import tempfile
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
        
        # Split Stage J data into four parts (for prompt)
        total_records = len(stage_j_records_for_prompt)
        part_size = total_records // 4
        part1_data = stage_j_records_for_prompt[:part_size]
        part2_data = stage_j_records_for_prompt[part_size:part_size * 2]
        part3_data = stage_j_records_for_prompt[part_size * 2:part_size * 3]
        part4_data = stage_j_records_for_prompt[part_size * 3:]
        
        _progress(f"Splitting Stage J data into 4 parts: Part 1 ({len(part1_data)} records), Part 2 ({len(part2_data)} records), Part 3 ({len(part3_data)} records), Part 4 ({len(part4_data)} records)")
        
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
        
        # Process Part 1
        _progress("Processing Part 1...")
        part1_json_str = json.dumps(part1_data, ensure_ascii=False, indent=2)
        part1_prompt = f"""{base_prompt_template}

Stage J Data - Part 1 (without Imp and Type columns):
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

Stage J Data - Part 2 (without Imp column, 7 columns):
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
        
        # Process Part 3
        _progress("Processing Part 3...")
        part3_json_str = json.dumps(part3_data, ensure_ascii=False, indent=2)
        part3_prompt = f"""{base_prompt_template}

Stage J Data - Part 3 (without Imp and Type columns):
{part3_json_str}"""
        
        part3_response = None
        for attempt in range(max_retries):
            try:
                part3_response = self.api_client.process_text(
                    text=part3_prompt,
                    system_prompt=None,
                    model_name=model_name,
                    temperature=APIConfig.DEFAULT_TEMPERATURE,
                    max_tokens=APIConfig.DEFAULT_MAX_TOKENS
                )
                if part3_response:
                    _progress(f"Part 3 response received ({len(part3_response)} characters)")
                    break
            except Exception as e:
                self.logger.warning(f"Part 3 model call attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    _progress(f"Retrying Part 3... (attempt {attempt + 2}/{max_retries})")
                else:
                    self.logger.error("All Part 3 model call attempts failed")
                    return None
        
        if not part3_response:
            self.logger.error("No response from model for Part 3")
            return None
        
        # Process Part 4
        _progress("Processing Part 4...")
        part4_json_str = json.dumps(part4_data, ensure_ascii=False, indent=2)
        part4_prompt = f"""{base_prompt_template}

Stage J Data - Part 4 (without Imp and Type columns):
{part4_json_str}"""
        
        part4_response = None
        for attempt in range(max_retries):
            try:
                part4_response = self.api_client.process_text(
                    text=part4_prompt,
                    system_prompt=None,
                    model_name=model_name,
                    temperature=APIConfig.DEFAULT_TEMPERATURE,
                    max_tokens=APIConfig.DEFAULT_MAX_TOKENS
                )
                if part4_response:
                    _progress(f"Part 4 response received ({len(part4_response)} characters)")
                    break
            except Exception as e:
                self.logger.warning(f"Part 4 model call attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    _progress(f"Retrying Part 4... (attempt {attempt + 2}/{max_retries})")
                else:
                    self.logger.error("All Part 4 model call attempts failed")
                    return None
        
        if not part4_response:
            self.logger.error("No response from model for Part 4")
            return None
        
        # Save raw responses to TXT file (just the responses, no headers)
        base_dir = os.path.dirname(stage_j_path) or os.getcwd()
        base_name, _ = os.path.splitext(os.path.basename(stage_j_path))
        txt_path = os.path.join(base_dir, f"{base_name}_stage_h.txt")
        
        try:
            with open(txt_path, 'w', encoding='utf-8') as f:
                f.write(part1_response)
                f.write("\n\n")
                f.write(part2_response)
                f.write("\n\n")
                f.write(part3_response)
                f.write("\n\n")
                f.write(part4_response)
            _progress(f"Saved raw model responses to: {txt_path}")
        except Exception as e:
            self.logger.warning(f"Failed to save TXT file: {e}")
        
        # Extract JSON from all four responses
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
        
        _progress("Extracting JSON from Part 3 response...")
        part3_output = self.extract_json_from_response(part3_response)
        if not part3_output:
            # Try loading from text directly
            _progress("Trying to extract Part 3 JSON from text...")
            part3_output = self.load_txt_as_json_from_text(part3_response)
        
        _progress("Extracting JSON from Part 4 response...")
        part4_output = self.extract_json_from_response(part4_response)
        if not part4_output:
            # Try loading from text directly
            _progress("Trying to extract Part 4 JSON from text...")
            part4_output = self.load_txt_as_json_from_text(part4_response)
        
        # Combine all four outputs
        _progress("Combining JSON from all four parts...")
        part1_data_list = self.get_data_from_json(part1_output) if part1_output else []
        part2_data_list = self.get_data_from_json(part2_output) if part2_output else []
        part3_data_list = self.get_data_from_json(part3_output) if part3_output else []
        part4_data_list = self.get_data_from_json(part4_output) if part4_output else []
        
        # Create combined model output
        combined_model_data = part1_data_list + part2_data_list + part3_data_list + part4_data_list
        
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
        _progress(f"Extracted {len(model_data)} records from combined model output (Part 1: {len(part1_data_list)}, Part 2: {len(part2_data_list)}, Part 3: {len(part3_data_list)}, Part 4: {len(part4_data_list)})")
        
        # Create a mapping from model output
        pointid_to_flashcard = {}
        for record in model_data:
            point_id = record.get("PointId", "") or record.get("point_id", "")
            if point_id:
                point_id = str(point_id)
                pointid_to_flashcard[point_id] = record
        
        _progress(f"Created mapping for {len(pointid_to_flashcard)} PointIds from model output")
        
        # Merge Stage J data with model output
        # Final output should have 16 columns:
        # From file a: PointId, chapter, subchapter, topic, subtopic, subsubtopic, Points, Type, Imp
        # From model: PointId, Qtext, Choice1, Choice2, Choice3, Choice4, Correct
        # New column: Mainanswer (default: "زیرعنوان")
        _progress("Merging Stage J data with model output...")
        matched_count = 0
        flashcard_records = []
        
        for record in stage_j_records:
            point_id = record.get("PointId", "")
            if point_id:
                point_id = str(point_id)
            
            # Get flashcard data from model output
            flashcard_data = pointid_to_flashcard.get(point_id, {})
            
            # Create flashcard record with exactly 16 columns
            flashcard_record = {
                # From file a (9 columns)
                "PointId": point_id,
                "chapter": record.get("chapter", ""),
                "subchapter": record.get("subchapter", ""),
                "topic": record.get("topic", ""),
                "subtopic": record.get("subtopic", ""),
                "subsubtopic": record.get("subsubtopic", ""),
                "Points": record.get("Points", record.get("points", "")),
                "Type": record.get("Type", ""),
                "Imp": record.get("Imp", ""),
                # From model (7 columns)
                "Qtext": flashcard_data.get("Qtext", ""),
                "Choice1": flashcard_data.get("Choice1", ""),
                "Choice2": flashcard_data.get("Choice2", ""),
                "Choice3": flashcard_data.get("Choice3", ""),
                "Choice4": flashcard_data.get("Choice4", ""),
                "Correct": flashcard_data.get("Correct", ""),  # No default value, only from model
                # New column
                "Mainanswer": "زیرعنوان"
            }
            
            # Track matches
            if flashcard_data.get("Qtext") or flashcard_data.get("Correct"):
                matched_count += 1
            
            flashcard_records.append(flashcard_record)
        
        _progress(f"Generated {len(flashcard_records)} flashcard records")
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
            "total_records": len(flashcard_records),
            "records_with_flashcards": matched_count
        }
        
        # Save JSON
        _progress(f"Saving Stage H output to: {output_path}")
        success = self.save_json_file(flashcard_records, output_path, output_metadata, "H")
        
        if success:
            _progress(f"Stage H completed successfully: {output_path}")
            return output_path
        else:
            self.logger.error("Failed to save Stage H output")
            return None


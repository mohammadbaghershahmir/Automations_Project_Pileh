"""
Stage V Processor: Test File Generation
Generates test files from Stage J data and Word document in two steps:
Step 1: Generate initial test questions
Step 2: Refine and combine with QId mapping
"""

import json
import logging
import os
from typing import Optional, Dict, List, Any, Callable
from base_stage_processor import BaseStageProcessor
from word_file_processor import WordFileProcessor
from api_layer import APIConfig


class StageVProcessor(BaseStageProcessor):
    """Process Stage V: Generate test files from Stage J and Word document"""
    
    def __init__(self, api_client):
        super().__init__(api_client)
        self.logger = logging.getLogger(__name__)
        self.word_processor = WordFileProcessor()
    
    def process_stage_v(
        self,
        stage_j_path: str,
        word_file_path: str,
        prompt_1: str,
        model_name_1: str,
        prompt_2: str,
        model_name_2: str,
        output_dir: Optional[str] = None,
        progress_callback: Optional[Callable[[str], None]] = None
    ) -> Optional[str]:
        """
        Process Stage V: Generate test files from Stage J and Word document.
        
        Args:
            stage_j_path: Path to Stage J JSON file (a{book}{chapter}.json)
            word_file_path: Path to Word file containing test questions
            prompt_1: Prompt for Step 1 (initial test questions generation)
            model_name_1: Gemini model name for Step 1
            prompt_2: Prompt for Step 2 (refine and QId mapping)
            model_name_2: Gemini model name for Step 2
            output_dir: Output directory (defaults to stage_j_path directory)
            progress_callback: Optional callback for progress updates
            
        Returns:
            Path to output file (b.json) or None on error
        """
        def _progress(msg: str):
            if progress_callback:
                progress_callback(msg)
            self.logger.info(msg)
        
        _progress("Starting Stage V processing...")
        
        # Extract book and chapter from Stage J file
        stage_j_data = self.load_json_file(stage_j_path)
        if not stage_j_data:
            self.logger.error("Failed to load Stage J JSON")
            return None
        
        stage_j_records = self.get_data_from_json(stage_j_data)
        if not stage_j_records:
            self.logger.error("Stage J JSON has no data")
            return None
        
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
        
        # ========== STEP 1: Generate Initial Test Questions ==========
        _progress("=" * 60)
        _progress("STEP 1: Generating initial test questions...")
        _progress("=" * 60)
        
        step1_output = self._step1_generate_initial_questions(
            stage_j_path=stage_j_path,
            word_file_path=word_file_path,
            prompt=prompt_1,
            model_name=model_name_1,
            output_dir=output_dir,
            progress_callback=progress_callback
        )
        
        if not step1_output:
            self.logger.error("Step 1 failed")
            return None
        
        _progress(f"Step 1 completed. Output saved to: {step1_output}")
        
        # ========== STEP 2: Refine and Combine with QId Mapping ==========
        _progress("=" * 60)
        _progress("STEP 2: Refining questions and adding QId mapping...")
        _progress("=" * 60)
        
        step2_output = self._step2_refine_with_qid_mapping(
            step1_output_path=step1_output,
            stage_j_path=stage_j_path,
            prompt=prompt_2,
            model_name=model_name_2,
            book_id=book_id,
            chapter_id=chapter_id,
            output_dir=output_dir,
            progress_callback=progress_callback
        )
        
        if not step2_output:
            self.logger.error("Step 2 failed")
            return None
        
        _progress(f"Stage V completed successfully: {step2_output}")
        return step2_output
    
    def _step1_generate_initial_questions(
        self,
        stage_j_path: str,
        word_file_path: str,
        prompt: str,
        model_name: str,
        output_dir: Optional[str] = None,
        progress_callback: Optional[Callable[[str], None]] = None
    ) -> Optional[str]:
        """
        Step 1: Generate initial test questions from Stage J and Word document.
        
        Args:
            stage_j_path: Path to Stage J JSON file
            word_file_path: Path to Word file
            prompt: Prompt for initial question generation
            model_name: Gemini model name
            output_dir: Output directory
            progress_callback: Optional callback for progress updates
            
        Returns:
            Path to Step 1 output file or None on error
        """
        def _progress(msg: str):
            if progress_callback:
                progress_callback(msg)
            self.logger.info(msg)
        
        _progress("Loading Stage J JSON for Step 1...")
        stage_j_data = self.load_json_file(stage_j_path)
        if not stage_j_data:
            return None
        
        stage_j_records = self.get_data_from_json(stage_j_data)
        if not stage_j_records:
            return None
        
        _progress(f"Loaded {len(stage_j_records)} records from Stage J")
        
        # Prepare Stage J data for prompt (without Type column)
        stage_j_records_for_prompt = []
        for record in stage_j_records:
            clean_record = {
                "PointId": record.get("PointId", ""),
                "chapter": record.get("chapter", ""),
                "subchapter": record.get("subchapter", ""),
                "topic": record.get("topic", ""),
                "subtopic": record.get("subtopic", ""),
                "subsubtopic": record.get("subsubtopic", ""),
                "Points": record.get("Points", record.get("points", "")),
                "Imp": record.get("Imp", "")
                # Type column removed as per requirements
            }
            stage_j_records_for_prompt.append(clean_record)
        
        # Load Word file
        _progress("Loading Word document...")
        word_content = self.word_processor.read_word_file(word_file_path)
        if not word_content:
            self.logger.error("Failed to read Word file")
            return None
        
        word_content_formatted = self.word_processor.prepare_word_for_model(
            word_content,
            context="Test Questions"
        )
        
        # Prepare base prompt template
        base_prompt_template = f"""{prompt}

Word Document (Test Questions):
{word_content_formatted}

Please analyze the Stage J data and the Word document, and generate initial test questions.

Generate a JSON response with the following structure:
{{
  "data": [
    {{
      "PointId": "point_id_string",
      "Question": "question_text",
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
- Use EXACT field names: PointId, Question, Choice1, Choice2, Choice3, Choice4, Correct
- PointId must be a STRING matching exactly the PointId from Stage J data
- Question: The test question text
- Choice1, Choice2, Choice3, Choice4: Four multiple choice options
- Correct: The number (1, 2, 3, or 4) indicating which choice is correct
- Generate meaningful test questions based on the Points content and Word document
- Each PointId should have at least one test question

Return ONLY valid JSON, no additional text."""
        
        # Prepare full prompt (no splitting into parts)
        full_stage_j_json = json.dumps(stage_j_records_for_prompt, ensure_ascii=False, indent=2)
        full_prompt = f"""{base_prompt_template}

Stage J Data (all records, without Type column):
{full_stage_j_json}"""
        
        # Call model once and collect raw response
        all_raw_responses = []
        max_retries = 3
        
        _progress("Processing Stage V - Step 1 (single part)...")
        part_response = None
        for attempt in range(max_retries):
            try:
                part_response = self.api_client.process_text(
                    text=full_prompt,
                    system_prompt=None,
                    model_name=model_name,
                    temperature=APIConfig.DEFAULT_TEMPERATURE,
                    max_tokens=APIConfig.DEFAULT_MAX_TOKENS
                )
                if part_response:
                    _progress(f"Step 1 response received ({len(part_response)} characters)")
                    break
            except Exception as e:
                self.logger.warning(f"Step 1 attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    _progress(f"Retrying Step 1... (attempt {attempt + 2}/{max_retries})")
                else:
                    self.logger.error("All Step 1 attempts failed")
        
        if not part_response:
            self.logger.error("No response from model in Step 1")
            return None
        
        # Store raw response as single part
        all_raw_responses.append(part_response)
        
        if not all_raw_responses:
            self.logger.error("No responses received in Step 1")
            return None
        
        # Save all raw responses to TXT file
        base_dir = os.path.dirname(stage_j_path) or os.getcwd()
        base_name, _ = os.path.splitext(os.path.basename(stage_j_path))
        txt_path = os.path.join(base_dir, f"{base_name}_stage_v_step1.txt")
        
        try:
            with open(txt_path, 'w', encoding='utf-8') as f:
                for idx, response in enumerate(all_raw_responses, 1):
                    f.write(f"=== PART {idx} RESPONSE ===\n")
                    f.write(response)
                    f.write("\n\n")
            _progress(f"Saved raw model responses to: {txt_path}")
        except Exception as e:
            self.logger.warning(f"Failed to save TXT file: {e}")
        
        # Extract JSON from all responses
        all_questions = []
        for part_idx, part_response in enumerate(all_raw_responses, 1):
            _progress(f"Extracting JSON from Part {part_idx} response...")
            part_output = self.extract_json_from_response(part_response)
            if not part_output:
                _progress(f"Trying to extract Part {part_idx} JSON from text...")
                part_output = self.load_txt_as_json_from_text(part_response)
            
            if part_output:
                # Handle both list and dict JSON structures
                if isinstance(part_output, list):
                    part_questions = part_output
                elif isinstance(part_output, dict):
                    part_questions = self.get_data_from_json(part_output)
                else:
                    part_questions = []
                
                if part_questions:
                    all_questions.extend(part_questions)
                    _progress(f"Extracted {len(part_questions)} questions from Part {part_idx}")
        
        if not all_questions:
            self.logger.error("Failed to extract JSON from model responses")
            # Try loading from TXT file as fallback (whole TXT → JSON → list)
            _progress("Trying to load JSON from TXT file as fallback...")
            model_output = self.load_txt_as_json(txt_path)
            if model_output:
                if isinstance(model_output, list):
                    all_questions = model_output
                elif isinstance(model_output, dict):
                    all_questions = self.get_data_from_json(model_output)
        
        if not all_questions:
            self.logger.error("No questions generated in Step 1")
            return None
        
        _progress(f"Total questions generated in Step 1: {len(all_questions)}")
        
        # Save Step 1 output
        if not output_dir:
            output_dir = os.path.dirname(stage_j_path) or os.getcwd()
        
        base_name, _ = os.path.splitext(os.path.basename(stage_j_path))
        step1_output_path = os.path.join(output_dir, f"{base_name}_stage_v_step1.json")
        
        step1_metadata = {
            "step": 1,
            "source_stage_j": os.path.basename(stage_j_path),
            "source_word_file": os.path.basename(word_file_path),
            "model_used": model_name,
            "total_questions": len(all_questions)
        }
        
        success = self.save_json_file(all_questions, step1_output_path, step1_metadata, "V-Step1")
        if success:
            _progress(f"Step 1 output saved to: {step1_output_path}")
            return step1_output_path
        else:
            self.logger.error("Failed to save Step 1 output")
            return None
    
    def _step2_refine_with_qid_mapping(
        self,
        step1_output_path: str,
        stage_j_path: str,
        prompt: str,
        model_name: str,
        book_id: int,
        chapter_id: int,
        output_dir: Optional[str] = None,
        progress_callback: Optional[Callable[[str], None]] = None
    ) -> Optional[str]:
        """
        Step 2: Refine questions and add QId mapping to PointIds.
        
        Args:
            step1_output_path: Path to Step 1 output file
            stage_j_path: Path to Stage J JSON file (for PointId mapping)
            prompt: Prompt for refinement and QId mapping
            model_name: Gemini model name
            book_id: Book ID
            chapter_id: Chapter ID
            output_dir: Output directory
            progress_callback: Optional callback for progress updates
            
        Returns:
            Path to final output file (b.json) or None on error
        """
        def _progress(msg: str):
            if progress_callback:
                progress_callback(msg)
            self.logger.info(msg)
        
        # Load Step 1 output
        _progress("Loading Step 1 output...")
        step1_data = self.load_json_file(step1_output_path)
        if not step1_data:
            self.logger.error("Failed to load Step 1 output")
            return None
        
        step1_questions = self.get_data_from_json(step1_data)
        if not step1_questions:
            self.logger.error("Step 1 output has no questions")
            return None
        
        _progress(f"Loaded {len(step1_questions)} questions from Step 1")
        
        # Load Stage J for PointId mapping
        _progress("Loading Stage J JSON for PointId mapping...")
        stage_j_data = self.load_json_file(stage_j_path)
        if not stage_j_data:
            return None
        
        stage_j_records = self.get_data_from_json(stage_j_data)
        if not stage_j_records:
            return None
        
        # Create PointId to record mapping (without Type column)
        pointid_to_record = {}
        stage_j_records_for_prompt = []
        for record in stage_j_records:
            point_id = str(record.get("PointId", ""))
            if point_id:
                pointid_to_record[point_id] = record
                # Prepare clean record without Type column
                clean_record = {
                    "PointId": record.get("PointId", ""),
                    "chapter": record.get("chapter", ""),
                    "subchapter": record.get("subchapter", ""),
                    "topic": record.get("topic", ""),
                    "subtopic": record.get("subtopic", ""),
                    "subsubtopic": record.get("subsubtopic", ""),
                    "Points": record.get("Points", record.get("points", "")),
                    "Imp": record.get("Imp", "")
                    # Type column removed as per requirements
                }
                stage_j_records_for_prompt.append(clean_record)
        
        _progress(f"Created mapping for {len(pointid_to_record)} PointIds")
        
        # Prepare base prompt template
        base_prompt_template = f"""{prompt}

Stage J Data (for PointId reference, without Type column):
{json.dumps(stage_j_records_for_prompt[:10], ensure_ascii=False, indent=2)}
(Showing first 10 records as reference)

Please refine the test questions and add QId mapping.

Generate a JSON response with the following structure:
{{
  "data": [
    {{
      "QID": "unique_question_id",
      "PointId": "point_id_string",
      "Question": "refined_question_text",
      "Choice1": "option_1_text",
      "Choice2": "option_2_text",
      "Choice3": "option_3_text",
      "Choice4": "option_4_text",
      "Correct": "correct_choice_number_1_to_4",
      "Script": "script_text_for_tts"
    }},
    ...
  ]
}}

IMPORTANT:
- Use EXACT field names: QId, PointId, Question, Choice1, Choice2, Choice3, Choice4, Correct, Script
- QId: Generate a numeric question ID (format: {{book_id}}{{chapter_id}}{{sequential_number_4_digits}}, e.g., {book_id:03d}{chapter_id:03d}0001)
- PointId: Must match exactly the PointId from Step 1 questions
- Question: Refined and improved question text
- Choice1-4: Refined multiple choice options
- Correct: The number (1, 2, 3, or 4) indicating which choice is correct
- Script: Text suitable for text-to-speech (TTS) generation
- Ensure each question is properly mapped to its PointId
- QId should be sequential starting from {book_id:03d}{chapter_id:03d}0001

Return ONLY valid JSON, no additional text."""
        
        # Prepare full prompt for Step 2 (no splitting into parts)
        full_step1_json = json.dumps(step1_questions, ensure_ascii=False, indent=2)
        full_prompt = f"""{base_prompt_template}

Step 1 Questions (all records):
{full_step1_json}"""
        
        # Call model once and collect raw response
        all_raw_responses = []
        max_retries = 3
        
        _progress("Processing Stage V - Step 2 (single part)...")
        part_response = None
        for attempt in range(max_retries):
            try:
                part_response = self.api_client.process_text(
                    text=full_prompt,
                    system_prompt=None,
                    model_name=model_name,
                    temperature=APIConfig.DEFAULT_TEMPERATURE,
                    max_tokens=APIConfig.DEFAULT_MAX_TOKENS
                )
                if part_response:
                    _progress(f"Step 2 response received ({len(part_response)} characters)")
                    break
            except Exception as e:
                self.logger.warning(f"Step 2 attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    _progress(f"Retrying Step 2... (attempt {attempt + 2}/{max_retries})")
                else:
                    self.logger.error("All Step 2 attempts failed")
        
        if not part_response:
            self.logger.error("No response from model in Step 2")
            return None
        
        # Store raw response as single part
        all_raw_responses.append(part_response)
        
        if not all_raw_responses:
            self.logger.error("No responses received in Step 2")
            return None
        
        # Save all raw responses to TXT file
        base_dir = os.path.dirname(stage_j_path) or os.getcwd()
        base_name, _ = os.path.splitext(os.path.basename(stage_j_path))
        txt_path = os.path.join(base_dir, f"{base_name}_stage_v_step2.txt")
        
        try:
            with open(txt_path, 'w', encoding='utf-8') as f:
                for idx, response in enumerate(all_raw_responses, 1):
                    f.write(f"=== PART {idx} RESPONSE ===\n")
                    f.write(response)
                    f.write("\n\n")
            _progress(f"Saved raw model responses to: {txt_path}")
        except Exception as e:
            self.logger.warning(f"Failed to save TXT file: {e}")
        
        # Extract JSON from all responses
        all_refined_questions = []
        for part_idx, part_response in enumerate(all_raw_responses, 1):
            _progress(f"Extracting JSON from Part {part_idx} response...")
            part_output = self.extract_json_from_response(part_response)
            if not part_output:
                _progress(f"Trying to extract Part {part_idx} JSON from text...")
                part_output = self.load_txt_as_json_from_text(part_response)
            
            if part_output:
                # Handle both list and dict JSON structures
                if isinstance(part_output, list):
                    part_refined = part_output
                elif isinstance(part_output, dict):
                    part_refined = self.get_data_from_json(part_output)
                else:
                    part_refined = []
                
                if part_refined:
                    all_refined_questions.extend(part_refined)
                    _progress(f"Extracted {len(part_refined)} refined questions from Part {part_idx}")
        
        if not all_refined_questions:
            self.logger.error("Failed to extract JSON from model responses")
            # Try loading from TXT file as fallback (whole TXT → JSON → list)
            _progress("Trying to load JSON from TXT file as fallback...")
            model_output = self.load_txt_as_json(txt_path)
            if model_output:
                if isinstance(model_output, list):
                    all_refined_questions = model_output
                elif isinstance(model_output, dict):
                    all_refined_questions = self.get_data_from_json(model_output)
        
        if not all_refined_questions:
            self.logger.error("No refined questions generated in Step 2")
            return None
        
        _progress(f"Total refined questions: {len(all_refined_questions)}")
        
        # Combine Step 1 and Step 2 questions (Step1 first, then refined)
        _progress("Combining Step 1 and Step 2 questions...")
        combined_questions = []
        combined_questions.extend(step1_questions)
        combined_questions.extend(all_refined_questions)
        
        # Generate QIds for ALL combined questions (numeric only, sequential)
        _progress("Generating QIds for combined questions...")
        qid_counter = 1
        for question in combined_questions:
            qid = f"{book_id:03d}{chapter_id:03d}{qid_counter:04d}"
            question["QId"] = qid
            qid_counter += 1
        
        # Save final output (b{book}{chapter}.json)
        if not output_dir:
            output_dir = os.path.dirname(stage_j_path) or os.getcwd()
        
        output_path = self.generate_filename("b", book_id, chapter_id, output_dir)
        
        output_metadata = {
            "book_id": book_id,
            "chapter_id": chapter_id,
            "source_stage_j": os.path.basename(stage_j_path),
            "source_step1": os.path.basename(step1_output_path),
            "model_step1": "N/A",  # Could be stored in step1 metadata
            "model_step2": model_name,
            "total_questions": len(combined_questions)
        }
        
        success = self.save_json_file(combined_questions, output_path, output_metadata, "V")
        if success:
            _progress(f"Final output saved to: {output_path}")
            return output_path
        else:
            self.logger.error("Failed to save final output")
            return None


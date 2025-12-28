"""
Stage V Processor: Test File Generation
Generates test files from Stage J data and Word document in three steps:
Step 1: Generate initial test questions
Step 2: Refine questions (without QId mapping)
Step 3: Add QId mapping
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
        prompt_3: str,
        model_name_3: str,
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
            prompt_2: Prompt for Step 2 (refine questions, NO QId mapping)
            model_name_2: Gemini model name for Step 2
            prompt_3: Prompt for Step 3 (QId mapping)
            model_name_3: Gemini model name for Step 3
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
        
        # Group Stage J records by Topic
        _progress("Grouping Stage J records by Topic...")
        topics: Dict[str, List[Dict[str, Any]]] = {}
        unmatched_records = []
        
        for record in stage_j_records:
            if not isinstance(record, dict):
                continue
            topic_value = record.get("topic", "") or record.get("Topic", "")
            if topic_value:
                topic_key = str(topic_value).strip().lower()
                topics.setdefault(topic_key, []).append(record)
            else:
                unmatched_records.append(record)
        
        # Assign unmatched records to default topic
        if unmatched_records:
            if "default" not in topics:
                topics["default"] = []
            topics["default"].extend(unmatched_records)
        
        if not topics:
            self.logger.error("No valid Topic information found in Stage J data")
            return None
        
        sorted_topics = sorted(topics.keys())
        _progress(f"Found {len(sorted_topics)} topics: {sorted_topics}")
        
        # ========== STEP 1: Generate Initial Test Questions (per Topic) ==========
        _progress("=" * 60)
        _progress("STEP 1: Generating initial test questions (per Topic)...")
        _progress("=" * 60)
        
        step1_topic_outputs: Dict[str, str] = {}
        for topic_id in sorted_topics:
            topic_records = topics[topic_id]
            _progress(f"Processing Step 1 for Topic '{topic_id}' ({len(topic_records)} records)...")
            
            # Create temporary Stage J JSON for this topic
            topic_stage_j_data = {
                "metadata": stage_j_data.get("metadata", {}),
                "data": topic_records
            }
            
            # Save temporary JSON file
            import tempfile
            temp_dir = output_dir or os.path.dirname(stage_j_path) or os.getcwd()
            temp_stage_j_path = os.path.join(temp_dir, f"temp_stage_j_topic_{topic_id.replace('/', '_').replace(' ', '_')}.json")
            try:
                with open(temp_stage_j_path, 'w', encoding='utf-8') as f:
                    json.dump(topic_stage_j_data, f, ensure_ascii=False, indent=2)
            except Exception as e:
                self.logger.error(f"Failed to create temporary Stage J file for topic {topic_id}: {e}")
                continue
            
            topic_step1_output = self._step1_generate_initial_questions(
                stage_j_path=temp_stage_j_path,
                word_file_path=word_file_path,
                prompt=prompt_1,
                model_name=model_name_1,
                output_dir=output_dir,
                progress_callback=progress_callback
            )
            
            # Clean up temporary file
            try:
                if os.path.exists(temp_stage_j_path):
                    os.remove(temp_stage_j_path)
            except:
                pass
            
            if topic_step1_output:
                step1_topic_outputs[topic_id] = topic_step1_output
                _progress(f"Step 1 completed for Topic '{topic_id}': {topic_step1_output}")
            else:
                self.logger.warning(f"Step 1 failed for Topic '{topic_id}', skipping...")
        
        if not step1_topic_outputs:
            self.logger.error("Step 1 failed for all topics")
            return None
        
        # Combine Step 1 outputs from all topics
        _progress("Combining Step 1 outputs from all topics...")
        step1_combined_data = []
        for topic_id, topic_step1_path in step1_topic_outputs.items():
            topic_step1_data = self.load_json_file(topic_step1_path)
            if topic_step1_data:
                topic_records = self.get_data_from_json(topic_step1_data)
                if topic_records:
                    step1_combined_data.extend(topic_records)
        
        if not step1_combined_data:
            self.logger.error("Failed to combine Step 1 outputs")
            return None
        
        # Save combined Step 1 output
        step1_combined_path = os.path.join(output_dir or os.path.dirname(stage_j_path) or os.getcwd(), 
                                          f"step1_combined_{book_id}{chapter_id:03d}.json")
        step1_combined_json = {
            "metadata": {
                "book_id": book_id,
                "chapter_id": chapter_id,
                "source": "Stage V - Step 1 (Combined from Topics)",
                "total_topics": len(step1_topic_outputs),
                "total_records": len(step1_combined_data)
            },
            "data": step1_combined_data
        }
        try:
            with open(step1_combined_path, 'w', encoding='utf-8') as f:
                json.dump(step1_combined_json, f, ensure_ascii=False, indent=2)
            _progress(f"Step 1 combined output saved to: {step1_combined_path}")
        except Exception as e:
            self.logger.error(f"Failed to save combined Step 1 output: {e}")
            return None
        
        step1_output = step1_combined_path
        
        # ========== STEP 2: Refine Questions (NO QId Mapping) (per Topic) ==========
        _progress("=" * 60)
        _progress("STEP 2: Refining questions (without QId mapping) (per Topic)...")
        _progress("=" * 60)
        
        step2_topic_outputs: Dict[str, str] = {}
        for topic_id in sorted_topics:
            if topic_id not in step1_topic_outputs:
                continue
            
            topic_records = topics[topic_id]
            _progress(f"Processing Step 2 for Topic '{topic_id}' ({len(topic_records)} records)...")
            
            # Create temporary Stage J JSON for this topic
            topic_stage_j_data = {
                "metadata": stage_j_data.get("metadata", {}),
                "data": topic_records
            }
            
            # Save temporary JSON file
            temp_dir = output_dir or os.path.dirname(stage_j_path) or os.getcwd()
            temp_stage_j_path = os.path.join(temp_dir, f"temp_stage_j_topic_{topic_id.replace('/', '_').replace(' ', '_')}.json")
            try:
                with open(temp_stage_j_path, 'w', encoding='utf-8') as f:
                    json.dump(topic_stage_j_data, f, ensure_ascii=False, indent=2)
            except Exception as e:
                self.logger.error(f"Failed to create temporary Stage J file for topic {topic_id}: {e}")
                continue
            
            topic_step2_output = self._step2_refine_questions(
                stage_j_path=temp_stage_j_path,
                word_file_path=word_file_path,
                step1_output_path=step1_topic_outputs[topic_id],
                prompt=prompt_2,
                model_name=model_name_2,
                output_dir=output_dir,
                progress_callback=progress_callback
            )
            
            # Clean up temporary file
            try:
                if os.path.exists(temp_stage_j_path):
                    os.remove(temp_stage_j_path)
            except:
                pass
            
            if topic_step2_output:
                step2_topic_outputs[topic_id] = topic_step2_output
                _progress(f"Step 2 completed for Topic '{topic_id}': {topic_step2_output}")
            else:
                self.logger.warning(f"Step 2 failed for Topic '{topic_id}', skipping...")
        
        if not step2_topic_outputs:
            self.logger.error("Step 2 failed for all topics")
            return None
        
        # Combine Step 2 outputs from all topics
        _progress("Combining Step 2 outputs from all topics...")
        step2_combined_data = []
        for topic_id, topic_step2_path in step2_topic_outputs.items():
            topic_step2_data = self.load_json_file(topic_step2_path)
            if topic_step2_data:
                topic_records = self.get_data_from_json(topic_step2_data)
                if topic_records:
                    step2_combined_data.extend(topic_records)
        
        if not step2_combined_data:
            self.logger.error("Failed to combine Step 2 outputs")
            return None
        
        # Save combined Step 2 output
        step2_combined_path = os.path.join(output_dir or os.path.dirname(stage_j_path) or os.getcwd(), 
                                          f"step2_combined_{book_id}{chapter_id:03d}.json")
        step2_combined_json = {
            "metadata": {
                "book_id": book_id,
                "chapter_id": chapter_id,
                "source": "Stage V - Step 2 (Combined from Topics)",
                "total_topics": len(step2_topic_outputs),
                "total_records": len(step2_combined_data)
            },
            "data": step2_combined_data
        }
        try:
            with open(step2_combined_path, 'w', encoding='utf-8') as f:
                json.dump(step2_combined_json, f, ensure_ascii=False, indent=2)
            _progress(f"Step 2 combined output saved to: {step2_combined_path}")
        except Exception as e:
            self.logger.error(f"Failed to save combined Step 2 output: {e}")
            return None
        
        step2_output = step2_combined_path
        
        # ========== STEP 3: Add QId Mapping ==========
        _progress("=" * 60)
        _progress("STEP 3: Adding QId mapping...")
        _progress("=" * 60)
        
        step3_output = self._step3_add_qid_mapping(
            step1_output_path=step1_output,
            step2_output_path=step2_output,
            stage_j_path=stage_j_path,
            prompt=prompt_3,
            model_name=model_name_3,
            book_id=book_id,
            chapter_id=chapter_id,
            output_dir=output_dir,
            progress_callback=progress_callback
        )
        
        if not step3_output:
            self.logger.error("Step 3 failed")
            return None
        
        _progress(f"Stage V completed successfully: {step3_output}")
        return step3_output
    
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
        
        # Prepare full prompt (only user prompt + data, no instructions)
        full_stage_j_json = json.dumps(stage_j_records_for_prompt, ensure_ascii=False, indent=2)
        full_prompt = f"""{prompt}

Word Document (Test Questions):
{word_content_formatted}

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
    
    def _step2_refine_questions(
        self,
        stage_j_path: str,
        word_file_path: str,
        step1_output_path: str,
        prompt: str,
        model_name: str,
        output_dir: Optional[str] = None,
        progress_callback: Optional[Callable[[str], None]] = None
    ) -> Optional[str]:
        """
        Step 2: Refine questions WITHOUT QId mapping.
        
        Args:
            stage_j_path: Path to Stage J JSON file (without Type column)
            word_file_path: Path to Word file containing test questions
            step1_output_path: Path to Step 1 output file (to keep it in output)
            prompt: Prompt for refinement (NO QId mapping)
            model_name: Gemini model name
            output_dir: Output directory
            progress_callback: Optional callback for progress updates
            
        Returns:
            Path to Step 2 output file or None on error
        """
        def _progress(msg: str):
            if progress_callback:
                progress_callback(msg)
            self.logger.info(msg)
        
        # Load Stage J JSON (without Type column)
        _progress("Loading Stage J JSON for Step 2...")
        stage_j_data = self.load_json_file(stage_j_path)
        if not stage_j_data:
            self.logger.error("Failed to load Stage J JSON")
            return None
        
        stage_j_records = self.get_data_from_json(stage_j_data)
        if not stage_j_records:
            self.logger.error("Stage J JSON has no data")
            return None
        
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
            }
            stage_j_records_for_prompt.append(clean_record)
        
        _progress(f"Prepared {len(stage_j_records_for_prompt)} Stage J records (without Type column)")
        
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
        
        # Load Step 1 output to keep it in final output
        _progress("Loading Step 1 output to include in Step 2 output...")
        step1_data = self.load_json_file(step1_output_path)
        step1_questions = []
        if step1_data:
            step1_questions = self.get_data_from_json(step1_data) or []
        _progress(f"Loaded {len(step1_questions)} questions from Step 1")
        
        # Prepare full prompt (only user prompt + data, no instructions)
        full_stage_j_json = json.dumps(stage_j_records_for_prompt, ensure_ascii=False, indent=2)
        full_prompt = f"""{prompt}

Word Document (Test Questions):
{word_content_formatted}

Stage J Data (all records, without Type column):
{full_stage_j_json}"""
        
        # Call model once and collect raw response
        all_raw_responses = []
        max_retries = 3
        
        _progress("Processing Stage V - Step 2")
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
                    # Remove QId if model accidentally added it (Step 2 should NOT have QId)
                    for question in part_refined:
                        if "QId" in question:
                            del question["QId"]
                        if "qId" in question:
                            del question["qId"]
                        if "QID" in question:
                            del question["QID"]
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
                
                # Remove QId if model accidentally added it (Step 2 should NOT have QId)
                for question in all_refined_questions:
                    if "QId" in question:
                        del question["QId"]
                    if "qId" in question:
                        del question["qId"]
                    if "QID" in question:
                        del question["QID"]
        
        if not all_refined_questions:
            self.logger.error("No refined questions generated in Step 2")
            return None
        
        _progress(f"Total refined questions from Step 2: {len(all_refined_questions)}")
        
        # Combine Step 1 and Step 2 questions
        all_combined_questions = step1_questions + all_refined_questions
        _progress(f"Total combined questions (Step 1 + Step 2): {len(all_combined_questions)}")
        
        # Save Step 2 output (contains Step 1 + Step 2 questions)
        if not output_dir:
            output_dir = os.path.dirname(stage_j_path) or os.getcwd()
        
        base_name, _ = os.path.splitext(os.path.basename(stage_j_path))
        step2_output_path = os.path.join(output_dir, f"{base_name}_stage_v_step2.json")
        
        step2_metadata = {
            "step": 2,
            "source_stage_j": os.path.basename(stage_j_path),
            "source_word_file": os.path.basename(word_file_path),
            "source_step1": os.path.basename(step1_output_path),
            "model_used": model_name,
            "total_questions_step1": len(step1_questions),
            "total_questions_step2": len(all_refined_questions),
            "total_questions_combined": len(all_combined_questions)
        }
        
        success = self.save_json_file(all_combined_questions, step2_output_path, step2_metadata, "V-Step2")
        if success:
            _progress(f"Step 2 output saved to: {step2_output_path}")
            return step2_output_path
        else:
            self.logger.error("Failed to save Step 2 output")
            return None
    
    def _step3_add_qid_mapping(
        self,
        step1_output_path: str,
        step2_output_path: str,
        stage_j_path: str,
        prompt: str,
        model_name: str,
        book_id: int,
        chapter_id: int,
        output_dir: Optional[str] = None,
        progress_callback: Optional[Callable[[str], None]] = None
    ) -> Optional[str]:
        """
        Step 3: Add QId mapping to questions from both Step 1 and Step 2.
        
        Args:
            step1_output_path: Path to Step 1 output file
            step2_output_path: Path to Step 2 output file (contains Step 1 + Step 2)
            stage_j_path: Path to Stage J JSON file (for PointId reference)
            prompt: Prompt for QId mapping
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
        
        # Load Stage J for PointId reference (without Type column)
        _progress("Loading Stage J JSON for PointId reference...")
        stage_j_data = self.load_json_file(stage_j_path)
        if not stage_j_data:
            return None
        
        stage_j_records = self.get_data_from_json(stage_j_data)
        if not stage_j_records:
            return None
        
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
            }
            stage_j_records_for_prompt.append(clean_record)
        
        _progress(f"Prepared {len(stage_j_records_for_prompt)} Stage J records for reference")
        
        # Prepare full prompt (only user prompt + data, no instructions)
        stage_j_sample = json.dumps(stage_j_records_for_prompt[:10], ensure_ascii=False, indent=2)
        
        full_prompt = f"""{prompt}

Stage J Data (for PointId reference, without Type column):
{stage_j_sample}
(Showing first 10 records as reference)"""
        
        # Call model
        all_raw_responses = []
        max_retries = 3
        
        _progress("Processing Stage V - Step 3")
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
                    _progress(f"Step 3 response received ({len(part_response)} characters)")
                    break
            except Exception as e:
                self.logger.warning(f"Step 3 attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    _progress(f"Retrying Step 3... (attempt {attempt + 2}/{max_retries})")
                else:
                    self.logger.error("All Step 3 attempts failed")
        
        if not part_response:
            self.logger.error("No response from model in Step 3")
            return None
        
        all_raw_responses.append(part_response)
        
        # Save raw response to TXT
        base_dir = os.path.dirname(stage_j_path) or os.getcwd()
        base_name, _ = os.path.splitext(os.path.basename(stage_j_path))
        step3_txt_path = os.path.join(base_dir, f"{base_name}_stage_v_step3.txt")
        
        try:
            with open(step3_txt_path, 'w', encoding='utf-8') as f:
                for idx, response in enumerate(all_raw_responses, 1):
                    f.write(f"=== PART {idx} RESPONSE ===\n")
                    f.write(response)
                    f.write("\n\n")
            _progress(f"Saved raw model responses to: {step3_txt_path}")
        except Exception as e:
            self.logger.warning(f"Failed to save TXT file: {e}")
        
        # Now load and extract JSON from all three TXT files (Step 1, Step 2, Step 3)
        _progress("=" * 60)
        _progress("Loading and extracting JSON from all three TXT files...")
        _progress("=" * 60)
        
        # Step 1 TXT file path
        step1_txt_path = os.path.join(base_dir, f"{base_name}_stage_v_step1.txt")
        step1_questions = []
        if os.path.exists(step1_txt_path):
            _progress(f"Loading Step 1 TXT: {step1_txt_path}")
            step1_output = self.load_txt_as_json(step1_txt_path)
            if step1_output:
                if isinstance(step1_output, list):
                    step1_questions = step1_output
                elif isinstance(step1_output, dict):
                    step1_questions = self.get_data_from_json(step1_output) or []
            _progress(f"Extracted {len(step1_questions)} questions from Step 1 TXT")
        else:
            _progress(f"Warning: Step 1 TXT file not found: {step1_txt_path}")
        
        # Step 2 TXT file path
        step2_txt_path = os.path.join(base_dir, f"{base_name}_stage_v_step2.txt")
        step2_questions = []
        if os.path.exists(step2_txt_path):
            _progress(f"Loading Step 2 TXT: {step2_txt_path}")
            step2_output = self.load_txt_as_json(step2_txt_path)
            if step2_output:
                if isinstance(step2_output, list):
                    step2_questions = step2_output
                elif isinstance(step2_output, dict):
                    step2_questions = self.get_data_from_json(step2_output) or []
            _progress(f"Extracted {len(step2_questions)} questions from Step 2 TXT")
        else:
            _progress(f"Warning: Step 2 TXT file not found: {step2_txt_path}")
        
        # Step 3 TXT file path (just saved)
        step3_questions = []
        if os.path.exists(step3_txt_path):
            _progress(f"Loading Step 3 TXT: {step3_txt_path}")
            step3_output = self.load_txt_as_json(step3_txt_path)
            if step3_output:
                if isinstance(step3_output, list):
                    step3_questions = step3_output
                elif isinstance(step3_output, dict):
                    step3_questions = self.get_data_from_json(step3_output) or []
            _progress(f"Extracted {len(step3_questions)} questions from Step 3 TXT")
        else:
            _progress(f"Warning: Step 3 TXT file not found: {step3_txt_path}")
        
        # Combine all questions from Step 1, Step 2, and Step 3
        # Priority: Step 3 > Step 2 > Step 1 (if TestID is duplicate, keep from higher priority step)
        _progress("Combining questions and removing duplicate TestIDs (priority: Step 3 > Step 2 > Step 1)...")
        
        # Use dictionary to track questions by TestID, with priority
        questions_by_testid = {}
        
        # First add Step 1 questions
        for question in step1_questions:
            test_id = question.get("TestID")
            if test_id is not None:
                questions_by_testid[test_id] = question
        
        _progress(f"Added {len(questions_by_testid)} questions from Step 1")
        
        # Then add Step 2 questions (will overwrite Step 1 if TestID duplicate)
        step2_added = 0
        step2_overwritten = 0
        for question in step2_questions:
            test_id = question.get("TestID")
            if test_id is not None:
                if test_id in questions_by_testid:
                    step2_overwritten += 1
                questions_by_testid[test_id] = question
                step2_added += 1
        
        _progress(f"Added {step2_added} questions from Step 2 ({step2_overwritten} overwrote Step 1)")
        
        # Finally add Step 3 questions (will overwrite Step 1 and Step 2 if TestID duplicate)
        step3_added = 0
        step3_overwritten = 0
        for question in step3_questions:
            test_id = question.get("TestID")
            if test_id is not None:
                if test_id in questions_by_testid:
                    step3_overwritten += 1
                questions_by_testid[test_id] = question
                step3_added += 1
        
        _progress(f"Added {step3_added} questions from Step 3 ({step3_overwritten} overwrote previous steps)")
        
        # Convert dictionary values to list
        all_questions_combined = list(questions_by_testid.values())
        _progress(f"Total unique questions after removing duplicates: {len(all_questions_combined)} (Step 1: {len(step1_questions)}, Step 2: {len(step2_questions)}, Step 3: {len(step3_questions)})")
        
        if not all_questions_combined:
            self.logger.error("No questions available to process in Step 3")
            return None
        
        # Reassign TestID to be sequential (1, 2, 3, ...) and ensure QIds are sequential
        _progress("Reassigning TestIDs sequentially and fixing QIds...")
        qid_counter = 1
        for idx, question in enumerate(all_questions_combined, 1):
            # Reassign TestID sequentially
            question["TestID"] = idx
            # Ensure QId is numeric and sequential
            qid = f"{book_id:03d}{chapter_id:03d}{qid_counter:04d}"
            question["QId"] = qid
            qid_counter += 1
        
        all_questions_with_qid = all_questions_combined
        
        # Save final output (b{book}{chapter}.json)
        if not output_dir:
            output_dir = os.path.dirname(stage_j_path) or os.getcwd()
        
        output_path = self.generate_filename("b", book_id, chapter_id, output_dir)
        
        output_metadata = {
            "book_id": book_id,
            "chapter_id": chapter_id,
            "source_stage_j": os.path.basename(stage_j_path),
            "source_step1": os.path.basename(step1_output_path),
            "source_step2": os.path.basename(step2_output_path),
            "model_step1": "N/A",
            "model_step2": "N/A",
            "model_step3": model_name,
            "total_questions_step1": len(step1_questions),
            "total_questions_step2": len(step2_questions),
            "total_questions_step3": len(step3_questions),
            "total_questions": len(all_questions_with_qid)
        }
        
        success = self.save_json_file(all_questions_with_qid, output_path, output_metadata, "V")
        if success:
            _progress(f"Final output saved to: {output_path}")
            return output_path
        else:
            self.logger.error("Failed to save final output")
            return None


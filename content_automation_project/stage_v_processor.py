"""
Stage V Processor: Test File Generation
Generates test files from Stage J data and Word document in two steps:
Step 1: Generate initial test questions (per topic, with full Stage J)
Step 2: Refine questions and add QId (per topic, with full Stage J)
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
        ocr_extraction_json_path: str,
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
            ocr_extraction_json_path: Path to OCR Extraction JSON file (for topic structure)
            prompt_1: Prompt for Step 1 (initial test questions generation, should contain {Topic_NAME} and {Subchapter_Name})
            model_name_1: Gemini model name for Step 1
            prompt_2: Prompt for Step 2 (refine questions and add QId, should contain {Topic_NAME} and {Subchapter_Name})
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
        
        # Set stage if using UnifiedAPIClient (for API routing)
        if hasattr(self.api_client, 'set_stage'):
            self.api_client.set_stage("stage_v")
        
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
        
        # Load OCR Extraction JSON to extract topics structure
        _progress("Loading OCR Extraction JSON for topic structure...")
        if not os.path.exists(ocr_extraction_json_path):
            self.logger.error(f"OCR Extraction JSON file not found: {ocr_extraction_json_path}")
            return None
        
        try:
            with open(ocr_extraction_json_path, "r", encoding="utf-8") as f:
                ocr_data = json.load(f)
        except Exception as e:
            self.logger.error(f"Failed to load OCR Extraction JSON file {ocr_extraction_json_path}: {e}")
            return None
        
        # Extract chapters structure from OCR Extraction JSON
        chapters = ocr_data.get("chapters", [])
        if not chapters:
            self.logger.error("OCR Extraction JSON has no 'chapters' structure")
            return None
        
        # Extract chapter name from OCR JSON input
        chapter_name_from_input = ocr_data.get("metadata", {}).get("chapter", "")
        if not chapter_name_from_input and chapters:
            chapter_name_from_input = chapters[0].get("chapter", "")
        
        if not chapter_name_from_input:
            self.logger.warning("No chapter name found in OCR Extraction JSON, will use empty string")
        
        self.logger.info(f"Chapter name from OCR Extraction JSON: {chapter_name_from_input}")
        
        # Collect all topics with their subchapter info (similar to Document Processing)
        topics_list = []  # List of (subchapter_name, topic_name) tuples
        subchapters_dict = {}  # subchapter_name -> list of topic names (for logging)
        subchapter_full_topics: Dict[str, List[str]] = {}  # subchapter_name -> list of topic names (for context)
        
        for chapter in chapters:
            if not isinstance(chapter, dict):
                continue
            subchapters = chapter.get("subchapters", [])
            
            for subchapter in subchapters:
                if not isinstance(subchapter, dict):
                    continue
                subchapter_name = subchapter.get("subchapter", "")
                topics = subchapter.get("topics", [])
                
                if not subchapter_name:
                    continue
                
                # Initialize structures for this subchapter if not exists
                if subchapter_name not in subchapters_dict:
                    subchapters_dict[subchapter_name] = []
                if subchapter_name not in subchapter_full_topics:
                    subchapter_full_topics[subchapter_name] = []
                
                # Collect each topic individually
                for topic in topics:
                    if not isinstance(topic, dict):
                        continue
                    topic_name = topic.get("topic", "")
                    
                    if topic_name:
                        # For per-topic processing
                        topics_list.append((subchapter_name, topic_name))
                        # For logging
                        subchapters_dict[subchapter_name].append(topic_name)
                        # For context (all topics in subchapter)
                        subchapter_full_topics[subchapter_name].append(topic_name)
        
        if not topics_list:
            self.logger.error("No topics found in OCR Extraction JSON")
            return None
        
        # Count unique subchapters
        unique_subchapters = set(subchapter_name for subchapter_name, _ in topics_list)
        
        self.logger.info("=" * 80)
        self.logger.info("DOCUMENT PROCESSING - PROCESSING BY TOPIC")
        self.logger.info("=" * 80)
        self.logger.info(f"Found {len(topics_list)} topics to process")
        self.logger.info(f"Found {len(unique_subchapters)} unique subchapters")
        self.logger.info("")
        
        # Log subchapters and their topics
        subchapter_idx_map = {}
        for idx, subchapter_name in enumerate(sorted(unique_subchapters), 1):
            subchapter_idx_map[subchapter_name] = idx
            topic_names = subchapters_dict[subchapter_name]
            self.logger.info(f"  Subchapter {idx}: '{subchapter_name}' - {len(topic_names)} topics")
            for topic_idx, topic_name in enumerate(topic_names, 1):
                self.logger.info(f"    Topic {topic_idx}: '{topic_name}'")
        self.logger.info("=" * 80)
        
        if progress_callback:
            progress_callback(f"Found {len(topics_list)} topics in {len(unique_subchapters)} subchapters to process")
        
        # Prepare full Stage J data (all records) for model input
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
        
        full_stage_j_json = json.dumps(stage_j_records_for_prompt, ensure_ascii=False, indent=2)
        
        # ========== STEP 1: Generate Initial Test Questions (per Topic) ==========
        _progress("=" * 60)
        _progress("STEP 1: Generating initial test questions (per Topic, with full Stage J)...")
        _progress("=" * 60)
        
        step1_topic_outputs: Dict[str, str] = {}  # (subchapter_name, topic_name) -> output_path
        
        for topic_idx, (subchapter_name, topic_name) in enumerate(topics_list, 1):
            _progress(f"[{topic_idx}/{len(topics_list)}] Processing Step 1 for Topic '{topic_name}' (Subchapter: '{subchapter_name}')...")
            self.logger.info("")
            self.logger.info("=" * 80)
            self.logger.info(f"PROCESSING TOPIC {topic_idx}/{len(topics_list)}: '{topic_name}'")
            self.logger.info("=" * 80)
            self.logger.info(f"  Subchapter: '{subchapter_name}'")
            self.logger.info(f"  Topic: '{topic_name}'")
            self.logger.info(f"  Total Stage J records: {len(stage_j_records)}")
            
            topic_key = (subchapter_name, topic_name)
            topic_step1_output = self._step1_generate_initial_questions(
                stage_j_path=stage_j_path,
                word_file_path=word_file_path,
                full_stage_j_json=full_stage_j_json,
                current_topic_name=topic_name,
                current_topic_subchapter=subchapter_name,
                prompt=prompt_1,
                model_name=model_name_1,
                topic_idx=topic_idx,
                total_topics=len(topics_list),
                output_dir=output_dir,
                progress_callback=progress_callback
            )
            
            if topic_step1_output:
                step1_topic_outputs[topic_key] = topic_step1_output
                _progress(f"Step 1 completed for Topic '{topic_name}': {topic_step1_output}")
                self.logger.info(f"  ✓ Step 1 completed for Topic '{topic_name}'")
            else:
                self.logger.warning(f"  ✗ Step 1 failed for Topic '{topic_name}', skipping...")
        
        if not step1_topic_outputs:
            self.logger.error("Step 1 failed for all topics")
            return None
        
        # Combine Step 1 outputs from all topics
        _progress("Combining Step 1 outputs from all topics...")
        self.logger.info(f"Combining Step 1 outputs from {len(step1_topic_outputs)} topics...")
        step1_combined_data = []
        for (subchapter_name, topic_name), topic_step1_path in step1_topic_outputs.items():
            topic_step1_data = self.load_json_file(topic_step1_path)
            if topic_step1_data:
                topic_records = self.get_data_from_json(topic_step1_data)
                if topic_records:
                    step1_combined_data.extend(topic_records)
                    self.logger.info(f"  Added {len(topic_records)} questions from Topic '{topic_name}' (Subchapter: '{subchapter_name}')")
        
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
                "total_questions": len(step1_combined_data)
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
        
        # ========== STEP 2: Refine Questions and Add QId (per Topic) ==========
        _progress("=" * 60)
        _progress("STEP 2: Refining questions and adding QId (per Topic, with full Stage J)...")
        _progress("=" * 60)
        
        step2_topic_outputs: Dict[str, str] = {}  # (subchapter_name, topic_name) -> output_path
        global_qid_counter = 1  # Global counter for QId across all topics
        
        for topic_idx, (subchapter_name, topic_name) in enumerate(topics_list, 1):
            topic_key = (subchapter_name, topic_name)
            if topic_key not in step1_topic_outputs:
                continue
            
            _progress(f"[{topic_idx}/{len(topics_list)}] Processing Step 2 for Topic '{topic_name}' (Subchapter: '{subchapter_name}')...")
            self.logger.info("")
            self.logger.info("=" * 80)
            self.logger.info(f"PROCESSING TOPIC {topic_idx}/{len(topics_list)}: '{topic_name}'")
            self.logger.info("=" * 80)
            self.logger.info(f"  Subchapter: '{subchapter_name}'")
            self.logger.info(f"  Topic: '{topic_name}'")
            self.logger.info(f"  Total Stage J records: {len(stage_j_records)}")
            
            topic_step2_output, num_questions = self._step2_refine_questions_and_add_qid(
                stage_j_path=stage_j_path,
                word_file_path=word_file_path,
                full_stage_j_json=full_stage_j_json,
                current_topic_name=topic_name,
                current_topic_subchapter=subchapter_name,
                step1_output_path=step1_topic_outputs[topic_key],
                prompt=prompt_2,
                model_name=model_name_2,
                book_id=book_id,
                chapter_id=chapter_id,
                topic_idx=topic_idx,
                total_topics=len(topics_list),
                qid_start_counter=global_qid_counter,
                output_dir=output_dir,
                progress_callback=progress_callback
            )
            
            if topic_step2_output:
                step2_topic_outputs[topic_key] = topic_step2_output
                global_qid_counter += num_questions  # Update global counter
                _progress(f"Step 2 completed for Topic '{topic_name}': {topic_step2_output}")
                self.logger.info(f"  ✓ Step 2 completed for Topic '{topic_name}' ({num_questions} questions, QId range: {global_qid_counter - num_questions} to {global_qid_counter - 1})")
            else:
                self.logger.warning(f"  ✗ Step 2 failed for Topic '{topic_name}', skipping...")
        
        if not step2_topic_outputs:
            self.logger.error("Step 2 failed for all topics")
            return None
        
        # Combine Step 2 outputs from all topics
        _progress("Combining Step 2 outputs from all topics...")
        self.logger.info(f"Combining Step 2 outputs from {len(step2_topic_outputs)} topics...")
        step2_combined_data = []
        for (subchapter_name, topic_name), topic_step2_path in step2_topic_outputs.items():
            topic_step2_data = self.load_json_file(topic_step2_path)
            if topic_step2_data:
                topic_records = self.get_data_from_json(topic_step2_data)
                if topic_records:
                    step2_combined_data.extend(topic_records)
                    self.logger.info(f"  Added {len(topic_records)} questions from Topic '{topic_name}' (Subchapter: '{subchapter_name}')")
        
        if not step2_combined_data:
            self.logger.error("Failed to combine Step 2 outputs")
            return None
        
        # Extract chapter name from OCR Extraction JSON
        chapter_name = ""
        if ocr_data:
            ocr_metadata = ocr_data.get("metadata", {})
            chapter_name = (
                ocr_metadata.get("chapter", "") or
                ocr_metadata.get("Chapter", "") or
                ocr_metadata.get("chapter_name", "") or
                ocr_metadata.get("Chapter_Name", "") or
                ""
            )
            # If not found in metadata, try to get from chapters structure
            if not chapter_name and chapters:
                chapter_name = chapters[0].get("chapter", "")
        
        # Clean chapter name for filename (remove invalid characters)
        if chapter_name:
            import re
            # Replace spaces and invalid filename characters with underscore
            chapter_name_clean = re.sub(r'[<>:"/\\|?*]', '_', chapter_name)
            chapter_name_clean = chapter_name_clean.replace(' ', '_')
            # Remove multiple underscores
            chapter_name_clean = re.sub(r'_+', '_', chapter_name_clean)
            # Remove leading/trailing underscores
            chapter_name_clean = chapter_name_clean.strip('_')
        else:
            chapter_name_clean = ""
        
        if chapter_name_clean:
            _progress(f"Detected Chapter Name: {chapter_name}")
        else:
            _progress("No chapter name found, using empty string")
        
        # Save final output (b{book}{chapter}+namechapter.json)
        output_dir_final = output_dir or os.path.dirname(stage_j_path) or os.getcwd()
        if chapter_name_clean:
            base_filename = f"b{book_id:03d}{chapter_id:03d}+{chapter_name_clean}.json"
        else:
            # Fallback if no chapter name: use timestamp to avoid overwriting
            from datetime import datetime
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            base_filename = f"b{book_id:03d}{chapter_id:03d}+{timestamp}.json"
            self.logger.warning(f"No chapter name found, using timestamp in filename: {timestamp}")
        
        output_path = os.path.join(output_dir_final, base_filename)
        
        # Check if file already exists and add counter if needed
        if os.path.exists(output_path) and chapter_name_clean:
            # If file exists and we have chapter name, add counter
            counter = 1
            while os.path.exists(output_path):
                base_filename = f"b{book_id:03d}{chapter_id:03d}+{chapter_name_clean}_{counter}.json"
                output_path = os.path.join(output_dir_final, base_filename)
                counter += 1
            if counter > 1:
                self.logger.info(f"File already exists, using counter: {counter - 1}")
        
        output_metadata = {
            "book_id": book_id,
            "chapter_id": chapter_id,
            "source_stage_j": os.path.basename(stage_j_path),
            "source_ocr_extraction": os.path.basename(ocr_extraction_json_path),
            "model_step1": model_name_1,
            "model_step2": model_name_2,
            "total_topics": len(step2_topic_outputs),
            "total_questions": len(step2_combined_data)
        }
        
        success = self.save_json_file(step2_combined_data, output_path, output_metadata, "V")
        if success:
            _progress(f"Final output saved to: {output_path}")
            return output_path
        else:
            self.logger.error("Failed to save final output")
            return None
    
    def _step1_generate_initial_questions(
        self,
        stage_j_path: str,
        word_file_path: str,
        full_stage_j_json: str,
        current_topic_name: str,
        current_topic_subchapter: str,
        prompt: str,
        model_name: str,
        topic_idx: int,
        total_topics: int,
        output_dir: Optional[str] = None,
        progress_callback: Optional[Callable[[str], None]] = None
    ) -> Optional[str]:
        """
        Step 1: Generate initial test questions from Stage J and Word document.
        Uses FULL Stage J data but focuses on current topic.
        
        Args:
            stage_j_path: Path to Stage J JSON file
            word_file_path: Path to Word file
            full_stage_j_json: Full Stage J JSON string (all records)
            current_topic_name: Current topic name to focus on
            current_topic_subchapter: Current topic's subchapter
            prompt: Prompt for initial question generation (should contain {Topic_NAME} and {Subchapter_Name})
            model_name: Gemini model name
            topic_idx: Current topic index (1-based)
            total_topics: Total number of topics
            output_dir: Output directory
            progress_callback: Optional callback for progress updates
            
        Returns:
            Path to Step 1 output file or None on error
        """
        def _progress(msg: str):
            if progress_callback:
                progress_callback(msg)
            self.logger.info(msg)
        
        _progress(f"Loading Word document for Topic {topic_idx}/{total_topics}...")
        
        # Load Word file
        word_content = self.word_processor.read_word_file(word_file_path)
        if not word_content:
            self.logger.error("Failed to read Word file")
            return None
        
        word_content_formatted = self.word_processor.prepare_word_for_model(
            word_content,
            context="Test Questions"
        )
        
        # Replace placeholders in prompt with current topic and subchapter
        topic_prompt = prompt.replace("{Topic_NAME}", current_topic_name)
        topic_prompt = topic_prompt.replace("{Subchapter_Name}", current_topic_subchapter)
        
        # Prepare full prompt with FULL Stage J data
        full_prompt = f"""{topic_prompt}

Word Document (Test Questions):
{word_content_formatted}

Stage J Data (FULL file - all records, without Type column):
{full_stage_j_json}

IMPORTANT: Focus on generating test questions for the topic: "{current_topic_name}" (Subchapter: "{current_topic_subchapter}")."""
        
        # Call model once and collect raw response
        all_raw_responses = []
        max_retries = 3
        
        _progress(f"Processing Stage V - Step 1 for Topic {topic_idx}/{total_topics} (full file)...")
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
                    _progress(f"Step 1 response received for Topic {topic_idx} ({len(part_response)} characters)")
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
        safe_topic_name = current_topic_name.replace('/', '_').replace(' ', '_').replace('\\', '_')
        safe_topic_name = ''.join(c for c in safe_topic_name if c.isalnum() or c in ('_', '-', '.'))
        txt_path = os.path.join(base_dir, f"{base_name}_stage_v_step1_topic_{topic_idx}_{safe_topic_name}.txt")
        
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
            self.logger.error(f"No questions generated in Step 1 for Topic {topic_idx}")
            return None
        
        _progress(f"Total questions generated in Step 1 for Topic {topic_idx}: {len(all_questions)}")
        
        # Save Step 1 output
        if not output_dir:
            output_dir = os.path.dirname(stage_j_path) or os.getcwd()
        
        base_name, _ = os.path.splitext(os.path.basename(stage_j_path))
        step1_output_path = os.path.join(output_dir, f"{base_name}_stage_v_step1_topic_{topic_idx}_{safe_topic_name}.json")
        
        step1_metadata = {
            "step": 1,
            "topic": current_topic_name,
            "subchapter": current_topic_subchapter,
            "topic_idx": topic_idx,
            "total_topics": total_topics,
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
    
    def _step2_refine_questions_and_add_qid(
        self,
        stage_j_path: str,
        word_file_path: str,
        full_stage_j_json: str,
        current_topic_name: str,
        current_topic_subchapter: str,
        step1_output_path: str,
        prompt: str,
        model_name: str,
        book_id: int,
        chapter_id: int,
        topic_idx: int,
        total_topics: int,
        qid_start_counter: int,
        output_dir: Optional[str] = None,
        progress_callback: Optional[Callable[[str], None]] = None
    ) -> tuple[Optional[str], int]:
        """
        Step 2: Refine questions and add QId mapping.
        Uses FULL Stage J data but focuses on current topic.
        
        Args:
            stage_j_path: Path to Stage J JSON file
            word_file_path: Path to Word file
            full_stage_j_json: Full Stage J JSON string (all records)
            current_topic_name: Current topic name to focus on
            current_topic_subchapter: Current topic's subchapter
            step1_output_path: Path to Step 1 output file for this topic
            prompt: Prompt for refinement (should contain {Topic_NAME} and {Subchapter_Name})
            model_name: Gemini model name
            book_id: Book ID
            chapter_id: Chapter ID
            topic_idx: Current topic index (1-based)
            total_topics: Total number of topics
            qid_start_counter: Starting QId counter for this topic (to maintain global sequence)
            output_dir: Output directory
            progress_callback: Optional callback for progress updates
            
        Returns:
            Tuple of (Path to Step 2 output file or None on error, number of questions processed)
        """
        def _progress(msg: str):
            if progress_callback:
                progress_callback(msg)
            self.logger.info(msg)
        
        _progress(f"Loading Word document for Topic {topic_idx}/{total_topics}...")
        
        # Load Word file
        word_content = self.word_processor.read_word_file(word_file_path)
        if not word_content:
            self.logger.error("Failed to read Word file")
            return None
        
        word_content_formatted = self.word_processor.prepare_word_for_model(
            word_content,
            context="Test Questions"
        )
        
        # Load Step 1 output for this topic
        _progress("Loading Step 1 output...")
        step1_data = self.load_json_file(step1_output_path)
        step1_questions = []
        if step1_data:
            step1_questions = self.get_data_from_json(step1_data) or []
        _progress(f"Loaded {len(step1_questions)} questions from Step 1")
        
        # Replace placeholders in prompt with current topic and subchapter
        topic_prompt = prompt.replace("{Topic_NAME}", current_topic_name)
        topic_prompt = topic_prompt.replace("{Subchapter_Name}", current_topic_subchapter)
        
        # Prepare full prompt with FULL Stage J data
        full_prompt = f"""{topic_prompt}

Word Document (Test Questions):
{word_content_formatted}

Stage J Data (FULL file - all records, without Type column):
{full_stage_j_json}

IMPORTANT: Focus on refining test questions for the topic: "{current_topic_name}" (Subchapter: "{current_topic_subchapter}")."""
        
        # Call model once and collect raw response
        all_raw_responses = []
        max_retries = 3
        
        _progress(f"Processing Stage V - Step 2 for Topic {topic_idx}/{total_topics} (full file)...")
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
                    _progress(f"Step 2 response received for Topic {topic_idx} ({len(part_response)} characters)")
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
        safe_topic_name = current_topic_name.replace('/', '_').replace(' ', '_').replace('\\', '_')
        safe_topic_name = ''.join(c for c in safe_topic_name if c.isalnum() or c in ('_', '-', '.'))
        txt_path = os.path.join(base_dir, f"{base_name}_stage_v_step2_topic_{topic_idx}_{safe_topic_name}.txt")
        
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
            self.logger.error(f"No refined questions generated in Step 2 for Topic {topic_idx}")
            return None, 0
        
        _progress(f"Total refined questions from Step 2 for Topic {topic_idx}: {len(all_refined_questions)}")
        
        # Combine Step 1 and Step 2 questions
        all_combined_questions = step1_questions + all_refined_questions
        _progress(f"Total combined questions (Step 1 + Step 2) for Topic {topic_idx}: {len(all_combined_questions)}")
        
        # Add QId to all questions after receiving model response
        _progress(f"Adding QId to all questions for Topic {topic_idx} (starting from QId counter {qid_start_counter})...")
        qid_counter = qid_start_counter
        for idx, question in enumerate(all_combined_questions, 1):
            # Reassign TestID sequentially (local to this topic)
            question["TestID"] = idx
            # Generate QId: BBBCCCPPPP format (global sequence across all topics)
            qid = f"{book_id:03d}{chapter_id:03d}{qid_counter:04d}"
            question["QId"] = qid
            qid_counter += 1
        
        num_questions = len(all_combined_questions)
        _progress(f"Added QId to {num_questions} questions for Topic {topic_idx} (QId range: {qid_start_counter} to {qid_counter - 1})")
        
        # Save Step 2 output
        if not output_dir:
            output_dir = os.path.dirname(stage_j_path) or os.getcwd()
        
        base_name, _ = os.path.splitext(os.path.basename(stage_j_path))
        step2_output_path = os.path.join(output_dir, f"{base_name}_stage_v_step2_topic_{topic_idx}_{safe_topic_name}.json")
        
        step2_metadata = {
            "step": 2,
            "topic": current_topic_name,
            "subchapter": current_topic_subchapter,
            "topic_idx": topic_idx,
            "total_topics": total_topics,
            "source_stage_j": os.path.basename(stage_j_path),
            "source_word_file": os.path.basename(word_file_path),
            "source_step1": os.path.basename(step1_output_path),
            "model_used": model_name,
            "book_id": book_id,
            "chapter_id": chapter_id,
            "total_questions_step1": len(step1_questions),
            "total_questions_step2": len(all_refined_questions),
            "total_questions_combined": len(all_combined_questions)
        }
        
        success = self.save_json_file(all_combined_questions, step2_output_path, step2_metadata, "V-Step2")
        if success:
            _progress(f"Step 2 output saved to: {step2_output_path}")
            return step2_output_path, num_questions
        else:
            self.logger.error("Failed to save Step 2 output")
            return None, 0
    


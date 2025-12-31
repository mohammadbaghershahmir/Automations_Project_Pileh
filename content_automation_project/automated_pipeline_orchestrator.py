"""
Automated Pipeline Orchestrator

This module provides a complete automated pipeline that:
1. Automatically executes Stage 3 and Stage 4 (no manual intervention)
2. Executes Stage J and Stage V (with Word file validation)
3. Tracks errors per stage and continues processing
4. Reports which stages failed to the user
5. Does NOT modify existing code structure - uses existing processors as-is

The orchestrator is completely separate from the existing GUI workflow.
"""

import json
import logging
import os
from datetime import datetime
from typing import Optional, Dict, List, Any, Callable, Tuple
from enum import Enum

from third_stage_chunk_processor import run_third_stage_chunked
from third_stage_converter import ThirdStageConverter
from stage_e_processor import StageEProcessor
from stage_f_processor import StageFProcessor
from stage_j_processor import StageJProcessor
from stage_v_processor import StageVProcessor
from stage_x_processor import StageXProcessor
from stage_y_processor import StageYProcessor
from stage_z_processor import StageZProcessor
from word_file_processor import WordFileProcessor


class StageStatus(Enum):
    """Status of a stage execution"""
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


class StageResult:
    """Result of a stage execution"""
    
    def __init__(self, stage_name: str):
        self.stage_name = stage_name
        self.status = StageStatus.PENDING
        self.output_path: Optional[str] = None
        self.error_message: Optional[str] = None
        self.start_time: Optional[datetime] = None
        self.end_time: Optional[datetime] = None
    
    def mark_success(self, output_path: str):
        """Mark stage as successful"""
        self.status = StageStatus.SUCCESS
        self.output_path = output_path
        self.end_time = datetime.now()
    
    def mark_failed(self, error_message: str):
        """Mark stage as failed"""
        self.status = StageStatus.FAILED
        self.error_message = error_message
        self.end_time = datetime.now()
    
    def mark_skipped(self, reason: str):
        """Mark stage as skipped"""
        self.status = StageStatus.SKIPPED
        self.error_message = reason
        self.end_time = datetime.now()
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for JSON serialization"""
        return {
            "stage_name": self.stage_name,
            "status": self.status.value,
            "output_path": self.output_path,
            "error_message": self.error_message,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "duration_seconds": (self.end_time - self.start_time).total_seconds() if self.start_time and self.end_time else None
        }


class AutomatedPipelineOrchestrator:
    """
    Automated pipeline orchestrator that executes stages sequentially
    with error tracking and reporting.
    """
    
    def __init__(self, api_client):
        """
        Initialize the orchestrator.
        
        Args:
            api_client: GeminiAPIClient instance
        """
        self.api_client = api_client
        self.logger = logging.getLogger(__name__)
        
        # Initialize processors
        self.stage_e_processor = StageEProcessor(api_client)
        self.stage_f_processor = StageFProcessor(api_client)
        self.stage_j_processor = StageJProcessor(api_client)
        self.stage_v_processor = StageVProcessor(api_client)
        self.stage_x_processor = StageXProcessor(api_client)
        self.stage_y_processor = StageYProcessor(api_client)
        self.stage_z_processor = StageZProcessor(api_client)
        self.word_processor = WordFileProcessor()
        self.converter = ThirdStageConverter()
        
        # Results tracking
        self.stage_results: Dict[str, StageResult] = {}
    
    def run_automated_pipeline(
        self,
        # Stage 1 inputs (PDF processing)
        pdf_path: str,
        stage1_prompt: str,
        stage1_model: str,
        
        # Stage 2 configuration
        stage2_prompt: str,
        stage2_model: str,
        chapter_name: str,
        
        # Stage 3 configuration
        stage3_prompt: str,
        stage3_model: str,
        
        # Stage 4 configuration
        stage4_prompt: str,
        stage4_model: str,
        
        # PointId
        start_pointid: str,  # 10 digits like "1050030001"
        
        # Stage E configuration
        stage_e_prompt: str,
        stage_e_model: str,
        
        # Stage J configuration
        word_file_path: Optional[str] = None,
        stage_j_prompt: Optional[str] = None,
        stage_j_model: Optional[str] = None,
        stage_f_json_path: Optional[str] = None,
        
        # Stage V configuration
        stage_v_prompt_1: Optional[str] = None,
        stage_v_model_1: Optional[str] = None,
        stage_v_prompt_2: Optional[str] = None,
        stage_v_model_2: Optional[str] = None,
        
        # Output directory
        output_dir: Optional[str] = None,
        
        # Progress callback
        progress_callback: Optional[Callable[[str], None]] = None,
        
        # Resume from stage (optional)
        resume_from_stage: Optional[str] = None,
        
    ) -> Dict[str, StageResult]:
        """
        Run the complete automated pipeline starting from PDF.
        
        Pipeline flow:
        1. Stage 1: Process PDF → Stage 1 JSON
        2. Stage 2: Process Stage 1 per Part → Stage 2 JSON
        3. Stage 3: Process Stage 2 JSON → Stage 3 JSON
        4. Stage 4: Process Stage 3 JSON → Stage 4 JSON (with PointId)
        5. Stage E: Process Stage 4 + Stage 1 → Stage E JSON
        6. Stage F: Process Stage E → Stage F JSON
        7. Stage J: Process Stage E + Word file → Stage J JSON (if Word file exists)
        8. Stage V: Process Stage J + Word file → Stage V JSON (if Word file exists)
        
        Args:
            pdf_path: Path to PDF file
            stage1_prompt: Prompt for Stage 1 (PDF processing)
            stage1_model: Model for Stage 1
            stage2_prompt: Prompt for Stage 2 (per-Part processing)
            stage2_model: Model for Stage 2
            chapter_name: Chapter name
            stage3_prompt: Prompt for Stage 3
            stage3_model: Model for Stage 3
            stage4_prompt: Prompt for Stage 4
            stage4_model: Model for Stage 4
            start_pointid: Starting PointID (10 digits, e.g., "1050030001")
            stage_e_prompt: Prompt for Stage E
            stage_e_model: Model for Stage E
            word_file_path: Optional path to Word file (required for Stage J and V)
            stage_j_prompt: Optional prompt for Stage J
            stage_j_model: Optional model for Stage J
            stage_f_json_path: Optional path to Stage F JSON
            stage_v_prompt_1: Optional prompt for Stage V Step 1
            stage_v_model_1: Optional model for Stage V Step 1
            stage_v_prompt_2: Optional prompt for Stage V Step 2
            stage_v_model_2: Optional model for Stage V Step 2
            output_dir: Output directory (defaults to PDF directory)
            progress_callback: Optional callback for progress updates
            resume_from_stage: Optional stage name to resume from (e.g., "J", "V")
            
        Returns:
            Dictionary mapping stage names to StageResult objects
        """
        def _progress(msg: str):
            if progress_callback:
                progress_callback(msg)
            self.logger.info(msg)
        
        # Validate and extract PointID
        try:
            if not start_pointid or len(start_pointid) != 10 or not start_pointid.isdigit():
                raise ValueError(f"Invalid PointId format: {start_pointid} (must be 10 digits)")
            book_id = int(start_pointid[0:3])
            chapter_id = int(start_pointid[3:6])
            start_index = int(start_pointid[6:10])
        except ValueError as e:
            error_msg = f"Invalid start_pointid format: {str(e)}"
            self.logger.error(error_msg)
            _progress(f"Error: {error_msg}")
            return self.stage_results
        
        # Initialize results
        self.stage_results = {}
        
        # Determine output directory
        if not output_dir:
            output_dir = os.path.dirname(pdf_path) or os.getcwd()
        os.makedirs(output_dir, exist_ok=True)
        
        _progress("=" * 80)
        _progress("Starting Automated Pipeline Orchestrator")
        _progress("=" * 80)
        _progress(f"PointID: {start_pointid} (Book: {book_id:03d}, Chapter: {chapter_id:03d}, Start Index: {start_index:04d})")
        
        # Initialize processors
        from multi_part_processor import MultiPartProcessor
        from multi_part_post_processor import MultiPartPostProcessor
        multi_part_processor = MultiPartProcessor(self.api_client, output_dir)
        multi_part_post_processor = MultiPartPostProcessor(self.api_client)
        
        # Determine starting stage for resume
        stages_to_run = ["1", "2", "3", "4", "E", "F", "J", "V"]
        if resume_from_stage:
            try:
                start_index = stages_to_run.index(resume_from_stage)
                stages_to_run = stages_to_run[start_index:]
                _progress(f"Resuming from stage: {resume_from_stage}")
            except ValueError:
                _progress(f"Warning: Invalid resume_from_stage '{resume_from_stage}', starting from beginning")
        
        # Variables to store intermediate results
        stage1_json_path = None
        stage2_json_path = None
        stage1_data = None
        stage2_data = None
        
        # ========== STAGE 1: Process PDF ==========
        if "1" in stages_to_run:
            _progress("\n" + "=" * 80)
            _progress("STAGE 1: Processing PDF")
            _progress("=" * 80)
            
            result = StageResult("1")
            result.start_time = datetime.now()
            self.stage_results["1"] = result
            result.status = StageStatus.RUNNING
            
            try:
                if not os.path.exists(pdf_path):
                    raise Exception(f"PDF file not found: {pdf_path}")
                
                _progress(f"Processing PDF: {os.path.basename(pdf_path)}")
                _progress(f"Using prompt: {len(stage1_prompt)} characters")
                _progress(f"Using model: {stage1_model}")
                
                # Process PDF using multi_part_processor
                stage1_json_path = multi_part_processor.process_multi_part(
                    pdf_path=pdf_path,
                    base_prompt=stage1_prompt,
                    model_name=stage1_model,
                    temperature=0.7,
                    progress_callback=progress_callback
                )
                
                if not stage1_json_path or not os.path.exists(stage1_json_path):
                    raise Exception("Stage 1 processing returned no output")
                
                # Load Stage 1 data
                with open(stage1_json_path, 'r', encoding='utf-8') as f:
                    stage1_data = json.load(f)
                
                result.mark_success(stage1_json_path)
                _progress(f"Stage 1 completed successfully: {stage1_json_path}")
                
            except Exception as e:
                error_msg = f"Stage 1 failed: {str(e)}"
                self.logger.error(error_msg, exc_info=True)
                result.mark_failed(error_msg)
                _progress(f"Stage 1 failed: {error_msg}")
                return self.stage_results  # Cannot continue without Stage 1
        
        # ========== STAGE 2: Process Stage 1 per Part ==========
        if "2" in stages_to_run:
            _progress("\n" + "=" * 80)
            _progress("STAGE 2: Processing Stage 1 per Part")
            _progress("=" * 80)
            
            result = StageResult("2")
            result.start_time = datetime.now()
            self.stage_results["2"] = result
            result.status = StageStatus.RUNNING
            
            try:
                # Get Stage 1 output path
                stage1_result = self.stage_results.get("1")
                if not stage1_result or stage1_result.status != StageStatus.SUCCESS:
                    raise Exception("Stage 1 must complete successfully before Stage 2")
                
                stage1_json_path = stage1_result.output_path
                
                # Replace {CHAPTER_NAME} in prompt
                stage2_prompt_final = stage2_prompt.replace("{CHAPTER_NAME}", chapter_name)
                
                _progress(f"Processing Stage 1 JSON per Part...")
                _progress(f"Using prompt: {len(stage2_prompt_final)} characters")
                _progress(f"Using model: {stage2_model}")
                
                # Process Stage 1 using multi_part_post_processor
                stage2_json_path = multi_part_post_processor.process_final_json_by_parts(
                    json_path=stage1_json_path,
                    user_prompt=stage2_prompt_final,
                    model_name=stage2_model
                )
                
                if not stage2_json_path or not os.path.exists(stage2_json_path):
                    raise Exception("Stage 2 processing returned no output")
                
                # Load Stage 2 data
                with open(stage2_json_path, 'r', encoding='utf-8') as f:
                    stage2_data = json.load(f)
                
                result.mark_success(stage2_json_path)
                _progress(f"Stage 2 completed successfully: {stage2_json_path}")
                
            except Exception as e:
                error_msg = f"Stage 2 failed: {str(e)}"
                self.logger.error(error_msg, exc_info=True)
                result.mark_failed(error_msg)
                _progress(f"Stage 2 failed: {error_msg}")
                return self.stage_results  # Cannot continue without Stage 2
        
        # ========== STAGE 3: Process Stage 2 JSON ==========
        if "3" in stages_to_run:
            _progress("\n" + "=" * 80)
            _progress("STAGE 3: Processing Stage 2 JSON")
            _progress("=" * 80)
            
            result = StageResult("3")
            result.start_time = datetime.now()
            self.stage_results["3"] = result
            result.status = StageStatus.RUNNING
            
            try:
                # Get Stage 2 output path
                stage2_result = self.stage_results.get("2")
                if not stage2_result or stage2_result.status != StageStatus.SUCCESS:
                    raise Exception("Stage 2 must complete successfully before Stage 3")
                
                stage2_json_path = stage2_result.output_path
                
                # Load Stage 1 and Stage 2 data if not already loaded
                if stage1_data is None:
                    stage1_result = self.stage_results.get("1")
                    if stage1_result and stage1_result.status == StageStatus.SUCCESS:
                        with open(stage1_result.output_path, 'r', encoding='utf-8') as f:
                            stage1_data = json.load(f)
                
                if stage2_data is None:
                    with open(stage2_json_path, 'r', encoding='utf-8') as f:
                        stage2_data = json.load(f)
                
                # Replace {CHAPTER_NAME} in prompt
                stage3_prompt_final = stage3_prompt.replace("{CHAPTER_NAME}", chapter_name)
                
                # Run Stage 3 chunked processing
                _progress("Running Stage 3 chunked processing...")
                stage3_output = run_third_stage_chunked(
                    api_client=self.api_client,
                    json1_data=stage1_data,  # Source JSON (Stage 1)
                    json2_data=stage2_data,   # Incomplete output (Stage 2)
                    base_prompt=stage3_prompt_final,
                    chapter_name=chapter_name,
                    model_name=stage3_model,
                    progress_callback=progress_callback,
                    stage_name="third"
                )
                
                if not stage3_output:
                    raise Exception("Stage 3 processing returned no output")
                
                # Save Stage 3 output
                stage3_output_path = os.path.join(output_dir, f"stage3_output_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
                with open(stage3_output_path, 'w', encoding='utf-8') as f:
                    json.dump(stage3_output, f, ensure_ascii=False, indent=2)
                
                result.mark_success(stage3_output_path)
                _progress(f"Stage 3 completed successfully: {stage3_output_path}")
                
            except Exception as e:
                error_msg = f"Stage 3 failed: {str(e)}"
                self.logger.error(error_msg, exc_info=True)
                result.mark_failed(error_msg)
                _progress(f"Stage 3 failed: {error_msg}")
        
        # ========== STAGE 4: Process Stage 3 JSON ==========
        if "4" in stages_to_run:
            _progress("\n" + "=" * 80)
            _progress("STAGE 4: Processing Stage 3 JSON")
            _progress("=" * 80)
            
            result = StageResult("4")
            result.start_time = datetime.now()
            self.stage_results["4"] = result
            result.status = StageStatus.RUNNING
            
            try:
                # Get Stage 3 output path
                stage3_result = self.stage_results.get("3")
                if not stage3_result or stage3_result.status != StageStatus.SUCCESS:
                    raise Exception("Stage 3 must complete successfully before Stage 4")
                
                stage3_output_path = stage3_result.output_path
                
                # Load Stage 3 JSON
                _progress("Loading Stage 3 JSON...")
                with open(stage3_output_path, 'r', encoding='utf-8') as f:
                    stage3_data = json.load(f)
                
                # Replace {CHAPTER_NAME} in prompt
                stage4_prompt_final = stage4_prompt.replace("{CHAPTER_NAME}", chapter_name)
                
                # Run Stage 4 chunked processing
                _progress("Running Stage 4 chunked processing...")
                # Load Stage 2 data if not already loaded
                if stage2_data is None:
                    with open(stage2_json_path, 'r', encoding='utf-8') as f:
                        stage2_data = json.load(f)
                
                stage4_output = run_third_stage_chunked(
                    api_client=self.api_client,
                    json1_data=stage1_data,  # Source JSON (Stage 1)
                    json2_data=stage3_data,   # Incomplete output (Stage 3)
                    base_prompt=stage4_prompt_final,
                    chapter_name=chapter_name,
                    model_name=stage4_model,
                    progress_callback=progress_callback,
                    stage_name="fourth"
                )
                
                if not stage4_output:
                    raise Exception("Stage 4 processing returned no output")
                
                # Save Stage 4 output (raw)
                stage4_raw_output_path = os.path.join(output_dir, f"stage4_raw_output_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
                with open(stage4_raw_output_path, 'w', encoding='utf-8') as f:
                    json.dump(stage4_output, f, ensure_ascii=False, indent=2)
                
                # Convert Stage 4 to flat JSON with PointId
                _progress("Converting Stage 4 to flat JSON with PointId...")
                
                # Create a temporary file for converter
                temp_stage4_file = os.path.join(output_dir, f"temp_stage4_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
                temp_stage4_data = {
                    "response": json.dumps(stage4_output, ensure_ascii=False)
                }
                with open(temp_stage4_file, 'w', encoding='utf-8') as f:
                    json.dump(temp_stage4_data, f, ensure_ascii=False, indent=2)
                
                # Convert using ThirdStageConverter
                stage4_converted_path = self.converter.convert_third_stage_file(
                    input_path=temp_stage4_file,
                    book_id=book_id,
                    chapter_id=chapter_id,
                    start_index=start_index,
                    output_path=None
                )
                
                if not stage4_converted_path or not os.path.exists(stage4_converted_path):
                    raise Exception("Stage 4 conversion failed")
                
                # Clean up temp file
                try:
                    os.remove(temp_stage4_file)
                except:
                    pass
                
                result.mark_success(stage4_converted_path)
                _progress(f"Stage 4 completed successfully: {stage4_converted_path}")
                
            except Exception as e:
                error_msg = f"Stage 4 failed: {str(e)}"
                self.logger.error(error_msg, exc_info=True)
                result.mark_failed(error_msg)
                _progress(f"Stage 4 failed: {error_msg}")
        
        # ========== STAGE E: Image Notes Processing ==========
        if "E" in stages_to_run:
            _progress("\n" + "=" * 80)
            _progress("STAGE E: Image Notes Processing")
            _progress("=" * 80)
            
            result = StageResult("E")
            result.start_time = datetime.now()
            self.stage_results["E"] = result
            result.status = StageStatus.RUNNING
            
            try:
                # Get Stage 4 output path
                stage4_result = self.stage_results.get("4")
                if not stage4_result or stage4_result.status != StageStatus.SUCCESS:
                    raise Exception("Stage 4 must complete successfully before Stage E")
                
                stage4_path = stage4_result.output_path
                
                # Get Stage 1 output path (OCR Extraction JSON)
                stage1_result = self.stage_results.get("1")
                if not stage1_result or stage1_result.status != StageStatus.SUCCESS:
                    raise Exception("Stage 1 (OCR Extraction) must complete successfully before Stage E")
                
                ocr_extraction_json_path = stage1_result.output_path
                
                # Run Stage E
                stage_e_output = self.stage_e_processor.process_stage_e(
                    stage4_path=stage4_path,
                    ocr_extraction_json_path=ocr_extraction_json_path,
                    prompt=stage_e_prompt,
                    model_name=stage_e_model,
                    output_dir=output_dir,
                    progress_callback=progress_callback
                )
                
                if not stage_e_output:
                    raise Exception("Stage E processing returned no output")
                
                result.mark_success(stage_e_output)
                _progress(f"Stage E completed successfully: {stage_e_output}")
                
            except Exception as e:
                error_msg = f"Stage E failed: {str(e)}"
                self.logger.error(error_msg, exc_info=True)
                result.mark_failed(error_msg)
                _progress(f"Stage E failed: {error_msg}")
        
        # ========== STAGE F: Image File Catalog ==========
        if "F" in stages_to_run:
            _progress("\n" + "=" * 80)
            _progress("STAGE F: Image File Catalog")
            _progress("=" * 80)
            
            result = StageResult("F")
            result.start_time = datetime.now()
            self.stage_results["F"] = result
            result.status = StageStatus.RUNNING
            
            try:
                # Get Stage E output path
                stage_e_result = self.stage_results.get("E")
                if not stage_e_result or stage_e_result.status != StageStatus.SUCCESS:
                    raise Exception("Stage E must complete successfully before Stage F")
                
                stage_e_path = stage_e_result.output_path
                
                # Run Stage F
                stage_f_output = self.stage_f_processor.process_stage_f(
                    stage_e_path=stage_e_path,
                    output_dir=output_dir,
                    progress_callback=progress_callback
                )
                
                if not stage_f_output:
                    raise Exception("Stage F processing returned no output")
                
                result.mark_success(stage_f_output)
                stage_f_json_path = stage_f_output  # Update for Stage J
                _progress(f"Stage F completed successfully: {stage_f_output}")
                
            except Exception as e:
                error_msg = f"Stage F failed: {str(e)}"
                self.logger.error(error_msg, exc_info=True)
                result.mark_failed(error_msg)
                _progress(f"Stage F failed: {error_msg}")
        
        # ========== STAGE J: Importance & Type Tagging ==========
        if "J" in stages_to_run:
            _progress("\n" + "=" * 80)
            _progress("STAGE J: Importance & Type Tagging")
            _progress("=" * 80)
            
            result = StageResult("J")
            result.start_time = datetime.now()
            self.stage_results["J"] = result
            result.status = StageStatus.RUNNING
            
            try:
                # Validate Word file exists
                if not word_file_path or not os.path.exists(word_file_path):
                    raise Exception(f"Word file is required for Stage J but not found: {word_file_path}")
                
                if not self.word_processor.is_word_file(word_file_path):
                    raise Exception(f"File is not a valid Word file: {word_file_path}")
                
                # Get Stage E output path
                stage_e_result = self.stage_results.get("E")
                if not stage_e_result or stage_e_result.status != StageStatus.SUCCESS:
                    raise Exception("Stage E must complete successfully before Stage J")
                
                stage_e_path = stage_e_result.output_path
                
                # Get Stage F path if available
                stage_f_result = self.stage_results.get("F")
                stage_f_path = stage_f_result.output_path if stage_f_result and stage_f_result.status == StageStatus.SUCCESS else None
                if not stage_f_path and stage_f_json_path:
                    stage_f_path = stage_f_json_path
                
                # Validate prompts and models
                if not stage_j_prompt:
                    raise Exception("Stage J prompt is required")
                if not stage_j_model:
                    raise Exception("Stage J model is required")
                
                # Run Stage J
                stage_j_output = self.stage_j_processor.process_stage_j(
                    stage_e_path=stage_e_path,
                    word_file_path=word_file_path,
                    stage_f_path=stage_f_path,
                    prompt=stage_j_prompt,
                    model_name=stage_j_model,
                    output_dir=output_dir,
                    progress_callback=progress_callback
                )
                
                if not stage_j_output:
                    raise Exception("Stage J processing returned no output")
                
                result.mark_success(stage_j_output)
                _progress(f"Stage J completed successfully: {stage_j_output}")
                
            except Exception as e:
                error_msg = f"Stage J failed: {str(e)}"
                self.logger.error(error_msg, exc_info=True)
                result.mark_failed(error_msg)
                _progress(f"Stage J failed: {error_msg}")
        
        # ========== STAGE V: Test Bank Generation ==========
        if "V" in stages_to_run:
            _progress("\n" + "=" * 80)
            _progress("STAGE V: Test Bank Generation")
            _progress("=" * 80)
            
            result = StageResult("V")
            result.start_time = datetime.now()
            self.stage_results["V"] = result
            result.status = StageStatus.RUNNING
            
            try:
                # Validate Word file exists
                if not word_file_path or not os.path.exists(word_file_path):
                    raise Exception(f"Word file is required for Stage V but not found: {word_file_path}")
                
                if not self.word_processor.is_word_file(word_file_path):
                    raise Exception(f"File is not a valid Word file: {word_file_path}")
                
                # Get Stage J output path
                stage_j_result = self.stage_results.get("J")
                if not stage_j_result or stage_j_result.status != StageStatus.SUCCESS:
                    raise Exception("Stage J must complete successfully before Stage V")
                
                stage_j_path = stage_j_result.output_path
                
                # Validate prompts and models
                if not stage_v_prompt_1 or not stage_v_model_1:
                    raise Exception("Stage V Step 1 prompt and model are required")
                if not stage_v_prompt_2 or not stage_v_model_2:
                    raise Exception("Stage V Step 2 prompt and model are required")
                
                # Get Stage 1 output path (OCR Extraction JSON)
                stage1_result = self.stage_results.get("1")
                if not stage1_result or stage1_result.status != StageStatus.SUCCESS:
                    raise Exception("Stage 1 (OCR Extraction) must complete successfully before Stage V")
                
                ocr_extraction_json_path = stage1_result.output_path
                
                # Run Stage V
                stage_v_output = self.stage_v_processor.process_stage_v(
                    stage_j_path=stage_j_path,
                    word_file_path=word_file_path,
                    ocr_extraction_json_path=ocr_extraction_json_path,
                    prompt_1=stage_v_prompt_1,
                    model_name_1=stage_v_model_1,
                    prompt_2=stage_v_prompt_2,
                    model_name_2=stage_v_model_2,
                    output_dir=output_dir,
                    progress_callback=progress_callback
                )
                
                if not stage_v_output:
                    raise Exception("Stage V processing returned no output")
                
                result.mark_success(stage_v_output)
                _progress(f"Stage V completed successfully: {stage_v_output}")
                
            except Exception as e:
                error_msg = f"Stage V failed: {str(e)}"
                self.logger.error(error_msg, exc_info=True)
                result.mark_failed(error_msg)
                _progress(f"Stage V failed: {error_msg}")
        
        # ========== STAGE X: Book Changes Detection ==========
        if "X" in stages_to_run:
            _progress("\n" + "=" * 80)
            _progress("STAGE X: Book Changes Detection")
            _progress("=" * 80)
            
            result = StageResult("X")
            result.start_time = datetime.now()
            self.stage_results["X"] = result
            result.status = StageStatus.RUNNING
            
            try:
                # Get Stage J output path (Stage A file)
                stage_j_result = self.stage_results.get("J")
                if not stage_j_result or stage_j_result.status != StageStatus.SUCCESS:
                    raise Exception("Stage J must complete successfully before Stage X")
                
                stage_a_path = stage_j_result.output_path
                
                # Validate required inputs
                if not old_book_pdf_path or not os.path.exists(old_book_pdf_path):
                    raise Exception(f"Old book PDF file not found: {old_book_pdf_path}")
                
                if not stage_x_pdf_extraction_prompt:
                    raise Exception("Stage X PDF extraction prompt is required")
                
                if not stage_x_change_prompt:
                    raise Exception("Stage X change detection prompt is required")
                
                # Use default models if not provided
                pdf_model = stage_x_pdf_extraction_model or stage1_model
                change_model = stage_x_change_model or stage1_model
                
                # Run Stage X
                stage_x_output = self.stage_x_processor.process_stage_x(
                    old_book_pdf_path=old_book_pdf_path,
                    pdf_extraction_prompt=stage_x_pdf_extraction_prompt,
                    pdf_extraction_model=pdf_model,
                    stage_a_path=stage_a_path,
                    changes_prompt=stage_x_change_prompt,
                    changes_model=change_model,
                    output_dir=output_dir,
                    progress_callback=progress_callback
                )
                
                if not stage_x_output:
                    raise Exception("Stage X processing returned no output")
                
                result.mark_success(stage_x_output)
                _progress(f"Stage X completed successfully: {stage_x_output}")
                
            except Exception as e:
                error_msg = f"Stage X failed: {str(e)}"
                self.logger.error(error_msg, exc_info=True)
                result.mark_failed(error_msg)
                _progress(f"Stage X failed: {error_msg}")
        
        # ========== STAGE Y: Deletion Detection ==========
        if "Y" in stages_to_run:
            _progress("\n" + "=" * 80)
            _progress("STAGE Y: Deletion Detection")
            _progress("=" * 80)
            
            result = StageResult("Y")
            result.start_time = datetime.now()
            self.stage_results["Y"] = result
            result.status = StageStatus.RUNNING
            
            try:
                # Get Stage 1 output path (OCR Extraction JSON)
                stage1_result = self.stage_results.get("1")
                if not stage1_result or stage1_result.status != StageStatus.SUCCESS:
                    raise Exception("Stage 1 (OCR Extraction) must complete successfully before Stage Y")
                
                ocr_extraction_json_path = stage1_result.output_path
                
                # Get old book PDF path from Stage X metadata
                old_book_pdf_path = None
                stage_x_result = self.stage_results.get("X")
                if stage_x_result and stage_x_result.status == StageStatus.SUCCESS:
                    with open(stage_x_result.output_path, 'r', encoding='utf-8') as f:
                        stage_x_data = json.load(f)
                    stage_x_metadata = stage_x_data.get("metadata", {})
                    old_book_pdf_path = stage_x_metadata.get("old_book_pdf_path")
                
                if not old_book_pdf_path or not os.path.exists(old_book_pdf_path):
                    raise Exception(f"Old book PDF path not found. Stage X must complete successfully before Stage Y, or old_book_pdf_path must be provided. Path: {old_book_pdf_path}")
                
                # Validate required inputs
                if not stage_y_prompt:
                    raise Exception("Stage Y deletion detection prompt is required")
                
                # Use default model if not provided
                y_model = stage_y_model or stage1_model
                
                # For OCR extraction prompt and model, use Stage 1 settings (same as OCR Extraction stage)
                ocr_extraction_prompt = stage1_prompt
                ocr_extraction_model = stage1_model
                
                # Run Stage Y
                stage_y_output = self.stage_y_processor.process_stage_y(
                    old_book_pdf_path=old_book_pdf_path,
                    ocr_extraction_prompt=ocr_extraction_prompt,
                    ocr_extraction_model=ocr_extraction_model,
                    ocr_extraction_json_path=ocr_extraction_json_path,
                    deletion_detection_prompt=stage_y_prompt,
                    deletion_detection_model=y_model,
                    output_dir=output_dir,
                    progress_callback=progress_callback
                )
                
                if not stage_y_output:
                    raise Exception("Stage Y processing returned no output")
                
                result.mark_success(stage_y_output)
                _progress(f"Stage Y completed successfully: {stage_y_output}")
                
            except Exception as e:
                error_msg = f"Stage Y failed: {str(e)}"
                self.logger.error(error_msg, exc_info=True)
                result.mark_failed(error_msg)
                _progress(f"Stage Y failed: {error_msg}")
        
        # ========== STAGE Z: RichText Generation ==========
        if "Z" in stages_to_run:
            _progress("\n" + "=" * 80)
            _progress("STAGE Z: RichText Generation")
            _progress("=" * 80)
            
            result = StageResult("Z")
            result.start_time = datetime.now()
            self.stage_results["Z"] = result
            result.status = StageStatus.RUNNING
            
            try:
                # Get Stage J output path (Stage A file)
                stage_j_result = self.stage_results.get("J")
                if not stage_j_result or stage_j_result.status != StageStatus.SUCCESS:
                    raise Exception("Stage J must complete successfully before Stage Z")
                
                stage_a_path = stage_j_result.output_path
                
                # Get Stage X output
                stage_x_result = self.stage_results.get("X")
                if not stage_x_result or stage_x_result.status != StageStatus.SUCCESS:
                    raise Exception("Stage X must complete successfully before Stage Z")
                
                stage_x_path = stage_x_result.output_path
                
                # Get Stage Y output
                stage_y_result = self.stage_results.get("Y")
                if not stage_y_result or stage_y_result.status != StageStatus.SUCCESS:
                    raise Exception("Stage Y must complete successfully before Stage Z")
                
                stage_y_path = stage_y_result.output_path
                
                # Validate required inputs
                if not stage_z_prompt:
                    raise Exception("Stage Z prompt is required")
                
                # Use default model if not provided
                z_model = stage_z_model or stage1_model
                
                # Run Stage Z
                stage_z_output = self.stage_z_processor.process_stage_z(
                    stage_a_path=stage_a_path,
                    stage_x_output_path=stage_x_path,
                    stage_y_output_path=stage_y_path,
                    prompt=stage_z_prompt,
                    model_name=z_model,
                    output_dir=output_dir,
                    progress_callback=progress_callback
                )
                
                if not stage_z_output:
                    raise Exception("Stage Z processing returned no output")
                
                result.mark_success(stage_z_output)
                _progress(f"Stage Z completed successfully: {stage_z_output}")
                
            except Exception as e:
                error_msg = f"Stage Z failed: {str(e)}"
                self.logger.error(error_msg, exc_info=True)
                result.mark_failed(error_msg)
                _progress(f"Stage Z failed: {error_msg}")
        
        # ========== Generate Summary Report ==========
        _progress("\n" + "=" * 80)
        _progress("PIPELINE EXECUTION SUMMARY")
        _progress("=" * 80)
        
        successful_stages = []
        failed_stages = []
        skipped_stages = []
        
        for stage_name, stage_result in self.stage_results.items():
            if stage_result.status == StageStatus.SUCCESS:
                successful_stages.append(stage_name)
            elif stage_result.status == StageStatus.FAILED:
                failed_stages.append(stage_name)
            elif stage_result.status == StageStatus.SKIPPED:
                skipped_stages.append(stage_name)
        
        _progress(f"\nSuccessful Stages ({len(successful_stages)}): {', '.join(successful_stages) if successful_stages else 'None'}")
        
        if failed_stages:
            _progress(f"\nFailed Stages ({len(failed_stages)}):")
            for stage_name in failed_stages:
                stage_result = self.stage_results[stage_name]
                _progress(f"  - Stage {stage_name}: {stage_result.error_message}")
        
        if skipped_stages:
            _progress(f"\nSkipped Stages ({len(skipped_stages)}):")
            for stage_name in skipped_stages:
                stage_result = self.stage_results[stage_name]
                _progress(f"  - Stage {stage_name}: {stage_result.error_message}")
        
        # Save execution report
        report_path = os.path.join(output_dir, f"pipeline_execution_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
        report_data = {
            "execution_time": datetime.now().isoformat(),
            "stages": {name: result.to_dict() for name, result in self.stage_results.items()},
            "summary": {
                "total_stages": len(self.stage_results),
                "successful": len(successful_stages),
                "failed": len(failed_stages),
                "skipped": len(skipped_stages)
            }
        }
        
        with open(report_path, 'w', encoding='utf-8') as f:
            json.dump(report_data, f, ensure_ascii=False, indent=2)
        
        _progress(f"\nExecution report saved to: {report_path}")
        _progress("=" * 80)
        
        return self.stage_results


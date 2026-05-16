"""
Stage J Processor: Add Imp & Type
Adds two new columns (Imp and Type) to Stage E data based on Word test file and prompt.
"""

import json
import logging
import math
import os
import re
import tempfile
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Optional, Dict, List, Any, Callable, Tuple

from base_stage_processor import BaseStageProcessor
from word_file_processor import WordFileProcessor
from api_layer import APIConfig


def _sj_normalize_key_part(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip().casefold()


def _sj_build_topic_key(chapter_name: Any, subchapter_name: Any, topic_name: Any) -> Tuple[str, str, str]:
    return (
        _sj_normalize_key_part(chapter_name),
        _sj_normalize_key_part(subchapter_name),
        _sj_normalize_key_part(topic_name),
    )


def _sj_step1_topic_key(row: Dict[str, Any]) -> Tuple[str, str, str]:
    return _sj_build_topic_key(
        row.get("Chapter", row.get("chapter", "")),
        row.get("Subchapter", row.get("subchapter", "")),
        row.get("Topic", row.get("topic", "")),
    )


STAGE_J_WEB_LOG_PREFIX = "[stage_j_web]"

_SJ_WEB_VALID_TYPES = frozenset({
    "Cp", "Epi", "Rf", "Tr", "Meth", "Def", "Histo", "Ddx", "Gen", "Comp",
    "Img", "Lab", "Screen", "Follow", "Phys", "Contra", "Dose", "Exam", "Prev",
})

_SJ_WEB_PID_RE = re.compile(
    r"(?:PointId|Point)\s*[:\*\s#]*\*{0,2}\s*(\d{8,})",
    re.IGNORECASE,
)
_SJ_WEB_IMP_RE = re.compile(r'\bImp\.?\s*[:\s]*["\']?([123])["\']?', re.IGNORECASE)
_SJ_WEB_TYPE_RE = re.compile(r'\bType\.?\s*[:\s]*\*{0,2}([A-Za-z]{2,10})\*{0,2}', re.IGNORECASE)

_SJ_WEB_JSON_RETRY_SUFFIX = (
    "\n\nCRITICAL (retry): Reply with ONLY one JSON object {\"data\":[...]}. "
    "No markdown scoring lists, no chain-of-thought, no analysis text."
)


def _sj_web_norm_type(raw: str) -> str:
    t = (raw or "").strip().rstrip(".")
    for code in _SJ_WEB_VALID_TYPES:
        if t.lower() == code.lower():
            return code
    return ""


def sj_web_parse_scoring_markdown(text: str) -> List[Dict[str, Any]]:
    """Salvage Imp/Type rows when the model returns markdown scoring instead of JSON."""
    if not (text or "").strip():
        return []
    rows: List[Dict[str, Any]] = []
    current_pid: Optional[str] = None
    current_imp = ""
    current_type = ""

    def flush() -> None:
        nonlocal current_pid, current_imp, current_type
        if not current_pid:
            return
        imp = current_imp if current_imp in ("1", "2", "3") else ""
        typ = _sj_web_norm_type(current_type)
        if imp or typ:
            rows.append({"PointId": current_pid, "Imp": imp, "Type": typ})
        current_pid = None
        current_imp = ""
        current_type = ""

    for line in text.splitlines():
        pid_m = _SJ_WEB_PID_RE.search(line)
        if pid_m:
            flush()
            current_pid = pid_m.group(1)
            imp_m = _SJ_WEB_IMP_RE.search(line)
            if imp_m:
                current_imp = imp_m.group(1)
            type_m = _SJ_WEB_TYPE_RE.search(line)
            if type_m:
                current_type = type_m.group(1)
            continue
        if not current_pid:
            continue
        imp_m = _SJ_WEB_IMP_RE.search(line)
        if imp_m:
            current_imp = imp_m.group(1)
        type_m = _SJ_WEB_TYPE_RE.search(line)
        if type_m:
            current_type = type_m.group(1)
    flush()
    return rows


def sj_web_is_context_limit_error(exc: BaseException) -> bool:
    """OpenRouter / GLM rejects when prompt + reserved completion exceeds the endpoint window."""
    msg = str(exc).lower()
    if "maximum context length" in msg:
        return True
    if "maximum context" in msg and "token" in msg:
        return True
    if "context length" in msg and ("exceed" in msg or "reduce" in msg):
        return True
    if "you requested about" in msg and "tokens" in msg:
        return True
    return False


def sj_web_log(logger: logging.Logger, message: str) -> None:
    """Single prefix for grep-friendly Docker logs."""
    logger.info("%s %s", STAGE_J_WEB_LOG_PREFIX, message)


def sj_web_slim_lesson_row(record: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "PointId": str(record.get("PointId", "") or ""),
        "chapter": record.get("chapter", ""),
        "subchapter": record.get("subchapter", ""),
        "topic": record.get("topic", ""),
        "subtopic": record.get("subtopic", ""),
        "subsubtopic": record.get("subsubtopic", ""),
        "Points": record.get("Points", record.get("points", "")),
    }


def sj_web_topic_sidebar_rows(
    chunk_ta_rows: List[Dict[str, Any]],
    table_by_topic: Dict[Tuple[str, str, str], List[Dict[str, str]]],
    image_by_topic: Dict[Tuple[str, str, str], List[Dict[str, str]]],
) -> List[Dict[str, Any]]:
    seen: set = set()
    out: List[Dict[str, Any]] = []
    for r in chunk_ta_rows:
        if not isinstance(r, dict):
            continue
        key = _sj_build_topic_key(r.get("chapter", ""), r.get("subchapter", ""), r.get("topic", ""))
        if key in seen:
            continue
        seen.add(key)
        entry: Dict[str, Any] = {
            "chapter": r.get("chapter", ""),
            "subchapter": r.get("subchapter", ""),
            "topic": r.get("topic", ""),
        }
        tabs = table_by_topic.get(key)
        imgs = image_by_topic.get(key)
        if tabs:
            entry["topic_table_captions"] = tabs
        if imgs:
            entry["topic_image_captions"] = imgs
        out.append(entry)
    return out


def sj_web_step1_rows_for_chunk_topics(
    step1_records: List[Dict[str, Any]],
    chunk_ta_rows: List[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    keys: set = set()
    for r in chunk_ta_rows:
        if isinstance(r, dict):
            keys.add(_sj_build_topic_key(r.get("chapter", ""), r.get("subchapter", ""), r.get("topic", "")))
    ref_candidates = [x for x in step1_records if isinstance(x, dict) and _sj_step1_topic_key(x) in keys]

    def _sort_key(row: Dict[str, Any]) -> Tuple[str, int]:
        pid = str(row.get("PointID") or row.get("PointId") or "")
        tid = row.get("TestID")
        try:
            tidn = int(tid) if tid is not None else 0
        except (TypeError, ValueError):
            tidn = 0
        return (pid, tidn)

    return sorted(ref_candidates, key=_sort_key)


def sj_web_build_user_prompt(
    prompt_body: str,
    part_num: int,
    total_parts: int,
    topics_context: List[Dict[str, Any]],
    slim_rows: List[Dict[str, Any]],
    ref_questions: List[Dict[str, Any]],
) -> str:
    tc = json.dumps(topics_context, ensure_ascii=False, separators=(",", ":"))
    sj = json.dumps(slim_rows, ensure_ascii=False, separators=(",", ":"))
    rq = json.dumps(ref_questions, ensure_ascii=False, separators=(",", ":"))
    return f"""{prompt_body}

Structured inputs for this chunk (part {part_num} of {total_parts}):

topics_context — table/image captions grouped once per topic (apply to all lesson rows with the same chapter, subchapter, topic):
{tc}

Reference test questions (Step 1 rows whose Chapter/Subchapter/Topic overlap topics in this chunk):
{rq}

Lesson rows (PointId + hierarchy + Points text only):
{sj}

For this chunk only: score every PointId in lesson rows above. Reply with ONE JSON object per the Output rules in the prompt (root key "data"; keys PointId, Imp, Type; Imp as "1"|"2"|"3"; one row per PointId). No markdown, no text outside JSON."""


class StageJProcessor(BaseStageProcessor):
    """Process Stage J: Add Imp and Type columns to Stage E data"""

    STAGE_J_WEB_CHUNK_SIZE = 100
    STAGE_J_WEB_PARALLEL_CHUNKS = 6
    STAGE_J_WEB_MAX_OUTPUT_TOKENS = 8192
    STAGE_J_WEB_LLM_PARSE_RETRIES = 3

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
        
        # Set stage if using UnifiedAPIClient (for API routing)
        if hasattr(self.api_client, 'set_stage'):
            self.api_client.set_stage("stage_j")
        
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

    def _sj_index_pic_rows(self, pic_records: List[Dict[str, Any]]) -> Dict[Tuple[str, str, str], List[Dict[str, str]]]:
        by_topic: Dict[Tuple[str, str, str], List[Dict[str, str]]] = defaultdict(list)
        for r in pic_records:
            if not isinstance(r, dict):
                continue
            key = _sj_build_topic_key(r.get("chapter", ""), r.get("subchapter", ""), r.get("topic", ""))
            by_topic[key].append(
                {
                    "point_text": str(r.get("point_text", "") or ""),
                    "caption": str(r.get("caption", "") or ""),
                }
            )
        return dict(by_topic)

    def _parse_imp_type_rows_from_llm_response(self, response_text: str) -> List[Dict[str, Any]]:
        if not (response_text or "").strip():
            return []
        part_output = self.extract_json_from_response(response_text)
        if not part_output:
            part_output = self.load_txt_as_json_from_text(response_text)
        if part_output:
            rows = self.get_data_from_json(part_output)
            parsed = [x for x in rows if isinstance(x, dict)] if rows else []
            if parsed:
                return parsed
        md_rows = sj_web_parse_scoring_markdown(response_text)
        if md_rows:
            sj_web_log(self.logger, f"parsed_rows_from_markdown count={len(md_rows)}")
        return md_rows

    def _run_web_chunk_importance_type(
        self,
        chunk_ta_rows: List[Dict[str, Any]],
        table_by_topic: Dict[Tuple[str, str, str], List[Dict[str, str]]],
        image_by_topic: Dict[Tuple[str, str, str], List[Dict[str, str]]],
        step1_records: List[Dict[str, Any]],
        part_num: int,
        total_parts: int,
        prompt_body: str,
        model_name: str,
        cancel_check: Optional[Callable[[], bool]],
    ) -> Optional[List[Dict[str, Any]]]:
        """
        One bounded slice of lesson rows → one prompt → LLM (fixed max_tokens).
        Retries only on empty/unparseable model text (same prompt, same max_tokens).
        On OpenRouter context-limit errors: log and return None (no bisect, no taper).
        """
        logger = self.logger
        if cancel_check and cancel_check():
            return None
        if not chunk_ta_rows:
            return []

        slim_rows = [sj_web_slim_lesson_row(r) for r in chunk_ta_rows if isinstance(r, dict)]
        topics_context = sj_web_topic_sidebar_rows(chunk_ta_rows, table_by_topic, image_by_topic)
        ref_questions = sj_web_step1_rows_for_chunk_topics(step1_records, chunk_ta_rows)
        full_prompt = sj_web_build_user_prompt(
            prompt_body, part_num, total_parts, topics_context, slim_rows, ref_questions
        )
        prompt_chars = len(full_prompt)
        sj_web_log(
            logger,
            "chunk_begin "
            f"part={part_num}/{total_parts} lesson_rows={len(chunk_ta_rows)} "
            f"topics_in_sidebar={len(topics_context)} step1_ref_rows={len(ref_questions)} "
            f"prompt_chars={prompt_chars} max_tokens={self.STAGE_J_WEB_MAX_OUTPUT_TOKENS}",
        )

        max_out = self.STAGE_J_WEB_MAX_OUTPUT_TOKENS

        for attempt in range(1, self.STAGE_J_WEB_LLM_PARSE_RETRIES + 1):
            if cancel_check and cancel_check():
                return None

            attempt_prompt = full_prompt
            if attempt >= 2:
                attempt_prompt = full_prompt + _SJ_WEB_JSON_RETRY_SUFFIX
            # Last attempt: allow reasoning markdown salvage if JSON-only calls still fail.
            last_attempt = attempt >= self.STAGE_J_WEB_LLM_PARSE_RETRIES
            use_content_only = not last_attempt
            use_reasoning_none = not last_attempt

            try:
                resp = self.api_client.process_text(
                    text=attempt_prompt,
                    system_prompt=None,
                    model_name=model_name,
                    temperature=APIConfig.DEFAULT_TEMPERATURE,
                    max_tokens=max_out,
                    reasoning_effort_none=use_reasoning_none,
                    content_only=use_content_only,
                )
            except Exception as e:
                if sj_web_is_context_limit_error(e):
                    sj_web_log(
                        logger,
                        "chunk_fail reason=context_limit "
                        f"part={part_num}/{total_parts} lesson_rows={len(chunk_ta_rows)} "
                        f"prompt_chars={prompt_chars}",
                    )
                    logger.error("%s context_limit_error: %s", STAGE_J_WEB_LOG_PREFIX, e)
                    return None
                sj_web_log(
                    logger,
                    f"chunk_fail reason=api_error part={part_num}/{total_parts} attempt={attempt}",
                )
                logger.exception("%s api_error", STAGE_J_WEB_LOG_PREFIX)
                return None

            if not (resp or "").strip():
                sj_web_log(
                    logger,
                    f"chunk_retry reason=empty_response part={part_num}/{total_parts} attempt={attempt} "
                    f"model={model_name!r} — see [openrouter] empty_assistant_text / stream_empty in worker logs",
                )
                continue

            rows = self._parse_imp_type_rows_from_llm_response(resp)
            min_rows = max(1, int(len(chunk_ta_rows) * 0.5))
            if rows and len(rows) >= min_rows:
                sj_web_log(
                    logger,
                    "chunk_ok "
                    f"part={part_num}/{total_parts} parsed_rows={len(rows)} attempt={attempt} "
                    f"reasoning_none={use_reasoning_none} content_only={use_content_only}",
                )
                return rows

            rlen = len(resp)
            tail = resp[-500:] if rlen > 500 else resp
            reason = "too_few_rows" if rows else "unparseable_json"
            sj_web_log(
                logger,
                f"chunk_retry reason={reason} part={part_num}/{total_parts} attempt={attempt} "
                f"response_chars={rlen} parsed_rows={len(rows)} need>={min_rows} "
                f"reasoning_none={use_reasoning_none} content_only={use_content_only}",
            )
            logger.warning("%s parse_fail_tail part=%s/%s: %r", STAGE_J_WEB_LOG_PREFIX, part_num, total_parts, tail)

        sj_web_log(
            logger,
            "chunk_fail reason=parse_exhausted "
            f"part={part_num}/{total_parts} retries={self.STAGE_J_WEB_LLM_PARSE_RETRIES}",
        )
        return None

    def process_stage_j_web_four_json(
        self,
        ta_json_path: str,
        tablepic_json_path: str,
        filepic_json_path: str,
        step1_combined_path: str,
        prompt: str,
        model_name: str,
        output_dir: Optional[str] = None,
        progress_callback: Optional[Callable[[str], None]] = None,
        cancel_check: Optional[Callable[[], bool]] = None,
        chunk_size: Optional[int] = None,
        max_parallel_chunks: Optional[int] = None,
    ) -> Optional[str]:
        """
        Web Stage J: TA merged JSON + tablepic + filepic + Step 1 combined.
        Enriches rows with per-topic captions in memory, chunks (~100 rows), parallel LLM calls, writes a*.json.
        """
        chunk_sz = chunk_size if chunk_size is not None else self.STAGE_J_WEB_CHUNK_SIZE
        max_par = max_parallel_chunks if max_parallel_chunks is not None else self.STAGE_J_WEB_PARALLEL_CHUNKS

        def _progress(msg: str) -> None:
            if progress_callback:
                progress_callback(msg)
            self.logger.info(msg)

        if hasattr(self.api_client, "set_stage"):
            self.api_client.set_stage("stage_j")

        if cancel_check and cancel_check():
            return None

        _progress("Starting Web Stage J (four JSON inputs, parallel chunks)...")
        sj_web_log(
            self.logger,
            f"job_begin model={model_name!r} chunk_size={chunk_sz} max_parallel={max_par} "
            f"parse_retries_per_chunk={self.STAGE_J_WEB_LLM_PARSE_RETRIES}",
        )

        ta_data = self.load_json_file(ta_json_path)
        if not ta_data:
            self.logger.error("Failed to load TA merged JSON")
            return None
        ta_records = self.get_data_from_json(ta_data)
        if not ta_records:
            self.logger.error("TA merged JSON has no data")
            return None

        first_point_id = ta_records[0].get("PointId")
        if not first_point_id:
            self.logger.error("No PointId found in TA data")
            return None
        try:
            book_id, chapter_id = self.extract_book_chapter_from_pointid(first_point_id)
        except ValueError as e:
            self.logger.error("Error extracting book/chapter: %s", e)
            return None

        tp_data = self.load_json_file(tablepic_json_path)
        fp_data = self.load_json_file(filepic_json_path)
        s1_data = self.load_json_file(step1_combined_path)
        tablepic_records = self.get_data_from_json(tp_data) if tp_data else []
        filepic_records = self.get_data_from_json(fp_data) if fp_data else []
        step1_records = self.get_data_from_json(s1_data) if s1_data else []

        table_by_topic = self._sj_index_pic_rows([r for r in tablepic_records if isinstance(r, dict)])
        image_by_topic = self._sj_index_pic_rows([r for r in filepic_records if isinstance(r, dict)])

        _progress(
            f"Loaded TA rows={len(ta_records)}, tablepic={len(tablepic_records)}, "
            f"filepic={len(filepic_records)}, step1={len(step1_records)}"
        )

        n_chunks = max(1, math.ceil(len(ta_records) / chunk_sz))
        _progress(f"Chunk size={chunk_sz}, chunks={n_chunks}, parallel workers={min(max_par, n_chunks)}")

        chunks: List[List[Dict[str, Any]]] = []
        for i in range(0, len(ta_records), chunk_sz):
            chunks.append(ta_records[i : i + chunk_sz])

        nw = min(max_par, len(chunks)) if chunks else 1

        chunk_responses: Dict[int, Optional[List[Dict[str, Any]]]] = {}
        with ThreadPoolExecutor(max_workers=nw) as executor:
            future_to_idx: Dict[Any, int] = {}
            for idx, chunk in enumerate(chunks):
                if cancel_check and cancel_check():
                    _progress("Cancelled before scheduling chunks.")
                    break
                chunk_ta = [r for r in chunk if isinstance(r, dict)]

                fut = executor.submit(
                    self._run_web_chunk_importance_type,
                    chunk_ta,
                    table_by_topic,
                    image_by_topic,
                    step1_records,
                    idx + 1,
                    len(chunks),
                    prompt,
                    model_name,
                    cancel_check,
                )
                future_to_idx[fut] = idx

            for fut in as_completed(future_to_idx):
                idx = future_to_idx[fut]
                try:
                    chunk_responses[idx] = fut.result()
                except Exception as e:
                    sj_web_log(
                        self.logger,
                        f"chunk_worker_crash part_index={idx} err={e!r}",
                    )
                    self.logger.exception("%s chunk_worker_crash idx=%s", STAGE_J_WEB_LOG_PREFIX, idx)
                    chunk_responses[idx] = None

        if cancel_check and cancel_check():
            return None

        failed_parts = sorted(i + 1 for i, rows in chunk_responses.items() if rows is None)
        if failed_parts:
            msg = (
                f"Importance & Type stopped: chunk(s) {failed_parts} of {len(chunks)} returned no data. "
                f"Search logs for {STAGE_J_WEB_LOG_PREFIX} (context_limit, parse_exhausted, api_error)."
            )
            sj_web_log(self.logger, f"job_fail failed_chunks={failed_parts} total_chunks={len(chunks)}")
            _progress(msg)
            self.logger.error("%s %s", STAGE_J_WEB_LOG_PREFIX, msg)
            return None

        combined_model_data: List[Dict[str, Any]] = []
        for idx in sorted(chunk_responses.keys()):
            part_rows = chunk_responses.get(idx)
            if part_rows:
                combined_model_data.extend(part_rows)
                _progress(f"Chunk {idx + 1}/{len(chunks)}: collected {len(part_rows)} Imp/Type row(s)")
            else:
                _progress(f"Chunk {idx + 1}/{len(chunks)}: internal error (empty chunk result after success gate)")

        if not combined_model_data:
            sj_web_log(self.logger, "job_fail reason=no_rows_after_successful_chunks")
            self.logger.error("No Imp/Type rows after chunk merge")
            return None

        base_dir = os.path.dirname(ta_json_path) or os.getcwd()
        base_name, _ = os.path.splitext(os.path.basename(ta_json_path))
        txt_path = os.path.join(base_dir, f"{base_name}_stage_j_web.txt")
        try:
            txt_chunks = []
            for idx in sorted(chunk_responses.keys()):
                pr = chunk_responses.get(idx)
                if pr:
                    txt_chunks.append(
                        f"=== CHUNK {idx + 1} PARSED IMP/TYPE ROWS ({len(pr)}) ===\n"
                        + json.dumps(pr, ensure_ascii=False, indent=2)
                    )
            combined_response = "\n\n".join(txt_chunks)
            with open(txt_path, "w", encoding="utf-8") as f:
                f.write(combined_response)
            _progress(f"Saved combined parsed rows dump to: {txt_path}")
        except OSError as e:
            self.logger.warning("Failed to save TXT file: %s", e)

        pointid_to_imp_type: Dict[str, Dict[str, str]] = {}
        for record in combined_model_data:
            point_id = record.get("PointId", "") or record.get("point_id", "")
            if point_id:
                point_id = str(point_id)
                imp_value = record.get("Imp", "") or record.get("importance_level", "")
                type_value = record.get("Type", "") or record.get("point_type", "")
                if imp_value:
                    imp_value = str(imp_value)
                if type_value:
                    type_value = str(type_value)
                pointid_to_imp_type[point_id] = {"Imp": imp_value, "Type": type_value}

        _progress(f"Created mapping for {len(pointid_to_imp_type)} PointIds")

        merged_records = []
        matched_count = 0
        for record in ta_records:
            if not isinstance(record, dict):
                continue
            point_id = record.get("PointId", "")
            if point_id:
                point_id = str(point_id)
            imp_type_data = pointid_to_imp_type.get(point_id, {"Imp": "", "Type": ""})
            if imp_type_data.get("Imp") or imp_type_data.get("Type"):
                matched_count += 1
            merged_records.append(
                {
                    "PointId": point_id,
                    "chapter": record.get("chapter", ""),
                    "subchapter": record.get("subchapter", ""),
                    "topic": record.get("topic", ""),
                    "subtopic": record.get("subtopic", ""),
                    "subsubtopic": record.get("subsubtopic", ""),
                    "Points": record.get("Points", record.get("points", "")),
                    "Imp": imp_type_data.get("Imp", ""),
                    "Type": imp_type_data.get("Type", ""),
                }
            )

        _progress(f"Merged {len(merged_records)} records; matched Imp/Type for {matched_count}")
        if matched_count == 0:
            self.logger.warning("No records matched Imp/Type — check PointId alignment")

        out_dir = output_dir or base_dir
        output_path = self.generate_filename("a", book_id, chapter_id, out_dir)

        output_metadata = {
            "book_id": book_id,
            "chapter_id": chapter_id,
            "source_ta_json": os.path.basename(ta_json_path),
            "source_tablepic_json": os.path.basename(tablepic_json_path),
            "source_filepic_json": os.path.basename(filepic_json_path),
            "source_step1_combined": os.path.basename(step1_combined_path),
            "model_used": model_name,
            "stage_j_txt_file": os.path.basename(txt_path),
            "stage_j_call_mode": "parallel_chunks",
            "stage_j_web_strategy": "single_llm_call_per_chunk_parse_retries_only",
            "stage_j_web_parse_retries_per_chunk": self.STAGE_J_WEB_LLM_PARSE_RETRIES,
            "stage_j_web_output_max_tokens": self.STAGE_J_WEB_MAX_OUTPUT_TOKENS,
            "topic_captions_once_per_chunk_topic": True,
            "chunk_size": chunk_sz,
            "parallel_workers_used": nw,
            "total_records": len(merged_records),
            "records_with_imp_type": len([r for r in merged_records if r.get("Imp") or r.get("Type")]),
        }

        _progress(f"Saving Web Stage J output to: {output_path}")
        success = self.save_json_file(merged_records, output_path, output_metadata, "J")
        if success:
            sj_web_log(
                self.logger,
                f"job_ok path={os.path.basename(output_path)} chunks={len(chunks)} "
                f"imp_type_rows={len(combined_model_data)} merged={len(merged_records)}",
            )
            _progress(f"Web Stage J completed successfully: {output_path}")
            return output_path
        self.logger.error("Failed to save Web Stage J output")
        sj_web_log(self.logger, f"job_fail reason=save_failed path={os.path.basename(output_path)}")
        return None



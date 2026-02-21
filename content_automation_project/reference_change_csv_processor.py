"""
Reference Change CSV Processor
Processes CSV file with old/cold/new/cnew columns and builds chunks for model input
"""

import csv
import json
import logging
from typing import List, Dict, Any, Optional, Tuple
from collections import defaultdict
from reference_change_topic_extractor import extract_topics_from_ocr_json, find_matching_topic


logger = logging.getLogger(__name__)


def parse_csv_file(csv_path: str) -> Tuple[List[Dict[str, str]], Optional[str]]:
    """
    Parse CSV file with columns: old, cold, new, cnew
    
    Args:
        csv_path: Path to CSV file
        
    Returns:
        Tuple of (list of rows as dicts, error_message if any)
    """
    rows = []
    
    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            
            # Validate columns
            required_columns = {'old', 'cold', 'new', 'cnew'}
            if not required_columns.issubset(reader.fieldnames or []):
                missing = required_columns - set(reader.fieldnames or [])
                return [], f"Missing required columns: {', '.join(missing)}"
            
            for row_num, row in enumerate(reader, start=2):  # Start from 2 (row 1 is header)
                # Clean values
                old = row.get('old', '').strip()
                cold = row.get('cold', '').strip()
                new = row.get('new', '').strip()
                cnew = row.get('cnew', '').strip()
                
                # Skip empty rows
                if not old and not new:
                    continue
                
                # Validate cold and cnew are numeric
                try:
                    cold_int = int(cold) if cold else None
                    cnew_int = int(cnew) if cnew else None
                except ValueError:
                    return [], f"Row {row_num}: cold and cnew must be integers (got cold='{cold}', cnew='{cnew}')"
                
                rows.append({
                    'old': old,
                    'cold': cold_int,
                    'new': new,
                    'cnew': cnew_int,
                    'row_num': row_num
                })
        
        if not rows:
            return [], "CSV file is empty or has no valid rows"
        
        logger.info(f"Parsed {len(rows)} rows from CSV file")
        return rows, None
        
    except FileNotFoundError:
        return [], f"CSV file not found: {csv_path}"
    except Exception as e:
        return [], f"Error parsing CSV file: {str(e)}"


def get_chapter_topics_from_ocr(ocr_json_path: str, chapter_num: int) -> Dict[str, Any]:
    """
    Extract all topics from OCR JSON
    
    Args:
        ocr_json_path: Path to OCR JSON file
        chapter_num: Ignored (we now search all chapters/subchapters)
        
    Returns:
        Dictionary with:
        - 'success': bool
        - 'topics_map': dict mapping topic_name -> topic_data
        - 'available_topics': list of topic names
        - 'chapter_name': chapter name if found
        - 'error': str if any error
    """
    result = {
        'success': False,
        'topics_map': {},
        'available_topics': [],
        'chapter_name': '',
        'error': None
    }
    
    try:
        with open(ocr_json_path, 'r', encoding='utf-8') as f:
            ocr_data = json.load(f)
        
        chapters = ocr_data.get('chapters', [])
        if not chapters:
            result['error'] = "No chapters found in OCR JSON"
            return result

        # Search through ALL chapters and ALL subchapters for ALL topics
        for ch in chapters:
            if not isinstance(ch, dict): continue
            subchapters = ch.get('subchapters', [])
            for sub in subchapters:
                if not isinstance(sub, dict): continue
                topics = sub.get('topics', [])
                for topic_obj in topics:
                    if not isinstance(topic_obj, dict): continue
                    topic_name = topic_obj.get('topic', '').strip()
                    if topic_name:
                        result['topics_map'][topic_name] = topic_obj
                        result['available_topics'].append(topic_name)
                
                if not result['chapter_name']:
                    result['chapter_name'] = ch.get('chapter', '')
        
        if result['available_topics']:
            result['success'] = True
            logger.info(f"Found {len(result['available_topics'])} total topics in OCR JSON")
            return result
        
        result['error'] = "No topics found in OCR JSON"
        
    except FileNotFoundError:
        result['error'] = f"OCR JSON file not found: {ocr_json_path}"
    except json.JSONDecodeError as e:
        result['error'] = f"Invalid JSON file: {str(e)}"
    except Exception as e:
        result['error'] = f"Error extracting topics: {str(e)}"
        logger.exception("Error extracting topics from OCR JSON")
    
    return result


def extract_topic_content_from_ocr(ocr_json_path: str, topic_names: List[str], chapter_num: Optional[int] = None, exclude_table_type: bool = False) -> Dict[str, Any]:
    """
    Extract OCR content for specific topics by searching the entire file.
    Includes all extraction types: paragraphs, tables, figures (nothing left out).

    Args:
        ocr_json_path: Path to OCR JSON file
        topic_names: List of topic names to extract
        chapter_num: Ignored (we now search all chapters/subchapters)
        exclude_table_type: If True, exclude table extractions (default False = include all)
    """
    result = {
        'success': False,
        'extracted_content': '',
        'matched_topics': [],
        'missing_topics': [],
        'error': None
    }
    
    try:
        with open(ocr_json_path, 'r', encoding='utf-8') as f:
            ocr_data = json.load(f)
        
        chapters = ocr_data.get('chapters', [])
        
        # Extract ALL available topics from the entire file
        available_topics = []
        topics_map = {}
        
        for chapter_obj in chapters:
            if not isinstance(chapter_obj, dict): continue
            subchapters = chapter_obj.get('subchapters', [])
            for subchapter_obj in subchapters:
                if not isinstance(subchapter_obj, dict): continue
                topics = subchapter_obj.get('topics', [])
                for topic_obj in topics:
                    if not isinstance(topic_obj, dict): continue
                    topic_name = topic_obj.get('topic', '').strip()
                    if topic_name:
                        available_topics.append(topic_name)
                        topics_map[topic_name] = topic_obj
        
        # Match user-provided topics
        matched_topic_data = []
        missing_topics = []
        
        for user_topic in topic_names:
            if not user_topic or not user_topic.strip():
                continue
            
            matched_topic = find_matching_topic(user_topic.strip(), available_topics)
            
            if matched_topic:
                matched_topic_data.append(topics_map[matched_topic])
                result['matched_topics'].append(matched_topic)
            else:
                missing_topics.append(user_topic)
                result['missing_topics'].append(user_topic)
        
        if missing_topics:
            result['error'] = f"Topics not found: {', '.join(missing_topics)}"
            return result
        
        if not exclude_table_type:
            logger.info(
                "[Reference Change] extract_topic_content_from_ocr: استخراج همه موارد (پاراگراف + جدول + شکل) برای تاپیک‌ها: %s",
                result['matched_topics'],
            )
        # Extract content from matched topics
        extracted_parts = []
        
        for topic_data in matched_topic_data:
            topic_name = topic_data.get('topic', '')
            extractions = topic_data.get('extractions', [])
            
            if not extractions:
                logger.warning(f"Topic '{topic_name}' has no extractions")
                continue
            
            # Handle both list and dict structures for extractions — include all (paragraphs, tables, figs, any other key)
            extraction_list = []
            if isinstance(extractions, list):
                extraction_list = extractions
            elif isinstance(extractions, dict):
                known_keys = {'paragraphs', 'tables', 'figs'}
                for key in known_keys:
                    extraction_list.extend(extractions.get(key, []))
                for key, val in extractions.items():
                    if key in known_keys or not isinstance(val, list):
                        continue
                    extraction_list.extend(val)

            # Include all extractions (paragraphs, tables, figs) — nothing left out unless exclude_table_type=True
            for extraction in extraction_list:
                if not isinstance(extraction, dict):
                    continue
                if exclude_table_type and extraction.get('type', '').lower() in ['table', 'e-table']:
                    continue
                extraction_text = extraction.get('content', extraction.get('Extraction', extraction.get('extraction', '')))
                if extraction_text:
                    extracted_parts.append(extraction_text)
        
        result['extracted_content'] = '\n\n'.join(extracted_parts)
        result['success'] = True
        
    except Exception as e:
        result['error'] = f"Error extracting topic content: {str(e)}"
        logger.exception("Error extracting topic content")
    
    return result


def build_chunks_from_csv(
    csv_path: str,
    old_ocr_json_path: str,
    new_ocr_json_path: str,
    exclude_table_type: bool = False
) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """
    Build chunks from CSV (e.g. tableConvert.com_1dk705.csv):
    - Rows with cold filled (e.g. 1..23): one chunk per such row. Rows with cold empty are
      not a separate chunk (they only contribute to "new" side via cnew).
    - For each chunk: old = this row's "old" topic; new = ALL "new" topics from every row
      where cnew == this row's cold (e.g. cold=12 → all rows with cnew=12 → 4 new topics).
    """
    logger.info(
        "[Reference Change] build_chunks_from_csv: استفاده از استخراج کامل (همه موارد: پاراگراف، جدول، شکل) | "
        "exclude_table_type=%s",
        exclude_table_type,
    )
    csv_rows, error = parse_csv_file(csv_path)
    if error:
        return [], error
    
    # Chunks only from rows that have cold (e.g. 23 chunks; rows with cold empty are skipped)
    cold_rows = [r for r in csv_rows if r.get('cold') is not None]
    
    chunks = []
    for chunk_idx, row in enumerate(cold_rows, 1):
        cold_num = row['cold']
        old_topic = (row.get('old') or '').strip()
        # In full CSV: find every row where cnew == this row's cold; collect ALL their 'new' topics
        new_topics = []
        seen_new = set()
        for r in csv_rows:
            if r.get('cnew') != cold_num:
                continue
            t = (r.get('new') or '').strip()
            if t and t not in seen_new:
                seen_new.add(t)
                new_topics.append(t)
        
        chunk = {
            'chunk_id': chunk_idx,
            'cold': cold_num,
            'cnew': cold_num,
            'old_topics': [old_topic] if old_topic else [],
            'new_topics': new_topics,
            'old_content': '',
            'new_content': '',
            'old_chapter_name': '',
            'new_chapter_name': ''
        }
        if old_topic:
            old_result = extract_topic_content_from_ocr(
                old_ocr_json_path,
                [old_topic],
                chapter_num=cold_num,
                exclude_table_type=exclude_table_type
            )
            if old_result['success']:
                chunk['old_content'] = old_result['extracted_content']
                ci = get_chapter_topics_from_ocr(old_ocr_json_path, cold_num or 0)
                if ci['success']:
                    chunk['old_chapter_name'] = ci['chapter_name']
            else:
                chunk['old_content'] = f"[ERROR: {old_result.get('error')}]"
                logger.warning(f"Chunk {chunk_idx}: Failed old content for '{old_topic}'")
        if new_topics:
            new_result = extract_topic_content_from_ocr(
                new_ocr_json_path,
                new_topics,
                chapter_num=cold_num,
                exclude_table_type=exclude_table_type
            )
            if new_result['success']:
                chunk['new_content'] = new_result['extracted_content']
                ci = get_chapter_topics_from_ocr(new_ocr_json_path, cold_num or 0)
                if ci['success']:
                    chunk['new_chapter_name'] = ci['chapter_name']
            else:
                chunk['new_content'] = f"[ERROR: {new_result.get('error')}]"
                logger.warning(f"Chunk {chunk_idx}: Failed new content for {len(new_topics)} topics")
        chunks.append(chunk)
        old_ok = not (chunk['old_content'] or '').startswith("[ERROR:")
        new_ok = not (chunk['new_content'] or '').startswith("[ERROR:")
        status = "OK" if (old_ok and new_ok) else "ERROR"
        logger.info(
            f"Chunk {chunk_idx}/{len(cold_rows)}: cold={cold_num} | old=1 topic, new={len(new_topics)} topics | {status}"
        )
    
    logger.info(f"Built {len(chunks)} chunks from CSV (cold rows only: {len(cold_rows)}; total CSV rows: {len(csv_rows)})")
    for c in chunks:
        logger.info(f"  Chunk {c['chunk_id']}: {c.get('old_topics', [])} -> {c.get('new_topics', [])}")
    return chunks, None


def create_chunks_json(chunks: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Create JSON structure from chunks for model input
    
    Args:
        chunks: List of chunk dictionaries
        
    Returns:
        JSON structure with chunks
    """
    return {
        "chunks": chunks,
        "total_chunks": len(chunks),
        "metadata": {
            "chunk_count": len(chunks),
            "chunks_info": [
                {
                    "chunk_id": chunk['chunk_id'],
                    "cold": chunk['cold'],
                    "cnew": chunk['cnew'],
                    "old_topics_count": len(chunk['old_topics']),
                    "new_topics_count": len(chunk['new_topics']),
                    "old_content_length": len(chunk['old_content']),
                    "new_content_length": len(chunk['new_content'])
                }
                for chunk in chunks
            ]
        }
    }

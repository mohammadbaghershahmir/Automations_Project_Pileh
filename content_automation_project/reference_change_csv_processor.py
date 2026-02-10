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
    Extract all topics from a specific chapter in OCR JSON
    
    Args:
        ocr_json_path: Path to OCR JSON file
        chapter_num: Chapter number (1-indexed)
        
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
        
        # Get chapter by index (1-indexed, so subtract 1)
        if chapter_num < 1 or chapter_num > len(chapters):
            result['error'] = f"Chapter {chapter_num} not found (available: 1-{len(chapters)})"
            return result
        
        chapter_obj = chapters[chapter_num - 1]
        if not isinstance(chapter_obj, dict):
            result['error'] = f"Chapter {chapter_num} has invalid structure"
            return result
        
        chapter_name = chapter_obj.get('chapter', '')
        result['chapter_name'] = chapter_name
        
        # Extract topics from all subchapters in this chapter
        subchapters = chapter_obj.get('subchapters', [])
        for subchapter_obj in subchapters:
            if not isinstance(subchapter_obj, dict):
                continue
            
            topics = subchapter_obj.get('topics', [])
            for topic_obj in topics:
                if not isinstance(topic_obj, dict):
                    continue
                
                topic_name = topic_obj.get('topic', '').strip()
                if topic_name:
                    result['topics_map'][topic_name] = topic_obj
                    result['available_topics'].append(topic_name)
        
        result['success'] = True
        logger.info(f"Found {len(result['available_topics'])} topics in chapter {chapter_num} ({chapter_name})")
        
    except FileNotFoundError:
        result['error'] = f"OCR JSON file not found: {ocr_json_path}"
    except json.JSONDecodeError as e:
        result['error'] = f"Invalid JSON file: {str(e)}"
    except Exception as e:
        result['error'] = f"Error extracting topics: {str(e)}"
        logger.exception("Error extracting topics from OCR JSON")
    
    return result


def extract_topic_content_from_ocr(ocr_json_path: str, topic_names: List[str], chapter_num: Optional[int] = None, exclude_table_type: bool = True) -> Dict[str, Any]:
    """
    Extract OCR content for specific topics, optionally filtered by chapter
    
    Args:
        ocr_json_path: Path to OCR JSON file
        topic_names: List of topic names to extract
        chapter_num: Optional chapter number to filter (1-indexed)
        exclude_table_type: If True, exclude table extractions
        
    Returns:
        Dictionary with extracted content and metadata
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
        
        # If chapter_num specified, only search in that chapter
        if chapter_num:
            if chapter_num < 1 or chapter_num > len(chapters):
                result['error'] = f"Chapter {chapter_num} not found"
                return result
            chapters_to_search = [chapters[chapter_num - 1]]
        else:
            chapters_to_search = chapters
        
        # Extract all available topics
        available_topics = []
        topics_map = {}
        
        for chapter_obj in chapters_to_search:
            if not isinstance(chapter_obj, dict):
                continue
            
            subchapters = chapter_obj.get('subchapters', [])
            for subchapter_obj in subchapters:
                if not isinstance(subchapter_obj, dict):
                    continue
                
                topics = subchapter_obj.get('topics', [])
                for topic_obj in topics:
                    if not isinstance(topic_obj, dict):
                        continue
                    
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
        
        # Extract content from matched topics
        extracted_parts = []
        
        for topic_data in matched_topic_data:
            topic_name = topic_data.get('topic', '')
            extractions = topic_data.get('extractions', [])
            
            if not extractions:
                logger.warning(f"Topic '{topic_name}' has no extractions")
                continue
            
            # Filter extractions
            filtered_extractions = []
            for extraction in extractions:
                if not isinstance(extraction, dict):
                    continue
                
                if exclude_table_type and extraction.get('type', '').lower() == 'table':
                    continue
                
                filtered_extractions.append(extraction)
            
            # Extract text from filtered extractions
            for extraction in filtered_extractions:
                extraction_text = extraction.get('Extraction', extraction.get('extraction', ''))
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
    exclude_table_type: bool = True
) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """
    Build chunks from CSV file for reference change processing
    
    Args:
        csv_path: Path to CSV file with old, cold, new, cnew columns
        old_ocr_json_path: Path to old reference OCR JSON
        new_ocr_json_path: Path to new reference OCR JSON
        exclude_table_type: If True, exclude table extractions
        
    Returns:
        Tuple of (list of chunks, error_message if any)
        
    Each chunk is a dict with:
    {
        'chunk_id': int,
        'cold': int,
        'cnew': int,
        'old_topics': List[str],
        'new_topics': List[str],
        'old_content': str,
        'new_content': str,
        'old_chapter_name': str,
        'new_chapter_name': str
    }
    """
    # Parse CSV
    csv_rows, error = parse_csv_file(csv_path)
    if error:
        return [], error
    
    # Group rows by (cold, cnew) to create chunks
    chunk_groups = defaultdict(lambda: {'old_topics': [], 'new_topics': [], 'rows': []})
    
    for row in csv_rows:
        key = (row['cold'], row['cnew'])
        if row['old']:
            chunk_groups[key]['old_topics'].append(row['old'])
        if row['new']:
            chunk_groups[key]['new_topics'].append(row['new'])
        chunk_groups[key]['rows'].append(row)
    
    chunks = []
    
    # Build each chunk
    for chunk_idx, ((cold, cnew), group_data) in enumerate(chunk_groups.items(), 1):
        old_topics = list(set(group_data['old_topics']))  # Remove duplicates
        new_topics = list(set(group_data['new_topics']))  # Remove duplicates
        
        if not old_topics and not new_topics:
            logger.warning(f"Chunk {chunk_idx}: Skipping empty chunk (cold={cold}, cnew={cnew})")
            continue
        
        chunk = {
            'chunk_id': chunk_idx,
            'cold': cold,
            'cnew': cnew,
            'old_topics': old_topics,
            'new_topics': new_topics,
            'old_content': '',
            'new_content': '',
            'old_chapter_name': '',
            'new_chapter_name': ''
        }
        
        # Extract old content if topics exist
        if old_topics and cold:
            old_result = extract_topic_content_from_ocr(
                old_ocr_json_path,
                old_topics,
                chapter_num=cold,
                exclude_table_type=exclude_table_type
            )
            
            if old_result['success']:
                chunk['old_content'] = old_result['extracted_content']
                # Try to get chapter name
                chapter_info = get_chapter_topics_from_ocr(old_ocr_json_path, cold)
                if chapter_info['success']:
                    chunk['old_chapter_name'] = chapter_info['chapter_name']
            else:
                logger.warning(f"Chunk {chunk_idx}: Failed to extract old content: {old_result.get('error')}")
                chunk['old_content'] = f"[ERROR: {old_result.get('error')}]"
        
        # Extract new content if topics exist
        if new_topics and cnew:
            new_result = extract_topic_content_from_ocr(
                new_ocr_json_path,
                new_topics,
                chapter_num=cnew,
                exclude_table_type=exclude_table_type
            )
            
            if new_result['success']:
                chunk['new_content'] = new_result['extracted_content']
                # Try to get chapter name
                chapter_info = get_chapter_topics_from_ocr(new_ocr_json_path, cnew)
                if chapter_info['success']:
                    chunk['new_chapter_name'] = chapter_info['chapter_name']
            else:
                logger.warning(f"Chunk {chunk_idx}: Failed to extract new content: {new_result.get('error')}")
                chunk['new_content'] = f"[ERROR: {new_result.get('error')}]"
        
        chunks.append(chunk)
    
    logger.info(f"Built {len(chunks)} chunks from CSV")
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

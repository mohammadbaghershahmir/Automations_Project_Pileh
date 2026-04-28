"""
Reference Change Topic Extractor
Extracts OCR content from JSON files based on topic names with fuzzy matching
"""

import json
import logging
from typing import List, Dict, Any, Optional
from difflib import SequenceMatcher


def similarity(a: str, b: str) -> float:
    """Calculate similarity ratio between two strings (0.0 to 1.0)"""
    return SequenceMatcher(None, a.lower().strip(), b.lower().strip()).ratio()


def find_matching_topic(topic_name: str, available_topics: List[str], threshold: float = 0.8) -> Optional[str]:
    """
    Find matching topic from available topics using fuzzy matching
    
    Args:
        topic_name: Topic name to search for
        available_topics: List of available topic names
        threshold: Minimum similarity threshold (default: 0.8 - increased for precision)
    
    Returns:
        Matching topic name or None if no match found
    """
    if not topic_name or not available_topics:
        return None
    
    topic_name_clean = topic_name.strip().lower()
    best_match = None
    best_score = 0.0
    
    for available_topic in available_topics:
        if not available_topic:
            continue
        
        available_topic_clean = available_topic.strip().lower()
        
        # Exact match (case-insensitive)
        if topic_name_clean == available_topic_clean:
            return available_topic
        
        # Fuzzy match
        score = similarity(topic_name_clean, available_topic_clean)
        if score > best_score:
            best_score = score
            best_match = available_topic
    
    if best_score >= threshold:
        return best_match
    
    return None


def extract_topics_from_ocr_json(
    ocr_json_path: str,
    topic_names: List[str],
    exclude_table_type: bool = True
) -> Dict[str, Any]:
    """
    Extract OCR content from JSON file based on topic names
    
    Args:
        ocr_json_path: Path to OCR JSON file
        topic_names: List of topic names to extract
        exclude_table_type: If True, exclude extractions with type="table"
    
    Returns:
        Dictionary with:
        - 'success': bool
        - 'extracted_content': str (combined OCR text)
        - 'matched_topics': List[str] (successfully matched topics)
        - 'missing_topics': List[str] (topics not found in JSON)
        - 'error': str (if any error occurred)
    """
    logger = logging.getLogger(__name__)
    
    result = {
        'success': False,
        'extracted_content': '',
        'matched_topics': [],
        'missing_topics': [],
        'error': None
    }
    
    try:
        # Load OCR JSON file
        with open(ocr_json_path, 'r', encoding='utf-8') as f:
            ocr_data = json.load(f)
        
        # Extract all available topics from JSON
        available_topics = []
        topics_map = {}  # topic_name -> full topic data
        
        chapters = ocr_data.get('chapters', [])
        for chapter_obj in chapters:
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
        
        logger.info(f"Found {len(available_topics)} topics in OCR JSON")
        
        # Match user-provided topics with available topics
        matched_topic_data = []
        missing_topics = []
        
        for user_topic in topic_names:
            if not user_topic or not user_topic.strip():
                continue
            
            matched_topic = find_matching_topic(user_topic.strip(), available_topics)
            
            if matched_topic:
                matched_topic_data.append(topics_map[matched_topic])
                result['matched_topics'].append(matched_topic)
                logger.info(f"Matched '{user_topic}' -> '{matched_topic}'")
            else:
                missing_topics.append(user_topic)
                result['missing_topics'].append(user_topic)
                logger.warning(f"Topic '{user_topic}' not found in OCR JSON")
        
        # If any topics are missing, return error
        if missing_topics:
            result['error'] = f"Topics not found in OCR JSON: {', '.join(missing_topics)}"
            return result
        
        # Extract content from matched topics
        extracted_parts = []
        
        for topic_data in matched_topic_data:
            topic_name = topic_data.get('topic', '')
            extractions = topic_data.get('extractions', [])
            
            if not extractions:
                logger.warning(f"Topic '{topic_name}' has no extractions")
                continue
            
            # Filter extractions (exclude tables if requested)
            filtered_extractions = []
            for extraction in extractions:
                if not isinstance(extraction, dict):
                    continue
                
                # Exclude table type if requested
                if exclude_table_type and extraction.get('type', '').lower() == 'table':
                    continue
                
                filtered_extractions.append(extraction)
            
            # Extract text from filtered extractions
            for extraction in filtered_extractions:
                extraction_text = extraction.get('Extraction', extraction.get('extraction', ''))
                if extraction_text:
                    extracted_parts.append(extraction_text)
        
        # Combine all extracted content
        result['extracted_content'] = '\n\n'.join(extracted_parts)
        result['success'] = True
        
        logger.info(f"Extracted {len(extracted_parts)} extraction parts, total length: {len(result['extracted_content'])} chars")
        
    except FileNotFoundError:
        result['error'] = f"OCR JSON file not found: {ocr_json_path}"
        logger.error(result['error'])
    except json.JSONDecodeError as e:
        result['error'] = f"Invalid JSON file: {str(e)}"
        logger.error(result['error'])
    except Exception as e:
        result['error'] = f"Error extracting topics: {str(e)}"
        logger.exception("Error extracting topics from OCR JSON")
    
    return result

"""
Base class for all stage processors.
Provides common functionality for JSON handling, file operations, and PointId management.
"""

import json
import logging
import os
from datetime import datetime
from typing import Optional, Dict, List, Any
from third_stage_converter import ThirdStageConverter
from txt_stage_json_utils import load_stage_txt_as_json


class BaseStageProcessor:
    """Base class providing common functionality for all stage processors"""
    
    def __init__(self, api_client):
        """
        Initialize base processor.
        
        Args:
            api_client: GeminiAPIClient instance for API calls
        """
        self.api_client = api_client
        self.logger = logging.getLogger(self.__class__.__name__)
        self.converter = ThirdStageConverter()
    
    def extract_json_from_response(self, response_text: str) -> Optional[Dict | List]:
        """
        Extract JSON from model response.
        Uses ThirdStageConverter for robust extraction.
        
        Args:
            response_text: Raw response text from model
            
        Returns:
            Parsed JSON dictionary, list, or None on error
        """
        if not response_text or not response_text.strip():
            self.logger.warning("Empty response text provided")
            return None
        
        try:
            json_obj = self.converter._extract_json_from_response(response_text)
            if json_obj is not None:
                self.logger.info("Successfully extracted JSON from response")
                return json_obj
            else:
                self.logger.warning("Failed to extract JSON from response")
                return None
        except Exception as e:
            self.logger.error(f"Error extracting JSON from response: {e}")
            return None
    
    def load_json_file(self, file_path: str) -> Optional[Dict]:
        """
        Load JSON file with comprehensive error handling.
        
        Args:
            file_path: Path to JSON file
            
        Returns:
            Parsed JSON dictionary or None on error
        """
        if not file_path or not file_path.strip():
            self.logger.error("Empty file path provided")
            return None
        
        if not os.path.exists(file_path):
            self.logger.error(f"File not found: {file_path}")
            return None
        
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            self.logger.info(f"Successfully loaded JSON: {file_path}")
            return data
        except json.JSONDecodeError as e:
            self.logger.error(f"Invalid JSON in {file_path}: {e}")
            return None
        except Exception as e:
            self.logger.error(f"Error loading {file_path}: {e}")
            return None
    
    def save_json_file(self, data: List[Dict], file_path: str, 
                      metadata: Dict, stage_name: str) -> bool:
        """
        Save JSON with standard structure: {metadata, data}
        
        Args:
            data: List of data records
            file_path: Output file path
            metadata: Additional metadata to include
            stage_name: Name of the stage (e.g., "E", "F", "J")
            
        Returns:
            True if successful, False otherwise
        """
        if not data:
            self.logger.warning("No data to save")
            data = []
        
        try:
            standard_json = {
                "metadata": {
                    "stage": stage_name,
                    "processed_at": datetime.now().isoformat(),
                    "total_records": len(data),
                    **metadata
                },
                "data": data
            }
            
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(file_path) or '.', exist_ok=True)
            
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(standard_json, f, ensure_ascii=False, indent=2)
            
            self.logger.info(f"Successfully saved JSON: {file_path} ({len(data)} records)")
            return True
        except Exception as e:
            self.logger.error(f"Error saving {file_path}: {e}")
            return False
    
    def generate_filename(self, prefix: str, book: int, chapter: int, 
                         base_dir: Optional[str] = None) -> str:
        """
        Generate filename: prefix + book + chapter
        Example: e105003.json
        
        Args:
            prefix: File prefix (e.g., "e", "f", "a")
            book: Book ID (3 digits)
            chapter: Chapter ID (3 digits)
            base_dir: Optional base directory
            
        Returns:
            Full file path
        """
        filename = f"{prefix}{book:03d}{chapter:03d}.json"
        if base_dir:
            return os.path.join(base_dir, filename)
        return filename
    
    def extract_book_chapter_from_pointid(self, point_id: str) -> tuple[int, int]:
        """
        Extract book and chapter from PointId.
        Format: BBBCCCPPPP (10 digits)
        - BBB (3 digits): Book ID
        - CCC (3 digits): Chapter ID
        - PPPP (4 digits): Point index
        
        Args:
            point_id: 10-digit PointId string
            
        Returns:
            Tuple of (book_id, chapter_id)
            
        Raises:
            ValueError: If PointId format is invalid
        """
        try:
            if not point_id or len(point_id) != 10 or not point_id.isdigit():
                raise ValueError(f"Invalid PointId format: {point_id} (must be 10 digits)")
            
            book = int(point_id[0:3])
            chapter = int(point_id[3:6])
            return book, chapter
        except (ValueError, IndexError) as e:
            self.logger.error(f"Error extracting book/chapter from PointId {point_id}: {e}")
            raise
    
    def get_data_from_json(self, json_data: Dict | List) -> List[Dict]:
        """
        Extract data array from JSON.
        Supports multiple structures: 
        - Direct array: [{...}, {...}]
        - Object with data: {metadata, data}, {metadata, points}, {rows}
        
        Args:
            json_data: JSON dictionary or list
            
        Returns:
            List of data records
        """
        # If json_data is already a list, return it directly
        if isinstance(json_data, list):
            return json_data
        
        # If json_data is a dict, look for data/points/rows keys
        if isinstance(json_data, dict):
            if "data" in json_data:
                return json_data["data"]
            elif "points" in json_data:
                return json_data["points"]
            elif "rows" in json_data:
                return json_data["rows"]
            else:
                self.logger.warning("No data/points/rows found in JSON, returning empty list")
                return []
        
        # Fallback: return empty list
        self.logger.warning(f"Unexpected JSON data type: {type(json_data)}, returning empty list")
        return []
    
    def get_metadata_from_json(self, json_data: Dict) -> Dict:
        """
        Extract metadata from JSON.
        
        Args:
            json_data: JSON dictionary
            
        Returns:
            Metadata dictionary (empty dict if not found)
        """
        return json_data.get("metadata", {})
    
    def get_first_pointid_from_json(self, json_data: Dict) -> Optional[str]:
        """
        Get the first PointId from JSON data.
        Useful for extracting book/chapter information.
        
        Args:
            json_data: JSON dictionary
            
        Returns:
            First PointId string or None
        """
        data = self.get_data_from_json(json_data)
        if not data:
            return None
        
        first_record = data[0]
        return first_record.get("PointId", None)
    
    def load_txt_as_json(self, txt_path: str) -> Optional[Dict]:
        """
        Load TXT file and extract JSON from it.
        Uses txt_stage_json_utils for robust extraction.
        
        Args:
            txt_path: Path to TXT file
            
        Returns:
            Parsed JSON dictionary or None on error
        """
        if not os.path.exists(txt_path):
            self.logger.error(f"TXT file not found: {txt_path}")
            return None
        
        try:
            json_obj = load_stage_txt_as_json(txt_path)
            if json_obj:
                self.logger.info(f"Successfully loaded JSON from TXT: {txt_path}")
            return json_obj
        except Exception as e:
            self.logger.error(f"Error loading TXT as JSON {txt_path}: {e}")
            return None
    
    def load_txt_as_json_from_text(self, text: str) -> Optional[Dict | List]:
        """
        Extract JSON directly from text (without file).
        Uses ThirdStageConverter for robust extraction.
        
        Args:
            text: Text content containing JSON
            
        Returns:
            Parsed JSON dictionary, list, or None on error
        """
        if not text or not text.strip():
            return None
        
        try:
            json_obj = self.converter._extract_json_from_response(text)
            if json_obj:
                self.logger.info(f"Successfully extracted JSON from text ({len(text)} chars)")
            return json_obj
        except Exception as e:
            self.logger.error(f"Error extracting JSON from text: {e}")
            return None


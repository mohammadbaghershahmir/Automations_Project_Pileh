"""
Pre-OCR Topic Extraction Processor
Extracts topics from PDF before OCR extraction.
Output: t{book}{chapter}.json
"""

import json
import logging
import os
from typing import Optional, Dict, List, Any, Callable

from base_stage_processor import BaseStageProcessor
from api_layer import APIConfig


class PreOCRTopicProcessor(BaseStageProcessor):
    """Process Pre-OCR: Extract topics from PDF"""
    
    def __init__(self, api_client):
        super().__init__(api_client)
        self.logger = logging.getLogger(__name__)
    
    def process_pre_ocr_topic(
        self,
        pdf_path: str,
        prompt: str,
        model_name: str,
        book_id: Optional[int] = None,
        chapter_id: Optional[int] = None,
        output_dir: Optional[str] = None,
        progress_callback: Optional[Callable[[str], None]] = None
    ) -> Optional[str]:
        """
        Process Pre-OCR: Extract topics from PDF using LLM.
        
        Args:
            pdf_path: Path to PDF file
            prompt: Prompt for topic extraction
            model_name: Gemini model name
            book_id: Book ID (optional, will try to extract from PDF if not provided)
            chapter_id: Chapter ID (optional, will try to extract from PDF if not provided)
            output_dir: Output directory (defaults to PDF directory)
            progress_callback: Optional callback for progress updates
            
        Returns:
            Path to output file (t{book}{chapter}.json) or None on error
        """
        def _progress(msg: str):
            if progress_callback:
                progress_callback(msg)
            self.logger.info(msg)
        
        _progress("Starting Pre-OCR Topic Extraction...")
        
        # Determine output directory
        if not output_dir:
            output_dir = os.path.dirname(pdf_path) or os.getcwd()
        os.makedirs(output_dir, exist_ok=True)
        
        # Extract book_id and chapter_id from filename if not provided
        if book_id is None or chapter_id is None:
            try:
                base_name = os.path.splitext(os.path.basename(pdf_path))[0]
                import re
                numbers = re.findall(r'\d+', base_name)
                if len(numbers) >= 2:
                    book_id = int(numbers[0])
                    chapter_id = int(numbers[1])
                    _progress(f"Extracted Book ID: {book_id}, Chapter ID: {chapter_id} from filename")
            except:
                pass
        
        # Default values
        if book_id is None:
            book_id = 105
        if chapter_id is None:
            chapter_id = 3
        
        _progress(f"Using Book ID: {book_id}, Chapter ID: {chapter_id}")
        
        # Extract text from PDF with complete metadata (formatting, structure, etc.)
        _progress("Extracting text from PDF with complete metadata...")
        from pdf_processor import PDFProcessor
        pdf_proc = PDFProcessor()
        extracted_text = pdf_proc.extract_text_with_formatting(pdf_path)
        
        if not extracted_text:
            self.logger.error("Failed to extract text from PDF")
            _progress("Error: Failed to extract text from PDF")
            return None
        
        char_count = len(extracted_text)
        _progress(f"Extracted {char_count:,} characters from PDF with complete metadata")
        
        # Process PDF text to extract topics using LLM
        _progress("Processing PDF text to extract topics with LLM...")
        
        # Set stage if using UnifiedAPIClient (for API routing)
        if hasattr(self.api_client, 'set_stage'):
            self.api_client.set_stage("pre_ocr_topic")
        
        # Prepare prompt with extracted text
        full_prompt = f"""{prompt}

Please analyze the PDF content and extract the topics structure. Return a JSON format with topics information.

--- PDF Content ---
{extracted_text}"""
        
        # Use api_client.process_text which handles routing automatically
        # Determine maximum tokens based on model
        if '2.5' in model_name or '2.0' in model_name:
            model_max_tokens = 32768
        elif '1.5' in model_name:
            model_max_tokens = 8192
        else:
            model_max_tokens = 32768
        
        # Call model using process_text (handles API routing automatically)
        response_text = self.api_client.process_text(
            text=full_prompt,
            system_prompt=None,
            model_name=model_name,
            temperature=0.7,
            max_tokens=model_max_tokens
        )
        
        if not response_text:
            self.logger.error("No response from model")
            _progress("Error: No response from model")
            return None
        
        _progress(f"Received response ({len(response_text)} characters)")
        
        # Extract JSON from response
        _progress("Extracting JSON from response...")
        topics_data = self.extract_json_from_response(response_text)
        if not topics_data:
            _progress("Trying to extract JSON from text using fallback...")
            topics_data = self.load_txt_as_json_from_text(response_text)
        
        if not topics_data:
            self.logger.error("Failed to extract JSON from model response")
            _progress("Error: Failed to extract JSON from response")
            return None
        
        # Ensure topics_data is a list or dict
        if isinstance(topics_data, dict):
            # Try to get topics from common keys
            topics_list = topics_data.get("topics", topics_data.get("data", topics_data.get("rows", [])))
            if not isinstance(topics_list, list):
                topics_list = [topics_data]
        elif isinstance(topics_data, list):
            topics_list = topics_data
        else:
            topics_list = [topics_data]
        
        _progress(f"Extracted {len(topics_list)} topics")
        
        # Save output JSON with unique filename (includes PDF name to prevent overwriting)
        pdf_basename = os.path.splitext(os.path.basename(pdf_path))[0]
        output_filename = self.generate_unique_filename("t", book_id, chapter_id, pdf_basename, output_dir)
        output_path = os.path.join(output_dir, output_filename)
        
        output_metadata = {
            "stage": "Pre-OCR-Topic",
            "processed_at": self._get_timestamp(),
            "total_records": len(topics_list),
            "book_id": book_id,
            "chapter_id": chapter_id,
            "model": model_name,
            "source_pdf": os.path.basename(pdf_path),
            "total_topics": len(topics_list)
        }
        
        # Output structure matching the JSON format
        output_data = {
            "metadata": output_metadata,
            "data": topics_list
        }
        
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                json.dump(output_data, f, ensure_ascii=False, indent=2)
            
            self.logger.info(f"Pre-OCR Topic Extraction completed: {output_path}")
            _progress(f"Pre-OCR Topic Extraction completed: {output_filename}")
            
            return output_path
        except Exception as e:
            self.logger.error(f"Failed to save output JSON: {e}")
            _progress(f"Error: Failed to save output file")
            return None
    
    def _get_timestamp(self) -> str:
        """Get current timestamp"""
        from datetime import datetime
        return datetime.now().isoformat()
    
    def generate_unique_filename(self, prefix: str, book: int, chapter: int, 
                                 pdf_name: str, base_dir: Optional[str] = None) -> str:
        """
        Generate unique filename with PDF name: prefix + book + chapter + pdf_name
        Example: t105003_Bolognia 5th Edition 2024 (1)-29-6-22.json
        
        Args:
            prefix: File prefix (e.g., "t")
            book: Book ID (3 digits)
            chapter: Chapter ID (3 digits)
            pdf_name: PDF filename without extension (sanitized)
            base_dir: Optional base directory
            
        Returns:
            Full file path with PDF name appended
        """
        import re
        # Sanitize PDF name: remove/replace invalid filename characters
        sanitized_pdf_name = re.sub(r'[<>:"/\\|?*]', '_', pdf_name)
        sanitized_pdf_name = sanitized_pdf_name.strip()
        # Limit length to avoid very long filenames
        if len(sanitized_pdf_name) > 100:
            sanitized_pdf_name = sanitized_pdf_name[:100]
        
        filename = f"{prefix}{book:03d}{chapter:03d}_{sanitized_pdf_name}.json"
        if base_dir:
            return os.path.join(base_dir, filename)
        return filename
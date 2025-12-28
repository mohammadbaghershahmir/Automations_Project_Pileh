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
        
        # Extract text from PDF
        _progress("Extracting text from PDF...")
        from pdf_processor import PDFProcessor
        pdf_proc = PDFProcessor()
        extracted_text = pdf_proc.extract_text(pdf_path)
        
        if not extracted_text:
            self.logger.error("Failed to extract text from PDF")
            _progress("Error: Failed to extract text from PDF")
            return None
        
        char_count = len(extracted_text)
        _progress(f"Extracted {char_count:,} characters from PDF")
        
        # Save extracted text to TXT file
        base_name = os.path.splitext(os.path.basename(pdf_path))[0]
        txt_filename = f"{base_name}_extracted_text.txt"
        txt_path = os.path.join(output_dir, txt_filename)
        
        try:
            with open(txt_path, 'w', encoding='utf-8') as f:
                f.write(extracted_text)
            _progress(f"Saved extracted text to: {os.path.basename(txt_filename)}")
            self.logger.info(f"Extracted text saved to: {txt_path}")
        except Exception as e:
            self.logger.warning(f"Failed to save extracted text: {e}")
            txt_path = None
        
        # Process PDF text to extract topics using LLM
        _progress("Processing PDF text to extract topics with LLM...")
        
        # Ensure text_client is using the correct model
        if (not hasattr(self.api_client, '_current_model_name') or 
            self.api_client._current_model_name != model_name):
            if not self.api_client.initialize_text_client(model_name):
                self.logger.error("Failed to initialize text client")
                return None
        elif not self.api_client.text_client:
            if not self.api_client.initialize_text_client(model_name):
                self.logger.error("Failed to initialize text client")
                return None
        
        # Prepare prompt with extracted text
        full_prompt = f"""{prompt}

Please analyze the PDF content and extract the topics structure. Return a JSON format with topics information.

--- PDF Content ---
{extracted_text}"""
        
        # Call model
        max_retries = 3
        response_text = None
        for attempt in range(max_retries):
            try:
                # Ensure text_client is using the correct model before each call
                if (not hasattr(self.api_client, '_current_model_name') or 
                    self.api_client._current_model_name != model_name):
                    import google.generativeai as genai
                    key = self.api_client.key_manager.get_next_key()
                    if not key:
                        self.logger.error("No API key available")
                        return None
                    genai.configure(api_key=key)
                    self.api_client.text_client = genai.GenerativeModel(model_name)
                    self.api_client._current_model_name = model_name
                    self.logger.info(f"Recreated text_client with model: {model_name}")
                
                # Determine maximum tokens based on model
                import google.generativeai as genai
                if '2.5' in model_name or '2.0' in model_name:
                    model_max_tokens = 32768
                elif '1.5' in model_name:
                    model_max_tokens = 8192
                else:
                    model_max_tokens = 32768
                
                generation_config = genai.types.GenerationConfig(
                    temperature=0.7,
                    max_output_tokens=model_max_tokens,
                )
                
                response = self.api_client.text_client.generate_content(
                    full_prompt,
                    generation_config=generation_config,
                    stream=False
                )
                
                response_text = response.text if hasattr(response, 'text') and response.text else None
                if response_text:
                    _progress(f"Received response ({len(response_text)} characters)")
                    break
            except Exception as e:
                self.logger.warning(f"Attempt {attempt + 1} failed: {e}")
                if attempt < max_retries - 1:
                    _progress(f"Retrying... (attempt {attempt + 2}/{max_retries})")
                else:
                    self.logger.error("All attempts failed")
        
        if not response_text:
            self.logger.error("No response from model")
            _progress("Error: No response from model")
            return None
        
        # Save raw response to TXT file
        response_txt_filename = f"{base_name}_pre_ocr_topic.txt"
        response_txt_path = os.path.join(output_dir, response_txt_filename)
        try:
            with open(response_txt_path, 'w', encoding='utf-8') as f:
                f.write("=== PRE-OCR TOPIC EXTRACTION RESPONSE ===\n\n")
                f.write(response_text)
            _progress(f"Saved raw response to: {os.path.basename(response_txt_filename)}")
        except Exception as e:
            self.logger.warning(f"Failed to save response TXT file: {e}")
        
        # Extract JSON from response
        _progress("Extracting JSON from response...")
        topics_data = self.extract_json_from_response(response_text)
        if not topics_data:
            _progress("Trying to extract JSON from text using fallback...")
            topics_data = self.load_txt_as_json_from_text(response_text)
        if not topics_data:
            _progress("Trying to load JSON from TXT file...")
            topics_data = self.load_txt_as_json(response_txt_path)
        
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
        
        # Save output JSON
        output_filename = self.generate_filename("t", book_id, chapter_id, output_dir)
        output_path = os.path.join(output_dir, output_filename)
        
        output_metadata = {
            "stage": "Pre-OCR-Topic",
            "processed_at": self._get_timestamp(),
            "total_records": len(topics_list),
            "book_id": book_id,
            "chapter_id": chapter_id,
            "model": model_name,
            "source_pdf": os.path.basename(pdf_path),
            "total_topics": len(topics_list),
            "txt_file": os.path.basename(response_txt_path)
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


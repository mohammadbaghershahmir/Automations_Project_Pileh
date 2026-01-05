"""
Word file processor for reading Word documents as plain text.
Converts Word files to plain text for model input.
"""

import logging
import os
from typing import Optional


class WordFileProcessor:
    """Process Word files and convert to plain text for model input"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self._docx_available = False
        self._check_docx_availability()
    
    def _check_docx_availability(self):
        """Check if python-docx is available"""
        try:
            import docx
            self._docx_available = True
            self.logger.info("python-docx library is available")
        except ImportError:
            self._docx_available = False
            self.logger.warning("python-docx not available. Will try alternative methods.")
    
    def read_word_file(self, file_path: str) -> Optional[str]:
        """
        Read Word file and return as plain text.
        
        Args:
            file_path: Path to Word file (.docx or .doc)
            
        Returns:
            Plain text content or None on error
        """
        if not file_path or not file_path.strip():
            self.logger.error("Empty file path provided")
            return None
        
        if not os.path.exists(file_path):
            self.logger.error(f"Word file not found: {file_path}")
            return None
        
        # Try python-docx first (for .docx files)
        if self._docx_available:
            try:
                from docx import Document
                doc = Document(file_path)
                paragraphs = [para.text for para in doc.paragraphs if para.text.strip()]
                text = '\n'.join(paragraphs)
                
                if text.strip():
                    self.logger.info(f"Successfully read Word file using python-docx: {file_path} ({len(text)} chars)")
                    return text
                else:
                    self.logger.warning(f"Word file is empty: {file_path}")
                    return None
            except Exception as e:
                self.logger.warning(f"python-docx failed for {file_path}: {e}. Trying alternative...")
        
        # Fallback: Try to read as text file (if it's actually a text file)
        try:
            with open(file_path, 'r', encoding='utf-8') as f:
                content = f.read()
                if content.strip():
                    self.logger.info(f"Read file as plain text: {file_path} ({len(content)} chars)")
                    return content
                else:
                    self.logger.warning(f"File is empty: {file_path}")
                    return None
        except UnicodeDecodeError:
            # Try with different encoding
            try:
                with open(file_path, 'r', encoding='latin-1') as f:
                    content = f.read()
                    if content.strip():
                        self.logger.info(f"Read file as plain text (latin-1): {file_path}")
                        return content
            except Exception as e:
                self.logger.error(f"Failed to read {file_path} with latin-1 encoding: {e}")
        except Exception as e:
            self.logger.error(f"Failed to read {file_path} as text: {e}")
        
        self.logger.error(f"Could not read Word file: {file_path}")
        return None
    
    def prepare_word_for_model(self, word_text: str, context: str = "") -> str:
        """
        Prepare Word content for model input.
        Adds a header to identify the content source.
        
        Args:
            word_text: Plain text content from Word file
            context: Optional context description
            
        Returns:
            Formatted text for model input
        """
        if not word_text or not word_text.strip():
            return ""
        
        formatted = "Word Document Content"
        if context:
            formatted += f" ({context})"
        formatted += ":\n\n"
        formatted += word_text.strip()
        
        return formatted
    
    def is_word_file(self, file_path: str) -> bool:
        """
        Check if file is a Word file based on extension.
        
        Args:
            file_path: Path to file
            
        Returns:
            True if file has Word extension
        """
        if not file_path:
            return False
        
        ext = os.path.splitext(file_path)[1].lower()
        return ext in ['.docx', '.doc']





















"""
PDF Processor Module
Handles PDF validation, page counting, and text extraction
"""

import os
import logging
from typing import Optional, Tuple
from pathlib import Path

try:
    import PyPDF2
    PDF_LIBRARY_AVAILABLE = True
except ImportError:
    PDF_LIBRARY_AVAILABLE = False
    logging.warning("PyPDF2 library not available. PDF features will be disabled.")

try:
    import fitz  # PyMuPDF
    MUPDF_AVAILABLE = True
except ImportError:
    MUPDF_AVAILABLE = False


class PDFProcessor:
    """Handles PDF file operations"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
    
    def validate_pdf(self, file_path: str) -> Tuple[bool, Optional[str], int]:
        """
        Validate PDF file and count pages
        
        Args:
            file_path: Path to PDF file
            
        Returns:
            Tuple of (is_valid, error_message, page_count)
            If valid: (True, None, page_count)
            If invalid: (False, error_message, 0)
        """
        if not os.path.exists(file_path):
            return False, "PDF file does not exist", 0
        
        if not file_path.lower().endswith('.pdf'):
            return False, "File is not a PDF", 0
        
        try:
            page_count = self.count_pages(file_path)
            
            if page_count == 0:
                return False, "PDF file is empty or corrupted", 0
            
            return True, None, page_count
            
        except Exception as e:
            self.logger.error(f"Error validating PDF: {str(e)}")
            return False, f"Error reading PDF: {str(e)}", 0
    
    def count_pages(self, file_path: str) -> int:
        """
        Count number of pages in PDF
        
        Args:
            file_path: Path to PDF file
            
        Returns:
            Number of pages
        """
        if not PDF_LIBRARY_AVAILABLE:
            self.logger.error("PyPDF2 library not available")
            return 0
        
        try:
            with open(file_path, 'rb') as file:
                pdf_reader = PyPDF2.PdfReader(file)
                return len(pdf_reader.pages)
        except Exception as e:
            self.logger.error(f"Error counting PDF pages: {str(e)}")
            return 0
    
    def extract_text(self, file_path: str, max_pages: Optional[int] = None) -> Optional[str]:
        """
        Extract text from PDF
        
        Args:
            file_path: Path to PDF file
            max_pages: Maximum number of pages to extract (None for all)
            
        Returns:
            Extracted text or None if failed
        """
        if not PDF_LIBRARY_AVAILABLE:
            self.logger.error("PyPDF2 library not available")
            return None
        
        try:
            with open(file_path, 'rb') as file:
                pdf_reader = PyPDF2.PdfReader(file)
                total_pages = len(pdf_reader.pages)
                pages_to_extract = min(total_pages, max_pages) if max_pages else total_pages
                
                text_content = []
                for page_num in range(pages_to_extract):
                    page = pdf_reader.pages[page_num]
                    text = page.extract_text()
                    if text.strip():
                        text_content.append(f"--- Page {page_num + 1} ---\n{text}\n")
                
                extracted_text = "\n".join(text_content)
                self.logger.info(f"Extracted text from {pages_to_extract} pages")
                return extracted_text
                
        except Exception as e:
            self.logger.error(f"Error extracting text from PDF: {str(e)}")
            return None
    
    def get_pdf_info(self, file_path: str) -> dict:
        """
        Get PDF file information
        
        Args:
            file_path: Path to PDF file
            
        Returns:
            Dictionary with PDF information
        """
        info = {
            'file_path': file_path,
            'file_name': os.path.basename(file_path),
            'file_size': 0,
            'page_count': 0,
            'is_valid': False
        }
        
        if not os.path.exists(file_path):
            return info
        
        try:
            info['file_size'] = os.path.getsize(file_path)
            info['page_count'] = self.count_pages(file_path)
            is_valid, _, _ = self.validate_pdf(file_path)
            info['is_valid'] = is_valid
        except Exception as e:
            self.logger.error(f"Error getting PDF info: {str(e)}")
        
        return info



"""
PDF Processor Module
Handles PDF validation, page counting, and text extraction using PyMuPDF
"""

import os
import logging
from typing import Optional, Tuple, List, Dict, Any
from pathlib import Path

try:
    import fitz  # PyMuPDF
    MUPDF_AVAILABLE = True
except ImportError:
    MUPDF_AVAILABLE = False
    logging.error("PyMuPDF (fitz) library not available. PDF features will be disabled.")

# Keep PyPDF2 as fallback only
try:
    import PyPDF2
    PDF_LIBRARY_AVAILABLE = True
except ImportError:
    PDF_LIBRARY_AVAILABLE = False


class PDFProcessor:
    """Handles PDF file operations using PyMuPDF (primary) with PyPDF2 as fallback"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        if not MUPDF_AVAILABLE:
            self.logger.warning("PyMuPDF not available. Some features may be limited.")
    
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
        Count number of pages in PDF using PyMuPDF (primary) or PyPDF2 (fallback)
        
        Args:
            file_path: Path to PDF file
            
        Returns:
            Number of pages
        """
        # Priority 1: Use PyMuPDF
        if MUPDF_AVAILABLE:
            try:
                doc = fitz.open(file_path)
                page_count = len(doc)
                doc.close()
                return page_count
            except Exception as e:
                self.logger.warning(f"PyMuPDF failed to count pages: {e}, trying PyPDF2...")
        
        # Priority 2: Fallback to PyPDF2
        if PDF_LIBRARY_AVAILABLE:
            try:
                with open(file_path, 'rb') as file:
                    pdf_reader = PyPDF2.PdfReader(file)
                    return len(pdf_reader.pages)
            except Exception as e:
                self.logger.error(f"PyPDF2 also failed to count pages: {e}")
                return 0
        
        self.logger.error("No PDF library available to count pages")
        return 0
    
    def extract_text(self, file_path: str, max_pages: Optional[int] = None) -> Optional[str]:
        """
        Extract text from PDF using PyMuPDF (primary) or PyPDF2 (fallback).
        This method extracts plain text without formatting markers.
        
        Args:
            file_path: Path to PDF file
            max_pages: Maximum number of pages to extract (None for all)
            
        Returns:
            Extracted text or None if failed
        """
        # Priority 1: Use PyMuPDF (better quality)
        if MUPDF_AVAILABLE:
            try:
                doc = fitz.open(file_path)
                total_pages = len(doc)
                pages_to_extract = min(total_pages, max_pages) if max_pages else total_pages
                
                text_content = []
                for page_num in range(pages_to_extract):
                    page = doc[page_num]
                    # get_text() is the most reliable method for plain text extraction
                    text = page.get_text()
                    if text and text.strip():
                        text_content.append(f"--- Page {page_num + 1} ---\n{text}\n")
                
                doc.close()
                
                if text_content:
                    extracted_text = "\n".join(text_content)
                    char_count = len(extracted_text)
                    self.logger.info(
                        f"Successfully extracted {char_count} characters from {pages_to_extract} pages using PyMuPDF"
                    )
                    return extracted_text
                else:
                    self.logger.warning("PyMuPDF returned empty text, trying PyPDF2...")
            except Exception as e:
                self.logger.warning(f"PyMuPDF extraction failed: {e}, trying PyPDF2...")
        
        # Priority 2: Fallback to PyPDF2
        if PDF_LIBRARY_AVAILABLE:
            try:
                with open(file_path, 'rb') as file:
                    pdf_reader = PyPDF2.PdfReader(file)
                    total_pages = len(pdf_reader.pages)
                    pages_to_extract = min(total_pages, max_pages) if max_pages else total_pages
                    
                    text_content = []
                    for page_num in range(pages_to_extract):
                        page = pdf_reader.pages[page_num]
                        text = page.extract_text()
                        if text and text.strip():
                            text_content.append(f"--- Page {page_num + 1} ---\n{text}\n")
                    
                    if text_content:
                        extracted_text = "\n".join(text_content)
                        char_count = len(extracted_text)
                        self.logger.info(
                            f"Successfully extracted {char_count} characters from {pages_to_extract} pages using PyPDF2 (fallback)"
                        )
                        return extracted_text
            except Exception as e:
                self.logger.error(f"PyPDF2 extraction also failed: {e}")
        
        self.logger.error("All text extraction methods failed. PDF may be corrupted or image-based.")
        return None
    
    def extract_text_with_formatting(self, file_path: str, max_pages: Optional[int] = None) -> Optional[str]:
        """
        Extract text from PDF with formatting markers (Markdown style).
        Preserves italic (*text*), bold (**text**), and bold-italic (***text***).
        
        Args:
            file_path: Path to PDF file
            max_pages: Maximum number of pages to extract (None for all)
            
        Returns:
            Extracted text with formatting markers or None if failed
        """
        if not MUPDF_AVAILABLE:
            self.logger.warning("PyMuPDF not available for formatting extraction. Falling back to plain text.")
            return self.extract_text(file_path, max_pages)
        
        try:
            doc = fitz.open(file_path)
            total_pages = len(doc)
            pages_to_extract = min(total_pages, max_pages) if max_pages else total_pages
            
            text_content = []
            for page_num in range(pages_to_extract):
                page = doc[page_num]
                blocks = page.get_text("dict")
                
                page_text = [f"--- Page {page_num + 1} ---\n"]
                
                for block in blocks.get("blocks", []):
                    if "lines" in block:  # Text block
                        for line in block["lines"]:
                            line_parts = []
                            for span in line.get("spans", []):
                                text = span.get("text", "")
                                if not text:
                                    continue
                                
                                flags = span.get("flags", 0)
                                
                                # Detect formatting (PyMuPDF flag values)
                                is_bold = flags & 16  # Bit 4 = bold
                                is_italic = flags & 2  # Bit 1 = italic
                                
                                # Apply Markdown formatting
                                if is_bold and is_italic:
                                    formatted_text = f"***{text}***"
                                elif is_bold:
                                    formatted_text = f"**{text}**"
                                elif is_italic:
                                    formatted_text = f"*{text}*"
                                else:
                                    formatted_text = text
                                
                                line_parts.append(formatted_text)
                            
                            if line_parts:
                                page_text.append(" ".join(line_parts) + "\n")
                
                if len(page_text) > 1:  # More than just the page header
                    text_content.append("".join(page_text))
            
            doc.close()
            
            if text_content:
                extracted_text = "\n".join(text_content)
                char_count = len(extracted_text)
                self.logger.info(
                    f"Successfully extracted {char_count} characters with formatting from {pages_to_extract} pages using PyMuPDF"
                )
                return extracted_text
            else:
                self.logger.warning("No text extracted with formatting, falling back to plain text")
                return self.extract_text(file_path, max_pages)
                
        except Exception as e:
            self.logger.error(f"Error extracting text with formatting: {e}, falling back to plain text")
            return self.extract_text(file_path, max_pages)
    
    def extract_text_range(self, file_path: str, start_page: int, end_page: int) -> Optional[str]:
        """
        Extract text from a specific page range in PDF.
        
        Args:
            file_path: Path to PDF file
            start_page: Start page number (1-indexed)
            end_page: End page number (1-indexed, inclusive)
            
        Returns:
            Extracted text from the specified page range or None if failed
        """
        # Priority 1: Use PyMuPDF
        if MUPDF_AVAILABLE:
            try:
                doc = fitz.open(file_path)
                total_pages = len(doc)
                
                # Validate page range
                start_page = max(1, start_page)
                end_page = min(end_page, total_pages)
                
                if start_page > end_page:
                    self.logger.error(f"Invalid page range: {start_page} > {end_page}")
                    doc.close()
                    return None
                
                # Convert to 0-indexed for PyMuPDF
                start_idx = start_page - 1
                end_idx = end_page
                
                text_content = []
                for page_num in range(start_idx, end_idx):
                    page = doc[page_num]
                    text = page.get_text()
                    if text and text.strip():
                        text_content.append(f"--- Page {page_num + 1} ---\n{text}\n")
                
                doc.close()
                
                if text_content:
                    extracted_text = "\n".join(text_content)
                    char_count = len(extracted_text)
                    self.logger.info(
                        f"Successfully extracted {char_count} characters from pages {start_page}-{end_page} using PyMuPDF"
                    )
                    return extracted_text
                else:
                    self.logger.warning(f"No text extracted from pages {start_page}-{end_page}, trying PyPDF2...")
            except Exception as e:
                self.logger.warning(f"PyMuPDF extraction failed for page range: {e}, trying PyPDF2...")
        
        # Priority 2: Fallback to PyPDF2
        if PDF_LIBRARY_AVAILABLE:
            try:
                with open(file_path, 'rb') as file:
                    pdf_reader = PyPDF2.PdfReader(file)
                    total_pages = len(pdf_reader.pages)
                    
                    # Validate page range
                    start_page = max(1, start_page)
                    end_page = min(end_page, total_pages)
                    
                    if start_page > end_page:
                        self.logger.error(f"Invalid page range: {start_page} > {end_page}")
                        return None
                    
                    # Convert to 0-indexed for PyPDF2
                    start_idx = start_page - 1
                    end_idx = end_page
                    
                    text_content = []
                    for page_num in range(start_idx, end_idx):
                        page = pdf_reader.pages[page_num]
                        text = page.extract_text()
                        if text and text.strip():
                            text_content.append(f"--- Page {page_num + 1} ---\n{text}\n")
                    
                    if text_content:
                        extracted_text = "\n".join(text_content)
                        char_count = len(extracted_text)
                        self.logger.info(
                            f"Successfully extracted {char_count} characters from pages {start_page}-{end_page} using PyPDF2 (fallback)"
                        )
                        return extracted_text
            except Exception as e:
                self.logger.error(f"PyPDF2 extraction also failed for page range: {e}")
        
        self.logger.error(f"All text extraction methods failed for pages {start_page}-{end_page}")
        return None
    
    def extract_chapter_structure(self, file_path: str) -> Optional[List[Dict[str, Any]]]:
        """
        Extract chapter structure (subchapter and topics) from PDF without using LLM.
        Uses font size and formatting analysis to identify subchapters and topics.
        
        Rules:
        1. Items in "Chapter contents" on first page are NOT topics (even if bold/large)
        2. Only Bold with larger font size are topics (not italic with smaller font)
        3. Must not miss any subchapter or topic
        
        Args:
            file_path: Path to PDF file
            
        Returns:
            List of dictionaries with structure:
            [
                {
                    "Subchapter": "Bullous Pemphigoid",
                    "Topics": ["Epidemiology", "Pathogenesis", ...]
                },
                ...
            ]
        """
        if not MUPDF_AVAILABLE:
            self.logger.error("PyMuPDF required for structure extraction")
            return None
        
        try:
            import re
            from collections import defaultdict
            
            doc = fitz.open(file_path)
            
            # Collect all lines with font information (including italic detection)
            lines_data = []
            first_page_lines = []  # For detecting "Chapter contents"
            
            for page_num in range(len(doc)):
                page = doc[page_num]
                blocks = page.get_text("dict")
                
                for block in blocks.get("blocks", []):
                    if "lines" not in block:
                        continue
                    
                    for line in block["lines"]:
                        if not line.get("spans"):
                            continue
                        
                        line_text = ""
                        font_sizes = []
                        is_bold_list = []
                        is_italic_list = []
                        y_position = line.get("bbox", [0, 0, 0, 0])[1]
                        
                        for span in line["spans"]:
                            text = span.get("text", "").strip()
                            if text:
                                line_text += text + " "
                                font_sizes.append(span.get("size", 0))
                                flags = span.get("flags", 0)
                                is_bold_list.append(bool(flags & 16))  # Bit 4 = bold
                                is_italic_list.append(bool(flags & 2))  # Bit 1 = italic
                        
                        line_text = line_text.strip()
                        if not line_text or len(line_text) < 2:
                            continue
                        
                        avg_font_size = sum(font_sizes) / len(font_sizes) if font_sizes else 0
                        is_bold = any(is_bold_list)
                        is_italic = any(is_italic_list)
                        
                        line_info = {
                            "text": line_text,
                            "font_size": avg_font_size,
                            "is_bold": is_bold,
                            "is_italic": is_italic,
                            "y_position": y_position,
                            "page": page_num + 1
                        }
                        
                        lines_data.append(line_info)
                        
                        # Collect first page lines separately
                        if page_num == 0:
                            first_page_lines.append(line_info)
            
            doc.close()
            
            if not lines_data:
                self.logger.warning("No text lines found in PDF")
                return None
            
            # Extract Subchapters from "Chapter contents" section on first page
            subchapters_from_contents = []
            in_chapter_contents = False
            
            for i, line in enumerate(first_page_lines):
                text_lower = line["text"].lower()
                # Check if this line indicates start of "Chapter contents"
                if "chapter" in text_lower and "content" in text_lower:
                    in_chapter_contents = True
                    continue
                
                # If we're in chapter contents section, collect subchapters
                if in_chapter_contents:
                    clean_text = re.sub(r'^[\d\.\-\•\*\:\;]\s*', '', line["text"]).strip()
                    if clean_text and len(clean_text) > 3:
                        # Remove page numbers if present (e.g., "Bullous Pemphigoid 123")
                        clean_text = re.sub(r'\s+\d+\s*$', '', clean_text).strip()
                        if clean_text:
                            subchapters_from_contents.append(clean_text)
            
            self.logger.info(f"Found {len(subchapters_from_contents)} subchapters in Chapter contents: {subchapters_from_contents[:3]}...")
            
            if not subchapters_from_contents:
                self.logger.warning("No subchapters found in Chapter contents. Will try to detect from document structure.")
            
            # Analyze font hierarchy (excluding first page chapter contents)
            font_size_groups = defaultdict(list)
            for line in lines_data:
                # Round font to 0.5
                font_key = round(line["font_size"] * 2) / 2
                font_size_groups[font_key].append(line)
            
            # Find different font sizes
            sorted_font_sizes = sorted(font_size_groups.keys(), reverse=True)
            
            if not sorted_font_sizes:
                return None
            
            # Determine thresholds for subchapter and topic
            subchapter_font_threshold = sorted_font_sizes[0] if sorted_font_sizes else 14
            
            # Find topic font (usually second or third largest font, but must be Bold)
            topic_font_threshold = None
            if len(sorted_font_sizes) > 1:
                # Find the largest font size that has bold items
                for font_size in sorted_font_sizes[1:]:  # Skip the largest (subchapter)
                    # Check if this font size has bold items
                    has_bold = any(line["is_bold"] for line in font_size_groups[font_size])
                    if has_bold:
                        topic_font_threshold = font_size
                        break
                
                if topic_font_threshold is None:
                    # Fallback: use second font if no bold found
                    topic_font_threshold = sorted_font_sizes[1] if len(sorted_font_sizes) > 1 else subchapter_font_threshold * 0.8
            else:
                topic_font_threshold = subchapter_font_threshold * 0.85
            
            self.logger.info(f"Subchapter font threshold: {subchapter_font_threshold}, Topic font threshold: {topic_font_threshold}")
            
            # Extract structure
            structure = []
            current_subchapter = None
            current_topics = []
            seen_subchapters = set()
            seen_topics = set()
            
            # Create a mapping of subchapter names (normalized) to original names
            subchapter_map = {}
            for sub in subchapters_from_contents:
                normalized = sub.lower().strip()
                subchapter_map[normalized] = sub
            
            # Sort lines by page and position
            sorted_lines = sorted(lines_data, key=lambda x: (x["page"], x["y_position"]))
            
            # Skip first page (Chapter contents)
            for line in sorted_lines:
                text = line["text"]
                font_size = line["font_size"]
                is_bold = line["is_bold"]
                is_italic = line["is_italic"]
                page_num = line["page"]
                
                # Skip first page entirely (Chapter contents)
                if page_num == 1:
                    continue
                
                # Clean text (remove numbering and extra characters)
                clean_text = re.sub(r'^[\d\.\-\•\*\:\;]\s*', '', text).strip()
                if not clean_text:
                    continue
                
                # Remove page numbers if present at the end
                clean_text = re.sub(r'\s+\d+\s*$', '', clean_text).strip()
                if not clean_text:
                    continue
                
                # Check if this line matches a subchapter from Chapter contents
                normalized_text = clean_text.lower().strip()
                matched_subchapter = None
                
                # Try exact match first
                if normalized_text in subchapter_map:
                    matched_subchapter = subchapter_map[normalized_text]
                else:
                    # Try partial match (in case of slight variations)
                    for sub_norm, sub_orig in subchapter_map.items():
                        # Check if clean_text contains the subchapter or vice versa
                        if (sub_norm in normalized_text or normalized_text in sub_norm) and \
                           abs(len(sub_norm) - len(normalized_text)) <= 5:  # Allow small differences
                            matched_subchapter = sub_orig
                            break
                
                if matched_subchapter:
                    # This is a subchapter - save previous one
                    if current_subchapter:
                        structure.append({
                            "Subchapter": current_subchapter,
                            "Topics": current_topics.copy()
                        })
                        seen_subchapters.add(current_subchapter.lower())
                    
                    # Start new subchapter
                    current_subchapter = matched_subchapter
                    current_topics = []
                    continue
                
                # If we don't have a current subchapter yet, skip (wait for first subchapter)
                if not current_subchapter:
                    continue
                
                # Detect topic (Bold with larger font, NOT italic with smaller font)
                # Rule: Only Bold with larger font are topics (not italic)
                if is_italic:
                    # Italic items are NOT topics
                    continue
                
                # Must be Bold
                if not is_bold:
                    continue
                
                # Check font size (should be in topic range, smaller than subchapter)
                is_topic = False
                if (topic_font_threshold * 0.7 <= font_size <= topic_font_threshold * 1.4):
                    is_topic = True
                elif font_size >= topic_font_threshold * 0.6 and font_size < subchapter_font_threshold * 0.9:
                    is_topic = True
                
                if is_topic:
                    # Additional validation
                    if (len(clean_text) < 100 and  # Topics are usually short
                        clean_text[0].isupper() and
                        not clean_text.lower().startswith(('the ', 'a ', 'an ', 'and ', 'or '))):
                        
                        # Avoid duplicates
                        topic_key = clean_text.lower()
                        if topic_key not in seen_topics:
                            current_topics.append(clean_text)
                            seen_topics.add(topic_key)
            
            # Save last subchapter
            if current_subchapter:
                structure.append({
                    "Subchapter": current_subchapter,
                    "Topics": current_topics.copy()
                })
                seen_subchapters.add(current_subchapter.lower())
            
            # Verify all subchapters from Chapter contents were found
            found_subchapters = {s["Subchapter"].lower() for s in structure}
            missing_subchapters = []
            for sub in subchapters_from_contents:
                sub_lower = sub.lower()
                found = False
                for found_sub in found_subchapters:
                    if sub_lower in found_sub or found_sub in sub_lower:
                        found = True
                        break
                if not found:
                    missing_subchapters.append(sub)
            
            if missing_subchapters:
                self.logger.warning(f"Some subchapters from Chapter contents were not found in document: {missing_subchapters}")
            
            # Filter empty subchapters
            structure = [s for s in structure if s.get("Subchapter") and s.get("Topics")]
            
            # Final verification: check if we might have missed anything
            # Look for any remaining large bold text that might be subchapters
            for line in sorted_lines:
                if line["page"] == 1:  # Skip first page
                    continue
                
                text = line["text"]
                font_size = line["font_size"]
                is_bold = line["is_bold"]
                clean_text = re.sub(r'^[\d\.\-\•\*\:\;]\s*', '', text).strip()
                
                if not clean_text:
                    continue
                
                # Check for missed subchapters
                if (font_size >= subchapter_font_threshold * 0.8 and 
                    clean_text.lower() not in seen_subchapters and
                    len(clean_text) < 100 and
                    clean_text[0].isupper()):
                    # This might be a missed subchapter
                    self.logger.warning(f"Potential missed subchapter: {clean_text} (font: {font_size})")
            
            if structure:
                total_topics = sum(len(s.get("Topics", [])) for s in structure)
                self.logger.info(f"Extracted {len(structure)} subchapters with {total_topics} total topics")
                return structure
            else:
                self.logger.warning("No structure extracted from PDF")
                return None
                
        except Exception as e:
            self.logger.error(f"Error extracting chapter structure: {e}", exc_info=True)
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



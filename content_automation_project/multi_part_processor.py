"""
Multi-Part Response Processor for Gemini API

This module implements a robust, production-safe multi-part response pipeline
that handles very large model outputs by splitting them into manageable chunks.

Key Features:
- Multi-request chunked JSON assembly (NOT streaming)
- Rate limit handling with proper backoff
- Validation and duplicate detection
- Safe token limits (5000-6000 per part)
- Saves only final JSON and CSV outputs (no intermediate part files)
"""

import os
import json
import time
import logging
import hashlib
import csv
from typing import Optional, Dict, List, Any, Callable, Tuple
from pathlib import Path
from datetime import datetime

try:
    import google.generativeai as genai
    GENERATIVEAI_AVAILABLE = True
except ImportError:
    GENERATIVEAI_AVAILABLE = False
    logging.warning("google.generativeai library not available.")


class MultiPartProcessor:
    """
    Processes large PDF outputs in multiple parts, assembling them into a final JSON file.
    
    This is NOT streaming - it makes multiple API calls with chunk_index tracking.
    Only final JSON and CSV outputs are saved (no intermediate part files).
    """
    
    # Safe limits to prevent rate limiting
    MAX_TOKENS_PER_PART = 40000  # Increased to allow more pages per part
    MIN_DELAY_BETWEEN_PARTS = 8  # Minimum seconds between parts
    MAX_DELAY_BETWEEN_PARTS = 15  # Maximum seconds between parts
    MAX_PARTS_LIMIT = 5  # Maximum number of parts to process
    
    # Rate limit backoff (DO NOT rotate keys immediately on 429)
    RATE_LIMIT_BACKOFF = [120, 240, 480]  # 120s → 240s → 480s
    MAX_RATE_LIMIT_RETRIES = 3
    
    def __init__(self, api_client, output_dir: Optional[str] = None):
        """
        Initialize Multi-Part Processor
        
        Args:
            api_client: GeminiAPIClient instance
            output_dir: Directory to save part files and final output (default: current directory)
        """
        self.api_client = api_client
        self.logger = logging.getLogger(__name__)
        self.output_dir = Path(output_dir) if output_dir else Path.cwd()
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Track state
        self.base_prompt_hash = None
        self.parts_dir = None
        
    def _get_parts_directory(self, pdf_path: str, base_prompt: str) -> Path:
        """
        Get or create directory for storing part files.
        Uses hash of PDF path + prompt to ensure consistency.
        
        Args:
            pdf_path: Path to PDF file
            base_prompt: Base prompt text
            
        Returns:
            Path to parts directory
        """
        # Create unique identifier from PDF path and prompt
        identifier = f"{os.path.basename(pdf_path)}_{hashlib.md5(base_prompt.encode()).hexdigest()[:8]}"
        parts_dir = self.output_dir / f"parts_{identifier}"
        parts_dir.mkdir(parents=True, exist_ok=True)
        return parts_dir
    
    def _build_part_prompt(self, base_prompt: str, chunk_index: int, is_first: bool) -> str:
        """
        Build prompt for a specific chunk. Only adds chunk_index info, no additional instructions.
        The base_prompt already contains all necessary format instructions.
        
        Args:
            base_prompt: Base OCR/structuring prompt (contains all format instructions)
            chunk_index: Current chunk number (1-based) - used internally, but prompt uses chunk_index
            is_first: Whether this is the first chunk
            
        Returns:
            Prompt with only chunk_index information added
        """
        # Only add minimal chunk_index information - base_prompt has all instructions
        # User's prompt uses chunk_index instead of chunk_index
        # Add guidance to avoid duplicates and limit to 5 chunks
        if is_first:
            # First chunk: just add chunk_index info with guidance
            chunk_info = f"\n\nThis is CHUNK {chunk_index} of the output (maximum 5 chunks total). DO NOT include any duplicate content within this chunk."
            return f"{base_prompt}{chunk_info}"
        else:
            # Subsequent chunks: add continuation info with guidance
            continuation_info = f"\n\nThis is CHUNK {chunk_index} of the output (maximum 5 chunks total). Continue from where Chunk {chunk_index - 1} ended. DO NOT repeat any content from previous chunks. DO NOT include any duplicate content within this chunk."
            return f"{base_prompt}{continuation_info}"
    
    def _save_part(self, part_data: Dict[str, Any], chunk_index: int) -> Path:
        """
        Persist a part to disk (currently disabled - only final output is saved).
        
        Args:
            part_data: Part data dictionary
            chunk_index: Chunk number
            
        Returns:
            Path to saved part file (not used anymore)
        """
        part_file = self.parts_dir / f"part_{chunk_index}.json"
        try:
            with open(part_file, 'w', encoding='utf-8') as f:
                json.dump(part_data, f, ensure_ascii=False, indent=2)
            self.logger.info(f"✓ Saved part {chunk_index} to {part_file}")
            return part_file
        except Exception as e:
            self.logger.error(f"Failed to save part {chunk_index}: {str(e)}")
            raise
    
    def _load_part(self, chunk_index: int) -> Optional[Dict[str, Any]]:
        """
        Load a previously saved part from disk.
        
        Args:
            chunk_index: Chunk number
            
        Returns:
            Part data dictionary or None if not found
        """
        part_file = self.parts_dir / f"part_{chunk_index}.json"
        if part_file.exists():
            try:
                with open(part_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                self.logger.warning(f"Failed to load part {chunk_index}: {str(e)}")
        return None
    
    def _find_last_completed_part(self) -> int:
        """
        Find the last successfully completed part by scanning disk.
        
        Returns:
            Last completed part index (0 if none found)
        """
        if not self.parts_dir or not self.parts_dir.exists():
            return 0
        
        max_part = 0
        for part_file in sorted(self.parts_dir.glob("part_*.json")):
            try:
                part_num = int(part_file.stem.split('_')[1])
                # Verify part is valid
                part_data = self._load_part(part_num)
                if part_data and self._validate_part(part_data, part_num):
                    max_part = max(max_part, part_num)
            except (ValueError, Exception) as e:
                self.logger.warning(f"Invalid part file {part_file}: {str(e)}")
        
        return max_part
    
    def _validate_part(self, part_data: Dict[str, Any], expected_index: int) -> bool:
        """
        Validate a part's structure and content.
        
        Args:
            part_data: Part data dictionary
            expected_index: Expected chunk_index value
            
        Returns:
            True if valid, False otherwise
        """
        # Check required fields
        if not isinstance(part_data, dict):
            self.logger.error(f"Part {expected_index}: Not a dictionary")
            return False
        
        # Check for chunk_index (matching user's prompt format)
        if 'chunk_index' not in part_data:
            self.logger.error(f"Part {expected_index}: Missing 'chunk_index' field")
            return False
        
        if part_data['chunk_index'] != expected_index:
            self.logger.error(f"Part {expected_index}: chunk_index mismatch (got {part_data['chunk_index']})")
            return False
        
        if 'is_last' not in part_data:
            self.logger.error(f"Part {expected_index}: Missing 'is_last' field")
            return False
        
        if 'rows' not in part_data:
            self.logger.error(f"Part {expected_index}: Missing 'rows' field")
            return False
        
        # Validate rows
        if not isinstance(part_data['rows'], list):
            self.logger.error(f"Part {expected_index}: 'rows' is not a list")
            return False
        
        # Rows must not be empty unless is_last=true
        if len(part_data['rows']) == 0 and not part_data['is_last']:
            self.logger.error(f"Part {expected_index}: Empty rows but is_last=false")
            return False
        
        # Validate row structure
        for i, row in enumerate(part_data['rows']):
            if not isinstance(row, dict):
                self.logger.error(f"Part {expected_index}, row {i}: Not a dictionary")
                return False
            
            required_fields = ['Type', 'Extraction', 'Number', 'Part']
            for field in required_fields:
                if field not in row:
                    self.logger.error(f"Part {expected_index}, row {i}: Missing '{field}' field")
                    return False
            
            # Check Extraction is single-line (no newlines)
            if isinstance(row['Extraction'], str) and '\n' in row['Extraction']:
                self.logger.warning(f"Part {expected_index}, row {i}: Extraction contains newlines (should use \\n)")
        
        return True
    
    def _parse_part_response(self, response_text: str, expected_index: int) -> Optional[Dict[str, Any]]:
        """
        Safely parse JSON from model response.
        
        Args:
            response_text: Raw response text from model
            expected_index: Expected chunk_index
            
        Returns:
            Parsed part data or None if parsing fails
        """
        if not response_text:
            self.logger.error(f"Part {expected_index}: Empty response")
            return None
        
        # Try to extract JSON (handle code blocks, markdown, etc.)
        cleaned = response_text.strip()
        
        # Remove markdown code blocks if present
        if cleaned.startswith('```'):
            # Find closing ```
            end_idx = cleaned.find('```', 3)
            if end_idx > 0:
                cleaned = cleaned[3:end_idx].strip()
                # Remove language identifier if present
                if cleaned.startswith('json'):
                    cleaned = cleaned[4:].strip()
        
        # Try parsing as JSON
        try:
            part_data = json.loads(cleaned)
            
            # Log what we got
            self.logger.debug(f"Part {expected_index}: Parsed JSON successfully")
            self.logger.debug(f"Part {expected_index}: Keys in response: {list(part_data.keys())}")
            if 'chunk_index' in part_data:
                self.logger.debug(f"Part {expected_index}: chunk_index = {part_data['chunk_index']}")
            if 'is_last' in part_data:
                self.logger.debug(f"Part {expected_index}: is_last = {part_data['is_last']}")
            if 'rows' in part_data:
                self.logger.debug(f"Part {expected_index}: rows count = {len(part_data['rows'])}")
            
            if self._validate_part(part_data, expected_index):
                return part_data
            else:
                self.logger.error(f"Part {expected_index}: Validation failed")
                self.logger.error(f"Part {expected_index}: Part data structure: {json.dumps(part_data, indent=2)[:1000]}")
                return None
        except json.JSONDecodeError as e:
            self.logger.error(f"Part {expected_index}: JSON parse error: {str(e)}")
            self.logger.error(f"Part {expected_index}: Response text (first 1000 chars): {cleaned[:1000]}")
            self.logger.error(f"Part {expected_index}: Response text (last 500 chars): {cleaned[-500:] if len(cleaned) > 500 else cleaned}")
            return None
    
    def _call_model_with_retry(self, pdf_path: str, prompt: str, chunk_index: int,
                               model_name: str, temperature: float,
                               progress_callback: Optional[Callable[[str], None]] = None) -> Optional[str]:
        """
        Call model with proper retry logic for rate limits.
        DO NOT rotate keys immediately on 429 - use backoff instead.
        
        Args:
            pdf_path: Path to PDF file
            prompt: Part-specific prompt
            chunk_index: Current chunk number
            model_name: Model name
            temperature: Temperature setting
            progress_callback: Optional progress callback
            
        Returns:
            Response text or None if failed after retries
        """
        for retry_attempt in range(self.MAX_RATE_LIMIT_RETRIES):
            try:
                # Call API (streaming MUST be disabled for multi-part processing)
                response = self.api_client.process_pdf_with_prompt(
                    pdf_path=pdf_path,
                    prompt=prompt,
                    model_name=model_name,
                    temperature=temperature,
                    max_tokens=self.MAX_TOKENS_PER_PART,
                    return_json=False,
                    force_no_streaming=True  # CRITICAL: Disable streaming for multi-part
                )
                
                if response:
                    return response
                else:
                    self.logger.error(f"Part {chunk_index}: Empty response from API")
                    return None
                    
            except Exception as e:
                error_str = str(e)
                error_type = type(e).__name__
                
                # Log full error details for debugging
                self.logger.error(f"Part {chunk_index}: Exception in API call (attempt {retry_attempt + 1}/{self.MAX_RATE_LIMIT_RETRIES})")
                self.logger.error(f"Part {chunk_index}: Error type: {error_type}")
                self.logger.error(f"Part {chunk_index}: Error message: {error_str}")
                
                # Check if it's a rate limit error (429)
                if '429' in error_str or 'rate limit' in error_str.lower() or 'quota' in error_str.lower():
                    # Extract Retry-After if available
                    retry_after = None
                    if hasattr(e, 'response') and hasattr(e.response, 'headers'):
                        headers = e.response.headers
                        retry_after = headers.get('Retry-After') or headers.get('retry-after')
                    
                    if retry_after:
                        try:
                            delay = int(retry_after)
                            self.logger.warning(f"Part {chunk_index}: Rate limit (429), Retry-After={delay}s")
                        except:
                            delay = self.RATE_LIMIT_BACKOFF[retry_attempt] if retry_attempt < len(self.RATE_LIMIT_BACKOFF) else self.RATE_LIMIT_BACKOFF[-1]
                    else:
                        # Use exponential backoff: 120s → 240s → 480s
                        delay = self.RATE_LIMIT_BACKOFF[retry_attempt] if retry_attempt < len(self.RATE_LIMIT_BACKOFF) else self.RATE_LIMIT_BACKOFF[-1]
                        self.logger.warning(f"Part {chunk_index}: Rate limit (429), using backoff delay={delay}s")
                    
                    if retry_attempt < self.MAX_RATE_LIMIT_RETRIES - 1:
                        if progress_callback:
                            progress_callback(f"Rate limit hit, waiting {delay}s before retry {retry_attempt + 1}/{self.MAX_RATE_LIMIT_RETRIES}...")
                        self.logger.info(f"Part {chunk_index}: Waiting {delay}s before retry...")
                        time.sleep(delay)
                        continue
                    else:
                        # All retries exhausted - abort and persist partial progress
                        self.logger.error(f"Part {chunk_index}: Rate limit persisted after {self.MAX_RATE_LIMIT_RETRIES} retries. Aborting.")
                        if progress_callback:
                            progress_callback(f"ERROR: Rate limit persisted. Partial progress saved. Please retry later.")
                        raise Exception(f"Rate limit persisted after {self.MAX_RATE_LIMIT_RETRIES} retries. Partial progress saved.")
                else:
                    # Non-rate-limit error - log and decide whether to retry or fail
                    # Some errors might be transient and worth retrying
                    if retry_attempt < self.MAX_RATE_LIMIT_RETRIES - 1:
                        # Retry for transient errors (network, timeout, etc.)
                        if any(keyword in error_str.lower() for keyword in ['timeout', 'connection', 'network', 'temporary', '503', '502', '500']):
                            delay = 10  # Short delay for transient errors
                            self.logger.warning(f"Part {chunk_index}: Transient error detected, retrying in {delay}s...")
                            if progress_callback:
                                progress_callback(f"Transient error, retrying in {delay}s...")
                            time.sleep(delay)
                            continue
                        else:
                            # Non-transient error, but we'll try once more
                            self.logger.error(f"Part {chunk_index}: Non-rate-limit error: {error_str}")
                            if retry_attempt == 0:  # Only retry once for non-transient errors
                                delay = 5
                                self.logger.warning(f"Part {chunk_index}: Retrying once more after {delay}s...")
                                time.sleep(delay)
                                continue
                            else:
                                # Give up after one retry
                                self.logger.error(f"Part {chunk_index}: Error persisted after retry. Giving up.")
                                raise
                    else:
                        # All retries exhausted
                        self.logger.error(f"Part {chunk_index}: Error persisted after {self.MAX_RATE_LIMIT_RETRIES} retries: {error_str}")
                        raise
        
        # If we get here, all retries were exhausted but no exception was raised (shouldn't happen)
        self.logger.error(f"Part {chunk_index}: All retries exhausted but no exception raised. This should not happen.")
        return None
    
    def _deduplicate_rows(self, all_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Remove duplicate rows based on page number, type, and content.
        More aggressive deduplication to handle overlapping parts.
        
        Args:
            all_rows: List of all row dictionaries
            
        Returns:
            Deduplicated list of rows
        """
        seen_hashes = set()
        seen_page_type_content = {}  # (page_number, type, content_start) -> row_index
        unique_rows = []
        duplicates_count = 0
        
        for row in all_rows:
            # Method 1: Full content hash (excluding Part field)
            row_copy = {k: v for k, v in row.items() if k != 'Part'}
            row_hash = hashlib.md5(json.dumps(row_copy, sort_keys=True).encode()).hexdigest()
            
            # Method 2: Page number + Type + Content start (for detecting overlapping content)
            page_num = row.get('Number', 0)
            row_type = row.get('Type', '')
            extraction = str(row.get('Extraction', ''))
            # Use first 200 chars of extraction as signature
            content_start = extraction[:200] if len(extraction) > 200 else extraction
            page_type_content_key = (page_num, row_type, content_start)
            
            # Check both methods
            is_duplicate = False
            
            if row_hash in seen_hashes:
                is_duplicate = True
                duplicates_count += 1
                self.logger.debug(f"Duplicate detected (hash): Page {page_num}, Type: {row_type}")
            elif page_type_content_key in seen_page_type_content:
                # Check if content is significantly similar
                existing_row_idx = seen_page_type_content[page_type_content_key]
                existing_row = unique_rows[existing_row_idx]
                existing_extraction = str(existing_row.get('Extraction', ''))
                
                # If extractions are very similar (more than 80% overlap), consider duplicate
                if len(extraction) > 0 and len(existing_extraction) > 0:
                    # Simple similarity check: if one is substring of another or vice versa
                    if extraction in existing_extraction or existing_extraction in extraction:
                        is_duplicate = True
                        duplicates_count += 1
                        self.logger.debug(f"Duplicate detected (content overlap): Page {page_num}, Type: {row_type}")
                    # Or if they're very similar in length and start the same way
                    elif abs(len(extraction) - len(existing_extraction)) < max(len(extraction), len(existing_extraction)) * 0.2:
                        # Check first 500 chars similarity
                        min_len = min(len(extraction), len(existing_extraction), 500)
                        if min_len > 0:
                            similarity = sum(1 for i in range(min_len) if extraction[i] == existing_extraction[i]) / min_len
                            if similarity > 0.8:
                                is_duplicate = True
                                duplicates_count += 1
                                self.logger.debug(f"Duplicate detected (high similarity {similarity:.2%}): Page {page_num}, Type: {row_type}")
            
            if not is_duplicate:
                seen_hashes.add(row_hash)
                seen_page_type_content[page_type_content_key] = len(unique_rows)
                unique_rows.append(row)
        
        if duplicates_count > 0:
            self.logger.warning(f"Removed {duplicates_count} duplicate rows using enhanced deduplication")
        
        return unique_rows
    
    def process_multi_part(self,
                          pdf_path: str,
                          base_prompt: str,
                          model_name: str = "gemini-2.5-pro",
                          temperature: float = 0.7,
                          resume: bool = True,
                          progress_callback: Optional[Callable[[str], None]] = None) -> Optional[str]:
        """
        Main entry point: Process PDF in multiple parts and assemble final JSON.
        
        Args:
            pdf_path: Path to PDF file
            base_prompt: Base OCR/structuring prompt (user provides this)
            model_name: Model name to use
            temperature: Temperature setting
            resume: Whether to resume from last completed part
            progress_callback: Optional callback for progress updates
            
        Returns:
            Path to final output JSON file or None if failed
        """
        if not GENERATIVEAI_AVAILABLE:
            self.logger.error("google.generativeai library not available")
            return None
        
        if not os.path.exists(pdf_path):
            self.logger.error(f"PDF file not found: {pdf_path}")
            return None
        
        # Initialize parts directory
        self.parts_dir = self._get_parts_directory(pdf_path, base_prompt)
        self.logger.info(f"Parts directory: {self.parts_dir}")
        
        # Set output directory to current directory (always use current directory)
        self.output_dir = Path.cwd()
        self.logger.info(f"Final output will be saved to current directory: {self.output_dir}")
        
        # Determine starting part
        # Resume is disabled since we don't save part files anymore
        start_part = 1
        all_rows = []
        
        # Main loop: process parts until is_last=true or max parts limit
        chunk_index = start_part
        
        while chunk_index <= self.MAX_PARTS_LIMIT:  # Continue until is_last=true or max parts limit
            self.logger.info(f"=== Processing Part {chunk_index} ===")
            if progress_callback:
                progress_callback(f"Processing part {chunk_index}...")
            
            # Build part-specific prompt
            is_first = (chunk_index == 1)
            part_prompt = self._build_part_prompt(base_prompt, chunk_index, is_first)
            
            # Call model with retry logic
            response_text = self._call_model_with_retry(
                pdf_path, part_prompt, chunk_index,
                model_name, temperature, progress_callback
            )
            
            if not response_text:
                self.logger.error(f"Part {chunk_index}: Failed to get response from API after all retries")
                self.logger.error(f"Part {chunk_index}: This could be due to:")
                self.logger.error(f"  - Rate limiting (429) that persisted after retries")
                self.logger.error(f"  - API key issues (403, quota exhausted)")
                self.logger.error(f"  - Network/connection issues")
                self.logger.error(f"  - Model errors or timeouts")
                self.logger.error(f"Part {chunk_index}: Stopping processing.")
                if progress_callback:
                    progress_callback(f"ERROR: Part {chunk_index} failed. Processing stopped.")
                break
            
            # Parse response
            part_data = self._parse_part_response(response_text, chunk_index)
            
            if not part_data:
                self.logger.error(f"Part {chunk_index}: Failed to parse response")
                self.logger.error(f"Part {chunk_index}: Response text (first 1000 chars): {response_text[:1000] if response_text else 'None'}")
                self.logger.error(f"Part {chunk_index}: Retrying same part...")
                # Retry same chunk_index (malformed JSON)
                time.sleep(5)
                continue
            
            # Validate part
            if not self._validate_part(part_data, chunk_index):
                self.logger.error(f"Part {chunk_index}: Validation failed")
                self.logger.error(f"Part {chunk_index}: Part data: {json.dumps(part_data, indent=2)[:500]}")
                self.logger.error(f"Part {chunk_index}: Retrying same part...")
                time.sleep(5)
                continue
            
            # Log part details
            is_last = part_data.get('is_last', False)
            rows_count = len(part_data.get('rows', []))
            self.logger.info(f"Part {chunk_index}: Parsed successfully - is_last={is_last}, rows={rows_count}")
            
            # Remove duplicates within this part before adding to all_rows
            if part_data.get('rows'):
                part_rows = part_data['rows']
                # Deduplicate within this part
                seen_in_part = set()
                unique_part_rows = []
                duplicates_in_part = 0
                
                for row in part_rows:
                    # Create hash of row content (excluding Part field)
                    row_copy = {k: v for k, v in row.items() if k != 'Part'}
                    row_hash = hashlib.md5(json.dumps(row_copy, sort_keys=True).encode()).hexdigest()
                    
                    if row_hash not in seen_in_part:
                        seen_in_part.add(row_hash)
                        unique_part_rows.append(row)
                    else:
                        duplicates_in_part += 1
                
                if duplicates_in_part > 0:
                    self.logger.warning(f"Part {chunk_index}: Removed {duplicates_in_part} duplicate rows within this part")
                    part_data['rows'] = unique_part_rows
                    rows_count = len(unique_part_rows)
            
            # CRITICAL: Enforce maximum parts rule
            if chunk_index >= self.MAX_PARTS_LIMIT and not is_last:
                self.logger.warning(f"Part {chunk_index}: Reached maximum part limit ({self.MAX_PARTS_LIMIT}). Forcing is_last=true.")
                is_last = True
                part_data['is_last'] = True
                if progress_callback:
                    progress_callback(f"Part {chunk_index}: Reached maximum part limit ({self.MAX_PARTS_LIMIT}). Marking as final part...")
            
            # CRITICAL: Check if model incorrectly set is_last=true on first part
            # Many models incorrectly mark the first part as last even when more content exists
            if is_last and chunk_index == 1:
                # Always force continuation on first part unless explicitly told otherwise
                # This prevents premature stopping when model incorrectly assumes completion
                self.logger.warning(f"Part {chunk_index}: Model set is_last=true on FIRST part - this is likely incorrect")
                self.logger.warning(f"Part {chunk_index}: Forcing continuation to part 2 (model may have more content)")
                is_last = False
                part_data['is_last'] = False  # Override model's decision
                if progress_callback:
                    progress_callback(f"Part {chunk_index}: Model marked as last, but forcing continuation to check for more content...")
            
            # Also check if is_last=true but we have very few rows (likely incomplete)
            # But only if we haven't reached part 4 yet
            if is_last and rows_count < 20 and chunk_index < 4:
                # If we get is_last=true with very few rows in early parts, it's suspicious
                self.logger.warning(f"Part {chunk_index}: is_last=true but only {rows_count} rows - this seems incomplete")
                self.logger.warning(f"Part {chunk_index}: Forcing continuation to verify...")
                is_last = False
                part_data['is_last'] = False
                if progress_callback:
                    progress_callback(f"Part {chunk_index}: Suspiciously few rows ({rows_count}) for last part - continuing to verify...")
            
            # Don't save part files - only save final JSON output
            # Part files are not needed anymore
            
            # Append rows
            if part_data.get('rows'):
                all_rows.extend(part_data['rows'])
                self.logger.info(f"Part {chunk_index}: Added {len(part_data['rows'])} rows (total: {len(all_rows)})")
            else:
                self.logger.warning(f"Part {chunk_index}: No rows in response (is_last={is_last})")
            
            # Check if this is the last part
            if is_last:
                self.logger.info(f"Part {chunk_index}: is_last=true, processing complete!")
                if progress_callback:
                    progress_callback(f"Part {chunk_index}: Last part received, assembling final output...")
                break
            else:
                self.logger.info(f"Part {chunk_index}: is_last=false, continuing to next part...")
                if progress_callback:
                    progress_callback(f"Part {chunk_index}: Completed ({rows_count} rows), continuing to part {chunk_index + 1}...")
            
            # Sleep between parts (8-15 seconds)
            # Only sleep if not the last part
            if not is_last:
                import random
                delay = random.uniform(self.MIN_DELAY_BETWEEN_PARTS, self.MAX_DELAY_BETWEEN_PARTS)
                self.logger.info(f"Waiting {delay:.1f}s before next part...")
                time.sleep(delay)
            
            chunk_index += 1
        
        # Check why loop ended
        if chunk_index > self.MAX_PARTS_LIMIT:
            self.logger.warning(f"Reached maximum parts limit ({self.MAX_PARTS_LIMIT}). Processing stopped.")
            if progress_callback:
                progress_callback(f"WARNING: Reached maximum parts limit ({self.MAX_PARTS_LIMIT}). Processing stopped.")
        
        # Calculate parts counts
        total_parts_in_final = 0  # No resume capability anymore
        
        if chunk_index == start_part:
            # No new parts were processed in this run
            if len(all_rows) > 0:
                self.logger.warning(f"No new parts processed (started at {start_part}), but {len(all_rows)} rows exist")
                self.logger.warning("This should not happen - no parts processed but rows exist")
                total_parts_processed = 0  # No new parts in this run
            else:
                self.logger.error(f"ERROR: No parts were processed! Started at {start_part}, ended at {chunk_index}")
                if progress_callback:
                    progress_callback(f"ERROR: No parts were successfully processed. Check logs for details.")
                return None
        else:
            total_parts_processed = chunk_index - start_part
        
        # Calculate total parts count
        total_parts_count = total_parts_processed
        
        # Log summary
        self.logger.info(f"=== Processing Summary ===")
        self.logger.info(f"Parts processed: {total_parts_processed}")
        self.logger.info(f"Total parts in final output: {total_parts_count}")
        self.logger.info(f"Total rows collected: {len(all_rows)}")
        
        if len(all_rows) == 0:
            self.logger.error("ERROR: No rows were collected from any part!")
            if progress_callback:
                progress_callback(f"ERROR: No rows collected. Check if model is returning correct format.")
            return None
        
        # Deduplicate rows
        self.logger.info(f"Deduplicating {len(all_rows)} rows...")
        unique_rows = self._deduplicate_rows(all_rows)
        self.logger.info(f"Final row count after deduplication: {len(unique_rows)}")
        
        # Sort rows by Number (page number) as float for proper decimal sorting
        def sort_key(row):
            number = row.get('Number', 0)
            try:
                # Convert to float for proper decimal sorting (e.g., 1.5, 2.3, etc.)
                number_num = float(number) if number else 0.0
            except (ValueError, TypeError):
                number_num = 0.0
            return number_num
        
        self.logger.info(f"Sorting {len(unique_rows)} rows by Number (as float)...")
        unique_rows = sorted(unique_rows, key=sort_key)
        self.logger.info(f"Rows sorted successfully")
        
        # Assemble final JSON
        end_part = chunk_index - 1 if total_parts_processed > 0 else start_part - 1
        
        final_output = {
            "metadata": {
                "total_parts": total_parts_count,
                "total_parts_processed": total_parts_processed,
                "start_part": start_part,
                "end_part": end_part,
                "total_rows": len(unique_rows),
                "processed_at": datetime.now().isoformat(),
                "pdf_path": os.path.basename(pdf_path)
            },
            "rows": unique_rows
        }
        
        # Save final output
        # Use a more descriptive filename based on PDF name
        pdf_name = os.path.splitext(os.path.basename(pdf_path))[0]
        # Sanitize filename (remove invalid characters)
        safe_pdf_name = "".join(c for c in pdf_name if c.isalnum() or c in (' ', '-', '_')).rstrip()
        final_filename = f"{safe_pdf_name}_final_output.json"
        final_file = self.output_dir / final_filename
        
        try:
            # Save JSON file
            with open(final_file, 'w', encoding='utf-8') as f:
                json.dump(final_output, f, ensure_ascii=False, indent=2)
            self.logger.info(f"✓ Final JSON output saved to {final_file}")
            self.logger.info(f"  - File path: {final_file.absolute()}")
            self.logger.info(f"  - Total parts: {total_parts_count}")
            self.logger.info(f"  - Total rows: {len(unique_rows)}")
            self.logger.info(f"  - File size: {final_file.stat().st_size / 1024:.2f} KB")
            
            # Save CSV file (sorted by Number as float)
            csv_filename = f"{safe_pdf_name}_final_output.csv"
            csv_file = self.output_dir / csv_filename
            
            # Prepare CSV data - rows are already sorted by Number (as float)
            csv_rows = []
            for row in unique_rows:
                csv_row = [
                    row.get('Type', ''),
                    row.get('Extraction', ''),
                    row.get('Number', ''),
                    row.get('Part', '')
                ]
                csv_rows.append(csv_row)
            
            # Write CSV file with UTF-8 BOM for Excel compatibility
            with open(csv_file, 'w', encoding='utf-8-sig', newline='') as f:
                writer = csv.writer(f, delimiter=';')
                # Write header
                writer.writerow(['Type', 'Extraction', 'Number', 'Part'])
                # Write data rows (already sorted by Number as float)
                writer.writerows(csv_rows)
            
            self.logger.info(f"✓ Final CSV output saved to {csv_file}")
            self.logger.info(f"  - File path: {csv_file.absolute()}")
            self.logger.info(f"  - Total rows: {len(unique_rows)}")
            self.logger.info(f"  - File size: {csv_file.stat().st_size / 1024:.2f} KB")
            
            if progress_callback:
                progress_callback(f"✓ Complete! Final outputs saved:")
                progress_callback(f"  JSON: {final_file}")
                progress_callback(f"  CSV: {csv_file}")
                progress_callback(f"  Total: {len(unique_rows)} rows from {total_parts_count} parts")
            return str(final_file)
        except Exception as e:
            self.logger.error(f"Failed to save final output: {str(e)}", exc_info=True)
            return None

"""
Multi-Part Response Processor for Gemini API

This module implements a robust, production-safe multi-part response pipeline
that handles very large model outputs by splitting them into manageable chunks.

Key Features:
- Multi-request chunked JSON assembly (NOT streaming)
- Rate limit handling with proper backoff
- Validation and duplicate detection
- Safe token limits (5000-6000 per part)
- Saves only final JSON and CSV outputs (no intermediate part files)
"""

import os
import json
import time
import logging
import hashlib
import csv
from typing import Optional, Dict, List, Any, Callable, Tuple
from pathlib import Path
from datetime import datetime

try:
    import google.generativeai as genai
    GENERATIVEAI_AVAILABLE = True
except ImportError:
    GENERATIVEAI_AVAILABLE = False
    logging.warning("google.generativeai library not available.")


class MultiPartProcessor:
    """
    Processes large PDF outputs in multiple parts, assembling them into a final JSON file.
    
    This is NOT streaming - it makes multiple API calls with chunk_index tracking.
    Only final JSON and CSV outputs are saved (no intermediate part files).
    """
    
    # Safe limits to prevent rate limiting
    MAX_TOKENS_PER_PART = 40000  # Increased to allow more pages per part
    MIN_DELAY_BETWEEN_PARTS = 8  # Minimum seconds between parts
    MAX_DELAY_BETWEEN_PARTS = 15  # Maximum seconds between parts
    MAX_PARTS_LIMIT = 5  # Maximum number of parts to process
    
    # Rate limit backoff (DO NOT rotate keys immediately on 429)
    RATE_LIMIT_BACKOFF = [120, 240, 480]  # 120s → 240s → 480s
    MAX_RATE_LIMIT_RETRIES = 3
    
    def __init__(self, api_client, output_dir: Optional[str] = None):
        """
        Initialize Multi-Part Processor
        
        Args:
            api_client: GeminiAPIClient instance
            output_dir: Directory to save part files and final output (default: current directory)
        """
        self.api_client = api_client
        self.logger = logging.getLogger(__name__)
        self.output_dir = Path(output_dir) if output_dir else Path.cwd()
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Track state
        self.base_prompt_hash = None
        self.parts_dir = None
        
    def _get_parts_directory(self, pdf_path: str, base_prompt: str) -> Path:
        """
        Get or create directory for storing part files.
        Uses hash of PDF path + prompt to ensure consistency.
        
        Args:
            pdf_path: Path to PDF file
            base_prompt: Base prompt text
            
        Returns:
            Path to parts directory
        """
        # Create unique identifier from PDF path and prompt
        identifier = f"{os.path.basename(pdf_path)}_{hashlib.md5(base_prompt.encode()).hexdigest()[:8]}"
        parts_dir = self.output_dir / f"parts_{identifier}"
        parts_dir.mkdir(parents=True, exist_ok=True)
        return parts_dir
    
    def _build_part_prompt(self, base_prompt: str, chunk_index: int, is_first: bool) -> str:
        """
        Build prompt for a specific chunk. Only adds chunk_index info, no additional instructions.
        The base_prompt already contains all necessary format instructions.
        
        Args:
            base_prompt: Base OCR/structuring prompt (contains all format instructions)
            chunk_index: Current chunk number (1-based) - used internally, but prompt uses chunk_index
            is_first: Whether this is the first chunk
            
        Returns:
            Prompt with only chunk_index information added
        """
        # Only add minimal chunk_index information - base_prompt has all instructions
        # User's prompt uses chunk_index instead of chunk_index
        # Add guidance to avoid duplicates and limit to 5 chunks
        if is_first:
            # First chunk: just add chunk_index info with guidance
            chunk_info = f"\n\nThis is CHUNK {chunk_index} of the output (maximum 5 chunks total). DO NOT include any duplicate content within this chunk."
            return f"{base_prompt}{chunk_info}"
        else:
            # Subsequent chunks: add continuation info with guidance
            continuation_info = f"\n\nThis is CHUNK {chunk_index} of the output (maximum 5 chunks total). Continue from where Chunk {chunk_index - 1} ended. DO NOT repeat any content from previous chunks. DO NOT include any duplicate content within this chunk."
            return f"{base_prompt}{continuation_info}"
    
    def _save_part(self, part_data: Dict[str, Any], chunk_index: int) -> Path:
        """
        Persist a part to disk (currently disabled - only final output is saved).
        
        Args:
            part_data: Part data dictionary
            chunk_index: Chunk number
            
        Returns:
            Path to saved part file (not used anymore)
        """
        part_file = self.parts_dir / f"part_{chunk_index}.json"
        try:
            with open(part_file, 'w', encoding='utf-8') as f:
                json.dump(part_data, f, ensure_ascii=False, indent=2)
            self.logger.info(f"✓ Saved part {chunk_index} to {part_file}")
            return part_file
        except Exception as e:
            self.logger.error(f"Failed to save part {chunk_index}: {str(e)}")
            raise
    
    def _load_part(self, chunk_index: int) -> Optional[Dict[str, Any]]:
        """
        Load a previously saved part from disk.
        
        Args:
            chunk_index: Chunk number
            
        Returns:
            Part data dictionary or None if not found
        """
        part_file = self.parts_dir / f"part_{chunk_index}.json"
        if part_file.exists():
            try:
                with open(part_file, 'r', encoding='utf-8') as f:
                    return json.load(f)
            except Exception as e:
                self.logger.warning(f"Failed to load part {chunk_index}: {str(e)}")
        return None
    
    def _find_last_completed_part(self) -> int:
        """
        Find the last successfully completed part by scanning disk.
        
        Returns:
            Last completed part index (0 if none found)
        """
        if not self.parts_dir or not self.parts_dir.exists():
            return 0
        
        max_part = 0
        for part_file in sorted(self.parts_dir.glob("part_*.json")):
            try:
                part_num = int(part_file.stem.split('_')[1])
                # Verify part is valid
                part_data = self._load_part(part_num)
                if part_data and self._validate_part(part_data, part_num):
                    max_part = max(max_part, part_num)
            except (ValueError, Exception) as e:
                self.logger.warning(f"Invalid part file {part_file}: {str(e)}")
        
        return max_part
    
    def _validate_part(self, part_data: Dict[str, Any], expected_index: int) -> bool:
        """
        Validate a part's structure and content.
        
        Args:
            part_data: Part data dictionary
            expected_index: Expected chunk_index value
            
        Returns:
            True if valid, False otherwise
        """
        # Check required fields
        if not isinstance(part_data, dict):
            self.logger.error(f"Part {expected_index}: Not a dictionary")
            return False
        
        # Check for chunk_index (matching user's prompt format)
        if 'chunk_index' not in part_data:
            self.logger.error(f"Part {expected_index}: Missing 'chunk_index' field")
            return False
        
        if part_data['chunk_index'] != expected_index:
            self.logger.error(f"Part {expected_index}: chunk_index mismatch (got {part_data['chunk_index']})")
            return False
        
        if 'is_last' not in part_data:
            self.logger.error(f"Part {expected_index}: Missing 'is_last' field")
            return False
        
        if 'rows' not in part_data:
            self.logger.error(f"Part {expected_index}: Missing 'rows' field")
            return False
        
        # Validate rows
        if not isinstance(part_data['rows'], list):
            self.logger.error(f"Part {expected_index}: 'rows' is not a list")
            return False
        
        # Rows must not be empty unless is_last=true
        if len(part_data['rows']) == 0 and not part_data['is_last']:
            self.logger.error(f"Part {expected_index}: Empty rows but is_last=false")
            return False
        
        # Validate row structure
        for i, row in enumerate(part_data['rows']):
            if not isinstance(row, dict):
                self.logger.error(f"Part {expected_index}, row {i}: Not a dictionary")
                return False
            
            required_fields = ['Type', 'Extraction', 'Number', 'Part']
            for field in required_fields:
                if field not in row:
                    self.logger.error(f"Part {expected_index}, row {i}: Missing '{field}' field")
                    return False
            
            # Check Extraction is single-line (no newlines)
            if isinstance(row['Extraction'], str) and '\n' in row['Extraction']:
                self.logger.warning(f"Part {expected_index}, row {i}: Extraction contains newlines (should use \\n)")
        
        return True
    
    def _parse_part_response(self, response_text: str, expected_index: int) -> Optional[Dict[str, Any]]:
        """
        Safely parse JSON from model response.
        
        Args:
            response_text: Raw response text from model
            expected_index: Expected chunk_index
            
        Returns:
            Parsed part data or None if parsing fails
        """
        if not response_text:
            self.logger.error(f"Part {expected_index}: Empty response")
            return None
        
        # Try to extract JSON (handle code blocks, markdown, etc.)
        cleaned = response_text.strip()
        
        # Remove markdown code blocks if present
        if cleaned.startswith('```'):
            # Find closing ```
            end_idx = cleaned.find('```', 3)
            if end_idx > 0:
                cleaned = cleaned[3:end_idx].strip()
                # Remove language identifier if present
                if cleaned.startswith('json'):
                    cleaned = cleaned[4:].strip()
        
        # Try parsing as JSON
        try:
            part_data = json.loads(cleaned)
            
            # Log what we got
            self.logger.debug(f"Part {expected_index}: Parsed JSON successfully")
            self.logger.debug(f"Part {expected_index}: Keys in response: {list(part_data.keys())}")
            if 'chunk_index' in part_data:
                self.logger.debug(f"Part {expected_index}: chunk_index = {part_data['chunk_index']}")
            if 'is_last' in part_data:
                self.logger.debug(f"Part {expected_index}: is_last = {part_data['is_last']}")
            if 'rows' in part_data:
                self.logger.debug(f"Part {expected_index}: rows count = {len(part_data['rows'])}")
            
            if self._validate_part(part_data, expected_index):
                return part_data
            else:
                self.logger.error(f"Part {expected_index}: Validation failed")
                self.logger.error(f"Part {expected_index}: Part data structure: {json.dumps(part_data, indent=2)[:1000]}")
                return None
        except json.JSONDecodeError as e:
            self.logger.error(f"Part {expected_index}: JSON parse error: {str(e)}")
            self.logger.error(f"Part {expected_index}: Response text (first 1000 chars): {cleaned[:1000]}")
            self.logger.error(f"Part {expected_index}: Response text (last 500 chars): {cleaned[-500:] if len(cleaned) > 500 else cleaned}")
            return None
    
    def _call_model_with_retry(self, pdf_path: str, prompt: str, chunk_index: int,
                               model_name: str, temperature: float,
                               progress_callback: Optional[Callable[[str], None]] = None) -> Optional[str]:
        """
        Call model with proper retry logic for rate limits.
        DO NOT rotate keys immediately on 429 - use backoff instead.
        
        Args:
            pdf_path: Path to PDF file
            prompt: Part-specific prompt
            chunk_index: Current chunk number
            model_name: Model name
            temperature: Temperature setting
            progress_callback: Optional progress callback
            
        Returns:
            Response text or None if failed after retries
        """
        for retry_attempt in range(self.MAX_RATE_LIMIT_RETRIES):
            try:
                # Call API (streaming MUST be disabled for multi-part processing)
                response = self.api_client.process_pdf_with_prompt(
                    pdf_path=pdf_path,
                    prompt=prompt,
                    model_name=model_name,
                    temperature=temperature,
                    max_tokens=self.MAX_TOKENS_PER_PART,
                    return_json=False,
                    force_no_streaming=True  # CRITICAL: Disable streaming for multi-part
                )
                
                if response:
                    return response
                else:
                    self.logger.error(f"Part {chunk_index}: Empty response from API")
                    return None
                    
            except Exception as e:
                error_str = str(e)
                error_type = type(e).__name__
                
                # Log full error details for debugging
                self.logger.error(f"Part {chunk_index}: Exception in API call (attempt {retry_attempt + 1}/{self.MAX_RATE_LIMIT_RETRIES})")
                self.logger.error(f"Part {chunk_index}: Error type: {error_type}")
                self.logger.error(f"Part {chunk_index}: Error message: {error_str}")
                
                # Check if it's a rate limit error (429)
                if '429' in error_str or 'rate limit' in error_str.lower() or 'quota' in error_str.lower():
                    # Extract Retry-After if available
                    retry_after = None
                    if hasattr(e, 'response') and hasattr(e.response, 'headers'):
                        headers = e.response.headers
                        retry_after = headers.get('Retry-After') or headers.get('retry-after')
                    
                    if retry_after:
                        try:
                            delay = int(retry_after)
                            self.logger.warning(f"Part {chunk_index}: Rate limit (429), Retry-After={delay}s")
                        except:
                            delay = self.RATE_LIMIT_BACKOFF[retry_attempt] if retry_attempt < len(self.RATE_LIMIT_BACKOFF) else self.RATE_LIMIT_BACKOFF[-1]
                    else:
                        # Use exponential backoff: 120s → 240s → 480s
                        delay = self.RATE_LIMIT_BACKOFF[retry_attempt] if retry_attempt < len(self.RATE_LIMIT_BACKOFF) else self.RATE_LIMIT_BACKOFF[-1]
                        self.logger.warning(f"Part {chunk_index}: Rate limit (429), using backoff delay={delay}s")
                    
                    if retry_attempt < self.MAX_RATE_LIMIT_RETRIES - 1:
                        if progress_callback:
                            progress_callback(f"Rate limit hit, waiting {delay}s before retry {retry_attempt + 1}/{self.MAX_RATE_LIMIT_RETRIES}...")
                        self.logger.info(f"Part {chunk_index}: Waiting {delay}s before retry...")
                        time.sleep(delay)
                        continue
                    else:
                        # All retries exhausted - abort and persist partial progress
                        self.logger.error(f"Part {chunk_index}: Rate limit persisted after {self.MAX_RATE_LIMIT_RETRIES} retries. Aborting.")
                        if progress_callback:
                            progress_callback(f"ERROR: Rate limit persisted. Partial progress saved. Please retry later.")
                        raise Exception(f"Rate limit persisted after {self.MAX_RATE_LIMIT_RETRIES} retries. Partial progress saved.")
                else:
                    # Non-rate-limit error - log and decide whether to retry or fail
                    # Some errors might be transient and worth retrying
                    if retry_attempt < self.MAX_RATE_LIMIT_RETRIES - 1:
                        # Retry for transient errors (network, timeout, etc.)
                        if any(keyword in error_str.lower() for keyword in ['timeout', 'connection', 'network', 'temporary', '503', '502', '500']):
                            delay = 10  # Short delay for transient errors
                            self.logger.warning(f"Part {chunk_index}: Transient error detected, retrying in {delay}s...")
                            if progress_callback:
                                progress_callback(f"Transient error, retrying in {delay}s...")
                            time.sleep(delay)
                            continue
                        else:
                            # Non-transient error, but we'll try once more
                            self.logger.error(f"Part {chunk_index}: Non-rate-limit error: {error_str}")
                            if retry_attempt == 0:  # Only retry once for non-transient errors
                                delay = 5
                                self.logger.warning(f"Part {chunk_index}: Retrying once more after {delay}s...")
                                time.sleep(delay)
                                continue
                            else:
                                # Give up after one retry
                                self.logger.error(f"Part {chunk_index}: Error persisted after retry. Giving up.")
                                raise
                    else:
                        # All retries exhausted
                        self.logger.error(f"Part {chunk_index}: Error persisted after {self.MAX_RATE_LIMIT_RETRIES} retries: {error_str}")
                        raise
        
        # If we get here, all retries were exhausted but no exception was raised (shouldn't happen)
        self.logger.error(f"Part {chunk_index}: All retries exhausted but no exception raised. This should not happen.")
        return None
    
    def _deduplicate_rows(self, all_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Remove duplicate rows based on page number, type, and content.
        More aggressive deduplication to handle overlapping parts.
        
        Args:
            all_rows: List of all row dictionaries
            
        Returns:
            Deduplicated list of rows
        """
        seen_hashes = set()
        seen_page_type_content = {}  # (page_number, type, content_start) -> row_index
        unique_rows = []
        duplicates_count = 0
        
        for row in all_rows:
            # Method 1: Full content hash (excluding Part field)
            row_copy = {k: v for k, v in row.items() if k != 'Part'}
            row_hash = hashlib.md5(json.dumps(row_copy, sort_keys=True).encode()).hexdigest()
            
            # Method 2: Page number + Type + Content start (for detecting overlapping content)
            page_num = row.get('Number', 0)
            row_type = row.get('Type', '')
            extraction = str(row.get('Extraction', ''))
            # Use first 200 chars of extraction as signature
            content_start = extraction[:200] if len(extraction) > 200 else extraction
            page_type_content_key = (page_num, row_type, content_start)
            
            # Check both methods
            is_duplicate = False
            
            if row_hash in seen_hashes:
                is_duplicate = True
                duplicates_count += 1
                self.logger.debug(f"Duplicate detected (hash): Page {page_num}, Type: {row_type}")
            elif page_type_content_key in seen_page_type_content:
                # Check if content is significantly similar
                existing_row_idx = seen_page_type_content[page_type_content_key]
                existing_row = unique_rows[existing_row_idx]
                existing_extraction = str(existing_row.get('Extraction', ''))
                
                # If extractions are very similar (more than 80% overlap), consider duplicate
                if len(extraction) > 0 and len(existing_extraction) > 0:
                    # Simple similarity check: if one is substring of another or vice versa
                    if extraction in existing_extraction or existing_extraction in extraction:
                        is_duplicate = True
                        duplicates_count += 1
                        self.logger.debug(f"Duplicate detected (content overlap): Page {page_num}, Type: {row_type}")
                    # Or if they're very similar in length and start the same way
                    elif abs(len(extraction) - len(existing_extraction)) < max(len(extraction), len(existing_extraction)) * 0.2:
                        # Check first 500 chars similarity
                        min_len = min(len(extraction), len(existing_extraction), 500)
                        if min_len > 0:
                            similarity = sum(1 for i in range(min_len) if extraction[i] == existing_extraction[i]) / min_len
                            if similarity > 0.8:
                                is_duplicate = True
                                duplicates_count += 1
                                self.logger.debug(f"Duplicate detected (high similarity {similarity:.2%}): Page {page_num}, Type: {row_type}")
            
            if not is_duplicate:
                seen_hashes.add(row_hash)
                seen_page_type_content[page_type_content_key] = len(unique_rows)
                unique_rows.append(row)
        
        if duplicates_count > 0:
            self.logger.warning(f"Removed {duplicates_count} duplicate rows using enhanced deduplication")
        
        return unique_rows
    
    def process_multi_part(self,
                          pdf_path: str,
                          base_prompt: str,
                          model_name: str = "gemini-2.5-pro",
                          temperature: float = 0.7,
                          resume: bool = True,
                          progress_callback: Optional[Callable[[str], None]] = None) -> Optional[str]:
        """
        Main entry point: Process PDF in multiple parts and assemble final JSON.
        
        Args:
            pdf_path: Path to PDF file
            base_prompt: Base OCR/structuring prompt (user provides this)
            model_name: Model name to use
            temperature: Temperature setting
            resume: Whether to resume from last completed part
            progress_callback: Optional callback for progress updates
            
        Returns:
            Path to final output JSON file or None if failed
        """
        if not GENERATIVEAI_AVAILABLE:
            self.logger.error("google.generativeai library not available")
            return None
        
        if not os.path.exists(pdf_path):
            self.logger.error(f"PDF file not found: {pdf_path}")
            return None
        
        # Initialize parts directory
        self.parts_dir = self._get_parts_directory(pdf_path, base_prompt)
        self.logger.info(f"Parts directory: {self.parts_dir}")
        
        # Set output directory to current directory (always use current directory)
        self.output_dir = Path.cwd()
        self.logger.info(f"Final output will be saved to current directory: {self.output_dir}")
        
        # Determine starting part
        # Resume is disabled since we don't save part files anymore
        start_part = 1
        all_rows = []
        
        # Main loop: process parts until is_last=true or max parts limit
        chunk_index = start_part
        
        while chunk_index <= self.MAX_PARTS_LIMIT:  # Continue until is_last=true or max parts limit
            self.logger.info(f"=== Processing Part {chunk_index} ===")
            if progress_callback:
                progress_callback(f"Processing part {chunk_index}...")
            
            # Build part-specific prompt
            is_first = (chunk_index == 1)
            part_prompt = self._build_part_prompt(base_prompt, chunk_index, is_first)
            
            # Call model with retry logic
            response_text = self._call_model_with_retry(
                pdf_path, part_prompt, chunk_index,
                model_name, temperature, progress_callback
            )
            
            if not response_text:
                self.logger.error(f"Part {chunk_index}: Failed to get response from API after all retries")
                self.logger.error(f"Part {chunk_index}: This could be due to:")
                self.logger.error(f"  - Rate limiting (429) that persisted after retries")
                self.logger.error(f"  - API key issues (403, quota exhausted)")
                self.logger.error(f"  - Network/connection issues")
                self.logger.error(f"  - Model errors or timeouts")
                self.logger.error(f"Part {chunk_index}: Stopping processing.")
                if progress_callback:
                    progress_callback(f"ERROR: Part {chunk_index} failed. Processing stopped.")
                break
            
            # Parse response
            part_data = self._parse_part_response(response_text, chunk_index)
            
            if not part_data:
                self.logger.error(f"Part {chunk_index}: Failed to parse response")
                self.logger.error(f"Part {chunk_index}: Response text (first 1000 chars): {response_text[:1000] if response_text else 'None'}")
                self.logger.error(f"Part {chunk_index}: Retrying same part...")
                # Retry same chunk_index (malformed JSON)
                time.sleep(5)
                continue
            
            # Validate part
            if not self._validate_part(part_data, chunk_index):
                self.logger.error(f"Part {chunk_index}: Validation failed")
                self.logger.error(f"Part {chunk_index}: Part data: {json.dumps(part_data, indent=2)[:500]}")
                self.logger.error(f"Part {chunk_index}: Retrying same part...")
                time.sleep(5)
                continue
            
            # Log part details
            is_last = part_data.get('is_last', False)
            rows_count = len(part_data.get('rows', []))
            self.logger.info(f"Part {chunk_index}: Parsed successfully - is_last={is_last}, rows={rows_count}")
            
            # Remove duplicates within this part before adding to all_rows
            if part_data.get('rows'):
                part_rows = part_data['rows']
                # Deduplicate within this part
                seen_in_part = set()
                unique_part_rows = []
                duplicates_in_part = 0
                
                for row in part_rows:
                    # Create hash of row content (excluding Part field)
                    row_copy = {k: v for k, v in row.items() if k != 'Part'}
                    row_hash = hashlib.md5(json.dumps(row_copy, sort_keys=True).encode()).hexdigest()
                    
                    if row_hash not in seen_in_part:
                        seen_in_part.add(row_hash)
                        unique_part_rows.append(row)
                    else:
                        duplicates_in_part += 1
                
                if duplicates_in_part > 0:
                    self.logger.warning(f"Part {chunk_index}: Removed {duplicates_in_part} duplicate rows within this part")
                    part_data['rows'] = unique_part_rows
                    rows_count = len(unique_part_rows)
            
            # CRITICAL: Enforce maximum parts rule
            if chunk_index >= self.MAX_PARTS_LIMIT and not is_last:
                self.logger.warning(f"Part {chunk_index}: Reached maximum part limit ({self.MAX_PARTS_LIMIT}). Forcing is_last=true.")
                is_last = True
                part_data['is_last'] = True
                if progress_callback:
                    progress_callback(f"Part {chunk_index}: Reached maximum part limit ({self.MAX_PARTS_LIMIT}). Marking as final part...")
            
            # CRITICAL: Check if model incorrectly set is_last=true on first part
            # Many models incorrectly mark the first part as last even when more content exists
            if is_last and chunk_index == 1:
                # Always force continuation on first part unless explicitly told otherwise
                # This prevents premature stopping when model incorrectly assumes completion
                self.logger.warning(f"Part {chunk_index}: Model set is_last=true on FIRST part - this is likely incorrect")
                self.logger.warning(f"Part {chunk_index}: Forcing continuation to part 2 (model may have more content)")
                is_last = False
                part_data['is_last'] = False  # Override model's decision
                if progress_callback:
                    progress_callback(f"Part {chunk_index}: Model marked as last, but forcing continuation to check for more content...")
            
            # Also check if is_last=true but we have very few rows (likely incomplete)
            # But only if we haven't reached part 4 yet
            if is_last and rows_count < 20 and chunk_index < 4:
                # If we get is_last=true with very few rows in early parts, it's suspicious
                self.logger.warning(f"Part {chunk_index}: is_last=true but only {rows_count} rows - this seems incomplete")
                self.logger.warning(f"Part {chunk_index}: Forcing continuation to verify...")
                is_last = False
                part_data['is_last'] = False
                if progress_callback:
                    progress_callback(f"Part {chunk_index}: Suspiciously few rows ({rows_count}) for last part - continuing to verify...")
            
            # Don't save part files - only save final JSON output
            # Part files are not needed anymore
            
            # Append rows
            if part_data.get('rows'):
                all_rows.extend(part_data['rows'])
                self.logger.info(f"Part {chunk_index}: Added {len(part_data['rows'])} rows (total: {len(all_rows)})")
            else:
                self.logger.warning(f"Part {chunk_index}: No rows in response (is_last={is_last})")
            
            # Check if this is the last part
            if is_last:
                self.logger.info(f"Part {chunk_index}: is_last=true, processing complete!")
                if progress_callback:
                    progress_callback(f"Part {chunk_index}: Last part received, assembling final output...")
                break
            else:
                self.logger.info(f"Part {chunk_index}: is_last=false, continuing to next part...")
                if progress_callback:
                    progress_callback(f"Part {chunk_index}: Completed ({rows_count} rows), continuing to part {chunk_index + 1}...")
            
            # Sleep between parts (8-15 seconds)
            # Only sleep if not the last part
            if not is_last:
                import random
                delay = random.uniform(self.MIN_DELAY_BETWEEN_PARTS, self.MAX_DELAY_BETWEEN_PARTS)
                self.logger.info(f"Waiting {delay:.1f}s before next part...")
                time.sleep(delay)
            
            chunk_index += 1
        
        # Check why loop ended
        if chunk_index > self.MAX_PARTS_LIMIT:
            self.logger.warning(f"Reached maximum parts limit ({self.MAX_PARTS_LIMIT}). Processing stopped.")
            if progress_callback:
                progress_callback(f"WARNING: Reached maximum parts limit ({self.MAX_PARTS_LIMIT}). Processing stopped.")
        
        # Calculate parts counts
        total_parts_in_final = 0  # No resume capability anymore
        
        if chunk_index == start_part:
            # No new parts were processed in this run
            if len(all_rows) > 0:
                self.logger.warning(f"No new parts processed (started at {start_part}), but {len(all_rows)} rows exist")
                self.logger.warning("This should not happen - no parts processed but rows exist")
                total_parts_processed = 0  # No new parts in this run
            else:
                self.logger.error(f"ERROR: No parts were processed! Started at {start_part}, ended at {chunk_index}")
                if progress_callback:
                    progress_callback(f"ERROR: No parts were successfully processed. Check logs for details.")
                return None
        else:
            total_parts_processed = chunk_index - start_part
        
        # Calculate total parts count
        total_parts_count = total_parts_processed
        
        # Log summary
        self.logger.info(f"=== Processing Summary ===")
        self.logger.info(f"Parts processed: {total_parts_processed}")
        self.logger.info(f"Total parts in final output: {total_parts_count}")
        self.logger.info(f"Total rows collected: {len(all_rows)}")
        
        if len(all_rows) == 0:
            self.logger.error("ERROR: No rows were collected from any part!")
            if progress_callback:
                progress_callback(f"ERROR: No rows collected. Check if model is returning correct format.")
            return None
        
        # Deduplicate rows
        self.logger.info(f"Deduplicating {len(all_rows)} rows...")
        unique_rows = self._deduplicate_rows(all_rows)
        self.logger.info(f"Final row count after deduplication: {len(unique_rows)}")
        
        # Sort rows by Number (page number) as float for proper decimal sorting
        def sort_key(row):
            number = row.get('Number', 0)
            try:
                # Convert to float for proper decimal sorting (e.g., 1.5, 2.3, etc.)
                number_num = float(number) if number else 0.0
            except (ValueError, TypeError):
                number_num = 0.0
            return number_num
        
        self.logger.info(f"Sorting {len(unique_rows)} rows by Number (as float)...")
        unique_rows = sorted(unique_rows, key=sort_key)
        self.logger.info(f"Rows sorted successfully")
        
        # Assemble final JSON
        end_part = chunk_index - 1 if total_parts_processed > 0 else start_part - 1
        
        final_output = {
            "metadata": {
                "total_parts": total_parts_count,
                "total_parts_processed": total_parts_processed,
                "start_part": start_part,
                "end_part": end_part,
                "total_rows": len(unique_rows),
                "processed_at": datetime.now().isoformat(),
                "pdf_path": os.path.basename(pdf_path)
            },
            "rows": unique_rows
        }
        
        # Save final output
        # Use a more descriptive filename based on PDF name
        pdf_name = os.path.splitext(os.path.basename(pdf_path))[0]
        # Sanitize filename (remove invalid characters)
        safe_pdf_name = "".join(c for c in pdf_name if c.isalnum() or c in (' ', '-', '_')).rstrip()
        final_filename = f"{safe_pdf_name}_final_output.json"
        final_file = self.output_dir / final_filename
        
        try:
            # Save JSON file
            with open(final_file, 'w', encoding='utf-8') as f:
                json.dump(final_output, f, ensure_ascii=False, indent=2)
            self.logger.info(f"✓ Final JSON output saved to {final_file}")
            self.logger.info(f"  - File path: {final_file.absolute()}")
            self.logger.info(f"  - Total parts: {total_parts_count}")
            self.logger.info(f"  - Total rows: {len(unique_rows)}")
            self.logger.info(f"  - File size: {final_file.stat().st_size / 1024:.2f} KB")
            
            # Save CSV file (sorted by Number as float)
            csv_filename = f"{safe_pdf_name}_final_output.csv"
            csv_file = self.output_dir / csv_filename
            
            # Prepare CSV data - rows are already sorted by Number (as float)
            csv_rows = []
            for row in unique_rows:
                csv_row = [
                    row.get('Type', ''),
                    row.get('Extraction', ''),
                    row.get('Number', ''),
                    row.get('Part', '')
                ]
                csv_rows.append(csv_row)
            
            # Write CSV file with UTF-8 BOM for Excel compatibility
            with open(csv_file, 'w', encoding='utf-8-sig', newline='') as f:
                writer = csv.writer(f, delimiter=';')
                # Write header
                writer.writerow(['Type', 'Extraction', 'Number', 'Part'])
                # Write data rows (already sorted by Number as float)
                writer.writerows(csv_rows)
            
            self.logger.info(f"✓ Final CSV output saved to {csv_file}")
            self.logger.info(f"  - File path: {csv_file.absolute()}")
            self.logger.info(f"  - Total rows: {len(unique_rows)}")
            self.logger.info(f"  - File size: {csv_file.stat().st_size / 1024:.2f} KB")
            
            if progress_callback:
                progress_callback(f"✓ Complete! Final outputs saved:")
                progress_callback(f"  JSON: {final_file}")
                progress_callback(f"  CSV: {csv_file}")
                progress_callback(f"  Total: {len(unique_rows)} rows from {total_parts_count} parts")
            return str(final_file)
        except Exception as e:
            self.logger.error(f"Failed to save final output: {str(e)}", exc_info=True)
            return None


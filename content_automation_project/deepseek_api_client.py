"""
DeepSeek API Client for Content Automation Project
Handles all interactions with DeepSeek API for text processing
"""

import os
import logging
import json
import time
import requests
from typing import Optional, Dict, List, Any, Callable
from api_layer import APIKeyManager


class DeepSeekAPIClient:
    """Main API client for DeepSeek services"""
    
    def __init__(self, api_key_manager: Optional[APIKeyManager] = None):
        """
        Initialize DeepSeek API client
        
        Args:
            api_key_manager: Optional APIKeyManager instance. If None, creates new one.
        """
        self.key_manager = api_key_manager or APIKeyManager()
        self.logger = logging.getLogger(__name__)
        
        # DeepSeek API endpoint
        self.base_url = "https://api.deepseek.com/v1/chat/completions"
        
        # Track current model
        self._current_model_name: Optional[str] = None
        
        # Track rate limit errors
        self._rate_limit_error_count = 0
        self._max_rate_limit_errors = 5
        
    def initialize_text_client(self, model_name: str = "deepseek-chat", 
                              api_key: Optional[str] = None) -> bool:
        """
        Initialize text processing client (for compatibility with GeminiAPIClient interface)
        
        Args:
            model_name: Name of the DeepSeek model to use
            api_key: Optional API key. If None, uses next key from manager.
            
        Returns:
            True if initialized successfully
        """
        try:
            key = api_key or self.key_manager.get_next_key()
            if not key:
                self.logger.error("No API key available")
                return False
                
            self._current_model_name = model_name
            self.logger.info(f"DeepSeek text client initialized with model: {model_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error initializing DeepSeek text client: {str(e)}")
            return False
    
    def process_text(self,
                    text: str,
                    system_prompt: Optional[str] = None,
                    model_name: str = "deepseek-chat",
                    temperature: float = 0.7,
                    max_tokens: int = 8192,
                    api_key: Optional[str] = None) -> Optional[str]:
        """
        Process text with DeepSeek API
        
        Args:
            text: Input text to process
            system_prompt: Optional system prompt/instructions
            model_name: DeepSeek model to use
            temperature: Temperature for generation (0.0-2.0)
            max_tokens: Maximum output tokens (will be capped at 8192 for DeepSeek API)
            api_key: Optional API key. If None, uses next key from manager.
            
        Returns:
            Processed text or None if failed
        """
        max_retries = 3
        timeout_delay = 10.0
        
        # DeepSeek API only supports max_tokens up to 8192
        DEEPSEEK_MAX_TOKENS = 8192
        effective_max_tokens = min(max_tokens, DEEPSEEK_MAX_TOKENS)
        if max_tokens > DEEPSEEK_MAX_TOKENS:
            self.logger.warning(f"max_tokens {max_tokens} exceeds DeepSeek limit ({DEEPSEEK_MAX_TOKENS}), capping to {DEEPSEEK_MAX_TOKENS}")
        
        for attempt in range(max_retries):
            try:
                key = api_key or self.key_manager.get_next_key()
                if not key:
                    self.logger.error("No API key available")
                    return None
                
                # Prepare messages
                messages = []
                if system_prompt:
                    messages.append({"role": "system", "content": system_prompt})
                messages.append({"role": "user", "content": text})
                
                # Prepare request
                headers = {
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {key}"
                }
                
                payload = {
                    "model": model_name,
                    "messages": messages,
                    "temperature": temperature,
                    "max_tokens": effective_max_tokens,
                    "stream": False
                }
                
                # Make API request
                response = requests.post(
                    self.base_url,
                    headers=headers,
                    json=payload,
                    timeout=300  # 5 minutes timeout
                )
                
                # Check response
                if response.status_code == 200:
                    result = response.json()
                    if "choices" in result and len(result["choices"]) > 0:
                        content = result["choices"][0]["message"]["content"]
                        self.logger.info(f"Text processed successfully with {model_name}")
                        self._rate_limit_error_count = 0  # Reset on success
                        return content
                    else:
                        self.logger.error("Unexpected response format from DeepSeek API")
                        return None
                        
                elif response.status_code == 429:
                    # Rate limit error
                    self._rate_limit_error_count += 1
                    if self._rate_limit_error_count >= self._max_rate_limit_errors:
                        self.logger.error("Max rate limit errors reached. Stopping.")
                        return None
                    
                    if attempt < max_retries - 1:
                        retry_after = int(response.headers.get("Retry-After", timeout_delay))
                        self.logger.warning(f"Rate limit (429) - Attempt {attempt + 1}/{max_retries}. Waiting {retry_after}s...")
                        time.sleep(retry_after)
                        # Try next key
                        continue
                    else:
                        self.logger.error("Rate limit error after all retries")
                        return None
                        
                elif response.status_code == 401:
                    # Invalid API key
                    self.logger.error("Invalid API key (401)")
                    # Try next key
                    if attempt < max_retries - 1:
                        continue
                    return None
                    
                else:
                    error_msg = f"API error {response.status_code}: {response.text}"
                    self.logger.error(error_msg)
                    if attempt < max_retries - 1:
                        time.sleep(timeout_delay)
                        continue
                    return None
                    
            except requests.exceptions.Timeout as e:
                if attempt < max_retries - 1:
                    self.logger.warning(f"⚠️ Timeout error - Attempt {attempt + 1}/{max_retries}")
                    self.logger.warning(f"   Waiting {timeout_delay:.1f}s before retry...")
                    time.sleep(timeout_delay)
                    timeout_delay *= 1.5
                else:
                    self.logger.error(f"Text processing failed after {max_retries} attempts: {str(e)}")
                    return None
                    
            except Exception as e:
                self.logger.error(f"Text processing failed: {str(e)}")
                if attempt < max_retries - 1:
                    time.sleep(timeout_delay)
                    continue
                return None
        
        return None
    
    def process_pdf_with_prompt(self,
                                pdf_path: str,
                                prompt: str,
                                model_name: str = "deepseek-chat",
                                temperature: float = 0.7,
                                max_tokens: int = 8192,
                                api_key: Optional[str] = None,
                                return_json: bool = False,
                                force_no_streaming: bool = False) -> Optional[str]:
        """
        Process PDF file with a prompt using DeepSeek API
        Note: DeepSeek doesn't support direct PDF upload, so we extract text first
        
        Args:
            pdf_path: Path to PDF file
            prompt: Prompt/instruction for processing the PDF
            model_name: DeepSeek model to use
            temperature: Temperature for generation (0.0-2.0)
            max_tokens: Maximum output tokens
            api_key: Optional API key. If None, uses next key from manager.
            return_json: Whether to extract and return JSON (default: False)
            force_no_streaming: Not used for DeepSeek
            
        Returns:
            Response text or None if failed
        """
        if not os.path.exists(pdf_path):
            self.logger.error(f"PDF file not found: {pdf_path}")
            return None
        
        try:
            # Extract text from PDF first
            from pdf_processor import PDFProcessor
            pdf_proc = PDFProcessor()
            extracted_text = pdf_proc.extract_text(pdf_path)
            
            if not extracted_text:
                self.logger.error("Failed to extract text from PDF")
                return None
            
            # Combine prompt with extracted text
            full_prompt = f"{prompt}\n\n--- PDF Content ---\n{extracted_text}"
            
            # Process using process_text
            response = self.process_text(
                text=full_prompt,
                system_prompt=None,
                model_name=model_name,
                temperature=temperature,
                max_tokens=max_tokens,
                api_key=api_key
            )
            
            if return_json and response:
                # Try to extract JSON from response
                try:
                    json_obj = json.loads(response)
                    return json.dumps(json_obj, ensure_ascii=False, indent=2)
                except:
                    # Try to extract JSON from code blocks
                    import re
                    json_match = re.search(r'```(?:json)?\s*(\{.*?\}|\[.*?\])\s*```', response, re.DOTALL)
                    if json_match:
                        return json_match.group(1)
            
            return response
            
        except Exception as e:
            self.logger.error(f"PDF processing failed: {str(e)}")
            return None
    
    def process_pdf_with_prompt_batch(self,
                                      pdf_path: str,
                                      prompt: str,
                                      model_name: str = "deepseek-chat",
                                      temperature: float = 0.7,
                                      max_tokens: int = 8192,
                                      pages_per_batch: int = 10,
                                      rows_per_batch: int = 500,
                                      api_key: Optional[str] = None,
                                      progress_callback: Optional[Callable[[str], None]] = None) -> Optional[str]:
        """
        Process PDF in batches (for compatibility with GeminiAPIClient interface)
        
        Args:
            pdf_path: Path to PDF file
            prompt: Base prompt/instruction
            model_name: DeepSeek model to use
            temperature: Temperature for generation
            max_tokens: Maximum output tokens per batch
            pages_per_batch: Number of pages to process per batch
            rows_per_batch: Target number of rows per batch
            api_key: Optional API key
            progress_callback: Optional callback function for progress updates
            
        Returns:
            Path to saved JSON file or None if failed
        """
        # For DeepSeek, we'll process the entire PDF at once since we extract text first
        # This is simpler than batching, but can be enhanced later if needed
        if progress_callback:
            progress_callback("Processing PDF with DeepSeek API...")
        
        response = self.process_pdf_with_prompt(
            pdf_path=pdf_path,
            prompt=prompt,
            model_name=model_name,
            temperature=temperature,
            max_tokens=max_tokens,
            api_key=api_key,
            return_json=True
        )
        
        if response and progress_callback:
            progress_callback("PDF processing completed")
        
        return response


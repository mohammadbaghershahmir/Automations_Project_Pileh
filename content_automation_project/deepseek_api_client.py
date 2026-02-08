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
from api_layer import APIKeyManager, APIConfig


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
        
        # Create a session for connection pooling and better connection management
        self.session = requests.Session()
        # Set default timeout and retry adapter
        from requests.adapters import HTTPAdapter
        from urllib3.util.retry import Retry
        
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["POST"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=10, pool_maxsize=10)
        self.session.mount("https://", adapter)
        
    def initialize_text_client(self, model_name: str = APIConfig.DEFAULT_DEEPSEEK_MODEL,
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
                    model_name: str = APIConfig.DEFAULT_DEEPSEEK_MODEL,
                    temperature: float = 0.7,
                    max_tokens: int = 8192,
                    api_key: Optional[str] = None,
                    auto_fallback_model: bool = True) -> Optional[str]:
        """
        Process text with DeepSeek API
        
        Args:
            text: Input text to process
            system_prompt: Optional system prompt/instructions
            model_name: DeepSeek model to use
            temperature: Temperature for generation (0.0-2.0)
            max_tokens: Maximum output tokens (will be capped at 8192 for DeepSeek API)
            api_key: Optional API key. If None, uses next key from manager.
            auto_fallback_model: If True, automatically try deepseek-chat if deepseek-reasoner fails
            
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
        
        # Calculate payload size to determine if we should reduce max_tokens
        payload_size = len(json.dumps({
            "model": model_name,
            "messages": [{"role": "user", "content": text}] + ([{"role": "system", "content": system_prompt}] if system_prompt else []),
            "temperature": temperature,
            "max_tokens": effective_max_tokens,
            "stream": False
        }).encode('utf-8'))
        
        # Auto-reduce max_tokens for large payloads to avoid connection issues
        if payload_size > 40 * 1024:  # > 40 KB
            # Reduce max_tokens proportionally
            reduction_factor = min(0.5, (40 * 1024) / payload_size)
            effective_max_tokens = max(2048, int(effective_max_tokens * reduction_factor))
            self.logger.warning(f"[DeepSeek API] Large payload ({payload_size/1024:.2f} KB) detected. Reducing max_tokens to {effective_max_tokens} to avoid connection issues.")
        
        # Track if we should try fallback model
        should_try_fallback = False
        fallback_model = "deepseek-chat" if model_name == "deepseek-reasoner" else None
        
        for attempt in range(max_retries):
            try:
                key = api_key or self.key_manager.get_next_key()
                if not key:
                    self.logger.error("No API key available")
                    return None
                
                # Log model name for Document Processing
                if attempt == 0:
                    self.logger.info(f"[DeepSeek API] Processing request with model: {model_name}")
                
                # Prepare messages
                messages = []
                if system_prompt:
                    messages.append({"role": "system", "content": system_prompt})
                messages.append({"role": "user", "content": text})
                
                # Prepare request
                headers = {
                    "Content-Type": "application/json",
                    "Authorization": f"Bearer {key}",
                    "Connection": "keep-alive",  # Keep connection alive
                    "Keep-Alive": "timeout=600"  # Keep alive for 10 minutes
                }
                
                payload = {
                    "model": model_name,
                    "messages": messages,
                    "temperature": temperature,
                    "max_tokens": effective_max_tokens,
                    "stream": False
                }
                
                # Make API request
                if attempt == 0:
                    self.logger.info(f"[DeepSeek API] Sending request to model: {model_name} (max_tokens: {effective_max_tokens})")
                
                # Calculate payload size before request
                payload_size = len(json.dumps(payload).encode('utf-8'))
                
                try:
                    # Use separate timeouts: (connect_timeout, read_timeout)
                    # connect_timeout: time to establish connection (10 seconds)
                    # read_timeout: time to read response (1200 seconds = 20 minutes for long responses)
                    # Total timeout can be up to 1210 seconds
                    connect_timeout = 10
                    read_timeout = 1200  # 20 minutes for long responses
                    
                    if attempt == 0:
                        self.logger.info(f"[DeepSeek API] Request timeout: connect={connect_timeout}s, read={read_timeout}s")
                    
                    # Log payload size for debugging
                    if attempt == 0:
                        self.logger.info(f"[DeepSeek API] Payload size: {payload_size:,} bytes ({payload_size/1024:.2f} KB)")
                        # Warn if payload is very large
                        if payload_size > 50 * 1024:  # > 50 KB
                            self.logger.warning(f"[DeepSeek API] Large payload detected ({payload_size/1024:.2f} KB). This may cause connection issues.")
                            self.logger.warning(f"[DeepSeek API] Consider splitting the request or using a different model.")
                    
                    response = self.session.post(
                        self.base_url,
                        headers=headers,
                        json=payload,
                        timeout=(connect_timeout, read_timeout),  # Separate connect and read timeouts
                        stream=False  # Disable streaming to avoid premature end issues
                    )
                except requests.exceptions.ChunkedEncodingError as e:
                    # This happens when connection closes before complete response
                    self.logger.error(f"[DeepSeek API] ChunkedEncodingError during request: {str(e)}")
                    self.logger.error(f"[DeepSeek API] This usually means the server closed the connection before sending complete response")
                    self.logger.warning(f"[DeepSeek API] Payload size: {payload_size:,} bytes ({payload_size/1024:.2f} KB) - may be too large for server")
                    self.logger.warning(f"[DeepSeek API] Consider: 1) Reducing payload size, 2) Using deepseek-chat instead of deepseek-reasoner, 3) Reducing max_tokens")
                    
                    if attempt < max_retries - 1:
                        # Progressive delay: 30s, 60s, 90s
                        retry_delay = timeout_delay * (3 + attempt * 2)
                        self.logger.warning(f"[DeepSeek API] Retrying after {retry_delay:.1f}s delay... (Attempt {attempt + 1}/{max_retries})")
                        time.sleep(retry_delay)
                        timeout_delay *= 1.5
                        # Try to recreate session for fresh connection
                        try:
                            self.session.close()
                            self.session = requests.Session()
                            from requests.adapters import HTTPAdapter
                            from urllib3.util.retry import Retry
                            retry_strategy = Retry(
                                total=3,
                                backoff_factor=1,
                                status_forcelist=[429, 500, 502, 503, 504],
                                allowed_methods=["POST"]
                            )
                            adapter = HTTPAdapter(max_retries=retry_strategy, pool_connections=10, pool_maxsize=10)
                            self.session.mount("https://", adapter)
                        except:
                            pass
                        continue
                    raise
                
                # Check response
                if response.status_code == 200:
                    try:
                        result = response.json()
                        if "choices" in result and len(result["choices"]) > 0:
                            content = result["choices"][0]["message"]["content"]
                            if content:
                                self.logger.info(f"[DeepSeek API] Text processed successfully with model: {model_name} (response length: {len(content)} chars)")
                                self._rate_limit_error_count = 0  # Reset on success
                                return content
                            else:
                                self.logger.warning(f"[DeepSeek API] Empty content in response for model: {model_name}")
                                # Check finish_reason if available
                                choice = result["choices"][0]
                                if "finish_reason" in choice:
                                    finish_reason = choice["finish_reason"]
                                    self.logger.warning(f"[DeepSeek API] Finish reason: {finish_reason}")
                                return None
                        else:
                            self.logger.error(f"[DeepSeek API] Unexpected response format: no choices in response")
                            self.logger.debug(f"[DeepSeek API] Response keys: {list(result.keys()) if isinstance(result, dict) else 'Not a dict'}")
                            return None
                    except (ValueError, KeyError) as e:
                        self.logger.error(f"[DeepSeek API] Error parsing response JSON: {str(e)}")
                        self.logger.debug(f"[DeepSeek API] Response text (first 500 chars): {response.text[:500] if hasattr(response, 'text') else 'N/A'}")
                        if attempt < max_retries - 1:
                            time.sleep(timeout_delay)
                            continue
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
                    self.logger.error(f"[DeepSeek API] Text processing failed after {max_retries} attempts: {str(e)}")
                    return None
            
            except requests.exceptions.ChunkedEncodingError as e:
                # Response ended prematurely - connection was closed before complete response
                error_msg = f"Response ended prematurely (ChunkedEncodingError) - connection closed before complete response"
                self.logger.error(f"[DeepSeek API] {error_msg}")
                self.logger.error(f"[DeepSeek API] Error details: {str(e)}")
                if attempt < max_retries - 1:
                    self.logger.warning(f"[DeepSeek API] Retrying... (Attempt {attempt + 1}/{max_retries})")
                    time.sleep(timeout_delay)
                    timeout_delay *= 1.5
                    continue
                else:
                    self.logger.error(f"[DeepSeek API] Failed after {max_retries} attempts due to connection issues")
                    return None
            
            except requests.exceptions.ConnectionError as e:
                # Connection error - network issue or server unreachable
                error_msg = f"Connection error - network issue or server unreachable"
                self.logger.error(f"[DeepSeek API] {error_msg}")
                self.logger.error(f"[DeepSeek API] Error details: {str(e)}")
                if attempt < max_retries - 1:
                    self.logger.warning(f"[DeepSeek API] Retrying... (Attempt {attempt + 1}/{max_retries})")
                    time.sleep(timeout_delay)
                    timeout_delay *= 1.5
                    continue
                else:
                    self.logger.error(f"[DeepSeek API] Failed after {max_retries} attempts due to connection issues")
                    return None
                    
            except Exception as e:
                error_msg = str(e)
                # Check if it's a premature end error
                if 'prematurely' in error_msg.lower() or 'ended' in error_msg.lower() or 'incomplete' in error_msg.lower():
                    self.logger.error(f"[DeepSeek API] Response ended prematurely: {error_msg}")
                    self.logger.error(f"[DeepSeek API] This usually means the connection was closed before receiving the complete response")
                    if attempt < max_retries - 1:
                        self.logger.warning(f"[DeepSeek API] Retrying... (Attempt {attempt + 1}/{max_retries})")
                        time.sleep(timeout_delay)
                        timeout_delay *= 1.5
                        continue
                    else:
                        self.logger.error(f"[DeepSeek API] Failed after {max_retries} attempts")
                        return None
                else:
                    self.logger.error(f"[DeepSeek API] Text processing failed: {error_msg}")
                    import traceback
                    self.logger.debug(f"[DeepSeek API] Full traceback:\n{traceback.format_exc()}")
                    if attempt < max_retries - 1:
                        time.sleep(timeout_delay)
                        continue
                    return None
        
        # If all retries failed and we have a fallback model, try it
        # This helps when deepseek-reasoner fails but deepseek-chat might work
        if auto_fallback_model and fallback_model and model_name == "deepseek-reasoner":
            self.logger.warning(f"[DeepSeek API] All retries with {model_name} failed. Attempting fallback to {fallback_model}...")
            return self.process_text(
                text=text,
                system_prompt=system_prompt,
                model_name=fallback_model,
                temperature=temperature,
                max_tokens=max_tokens,
                api_key=api_key,
                auto_fallback_model=False  # Prevent infinite recursion
            )
        
        return None
    
    def process_pdf_with_prompt(self,
                                pdf_path: str,
                                prompt: str,
                                model_name: str = APIConfig.DEFAULT_DEEPSEEK_MODEL,
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
                                      model_name: str = APIConfig.DEFAULT_DEEPSEEK_MODEL,
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


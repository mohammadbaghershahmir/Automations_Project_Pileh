"""
API Layer for Content Automation Project
Handles all interactions with Gemini API for text processing and TTS
"""

import csv
import os
import asyncio
import wave
import logging
import json
import math
import re
import sys
import threading
from typing import Optional, Dict, List, Any, Callable
from datetime import datetime

try:
    import google.genai as genai_new
    GENAI_AVAILABLE = True
except ImportError:
    GENAI_AVAILABLE = False
    logging.warning("google.genai library not available. TTS features will be disabled.")

try:
    import google.generativeai as genai
    GENERATIVEAI_AVAILABLE = True
except ImportError:
    GENERATIVEAI_AVAILABLE = False
    logging.warning("google.generativeai library not available. Text processing features will be disabled.")


class APIConfig:
    """Configuration class for API settings"""
    
    # Available Gemini TTS models
    TTS_MODELS = [
        "gemini-2.5-flash-preview-tts",
        "gemini-2.5-pro-preview-tts"
    ]
    
    # Available Gemini text processing models
    TEXT_MODELS = [
        "gemini-3-pro-preview",
        "gemini-2.5-flash",
        "gemini-2.5-pro",
        "gemini-2.0-flash",
        "gemini-1.5-pro",
        "gemini-1.5-flash"
    ]
    
    # Available DeepSeek text processing models
    DEEPSEEK_TEXT_MODELS = [
    "deepseek-chat",           # مدل پایه
    "deepseek-chat-v3",         # اگر موجود باشد
    "deepseek-chat-v2.5",       # اگر موجود باشد
    "deepseek-coder",           # برای کدنویسی
    "deepseek-reasoner",        # برای استدلال
    "deepseek-reasoner-v2", 
    ]
    
    # All available text models (combined)
    ALL_TEXT_MODELS = TEXT_MODELS + DEEPSEEK_TEXT_MODELS
    
    # Available Gemini TTS voices
    TTS_VOICES = [
        "Kore", "Orus", "Autonoe", "Umbriel", "Erinome", "Laomedeia",
        "Schedar", "Achird", "Sadachbia", "Zephyr", "Puck", "Charon",
        "Fenrir", "Aoede", "Enceladus", "Algieba", "Algenib", "Achernar",
        "Gacrux", "Zubenelgenubi", "Sadaltager", "Leda", "Callirrhoe",
        "Iapetus", "Despina", "Rasalgethi", "Alnilam", "Pulcherrima",
        "Vindemiatrix", "Sulafat"
    ]
    
    # Default settings
    DEFAULT_TTS_MODEL = "gemini-2.5-flash-preview-tts"
    DEFAULT_TEXT_MODEL = "gemini-2.5-flash"
    DEFAULT_VOICE = "Kore"
    DEFAULT_TEMPERATURE = 0.7
    # Maximum tokens for different models:
    # gemini-2.5-pro: up to 32768 tokens
    # gemini-2.5-flash: up to 32768 tokens
    # gemini-1.5-pro: up to 8192 tokens
    # gemini-1.5-flash: up to 8192 tokens
    DEFAULT_MAX_TOKENS = 16384  # Maximum for gemini-2.5 models
    DEFAULT_DEEPSEEK_MAX_TOKENS = 8192  # Maximum for DeepSeek API (hard limit)


class APIKeyManager:
    """Manages API keys with rotation support"""
    
    def __init__(self):
        self.api_keys: List[Dict[str, str]] = []
        self.current_index = 0
    
    @staticmethod
    def sanitize_error_message(error_msg: str, api_key: Optional[str] = None) -> str:
        """
        Remove API key from error messages to prevent leaking
        
        Args:
            error_msg: Original error message
            api_key: Optional API key to remove from message
            
        Returns:
            Sanitized error message without API key
        """
        if not error_msg:
            return error_msg
        
        sanitized = error_msg
        
        # Remove API key if provided
        if api_key:
            sanitized = sanitized.replace(api_key, "***REDACTED***")
        
        # Remove any potential API key patterns (starts with AIza...)
        api_key_pattern = r'AIza[0-9A-Za-z_-]{35}'
        sanitized = re.sub(api_key_pattern, "***REDACTED***", sanitized)
        
        return sanitized
        
    def load_from_csv(self, file_path: str, delimiter: str = ';') -> bool:
        """
        Load API keys from CSV file
        
        Args:
            file_path: Path to CSV file containing API keys
            delimiter: CSV delimiter (default: ';')
            
        Returns:
            True if loaded successfully, False otherwise
            
        Expected CSV format:
            account;project;api_key
        """
        try:
            if not os.path.exists(file_path):
                logging.error(f"API key file not found: {file_path}")
                return False
                
            with open(file_path, 'r', encoding='utf-8') as file:
                reader = csv.DictReader(file, delimiter=delimiter)
                self.api_keys = []
                
                for row in reader:
                    if 'api_key' in row and row['api_key'].strip():
                        self.api_keys.append({
                            'account': row.get('account', 'Unknown'),
                            'project': row.get('project', 'Unknown'),
                            'api_key': row['api_key'].strip()
                        })
            
            if not self.api_keys:
                logging.error("No valid API keys found in the file")
                return False
                
            logging.info(f"Loaded {len(self.api_keys)} API keys")
            return True
            
        except Exception as e:
            # Sanitize error message to prevent API key leakage
            error_msg = APIKeyManager.sanitize_error_message(str(e))
            logging.error(f"Error loading API keys: {error_msg}")
            return False
    
    def get_next_key(self) -> Optional[str]:
        """
        Get next API key in rotation
        
        Returns:
            API key string or None if no keys available
        """
        if not self.api_keys:
            return None
            
        api_key = self.api_keys[self.current_index]['api_key']
        self.current_index = (self.current_index + 1) % len(self.api_keys)
        return api_key
    
    def get_current_key_info(self) -> Optional[Dict[str, str]]:
        """
        Get information about current API key
        
        Returns:
            Dictionary with account, project, and api_key or None
        """
        if not self.api_keys:
            return None
            
        prev_index = (self.current_index - 1) % len(self.api_keys)
        return self.api_keys[prev_index]
    
    def add_key(self, api_key: str, account: str = "Manual", project: str = "Manual"):
        """
        Manually add an API key
        
        Args:
            api_key: The API key string
            account: Account name (optional)
            project: Project name (optional)
        """
        self.api_keys.append({
            'account': account,
            'project': project,
            'api_key': api_key
        })
        logging.info(f"Added API key for account: {account}")


class GeminiAPIClient:
    """Main API client for Gemini services"""
    
    def __init__(self, api_key_manager: Optional[APIKeyManager] = None):
        """
        Initialize Gemini API client
        
        Args:
            api_key_manager: Optional APIKeyManager instance. If None, creates new one.
        """
        self.key_manager = api_key_manager or APIKeyManager()
        self.logger = logging.getLogger(__name__)
        
        # Initialize clients
        self.tts_client: Optional[genai_new.Client] = None
        self.text_client: Optional[genai.GenerativeModel] = None
        self._current_model_name: Optional[str] = None  # Track current model name
        
        # Track response truncation
        self._response_truncated = False
        
        # Track 429 errors for hard stop
        self._rate_limit_error_count = 0
        self._max_rate_limit_errors = 5  # Hard stop after 5 consecutive 429s
        self._last_rate_limit_time = None
        
        # Track worker/thread info for concurrency detection
        self._worker_id = threading.current_thread().ident
        self._process_id = os.getpid()
        self._process_name = os.path.basename(sys.argv[0]) if sys.argv else "unknown"
        
    def initialize_tts_client(self, api_key: Optional[str] = None) -> bool:
        """
        Initialize TTS client
        
        Args:
            api_key: Optional API key. If None, uses next key from manager.
            
        Returns:
            True if initialized successfully
        """
        if not GENAI_AVAILABLE:
            self.logger.error("google.genai library not available")
            return False
            
        try:
            key = api_key or self.key_manager.get_next_key()
            if not key:
                self.logger.error("No API key available")
                return False
                
            self.tts_client = genai_new.Client(api_key=key)
            self.logger.info("TTS client initialized successfully")
            return True
            
        except Exception as e:
            self.logger.error(f"Error initializing TTS client: {str(e)}")
            return False
    
    def extract_from_code_block(self, text: str) -> str:
        """
        Extract content from markdown code blocks (```csv, ```, etc.)
        
        Args:
            text: Text that may contain code blocks
            
        Returns:
            Extracted content without code block markers
        """
        if not text:
            return text
        
        # Check if text starts with code block marker
        text_stripped = text.strip()
        
        # Pattern 1: ```csv ... ```
        if text_stripped.startswith('```csv'):
            # Find the closing ```
            end_marker = text_stripped.find('```', 6)  # Start after ```csv
            if end_marker != -1:
                # Extract content between markers
                extracted = text_stripped[6:end_marker].strip()  # Skip ```csv and closing ```
                self.logger.info("Extracted CSV from ```csv code block")
                return extracted
            else:
                # No closing marker found, might be truncated
                # Remove opening marker and return rest
                extracted = text_stripped[6:].strip()
                self.logger.warning("CSV code block missing closing marker - response may be truncated")
                return extracted
        
        # Pattern 2: ``` ... ``` (generic code block)
        if text_stripped.startswith('```'):
            # Find the first newline after ```
            first_newline = text_stripped.find('\n', 3)
            if first_newline != -1:
                # Find the closing ```
                end_marker = text_stripped.find('```', first_newline)
                if end_marker != -1:
                    # Extract content between markers
                    extracted = text_stripped[first_newline+1:end_marker].strip()
                    self.logger.info("Extracted content from generic code block")
                    return extracted
                else:
                    # No closing marker, might be truncated
                    extracted = text_stripped[first_newline+1:].strip()
                    self.logger.warning("Code block missing closing marker - response may be truncated")
                    return extracted
            else:
                # No newline found, just remove opening marker
                extracted = text_stripped[3:].strip()
                self.logger.warning("Code block format unusual - removing markers")
                return extracted
        
        # No code block markers found, return as is
        return text
    
    def initialize_text_client(self, model_name: str = APIConfig.DEFAULT_TEXT_MODEL, 
                              api_key: Optional[str] = None) -> bool:
        """
        Initialize text processing client
        
        Args:
            model_name: Name of the Gemini model to use
            api_key: Optional API key. If None, uses next key from manager.
            
        Returns:
            True if initialized successfully
        """
        if not GENERATIVEAI_AVAILABLE:
            self.logger.error("google.generativeai library not available")
            return False
            
        try:
            key = api_key or self.key_manager.get_next_key()
            if not key:
                self.logger.error("No API key available")
                return False
                
            genai.configure(api_key=key)
            self.text_client = genai.GenerativeModel(model_name)
            self._current_model_name = model_name
            self.logger.info(f"Text client initialized with model: {model_name}")
            return True
            
        except Exception as e:
            self.logger.error(f"Error initializing text client: {str(e)}")
            return False
    
    async def generate_tts_async(self, 
                                text: str,
                                output_file: str,
                                voice: str = APIConfig.DEFAULT_VOICE,
                                model: str = APIConfig.DEFAULT_TTS_MODEL,
                                api_key: Optional[str] = None,
                                instruction: Optional[str] = None,
                                multi_speaker_config: Optional[Dict] = None) -> bool:
        """
        Generate text-to-speech audio asynchronously
        
        Args:
            text: Text to convert to speech
            output_file: Output file path (should be .wav)
            voice: Voice name (for single speaker)
            model: TTS model name
            api_key: Optional API key. If None, uses next key from manager.
            instruction: Optional instruction/context to prepend to text
            multi_speaker_config: Optional dict with speaker1_voice and speaker2_voice for multi-speaker
            
        Returns:
            True if successful, False otherwise
        """
        if not GENAI_AVAILABLE:
            self.logger.error("google.genai library not available")
            return False
            
        try:
            # Initialize client if needed
            if not self.tts_client:
                key = api_key or self.key_manager.get_next_key()
                if not key:
                    self.logger.error("No API key available")
                    return False
                self.tts_client = genai_new.Client(api_key=key)
            elif api_key:
                # Reinitialize with provided key
                self.tts_client = genai_new.Client(api_key=api_key)
            
            # Ensure output file has .wav extension
            if not output_file.lower().endswith('.wav'):
                output_file = output_file.rsplit('.', 1)[0] + '.wav'
            
            # Combine instruction and text
            if instruction and instruction.strip():
                combined_content = f"{instruction}\n\n{text}"
            else:
                combined_content = text
            
            # Configure speech settings
            if multi_speaker_config:
                # Multi-speaker configuration
                speech_config = genai_new.types.SpeechConfig(
                    multi_speaker_voice_config=genai_new.types.MultiSpeakerVoiceConfig(
                        speaker_voice_configs=[
                            genai_new.types.SpeakerVoiceConfig(
                                speaker='Speaker1',
                                voice_config=genai_new.types.VoiceConfig(
                                    prebuilt_voice_config=genai_new.types.PrebuiltVoiceConfig(
                                        voice_name=multi_speaker_config.get('speaker1_voice', voice)
                                    )
                                )
                            ),
                            genai_new.types.SpeakerVoiceConfig(
                                speaker='Speaker2',
                                voice_config=genai_new.types.VoiceConfig(
                                    prebuilt_voice_config=genai_new.types.PrebuiltVoiceConfig(
                                        voice_name=multi_speaker_config.get('speaker2_voice', 'Puck')
                                    )
                                )
                            )
                        ]
                    )
                )
            else:
                # Single speaker configuration
                speech_config = genai_new.types.SpeechConfig(
                    voice_config=genai_new.types.VoiceConfig(
                        prebuilt_voice_config=genai_new.types.PrebuiltVoiceConfig(
                            voice_name=voice
                        )
                    )
                )
            
            # Generate TTS
            response = await self.tts_client.aio.models.generate_content(
                model=model,
                contents=combined_content,
                config=genai_new.types.GenerateContentConfig(
                    response_modalities=["AUDIO"],
                    speech_config=speech_config
                )
            )
            
            # Extract audio data
            audio_data = response.candidates[0].content.parts[0].inline_data.data
            
            # Write WAV file
            with wave.open(output_file, 'wb') as wf:
                wf.setnchannels(1)  # Mono
                wf.setsampwidth(2)  # 16-bit
                wf.setframerate(24000)  # 24kHz
                wf.writeframes(audio_data)
            
            self.logger.info(f"TTS generated successfully: {output_file}")
            return True
            
        except Exception as e:
            self.logger.error(f"TTS generation failed: {str(e)}")
            return False
    
    def generate_tts(self, 
                    text: str,
                    output_file: str,
                    voice: str = APIConfig.DEFAULT_VOICE,
                    model: str = APIConfig.DEFAULT_TTS_MODEL,
                    api_key: Optional[str] = None,
                    instruction: Optional[str] = None,
                    multi_speaker_config: Optional[Dict] = None) -> bool:
        """
        Generate text-to-speech audio (synchronous wrapper)
        
        Args:
            text: Text to convert to speech
            output_file: Output file path (should be .wav)
            voice: Voice name (for single speaker)
            model: TTS model name
            api_key: Optional API key. If None, uses next key from manager.
            instruction: Optional instruction/context to prepend to text
            multi_speaker_config: Optional dict with speaker1_voice and speaker2_voice
            
        Returns:
            True if successful, False otherwise
        """
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            try:
                return loop.run_until_complete(
                    self.generate_tts_async(text, output_file, voice, model, api_key, instruction, multi_speaker_config)
                )
            finally:
                loop.close()
        except Exception as e:
            self.logger.error(f"TTS generation error: {str(e)}")
            return False
    
    def process_text(self,
                    text: str,
                    system_prompt: Optional[str] = None,
                    model_name: str = APIConfig.DEFAULT_TEXT_MODEL,
                    temperature: float = APIConfig.DEFAULT_TEMPERATURE,
                    max_tokens: int = APIConfig.DEFAULT_MAX_TOKENS,
                    api_key: Optional[str] = None) -> Optional[str]:
        """
        Process text with Gemini AI with retry logic for timeout errors
        
        Args:
            text: Input text to process
            system_prompt: Optional system prompt/instructions
            model_name: Gemini model to use
            temperature: Temperature for generation (0.0-2.0)
            max_tokens: Maximum output tokens
            api_key: Optional API key. If None, uses next key from manager.
            
        Returns:
            Processed text or None if failed
        """
        if not GENERATIVEAI_AVAILABLE:
            self.logger.error("google.generativeai library not available")
            return None
            
        def _do_process():
            """Inner function to perform the actual processing"""
            # Initialize or recreate client if needed (model changed or client doesn't exist)
            key = api_key or self.key_manager.get_next_key()
            if not key:
                self.logger.error("No API key available")
                return None
            
            # Recreate client if model changed or client doesn't exist
            if not self.text_client or self._current_model_name != model_name or api_key:
                genai.configure(api_key=key)
                self.text_client = genai.GenerativeModel(model_name)
                self._current_model_name = model_name
                self.logger.info(f"Initialized/recreated text client with model: {model_name}")
            
            # Prepare prompt
            if system_prompt:
                full_prompt = f"{system_prompt}\n\nText to process:\n{text}"
            else:
                full_prompt = text
            
            # Determine maximum tokens based on model
            # Model-specific maximum tokens:
            # gemini-2.5-pro: up to 32768 tokens
            # gemini-2.5-flash: up to 32768 tokens
            # gemini-2.0-flash: up to 32768 tokens
            # gemini-1.5-pro: up to 8192 tokens
            # gemini-1.5-flash: up to 8192 tokens
            if '2.5' in model_name or '2.0' in model_name:
                # Newer models support up to 32768 tokens
                model_max_tokens = 32768
            elif '1.5' in model_name:
                # Older models support up to 8192 tokens
                model_max_tokens = 8192
            else:
                # Default to maximum for safety
                model_max_tokens = 32768
            
            # Use the maximum available for the model, but respect user's max_tokens if lower
            effective_max_tokens = min(max(max_tokens, model_max_tokens), model_max_tokens)
            
            # Generate content
            generation_config = genai.types.GenerationConfig(
                temperature=temperature,
                max_output_tokens=effective_max_tokens,
            )
            
            response = self.text_client.generate_content(
                full_prompt,
                generation_config=generation_config
            )
            
            processed_text = response.text
            self.logger.info(f"Text processed successfully with {model_name}")
            return processed_text
            
        # Try with retry logic for timeout errors
        import time
        max_retries = 3
        timeout_delay = 10.0  # Shorter delay for timeouts (10 seconds)
        
        for attempt in range(max_retries):
            try:
                result = _do_process()
                if result:
                    return result
            except Exception as e:
                if self._is_timeout_error(e):
                    if attempt < max_retries - 1:
                        self.logger.warning(f"⚠️ Timeout error (504) - Attempt {attempt + 1}/{max_retries}")
                        self.logger.warning(f"   Waiting {timeout_delay:.1f}s before retry...")
                        time.sleep(timeout_delay)
                        timeout_delay *= 1.5  # Exponential backoff
                    else:
                        # Last attempt failed
                        self.logger.error(f"Text processing failed after {max_retries} attempts: {str(e)}")
                        return None
                else:
                    # Not a timeout error, log and return None
                    self.logger.error(f"Text processing failed: {str(e)}")
                    return None
        
        return None
    
    def process_pdf_with_prompt_batch(self,
                                      pdf_path: str,
                                      prompt: str,
                                      model_name: str = APIConfig.DEFAULT_TEXT_MODEL,
                                      temperature: float = APIConfig.DEFAULT_TEMPERATURE,
                                      max_tokens: int = APIConfig.DEFAULT_MAX_TOKENS,
                                      pages_per_batch: int = 10,
                                      rows_per_batch: int = 500,
                                      api_key: Optional[str] = None,
                                      progress_callback: Optional[Callable[[str], None]] = None) -> Optional[str]:
        """
        Process PDF in batches with JSON output, then convert to CSV and append results.
        This method is optimized for large PDFs to avoid token limits.
        
        Args:
            pdf_path: Path to PDF file
            prompt: Base prompt/instruction (will be modified to request JSON)
            model_name: Gemini model to use
            temperature: Temperature for generation
            max_tokens: Maximum output tokens per batch
            pages_per_batch: Number of pages to process per batch
            rows_per_batch: Target number of rows per batch (200-1000 recommended)
            api_key: Optional API key
            progress_callback: Optional callback function for progress updates (takes message string)
            
        Returns:
            Path to saved JSON file or None if failed
        """
        if not GENERATIVEAI_AVAILABLE:
            self.logger.error("google.generativeai library not available")
            return None
        
        if not os.path.exists(pdf_path):
            self.logger.error(f"PDF file not found: {pdf_path}")
            return None
        
        try:
            # Get PDF page count
            from pdf_processor import PDFProcessor
            pdf_proc = PDFProcessor()
            total_pages = pdf_proc.count_pages(pdf_path)
            
            if total_pages == 0:
                self.logger.error("PDF has no pages")
                return None
            
            if progress_callback:
                progress_callback(f"PDF has {total_pages} pages. Starting batch processing...")
            
            self.logger.info(f"Processing PDF with {total_pages} pages in batches of {pages_per_batch} pages")
            
            # Add JSON output instruction WITHOUT modifying user's prompt
            # We append instructions separately to preserve original prompt
            json_instruction = """

IMPORTANT OUTPUT FORMAT:
Please output your response as a JSON array format instead of CSV. Each object in the array should represent one row with the structure you specified in your prompt.
Output ONLY valid JSON array, no code blocks, no markdown, just pure JSON.
The JSON will be automatically converted to CSV format after processing.
"""
            
            # Preserve original prompt, just add format instruction
            json_prompt = prompt + json_instruction
            self.logger.info("Using original prompt with JSON output instruction for batch processing (prompt preserved)")
            
            # Initialize API client
            key = api_key or self.key_manager.get_next_key()
            if not key:
                self.logger.error("No API key available")
                return None
                
            genai.configure(api_key=key)
            model = genai.GenerativeModel(model_name)
            
            # Upload PDF file once (reused for all batches)
            pdf_file = genai.upload_file(path=pdf_path)
            self.logger.info(f"Uploaded PDF file: {os.path.basename(pdf_path)}")
            
            if progress_callback:
                progress_callback("PDF uploaded. Waiting for processing...")
            
            # Wait for file to be processed
            import time
            while pdf_file.state.name == "PROCESSING":
                self.logger.info("Waiting for PDF to be processed...")
                time.sleep(2)
                pdf_file = genai.get_file(pdf_file.name)
            
            if pdf_file.state.name == "FAILED":
                self.logger.error("PDF file processing failed")
                return None
            
            # Determine max tokens for model
            if '2.5' in model_name or '2.0' in model_name:
                model_max_tokens = 32768
            elif '1.5' in model_name:
                model_max_tokens = 8192
            else:
                model_max_tokens = 32768
            
            effective_max_tokens = min(max(max_tokens, model_max_tokens), model_max_tokens)
            
            # Process in batches
            all_json_rows = []
            batch_number = 0
            total_batches = math.ceil(total_pages / pages_per_batch)
            
            for start_page in range(1, total_pages + 1, pages_per_batch):
                batch_number += 1
                end_page = min(start_page + pages_per_batch - 1, total_pages)
                
                if progress_callback:
                    progress_callback(f"Processing batch {batch_number}/{total_batches} (pages {start_page}-{end_page})...")
                
                self.logger.info(f"Processing batch {batch_number}/{total_batches}: pages {start_page}-{end_page}")
                
                # Create batch-specific prompt
                batch_prompt = f"{json_prompt}\n\nIMPORTANT: Process ONLY pages {start_page} to {end_page} of the PDF. Output JSON format with approximately {rows_per_batch} rows maximum."
                
                # Process this batch
                batch_result = self._process_single_batch(
                    model, pdf_file, batch_prompt, effective_max_tokens, 
                    start_page, end_page, progress_callback
                )
                
                if batch_result:
                    all_json_rows.extend(batch_result)
                    if progress_callback:
                        progress_callback(f"Batch {batch_number} completed: {len(batch_result)} rows extracted (Total: {len(all_json_rows)} rows)")
                else:
                    self.logger.warning(f"Batch {batch_number} returned no results")
                    if progress_callback:
                        progress_callback(f"Warning: Batch {batch_number} returned no results")
            
            # Convert all rows to JSON format and save
            if all_json_rows:
                # Save JSON (formatted for readability)
                json_output = json.dumps(all_json_rows, ensure_ascii=False, indent=2)
                
                # Save JSON to current directory
                json_file_path = self._save_json_to_current_directory(csv_path=pdf_path, json_content=json_output)
                
                if progress_callback:
                    progress_callback(f"✓ Processing complete! Total rows: {len(all_json_rows)}")
                    if json_file_path:
                        progress_callback(f"✓ JSON saved to: {os.path.basename(json_file_path)}")
                
                self.logger.info(f"JSON saved to: {json_file_path}")
                return json_file_path
            else:
                self.logger.error("No data extracted from any batch")
                return None
                
        except Exception as e:
            error_str = str(e)
            if self._is_quota_error(e):
                if '403' in error_str or 'leaked' in error_str.lower():
                    self.logger.error(f"❌ Batch PDF processing failed: API key leaked (403) - {error_str}")
                    self.logger.error("API key was reported as leaked. Please:")
                    self.logger.error("  1. Remove the leaked API key from your CSV file")
                    self.logger.error("  2. Add a new valid API key to your CSV file")
                else:
                    self.logger.error(f"❌ Batch PDF processing failed: Quota exhausted (429) - {error_str}")
                    self.logger.error("All available API keys have been exhausted. Please:")
                    self.logger.error("  1. Wait for quota reset")
                    self.logger.error("  2. Add more API keys to your CSV file")
            else:
                self.logger.error(f"Batch PDF processing failed: {error_str}", exc_info=True)
            return None
        finally:
            # Clean up uploaded file
            try:
                genai.delete_file(pdf_file.name)
            except:
                pass
    
    def _convert_prompt_to_json_format(self, prompt: str) -> str:
        """Convert CSV prompt to JSON format prompt"""
        # Add JSON format instructions
        json_instructions = """

IMPORTANT OUTPUT FORMAT CHANGE:
Instead of CSV format, output your response as a JSON array. Each object in the array represents one row with the following structure:

[
  {
    "Type": "page text",
    "Extraction": "full text content here",
    "Number": 1,
    "Part": 1
  },
  {
    "Type": "Table",
    "Extraction": "table content or JSON",
    "Number": 1,
    "Part": 1
  }
]

The JSON should be valid, minified (no extra whitespace), and contain all the data that would have been in CSV format.
Output ONLY the JSON array, no code blocks, no markdown, just pure JSON.
"""
        
        return prompt + json_instructions
    
    def _is_quota_error(self, error: Exception) -> bool:
        """
        Check if error is a 429 quota exhaustion error or 403 leaked key error
        These errors should trigger API key rotation
        """
        error_str = str(error).lower()
        error_code = str(error)
        return ('429' in error_code or 
                '403' in error_code or
                'quota' in error_str or 
                'exhausted' in error_str or 
                'resource has been exhausted' in error_str or
                'leaked' in error_str or
                'api key was reported as leaked' in error_str)
    
    def _extract_error_details(self, error: Exception) -> Dict[str, Any]:
        """
        Extract complete error details including headers, body, and retry-after
        
        Args:
            error: Exception object
            
        Returns:
            Dictionary with error details
        """
        error_details = {
            'error_type': type(error).__name__,
            'error_message': str(error),
            'error_repr': repr(error),
            'headers': {},
            'retry_after': None,
            'status_code': None,
            'error_body': None,
            'concurrency_info': {
                'pid': self._process_id,
                'process_name': self._process_name,
                'thread_id': threading.current_thread().ident,
                'thread_name': threading.current_thread().name,
                'worker_id': self._worker_id
            }
        }
        
        # Try to extract HTTP response details
        error_str = str(error)
        error_repr = repr(error)
        
        # Check for status code
        import re
        status_match = re.search(r'(\d{3})', error_str)
        if status_match:
            error_details['status_code'] = int(status_match.group(1))
        
        # Try to extract from exception attributes
        if hasattr(error, 'status_code'):
            error_details['status_code'] = error.status_code
        
        if hasattr(error, 'response'):
            response = error.response
            if hasattr(response, 'headers'):
                error_details['headers'] = dict(response.headers) if response.headers else {}
                # Extract Retry-After header
                if 'retry-after' in error_details['headers']:
                    error_details['retry_after'] = error_details['headers']['retry-after']
                elif 'Retry-After' in error_details['headers']:
                    error_details['retry_after'] = error_details['headers']['Retry-After']
            
            if hasattr(response, 'text'):
                error_details['error_body'] = response.text
            elif hasattr(response, 'content'):
                try:
                    error_details['error_body'] = response.content.decode('utf-8', errors='ignore')
                except:
                    error_details['error_body'] = str(response.content)
        
        # Try to extract from string representation
        if not error_details['retry_after']:
            retry_after_match = re.search(r'[Rr]etry-[Aa]fter[:\s]+(\d+)', error_str)
            if retry_after_match:
                error_details['retry_after'] = int(retry_after_match.group(1))
        
        # Extract error body from string if available
        if not error_details['error_body']:
            # Look for JSON-like error body in string
            json_match = re.search(r'\{[^{}]*"error"[^{}]*\}', error_str, re.DOTALL)
            if json_match:
                error_details['error_body'] = json_match.group(0)
        
        return error_details
    
    def _is_rate_limit_error(self, error: Exception) -> bool:
        """
        Check if error is a rate limit error (429) vs actual quota exhaustion
        Rate limit errors can be retried with backoff, quota errors need key rotation
        """
        error_str = str(error).lower()
        error_code = str(error)
        
        # 429 can be either rate limit or quota
        if '429' in error_code:
            # Check for rate limit specific messages
            if 'rate' in error_str or 'too many requests' in error_str or 'rate limit' in error_str:
                return True
            # If it's 429 but not explicitly rate limit, it might be quota
            # We'll treat it as potentially recoverable with retry
            return True
        
        return False
    
    def _is_timeout_error(self, error: Exception) -> bool:
        """
        Check if error is a timeout error (504 Deadline Exceeded)
        Timeout errors can be retried with shorter delays
        """
        error_str = str(error).lower()
        error_code = str(error)
        
        # Check for 504 status code or deadline exceeded messages
        if '504' in error_code or 'deadline exceeded' in error_str or 'timeout' in error_str:
            return True
        
        return False
    
    def _should_use_streaming(self, prompt_length: int, max_tokens: int) -> bool:
        """
        Determine if streaming should be used based on request size
        For large requests (long prompts or high max_tokens), disable streaming
        to avoid burst requests that trigger rate limits
        
        Args:
            prompt_length: Length of prompt in characters
            max_tokens: Maximum output tokens requested
            
        Returns:
            True if streaming should be used, False otherwise
        """
        # Disable streaming for large requests to prevent rate limit bursts
        # Threshold: prompt > 5000 chars OR max_tokens > 16000
        if prompt_length > 5000 or max_tokens > 16000:
            self.logger.info(f"Streaming disabled for large request (prompt: {prompt_length} chars, max_tokens: {max_tokens})")
            return False
        return True
    
    def _check_hard_stop(self) -> bool:
        """
        Check if we should hard stop after too many consecutive 429 errors
        
        Returns:
            True if should stop, False otherwise
        """
        if self._rate_limit_error_count >= self._max_rate_limit_errors:
            self.logger.error(f"🛑 HARD STOP: {self._rate_limit_error_count} consecutive 429 errors. Pausing for 5 minutes.")
            self.logger.error("   This indicates persistent rate limiting. Please:")
            self.logger.error("   1. Wait 5-10 minutes before retrying")
            self.logger.error("   2. Reduce request size (shorter prompts, lower max_tokens)")
            self.logger.error("   3. Process in smaller batches")
            return True
        return False
    
    def _reset_rate_limit_counter(self):
        """Reset rate limit error counter on successful request"""
        if self._rate_limit_error_count > 0:
            self.logger.info(f"✓ Request succeeded. Resetting rate limit counter (was: {self._rate_limit_error_count})")
        self._rate_limit_error_count = 0
        self._last_rate_limit_time = None
    
    def _retry_with_backoff(self, func, max_retries: int = 3, initial_delay: float = 60.0, 
                           max_delay: float = 180.0, backoff_factor: float = 1.5) -> Optional[Any]:
        """
        Retry a function with exponential backoff for rate limit errors
        Uses longer delays to avoid persistent 429 errors
        
        Args:
            func: Function to retry (should return result or raise exception)
            max_retries: Maximum number of retry attempts
            initial_delay: Initial delay in seconds (default: 60s)
            max_delay: Maximum delay in seconds (default: 180s)
            backoff_factor: Multiplier for delay between retries (default: 1.5x)
            
        Returns:
            Function result or None if all retries failed
        """
        import time
        
        delay = initial_delay
        for attempt in range(max_retries):
            try:
                result = func()
                # Reset counter on success
                self._reset_rate_limit_counter()
                return result
            except Exception as e:
                if not self._is_rate_limit_error(e):
                    # Not a rate limit error, re-raise
                    raise
                
                # Increment rate limit counter
                self._rate_limit_error_count += 1
                self._last_rate_limit_time = time.time()
                
                # Extract complete error details
                error_details = self._extract_error_details(e)
                
                # Log complete error information
                self.logger.error("=" * 80)
                self.logger.error("🔴 RATE LIMIT ERROR (429) - COMPLETE DETAILS")
                self.logger.error("=" * 80)
                self.logger.error(f"Error Type: {error_details['error_type']}")
                self.logger.error(f"Status Code: {error_details['status_code']}")
                self.logger.error(f"Error Message: {error_details['error_message']}")
                
                # Log concurrency information
                self.logger.error("")
                self.logger.error("📊 CONCURRENCY INFORMATION:")
                self.logger.error(f"  Process ID (PID): {error_details['concurrency_info']['pid']}")
                self.logger.error(f"  Process Name: {error_details['concurrency_info']['process_name']}")
                self.logger.error(f"  Thread ID: {error_details['concurrency_info']['thread_id']}")
                self.logger.error(f"  Thread Name: {error_details['concurrency_info']['thread_name']}")
                self.logger.error(f"  Worker ID: {error_details['concurrency_info']['worker_id']}")
                
                # Log headers
                if error_details['headers']:
                    self.logger.error("")
                    self.logger.error("📋 HTTP RESPONSE HEADERS:")
                    for header_name, header_value in error_details['headers'].items():
                        # Sanitize sensitive headers
                        if 'authorization' in header_name.lower() or 'api-key' in header_name.lower():
                            header_value = "***REDACTED***"
                        self.logger.error(f"  {header_name}: {header_value}")
                
                # Log Retry-After if available
                if error_details['retry_after']:
                    self.logger.error("")
                    self.logger.error(f"⏰ RETRY-AFTER HEADER: {error_details['retry_after']} seconds")
                    # Use Retry-After if it's longer than our calculated delay
                    if isinstance(error_details['retry_after'], (int, float)):
                        retry_after_seconds = float(error_details['retry_after'])
                        if retry_after_seconds > delay:
                            self.logger.info(f"   Using Retry-After value ({retry_after_seconds}s) instead of calculated delay ({delay}s)")
                            delay = retry_after_seconds
                
                # Log error body
                if error_details['error_body']:
                    self.logger.error("")
                    self.logger.error("📄 ERROR BODY (Complete):")
                    # Sanitize API keys from error body
                    sanitized_body = APIKeyManager.sanitize_error_message(error_details['error_body'])
                    # Log in chunks if too long
                    if len(sanitized_body) > 1000:
                        self.logger.error(f"  (First 500 chars): {sanitized_body[:500]}...")
                        self.logger.error(f"  (Last 500 chars): ...{sanitized_body[-500:]}")
                        self.logger.error(f"  (Full length: {len(sanitized_body)} chars)")
                    else:
                        self.logger.error(f"  {sanitized_body}")
                
                # Log full error representation
                self.logger.error("")
                self.logger.error("🔍 FULL ERROR REPRESENTATION:")
                error_repr_sanitized = APIKeyManager.sanitize_error_message(error_details['error_repr'])
                if len(error_repr_sanitized) > 1000:
                    self.logger.error(f"  (First 500 chars): {error_repr_sanitized[:500]}...")
                    self.logger.error(f"  (Last 500 chars): ...{error_repr_sanitized[-500:]}")
                else:
                    self.logger.error(f"  {error_repr_sanitized}")
                
                self.logger.error("=" * 80)
                
                # Check for hard stop
                if self._check_hard_stop():
                    import time as time_module
                    self.logger.error("Pausing for 5 minutes due to persistent rate limiting...")
                    time_module.sleep(300)  # 5 minutes
                    self._rate_limit_error_count = 0  # Reset after pause
                    raise Exception("Hard stop: Too many consecutive 429 errors. Please wait and retry with smaller requests.")
                
                if attempt < max_retries - 1:
                    error_msg = APIKeyManager.sanitize_error_message(str(e))
                    self.logger.warning(f"⚠️ Rate limit hit (429) - Attempt {attempt + 1}/{max_retries}")
                    self.logger.warning(f"   Waiting {delay:.1f}s before retry... (Error count: {self._rate_limit_error_count}/{self._max_rate_limit_errors})")
                    if error_details['retry_after']:
                        self.logger.warning(f"   Retry-After header suggests: {error_details['retry_after']}s")
                    time.sleep(delay)
                    delay = min(delay * backoff_factor, max_delay)
                else:
                    # Last attempt failed
                    raise
    
    def _retry_with_next_key(self, pdf_path: str, prompt: str, model_name: str, 
                             temperature: float, max_tokens: int, generation_config,
                             content_parts: List, json_instruction: str,
                             max_retries: int = 3) -> Optional[str]:
        """
        Retry request with next API key when quota is exhausted
        
        Args:
            pdf_path: Path to PDF file
            prompt: Original prompt
            model_name: Model name
            temperature: Temperature
            max_tokens: Max tokens
            generation_config: Generation config
            content_parts: Content parts (contains PDF file reference)
            json_instruction: JSON instruction
            max_retries: Maximum number of keys to try
            
        Returns:
            Response text or None if all keys exhausted
        """
        retry_count = 0
        
        # Extract PDF file from content_parts if available
        pdf_file_ref = None
        if len(content_parts) > 1:
            pdf_file_ref = content_parts[1]
        
        while retry_count < max_retries and retry_count < len(self.key_manager.api_keys):
            retry_count += 1
            next_key = self.key_manager.get_next_key()
            
            if not next_key:
                self.logger.error("No more API keys available for rotation")
                return None
            
            next_key_info = self.key_manager.get_current_key_info()
            self.logger.info(f"Retrying with API key {retry_count}/{max_retries}: {next_key_info.get('account', 'Unknown')}")
            
            try:
                # Reinitialize with new key
                genai.configure(api_key=next_key)
                model = genai.GenerativeModel(model_name)
                
                # Recreate content parts with original prompt + JSON instruction
                full_prompt = prompt + json_instruction
                
                # If we have PDF file reference, reuse it; otherwise upload again
                if pdf_file_ref and hasattr(pdf_file_ref, 'name'):
                    # Reuse existing PDF file
                    try:
                        pdf_file = genai.get_file(pdf_file_ref.name)
                        content_parts_retry = [full_prompt, pdf_file]
                    except:
                        # If file not found, upload again
                        pdf_file = genai.upload_file(path=pdf_path)
                        import time
                        while pdf_file.state.name == "PROCESSING":
                            time.sleep(2)
                            pdf_file = genai.get_file(pdf_file.name)
                        content_parts_retry = [full_prompt, pdf_file]
                else:
                    # Upload PDF again
                    pdf_file = genai.upload_file(path=pdf_path)
                    import time
                    while pdf_file.state.name == "PROCESSING":
                        time.sleep(2)
                        pdf_file = genai.get_file(pdf_file.name)
                    content_parts_retry = [full_prompt, pdf_file]
                
                # Determine if streaming should be used (same logic as main request)
                use_streaming = self._should_use_streaming(len(full_prompt), generation_config.max_output_tokens)
                
                # Make single request (no nested retries - backoff already handled in main function)
                # Add delay before trying new key to avoid rapid retries
                import time
                if retry_count > 1:
                    delay_before_key = 15 * retry_count  # 15s, 30s, 45s delays
                    self.logger.info(f"Waiting {delay_before_key}s before trying key {retry_count}...")
                    time.sleep(delay_before_key)
                
                try:
                    if use_streaming:
                        response_stream = model.generate_content(
                            content_parts_retry,
                            generation_config=generation_config,
                            stream=True
                        )
                        
                        response_text_parts = []
                        for chunk in response_stream:
                            if hasattr(chunk, 'text') and chunk.text:
                                response_text_parts.append(chunk.text)
                        
                        if response_text_parts:
                            full_response = ''.join(response_text_parts)
                        else:
                            full_response = None
                    else:
                        # Non-streaming for large requests
                        response = model.generate_content(
                            content_parts_retry,
                            generation_config=generation_config
                        )
                        full_response = response.text if hasattr(response, 'text') and response.text else None
                    
                    if full_response:
                        self.logger.info(f"✓ Successfully retried with new API key: {next_key_info.get('account', 'Unknown')}")
                        # Reset rate limit counter on success
                        self._reset_rate_limit_counter()
                        # Clean up uploaded file
                        try:
                            if hasattr(pdf_file, 'name'):
                                genai.delete_file(pdf_file.name)
                        except:
                            pass
                        return full_response
                    else:
                        self.logger.warning(f"API key {next_key_info.get('account', 'Unknown')} returned empty response, trying next...")
                        continue
                        
                except Exception as key_error:
                    if self._is_quota_error(key_error):
                        error_str_key = str(key_error)
                        if '403' in error_str_key or 'leaked' in error_str_key.lower():
                            self.logger.warning(f"API key {next_key_info.get('account', 'Unknown')} also leaked (403), trying next...")
                        else:
                            if self._is_rate_limit_error(key_error):
                                self.logger.warning(f"API key {next_key_info.get('account', 'Unknown')} hit rate limit (429), trying next...")
                            else:
                                self.logger.warning(f"API key {next_key_info.get('account', 'Unknown')} also exhausted, trying next...")
                        # Continue to next key
                        continue
                    else:
                        # Non-quota error, log and continue to next key
                        error_msg = APIKeyManager.sanitize_error_message(str(key_error))
                        self.logger.warning(f"Error with API key {next_key_info.get('account', 'Unknown')}: {error_msg}, trying next...")
                        continue
                        
            except Exception as e:
                if self._is_quota_error(e):
                    error_str_retry_final = str(e)
                    if '403' in error_str_retry_final or 'leaked' in error_str_retry_final.lower():
                        self.logger.warning(f"API key {next_key_info.get('account', 'Unknown')} also leaked (403), trying next...")
                    else:
                        self.logger.warning(f"API key {next_key_info.get('account', 'Unknown')} also exhausted, trying next...")
                    continue
                else:
                    # Sanitize error message to prevent API key leakage
                    error_msg = APIKeyManager.sanitize_error_message(str(e))
                    self.logger.error(f"Error with API key {next_key_info.get('account', 'Unknown')}: {error_msg}")
                    # Continue to next key for other errors too
                    continue
        
        # All keys failed - log concise error with actionable steps
        self.logger.error("=" * 70)
        self.logger.error(f"❌ All {retry_count} API keys failed. Cannot process request.")
        self.logger.error("=" * 70)
        self.logger.error("")
        self.logger.error("Possible reasons:")
        self.logger.error("  • All keys LEAKED (403) → Remove leaked keys, add new ones")
        self.logger.error("  • All keys QUOTA EXHAUSTED (429) → Wait for reset or add more keys")
        self.logger.error("  • Keys INVALID/EXPIRED → Generate new keys from Google AI Studio")
        self.logger.error("")
        self.logger.error("Quick fix:")
        self.logger.error("  1. Open API keys CSV file")
        self.logger.error("  2. Remove leaked/invalid keys")
        self.logger.error("  3. Add new keys from: https://aistudio.google.com/apikey")
        self.logger.error("  4. Save CSV and reload in application")
        self.logger.error("  5. Try again")
        self.logger.error("")
        self.logger.error("=" * 70)
        return None
    
    def _process_single_batch(self, model, pdf_file, batch_prompt: str, 
                             max_tokens: int, start_page: int, end_page: int,
                             progress_callback: Optional[Callable[[str], None]] = None) -> Optional[List[Dict[str, Any]]]:
        """Process a single batch and return JSON data"""
        try:
            content_parts = [batch_prompt, pdf_file]
            
            generation_config = genai.types.GenerationConfig(
                temperature=0.7,
                max_output_tokens=max_tokens,
            )
            
            # Use streaming for complete responses
            response_text_parts = []
            finish_reason = None
            
            try:
                response_stream = model.generate_content(
                    content_parts,
                    generation_config=generation_config,
                    stream=True
                )
                
                # Collect chunks and track finish reason
                for chunk in response_stream:
                    if hasattr(chunk, 'text') and chunk.text:
                        response_text_parts.append(chunk.text)
                    
                    # Check for finish reason
                    if hasattr(chunk, 'candidates') and chunk.candidates:
                        for candidate in chunk.candidates:
                            if hasattr(candidate, 'finish_reason') and candidate.finish_reason:
                                finish_reason = candidate.finish_reason
                
                if response_text_parts:
                    full_response = ''.join(response_text_parts)
                    # Log finish reason
                    if finish_reason:
                        if isinstance(finish_reason, int) and finish_reason == 2:
                            self.logger.warning(f"⚠️ Batch pages {start_page}-{end_page}: Response may be truncated (MAX_TOKENS)")
                            if progress_callback:
                                progress_callback(f"⚠️ Warning: Batch {start_page}-{end_page} may be truncated")
                        elif isinstance(finish_reason, int) and finish_reason == 1:
                            self.logger.info(f"✓ Batch pages {start_page}-{end_page}: Completed normally")
                else:
                    # Fallback to non-streaming
                    response = model.generate_content(content_parts, generation_config=generation_config)
                    full_response = response.text if hasattr(response, 'text') and response.text else ""
                    
                    # Check finish reason in non-streaming response
                    if hasattr(response, 'candidates') and response.candidates:
                        for candidate in response.candidates:
                            if hasattr(candidate, 'finish_reason') and candidate.finish_reason:
                                finish_reason = candidate.finish_reason
                                if isinstance(finish_reason, int) and finish_reason == 2:
                                    self.logger.warning(f"⚠️ Batch pages {start_page}-{end_page}: Response truncated")
                
            except Exception as stream_error:
                error_str = str(stream_error)
                # Check if it's a 429 quota error or 403 leaked key error
                if self._is_quota_error(stream_error):
                    if '403' in error_str or 'leaked' in error_str.lower():
                        self.logger.warning(f"⚠️ API key leaked (403) in batch processing. Will try next key in next batch.")
                    else:
                        self.logger.warning(f"⚠️ Quota exhausted (429) in batch processing. Will try next key in next batch.")
                    # Return None to skip this batch, next batch will use next key
                    return None
                
                self.logger.warning(f"Streaming failed, using non-streaming: {error_str}")
                try:
                    response = model.generate_content(content_parts, generation_config=generation_config)
                    full_response = response.text if hasattr(response, 'text') and response.text else ""
                except Exception as non_stream_error:
                    if self._is_quota_error(non_stream_error):
                        error_str_non_stream = str(non_stream_error)
                        if '403' in error_str_non_stream or 'leaked' in error_str_non_stream.lower():
                            self.logger.warning(f"⚠️ API key leaked (403) in batch processing. Will try next key in next batch.")
                        else:
                            self.logger.warning(f"⚠️ Quota exhausted (429) in batch processing. Will try next key in next batch.")
                        return None
                    raise
            
            # Extract JSON from response
            json_data = self._extract_json_from_response(full_response)
            
            if json_data:
                self.logger.info(f"Batch pages {start_page}-{end_page}: Extracted {len(json_data)} rows")
                return json_data
            else:
                self.logger.warning(f"Batch pages {start_page}-{end_page}: No JSON data extracted")
                return None
                
        except Exception as e:
            self.logger.error(f"Error processing batch pages {start_page}-{end_page}: {str(e)}")
            return None
    
    def _extract_json_from_response(self, response_text: str) -> Optional[List[Dict[str, Any]]]:
        """
        Extract JSON array from response text
        
        Args:
            response_text: Response text that may contain JSON
            
        Returns:
            List of dictionaries (JSON objects) or None if extraction fails
        """
        if not response_text:
            return None
        
        # Remove code blocks if present
        cleaned = self.extract_from_code_block(response_text)
        
        # Try to find JSON array
        try:
            # Try parsing as direct JSON
            data = json.loads(cleaned)
            if isinstance(data, list):
                self.logger.info(f"Successfully parsed JSON array with {len(data)} items")
                return data
            elif isinstance(data, dict):
                # Check if there's a data/rows/items key
                for key in ['data', 'rows', 'items', 'results']:
                    if key in data and isinstance(data[key], list):
                        self.logger.info(f"Found JSON array in '{key}' key with {len(data[key])} items")
                        return data[key]
                # If it's a single object, wrap in list
                self.logger.info("Single JSON object found, wrapping in list")
                return [data]
        except json.JSONDecodeError as e:
            # Try to find JSON in the text using regex
            import re
            self.logger.debug(f"Direct JSON parse failed: {str(e)}, trying regex extraction...")
            
            # Look for JSON array pattern (more flexible)
            json_match = re.search(r'\[[\s\S]*\]', cleaned)
            if json_match:
                try:
                    data = json.loads(json_match.group())
                    if isinstance(data, list):
                        self.logger.info(f"Successfully extracted JSON array using regex with {len(data)} items")
                        return data
                except json.JSONDecodeError as e2:
                    self.logger.warning(f"Regex-extracted JSON also failed to parse: {str(e2)}")
            
            # Try to fix common JSON issues (incomplete JSON)
            # Check if JSON might be truncated (missing closing bracket)
            if cleaned.strip().startswith('[') and not cleaned.strip().endswith(']'):
                self.logger.warning("JSON appears to be truncated (missing closing bracket)")
                # Try to add closing bracket
                try:
                    fixed_json = cleaned.strip() + ']'
                    data = json.loads(fixed_json)
                    if isinstance(data, list):
                        self.logger.warning("Fixed truncated JSON by adding closing bracket")
                        return data
                except:
                    pass
        
        self.logger.warning("Could not extract valid JSON from response")
        return None
    
    def _convert_json_rows_to_csv(self, json_rows: List[Dict[str, Any]], delimiter: str = ";;;") -> str:
        """
        Convert JSON rows to CSV format with specified delimiter
        
        Args:
            json_rows: List of dictionaries (JSON objects)
            delimiter: CSV delimiter (default: ";;;")
            
        Returns:
            CSV string
        """
        if not json_rows:
            return ""
        
        # Get headers from first row
        headers = list(json_rows[0].keys())
        
        # Build CSV with header first
        csv_lines = [delimiter.join(headers)]
        
        # Add data rows
        for row in json_rows:
            csv_line = delimiter.join(str(row.get(h, "")) for h in headers)
            csv_lines.append(csv_line)
        
        return "\n".join(csv_lines)
    
    def _save_json_to_current_directory(self, csv_path: Optional[str] = None, json_content: str = "") -> Optional[str]:
        """
        Save JSON content to current directory
        
        Args:
            csv_path: Optional path to PDF file (used for naming)
            json_content: JSON content to save
            
        Returns:
            Path to saved JSON file or None if failed
        """
        try:
            from datetime import datetime
            
            # Get current directory
            current_dir = os.getcwd()
            
            # Generate filename
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            
            if csv_path:
                pdf_name = os.path.splitext(os.path.basename(csv_path))[0]
                filename = f"{pdf_name}_output_{timestamp}.json"
            else:
                filename = f"output_{timestamp}.json"
            
            file_path = os.path.join(current_dir, filename)
            
            # Save JSON file
            with open(file_path, 'w', encoding='utf-8') as f:
                f.write(json_content)
            
            self.logger.info(f"JSON saved to: {file_path}")
            return file_path
            
        except Exception as e:
            self.logger.error(f"Error saving JSON to current directory: {str(e)}")
            return None
    
    def process_pdf_with_prompt(self,
                                pdf_path: str,
                                prompt: str,
                                model_name: str = APIConfig.DEFAULT_TEXT_MODEL,
                                temperature: float = APIConfig.DEFAULT_TEMPERATURE,
                                max_tokens: int = APIConfig.DEFAULT_MAX_TOKENS,
                                api_key: Optional[str] = None,
                                return_json: bool = False,
                                force_no_streaming: bool = False) -> Optional[str]:
        """
        Process PDF file with a prompt using Gemini API
        
        Args:
            pdf_path: Path to PDF file
            prompt: Prompt/instruction for processing the PDF
            model_name: Gemini model to use
            temperature: Temperature for generation (0.0-2.0)
            max_tokens: Maximum output tokens
            api_key: Optional API key. If None, uses next key from manager.
            return_json: Whether to extract and return JSON (default: False)
            force_no_streaming: Force disable streaming (for multi-part processing)
            
        Returns:
            Response text or None if failed
        """
        if not GENERATIVEAI_AVAILABLE:
            self.logger.error("google.generativeai library not available")
            return None
        
        if not os.path.exists(pdf_path):
            self.logger.error(f"PDF file not found: {pdf_path}")
            return None
        
        try:
            # Initialize client if needed with retry logic for quota errors
            key = api_key or self.key_manager.get_next_key()
            if not key:
                self.logger.error("No API key available")
                return None
            
            # Try to upload PDF with current key, retry with next key if quota exhausted
            pdf_file = None
            upload_retries = 0
            max_upload_retries = min(3, len(self.key_manager.api_keys))
            
            while upload_retries < max_upload_retries and pdf_file is None:
                try:
                    current_key = api_key or self.key_manager.get_next_key()
                    if not current_key:
                        self.logger.error("No API key available")
                        return None
                    
                    genai.configure(api_key=current_key)
                    pdf_file = genai.upload_file(path=pdf_path)
                    self.logger.info(f"PDF uploaded successfully with API key: {self.key_manager.get_current_key_info().get('account', 'Unknown')}")
                    break
                except Exception as upload_error:
                    if self._is_quota_error(upload_error):
                        upload_retries += 1
                        error_str_upload = str(upload_error)
                        if '403' in error_str_upload or 'leaked' in error_str_upload.lower():
                            self.logger.warning(f"⚠️ API key leaked (403) during PDF upload (attempt {upload_retries}/{max_upload_retries}), trying next key...")
                        else:
                            self.logger.warning(f"⚠️ Quota exhausted during PDF upload (attempt {upload_retries}/{max_upload_retries}), trying next key...")
                        if upload_retries >= max_upload_retries:
                            self.logger.error("❌ All API keys exhausted or invalid during PDF upload")
                            return None
                        continue
                    else:
                        raise
            
            if pdf_file is None:
                self.logger.error("Failed to upload PDF file")
                return None
            
            model = genai.GenerativeModel(model_name)
            self.logger.info(f"Uploaded PDF file: {os.path.basename(pdf_path)}")
            
            # Wait for file to be processed
            import time
            while pdf_file.state.name == "PROCESSING":
                self.logger.info("Waiting for PDF to be processed...")
                time.sleep(2)
                pdf_file = genai.get_file(pdf_file.name)
            
            if pdf_file.state.name == "FAILED":
                self.logger.error("PDF file processing failed")
                return None
            
            # Generate content with PDF and prompt
            # Use Part objects to ensure prompt is sent completely
            # Increase max_output_tokens for longer responses
            # Model-specific maximum tokens:
            # gemini-2.5-pro: up to 32768 tokens
            # gemini-2.5-flash: up to 32768 tokens
            # gemini-2.0-flash: up to 32768 tokens
            # gemini-1.5-pro: up to 8192 tokens
            # gemini-1.5-flash: up to 8192 tokens
            
            # Determine maximum tokens based on model
            if '2.5' in model_name or '2.0' in model_name:
                # Newer models support up to 32768 tokens
                model_max_tokens = 32768
            elif '1.5' in model_name:
                # Older models support up to 8192 tokens
                model_max_tokens = 8192
            else:
                # Default to maximum for safety
                model_max_tokens = 32768
            
            # Use the maximum available for the model, but respect user's max_tokens if lower
            effective_max_tokens = min(max(max_tokens, model_max_tokens), model_max_tokens)
            
            self.logger.info(f"Model: {model_name}, Max tokens for model: {model_max_tokens}, Using: {effective_max_tokens}")
            
            generation_config = genai.types.GenerationConfig(
                temperature=temperature,
                max_output_tokens=effective_max_tokens,
            )
            
            self.logger.info(f"Using max_output_tokens: {effective_max_tokens}")
            
            # Log full prompt for debugging
            self.logger.info(f"Using model: {model_name}")
            self.logger.info(f"Sending prompt (length: {len(prompt)} characters)")
            self.logger.info(f"Max output tokens: {generation_config.max_output_tokens}")
            self.logger.debug(f"Prompt content (first 500 chars): {prompt[:500]}...")
            self.logger.debug(f"Prompt content (last 500 chars): ...{prompt[-500:]}")
            
            # Use prompt as-is (user's prompt contains all necessary instructions)
            # No additional instructions added - user controls the prompt completely
            full_prompt = prompt
            
            content_parts = [
                full_prompt,  # User's original prompt (no modifications)
                pdf_file  # PDF file
            ]
            
            if force_no_streaming:
                self.logger.info("Multi-part mode: Using user's prompt as-is")
            else:
                self.logger.info("Using user's prompt as-is (no modifications)")
            
            # Determine if streaming should be used based on request size
            # Large requests (long prompts or high max_tokens) should use non-streaming
            # to avoid burst requests that trigger rate limits
            # force_no_streaming overrides this (for multi-part processing)
            if force_no_streaming:
                use_streaming = False
                self.logger.info("Streaming disabled (force_no_streaming=True for multi-part processing)")
            else:
                use_streaming = self._should_use_streaming(len(full_prompt), effective_max_tokens)
            
            if use_streaming:
                self.logger.info("Generating content with streaming enabled for complete response...")
            else:
                self.logger.info("Generating content with NON-STREAMING (large request detected to avoid rate limits)...")
            
            # Define request function for retry with backoff
            def _make_request():
                if use_streaming:
                    # Streaming request
                    response_stream = model.generate_content(
                        content_parts,
                        generation_config=generation_config,
                        stream=True
                    )
                    
                    response_text_parts = []
                    finish_reason = None
                    chunk_count = 0
                    
                    for chunk in response_stream:
                        chunk_count += 1
                        if hasattr(chunk, 'text') and chunk.text:
                            response_text_parts.append(chunk.text)
                            if chunk_count % 10 == 0:  # Log every 10 chunks
                                self.logger.debug(f"Received {chunk_count} chunks, {len(''.join(response_text_parts))} chars so far")
                        
                        # Check for finish reason in chunk
                        if hasattr(chunk, 'candidates') and chunk.candidates:
                            for candidate in chunk.candidates:
                                if hasattr(candidate, 'finish_reason') and candidate.finish_reason:
                                    finish_reason = candidate.finish_reason
                    
                    if response_text_parts:
                        full_response_text = ''.join(response_text_parts)
                        # Create a response-like object for compatibility
                        class StreamedResponse:
                            def __init__(self, text, finish_reason=None):
                                self.text = text
                                self.candidates = []
                                self._finish_reason = finish_reason
                            
                            def get_finish_reason(self):
                                return self._finish_reason
                        
                        return StreamedResponse(full_response_text, finish_reason)
                    return None
                else:
                    # Non-streaming request (for large requests)
                    return model.generate_content(
                        content_parts,
                        generation_config=generation_config
                    )
            
            try:
                # Try request with retry and backoff for rate limits
                # This will automatically retry with exponential backoff if rate limited
                response = self._retry_with_backoff(
                    _make_request,
                    max_retries=3,
                    initial_delay=60.0,  # Start with 60s delay
                    max_delay=180.0,     # Max 180s (3 minutes)
                    backoff_factor=1.5    # 60s → 90s → 135s → 180s
                )
                
                if response:
                    response_text = response.text if hasattr(response, 'text') else None
                    if response_text:
                        self.logger.info(f"✓ Request completed: {len(response_text)} characters received")
                        
                        # Get finish reason from response
                        finish_reason = None
                        if hasattr(response, 'get_finish_reason'):
                            finish_reason = response.get_finish_reason()
                        elif hasattr(response, 'candidates') and response.candidates:
                            for candidate in response.candidates:
                                if hasattr(candidate, 'finish_reason') and candidate.finish_reason:
                                    finish_reason = candidate.finish_reason
                        
                        # Check finish reason
                        if finish_reason:
                            finish_reason_str = str(finish_reason)
                            if isinstance(finish_reason, int):
                                reason_map = {
                                    0: "UNSPECIFIED",
                                    1: "STOP (normal completion)",
                                    2: "MAX_TOKENS (truncated!)",
                                    3: "SAFETY (blocked)",
                                    4: "RECITATION (blocked)",
                                    5: "OTHER"
                                }
                                finish_reason_str = reason_map.get(finish_reason, f"UNKNOWN ({finish_reason})")
                            
                            self.logger.info(f"Finish reason: {finish_reason_str}")
                            if finish_reason == 2 or (isinstance(finish_reason, str) and 'MAX_TOKENS' in str(finish_reason).upper()):
                                self.logger.warning(f"⚠️ Response may be truncated (finish_reason: {finish_reason_str})")
                                self._response_truncated = True
                            else:
                                self.logger.info("✓ Response completed normally")
                                self._response_truncated = False
                        
                        # Reset rate limit counter on success
                        self._reset_rate_limit_counter()
                        
                        # Extract text from response
                        if hasattr(response, 'text'):
                            cleaned_response = response.text
                        else:
                            cleaned_response = response_text
                    else:
                        self.logger.warning("Request returned no text")
                        cleaned_response = None
                else:
                    self.logger.warning("Request returned no response")
                    cleaned_response = None
            except Exception as request_error:
                error_str = str(request_error)
                
                # Extract complete error details for logging
                error_details = self._extract_error_details(request_error)
                
                # Log error details (especially for 429 errors)
                if self._is_quota_error(request_error) or self._is_rate_limit_error(request_error):
                    self.logger.error("=" * 80)
                    self.logger.error("🔴 API ERROR - COMPLETE DETAILS")
                    self.logger.error("=" * 80)
                    self.logger.error(f"Error Type: {error_details['error_type']}")
                    self.logger.error(f"Status Code: {error_details['status_code']}")
                    self.logger.error(f"Error Message: {error_details['error_message']}")
                    
                    # Log concurrency information
                    self.logger.error("")
                    self.logger.error("📊 CONCURRENCY INFORMATION:")
                    self.logger.error(f"  Process ID (PID): {error_details['concurrency_info']['pid']}")
                    self.logger.error(f"  Process Name: {error_details['concurrency_info']['process_name']}")
                    self.logger.error(f"  Thread ID: {error_details['concurrency_info']['thread_id']}")
                    self.logger.error(f"  Thread Name: {error_details['concurrency_info']['thread_name']}")
                    self.logger.error(f"  Worker ID: {error_details['concurrency_info']['worker_id']}")
                    
                    # Log headers
                    if error_details['headers']:
                        self.logger.error("")
                        self.logger.error("📋 HTTP RESPONSE HEADERS:")
                        for header_name, header_value in error_details['headers'].items():
                            if 'authorization' in header_name.lower() or 'api-key' in header_name.lower():
                                header_value = "***REDACTED***"
                            self.logger.error(f"  {header_name}: {header_value}")
                    
                    # Log Retry-After if available
                    if error_details['retry_after']:
                        self.logger.error("")
                        self.logger.error(f"⏰ RETRY-AFTER HEADER: {error_details['retry_after']} seconds")
                    
                    # Log error body
                    if error_details['error_body']:
                        self.logger.error("")
                        self.logger.error("📄 ERROR BODY (Complete):")
                        sanitized_body = APIKeyManager.sanitize_error_message(error_details['error_body'])
                        if len(sanitized_body) > 1000:
                            self.logger.error(f"  (First 500 chars): {sanitized_body[:500]}...")
                            self.logger.error(f"  (Last 500 chars): ...{sanitized_body[-500:]}")
                        else:
                            self.logger.error(f"  {sanitized_body}")
                    
                    self.logger.error("=" * 80)
                
                # Check if it's a 429 rate limit error - if so, backoff already handled it
                # Only switch keys if it's 403 (leaked) or if backoff exhausted
                if self._is_quota_error(request_error):
                    if '403' in error_str or 'leaked' in error_str.lower():
                        self.logger.warning(f"⚠️ API key leaked (403) or invalid. Attempting to switch to next key...")
                        # Add delay before switching keys to avoid rapid retries
                        import time
                        time.sleep(10)  # 10 second delay before key rotation
                        return self._retry_with_next_key(
                            pdf_path, prompt, model_name, temperature, max_tokens, 
                            generation_config, content_parts, json_instruction
                        )
                    elif self._is_rate_limit_error(request_error):
                        # Rate limit error - backoff should have handled it
                        # If we're here, backoff exhausted all retries
                        self.logger.error(f"⚠️ Rate limit (429) persisted after all backoff retries. Attempting to switch to next key...")
                        # Use Retry-After if available, otherwise use default delay
                        import time
                        delay_before_key = 30
                        if error_details['retry_after'] and isinstance(error_details['retry_after'], (int, float)):
                            delay_before_key = max(30, int(error_details['retry_after']))
                            self.logger.info(f"   Using Retry-After value: {delay_before_key}s")
                        time.sleep(delay_before_key)
                        return self._retry_with_next_key(
                            pdf_path, prompt, model_name, temperature, max_tokens, 
                            generation_config, content_parts, json_instruction
                        )
                    else:
                        # Other quota error
                        self.logger.warning(f"⚠️ Quota exhausted (429) with current API key. Attempting to switch to next key...")
                        import time
                        time.sleep(10)  # 10 second delay before key rotation
                        return self._retry_with_next_key(
                            pdf_path, prompt, model_name, temperature, max_tokens, 
                            generation_config, content_parts, json_instruction
                        )
                else:
                    # Non-quota error, re-raise
                    raise
            
            # Return cleaned_response if available
            if 'cleaned_response' in locals() and cleaned_response:
                return cleaned_response
            elif 'cleaned_response' in locals() and cleaned_response is None:
                self.logger.warning("No response received")
                return None
            
            # Log response info (fallback for old code path)
            if response:
                response_length = len(response.text) if response.text else 0
                self.logger.info(f"Response received (length: {response_length} characters)")
                
                # Check if response might be truncated
                if response_length > 0:
                    # Check if response ends abruptly (common signs of truncation)
                    if response.text and not response.text.rstrip().endswith(('.', '!', '?', ':', '\n')):
                        self.logger.warning("Response might be truncated - checking for finish reason")
                    
                    # Log finish reason if available
                    if hasattr(response, 'candidates') and response.candidates:
                        for i, candidate in enumerate(response.candidates):
                            finish_reason = getattr(candidate, 'finish_reason', None)
                            if finish_reason is not None:
                                # finish_reason can be an enum or integer
                                # 0 = FINISH_REASON_UNSPECIFIED
                                # 1 = STOP (normal completion)
                                # 2 = MAX_TOKENS (truncated)
                                # 3 = SAFETY (blocked)
                                # 4 = RECITATION (blocked)
                                # 5 = OTHER
                                
                                finish_reason_str = str(finish_reason)
                                if isinstance(finish_reason, int):
                                    reason_map = {
                                        0: "UNSPECIFIED",
                                        1: "STOP (normal completion)",
                                        2: "MAX_TOKENS (truncated!)",
                                        3: "SAFETY (blocked)",
                                        4: "RECITATION (blocked)",
                                        5: "OTHER"
                                    }
                                    finish_reason_str = reason_map.get(finish_reason, f"UNKNOWN ({finish_reason})")
                                
                                self.logger.info(f"Candidate {i} finish reason: {finish_reason_str}")
                                
                                if finish_reason == 2 or (isinstance(finish_reason, str) and 'MAX_TOKENS' in str(finish_reason).upper()):
                                    self.logger.error("❌ CRITICAL: Response was TRUNCATED due to MAX_TOKENS limit!")
                                    self.logger.error(f"Current max_output_tokens: {generation_config.max_output_tokens}")
                                    self.logger.error("The response is INCOMPLETE! Consider:")
                                    self.logger.error("  1. Using gemini-2.5-pro model (better for long responses)")
                                    self.logger.error("  2. Breaking the task into smaller parts")
                                    self.logger.error("  3. Simplifying the prompt to get shorter responses")
                                    # Store truncation flag for later use
                                    self._response_truncated = True
                                elif finish_reason == 1 or (isinstance(finish_reason, str) and 'STOP' in str(finish_reason).upper()):
                                    self.logger.info("✓ Response completed normally (STOP)")
                                    self._response_truncated = False
            
            # Clean up uploaded file
            try:
                genai.delete_file(pdf_file.name)
            except:
                pass
            
            if response:
                # Extract full response text
                response_text = None
                
                # Method 1: Direct text attribute
                if hasattr(response, 'text') and response.text:
                    response_text = response.text
                    self.logger.info(f"Extracted response from response.text ({len(response_text)} chars)")
                
                # Method 2: From candidates
                if not response_text and hasattr(response, 'candidates') and response.candidates:
                    all_text_parts = []
                    for candidate in response.candidates:
                        if hasattr(candidate, 'content') and hasattr(candidate.content, 'parts'):
                            for part in candidate.content.parts:
                                if hasattr(part, 'text') and part.text:
                                    all_text_parts.append(part.text)
                    
                    if all_text_parts:
                        response_text = '\n'.join(all_text_parts)
                        self.logger.info(f"Extracted response from candidates.parts ({len(response_text)} chars)")
                
                # Method 3: Try to get full response using response.resolve()
                if not response_text:
                    try:
                        response.resolve()
                        if hasattr(response, 'text') and response.text:
                            response_text = response.text
                            self.logger.info(f"Extracted response after resolve() ({len(response_text)} chars)")
                    except Exception as e:
                        self.logger.warning(f"Could not resolve response: {str(e)}")
                
                if response_text:
                    # Extract from code blocks if present
                    cleaned_response = self.extract_from_code_block(response_text)
                    
                    # Log full response for debugging (first and last parts)
                    self.logger.info(f"PDF processed successfully with {model_name}")
                    self.logger.info(f"Full response length: {len(response_text)} characters")
                    if cleaned_response != response_text:
                        self.logger.info(f"Extracted from code block: {len(cleaned_response)} characters")
                    self.logger.info(f"Response preview (first 500 chars): {cleaned_response[:500]}...")
                    self.logger.info(f"Response preview (last 500 chars): ...{cleaned_response[-500:]}")
                    
                    # If return_json is True, try to extract and return JSON
                    if return_json:
                        json_data = self._extract_json_from_response(cleaned_response)
                        if json_data:
                            # Return JSON as string
                            return json.dumps(json_data, ensure_ascii=False, indent=2)
                        else:
                            self.logger.warning("Could not extract JSON from response, returning raw text")
                            return cleaned_response
                    
                    # If return_json is False, return the raw response from model directly
                    # This is what the user requested - no JSON extraction, just return what model produces
                    self.logger.info("Returning raw response from model (no JSON extraction)")
                    
                    # Check if response seems complete (for logging purposes)
                    if len(cleaned_response) < 100:
                        self.logger.warning(f"⚠️ Response is very short ({len(cleaned_response)} chars) - might be incomplete")
                    elif len(cleaned_response) > 20000:
                        self.logger.info(f"✓ Response is very long ({len(cleaned_response)} chars) - likely complete")
                    
                    # Check if response ends properly (for CSV, check if last row is complete)
                    response_end = cleaned_response.rstrip()[-100:] if len(cleaned_response) > 100 else cleaned_response.rstrip()
                    
                    # For CSV format, check if it ends with a newline or has proper structure
                    is_csv_like = ';;;' in cleaned_response or (',' in cleaned_response and '\n' in cleaned_response)
                    if is_csv_like:
                        # For CSV, check if last line seems complete (has delimiter)
                        last_line = cleaned_response.rstrip().split('\n')[-1] if '\n' in cleaned_response else cleaned_response.rstrip()
                        if ';;;' in cleaned_response:
                            # Semicolon-delimited CSV
                            if last_line and ';;;' in last_line:
                                self.logger.info("✓ CSV response appears complete (last row has delimiter)")
                            else:
                                self.logger.warning("⚠️ CSV response might be incomplete (last row missing delimiter)")
                        elif ',' in cleaned_response:
                            # Comma-delimited CSV
                            if last_line and ',' in last_line:
                                self.logger.info("✓ CSV response appears complete (last row has delimiter)")
                            else:
                                self.logger.warning("⚠️ CSV response might be incomplete (last row missing delimiter)")
                    else:
                        # Regular text response
                        if not response_end.endswith(('.', '!', '?', ':', '\n', '}', ']')):
                            self.logger.warning("⚠️ Response might be cut off - doesn't end with proper punctuation")
                    
                    return cleaned_response
                else:
                    self.logger.error("Empty response from Gemini API - no text content found")
                    # Log response structure for debugging
                    self.logger.debug(f"Response object: {type(response)}")
                    self.logger.debug(f"Response attributes: {dir(response)}")
                    if hasattr(response, 'candidates'):
                        self.logger.debug(f"Number of candidates: {len(response.candidates) if response.candidates else 0}")
                    return None
            else:
                self.logger.error("No response received from Gemini API")
                return None
                
        except Exception as e:
            error_str = str(e)
            if self._is_quota_error(e):
                if '403' in error_str or 'leaked' in error_str.lower():
                    self.logger.error(f"❌ PDF processing failed: API key leaked (403) - {error_str}")
                    self.logger.error("API key was reported as leaked. Please:")
                    self.logger.error("  1. Remove the leaked API key from your CSV file")
                    self.logger.error("  2. Add a new valid API key to your CSV file")
                    self.logger.error("  3. The system will automatically try the next available key")
                else:
                    self.logger.error(f"❌ PDF processing failed: Quota exhausted (429) - {error_str}")
                    self.logger.error("All available API keys have been exhausted. Please:")
                    self.logger.error("  1. Wait for quota reset")
                    self.logger.error("  2. Add more API keys to your CSV file")
                    self.logger.error("  3. Check your API key quotas in Google Cloud Console")
            else:
                self.logger.error(f"PDF processing failed: {error_str}")
            return None


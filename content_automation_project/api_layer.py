"""
API Layer for Content Automation Project
Handles all interactions with Gemini API for text processing and TTS
"""

import csv
import os
import asyncio
import wave
import logging
from typing import Optional, Dict, List, Any
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
        "gemini-2.5-flash",
        "gemini-2.5-pro",
        "gemini-2.0-flash",
        "gemini-1.5-pro",
        "gemini-1.5-flash"
    ]
    
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
    DEFAULT_MAX_TOKENS = 32768  # Maximum for gemini-2.5 models


class APIKeyManager:
    """Manages API keys with rotation support"""
    
    def __init__(self):
        self.api_keys: List[Dict[str, str]] = []
        self.current_index = 0
        
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
            logging.error(f"Error loading API keys: {str(e)}")
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
        
        # Track response truncation
        self._response_truncated = False
        
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
        Process text with Gemini AI
        
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
            
        try:
            # Initialize client if needed
            if not self.text_client or api_key:
                key = api_key or self.key_manager.get_next_key()
                if not key:
                    self.logger.error("No API key available")
                    return None
                    
                genai.configure(api_key=key)
                self.text_client = genai.GenerativeModel(model_name)
            
            # Prepare prompt
            if system_prompt:
                full_prompt = f"{system_prompt}\n\nText to process:\n{text}"
            else:
                full_prompt = text
            
            # Generate content
            generation_config = genai.types.GenerationConfig(
                temperature=temperature,
                max_output_tokens=max_tokens,
            )
            
            response = self.text_client.generate_content(
                full_prompt,
                generation_config=generation_config
            )
            
            processed_text = response.text
            self.logger.info(f"Text processed successfully with {model_name}")
            return processed_text
            
        except Exception as e:
            self.logger.error(f"Text processing failed: {str(e)}")
            return None
    
    def process_pdf_with_prompt(self,
                                pdf_path: str,
                                prompt: str,
                                model_name: str = APIConfig.DEFAULT_TEXT_MODEL,
                                temperature: float = APIConfig.DEFAULT_TEMPERATURE,
                                max_tokens: int = APIConfig.DEFAULT_MAX_TOKENS,
                                api_key: Optional[str] = None) -> Optional[str]:
        """
        Process PDF file with a prompt using Gemini API
        
        Args:
            pdf_path: Path to PDF file
            prompt: Prompt/instruction for processing the PDF
            model_name: Gemini model to use
            temperature: Temperature for generation (0.0-2.0)
            max_tokens: Maximum output tokens
            api_key: Optional API key. If None, uses next key from manager.
            
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
            # Initialize client if needed
            key = api_key or self.key_manager.get_next_key()
            if not key:
                self.logger.error("No API key available")
                return None
                
            genai.configure(api_key=key)
            model = genai.GenerativeModel(model_name)
            
            # Upload PDF file to Gemini
            pdf_file = genai.upload_file(path=pdf_path)
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
            
            # Create content parts: prompt as text part, then PDF file
            content_parts = [
                prompt,  # Text prompt
                pdf_file  # PDF file
            ]
            
            # Use streaming to get complete responses for long outputs
            # This helps avoid truncation issues and allows receiving full responses
            self.logger.info("Generating content with streaming enabled for complete response...")
            response_text_parts = []
            last_chunk = None
            finish_reason = None
            
            try:
                # Generate content with streaming
                response_stream = model.generate_content(
                    content_parts,
                    generation_config=generation_config,
                    stream=True
                )
                
                # Collect all chunks and track finish reason
                chunk_count = 0
                for chunk in response_stream:
                    last_chunk = chunk
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
                
                # Combine all chunks
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
                    
                    response = StreamedResponse(full_response_text, finish_reason)
                    self.logger.info(f"Streaming completed: {chunk_count} chunks, {len(full_response_text)} characters received")
                    
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
                        
                        self.logger.info(f"Streaming finish reason: {finish_reason_str}")
                        if finish_reason == 2 or (isinstance(finish_reason, str) and 'MAX_TOKENS' in str(finish_reason).upper()):
                            self.logger.warning(f"⚠️ Response may be truncated (finish_reason: {finish_reason_str})")
                            self._response_truncated = True
                        else:
                            self.logger.info("✓ Response completed normally via streaming")
                            self._response_truncated = False
                else:
                    # Fallback to non-streaming if streaming fails
                    self.logger.warning("Streaming returned no chunks, falling back to non-streaming")
                    response = model.generate_content(
                        content_parts,
                        generation_config=generation_config
                    )
            except Exception as stream_error:
                # Fallback to non-streaming if streaming is not supported
                self.logger.warning(f"Streaming not available or failed: {str(stream_error)}, using non-streaming")
                response = model.generate_content(
                    content_parts,
                    generation_config=generation_config
                )
            
            # Log response info
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
                    # Log full response for debugging (first and last parts)
                    self.logger.info(f"PDF processed successfully with {model_name}")
                    self.logger.info(f"Full response length: {len(response_text)} characters")
                    self.logger.info(f"Response preview (first 500 chars): {response_text[:500]}...")
                    self.logger.info(f"Response preview (last 500 chars): ...{response_text[-500:]}")
                    
                    # Check if response seems complete
                    if len(response_text) < 100:
                        self.logger.warning(f"⚠️ Response is very short ({len(response_text)} chars) - might be incomplete")
                    elif len(response_text) > 20000:
                        self.logger.info(f"✓ Response is very long ({len(response_text)} chars) - likely complete")
                    
                    # Check if response ends properly
                    response_end = response_text.rstrip()[-50:] if len(response_text) > 50 else response_text.rstrip()
                    if not response_end.endswith(('.', '!', '?', ':', '\n', '}', ']')):
                        self.logger.warning("⚠️ Response might be cut off - doesn't end with proper punctuation")
                    
                    return response_text
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
            self.logger.error(f"PDF processing failed: {str(e)}")
            return None


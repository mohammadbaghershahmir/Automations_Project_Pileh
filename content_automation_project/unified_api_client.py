"""
Unified API Client that routes requests to Google or DeepSeek based on stage
"""

import logging
from typing import Optional, Dict, Any, Callable
from api_layer import GeminiAPIClient, APIKeyManager, APIConfig
from deepseek_api_client import DeepSeekAPIClient


class UnifiedAPIClient:
    """
    Unified API client that routes requests to appropriate API (Google or DeepSeek)
    based on the processing stage
    """
    
    # Mapping of stages to API providers
    STAGE_API_MAPPING = {
        "pre_ocr_topic": "google",
        "ocr_extraction": "google",
        "document_processing": "deepseek",
        "stage_1": "deepseek",
        "stage_2": "deepseek",
        "stage_3": "deepseek",
        "stage_4": "deepseek",
        "stage_e": "deepseek",
        "stage_f": "deepseek",
        "stage_j": "deepseek",
        "stage_h": "deepseek",
        "stage_v": "deepseek",
        "stage_m": "deepseek",
        "stage_l": "deepseek",
        "stage_x": "deepseek",
        "stage_y": "deepseek",
        "stage_z": "deepseek",
    }
    
    def __init__(self, google_api_key_manager: Optional[APIKeyManager] = None,
                 deepseek_api_key_manager: Optional[APIKeyManager] = None):
        """
        Initialize Unified API Client
        
        Args:
            google_api_key_manager: APIKeyManager for Google API keys
            deepseek_api_key_manager: APIKeyManager for DeepSeek API keys
        """
        self.logger = logging.getLogger(__name__)
        
        # Initialize both clients
        self.google_client = GeminiAPIClient(google_api_key_manager or APIKeyManager())
        self.deepseek_client = DeepSeekAPIClient(deepseek_api_key_manager or APIKeyManager())
        
        # Track current stage for routing
        self._current_stage: Optional[str] = None
    
    def set_stage(self, stage_name: str):
        """
        Set current processing stage for API routing
        
        Args:
            stage_name: Name of the current stage (e.g., "pre_ocr_topic", "stage_e")
        """
        self._current_stage = stage_name
        self.logger.info(f"Current stage set to: {stage_name}")
    
    def get_client_for_stage(self, stage_name: Optional[str] = None) -> Any:
        """
        Get appropriate API client for given stage
        
        Args:
            stage_name: Stage name (if None, uses current stage)
            
        Returns:
            GeminiAPIClient or DeepSeekAPIClient instance
        """
        stage = stage_name or self._current_stage
        
        if not stage:
            # Default to Google if no stage specified
            self.logger.warning("No stage specified, defaulting to Google API")
            return self.google_client
        
        api_provider = self.STAGE_API_MAPPING.get(stage.lower(), "google")
        
        if api_provider == "deepseek":
            self.logger.info(f"✓ Using DeepSeek API for stage: {stage}")
            return self.deepseek_client
        else:
            self.logger.info(f"✓ Using Google API for stage: {stage}")
            return self.google_client
    
    # Delegate all GeminiAPIClient methods to appropriate client
    
    def initialize_text_client(self, model_name: str = APIConfig.DEFAULT_TEXT_MODEL,
                              api_key: Optional[str] = None) -> bool:
        """Initialize text client for current stage"""
        client = self.get_client_for_stage()
        return client.initialize_text_client(model_name, api_key)
    
    def process_text(self,
                    text: str,
                    system_prompt: Optional[str] = None,
                    model_name: str = APIConfig.DEFAULT_TEXT_MODEL,
                    temperature: float = 0.7,
                    max_tokens: int = APIConfig.DEFAULT_MAX_TOKENS,
                    api_key: Optional[str] = None) -> Optional[str]:
        """Process text using appropriate API for current stage"""
        client = self.get_client_for_stage()
        # Cap max_tokens for DeepSeek API
        from deepseek_api_client import DeepSeekAPIClient
        if isinstance(client, DeepSeekAPIClient):
            # This is DeepSeek client, cap max_tokens
            max_tokens = min(max_tokens, APIConfig.DEFAULT_DEEPSEEK_MAX_TOKENS)
        return client.process_text(text, system_prompt, model_name, temperature, max_tokens, api_key)
    
    def process_pdf_with_prompt(self,
                                pdf_path: str,
                                prompt: str,
                                model_name: str = APIConfig.DEFAULT_TEXT_MODEL,
                                temperature: float = 0.7,
                                max_tokens: int = APIConfig.DEFAULT_MAX_TOKENS,
                                api_key: Optional[str] = None,
                                return_json: bool = False,
                                force_no_streaming: bool = False) -> Optional[str]:
        """Process PDF using appropriate API for current stage"""
        client = self.get_client_for_stage()
        # Cap max_tokens for DeepSeek API
        from deepseek_api_client import DeepSeekAPIClient
        if isinstance(client, DeepSeekAPIClient):
            # This is DeepSeek client, cap max_tokens
            max_tokens = min(max_tokens, APIConfig.DEFAULT_DEEPSEEK_MAX_TOKENS)
        return client.process_pdf_with_prompt(
            pdf_path, prompt, model_name, temperature, max_tokens, api_key, return_json, force_no_streaming
        )
    
    def process_pdf_with_prompt_batch(self,
                                      pdf_path: str,
                                      prompt: str,
                                      model_name: str = APIConfig.DEFAULT_TEXT_MODEL,
                                      temperature: float = 0.7,
                                      max_tokens: int = APIConfig.DEFAULT_MAX_TOKENS,
                                      pages_per_batch: int = 10,
                                      rows_per_batch: int = 500,
                                      api_key: Optional[str] = None,
                                      progress_callback: Optional[Callable[[str], None]] = None) -> Optional[str]:
        """Process PDF in batches using appropriate API for current stage"""
        client = self.get_client_for_stage()
        # Cap max_tokens for DeepSeek API
        from deepseek_api_client import DeepSeekAPIClient
        if isinstance(client, DeepSeekAPIClient):
            # This is DeepSeek client, cap max_tokens
            max_tokens = min(max_tokens, APIConfig.DEFAULT_DEEPSEEK_MAX_TOKENS)
        return client.process_pdf_with_prompt_batch(
            pdf_path, prompt, model_name, temperature, max_tokens,
            pages_per_batch, rows_per_batch, api_key, progress_callback
        )
    
    # Delegate other common methods
    @property
    def key_manager(self):
        """Get key manager for current stage"""
        client = self.get_client_for_stage()
        return client.key_manager
    
    @property
    def text_client(self):
        """Get text client for current stage"""
        client = self.get_client_for_stage()
        return getattr(client, 'text_client', None)
    
    def extract_from_code_block(self, text: str) -> str:
        """Extract content from code blocks"""
        return self.google_client.extract_from_code_block(text)
    
    # Delegate TTS methods to Google client (DeepSeek doesn't support TTS)
    def initialize_tts_client(self, api_key: Optional[str] = None) -> bool:
        """Initialize TTS client (Google only)"""
        return self.google_client.initialize_tts_client(api_key)
    
    def generate_tts(self, 
                    text: str,
                    output_file: str,
                    voice: str = APIConfig.DEFAULT_VOICE,
                    model: str = APIConfig.DEFAULT_TTS_MODEL,
                    api_key: Optional[str] = None,
                    instruction: Optional[str] = None,
                    multi_speaker_config: Optional[Dict] = None) -> bool:
        """Generate TTS (Google only)"""
        return self.google_client.generate_tts(
            text, output_file, voice, model, api_key, instruction, multi_speaker_config
        )
    
    async def generate_tts_async(self, 
                                text: str,
                                output_file: str,
                                voice: str = APIConfig.DEFAULT_VOICE,
                                model: str = APIConfig.DEFAULT_TTS_MODEL,
                                api_key: Optional[str] = None,
                                instruction: Optional[str] = None,
                                multi_speaker_config: Optional[Dict] = None) -> bool:
        """Generate TTS async (Google only)"""
        return await self.google_client.generate_tts_async(
            text, output_file, voice, model, api_key, instruction, multi_speaker_config
        )


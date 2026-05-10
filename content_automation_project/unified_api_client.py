"""
Unified API client for OpenRouter-only operation.
"""

import logging
import os
from typing import Optional, Dict, Any, Callable
from api_layer import APIKeyManager, APIConfig
from openrouter_api_client import OpenRouterAPIClient
from stage_settings_manager import StageSettingsManager


class UnifiedAPIClient:
    """
    Unified API client that routes all requests to OpenRouter.
    """
    
    STAGE_API_MAPPING = {}
    
    def __init__(
        self,
        google_api_key_manager: Optional[APIKeyManager] = None,
        deepseek_api_key_manager: Optional[APIKeyManager] = None,
        openrouter_api_key_manager: Optional[APIKeyManager] = None,
        stage_settings_manager: Optional[StageSettingsManager] = None,
    ):
        """
        Initialize Unified API Client
        
        Args:
            stage_settings_manager: Optional StageSettingsManager for stage-specific settings.
        """
        self.logger = logging.getLogger(__name__)

        # OpenRouter is the only active provider.
        self.openrouter_client = OpenRouterAPIClient(openrouter_api_key_manager or APIKeyManager())

        # Keep backward-compatible aliases used by existing UI code.
        self.google_client = self.openrouter_client
        self.deepseek_client = self.openrouter_client

        self.stage_settings = stage_settings_manager or StageSettingsManager()

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
        Return OpenRouter client for every stage.
        """
        stage = stage_name or self._current_stage or "unknown"
        self.logger.info(f"Using OpenRouter API for stage: {stage}")
        return self.openrouter_client
    
    def _resolve_model(self, model_name: Optional[str], stage_name: str) -> str:
        stage_model = self.stage_settings.get_stage_model(stage_name)
        if stage_model and (not model_name or model_name == APIConfig.DEFAULT_TEXT_MODEL):
            return stage_model
        if model_name and model_name != APIConfig.DEFAULT_TEXT_MODEL:
            return model_name
        return (os.getenv("OPENROUTER_MODEL") or APIConfig.DEFAULT_OPENROUTER_MODEL).strip()

    def initialize_text_client(self, model_name: str = APIConfig.DEFAULT_TEXT_MODEL,
                              api_key: Optional[str] = None) -> bool:
        """
        Initialize text client for current stage
        
        Initialize OpenRouter text client for current stage.
        """
        client = self.get_client_for_stage()
        stage = self._current_stage or "unknown"
        model_name = self._resolve_model(model_name, stage)
        self.logger.info(f"[UnifiedAPIClient] Initializing {model_name} for stage: {stage}")
        stage_api_key = self.stage_settings.get_stage_api_key(stage)
        result = client.initialize_text_client(model_name, api_key or stage_api_key)
        if result:
            self.logger.info(f"[UnifiedAPIClient] Successfully initialized {model_name} for stage: {stage}")
        else:
            self.logger.error(f"[UnifiedAPIClient] Failed to initialize {model_name} for stage: {stage}")
        return result
    
    def process_text(self,
                    text: str,
                    system_prompt: Optional[str] = None,
                    model_name: str = APIConfig.DEFAULT_TEXT_MODEL,
                    temperature: float = 0.7,
                    max_tokens: int = APIConfig.DEFAULT_MAX_TOKENS,
                    api_key: Optional[str] = None,
                    cancel_check: Optional[Callable[[], bool]] = None,
                    timeout_s: float = 600.0) -> Optional[str]:
        """
        Process text using appropriate API for current stage
        
        Process text using OpenRouter for current stage.
        cancel_check: optional callable returning True to abort (uses streaming on OpenRouter).
        """
        client = self.get_client_for_stage()
        stage = self._current_stage or "unknown"
        model_name = self._resolve_model(model_name, stage)
        stage_api_key = self.stage_settings.get_stage_api_key(stage)
        self.logger.info(f"[UnifiedAPIClient] Processing text with OpenRouter model: {model_name} (stage: {stage})")
        return client.process_text(
            text,
            system_prompt,
            model_name,
            temperature,
            max_tokens,
            api_key or stage_api_key,
            cancel_check=cancel_check,
            timeout_s=timeout_s,
        )
    
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
        stage = self._current_stage or "unknown"
        model_name = self._resolve_model(model_name, stage)
        stage_api_key = self.stage_settings.get_stage_api_key(stage)
        return client.process_pdf_with_prompt(
            pdf_path, prompt, model_name, temperature, max_tokens, api_key or stage_api_key, return_json, force_no_streaming
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
        stage = self._current_stage or "unknown"
        model_name = self._resolve_model(model_name, stage)
        stage_api_key = self.stage_settings.get_stage_api_key(stage)
        return client.process_pdf_with_prompt_batch(
            pdf_path, prompt, model_name, temperature, max_tokens,
            pages_per_batch, rows_per_batch, api_key or stage_api_key, progress_callback
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
        return self.openrouter_client.extract_from_code_block(text)
    
    def initialize_tts_client(self, api_key: Optional[str] = None) -> bool:
        """TTS is not supported in OpenRouter-only mode."""
        self.logger.error("TTS is disabled in OpenRouter-only mode.")
        return False
    
    def generate_tts(self, 
                    text: str,
                    output_file: str,
                    voice: str = APIConfig.DEFAULT_VOICE,
                    model: str = APIConfig.DEFAULT_TTS_MODEL,
                    api_key: Optional[str] = None,
                    instruction: Optional[str] = None,
                    multi_speaker_config: Optional[Dict] = None) -> bool:
        """TTS is not supported in OpenRouter-only mode."""
        self.logger.error("TTS is disabled in OpenRouter-only mode.")
        return False
    
    async def generate_tts_async(self, 
                                text: str,
                                output_file: str,
                                voice: str = APIConfig.DEFAULT_VOICE,
                                model: str = APIConfig.DEFAULT_TTS_MODEL,
                                api_key: Optional[str] = None,
                                instruction: Optional[str] = None,
                                multi_speaker_config: Optional[Dict] = None) -> bool:
        """TTS is not supported in OpenRouter-only mode."""
        self.logger.error("TTS is disabled in OpenRouter-only mode.")
        return False


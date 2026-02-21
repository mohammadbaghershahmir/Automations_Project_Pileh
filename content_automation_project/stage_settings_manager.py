"""
Stage Settings Manager
Manages model and API key settings for each processing stage
"""

import json
import os
import logging
from typing import Optional, Dict, Any


class StageSettingsManager:
    """Manages stage-specific settings (model and API key)"""
    
    def __init__(self, settings_file: str = "stage_settings.json"):
        """
        Initialize Stage Settings Manager
        
        Args:
            settings_file: Path to JSON file storing stage settings
        """
        self.settings_file = settings_file
        self.logger = logging.getLogger(__name__)
        self._settings: Dict[str, Dict[str, Any]] = {}
        self.load_settings()
    
    def load_settings(self) -> bool:
        """
        Load settings from JSON file
        
        Returns:
            True if loaded successfully, False otherwise
        """
        try:
            if os.path.exists(self.settings_file):
                with open(self.settings_file, 'r', encoding='utf-8') as f:
                    self._settings = json.load(f)
                self.logger.info(f"Loaded stage settings from {self.settings_file}")
                return True
            else:
                self._settings = {}
                self.logger.info("No existing settings file found, starting with empty settings")
                return True
        except Exception as e:
            self.logger.error(f"Failed to load settings: {e}")
            self._settings = {}
            return False
    
    def save_settings(self) -> bool:
        """
        Save settings to JSON file
        
        Returns:
            True if saved successfully, False otherwise
        """
        try:
            with open(self.settings_file, 'w', encoding='utf-8') as f:
                json.dump(self._settings, f, ensure_ascii=False, indent=2)
            self.logger.info(f"Saved stage settings to {self.settings_file}")
            return True
        except Exception as e:
            self.logger.error(f"Failed to save settings: {e}")
            return False
    
    def get_stage_setting(self, stage_name: str, setting_key: str, default: Any = None) -> Any:
        """
        Get a specific setting for a stage
        
        Args:
            stage_name: Name of the stage
            setting_key: Key of the setting (e.g., 'model', 'api_key')
            default: Default value if setting not found
            
        Returns:
            Setting value or default
        """
        stage_settings = self._settings.get(stage_name, {})
        return stage_settings.get(setting_key, default)
    
    def set_stage_setting(self, stage_name: str, setting_key: str, value: Any) -> bool:
        """
        Set a specific setting for a stage
        
        Args:
            stage_name: Name of the stage
            setting_key: Key of the setting (e.g., 'model', 'api_key')
            value: Value to set
            
        Returns:
            True if set successfully, False otherwise
        """
        if stage_name not in self._settings:
            self._settings[stage_name] = {}
        
        self._settings[stage_name][setting_key] = value
        return self.save_settings()
    
    def get_stage_model(self, stage_name: str, default: Optional[str] = None) -> Optional[str]:
        """
        Get model for a specific stage
        
        Args:
            stage_name: Name of the stage
            default: Default model if not set
            
        Returns:
            Model name or default
        """
        return self.get_stage_setting(stage_name, 'model', default)
    
    def set_stage_model(self, stage_name: str, model: str) -> bool:
        """
        Set model for a specific stage
        
        Args:
            stage_name: Name of the stage
            model: Model name to set
            
        Returns:
            True if set successfully, False otherwise
        """
        return self.set_stage_setting(stage_name, 'model', model)
    
    def get_stage_api_key(self, stage_name: str, default: Optional[str] = None) -> Optional[str]:
        """
        Get API key for a specific stage
        
        Args:
            stage_name: Name of the stage
            default: Default API key if not set
            
        Returns:
            API key or default
        """
        return self.get_stage_setting(stage_name, 'api_key', default)
    
    def set_stage_api_key(self, stage_name: str, api_key: str) -> bool:
        """
        Set API key for a specific stage
        
        Args:
            stage_name: Name of the stage
            api_key: API key to set
            
        Returns:
            True if set successfully, False otherwise
        """
        return self.set_stage_setting(stage_name, 'api_key', api_key)
    
    def get_stage_provider(self, stage_name: str, default: Optional[str] = None) -> Optional[str]:
        """
        Get API provider for a specific stage
        
        Args:
            stage_name: Name of the stage
            default: Default provider if not set
            
        Returns:
            Provider name ("google", "deepseek", or "openrouter") or default
        """
        return self.get_stage_setting(stage_name, 'provider', default)
    
    def set_stage_provider(self, stage_name: str, provider: str) -> bool:
        """
        Set API provider for a specific stage
        
        Args:
            stage_name: Name of the stage
            provider: Provider name ("google" or "deepseek")
            
        Returns:
            True if set successfully, False otherwise
        """
        if provider not in ["google", "deepseek", "openrouter"]:
            self.logger.warning(f"Invalid provider: {provider}. Must be 'google', 'deepseek', or 'openrouter'")
            return False
        return self.set_stage_setting(stage_name, 'provider', provider)
    
    def get_all_stage_settings(self, stage_name: str) -> Dict[str, Any]:
        """
        Get all settings for a specific stage
        
        Args:
            stage_name: Name of the stage
            
        Returns:
            Dictionary of all settings for the stage
        """
        return self._settings.get(stage_name, {}).copy()
    
    def clear_stage_settings(self, stage_name: str) -> bool:
        """
        Clear all settings for a specific stage
        
        Args:
            stage_name: Name of the stage
            
        Returns:
            True if cleared successfully, False otherwise
        """
        if stage_name in self._settings:
            del self._settings[stage_name]
            return self.save_settings()
        return True
    
    def get_all_settings(self) -> Dict[str, Dict[str, Any]]:
        """
        Get all settings for all stages
        
        Returns:
            Dictionary of all stage settings
        """
        return self._settings.copy()

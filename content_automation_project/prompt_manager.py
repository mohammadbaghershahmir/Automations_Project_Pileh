"""
Prompt Manager Module
Manages predefined prompts and custom prompt handling
"""

import json
import os
import logging
from typing import Dict, List, Optional
from pathlib import Path


class PromptManager:
    """Manages predefined and custom prompts"""
    
    def __init__(self, prompts_file: Optional[str] = None):
        """
        Initialize Prompt Manager
        
        Args:
            prompts_file: Optional path to JSON file containing predefined prompts
        """
        self.logger = logging.getLogger(__name__)
        self.prompts_file = prompts_file or "prompts.json"
        self.predefined_prompts: Dict[str, str] = {}
        self.load_predefined_prompts()
    
    def load_predefined_prompts(self):
        """Load predefined prompts from file or create default"""
        if os.path.exists(self.prompts_file):
            try:
                with open(self.prompts_file, 'r', encoding='utf-8') as f:
                    self.predefined_prompts = json.load(f)
                self.logger.info(f"Loaded {len(self.predefined_prompts)} predefined prompts")
            except Exception as e:
                self.logger.error(f"Error loading prompts file: {str(e)}")
                self._create_default_prompts()
        else:
            self._create_default_prompts()
            self.save_prompts()
    
    def _create_default_prompts(self):
        """Create default predefined prompts"""
        self.predefined_prompts = {
            "Summarize Content": "Please provide a comprehensive summary of the following content. Include key points, main ideas, and important details.",
            
            "Extract Key Information": "Extract and organize the key information from the following content. Create a structured list of important points, facts, and data.",
            
            "Translate to English": "Translate the following content to English while maintaining the original meaning and context.",
            
            "Improve Writing": "Improve the following text for clarity, grammar, and professional tone. Maintain the original meaning while enhancing readability.",
            
            "Create Questions": "Based on the following content, create a set of comprehensive questions that test understanding of the material. Include both factual and analytical questions.",
            
            "Test Prompt": "This is a test prompt for content automation. Please analyze the provided content and provide a detailed response."
        }
        self.logger.info("Created default predefined prompts")
    
    def save_prompts(self):
        """Save predefined prompts to file"""
        try:
            with open(self.prompts_file, 'w', encoding='utf-8') as f:
                json.dump(self.predefined_prompts, f, indent=2, ensure_ascii=False)
            self.logger.info(f"Saved {len(self.predefined_prompts)} prompts to {self.prompts_file}")
        except Exception as e:
            self.logger.error(f"Error saving prompts: {str(e)}")
    
    def get_prompt_names(self) -> List[str]:
        """
        Get list of predefined prompt names
        
        Returns:
            List of prompt names
        """
        return list(self.predefined_prompts.keys())
    
    def get_prompt(self, name: str) -> Optional[str]:
        """
        Get predefined prompt by name
        
        Args:
            name: Name of the prompt
            
        Returns:
            Prompt text or None if not found
        """
        return self.predefined_prompts.get(name)
    
    def add_prompt(self, name: str, prompt: str):
        """
        Add a new predefined prompt
        
        Args:
            name: Name of the prompt
            prompt: Prompt text
        """
        self.predefined_prompts[name] = prompt
        self.save_prompts()
        self.logger.info(f"Added new prompt: {name}")
    
    def update_prompt(self, name: str, prompt: str):
        """
        Update an existing predefined prompt
        
        Args:
            name: Name of the prompt
            prompt: New prompt text
        """
        if name in self.predefined_prompts:
            self.predefined_prompts[name] = prompt
            self.save_prompts()
            self.logger.info(f"Updated prompt: {name}")
        else:
            self.logger.warning(f"Prompt '{name}' not found for update")
    
    def delete_prompt(self, name: str):
        """
        Delete a predefined prompt
        
        Args:
            name: Name of the prompt to delete
        """
        if name in self.predefined_prompts:
            del self.predefined_prompts[name]
            self.save_prompts()
            self.logger.info(f"Deleted prompt: {name}")
        else:
            self.logger.warning(f"Prompt '{name}' not found for deletion")









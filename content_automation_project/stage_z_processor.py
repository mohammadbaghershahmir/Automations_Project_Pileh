"""
Stage Z Processor: RichText Generation
Generates RichText format output from Stage A, Stage X, and Stage Y data.
"""

import json
import logging
import os
from typing import Optional, Dict, List, Any, Callable

from base_stage_processor import BaseStageProcessor
from api_layer import APIConfig


class StageZProcessor(BaseStageProcessor):
    """Process Stage Z: Generate RichText output"""
    
    def __init__(self, api_client):
        super().__init__(api_client)
        self.logger = logging.getLogger(__name__)
    
    def process_stage_z(
        self,
        stage_a_path: str,  # File a without Imp column
        stage_x_output_path: str,  # Output from Stage X
        stage_y_output_path: str,  # Output from Stage Y
        prompt: str,
        model_name: str,
        output_dir: Optional[str] = None,
        progress_callback: Optional[Callable[[str], None]] = None
    ) -> Optional[str]:
        """
        Process Stage Z: Generate RichText output.
        
        Args:
            stage_a_path: Path to Stage A JSON file (a{book}{chapter}.json) - will remove Imp column
            stage_x_output_path: Path to Stage X output JSON
            stage_y_output_path: Path to Stage Y output JSON
            prompt: Prompt for RichText generation
            model_name: Gemini model name
            output_dir: Output directory (defaults to stage_a_path directory)
            progress_callback: Optional callback for progress updates
            
        Returns:
            Path to output file (z{book}{chapter}+{chapter_name}.rtf) or None on error
        """
        def _progress(msg: str):
            if progress_callback:
                progress_callback(msg)
            self.logger.info(msg)
        
        # Set stage if using UnifiedAPIClient (for API routing)
        if hasattr(self.api_client, 'set_stage'):
            self.api_client.set_stage("stage_z")
        
        _progress("Starting Stage Z processing...")
        
        # Determine output directory
        if not output_dir:
            output_dir = os.path.dirname(stage_a_path) or os.getcwd()
        os.makedirs(output_dir, exist_ok=True)
        
        # Load Stage A data and remove Imp column
        stage_a_data = self.load_json_file(stage_a_path)
        if not stage_a_data:
            self.logger.error("Failed to load Stage A JSON")
            return None
        
        stage_a_records = self.get_data_from_json(stage_a_data)
        if not stage_a_records:
            self.logger.error("Stage A JSON has no data")
            return None
        
        # Remove Imp column from each record
        stage_a_without_imp = []
        for record in stage_a_records:
            record_copy = record.copy()
            record_copy.pop("Imp", None)  # Remove Imp column if exists
            stage_a_without_imp.append(record_copy)
        
        _progress(f"Loaded {len(stage_a_without_imp)} records from Stage A (Imp column removed)")
        
        # Load Stage X output
        stage_x_data = self.load_json_file(stage_x_output_path)
        if not stage_x_data:
            self.logger.error("Failed to load Stage X output")
            return None
        
        stage_x_changes = self.get_data_from_json(stage_x_data)
        _progress(f"Loaded {len(stage_x_changes)} changes from Stage X")
        
        # Load Stage Y output
        stage_y_data = self.load_json_file(stage_y_output_path)
        if not stage_y_data:
            self.logger.error("Failed to load Stage Y output")
            return None
        
        stage_y_deletions = self.get_data_from_json(stage_y_data)
        _progress(f"Loaded {len(stage_y_deletions)} deletions from Stage Y")
        
        # Process Stage A as a whole (no splitting)
        _progress(f"Processing Stage A as a whole ({len(stage_a_without_imp)} records)")
        
        base_name = os.path.splitext(os.path.basename(stage_a_path))[0]
        
        # Prepare data for model (all Stage A data at once)
        richtext_data = {
            "current_data": stage_a_without_imp,
            "changes": stage_x_changes,
            "deletions": stage_y_deletions
        }
        
        richtext_text = json.dumps(richtext_data, ensure_ascii=False, indent=2)
        
        # Call model for RichText generation
        _progress("Calling model for RichText generation...")
        response_text = self.api_client.process_text(
            text=richtext_text,
            system_prompt=prompt,
            model_name=model_name
        )
        
        if not response_text:
            self.logger.error("Model returned no response")
            return None
        
        # Save raw response to TXT file FIRST (like Stage V)
        txt_path = os.path.join(output_dir, f"{base_name}_stage_z.txt")
        try:
            with open(txt_path, 'w', encoding='utf-8') as f:
                f.write("=== STAGE Z (RichText Generation) RESPONSE ===\n\n")
                f.write(response_text)
            _progress(f"Saved raw model response to: {os.path.basename(txt_path)}")
            self.logger.info(f"Saved Stage Z raw response to: {txt_path}")
        except Exception as e:
            self.logger.warning(f"Failed to save TXT file: {e}")
        
        # Extract RichText from response (might be wrapped in JSON or markdown)
        richtext_content = self._extract_richtext_from_response(response_text)
        
        if not richtext_content:
            self.logger.error("Failed to extract RichText from model responses")
            return None
        
        # Extract book and chapter from Stage A
        metadata = self.get_metadata_from_json(stage_a_data)
        book_id = metadata.get("book_id", 105)
        chapter_id = metadata.get("chapter_id", 3)
        
        # Extract chapter name from Stage A metadata or filename
        chapter_name = ""
        chapter_name = (
            metadata.get("chapter", "") or
            metadata.get("Chapter", "") or
            metadata.get("chapter_name", "") or
            metadata.get("Chapter_Name", "") or
            ""
        )
        
        # If not found in metadata, try to get from first record
        if not chapter_name and stage_a_without_imp:
            chapter_name = stage_a_without_imp[0].get("chapter", "")
        
        # If still not found, try to extract from Stage A filename (a{book}{chapter}+{chapter_name}.json)
        if not chapter_name:
            import re
            stage_a_basename = os.path.basename(stage_a_path)
            stage_a_name_without_ext = os.path.splitext(stage_a_basename)[0]
            # Try to extract chapter name from filename pattern: a{book}{chapter}+{chapter_name}
            match = re.match(r'^a\d{6}\+(.+)$', stage_a_name_without_ext)
            if match:
                chapter_name = match.group(1)
        
        # Clean chapter name for filename (remove invalid characters)
        if chapter_name:
            import re
            # Replace spaces and invalid filename characters with underscore
            chapter_name_clean = re.sub(r'[<>:"/\\|?*]', '_', chapter_name)
            chapter_name_clean = chapter_name_clean.replace(' ', '_')
            # Remove multiple underscores
            chapter_name_clean = re.sub(r'_+', '_', chapter_name_clean)
            # Remove leading/trailing underscores
            chapter_name_clean = chapter_name_clean.strip('_')
        else:
            chapter_name_clean = ""
        
        if chapter_name_clean:
            _progress(f"Detected Chapter Name: {chapter_name}")
        else:
            _progress("No chapter name found, will use timestamp in filename")
        
        # Generate output filename: z{book}{chapter}+{chapter_name}.rtf (matching Stage X/Y pattern)
        # If chapter name is empty, use timestamp to avoid overwriting
        if chapter_name_clean:
            base_filename = f"z{book_id:03d}{chapter_id:03d}+{chapter_name_clean}.rtf"
        else:
            # Fallback if no chapter name: use timestamp to avoid overwriting
            from datetime import datetime
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            base_filename = f"z{book_id:03d}{chapter_id:03d}+{timestamp}.rtf"
            self.logger.warning(f"No chapter name found, using timestamp in filename: {timestamp}")
        
        output_path = os.path.join(output_dir, base_filename)
        
        # Check if file already exists and add counter if needed
        if os.path.exists(output_path) and chapter_name_clean:
            # If file exists and we have chapter name, add counter
            counter = 1
            while os.path.exists(output_path):
                base_filename = f"z{book_id:03d}{chapter_id:03d}+{chapter_name_clean}_{counter}.rtf"
                output_path = os.path.join(output_dir, base_filename)
                counter += 1
            if counter > 1:
                self.logger.info(f"File already exists, using counter: {counter - 1}")
        
        try:
            with open(output_path, 'w', encoding='utf-8') as f:
                f.write(richtext_content)
            _progress(f"Stage Z completed: {base_filename}")
            return output_path
        except Exception as e:
            self.logger.error(f"Failed to save RichText file: {e}")
            return None
    
    def _extract_richtext_from_response(self, response_text: str) -> str:
        """
        Extract RichText content from model response.
        Handles cases where response might be wrapped in JSON or markdown.
        """
        # Try to extract from JSON
        try:
            json_obj = json.loads(response_text)
            if isinstance(json_obj, dict):
                # Try common keys
                richtext = json_obj.get("richtext", json_obj.get("content", json_obj.get("text", "")))
                if richtext:
                    return richtext
        except:
            pass
        
        # Try to extract from markdown code blocks
        if "```" in response_text:
            # Look for rtf or richtext code blocks
            import re
            pattern = r"```(?:rtf|richtext)?\s*\n(.*?)```"
            matches = re.findall(pattern, response_text, re.DOTALL)
            if matches:
                return matches[0].strip()
        
        # If no extraction worked, return as-is
        return response_text.strip()
    
    def _get_timestamp(self) -> str:
        """Get current timestamp"""
        from datetime import datetime
        return datetime.now().isoformat()


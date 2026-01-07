"""
Stage M Processor: Topic ID List
Extracts unique (chapter, subchapter, topic) combinations from Stage H (ac) file.
"""

import json
import logging
import os
from typing import Optional, Dict, List, Any, Callable

from base_stage_processor import BaseStageProcessor


class StageMProcessor(BaseStageProcessor):
    """Process Stage M: Extract unique topic combinations from Stage H (ac) file."""

    def __init__(self, api_client):
        super().__init__(api_client)
        self.logger = logging.getLogger(__name__)

    def process_stage_m(
        self,
        stage_h_path: str,
        output_dir: Optional[str] = None,
        progress_callback: Optional[Callable[[str], None]] = None
    ) -> Optional[str]:
        """
        Process Stage M: Extract unique (chapter, subchapter, topic) combinations.

        Args:
            stage_h_path: Path to Stage H JSON file (ac{book}{chapter}+{chapter_name}.json)
            output_dir: Output directory (defaults to stage_h_path directory)
            progress_callback: Optional callback for progress updates

        Returns:
            Path to output file (i{book}{chapter}+{chapter_name}.json) or None on error
        """

        def _progress(msg: str):
            if progress_callback:
                progress_callback(msg)
            self.logger.info(msg)

        # Set stage if using UnifiedAPIClient (for API routing)
        if hasattr(self.api_client, 'set_stage'):
            self.api_client.set_stage("stage_m")

        _progress("Starting Stage M processing...")

        # Load Stage H JSON (ac file)
        _progress("Loading Stage H JSON...")
        stage_h_data = self.load_json_file(stage_h_path)
        if not stage_h_data:
            self.logger.error("Failed to load Stage H JSON")
            return None

        # Get data from Stage H
        stage_h_records = self.get_data_from_json(stage_h_data)
        if not stage_h_records:
            self.logger.error("Stage H JSON has no data")
            return None

        _progress(f"Loaded {len(stage_h_records)} records from Stage H")

        # Try to detect book_id and chapter_id from first PointId (if available)
        book_id = None
        chapter_id = None

        first_point_id = None
        for record in stage_h_records:
            first_point_id = record.get("PointId") or record.get("point_id")
            if first_point_id:
                break

        if first_point_id:
            try:
                book_id, chapter_id = self.extract_book_chapter_from_pointid(str(first_point_id))
                _progress(f"Detected Book ID: {book_id}, Chapter ID: {chapter_id}")
            except ValueError:
                self.logger.warning("Could not extract book/chapter from PointId. Using defaults (0).")

        if book_id is None:
            book_id = 0
        if chapter_id is None:
            chapter_id = 0

        # Extract chapter name from Stage H metadata (similar to Stage H extraction from Stage J)
        chapter_name = ""
        stage_h_metadata = self.get_metadata_from_json(stage_h_data)
        chapter_name = (
            stage_h_metadata.get("chapter", "") or
            stage_h_metadata.get("Chapter", "") or
            stage_h_metadata.get("chapter_name", "") or
            stage_h_metadata.get("Chapter_Name", "") or
            ""
        )

        # If not found in metadata, try to get from first record
        if not chapter_name and stage_h_records:
            chapter_name = stage_h_records[0].get("chapter", "")

        # If still not found, try to extract from Stage H filename (ac{book}{chapter}+{chapter_name}.json)
        if not chapter_name:
            import re
            stage_h_basename = os.path.basename(stage_h_path)
            stage_h_name_without_ext = os.path.splitext(stage_h_basename)[0]
            # Try to extract chapter name from filename pattern: ac{book}{chapter}+{chapter_name}
            match = re.match(r'^ac\d{6}\+(.+)$', stage_h_name_without_ext)
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

        # Extract unique (chapter, subchapter, topic) combinations
        seen_keys = set()
        unique_topics: List[Dict[str, Any]] = []

        _progress("Extracting unique (chapter, subchapter, topic) combinations...")

        for record in stage_h_records:
            chapter = record.get("chapter", "")
            subchapter = record.get("subchapter", "")
            topic = record.get("topic", "")

            # Build uniqueness key
            key = (chapter, subchapter, topic)
            if key in seen_keys:
                continue

            seen_keys.add(key)

            unique_topics.append(
                {
                    "chapter": chapter,
                    "subchapter": subchapter,
                    "topic": topic,
                }
            )

        _progress(f"Extracted {len(unique_topics)} unique topics")

        if not output_dir:
            output_dir = os.path.dirname(stage_h_path) or os.getcwd()

        # Generate output filename: i{book}{chapter}+{chapter_name}.json (matching Stage H/V pattern)
        # If chapter name is empty, use timestamp to avoid overwriting
        if chapter_name_clean:
            base_filename = f"i{book_id:03d}{chapter_id:03d}+{chapter_name_clean}.json"
        else:
            # Fallback if no chapter name: use timestamp to avoid overwriting
            from datetime import datetime
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            base_filename = f"i{book_id:03d}{chapter_id:03d}+{timestamp}.json"
            self.logger.warning(f"No chapter name found, using timestamp in filename: {timestamp}")

        output_path = os.path.join(output_dir, base_filename)

        # Check if file already exists and add counter if needed
        if os.path.exists(output_path) and chapter_name_clean:
            # If file exists and we have chapter name, add counter
            counter = 1
            while os.path.exists(output_path):
                base_filename = f"i{book_id:03d}{chapter_id:03d}+{chapter_name_clean}_{counter}.json"
                output_path = os.path.join(output_dir, base_filename)
                counter += 1
            if counter > 1:
                self.logger.info(f"File already exists, using counter: {counter - 1}")

        # Prepare metadata
        output_metadata = {
            "book_id": book_id,
            "chapter_id": chapter_id,
            "source_stage_h": os.path.basename(stage_h_path),
            "total_topics": len(unique_topics),
        }

        # Save JSON
        _progress(f"Saving Stage M output to: {output_path}")
        success = self.save_json_file(unique_topics, output_path, output_metadata, "M")

        if success:
            _progress(f"Stage M completed successfully: {output_path}")
            return output_path
        else:
            self.logger.error("Failed to save Stage M output")
            return None
























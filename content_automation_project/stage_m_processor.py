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
            stage_h_path: Path to Stage H JSON file (ac{book}{chapter}.json)
            output_dir: Output directory (defaults to stage_h_path directory)
            progress_callback: Optional callback for progress updates

        Returns:
            Path to output file (i{book}{chapter}.json) or None on error
        """

        def _progress(msg: str):
            if progress_callback:
                progress_callback(msg)
            self.logger.info(msg)

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

        # Generate output filename: i{book}{chapter}.json
        output_path = self.generate_filename("i", book_id, chapter_id, output_dir)

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



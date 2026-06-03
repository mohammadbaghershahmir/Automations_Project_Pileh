"""
Caption loading and topic context for Voice Class Step 1.

Pure business logic — no API calls, no filesystem access.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Callable, Dict, List, Optional, Tuple

from stage_j_processor import _sj_build_topic_key, sj_index_pic_captions_by_topic

TopicKey = Tuple[str, str, str]
CaptionIndex = Dict[TopicKey, List[Dict[str, str]]]
LoadJsonFn = Callable[[str], Any]
ExtractRowsFn = Callable[[Any], List[Dict[str, Any]]]


@dataclass(frozen=True)
class TopicCaptionIndexes:
    """Image and table captions indexed by topic."""

    image_by_topic: CaptionIndex
    table_by_topic: CaptionIndex

    @property
    def image_topic_count(self) -> int:
        return len(self.image_by_topic)

    @property
    def table_topic_count(self) -> int:
        return len(self.table_by_topic)


def load_caption_index_from_json(
    json_path: Optional[str],
    *,
    load_json: LoadJsonFn,
    extract_rows: ExtractRowsFn,
) -> CaptionIndex:
    """Load one filepic or tablepic JSON file and index captions by topic."""
    if not json_path:
        return {}

    raw_data = load_json(json_path)
    if not raw_data:
        return {}

    records = extract_rows(raw_data)
    dict_rows = [row for row in records if isinstance(row, dict)]
    return sj_index_pic_captions_by_topic(dict_rows)


def load_topic_caption_indexes(
    filepic_json_path: Optional[str],
    tablepic_json_path: Optional[str],
    *,
    load_json: LoadJsonFn,
    extract_rows: ExtractRowsFn,
) -> TopicCaptionIndexes:
    """Load filepic and tablepic sidecars into per-topic caption indexes."""
    image_by_topic = load_caption_index_from_json(
        filepic_json_path,
        load_json=load_json,
        extract_rows=extract_rows,
    )
    table_by_topic = load_caption_index_from_json(
        tablepic_json_path,
        load_json=load_json,
        extract_rows=extract_rows,
    )
    return TopicCaptionIndexes(
        image_by_topic=image_by_topic,
        table_by_topic=table_by_topic,
    )


def build_topic_caption_context(
    chapter: str,
    subchapter: str,
    topic: str,
    caption_indexes: TopicCaptionIndexes,
) -> Optional[Dict[str, Any]]:
    """
    Build caption context for one topic, or None when no captions exist.

    Matches Importance & Type ``topics_context`` shape used in Stage J.
    """
    topic_key = _sj_build_topic_key(chapter, subchapter, topic)
    table_captions = caption_indexes.table_by_topic.get(topic_key)
    image_captions = caption_indexes.image_by_topic.get(topic_key)

    if not table_captions and not image_captions:
        return None

    context: Dict[str, Any] = {
        "chapter": chapter,
        "subchapter": subchapter,
        "topic": topic,
    }
    if table_captions:
        context["topic_table_captions"] = table_captions
    if image_captions:
        context["topic_image_captions"] = image_captions
    return context


def format_caption_block_for_prompt(topics_context: Dict[str, Any]) -> str:
    """Format caption context as a readable block for the script LLM prompt."""
    serialized = json.dumps(topics_context, ensure_ascii=False, indent=2)
    return (
        "\n\nImage and table captions for THIS TOPIC "
        "(use these to explain figures and tables naturally in the spoken script):\n"
        f"{serialized}\n"
    )

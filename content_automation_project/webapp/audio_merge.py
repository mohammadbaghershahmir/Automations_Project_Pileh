"""Merge intro, TTS WAV segments, and outro into a single MP3."""

from __future__ import annotations

import logging
import os
from typing import List, Optional

logger = logging.getLogger(__name__)


def _standardize_segment(audio):
    from pydub import AudioSegment

    if audio.frame_rate != 44100:
        audio = audio.set_frame_rate(44100)
    if audio.channels != 2:
        audio = audio.set_channels(2)
    return audio


def merge_voice_tracks(
    intro_mp3: Optional[str],
    segment_wav_paths: List[str],
    outro_mp3: Optional[str],
    output_mp3: str,
) -> bool:
    """
    Concatenate intro MP3 + segment WAVs + outro MP3 into one MP3 file.
    Returns True on success.
    """
    try:
        from pydub import AudioSegment
    except ImportError:
        logger.error("pydub is not installed")
        return False

    combined = None
    try:
        if intro_mp3 and os.path.isfile(intro_mp3):
            intro = _standardize_segment(AudioSegment.from_file(intro_mp3))
            combined = intro

        for path in segment_wav_paths:
            if not path or not os.path.isfile(path):
                logger.error("Missing segment WAV: %s", path)
                return False
            seg = _standardize_segment(AudioSegment.from_file(path))
            combined = seg if combined is None else combined + seg

        if outro_mp3 and os.path.isfile(outro_mp3):
            outro = _standardize_segment(AudioSegment.from_file(outro_mp3))
            combined = outro if combined is None else combined + outro

        if combined is None:
            logger.error("No audio content to merge")
            return False

        os.makedirs(os.path.dirname(output_mp3) or ".", exist_ok=True)
        combined.export(output_mp3, format="mp3", bitrate="64k")
        logger.info("Merged voice MP3: %s", output_mp3)
        return True
    except Exception as e:
        logger.error("Audio merge failed: %s", e)
        return False


def wav_duration_seconds(wav_path: str) -> Optional[float]:
    try:
        from pydub import AudioSegment

        if not os.path.isfile(wav_path):
            return None
        audio = AudioSegment.from_file(wav_path)
        return len(audio) / 1000.0
    except Exception:
        return None

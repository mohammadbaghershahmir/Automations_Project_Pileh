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
    *,
    require_intro_outro: bool = True,
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

    if require_intro_outro:
        if not intro_mp3 or not os.path.isfile(intro_mp3):
            logger.error("Intro MP3 not found: %s", intro_mp3)
            return False
        if not outro_mp3 or not os.path.isfile(outro_mp3):
            logger.error("Outro MP3 not found: %s", outro_mp3)
            return False

    combined = None
    parts: List[str] = []
    try:
        if intro_mp3 and os.path.isfile(intro_mp3):
            intro = _standardize_segment(AudioSegment.from_file(intro_mp3))
            combined = intro
            parts.append(f"intro ({len(intro) / 1000:.1f}s)")

        for path in segment_wav_paths:
            if not path or not os.path.isfile(path):
                logger.error("Missing segment WAV: %s", path)
                return False
            seg = _standardize_segment(AudioSegment.from_file(path))
            combined = seg if combined is None else combined + seg
            parts.append(f"{os.path.basename(path)} ({len(seg) / 1000:.1f}s)")

        if outro_mp3 and os.path.isfile(outro_mp3):
            outro = _standardize_segment(AudioSegment.from_file(outro_mp3))
            combined = outro if combined is None else combined + outro
            parts.append(f"outro ({len(outro) / 1000:.1f}s)")

        if combined is None:
            logger.error("No audio content to merge")
            return False

        os.makedirs(os.path.dirname(output_mp3) or ".", exist_ok=True)
        combined.export(output_mp3, format="mp3", bitrate="64k")
        logger.info(
            "Merged voice MP3: %s — parts: %s — total %.1fs",
            output_mp3,
            ", ".join(parts),
            len(combined) / 1000.0,
        )
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


def analyze_audio_for_preview(audio_path: str, *, bar_count: int = 80) -> Optional[dict]:
    """
    Lightweight audio analysis for admin preview: duration, silence hint, downsampled peaks.
    Supports WAV, MP3, and other formats pydub/ffmpeg can read.
    """
    try:
        from pydub import AudioSegment
    except ImportError:
        return None

    if not os.path.isfile(audio_path):
        return None

    try:
        audio = AudioSegment.from_file(audio_path)
        raw = audio.get_array_of_samples()
        if not raw:
            return {
                "duration_seconds": 0.0,
                "max_amplitude": 0.0,
                "is_silent": True,
                "waveform_peaks": [0.0] * bar_count,
            }

        channels = max(1, int(audio.channels))
        max_possible = float(1 << (8 * audio.sample_width - 1))
        mono: List[int] = []
        if channels == 1:
            mono = [abs(int(s)) for s in raw]
        else:
            for i in range(0, len(raw) - channels + 1, channels):
                frame = [abs(int(raw[i + c])) for c in range(channels)]
                mono.append(max(frame))

        if not mono:
            return {
                "duration_seconds": round(len(audio) / 1000.0, 2),
                "max_amplitude": 0.0,
                "is_silent": True,
                "waveform_peaks": [0.0] * bar_count,
            }

        max_amp = max(mono)
        normalized_max = max_amp / max_possible if max_possible else 0.0
        rms = (sum(v * v for v in mono) / len(mono)) ** 0.5
        normalized_rms = rms / max_possible if max_possible else 0.0
        duration = len(audio) / 1000.0
        is_silent = duration < 0.05 or normalized_max < 0.005 or normalized_rms < 0.002

        chunk = max(1, len(mono) // bar_count)
        peaks: List[float] = []
        for i in range(0, len(mono), chunk):
            block = mono[i : i + chunk]
            peaks.append((max(block) / max_possible) if block else 0.0)
        peaks = peaks[:bar_count]
        while len(peaks) < bar_count:
            peaks.append(0.0)

        visual_max = max(peaks) or 1.0
        peaks = [round(p / visual_max, 4) for p in peaks]

        return {
            "duration_seconds": round(duration, 2),
            "max_amplitude": round(normalized_max, 4),
            "is_silent": is_silent,
            "waveform_peaks": peaks,
        }
    except Exception as e:
        logger.warning("Audio analysis failed for %s: %s", audio_path, e)
        return None


def analyze_wav_for_preview(wav_path: str, *, bar_count: int = 80) -> Optional[dict]:
    """Backward-compatible alias."""
    return analyze_audio_for_preview(wav_path, bar_count=bar_count)

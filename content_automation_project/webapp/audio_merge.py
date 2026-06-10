"""Merge intro, TTS WAV segments, and outro into a single MP3."""

from __future__ import annotations

import logging
import os
import shutil
import subprocess
import tempfile
from typing import Callable, List, Optional

logger = logging.getLogger(__name__)

# Above this count, pydub loads every segment into RAM and can OOM the worker (e.g. 140 × ~45s).
_FFMPEG_MERGE_MIN_SEGMENTS = 8


def _standardize_segment(audio):
    from pydub import AudioSegment

    if audio.frame_rate != 44100:
        audio = audio.set_frame_rate(44100)
    if audio.channels != 2:
        audio = audio.set_channels(2)
    return audio


def _ffmpeg_available() -> bool:
    try:
        proc = subprocess.run(
            ["ffmpeg", "-version"],
            capture_output=True,
            timeout=10,
            check=False,
        )
        return proc.returncode == 0
    except (FileNotFoundError, subprocess.TimeoutExpired, OSError):
        return False


def _escape_concat_path(path: str) -> str:
    return path.replace("'", "'\\''")


def _run_ffmpeg(cmd: List[str], *, timeout: int, label: str) -> bool:
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout, check=False)
    except subprocess.TimeoutExpired:
        logger.error("ffmpeg %s timed out after %ss", label, timeout)
        return False
    if proc.returncode != 0:
        tail = (proc.stderr or proc.stdout or "")[-500:]
        logger.error("ffmpeg %s failed (exit %s): %s", label, proc.returncode, tail)
        return False
    return True


def _write_concat_list(paths: List[str], list_path: str) -> None:
    with open(list_path, "w", encoding="utf-8") as f:
        for path in paths:
            f.write(f"file '{_escape_concat_path(path)}'\n")


def _concat_wavs_copy(wav_paths: List[str], output_wav: str) -> bool:
    """Stream-copy concat when all inputs share the same WAV codec/format (Gemini TTS segments)."""
    if not wav_paths:
        return False
    if len(wav_paths) == 1:
        try:
            shutil.copy2(wav_paths[0], output_wav)
            return os.path.isfile(output_wav) and os.path.getsize(output_wav) > 44
        except OSError as e:
            logger.error("copy single wav failed: %s", e)
            return False
    list_path = ""
    try:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False, encoding="utf-8") as f:
            list_path = f.name
            for path in wav_paths:
                f.write(f"file '{_escape_concat_path(path)}'\n")
        cmd = [
            "ffmpeg",
            "-y",
            "-nostdin",
            "-hide_banner",
            "-loglevel",
            "error",
            "-f",
            "concat",
            "-safe",
            "0",
            "-i",
            list_path,
            "-c",
            "copy",
            output_wav,
        ]
        ok = _run_ffmpeg(cmd, timeout=900, label="concat_wavs_copy")
        try:
            return ok and os.path.isfile(output_wav) and os.path.getsize(output_wav) > 44
        except OSError:
            return False
    finally:
        if list_path:
            try:
                os.unlink(list_path)
            except OSError:
                pass


def _normalize_part_to_wav(src: str, dst: str, *, timeout: int = 600) -> bool:
    """Decode any supported input to stereo 44.1 kHz PCM WAV (one file at a time)."""
    cmd = [
        "ffmpeg",
        "-y",
        "-nostdin",
        "-hide_banner",
        "-loglevel",
        "error",
        "-i",
        src,
        "-ar",
        "44100",
        "-ac",
        "2",
        "-c:a",
        "pcm_s16le",
        dst,
    ]
    if not _run_ffmpeg(cmd, timeout=timeout, label=f"normalize {os.path.basename(src)}"):
        return False
    try:
        return os.path.isfile(dst) and os.path.getsize(dst) > 44
    except OSError:
        return False


def _encode_concat_to_mp3(normalized_paths: List[str], output_mp3: str) -> bool:
    list_path = ""
    try:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False, encoding="utf-8") as f:
            list_path = f.name
            _write_concat_list(normalized_paths, list_path)
        os.makedirs(os.path.dirname(output_mp3) or ".", exist_ok=True)
        cmd = [
            "ffmpeg",
            "-y",
            "-nostdin",
            "-hide_banner",
            "-loglevel",
            "error",
            "-f",
            "concat",
            "-safe",
            "0",
            "-i",
            list_path,
            "-c:a",
            "libmp3lame",
            "-q:a",
            "4",
            output_mp3,
        ]
        return _run_ffmpeg(cmd, timeout=7200, label=f"encode_mp3 {os.path.basename(output_mp3)}")
    finally:
        if list_path:
            try:
                os.unlink(list_path)
            except OSError:
                pass


def _merge_inputs_single_pass(inputs: List[str], output_mp3: str, *, timeout: int = 7200) -> bool:
    """
    Resample each input to stereo 44.1 kHz and concat in one ffmpeg pass.
    Avoids writing a full-length normalized PCM WAV (~1 GB for long voice jobs), which OOMs small VPS hosts.
    """
    if not inputs:
        return False
    n = len(inputs)
    labels = []
    parts: List[str] = []
    for i in range(n):
        label = f"a{i}"
        labels.append(f"[{label}]")
        parts.append(
            f"[{i}:a]aresample=44100,aformat=sample_fmts=s16:channel_layouts=stereo[{label}]"
        )
    parts.append(f"{''.join(labels)}concat=n={n}:v=0:a=1[out]")
    filter_complex = ";".join(parts)
    cmd: List[str] = [
        "ffmpeg",
        "-y",
        "-nostdin",
        "-hide_banner",
        "-loglevel",
        "error",
    ]
    for path in inputs:
        cmd.extend(["-i", path])
    cmd.extend(
        [
            "-filter_complex",
            filter_complex,
            "-map",
            "[out]",
            "-threads",
            "1",
            "-c:a",
            "libmp3lame",
            "-q:a",
            "4",
            output_mp3,
        ]
    )
    os.makedirs(os.path.dirname(output_mp3) or ".", exist_ok=True)
    return _run_ffmpeg(cmd, timeout=timeout, label=f"single_pass_merge → {os.path.basename(output_mp3)}")


def _merge_with_ffmpeg_batch(
    intro_mp3: Optional[str],
    segment_wav_paths: List[str],
    outro_mp3: Optional[str],
    output_mp3: str,
    tmpdir: str,
    *,
    cancel_check: Optional[Callable[[], bool]] = None,
    progress_callback: Optional[Callable[[str], None]] = None,
) -> bool:
    """Fast path: concat TTS WAVs with stream copy, then single-pass resample+concat+MP3 encode."""
    from webapp.debug_session_log import debug_log

    for path in segment_wav_paths:
        if not path or not os.path.isfile(path):
            logger.error("Missing segment WAV: %s", path)
            return False

    if cancel_check and cancel_check():
        return False

    if progress_callback:
        progress_callback(f"Concatenating {len(segment_wav_paths)} segment WAVs (stream copy)…")

    segments_raw = os.path.join(tmpdir, "segments_raw.wav")
    if not _concat_wavs_copy(segment_wav_paths, segments_raw):
        debug_log(
            "H8",
            "audio_merge.py:_merge_with_ffmpeg_batch",
            "segment_concat_failed",
            {"segment_count": len(segment_wav_paths)},
        )
        return False

    if cancel_check and cancel_check():
        return False

    merge_inputs: List[str] = []
    if intro_mp3 and os.path.isfile(intro_mp3):
        merge_inputs.append(intro_mp3)
    merge_inputs.append(segments_raw)
    if outro_mp3 and os.path.isfile(outro_mp3):
        merge_inputs.append(outro_mp3)

    if not merge_inputs:
        logger.error("No audio content to merge")
        return False

    try:
        segments_bytes = os.path.getsize(segments_raw)
    except OSError:
        segments_bytes = 0

    # #region agent log
    debug_log(
        "H9",
        "audio_merge.py:_merge_with_ffmpeg_batch",
        "single_pass_merge_start",
        {
            "segment_count": len(segment_wav_paths),
            "merge_input_count": len(merge_inputs),
            "segments_raw_bytes": segments_bytes,
        },
    )
    # #endregion

    if progress_callback:
        progress_callback(
            f"Encoding final MP3 (single pass: intro + {len(segment_wav_paths)} segments + outro)… "
            "This may take several minutes on a small server."
        )

    if not _merge_inputs_single_pass(merge_inputs, output_mp3):
        debug_log(
            "H9",
            "audio_merge.py:_merge_with_ffmpeg_batch",
            "single_pass_merge_failed",
            {"segment_count": len(segment_wav_paths), "segments_raw_bytes": segments_bytes},
        )
        return False

    logger.info(
        "Merged voice MP3 via ffmpeg (batch single-pass): %s — %d segment(s)",
        output_mp3,
        len(segment_wav_paths),
    )
    debug_log(
        "H9",
        "audio_merge.py:_merge_with_ffmpeg_batch",
        "single_pass_merge_succeeded",
        {"output_mp3": output_mp3, "segment_count": len(segment_wav_paths)},
    )
    return True


def _merge_with_ffmpeg(
    intro_mp3: Optional[str],
    segment_wav_paths: List[str],
    outro_mp3: Optional[str],
    output_mp3: str,
    *,
    cancel_check: Optional[Callable[[], bool]] = None,
    progress_callback: Optional[Callable[[str], None]] = None,
) -> bool:
    """Normalize parts to PCM WAV, concat, then encode MP3 (low memory, mixed formats safe)."""
    from webapp.debug_session_log import debug_log

    for path in segment_wav_paths:
        if not path or not os.path.isfile(path):
            logger.error("Missing segment WAV: %s", path)
            return False

    paths: List[str] = []
    if intro_mp3 and os.path.isfile(intro_mp3):
        paths.append(intro_mp3)
    paths.extend(segment_wav_paths)
    if outro_mp3 and os.path.isfile(outro_mp3):
        paths.append(outro_mp3)

    if not paths:
        logger.error("No audio content to merge")
        return False

    tmpdir = tempfile.mkdtemp(prefix="voice_merge_")
    list_path = ""
    try:
        use_batch = len(segment_wav_paths) >= _FFMPEG_MERGE_MIN_SEGMENTS
        if use_batch:
            return _merge_with_ffmpeg_batch(
                intro_mp3,
                segment_wav_paths,
                outro_mp3,
                output_mp3,
                tmpdir,
                cancel_check=cancel_check,
                progress_callback=progress_callback,
            )

        normalized: List[str] = []
        total = len(paths)
        for i, path in enumerate(paths):
            if cancel_check and cancel_check():
                logger.info("Merge cancelled by user at part %s/%s", i, total)
                return False
            norm = os.path.join(tmpdir, f"part_{i:04d}.wav")
            if not _normalize_part_to_wav(path, norm):
                debug_log(
                    "H4",
                    "audio_merge.py:_merge_with_ffmpeg:normalize_fail",
                    "normalize_part_failed",
                    {
                        "index": i,
                        "basename": os.path.basename(path),
                        "size_bytes": os.path.getsize(path) if os.path.isfile(path) else 0,
                    },
                )
                return False
            normalized.append(norm)
            if progress_callback and ((i + 1) % 20 == 0 or i + 1 == total):
                progress_callback(f"Normalized {i + 1}/{total} audio parts…")

        if progress_callback:
            progress_callback(f"Encoding final MP3 from {total} parts…")

        if not _encode_concat_to_mp3(normalized, output_mp3):
            debug_log(
                "H4",
                "audio_merge.py:_merge_with_ffmpeg:fail",
                "ffmpeg_merge_failed",
                {"part_count": len(paths)},
            )
            return False

        logger.info(
            "Merged voice MP3 via ffmpeg: %s — %d part(s)",
            output_mp3,
            len(paths),
        )
        debug_log(
            "H4",
            "audio_merge.py:_merge_with_ffmpeg:ok",
            "ffmpeg_merge_succeeded",
            {"output_mp3": output_mp3, "part_count": len(paths)},
        )
        return True
    except Exception as e:
        logger.error("ffmpeg merge error: %s", e)
        return False
    finally:
        if list_path:
            try:
                os.unlink(list_path)
            except OSError:
                pass
        try:
            shutil.rmtree(tmpdir, ignore_errors=True)
        except OSError:
            pass


def _merge_with_pydub(
    intro_mp3: Optional[str],
    segment_wav_paths: List[str],
    outro_mp3: Optional[str],
    output_mp3: str,
) -> bool:
    """In-memory merge — fine for a handful of short segments only."""
    try:
        from pydub import AudioSegment
    except ImportError:
        logger.error("pydub is not installed")
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


def merge_voice_tracks(
    intro_mp3: Optional[str],
    segment_wav_paths: List[str],
    outro_mp3: Optional[str],
    output_mp3: str,
    *,
    require_intro_outro: bool = True,
    cancel_check: Optional[Callable[[], bool]] = None,
    progress_callback: Optional[Callable[[str], None]] = None,
) -> bool:
    """
    Concatenate intro MP3 + segment WAVs + outro MP3 into one MP3 file.
    Returns True on success.
    """
    if require_intro_outro:
        if not intro_mp3 or not os.path.isfile(intro_mp3):
            logger.error("Intro MP3 not found: %s", intro_mp3)
            return False
        if not outro_mp3 or not os.path.isfile(outro_mp3):
            logger.error("Outro MP3 not found: %s", outro_mp3)
            return False

    ffmpeg_ok = _ffmpeg_available()
    use_ffmpeg = len(segment_wav_paths) >= _FFMPEG_MERGE_MIN_SEGMENTS and ffmpeg_ok
    # #region agent log
    from webapp.debug_session_log import debug_log

    debug_log(
        "H2",
        "audio_merge.py:merge_voice_tracks:branch",
        "merge_branch_selected",
        {
            "segment_count": len(segment_wav_paths),
            "ffmpeg_available": ffmpeg_ok,
            "use_ffmpeg": use_ffmpeg,
            "min_for_ffmpeg": _FFMPEG_MERGE_MIN_SEGMENTS,
            "output_mp3": output_mp3,
        },
    )
    # #endregion
    if use_ffmpeg:
        return _merge_with_ffmpeg(
            intro_mp3,
            segment_wav_paths,
            outro_mp3,
            output_mp3,
            cancel_check=cancel_check,
            progress_callback=progress_callback,
        )
    if len(segment_wav_paths) >= _FFMPEG_MERGE_MIN_SEGMENTS and not ffmpeg_ok:
        logger.warning(
            "ffmpeg not found; falling back to pydub for %d segments (may use a lot of RAM)",
            len(segment_wav_paths),
        )
    pydub_ok = _merge_with_pydub(intro_mp3, segment_wav_paths, outro_mp3, output_mp3)
    # #region agent log
    debug_log(
        "H2",
        "audio_merge.py:merge_voice_tracks:pydub_result",
        "pydub_merge_finished",
        {"ok": pydub_ok, "segment_count": len(segment_wav_paths)},
    )
    # #endregion
    return pydub_ok


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

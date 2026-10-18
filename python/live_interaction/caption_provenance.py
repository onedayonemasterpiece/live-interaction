"""Bounded provenance for optional native captions, never a guessed turn clock.

Gemini documents offsets relative to the audio, but does not specify the clock
across activityEnd/audioStreamEnd/connection renewal. The default is deliberately
unverified. A consumer may enable connection_pcm only after recording a real
multi-utterance and reconnect acceptance for the exact provider configuration.
"""
from __future__ import annotations

import hashlib
import re
from collections import deque
from dataclasses import dataclass

SAMPLE_NS = 62_500  # PCM16 mono at 16 kHz.
MAX_SPANS = 128
_DURATION = re.compile(r"^(0|[1-9][0-9]{0,9})(?:\.([0-9]{1,9}))?s$")
_SCOPE_IDS = ("session_id", "conversation_id", "source_id", "utterance_id", "turn_id")


def caption_scope(value):
    if not isinstance(value, dict):
        return None
    if any(not isinstance(value.get(key), str) or not 1 <= len(value[key]) <= 200
           for key in _SCOPE_IDS):
        return None
    sequence = value.get("utterance_sequence")
    if isinstance(sequence, bool) or not isinstance(sequence, int) or sequence < 1:
        return None
    return {**{key: value[key] for key in _SCOPE_IDS}, "utterance_sequence": sequence}


def _duration_ns(value):
    if not isinstance(value, str):
        return None
    match = _DURATION.fullmatch(value)
    if match is None:
        return None
    return int(match[1]) * 1_000_000_000 + int((match[2] or "").ljust(9, "0"))


def _pcm_samples(message):
    # Sidecars supply canonical base64. Its length yields the wire PCM sample
    # count without decoding/copying every 100 ms buffer a second time.
    data = message.get("data")
    if not isinstance(data, str) or not data or len(data) % 4:
        return None
    padding = len(data) - len(data.rstrip("="))
    if padding > 2:
        return None
    size = len(data) // 4 * 3 - padding
    return size // 2 if size > 0 and size % 2 == 0 else None


@dataclass
class _Span:
    start: int
    end: int
    scope: dict | None
    capture_start_us: int | None
    capture_end_us: int | None


class CaptionAudioLedger:
    """Tracks audio only after successful provider writes; bounds old turn spans."""

    def __init__(self, *, offset_clock="unverified", diagnostics=False, max_spans=MAX_SPANS):
        if offset_clock not in {"unverified", "connection_pcm"}:
            raise ValueError("unsupported_caption_offset_clock")
        if not isinstance(max_spans, int) or not 1 <= max_spans <= MAX_SPANS:
            raise ValueError("invalid_caption_ledger_bound")
        self.offset_clock = offset_clock
        self.diagnostics = diagnostics is True
        self.provider_connection_generation = 0
        self.sent_pcm_samples = 0
        self._spans = deque(maxlen=max_spans)
        self._clock_valid = True

    def connected(self):
        self.provider_connection_generation += 1
        self.sent_pcm_samples = 0
        self._spans.clear()
        self._clock_valid = True

    def sent(self, message):
        if message.get("type") == "audio_stream_end":
            # This closes/reopens a provider audio stream. Its clock semantics
            # need a separate receipt; activityEnd alone is only a turn boundary.
            self._clock_valid = False
            return None
        if message.get("type") != "audio":
            return None
        samples = _pcm_samples(message)
        if samples is None:
            self._clock_valid = False
            return None
        start = self.sent_pcm_samples
        self.sent_pcm_samples += samples
        scope = caption_scope(message.get("caption_scope"))
        capture_start = message.get("capture_start_us")
        capture_end = message.get("capture_end_us")
        if (isinstance(capture_start, bool) or not isinstance(capture_start, int)
                or capture_start < 0 or isinstance(capture_end, bool)
                or not isinstance(capture_end, int) or capture_end <= capture_start
                or capture_end - capture_start != samples * 1_000_000 // 16_000):
            scope, capture_start, capture_end = None, None, None
        previous = self._spans[-1] if self._spans else None
        if (previous is not None and previous.end == start and previous.scope == scope
                and previous.capture_end_us == capture_start):
            previous.end = self.sent_pcm_samples
            previous.capture_end_us = capture_end
        else:
            self._spans.append(_Span(start, self.sent_pcm_samples, scope, capture_start, capture_end))
        return {
            "type": "caption_audio_sent",
            "provider_connection_generation": self.provider_connection_generation,
            "stream_start_sample": start,
            "stream_end_sample": self.sent_pcm_samples,
            "capture_start_us": capture_start,
            "capture_end_us": capture_end,
            "caption_scope": scope,
        }

    def _assign(self, start_offset, end_offset):
        if self.offset_clock != "connection_pcm" or not self._clock_valid:
            return None
        start, end = _duration_ns(start_offset), _duration_ns(end_offset)
        if start is None or end is None or end <= start or end > self.sent_pcm_samples * SAMPLE_NS:
            return None
        matching = [span for span in self._spans
                    if start >= span.start * SAMPLE_NS and end <= span.end * SAMPLE_NS]
        # A range spanning distinct turns, an evicted range or a missing source
        # cannot be attached to whichever user turn happens to be current.
        return matching[0].scope if len(matching) == 1 else None

    def transcript(self, kind, transcription):
        text = transcription["text"]
        start = transcription.get("startOffset")
        end = transcription.get("endOffset")
        # Preserve the original Duration representation for the canary. Do not
        # coerce a provider field of another type into a plausible clock.
        start = start if isinstance(start, str) else None
        end = end if isinstance(end, str) else None
        scope = self._assign(start, end)
        probe = {
            "type": "caption_timing_probe",
            "provider_connection_generation": self.provider_connection_generation,
            "transcript_kind": kind,
            "start_offset": start,
            "end_offset": end,
            "sent_pcm_samples": self.sent_pcm_samples,
            "offset_clock": self.offset_clock,
            "assigned": scope is not None,
            "text_length": len(text),
            "text_sha256": hashlib.sha256(" ".join(text.casefold().split()).encode()).hexdigest(),
        }
        if isinstance(transcription.get("finished"), bool):
            probe["observed_finished"] = transcription["finished"]
        event = {"type": kind, "text": text, "caption_timing": probe}
        if scope is not None:
            event["caption_scope"] = dict(scope)
        return event

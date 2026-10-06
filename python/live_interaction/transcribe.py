"""Gemini 3.5 Transcribe Live sidecar transport.

This module is intentionally transcription-only. It has no tools, dialogue
history, semantic routing, or audio output. Product hosts may fan out already
accepted PCM to it for speculative UI captions while the conversational Live
model remains authoritative.
"""
from __future__ import annotations

import asyncio
import json
import re
from urllib.parse import quote

from .provider import (
    ENDPOINT,
    _guard_check,
    _guarded_recv,
    _guarded_send,
    _is_resource_failure,
    default_key,
    read_line,
)

MODEL = "gemini-3.5-transcribe-live"
MAX_VOCABULARY = 1000


def setup_config(configuration=None):
    configuration = configuration or {}
    transcription = configuration.get("input_audio_transcription") or {}
    if not isinstance(transcription, dict):
        raise ValueError("Unsupported input_audio_transcription")
    transcription = dict(transcription)
    unknown = set(transcription) - {"languageCodes", "customVocabulary", "mode"}
    if unknown:
        raise ValueError("Unsupported input_audio_transcription field")

    languages = transcription.get("languageCodes")
    if languages is not None and (
        not isinstance(languages, list)
        or len(languages) > 8
        or any(not isinstance(item, str) or len(item) > 32 for item in languages)
    ):
        raise ValueError("Unsupported transcription languageCodes")

    vocabulary = transcription.get("customVocabulary")
    if vocabulary is not None and (
        not isinstance(vocabulary, list)
        or len(vocabulary) > MAX_VOCABULARY
        or any(not isinstance(item, str) or not item.strip() or len(item) > 120 for item in vocabulary)
    ):
        raise ValueError("Unsupported transcription customVocabulary")

    mode = transcription.get("mode")
    if mode is not None and mode not in {"VERBATIM", "SMART"}:
        raise ValueError("Unsupported transcription mode")

    setup = {
        "model": "models/" + MODEL,
        "generationConfig": {"responseModalities": ["TEXT"]},
        "inputAudioTranscription": transcription,
    }
    if configuration.get("manual_activity_detection", True):
        setup["realtimeInputConfig"] = {
            "automaticActivityDetection": {"disabled": True}
        }
    return {"setup": setup}


def input_payload(message):
    if not isinstance(message, dict):
        raise ValueError("invalid_transcribe_input")
    kind = message.get("type")
    if kind == "activity_start":
        return {"realtimeInput": {"activityStart": {}}}
    if kind == "activity_end":
        return {"realtimeInput": {"activityEnd": {}}}
    if kind == "audio_stream_end":
        return {"realtimeInput": {"audioStreamEnd": True}}
    if kind == "audio":
        data = message.get("data")
        if not isinstance(data, str) or not data or len(data) > 16_000:
            raise ValueError("invalid_transcribe_audio")
        return {
            "realtimeInput": {
                "audio": {
                    "data": data,
                    "mimeType": "audio/pcm;rate=16000",
                }
            }
        }
    # Snapshots/reconfigure/text/tool traffic are semantic inputs for the primary
    # Live session and must never be mirrored into the caption sidecar.
    return None


def handle_server_message(obj, emit):
    if "error" in obj:
        raise RuntimeError(json.dumps(obj["error"], ensure_ascii=False))
    content = obj.get("serverContent") or {}
    interim = (content.get("interimInputTranscription") or {}).get("text")
    if isinstance(interim, str) and interim.strip():
        emit({"type": "interim_input_transcript", "text": interim})
    final = (content.get("inputTranscription") or {}).get("text")
    if isinstance(final, str) and final.strip():
        emit({"type": "input_transcript", "text": final})
    if obj.get("usageMetadata"):
        emit({"type": "usage", "metadata": obj["usageMetadata"]})
    if content.get("turnComplete"):
        emit({"type": "turn_complete"})


def _provider_error_event(exc, key, *, stopped):
    if stopped:
        return None
    safe = str(exc).replace(key, "[REDACTED]").replace(quote(key, safe=""), "[REDACTED]")
    return {
        "type": "error",
        "code": type(exc).__name__,
        "message": re.sub(r"AIza[A-Za-z0-9_-]{20,}", "[REDACTED]", safe)[:500],
    }


async def run(*, load_key=default_key, reader=None, on_event=None, resource_guard=None):
    from websockets import connect

    emit = on_event or (lambda _event: None)
    loop = asyncio.get_running_loop()
    if reader is None:
        import sys

        reader = asyncio.StreamReader(limit=1024 * 1024)
        await loop.connect_read_pipe(
            lambda: asyncio.StreamReaderProtocol(reader), sys.stdin.buffer
        )

    start = await read_line(reader)
    if not start or start.get("type") != "start":
        raise ValueError("missing_start")
    if start.get("model") != MODEL:
        raise ValueError("invalid_model")

    _guard_check(resource_guard)
    key = resource_guard.key() if resource_guard is not None else load_key()
    if not key:
        emit({
            "type": "error",
            "code": "LIVE_UNAVAILABLE",
            "message": "application_google_binding_missing",
        })
        return

    stopped = False
    try:
        async with connect(
            ENDPOINT + "?key=" + quote(key, safe=""),
            open_timeout=15,
            close_timeout=2,
            max_size=4 * 1024 * 1024,
        ) as ws:
            await _guarded_send(
                ws,
                setup_config(start.get("configuration")) ,
                resource_guard,
            )
            async with asyncio.timeout(20):
                while True:
                    obj = json.loads(await _guarded_recv(ws, resource_guard))
                    if "setupComplete" in obj:
                        break
                    handle_server_message(obj, emit)
            emit({"type": "ready", "model": MODEL})

            async def sender():
                nonlocal stopped
                while not stopped:
                    message = await read_line(reader)
                    if message is None or message.get("type") == "stop":
                        stopped = True
                        await ws.close()
                        return
                    payload = input_payload(message)
                    if payload is not None:
                        await _guarded_send(ws, payload, resource_guard)

            sender_task = asyncio.create_task(sender())
            try:
                while not stopped:
                    obj = json.loads(await _guarded_recv(ws, resource_guard))
                    if obj.get("goAway"):
                        emit({
                            "type": "go_away",
                            "time_left": (obj.get("goAway") or {}).get("timeLeft"),
                        })
                        break
                    handle_server_message(obj, emit)
            finally:
                stopped = True
                sender_task.cancel()
                await asyncio.gather(sender_task, return_exceptions=True)
    except Exception as exc:
        if _is_resource_failure(exc):
            raise
        event = _provider_error_event(exc, key, stopped=stopped)
        if event is not None:
            emit(event)

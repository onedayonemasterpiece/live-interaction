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
import time
from collections import deque
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
from .reconnect import ReconnectBudget
from .caption_provenance import CaptionAudioLedger

MODEL = "gemini-3.5-transcribe-live"
MAX_VOCABULARY = 1000
# Same byte ceiling as the host's bounded provider reader. This queue preserves
# only the initial setup prefix, never speech accumulated after a ready session.
MAX_STARTUP_INPUT_BYTES = 768 * 1024


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
    if configuration.get("native_caption_provenance") is not None:
        if mode != "VERBATIM":
            raise ValueError("native_caption_timestamps_require_verbatim")
        setup.setdefault("realtimeInputConfig", {})["interimTranscriptTimestampEnabled"] = True
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


def handle_server_message(obj, emit, *, caption_ledger=None):
    if "error" in obj:
        raise RuntimeError(json.dumps(obj["error"], ensure_ascii=False))
    content = obj.get("serverContent") or {}
    for field, kind in (("interimInputTranscription", "interim_input_transcript"),
                        ("inputTranscription", "input_transcript")):
        transcription = content.get(field) or {}
        text = transcription.get("text") if isinstance(transcription, dict) else None
        if isinstance(text, str) and text.strip():
            emit(caption_ledger.transcript(kind, transcription) if caption_ledger is not None
                 else {"type": kind, "text": text})
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


async def run(*, load_key=default_key, reader=None, on_event=None, resource_guard=None,
              reconnect_budget=None):
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

    provenance = (start.get("configuration") or {}).get("native_caption_provenance")
    caption_ledger = None
    if provenance is not None:
        if not isinstance(provenance, dict):
            raise ValueError("invalid_native_caption_provenance")
        caption_ledger = CaptionAudioLedger(
            offset_clock=provenance.get("offset_clock", "unverified"),
            diagnostics=provenance.get("diagnostics", False),
        )

    def record_sent(message):
        if caption_ledger is not None:
            event = caption_ledger.sent(message)
            if event is not None and caption_ledger.diagnostics:
                emit(event)

    _guard_check(resource_guard)
    key = resource_guard.key() if resource_guard is not None else load_key()
    if not key:
        emit({
            "type": "error",
            "code": "LIVE_UNAVAILABLE",
            "message": "application_google_binding_missing",
        })
        return

    budget = reconnect_budget or ReconnectBudget()
    stopped = False
    connected_once = False
    activity_open = False
    current_ws = None
    drop_inputs_before = 0
    loss_reported = False
    send_lock = asyncio.Lock()
    startup_inputs = deque()
    startup_bytes = 0

    def dropped(kind):
        nonlocal loss_reported
        if not loss_reported:
            loss_reported = True
            emit({"type": "input_dropped", "reason": "provider_reconnect", "input_type": kind})

    async def sender():
        # Keep consuming the bounded input reader through provider renewal. Stop
        # cancels a pending connect/backoff, and outage PCM is never queued for a
        # later realtime connection (durable replay is the product's separate path).
        nonlocal stopped, activity_open, current_ws, startup_bytes
        while not stopped:
            message = await read_line(reader)
            if message is None or message.get("type") == "stop":
                stopped = True
                return
            payload = input_payload(message)
            if payload is None:
                continue
            kind = message.get("type")
            if not connected_once:
                # Preserve the old useful startup buffering while continuing to
                # read Stop. The first connection drains this prefix only once.
                size = len(json.dumps(payload).encode("utf-8")) + len(json.dumps(message).encode("utf-8"))
                if startup_bytes + size > MAX_STARTUP_INPUT_BYTES:
                    raise ValueError("caption_startup_backpressure")
                startup_inputs.append((payload, size, message))
                startup_bytes += size
                if kind == "activity_start":
                    activity_open = True
                elif kind in ("activity_end", "audio_stream_end"):
                    activity_open = False
                continue
            async with send_lock:
                if kind == "activity_start":
                    activity_open = True
                elif kind in ("activity_end", "audio_stream_end"):
                    activity_open = False
                ws = current_ws
                queued_at = message.get("queued_at")
                stale = (isinstance(queued_at, (int, float)) and drop_inputs_before
                         and queued_at <= drop_inputs_before)
                if ws is None or stale:
                    dropped(kind)
                    continue
                try:
                    sent = await _guarded_send(ws, payload, resource_guard,
                                               before_write=lambda: not stopped and current_ws is ws)
                    if not sent:
                        dropped(kind)
                    else:
                        record_sent(message)
                except Exception as exc:
                    if _is_resource_failure(exc):
                        raise
                    current_ws = None
                    dropped(kind)
                    # Wake the receiving task even if only the write side failed.
                    await ws.close()

    async def connections():
        nonlocal connected_once, current_ws, drop_inputs_before, loss_reported, startup_bytes
        while not stopped:
            planned = False
            try:
                async with connect(
                    ENDPOINT + "?key=" + quote(key, safe=""),
                    open_timeout=15,
                    close_timeout=2,
                    max_size=4 * 1024 * 1024,
                ) as ws:
                    if caption_ledger is not None:
                        caption_ledger.connected()
                    await _guarded_send(
                        ws,
                        setup_config(start.get("configuration")),
                        resource_guard,
                        before_write=lambda: not stopped,
                    )
                    async with asyncio.timeout(20):
                        while True:
                            obj = json.loads(await _guarded_recv(ws, resource_guard))
                            if "setupComplete" in obj:
                                break
                            handle_server_message(obj, emit, caption_ledger=caption_ledger)
                    if stopped:
                        return
                    budget.succeeded()
                    if not connected_once:
                        # Sender keeps accepting Stop and bounded startup input
                        # while admission or network writes await. No other task
                        # can write audio until this initial ordered prefix drains.
                        while startup_inputs and not stopped:
                            payload, size, message = startup_inputs.popleft()
                            startup_bytes -= size
                            if not await _guarded_send(ws, payload, resource_guard,
                                                       before_write=lambda: not stopped):
                                return
                            record_sent(message)
                    async with send_lock:
                        if connected_once and activity_open:
                            # Only reopen the current activity; old PCM stays dropped.
                            await _guarded_send(ws, {"realtimeInput": {"activityStart": {}}}, resource_guard,
                                               before_write=lambda: not stopped)
                        if stopped:
                            return
                        if connected_once:
                            drop_inputs_before = round(time.time() * 1000)
                        current_ws = ws
                        loss_reported = False
                    emit({"type": "resumed" if connected_once else "ready", "model": MODEL})
                    connected_once = True
                    try:
                        while not stopped:
                            obj = json.loads(await _guarded_recv(ws, resource_guard))
                            if obj.get("goAway"):
                                planned = True
                                emit({
                                    "type": "go_away",
                                    "time_left": (obj.get("goAway") or {}).get("timeLeft"),
                                })
                                break
                            # A response may race the completion of its audio
                            # write. Read the ledger only after that write records
                            # a successful send, never from queued/failed audio.
                            async with send_lock:
                                handle_server_message(obj, emit, caption_ledger=caption_ledger)
                    finally:
                        current_ws = None
                if stopped:
                    return
                if not planned:
                    raise ConnectionError("caption_connection_closed")
            except Exception as exc:
                current_ws = None
                if stopped or _is_resource_failure(exc) or not connected_once:
                    raise
                message = str(exc).lower()
                if any(x in message for x in ("permission", "unauthorized", "api key", "quota")):
                    raise
                if not budget.allow():
                    raise
                if not planned:
                    budget.failed()
                emit({"type": "reconnecting", "planned": planned})
                await asyncio.sleep(budget.delay())
                continue
            if not budget.allow():
                raise ConnectionError("caption_renewal_budget_exhausted")
            emit({"type": "reconnecting", "planned": True})

    tasks = [asyncio.create_task(sender()), asyncio.create_task(connections())]
    try:
        done, _pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
        for task in done:
            await task
    except Exception as exc:
        if _is_resource_failure(exc):
            raise
        event = _provider_error_event(exc, key, stopped=stopped)
        if event is not None:
            emit(event)
    finally:
        stopped = True
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        startup_inputs.clear()

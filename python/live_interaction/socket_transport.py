"""Framework-neutral wl-live-v1 relay. ASGI/aiohttp supply three I/O callbacks.

No framework, credential resolver, product function or Google client is imported
here. Input is ordered; output is pushed with a byte-bounded queue. ACK means
relay admission, not model comprehension or successful product execution.
"""
from __future__ import annotations

import asyncio
import base64
import json
import re
import struct
import time
from urllib.parse import urlsplit

from .session_host import LiveError
from .socket_host import SOCKET_PROTOCOL

INPUT_MAGIC = 0x574C4131
OUTPUT_MAGIC = 0x574C4F31
MAX_PCM_BYTES = 11000
MAX_CONTROL_BYTES = 65536
MAX_OUTPUT_BYTES = 256 * 1024


def socket_ticket(protocols):
    values = list(protocols or [])
    tickets = [value[10:] for value in values if isinstance(value, str) and value.startswith("wl-ticket.")]
    if SOCKET_PROTOCOL not in values or len(tickets) != 1 or not re.fullmatch(r"[A-Za-z0-9_-]{20,512}", tickets[0]):
        raise LiveError("LIVE_SOCKET_TICKET", "A versioned socket protocol and ticket are required")
    return tickets[0]


def same_origin(origin, host):
    try:
        parsed = urlsplit(origin or "")
        return (parsed.scheme in {"http", "https"} and not parsed.username and not parsed.password
                and not parsed.query and not parsed.fragment and parsed.path in {"", "/"}
                and parsed.netloc.lower() == str(host or "").lower())
    except (TypeError, ValueError):
        return False


def decode_audio(data):
    if not isinstance(data, bytes) or not 14 <= len(data) <= MAX_PCM_BYTES + 12 or len(data) % 2:
        raise LiveError("LIVE_SOCKET_FRAME", "Invalid PCM frame length")
    magic, seq, age = struct.unpack("!III", data[:12])
    if magic != INPUT_MAGIC:
        raise LiveError("LIVE_SOCKET_FRAME", "Invalid PCM frame version")
    return seq, age, data[12:]


def encode_output(event):
    if event.get("type") != "audio":
        return json.dumps({"type": "event", "event": event}, ensure_ascii=False, separators=(",", ":"))
    try:
        raw = base64.b64decode(event.get("data", ""), validate=True)
        match = re.search(r"rate=(\d+)", str(event.get("mime_type") or ""))
        rate = int(match[1]) if match else 24000
        seq = int(event["seq"])
    except (ValueError, TypeError, KeyError, IndexError):
        raise LiveError("LIVE_SOCKET_OUTPUT", "Invalid provider PCM metadata") from None
    if not raw or len(raw) % 2 or not 8000 <= rate <= 96000 or not 0 < seq <= 0xFFFFFFFF:
        raise LiveError("LIVE_SOCKET_OUTPUT", "Invalid provider PCM metadata")
    return struct.pack("!III", OUTPUT_MAGIC, seq, rate) + raw


async def serve_socket(binding, *, receive, send, close, hello_timeout=2.0,
                       send_timeout=5.0, max_output_bytes=MAX_OUTPUT_BYTES):
    """Run one already-authorized, accepted socket.

    receive() -> str | bytes | None (disconnect), send(str | bytes),
    close(code, reason). Every callback is async. No HTTP polling is used.
    Caller validates Origin and rejects query credentials before consuming ticket.
    """
    raw_send = send
    send_lock = asyncio.Lock()
    async def ordered_send(payload):
        async with send_lock:
            return await raw_send(payload)
    send = ordered_send
    # Backlog, live events and acknowledgements share one serialized writer.
    queue = asyncio.Queue(maxsize=320)
    queued_bytes = 0
    last_sent = 0
    failed = asyncio.get_running_loop().create_future()
    tasks = []
    unsubscribe = lambda: None
    close_code, close_reason = 1000, "closed"
    fatal = False

    def enqueue(event):
        nonlocal queued_bytes
        if failed.done():
            return
        try:
            payload = encode_output(event)
            size = len(payload.encode("utf-8")) if isinstance(payload, str) else len(payload)
            if queued_bytes + size > max_output_bytes or queue.full():
                raise LiveError("LIVE_SOCKET_EGRESS_BACKPRESSURE", "Live output queue exceeded its bound")
            queue.put_nowait((event, payload, size))
            queued_bytes += size
        except Exception as exc:
            failed.set_result(exc if isinstance(exc, LiveError) else LiveError("LIVE_SOCKET_OUTPUT", "Invalid Live output"))

    async def write_loop():
        nonlocal queued_bytes, last_sent
        while True:
            event, payload, size = await queue.get()
            try:
                seq = int(event.get("seq") or 0)
                if seq > last_sent:
                    await asyncio.wait_for(send(payload), send_timeout)
                    last_sent = seq
                if event.get("type") == "closed":
                    return
            finally:
                queued_bytes -= size
                queue.task_done()

    async def read_loop():
        while True:
            frame = await receive()
            if frame is None:
                return
            if isinstance(frame, bytes):
                seq, age, pcm = decode_audio(frame)
                await binding.input({"audio_base64": base64.b64encode(pcm).decode("ascii")},
                                    frame_seq=seq, capture_age_ms=age, pcm_bytes=len(pcm))
                await asyncio.wait_for(send(json.dumps({"type": "audio_ack", "seq": seq,
                    "server_received_at": round(time.time() * 1000)})), send_timeout)
                continue
            if not isinstance(frame, str) or len(frame.encode("utf-8")) > MAX_CONTROL_BYTES:
                raise LiveError("LIVE_SOCKET_MESSAGE", "Live control frame exceeds its bound")
            try:
                message = json.loads(frame)
            except (TypeError, ValueError):
                raise LiveError("LIVE_SOCKET_MESSAGE", "Live control frame is invalid JSON") from None
            if not isinstance(message, dict):
                raise LiveError("LIVE_SOCKET_MESSAGE", "Live control frame must be an object")
            if message.get("type") == "ping":
                binding.heartbeat()
                await asyncio.wait_for(send('{"type":"pong"}'), send_timeout)
            elif message.get("type") == "stop":
                await binding.stop()
                return
            elif message.get("type") == "input" and isinstance(message.get("message"), dict):
                body = message["message"]
                if "audio_base64" in body:
                    raise LiveError("LIVE_SOCKET_FRAME", "WSS PCM must use binary frames")
                await binding.input(body)
            else:
                raise LiveError("LIVE_SOCKET_MESSAGE", "Unsupported Live control frame")

    try:
        first = await asyncio.wait_for(receive(), hello_timeout)
        if not isinstance(first, str) or len(first.encode("utf-8")) > 4096:
            raise LiveError("LIVE_SOCKET_HELLO", "Live hello is required")
        try:
            hello = json.loads(first)
        except (ValueError, TypeError):
            raise LiveError("LIVE_SOCKET_HELLO", "Live hello is invalid") from None
        if not isinstance(hello, dict) or hello.get("type") != "hello" or hello.get("protocol") != SOCKET_PROTOCOL:
            raise LiveError("LIVE_SOCKET_HELLO", "Live hello protocol does not match")
        if hello.get("attempt_id") != binding.attempt_id:
            raise LiveError("LIVE_SOCKET_ATTEMPT", "Live attempt does not match")
        cursor = hello.get("cursor", 0)
        if isinstance(cursor, bool) or not isinstance(cursor, int) or not 0 <= cursor < binding.session.next_seq:
            raise LiveError("LIVE_SOCKET_CURSOR", "Live cursor is invalid")
        generation = hello.get("connection_generation")
        binding.connect(generation)
        last_sent = cursor
        # Do not yield between subscription and backlog capture. Both use the
        # host event loop, so live events cannot overtake older queued events.
        unsubscribe = binding.subscribe(enqueue)
        page = binding.events(cursor)
        while True:
            if page.get("gap"):
                raise LiveError("LIVE_SOCKET_EVENT_GAP", "Live event backlog is incomplete")
            for event in page["events"]:
                enqueue(event)
            if not page.get("has_more"):
                break
            page = binding.events(page["cursor"])
        await asyncio.wait_for(send(json.dumps({"type": "hello_ack", "protocol": SOCKET_PROTOCOL,
                                               "connection_generation": generation})), send_timeout)
        tasks = [asyncio.create_task(read_loop()), asyncio.create_task(write_loop())]
        done, _ = await asyncio.wait([*tasks, failed], return_when=asyncio.FIRST_COMPLETED)
        if failed in done:
            raise failed.result()
        for task in done:
            task.result()
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        fatal = True
        close_code = 1002 if isinstance(exc, LiveError) else 1011
        close_reason = exc.code if isinstance(exc, LiveError) else "LIVE_SOCKET_IO"
        binding.host.diagnostic(binding.session, "socket_failed", code=close_reason,
                                connection_generation=binding.state.generation, queue_bytes=queued_bytes)
    finally:
        unsubscribe()
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        if fatal:
            await binding.stop()
        await binding.close()
        try:
            await asyncio.wait_for(close(close_code, close_reason), 1.0)
        except Exception:
            pass

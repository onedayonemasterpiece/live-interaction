"""Framework-neutral wl-live-v1/v2 relay. ASGI/aiohttp supply three I/O callbacks.

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
STARTUP_CATCHUP_FLAG = 1 << 31
MAX_PCM_BYTES = 11000
MAX_CONTROL_BYTES = 65536
MAX_OUTPUT_BYTES = 256 * 1024

# A transient socket failure detaches only this socket. The logical Live session
# survives for the client's bounded reconnect; damaged-turn fences still apply.
# Protocol, authorization and storage/domain failures stay fail-closed.
TRANSIENT_SOCKET_CODES = frozenset({
    "LIVE_SOCKET_IO",
    "LIVE_SOCKET_SEND_TIMEOUT",
    "LIVE_SOCKET_EGRESS_BACKPRESSURE",
    "LIVE_SOCKET_STALE_AUDIO",
    "LIVE_AUDIO_BACKPRESSURE",
    "LIVE_AUDIO_STALE",
    "LIVE_AUDIO_OUTPUT_ACK_TIMEOUT",
    "LIVE_AUDIO_OUTPUT_HISTORY",
})


class SocketIOError(LiveError):
    """Failure of the socket I/O callbacks themselves, not of Live input."""


def socket_ticket(protocols):
    values = list(protocols or [])
    tickets = [value[10:] for value in values if isinstance(value, str) and value.startswith("wl-ticket.")]
    if not any(value in values for value in (SOCKET_PROTOCOL, "wl-live-v2")) or len(tickets) != 1 or not re.fullmatch(r"[A-Za-z0-9_-]{20,512}", tickets[0]):
        raise LiveError("LIVE_SOCKET_TICKET", "A versioned socket protocol and ticket are required")
    return tickets[0]


def socket_protocol(protocols, *, allow_v2=False):
    """Select the accepted subprotocol without changing Origin/cookie/ticket auth."""
    values = list(protocols or [])
    if allow_v2 and "wl-live-v2" in values:
        return "wl-live-v2"
    if SOCKET_PROTOCOL in values:
        return SOCKET_PROTOCOL
    raise LiveError("LIVE_SOCKET_PROTOCOL", "No supported Live socket protocol was offered")


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
    magic, seq, encoded_age = struct.unpack("!III", data[:12])
    if magic != INPUT_MAGIC:
        raise LiveError("LIVE_SOCKET_FRAME", "Invalid PCM frame version")
    startup_catchup = bool(encoded_age & STARTUP_CATCHUP_FLAG)
    age = encoded_age & (STARTUP_CATCHUP_FLAG - 1)
    return seq, age, startup_catchup, data[12:]


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
                       send_timeout=5.0, max_output_bytes=MAX_OUTPUT_BYTES, protocol=None):
    """Run one already-authorized, accepted socket.

    receive() -> str | bytes | None (disconnect), send(str | bytes),
    close(code, reason). Every callback is async. No HTTP polling is used.
    Caller validates Origin and rejects query credentials before consuming ticket.
    """
    raw_send, raw_receive = send, receive
    send_lock = asyncio.Lock()
    async def ordered_send(payload):
        async with send_lock:
            try:
                return await asyncio.wait_for(raw_send(payload), send_timeout)
            except asyncio.CancelledError:
                raise
            except (TimeoutError, asyncio.TimeoutError):
                raise SocketIOError("LIVE_SOCKET_SEND_TIMEOUT", "Live socket send timed out") from None
            except Exception:
                raise SocketIOError("LIVE_SOCKET_IO", "Live socket send failed") from None
    async def guarded_receive():
        try:
            return await raw_receive()
        except asyncio.CancelledError:
            raise
        except Exception:
            raise SocketIOError("LIVE_SOCKET_IO", "Live socket receive failed") from None
    send, receive = ordered_send, guarded_receive
    # Backlog, live events and acknowledgements share one serialized writer.
    queue = asyncio.Queue(maxsize=320)
    queued_bytes = 0
    last_sent = 0
    failed = asyncio.get_running_loop().create_future()
    tasks = []
    urgent_tasks = set()
    unsubscribe = lambda: None
    close_code, close_reason = 1000, "closed"
    fatal = False
    audio_v2 = None

    def enqueue(event):
        nonlocal queued_bytes
        if failed.done():
            return
        try:
            if audio_v2 is not None and event.get("type") == "interrupted":
                async def interrupt(response_id=binding.session.response_seq):
                    try:
                        await audio_v2.interrupt_output(response_id)
                    except Exception as error:
                        if not failed.done():
                            failed.set_result(error)
                task = asyncio.create_task(interrupt())
                urgent_tasks.add(task)
                task.add_done_callback(urgent_tasks.discard)
            if audio_v2 is not None:
                payload = None
                if event.get("type") == "audio":
                    # The independent archive already owns the provider PCM.
                    # v2 output() needs only this sequence to await its durable
                    # compressed original. Do not retain/count another raw PCM
                    # backlog while waiting for real device-durable receipts.
                    # Copy, never mutate the archive/ring's original event.
                    event = {"type": "audio", "seq": event["seq"],
                             "response_id": event["response_id"]}
                size = len(json.dumps(event).encode("utf-8"))
            else:
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
                if seq > last_sent or (audio_v2 is not None and event.get("type") == "audio"):
                    if audio_v2 is not None:
                        async for output in audio_v2.output(event):
                            await send(output)
                    else:
                        await send(payload)
                    last_sent = max(last_sent, seq)
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
                if audio_v2 is not None:
                    audio_v2.audio(frame)
                    continue
                seq, age, startup_catchup, pcm = decode_audio(frame)
                await binding.input({"audio_base64": base64.b64encode(pcm).decode("ascii")},
                                    frame_seq=seq, capture_age_ms=age, pcm_bytes=len(pcm),
                                    startup_catchup=startup_catchup)
                await send(json.dumps({"type": "audio_ack", "seq": seq,
                    "server_received_at": round(time.time() * 1000)}))
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
                await send('{"type":"pong"}')
            elif message.get("type") == "stop":
                await binding.stop()
                return
            elif audio_v2 is not None:
                audio_v2.control(message)
            elif message.get("type") == "input" and isinstance(message.get("message"), dict):
                body = message["message"]
                if "audio_base64" in body:
                    raise LiveError("LIVE_SOCKET_FRAME", "WSS PCM must use binary frames")
                await binding.input(body)
            else:
                raise LiveError("LIVE_SOCKET_MESSAGE", "Unsupported Live control frame")

    try:
        try:
            first = await asyncio.wait_for(receive(), hello_timeout)
        except (TimeoutError, asyncio.TimeoutError):
            # A slow mobile link is a transport condition, not a protocol violation.
            raise SocketIOError("LIVE_SOCKET_IO", "Live hello did not arrive in time") from None
        if not isinstance(first, str) or len(first.encode("utf-8")) > 4096:
            raise LiveError("LIVE_SOCKET_HELLO", "Live hello is required")
        try:
            hello = json.loads(first)
        except (ValueError, TypeError):
            raise LiveError("LIVE_SOCKET_HELLO", "Live hello is invalid") from None
        if (not isinstance(hello, dict) or hello.get("type") != "hello" or
                hello.get("protocol") not in (SOCKET_PROTOCOL, "wl-live-v2") or
                (protocol is not None and hello.get("protocol") != protocol)):
            raise LiveError("LIVE_SOCKET_HELLO", "Live hello protocol does not match")
        negotiated_protocol = hello["protocol"]
        v2_metadata = {}
        if negotiated_protocol == "wl-live-v2":
            services = getattr(binding.host, "audio_v2", None)
            if services is None:
                raise LiveError("LIVE_AUDIO_CODECS_UNAVAILABLE", "Compressed v2 audio is not configured")
            from .audio_socket import SocketAudioV2
            audio_v2 = SocketAudioV2(binding, services, send, failed)
            v2_metadata = audio_v2.negotiate(hello)
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
        event_gap = False
        backlog = []
        while True:
            # A reattach after a long gap must not destroy the conversation.
            # Deliver the retained backlog and tell the client what was missed.
            event_gap = event_gap or bool(page.get("gap"))
            backlog.extend(page["events"])
            if not page.get("has_more"):
                break
            page = binding.events(page["cursor"])
        # Text/state events are always replayed. Old model audio is not: after a
        # gap, stale speech must not play and must not overflow the egress bound.
        audio_budget = max_output_bytes // 2
        keep_audio = set()
        session = binding.session
        open_response = session.response_seq if getattr(session, "response_open", False) else None
        for event in reversed(backlog):
            if event.get("type") != "audio":
                continue
            if event.get("response_id") != open_response or open_response is None:
                continue
            size = len(event.get("data") or "") * 3 // 4 + 12
            if size > audio_budget:
                break
            audio_budget -= size
            keep_audio.add(id(event))
        skipped_audio = 0
        for event in backlog:
            if event.get("type") == "audio" and id(event) not in keep_audio:
                skipped_audio += 1
                continue
            enqueue(event)
        await send(json.dumps({"type": "hello_ack", "protocol": negotiated_protocol,
                               "connection_generation": generation, "event_gap": event_gap, **v2_metadata}))
        if event_gap:
            binding.host.diagnostic(binding.session, "socket_event_gap",
                                    connection_generation=binding.state.generation)
            await send(json.dumps({"type": "transport_notice", "code": "LIVE_SOCKET_EVENT_GAP"}))
        if skipped_audio:
            binding.host.diagnostic(binding.session, "socket_stale_output_skipped",
                                    connection_generation=binding.state.generation, skipped_audio_events=skipped_audio)
            await send(json.dumps({"type": "transport_notice", "code": "LIVE_SOCKET_STALE_OUTPUT_SKIPPED",
                                   "skipped_audio_events": skipped_audio}))
        tasks = [asyncio.create_task(read_loop()), asyncio.create_task(write_loop())]
        if audio_v2 is not None:
            tasks.extend(audio_v2.start())
        done, _ = await asyncio.wait([*tasks, failed], return_when=asyncio.FIRST_COMPLETED)
        if failed in done:
            raise failed.result()
        for task in done:
            task.result()
    except asyncio.CancelledError:
        raise
    except Exception as exc:
        code = exc.code if isinstance(exc, LiveError) else "LIVE_INPUT_FAILURE"
        transient = isinstance(exc, LiveError) and code in TRANSIENT_SOCKET_CODES
        fatal = not transient
        close_code = 1002 if isinstance(exc, LiveError) and not transient else 1011
        close_reason = code
        binding.host.diagnostic(binding.session, "socket_detached" if transient else "socket_failed",
                                code=close_reason, connection_generation=binding.state.generation,
                                queue_bytes=queued_bytes)
    finally:
        unsubscribe()
        for task in tasks:
            task.cancel()
        for task in urgent_tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        if urgent_tasks:
            await asyncio.gather(*urgent_tasks, return_exceptions=True)
        if audio_v2 is not None:
            await audio_v2.close()
        if fatal:
            await binding.stop()
        await binding.close()
        try:
            await asyncio.wait_for(close(close_code, close_reason), 1.0)
        except Exception:
            pass

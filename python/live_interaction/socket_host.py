"""WSS binding for the existing Python Live host; no second provider engine.

HTTP adapters authenticate bootstrap/renewal. A socket is authorized by a short
one-use ticket, bound to the already authenticated actor and resource. Domain
adapters, provider transport and resource accounting remain unchanged.
"""
from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import logging
import re
import secrets
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Callable

from .session_host import LiveError, LiveSessionHost, LiveStopReason, _maybe_await

SOCKET_PROTOCOL = "wl-live-v1"
TICKET_TTL_SECONDS = 15
MAX_CAPTURE_AGE_MS = 2500
MAX_STARTUP_CATCHUP_AGE_MS = 20_000
MAX_STARTUP_CATCHUP_PCM_BYTES = 720_000
LOG = logging.getLogger("live_interaction.socket")


@dataclass
class _SocketState:
    attempt_id: str
    ticket_digest: bytes | None = None
    ticket_expires: float = 0
    claim: str | None = None
    generation: int = 0
    used_wss: bool = False
    damaged: bool = False
    damaged_through_turn: int = 0
    turn_serial: int = 0
    turn_clean: bool = True
    turn_has_audio: bool = False
    boundaries: deque = field(default_factory=deque)
    subscribers: set = field(default_factory=set)
    output_archive: Any = None


class _GuardedAdapter:
    def __init__(self, host, adapter):
        self.host, self.adapter = host, adapter

    def __getattr__(self, name):
        return getattr(self.adapter, name)

    def _guard(self, session):
        state = self.host._socket_states.get(session.id)
        if state is not None and state.damaged:
            raise LiveError("LIVE_INPUT_DAMAGED", "Speech was interrupted; repeat the request as a clean turn")

    async def execute_tool(self, session, call):
        # Runs after the base host's serialization/deduplication, immediately
        # before a NEW domain operation. Accepted operations are not replayed.
        self._guard(session)
        return await _maybe_await(self.adapter.execute_tool(session, call))

    async def resolve_capability(self, session, call):
        state = self.host._socket_states.get(session.id)
        if state is not None and state.damaged:
            # Ordinary guarded execution returns a structured tool error.
            return None
        resolver = getattr(self.adapter, "resolve_capability", None)
        return await _maybe_await(resolver(session, call)) if callable(resolver) else None


class LiveSocketSessionHost(LiveSessionHost):
    """Native WSS extension of LiveSessionHost, also usable by ASGI/aiohttp.

    All provider input still passes through LiveSessionHost and its managed
    resource runner. The extension owns only tickets, push subscriptions and
    transport-loss admission. Legacy HTTP sessions remain explicitly compatible.
    """

    def __init__(self, *, adapter_factory, diagnostic: Callable | None = None, audio_v2=None, **kwargs):
        self._socket_states: dict[str, _SocketState] = {}
        self._diagnostic_callback = diagnostic
        self.audio_v2 = audio_v2
        self._host_close_task = None

        def guarded_factory(**hooks):
            return _GuardedAdapter(self, adapter_factory(**hooks))

        super().__init__(adapter_factory=guarded_factory, **kwargs)

    def diagnostic(self, session, event: str, **fields):
        allowed = {"code", "connection_generation", "frame_seq", "pcm_bytes", "capture_age_ms", "audio_turn_open", "queue_bytes", "duration_ms", "skipped_audio_events"}
        record = {"event": event, "transport": "wss", "session_id": session.id,
                  "resource_id": session.resource_id, "model": session.model}
        state = self._socket_states.get(session.id)
        if state:
            record["attempt_id"] = state.attempt_id
        record.update({k: v for k, v in fields.items() if k in allowed and isinstance(v, (str, int, float, bool))})
        if self._diagnostic_callback:
            self._diagnostic_callback(record)
        else:
            LOG.info("%s", json.dumps(record, ensure_ascii=False, separators=(",", ":")))

    def _emit(self, session, event):
        super()._emit(session, event)
        state = self._socket_states.get(session.id)
        if state is None:
            return
        if event.get("type") == "input_timing" and (event.get("activity_end_sent_at") or event.get("audio_stream_end_sent_at")):
            generation = event.get("audio_turn_generation")
            boundary = None
            if isinstance(generation, int) and not isinstance(generation, bool):
                while state.boundaries and state.boundaries[0][0] <= generation:
                    candidate = state.boundaries.popleft()
                    if candidate[0] == generation:
                        boundary = candidate
            elif state.boundaries:
                boundary = state.boundaries.popleft()
            if boundary:
                serial, clean = boundary
                if clean and state.damaged and serial > state.damaged_through_turn:
                    state.damaged = False
                    self.diagnostic(session, "input_damage_recovered", connection_generation=state.generation)
        if (event.get("type") == "input_dropped"
                and event.get("input_type") in ("audio", "activity_start", "activity_end", "audio_stream_end")):
            # Input admission and provider delivery are asynchronous: the host
            # may already have enqueued the end (or the next turn) when loss is
            # reported. Fence the affected turn and its pending boundary receipt.
            generation = event.get("audio_turn_generation")
            if not isinstance(generation, int) or isinstance(generation, bool) or generation < 1:
                generation = state.turn_serial
            state.damaged = True
            state.damaged_through_turn = max(state.damaged_through_turn, generation)
            if generation == state.turn_serial:
                state.turn_clean = False
            state.boundaries = deque((serial, clean and serial != generation) for serial, clean in state.boundaries)
            self.diagnostic(session, "provider_input_dropped", connection_generation=state.generation,
                            audio_turn_open=True)
        item = session.events[-1]
        if state.output_archive is not None:
            state.output_archive.enqueue(item)
        for listener in tuple(state.subscribers):
            try:
                listener(item)
            except Exception:
                self.diagnostic(session, "subscriber_failed", code="LIVE_SOCKET_SUBSCRIBER")

    async def start(self, *, resource_id, actor, attempt_id=None, **kwargs):
        if self._host_close_task is not None:
            raise LiveError("LIVE_HOST_CLOSED", "The Live host has been closed")
        if attempt_id is None:
            attempt_id = "attempt_" + secrets.token_hex(12)
        if not isinstance(attempt_id, str) or not re.fullmatch(r"[A-Za-z0-9._:-]{1,96}", attempt_id):
            raise LiveError("INVALID_ARGUMENT", "Invalid Live attempt identifier")
        result = await super().start(resource_id=resource_id, actor=actor, **kwargs)
        if self._host_close_task is not None:
            session = self.sessions.get(result["session_id"])
            if session is not None:
                await self._discard(session, graceful=True)
            raise LiveError("LIVE_HOST_CLOSED", "The Live host closed during session setup")
        session = self._get(result["session_id"], resource_id, actor)
        if session.closed:
            raise LiveError("LIVE_SESSION_CLOSED", "Live session closed during setup")
        self._socket_states[session.id] = _SocketState(attempt_id)
        if (self.audio_v2 is not None and self.audio_v2.output_store_factory is not None
                and self.audio_v2.codecs.profiles("downlink")):
            from .audio_socket import AudioWorkPool
            from .audio_output import AudioOutputArchive
            if not hasattr(self, "_audio_work_pool"):
                self._audio_work_pool = AudioWorkPool()
            archive = AudioOutputArchive(self, session, self.audio_v2, self._audio_work_pool)
            self._socket_states[session.id].output_archive = archive
            for event in session.events:
                archive.enqueue(event)
        ticket = self.issue_socket_ticket(session_id=session.id, resource_id=resource_id, actor=actor)
        return {**result, **ticket}

    def issue_socket_ticket(self, *, session_id, resource_id, actor):
        session = self._get(session_id, resource_id, actor)
        state = self._socket_states.get(session_id)
        if session.closed or state is None:
            raise LiveError("LIVE_SESSION_CLOSED", "Live session is closed")
        ticket = secrets.token_urlsafe(32)
        state.ticket_digest = hashlib.sha256(ticket.encode("ascii")).digest()
        state.ticket_expires = time.monotonic() + TICKET_TTL_SECONDS
        self._touch(session)
        return {"socket_ticket": ticket, "transport_protocol": SOCKET_PROTOCOL,
                "transport_protocols": [SOCKET_PROTOCOL, "wl-live-v2"] if state.output_archive is not None else [SOCKET_PROTOCOL],
                "expires_in_ms": TICKET_TTL_SECONDS * 1000, "attempt_id": state.attempt_id}

    def open_socket(self, *, session_id, resource_id, ticket):
        session = self._get(session_id, resource_id, None)
        state = self._socket_states.get(session_id)
        if session.closed or state is None:
            raise LiveError("LIVE_SESSION_CLOSED", "Live session is closed")
        if state.claim is not None:
            raise LiveError("LIVE_SOCKET_BUSY", "A Live socket is already attached")
        if (not isinstance(ticket, str) or not re.fullmatch(r"[A-Za-z0-9_-]{20,512}", ticket)
                or state.ticket_digest is None or time.monotonic() > state.ticket_expires
                or not hmac.compare_digest(hashlib.sha256(ticket.encode("ascii")).digest(), state.ticket_digest)):
            raise LiveError("LIVE_SOCKET_TICKET", "Live socket ticket is invalid, expired or already used")
        state.ticket_digest = None
        state.claim = secrets.token_hex(16)
        self._touch(session)
        return SocketBinding(self, session, state, state.claim)

    async def input(self, *, session_id, resource_id, message, actor=None, received_at=None, _socket_claim=None,
                    _audio_projection=None):
        session = self._get(session_id, resource_id, actor)
        state = self._socket_states.get(session_id)
        if not isinstance(message, dict):
            raise LiveError("INVALID_ARGUMENT", "Live input must be an object")
        if state and state.used_wss and (not _socket_claim or _socket_claim != state.claim):
            raise LiveError("LIVE_TRANSPORT_MISMATCH", "This Live session uses WSS; HTTP input is not a fallback")
        generation = state.generation if state else 0
        def delivery_guard():
            if (session.closed or session.stop_reason is not None or
                    (state and _socket_claim and (state.claim != _socket_claim or state.generation != generation))):
                raise LiveError("LIVE_SOCKET_CLOSED", "Live input admission changed while waiting")
        delivery_guard()
        begin = bool(message.get("activity_start")) or (
            "audio_base64" in message and not session.manual_activity_detection and not session.audio_turn_open)
        end = bool(message.get("activity_end") or message.get("audio_stream_end"))
        # Append boundary bookkeeping before provider scheduling can observe it.
        previous_serial = state.turn_serial if state else 0
        previous_clean = state.turn_clean if state else True
        previous_audio = state.turn_has_audio if state else False
        if state and begin:
            state.turn_serial += 1
            state.turn_clean = True
            state.turn_has_audio = False
        if state and message.get("audio_base64"):
            state.turn_has_audio = True
        marker = (state.turn_serial, state.turn_clean and state.turn_has_audio) if state and end else None
        if marker:
            state.boundaries.append(marker)
        try:
            return await super().input(session_id=session_id, resource_id=resource_id, message=message,
                                       actor=actor, received_at=received_at, _audio_projection=_audio_projection,
                                       _delivery_guard=delivery_guard)
        except Exception:
            if state and begin:
                state.turn_serial, state.turn_clean = previous_serial, previous_clean
            if state:
                state.turn_has_audio = previous_audio
            if marker and state.boundaries and state.boundaries[-1] == marker:
                state.boundaries.pop()
            raise

    async def stop(self, *, session_id, resource_id, actor=None,
                   reason: LiveStopReason = "explicit_stop"):
        if session_id not in self.sessions:
            return {"ok": True, "session_id": session_id, "already_closed": True}
        return await super().stop(session_id=session_id, resource_id=resource_id, actor=actor, reason=reason)

    async def _discard(self, session, graceful=False):
        try:
            await super()._discard(session, graceful=graceful)
        finally:
            state = self._socket_states.pop(session.id, None)
            if state:
                state.ticket_digest = None
                state.subscribers.clear()
                if state.output_archive is not None:
                    await state.output_archive.close()

    async def _close_runtime(self):
        deadline = asyncio.get_running_loop().time() + 15.0
        stopping = asyncio.create_task(self.stop_all())
        try:
            done, _ = await asyncio.wait({stopping}, timeout=15.0)
            if not done:
                # Native calls keep their context until they return; cancelling
                # an asyncio task cannot safely kill a running native thread.
                stopping.cancel()
                stopping.add_done_callback(lambda task: None if task.cancelled() else task.exception())
                raise LiveError("LIVE_HOST_SHUTDOWN_TIMEOUT", "Live shutdown exceeded its flush bound")
            stopping.result()
        finally:
            pool = getattr(self, "_audio_work_pool", None)
            if pool is not None:
                await pool.close(timeout=max(0.0, min(5.0, deadline - asyncio.get_running_loop().time())))

    async def close(self):
        """Terminal application shutdown; stop/stop_all keep this host reusable."""
        if self._host_close_task is None:
            self._host_close_task = asyncio.create_task(self._close_runtime(), name="live-host-shutdown")
        await asyncio.shield(self._host_close_task)


class SocketBinding:
    def __init__(self, host, session, state, claim):
        self.host, self.session, self.state, self.claim = host, session, state, claim
        self.last_audio_seq: int | None = None
        self.connected = False
        self.stopped = False
        self.startup_catchup_open = True
        self.startup_catchup_pcm_bytes = 0

    @property
    def attempt_id(self):
        return self.state.attempt_id

    def connect(self, generation):
        if isinstance(generation, bool) or not isinstance(generation, int) or generation <= self.state.generation:
            raise LiveError("LIVE_SOCKET_GENERATION", "Live connection generation is stale")
        if self.state.claim != self.claim or self.session.closed:
            raise LiveError("LIVE_SOCKET_CLOSED", "Live socket is closed")
        self.connected = True
        self.startup_catchup_open = generation == 1
        self.startup_catchup_pcm_bytes = 0
        self.state.generation = generation
        self.state.used_wss = True
        self.host._touch(self.session)
        self.host._emit(self.session, {"type": "transport_connected", "transport": "wss", "connection_generation": generation})
        self.host.diagnostic(self.session, "socket_connected", connection_generation=generation)

    def subscribe(self, listener):
        self.state.subscribers.add(listener)
        return lambda: self.state.subscribers.discard(listener)

    def events(self, after=0):
        return self.host.events(session_id=self.session.id, resource_id=self.session.resource_id,
                                actor=self.session.actor, after=after)

    def heartbeat(self):
        if self.state.claim != self.claim or self.session.closed:
            raise LiveError("LIVE_SOCKET_CLOSED", "Live socket is closed")
        self.host._touch(self.session)

    async def input(self, message, *, frame_seq=None, capture_age_ms=None, pcm_bytes=None,
                    startup_catchup=False):
        self.heartbeat()
        if frame_seq is not None:
            if frame_seq <= 0 or (self.last_audio_seq is not None and frame_seq != self.last_audio_seq + 1):
                raise LiveError("LIVE_SOCKET_SEQUENCE", "Audio frame sequence is not contiguous")
            if startup_catchup:
                if not self.startup_catchup_open or self.state.generation != 1:
                    raise LiveError("LIVE_SOCKET_STALE_AUDIO", "Startup catchup is not allowed")
                if capture_age_ms is None or not 0 <= capture_age_ms <= MAX_STARTUP_CATCHUP_AGE_MS:
                    raise LiveError("LIVE_SOCKET_STALE_AUDIO", "Startup catchup audio is stale")
                size = int(pcm_bytes or 0)
                if size <= 0 or self.startup_catchup_pcm_bytes + size > MAX_STARTUP_CATCHUP_PCM_BYTES:
                    raise LiveError("LIVE_SOCKET_BACKPRESSURE", "Startup catchup audio exceeds its bound")
                self.startup_catchup_pcm_bytes += size
            else:
                self.startup_catchup_open = False
                if capture_age_ms is None or not 0 <= capture_age_ms <= MAX_CAPTURE_AGE_MS:
                    raise LiveError("LIVE_SOCKET_STALE_AUDIO", "Audio frame is stale")
        result = await self.host.input(session_id=self.session.id, resource_id=self.session.resource_id,
                                      actor=self.session.actor, message=message, _socket_claim=self.claim)
        if frame_seq is not None:
            self.last_audio_seq = frame_seq
            if frame_seq == 1 or frame_seq % 16 == 0:
                self.host.diagnostic(self.session, "socket_audio_accepted", frame_seq=frame_seq,
                                     pcm_bytes=pcm_bytes, capture_age_ms=capture_age_ms,
                                     startup_catchup=bool(startup_catchup),
                                     connection_generation=self.state.generation)
        return result

    async def stop(self):
        self.stopped = True
        return await self.host.stop(session_id=self.session.id, resource_id=self.session.resource_id, actor=self.session.actor)

    async def input_pcm(self, projection):
        self.heartbeat()
        if self.stopped:
            raise LiveError("LIVE_SOCKET_CLOSED", "Live socket is stopped")
        return await self.host.input_pcm(session_id=self.session.id, resource_id=self.session.resource_id,
                                         actor=self.session.actor, projection=projection, _socket_claim=self.claim)

    async def activate_audio_source(self, source):
        self.heartbeat()
        hook = getattr(self.host.adapter, "activate_audio_source", None)
        if hook:
            await _maybe_await(hook(self.session, source))

    async def close(self):
        if self.state.claim != self.claim:
            return
        if self.connected and not self.stopped and not self.session.closed:
            opened = self.session.activity_open or self.session.audio_turn_open
            self.host.diagnostic(self.session, "socket_gap", connection_generation=self.state.generation, audio_turn_open=opened)
            if opened:
                self.state.damaged = True
                self.state.damaged_through_turn = max(self.state.damaged_through_turn, self.state.turn_serial)
                self.state.turn_clean = False
                key = "activity_end" if self.session.manual_activity_detection else "audio_stream_end"
                try:
                    await self.input({key: True})
                except LiveError:
                    # If the provider cannot accept the boundary, don't leave
                    # an incomplete command live. Closing is safer than replay.
                    await self.stop()
            self.host._emit(self.session, {"type": "transport_gap", "transport": "wss",
                                          "connection_generation": self.state.generation, "audio_turn_open": opened})
        self.state.claim = None

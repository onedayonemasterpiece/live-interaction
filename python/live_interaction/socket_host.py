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

from .session_host import LiveError, LiveSessionHost, _maybe_await

SOCKET_PROTOCOL = "wl-live-v1"
TICKET_TTL_SECONDS = 15
MAX_CAPTURE_AGE_MS = 2500
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
    turn_serial: int = 0
    turn_clean: bool = True
    turn_has_audio: bool = False
    boundaries: deque = field(default_factory=deque)
    subscribers: set = field(default_factory=set)


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

    def __init__(self, *, adapter_factory, diagnostic: Callable | None = None, **kwargs):
        self._socket_states: dict[str, _SocketState] = {}
        self._diagnostic_callback = diagnostic

        def guarded_factory(**hooks):
            return _GuardedAdapter(self, adapter_factory(**hooks))

        super().__init__(adapter_factory=guarded_factory, **kwargs)

    def diagnostic(self, session, event: str, **fields):
        allowed = {"code", "connection_generation", "frame_seq", "pcm_bytes", "capture_age_ms", "audio_turn_open", "queue_bytes", "duration_ms",
                   "output_event_type", "output_seq", "output_payload_bytes", "queue_count_at_reject",
                   "queue_bytes_at_reject", "egress_limit_bytes", "egress_limit_events", "egress_reject_reason"}
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
            if state.boundaries:
                _serial, clean = state.boundaries.popleft()
                if clean and state.damaged:
                    state.damaged = False
                    self.diagnostic(session, "input_damage_recovered", connection_generation=state.generation)
        item = session.events[-1]
        for listener in tuple(state.subscribers):
            try:
                listener(item)
            except Exception:
                self.diagnostic(session, "subscriber_failed", code="LIVE_SOCKET_SUBSCRIBER")

    async def start(self, *, resource_id, actor, attempt_id=None, **kwargs):
        if attempt_id is None:
            attempt_id = "attempt_" + secrets.token_hex(12)
        if not isinstance(attempt_id, str) or not re.fullmatch(r"[A-Za-z0-9._:-]{1,96}", attempt_id):
            raise LiveError("INVALID_ARGUMENT", "Invalid Live attempt identifier")
        result = await super().start(resource_id=resource_id, actor=actor, **kwargs)
        session = self._get(result["session_id"], resource_id, actor)
        if session.closed:
            raise LiveError("LIVE_SESSION_CLOSED", "Live session closed during setup")
        self._socket_states[session.id] = _SocketState(attempt_id)
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

    async def input(self, *, session_id, resource_id, message, actor=None, received_at=None, _socket_claim=None):
        session = self._get(session_id, resource_id, actor)
        state = self._socket_states.get(session_id)
        if not isinstance(message, dict):
            raise LiveError("INVALID_ARGUMENT", "Live input must be an object")
        if state and state.used_wss and (not _socket_claim or _socket_claim != state.claim):
            raise LiveError("LIVE_TRANSPORT_MISMATCH", "This Live session uses WSS; HTTP input is not a fallback")
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
                                       actor=actor, received_at=received_at)
        except Exception:
            if state and begin:
                state.turn_serial, state.turn_clean = previous_serial, previous_clean
            if state:
                state.turn_has_audio = previous_audio
            if marker and state.boundaries and state.boundaries[-1] == marker:
                state.boundaries.pop()
            raise

    async def stop(self, *, session_id, resource_id, actor=None):
        if session_id not in self.sessions:
            return {"ok": True, "session_id": session_id, "already_closed": True}
        return await super().stop(session_id=session_id, resource_id=resource_id, actor=actor)

    async def _discard(self, session, graceful=False):
        try:
            await super()._discard(session, graceful=graceful)
        finally:
            state = self._socket_states.pop(session.id, None)
            if state:
                state.ticket_digest = None
                state.subscribers.clear()


class SocketBinding:
    def __init__(self, host, session, state, claim):
        self.host, self.session, self.state, self.claim = host, session, state, claim
        self.last_audio_seq: int | None = None
        self.connected = False
        self.stopped = False

    @property
    def attempt_id(self):
        return self.state.attempt_id

    def connect(self, generation):
        if isinstance(generation, bool) or not isinstance(generation, int) or generation <= self.state.generation:
            raise LiveError("LIVE_SOCKET_GENERATION", "Live connection generation is stale")
        if self.state.claim != self.claim or self.session.closed:
            raise LiveError("LIVE_SOCKET_CLOSED", "Live socket is closed")
        self.connected = True
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

    async def input(self, message, *, frame_seq=None, capture_age_ms=None, pcm_bytes=None):
        self.heartbeat()
        if frame_seq is not None:
            if frame_seq <= 0 or (self.last_audio_seq is not None and frame_seq != self.last_audio_seq + 1):
                raise LiveError("LIVE_SOCKET_SEQUENCE", "Audio frame sequence is not contiguous")
            if capture_age_ms is None or not 0 <= capture_age_ms <= MAX_CAPTURE_AGE_MS:
                raise LiveError("LIVE_SOCKET_STALE_AUDIO", "Audio frame is stale")
        result = await self.host.input(session_id=self.session.id, resource_id=self.session.resource_id,
                                      actor=self.session.actor, message=message, _socket_claim=self.claim)
        if frame_seq is not None:
            self.last_audio_seq = frame_seq
            if frame_seq == 1 or frame_seq % 16 == 0:
                self.host.diagnostic(self.session, "socket_audio_accepted", frame_seq=frame_seq,
                                     pcm_bytes=pcm_bytes, capture_age_ms=capture_age_ms,
                                     connection_generation=self.state.generation)
        return result

    async def stop(self):
        self.stopped = True
        return await self.host.stop(session_id=self.session.id, resource_id=self.session.resource_id, actor=self.session.actor)

    async def close(self):
        if self.state.claim != self.claim:
            return
        if self.connected and not self.stopped and not self.session.closed:
            opened = self.session.activity_open or self.session.audio_turn_open
            self.host.diagnostic(self.session, "socket_gap", connection_generation=self.state.generation, audio_turn_open=opened)
            if opened:
                self.state.damaged = True
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

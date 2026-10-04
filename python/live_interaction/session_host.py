"""Generic in-process Live session host for Python product adapters.

The host owns provider lifecycle, resource/actor binding, bounded input, event
pagination and ordered/deduplicated tool execution. Product adapters own domain
authorization and mutations.
"""
from __future__ import annotations

from .tool_parts import function_response

import asyncio
import hashlib
import inspect
import json
import re
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable

from . import provider

LIVE_SESSION_MODELS = ("gemini-3.8-live", "gemini-3.8-live-extended-thinking")


class LiveError(RuntimeError):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


class _QueueReader:
    """StreamReader-shaped bounded NDJSON queue used by provider.run()."""

    def __init__(self, max_bytes: int = 768 * 1024):
        self._queue: asyncio.Queue[bytes | None] = asyncio.Queue()
        self._max_bytes = max_bytes
        self._queued_bytes = 0
        self._closed = False

    def feed(self, payload: dict[str, Any]) -> int:
        if self._closed:
            raise LiveError("LIVE_SESSION_CLOSED", "Live-сессия закрыта")
        data = (json.dumps(payload, ensure_ascii=False, separators=(",", ":")) + "\n").encode("utf-8")
        if self._queued_bytes + len(data) > self._max_bytes:
            raise LiveError("LIVE_BACKPRESSURE", "Live provider input is not keeping up")
        self._queued_bytes += len(data)
        self._queue.put_nowait(data)
        return len(data)

    async def readline(self) -> bytes:
        data = await self._queue.get()
        if data is None:
            return b""
        self._queued_bytes = max(0, self._queued_bytes - len(data))
        return data

    def close(self) -> None:
        if not self._closed:
            self._closed = True
            self._queue.put_nowait(None)


@dataclass
class _Session:
    id: str
    resource_id: str
    actor: Any
    model: str
    reader: _QueueReader
    state: dict[str, Any]
    events: deque[dict[str, Any]] = field(default_factory=lambda: deque(maxlen=320))
    next_seq: int = 1
    closed: bool = False
    cancelled: set[str] = field(default_factory=set)
    tool_results: dict[str, Any] = field(default_factory=dict)
    tool_order: deque[str] = field(default_factory=deque)
    tool_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    task: asyncio.Task | None = None
    ready: asyncio.Future | None = None
    awaiting_audio: bool = False
    tool_response_at: int | None = None
    last_client_at_ms: int = 0
    watchdog_task: asyncio.Task | None = None
    capability: str = "core"
    configuration_digest: str = ""
    pending_transition_id: str | None = None
    pending_transition: asyncio.Future | None = None
    manual_activity_detection: bool = False
    activity_open: bool = False
    audio_turn_generation: int = 0
    audio_turn_open: bool = False
    audio_end_sent_generation: int = 0
    audio_end_awaiting_ack: deque[int] = field(default_factory=deque)
    audio_end_event: asyncio.Event = field(default_factory=asyncio.Event)


def _now_ms() -> int:
    return round(time.time() * 1000)


async def _maybe_await(value: Any) -> Any:
    return await value if inspect.isawaitable(value) else value


def _configuration_meta(configuration: dict[str, Any] | None) -> dict[str, Any]:
    configuration = configuration or {}
    functions = configuration.get("functions")
    if not isinstance(functions, list):
        functions = []
    encoded = json.dumps(
        configuration,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode("utf-8")
    return {
        "configuration_digest": hashlib.sha256(encoded).hexdigest(),
        "function_count": len(functions),
        "schema_bytes": len(json.dumps(functions, ensure_ascii=False, separators=(",", ":")).encode("utf-8")),
        "configuration_bytes": len(encoded),
    }


def _validate_capability_spec(spec: Any) -> dict[str, Any]:
    if not isinstance(spec, dict):
        raise LiveError("LIVE_CAPABILITY_INVALID", "Capability resolver returned invalid state")
    capability = str(spec.get("capability") or "")
    if not re.fullmatch(r"[a-z][a-z0-9_-]{0,63}", capability):
        raise LiveError("LIVE_CAPABILITY_INVALID", "Capability id is invalid")
    configuration = spec.get("configuration")
    if not isinstance(configuration, dict):
        raise LiveError("LIVE_CAPABILITY_INVALID", "Capability configuration is required")
    meta = _configuration_meta(configuration)
    if meta["function_count"] > 9 or meta["configuration_bytes"] > 262_144:
        raise LiveError("LIVE_CAPABILITY_LIMIT", "Capability bundle exceeds shared limits")
    context = spec.get("context")
    if context is not None and not isinstance(context, dict):
        raise LiveError("LIVE_CAPABILITY_INVALID", "Capability context is invalid")
    continuation = spec.get("continuation")
    if continuation is not None and (not isinstance(continuation, str) or len(continuation) > 1200):
        raise LiveError("LIVE_CAPABILITY_INVALID", "Capability continuation is invalid")
    return {
        **spec,
        "capability": capability,
        "continuation": continuation.strip() if isinstance(continuation, str) else None,
        **meta,
    }


class LiveSessionHost:
    def __init__(
        self,
        *,
        adapter_factory: Callable[..., Any],
        key_resolver: Callable[[str, Any], str | None] | None = None,
        provider_run: Callable[..., Awaitable[None]] = provider.run,
        managed_runner: Callable[..., Awaitable[None]] | None = None,
        models: tuple[str, ...] = LIVE_SESSION_MODELS,
        ready_timeout_ms: int = 30_000,
        reconfigure_timeout_ms: int = 30_000,
        max_sessions: int = 2,
        client_liveness_timeout_ms: int = 60_000,
    ):
        self.models = tuple(models)
        self.ready_timeout_ms = ready_timeout_ms
        self.reconfigure_timeout_ms = reconfigure_timeout_ms
        self.max_sessions = max_sessions
        self.provider_run = provider_run
        self.managed_runner = managed_runner
        self.key_resolver = key_resolver or (lambda _resource_id, _actor: provider.default_key())
        self.client_liveness_timeout_ms = max(1_000, int(client_liveness_timeout_ms))
        self.sessions: dict[str, _Session] = {}
        self._start_lock = asyncio.Lock()
        self._starting_sessions = 0
        self.adapter = adapter_factory(
            emit=self._emit,
            write=self._write,
            measure=self._measure,
            timing=self._timing,
        )

    def _emit(self, session: _Session, event: dict[str, Any]) -> None:
        item = {"seq": session.next_seq, "at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), **event}
        session.next_seq += 1
        session.events.append(item)

    def _timing(self, session: _Session, stage: str, started_ms: int) -> None:
        self._emit(session, {"type": "timing", "stage": stage, "duration_ms": max(0, _now_ms() - started_ms)})

    async def _measure(self, session: _Session, stage: str, fn: Callable[[], Any]) -> Any:
        started = _now_ms()
        try:
            return await _maybe_await(fn())
        finally:
            self._timing(session, stage, started)

    def _write(self, session: _Session, message: dict[str, Any]) -> int:
        if session.closed:
            raise LiveError("LIVE_SESSION_CLOSED", "Live-сессия закрыта")
        queued_at = _now_ms()
        session.reader.feed({**message, "queued_at": queued_at})
        return queued_at

    @staticmethod
    def _actor_matches(expected: Any, actual: Any) -> bool:
        if actual is None:
            return True
        if isinstance(expected, dict) and isinstance(actual, dict):
            return (
                expected.get("subject") == actual.get("subject")
                and expected.get("tenant_id") == actual.get("tenant_id")
            )
        return expected == actual

    def _get(self, session_id: str, resource_id: str, actor: Any) -> _Session:
        session = self.sessions.get(session_id)
        if not session or session.resource_id != resource_id:
            raise LiveError("LIVE_SESSION_NOT_FOUND", "Live-сессия не найдена")
        if not self._actor_matches(session.actor, actor):
            raise LiveError("FORBIDDEN", "Live-сессия принадлежит другому пользователю")
        return session

    @staticmethod
    def _touch(session: _Session) -> None:
        session.last_client_at_ms = _now_ms()

    async def _watch_client(self, session: _Session) -> None:
        interval = max(250, min(5_000, self.client_liveness_timeout_ms // 3))
        try:
            while not session.closed:
                await asyncio.sleep(interval / 1000)
                if session.closed:
                    return
                if _now_ms() - session.last_client_at_ms < self.client_liveness_timeout_ms:
                    continue
                self._emit(session, {"type": "client_liveness_timeout"})
                await self._discard(session, graceful=False)
                return
        except asyncio.CancelledError:
            raise

    async def start(
        self,
        *,
        resource_id: str,
        actor: Any,
        model: str | None = None,
        history: list[dict[str, Any]] | None = None,
        **args: Any,
    ) -> dict[str, Any]:
        model = model or self.models[0]
        if not isinstance(resource_id, str) or not resource_id or len(resource_id) > 240:
            raise LiveError("INVALID_ARGUMENT", "resource_id is required")
        if model not in self.models:
            raise LiveError("INVALID_INPUT", "Unknown Live model")
        async with self._start_lock:
            active = sum(not item.closed for item in self.sessions.values())
            if active + self._starting_sessions >= self.max_sessions:
                raise LiveError("LIVE_BUSY", "Live session limit reached")
            self._starting_sessions += 1
        try:
            initialized = await _maybe_await(
                self.adapter.initialize(resource_id=resource_id, actor=actor, model=model, **args)
            )
            if not isinstance(initialized, dict):
                raise LiveError("LIVE_ADAPTER_ERROR", "Live adapter initialize() returned invalid state")
        finally:
            async with self._start_lock:
                self._starting_sessions -= 1

        session_id = "live_" + uuid.uuid4().hex
        reader = _QueueReader()
        loop = asyncio.get_running_loop()
        configuration = initialized.get("configuration") or {}
        initial_meta = _configuration_meta(configuration)
        initial_state = dict(initialized.get("state") or {})
        session = _Session(
            id=session_id,
            resource_id=resource_id,
            actor=actor,
            model=model,
            reader=reader,
            state=initial_state,
            ready=loop.create_future(),
            last_client_at_ms=_now_ms(),
            capability=str(initialized.get("capability") or initial_state.get("capability") or "core"),
            configuration_digest=initial_meta["configuration_digest"],
            manual_activity_detection=bool(configuration.get("manual_activity_detection")),
        )
        self.sessions[session_id] = session
        key = None
        if self.managed_runner is None:
            key = self.key_resolver(resource_id, actor)
            if not key:
                self.sessions.pop(session_id, None)
                raise LiveError("LIVE_UNAVAILABLE", "application_google_binding_missing")

        def on_event(event: dict[str, Any]) -> None:
            kind = event.get("type")
            if hasattr(self.adapter, "on_event"):
                observed = self.adapter.on_event(session, event)
                if inspect.isawaitable(observed):
                    asyncio.create_task(observed)
            projected = event
            if kind in {"input_transcript", "output_transcript"} and isinstance(event.get("text"), str):
                text = event["text"]
                projected = {**event, "text": text[:2000]}
                if len(text) > 2000:
                    projected["truncated"] = True
            if kind == "capability_ready":
                self._emit(session, projected)
                if (
                    session.pending_transition
                    and not session.pending_transition.done()
                    and session.pending_transition_id == event.get("transition_id")
                ):
                    session.pending_transition.set_result(event)
                return
            if kind == "capability_transition_error":
                self._emit(session, projected)
                if (
                    session.pending_transition
                    and not session.pending_transition.done()
                    and session.pending_transition_id == event.get("transition_id")
                ):
                    session.pending_transition.set_exception(
                        LiveError(
                            str(event.get("code") or "LIVE_CAPABILITY_ERROR"),
                            "Capability transition failed",
                        )
                    )
                return
            if kind == "ready":
                self._emit(session, projected)
                if session.ready and not session.ready.done():
                    session.ready.set_result(event)
                return
            if kind == "error":
                self._emit(session, projected)
                if session.ready and not session.ready.done():
                    session.ready.set_exception(
                        LiveError("LIVE_PROVIDER_ERROR", str(event.get("message") or "Gemini Live error")[:500])
                    )
                return
            if kind == "tool_cancelled":
                session.cancelled.update(str(v) for v in (event.get("ids") or []))
            if kind == "input_timing" and (event.get("audio_stream_end_sent_at") or event.get("activity_end_sent_at")):
                if session.audio_end_awaiting_ack:
                    session.audio_end_sent_generation = max(
                        session.audio_end_sent_generation, session.audio_end_awaiting_ack.popleft()
                    )
                session.audio_end_event.set()
            if kind == "resumed" and hasattr(self.adapter, "on_resumed"):
                asyncio.create_task(_maybe_await(self.adapter.on_resumed(session)))
            if kind == "audio" and session.awaiting_audio and session.tool_response_at is not None:
                self._timing(session, "first_audio_after_tool_response", session.tool_response_at)
                session.awaiting_audio = False
            if kind == "tool_call":
                calls = event.get("calls") or []
                self._emit(
                    session,
                    {
                        "type": "tool_call",
                        "provider_at": event.get("provider_at"),
                        "calls": [{"name": c.get("name"), "id": c.get("id")} for c in calls],
                    },
                )
                asyncio.create_task(self._handle_tool_calls(session, calls))
                return
            self._emit(session, projected)

        async def runner() -> None:
            try:
                if self.managed_runner is not None:
                    await self.managed_runner(session=session, reader=reader, on_event=on_event)
                else:
                    await self.provider_run(load_key=lambda: key, reader=reader, on_event=on_event)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._emit(session, {"type": "error", "code": type(exc).__name__, "message": str(exc)[:500]})
                if session.ready and not session.ready.done():
                    session.ready.set_exception(LiveError("LIVE_PROVIDER_ERROR", str(exc)[:500]))
            finally:
                session.closed = True
                reader.close()
                if session.pending_transition and not session.pending_transition.done():
                    session.pending_transition.set_exception(
                        LiveError("LIVE_PROVIDER_CLOSED", "Live provider closed during capability transition")
                    )
                session.pending_transition = None
                session.pending_transition_id = None
                if session.ready and not session.ready.done():
                    session.ready.set_exception(
                        LiveError("LIVE_PROVIDER_CLOSED", "Live provider closed during setup")
                    )
                self._emit(session, {"type": "closed"})

        session.task = asyncio.create_task(runner(), name=f"live-session-{session_id}")
        filtered_history = [
            {"role": item.get("role"), "text": str(item.get("text") or "")[-700:]}
            for item in (history or [])[-8:]
            if item.get("role") in {"user", "model"} and isinstance(item.get("text"), str)
        ]
        self._write(
            session,
            {
                "type": "start",
                "model": model,
                "configuration": initialized.get("configuration") or {},
                "context": initialized.get("context") or {},
                "history": filtered_history,
            },
        )
        try:
            await asyncio.wait_for(asyncio.shield(session.ready), timeout=self.ready_timeout_ms / 1000)
        except Exception:
            await self._discard(session)
            raise

        if hasattr(self.adapter, "on_started"):
            await _maybe_await(self.adapter.on_started(session))
        session.watchdog_task = asyncio.create_task(
            self._watch_client(session), name=f"live-client-watchdog-{session_id}"
        )
        response = initialized.get("response") or {}
        self._emit(
            session,
            {
                "type": "configuration_ready",
                "capability": session.capability,
                "configuration_digest": session.configuration_digest,
                "function_count": initial_meta["function_count"],
                "schema_bytes": initial_meta["schema_bytes"],
            },
        )
        return {
            "session_id": session_id,
            "model": model,
            "capability": session.capability,
            "configuration_digest": session.configuration_digest,
            **response,
        }

    def _remember_tool_result(self, session: _Session, call_id: str, result: Any) -> None:
        if not call_id:
            return
        session.tool_results[call_id] = result
        session.tool_order.append(call_id)
        while len(session.tool_order) > 100:
            old = session.tool_order.popleft()
            session.tool_results.pop(old, None)

    def _send_tool_responses(self, session: _Session, responses: list[dict[str, Any]]) -> None:
        if responses and not session.closed:
            session.tool_response_at = self._write(
                session, {"type": "tool_response", "responses": responses}
            )
            session.awaiting_audio = True
            self._emit(session, {"type": "timing", "stage": "tool_response_written"})

    async def _transition_capability(
        self, session: _Session, call: dict[str, Any], spec: Any
    ) -> None:
        resolved = _validate_capability_spec(spec)
        generation = session.audio_turn_generation
        if generation > session.audio_end_sent_generation:
            started = _now_ms()
            self._emit(session, {"type": "capability_waiting_for_audio_end"})
            try:
                async with asyncio.timeout(30):
                    while not session.closed and session.audio_end_sent_generation < generation:
                        session.audio_end_event.clear()
                        await session.audio_end_event.wait()
            except TimeoutError as exc:
                raise LiveError("LIVE_AUDIO_TURN_PENDING", "Speech did not finish before capability transition") from exc
            if session.closed:
                raise LiveError("LIVE_SESSION_CLOSED", "Live-сессия закрыта")
            self._emit(session, {"type": "capability_audio_end_ready", "duration_ms": _now_ms() - started})
        transition_id = "cap_" + uuid.uuid4().hex
        loop = asyncio.get_running_loop()
        future = loop.create_future()
        session.pending_transition_id = transition_id
        session.pending_transition = future
        self._emit(
            session,
            {
                "type": "capability_transition_requested",
                "transition_id": transition_id,
                "from_capability": session.capability,
                "to_capability": resolved["capability"],
                "configuration_digest": resolved["configuration_digest"],
                "function_count": resolved["function_count"],
                "schema_bytes": resolved["schema_bytes"],
            },
        )
        call_id = str(call.get("id") or "")
        call_name = str(call.get("name") or "unknown")
        fallback_intent = str((call.get("args") or {}).get("intent") or "").strip()
        continuation = resolved.get("continuation") or fallback_intent
        if len(continuation) > 1200:
            raise LiveError("LIVE_CAPABILITY_INVALID", "Capability continuation is invalid")
        acknowledgement = {"capability": resolved["capability"], "accepted": True}
        try:
            self._write(
                session,
                {
                    "type": "reconfigure",
                    "transition_id": transition_id,
                    "capability": resolved["capability"],
                    "configuration": resolved["configuration"],
                    "context": resolved.get("context") or {},
                    "continuation": continuation,
                    "router_response": {
                        "name": call_name,
                        "id": call_id,
                        "response": {"result": acknowledgement},
                        "scheduling": "SILENT",
                        "willContinue": False,
                    },
                },
            )
            await asyncio.wait_for(asyncio.shield(future), timeout=self.reconfigure_timeout_ms / 1000)
            session.capability = resolved["capability"]
            session.configuration_digest = resolved["configuration_digest"]
            session.manual_activity_detection = bool(
                resolved["configuration"].get("manual_activity_detection")
            )
            session.activity_open = False
            result = {
                "capability": session.capability,
                "ready": True,
                **(resolved.get("response") if isinstance(resolved.get("response"), dict) else {}),
            }
            self._remember_tool_result(session, call_id, result)
            self._emit(session, {
                "type": "tool_result",
                "name": call_name,
                "id": call_id,
                "status": "ok",
                "capability": session.capability,
            })
        except Exception as exc:
            code = getattr(exc, "code", "LIVE_CAPABILITY_ERROR")
            self._emit(session, {
                "type": "capability_transition_failed",
                "transition_id": transition_id,
                "from_capability": session.capability,
                "to_capability": resolved["capability"],
                "code": code,
            })
            self._emit(session, {
                "type": "tool_result",
                "name": call_name,
                "id": call_id,
                "status": "error",
                "code": code,
            })
        finally:
            if session.pending_transition_id == transition_id:
                session.pending_transition_id = None
                session.pending_transition = None

    async def _handle_tool_calls(self, session: _Session, calls: list[dict[str, Any]]) -> None:
        async with session.tool_lock:
            calls = calls if isinstance(calls, list) else []
            if not calls or session.closed:
                return
            if len(calls) == 1:
                call_id = str(calls[0].get("id") or "")
                if call_id and call_id in session.tool_results:
                    self._send_tool_responses(session, [function_response(
                        str(calls[0].get("name") or "unknown"), call_id, session.tool_results[call_id]
                    )])
                    return
            resolver = getattr(self.adapter, "resolve_capability", None)
            if callable(resolver):
                resolved = [
                    await _maybe_await(resolver(session, call))
                    for call in calls
                ]
                transitions = [
                    (index, spec)
                    for index, spec in enumerate(resolved)
                    if spec is not None
                ]
                if transitions:
                    if len(calls) != 1 or len(transitions) != 1:
                        responses = []
                        for call in calls:
                            name = str(call.get("name") or "unknown")
                            call_id = str(call.get("id") or "")
                            self._emit(
                                session,
                                {
                                    "type": "tool_result",
                                    "name": name,
                                    "id": call_id,
                                    "status": "error",
                                    "code": "LIVE_CAPABILITY_CONFLICT",
                                },
                            )
                            responses.append(
                                {
                                    "name": name,
                                    "id": call_id,
                                    "response": {
                                        "error": {
                                            "code": "LIVE_CAPABILITY_CONFLICT",
                                            "message": "Capability transition must be the only tool call in its batch",
                                        }
                                    },
                                }
                            )
                        self._send_tool_responses(session, responses)
                        return
                    try:
                        await self._transition_capability(
                            session, calls[0], transitions[0][1]
                        )
                    except Exception as exc:
                        code = getattr(exc, "code", "LIVE_CAPABILITY_ERROR")
                        name = str(calls[0].get("name") or "unknown")
                        call_id = str(calls[0].get("id") or "")
                        self._emit(
                            session,
                            {
                                "type": "capability_transition_rejected",
                                "from_capability": session.capability,
                                "to_capability": str(
                                    (transitions[0][1] or {}).get("capability") or ""
                                )[:80],
                                "code": code,
                            },
                        )
                        self._emit(
                            session,
                            {
                                "type": "tool_result",
                                "name": name,
                                "id": call_id,
                                "status": "error",
                                "code": code,
                            },
                        )
                        self._send_tool_responses(
                            session,
                            [
                                {
                                    "name": name,
                                    "id": call_id,
                                    "response": {
                                        "error": {
                                            "code": code,
                                            "message": str(exc)[:500],
                                        }
                                    },
                                }
                            ],
                        )
                    return
            responses: list[dict[str, Any]] = []
            for call in calls:
                if session.closed:
                    return
                call_id = str(call.get("id") or "")
                name = str(call.get("name") or "unknown")
                if call_id in session.cancelled:
                    continue
                started = _now_ms()
                try:
                    if call_id and call_id in session.tool_results:
                        result = session.tool_results[call_id]
                    else:
                        result = await _maybe_await(self.adapter.execute_tool(session, call))
                        self._remember_tool_result(session, call_id, result)
                    self._emit(
                        session,
                        {
                            "type": "tool_result",
                            "name": name,
                            "id": call_id,
                            "status": "ok",
                            "duration_ms": _now_ms() - started,
                            "revision": (result or {}).get("revision")
                            if isinstance(result, dict)
                            else None,
                        },
                    )
                    responses.append(function_response(name, call_id, result))
                except Exception as exc:
                    code = getattr(exc, "code", "LIVE_TOOL_ERROR")
                    self._emit(
                        session,
                        {
                            "type": "tool_result",
                            "name": name,
                            "id": call_id,
                            "status": "error",
                            "code": code,
                            "message": str(exc)[:240],
                        },
                    )
                    responses.append(
                        {
                            "name": name,
                            "id": call_id,
                            "response": {"error": {"code": code, "message": str(exc)[:500]}},
                        }
                    )
            self._send_tool_responses(session, responses)

    async def input(
        self,
        *,
        session_id: str,
        resource_id: str,
        message: dict[str, Any],
        actor: Any = None,
        received_at: int | None = None,
    ) -> dict[str, Any]:
        session = self._get(session_id, resource_id, actor)
        self._touch(session)
        if hasattr(self.adapter, "input"):
            await _maybe_await(self.adapter.input(session, message))
        received_at = received_at or _now_ms()
        written_at = None

        if message.get("activity_start"):
            if not session.manual_activity_detection:
                raise LiveError("INVALID_ARGUMENT", "Manual activity is not enabled")
            if session.activity_open:
                raise LiveError("INVALID_ARGUMENT", "Activity is already open")
            written_at = self._write(session, {"type": "activity_start"})
            session.activity_open = True
            session.audio_turn_generation += 1
            session.audio_turn_open = True
        if "audio_base64" in message:
            audio = message.get("audio_base64")
            if not isinstance(audio, str) or len(audio) > 16_000:
                raise LiveError("INVALID_ARGUMENT", "Audio chunk is invalid")
            if session.manual_activity_detection and not session.activity_open:
                raise LiveError("INVALID_ARGUMENT", "activity_start is required before buffered audio")
            written_at = self._write(session, {"type": "audio", "data": audio})
            if not session.manual_activity_detection and not session.audio_turn_open:
                session.audio_turn_generation += 1
                session.audio_turn_open = True
        if message.get("audio_stream_end"):
            if session.manual_activity_detection:
                raise LiveError("INVALID_ARGUMENT", "Use activity_end when manual activity is enabled")
            self._write(session, {"type": "audio_stream_end"})
            if session.audio_turn_open:
                session.audio_end_awaiting_ack.append(session.audio_turn_generation)
                session.audio_turn_open = False
        if message.get("activity_end"):
            if not session.manual_activity_detection:
                raise LiveError("INVALID_ARGUMENT", "Manual activity is not enabled")
            if not session.activity_open:
                raise LiveError("INVALID_ARGUMENT", "No activity is open")
            self._write(session, {"type": "activity_end"})
            session.activity_open = False
            if session.audio_turn_open:
                session.audio_end_awaiting_ack.append(session.audio_turn_generation)
                session.audio_turn_open = False
        if "text" in message:
            text = message.get("text")
            if not isinstance(text, str) or not text.strip() or len(text) > 4_000:
                raise LiveError("INVALID_ARGUMENT", "Text turn is invalid")
            self._write(session, {"type": "text", "text": text.strip()})
        return {
            "ok": True,
            "session_id": session.id,
            "timing": {"received_at": received_at, "provider_input_at": written_at, "handled_at": _now_ms()},
        }

    def events(
        self,
        *,
        session_id: str,
        resource_id: str,
        after: int = 0,
        actor: Any = None,
    ) -> dict[str, Any]:
        session = self._get(session_id, resource_id, actor)
        self._touch(session)
        all_events = list(session.events)
        items = [event for event in all_events if int(event["seq"]) > after][:64]
        cursor = int(items[-1]["seq"]) if items else after
        has_more = any(int(event["seq"]) > cursor for event in all_events)
        gap = bool(all_events and after < int(all_events[0]["seq"]) - 1)
        return {
            "session_id": session.id,
            "events": items,
            "cursor": cursor,
            "has_more": has_more,
            "gap": gap,
            "closed": session.closed and not has_more,
        }

    async def stop(self, *, session_id: str, resource_id: str, actor: Any = None) -> dict[str, Any]:
        session = self._get(session_id, resource_id, actor)
        if not session.closed:
            try:
                self._write(session, {"type": "stop"})
            except LiveError:
                pass
        if hasattr(self.adapter, "on_stopped"):            await _maybe_await(self.adapter.on_stopped(session))
        await self._discard(session, graceful=True)
        return {"ok": True, "session_id": session.id}

    async def _discard(self, session: _Session, graceful: bool = False) -> None:
        session.closed = True
        session.audio_end_event.set()
        if session.pending_transition and not session.pending_transition.done():
            session.pending_transition.set_exception(
                LiveError("LIVE_SESSION_CLOSED", "Live session stopped during capability transition")
            )
        session.pending_transition = None
        session.pending_transition_id = None
        current = asyncio.current_task()
        if session.watchdog_task and session.watchdog_task is not current and not session.watchdog_task.done():
            session.watchdog_task.cancel()
            await asyncio.gather(session.watchdog_task, return_exceptions=True)
        if session.task and not session.task.done():
            if graceful:
                try:
                    await asyncio.wait_for(asyncio.shield(session.task), timeout=1.2)
                except (asyncio.TimeoutError, asyncio.CancelledError):
                    pass
            if not session.task.done():
                session.task.cancel()
                await asyncio.gather(session.task, return_exceptions=True)
        session.reader.close()
        self.sessions.pop(session.id, None)

    async def stop_all(self) -> None:
        for session in list(self.sessions.values()):
            await self._discard(session, graceful=True)

    def size(self) -> int:
        return len(self.sessions)

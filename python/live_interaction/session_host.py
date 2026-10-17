"""Generic in-process Live session host for Python product adapters.

The host owns provider lifecycle, resource/actor binding, bounded input, event
pagination and ordered/deduplicated tool execution. Product adapters own domain
authorization and mutations.
"""
from __future__ import annotations

import asyncio
import base64
import hashlib
import inspect
import json
import math
import re
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Awaitable, Callable, Literal

from . import provider

LIVE_SESSION_MODELS = ("gemini-3.8-live", "gemini-3.8-live-extended-thinking")
LiveStopReason = Literal["explicit_stop", "detached_reclaim"]


class LiveError(RuntimeError):
    def __init__(self, code: str, message: str, *, retry_after_ms: Any = None):
        super().__init__(message)
        self.code = code
        if (isinstance(retry_after_ms, (int, float)) and not isinstance(retry_after_ms, bool)
                and math.isfinite(retry_after_ms) and retry_after_ms >= 0):
            self.retry_after_ms = min(86_400_000, math.ceil(retry_after_ms))


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
    stop_reason: LiveStopReason | None = None
    cancelled: set[str] = field(default_factory=set)
    tool_results: dict[str, Any] = field(default_factory=dict)
    tool_order: deque[str] = field(default_factory=deque)
    tool_lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    non_blocking_tools: frozenset[str] = field(default_factory=frozenset)
    tool_feedback_generation: int = 0
    tool_progress_task: asyncio.Task | None = None
    tool_progress_call_id: str | None = None
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
    # Output audio belongs to one model response. A reattach replays audio only
    # of the still-open response; finished or interrupted answers never replay.
    response_seq: int = 0
    response_open: bool = False
    response_audio_us: int = 0


def _now_ms() -> int:
    return round(time.time() * 1000)


async def _maybe_await(value: Any) -> Any:
    return await value if inspect.isawaitable(value) else value


def _non_blocking_tools(configuration: dict[str, Any]) -> frozenset[str]:
    functions = configuration.get("functions")
    return frozenset(
        item["name"] for item in (functions if isinstance(functions, list) else [])
        if isinstance(item, dict) and isinstance(item.get("name"), str)
        and item.get("behavior") == "NON_BLOCKING"
    )


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
        tool_progress_delay_ms: int = 2_500,
    ):
        self.models = tuple(models)
        self.ready_timeout_ms = ready_timeout_ms
        self.reconfigure_timeout_ms = reconfigure_timeout_ms
        self.max_sessions = max_sessions
        self.provider_run = provider_run
        self.managed_runner = managed_runner
        self.key_resolver = key_resolver or (lambda _resource_id, _actor: provider.default_key())
        self.client_liveness_timeout_ms = max(1_000, int(client_liveness_timeout_ms))
        self.tool_progress_delay_ms = max(1, min(60_000, int(tool_progress_delay_ms)))
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
        kind = event.get("type")
        if kind == "audio":
            if not session.response_open:
                session.response_seq += 1
                session.response_open = True
                session.response_audio_us = 0
            event = {**event, "response_id": session.response_seq, "audio_start_us": session.response_audio_us}
            rate_match = re.search(r"rate=(\d+)", str(event.get("mime_type") or ""))
            rate = int(rate_match[1]) if rate_match else 24000
            encoded = event.get("data")
            if isinstance(encoded, str) and 8000 <= rate <= 96000:
                size = len(encoded.rstrip("=")) * 3 // 4
                session.response_audio_us += round((size // 2) * 1_000_000 / rate)
        elif kind in ("turn_complete", "interrupted"):
            session.response_open = False
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
            if getattr(self, "_host_close_task", None) is not None:
                raise LiveError("LIVE_HOST_CLOSED", "The Live host closed during session initialization")
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
            non_blocking_tools=_non_blocking_tools(configuration),
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
            # Transcript text is user-visible conversational source, not a
            # diagnostic preview. Keep it lossless end-to-end.
            projected = event
            if kind == "resumption_checkpoint":
                # Provider checkpoint handles are host secrets: never pushed to clients.
                handle = event.get("handle")
                saver = getattr(self.adapter, "on_checkpoint", None)
                if isinstance(handle, str) and callable(saver):
                    try:
                        saved = saver(session, handle)
                        if inspect.isawaitable(saved):
                            asyncio.create_task(saved)
                    except Exception:
                        self._emit(session, {"type": "checkpoint_persist_failed"})
                return
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
                        LiveError(
                            str(event.get("code") or "LIVE_PROVIDER_ERROR")[:80],
                            str(event.get("message") or "Gemini Live error")[:500],
                            retry_after_ms=event.get("retry_after_ms"),
                        )
                    )
                return
            if kind == "tool_cancelled":
                session.cancelled.update(str(v) for v in (event.get("ids") or []))
                if session.tool_progress_call_id in session.cancelled:
                    self._cancel_tool_progress(session)
            if kind == "input_timing" and (event.get("audio_stream_end_sent_at") or event.get("activity_end_sent_at")):
                generation = event.get("audio_turn_generation")
                if isinstance(generation, int) and not isinstance(generation, bool):
                    # A boundary dropped on an old connection has no ACK. Match
                    # the provider receipt to its turn rather than shifting FIFO.
                    if generation in session.audio_end_awaiting_ack:
                        while session.audio_end_awaiting_ack and session.audio_end_awaiting_ack[0] <= generation:
                            session.audio_end_awaiting_ack.popleft()
                        session.audio_end_sent_generation = max(session.audio_end_sent_generation, generation)
                elif session.audio_end_awaiting_ack:
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
                asyncio.create_task(self._handle_tool_calls(
                    session, calls, feedback_generation=session.tool_feedback_generation,
                ))
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
                failure = LiveError(
                    str(getattr(exc, "code", None) or "LIVE_PROVIDER_ERROR")[:80],
                    str(exc)[:500], retry_after_ms=getattr(exc, "retry_after_ms", None),
                )
                event = {"type": "error", "code": failure.code, "message": str(failure)}
                if hasattr(failure, "retry_after_ms"):
                    event["retry_after_ms"] = failure.retry_after_ms
                self._emit(session, event)
                if session.ready and not session.ready.done():
                    session.ready.set_exception(failure)
            finally:
                session.closed = True
                self._cancel_tool_progress(session)
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
                **({"resume_handle": initialized["resume_handle"]}
                   if isinstance(initialized.get("resume_handle"), str) else {}),
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

    def _send_tool_responses(self, session: _Session, responses: list[dict[str, Any]]) -> bool:
        if responses and not session.closed and session.stop_reason is None:
            written_at = self._write(
                session, {"type": "tool_response", "responses": responses}
            )
            if any(item.get("scheduling") != "SILENT" for item in responses):
                session.tool_response_at = written_at
                session.awaiting_audio = True
            self._emit(session, {"type": "timing", "stage": "tool_response_written"})
            return True
        return False

    def _cancel_tool_progress(self, session: _Session) -> None:
        task = session.tool_progress_task
        if task and not task.done():
            task.cancel()

    def _advance_tool_feedback(self, session: _Session) -> None:
        # Text and speech invalidate voice feedback independently of the audio
        # transport generation used by damaged-turn and provider-end receipts.
        session.tool_feedback_generation += 1
        self._cancel_tool_progress(session)

    async def _tool_progress_after_delay(
        self, session: _Session, *, name: str, call_id: str,
        feedback_generation: int, started: float,
    ) -> None:
        await asyncio.sleep(self.tool_progress_delay_ms / 1000)
        if (session.closed or session.stop_reason is not None
                or call_id in session.cancelled
                or feedback_generation != session.tool_feedback_generation):
            return
        elapsed_ms = max(0, round((time.monotonic() - started) * 1000))
        try:
            sent = self._send_tool_responses(session, [{
                "name": name, "id": call_id,
                "response": {"status": "pending", "elapsed_ms": elapsed_ms},
                "willContinue": True, "scheduling": "WHEN_IDLE",
            }])
        except LiveError as exc:
            # An optional announcement cannot cancel an already accepted write.
            self._emit(session, {"type": "tool_progress_skipped", "id": call_id, "code": exc.code})
            return
        if sent:
            self._emit(session, {
                "type": "tool_progress", "name": name, "id": call_id,
                "status": "pending", "elapsed_ms": elapsed_ms,
            })

    def _send_tool_completions(
        self, session: _Session, responses: list[dict[str, Any]], feedback_generation: int,
    ) -> None:
        outgoing = []
        for response in responses:
            if response.get("id") in session.cancelled:
                continue
            if response.get("name") in session.non_blocking_tools:
                response = {
                    **response, "willContinue": False,
                    # Finish the same generator after new input without asking
                    # the model to announce a result for the superseded turn.
                    "scheduling": ("WHEN_IDLE" if feedback_generation == session.tool_feedback_generation
                                   else "SILENT"),
                }
            outgoing.append(response)
        self._send_tool_responses(session, outgoing)

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
            session.non_blocking_tools = _non_blocking_tools(resolved["configuration"])
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

    async def _handle_tool_calls(
        self, session: _Session, calls: list[dict[str, Any]], *, feedback_generation: int | None = None,
    ) -> None:
        if feedback_generation is None:
            feedback_generation = session.tool_feedback_generation
        async with session.tool_lock:
            calls = calls if isinstance(calls, list) else []
            if not calls or session.closed or session.stop_reason is not None:
                return
            if len(calls) == 1:
                call_id = str(calls[0].get("id") or "")
                if call_id and call_id in session.tool_results:
                    self._send_tool_completions(session, [{
                        "name": str(calls[0].get("name") or "unknown"),
                        "id": call_id,
                        "response": {"result": session.tool_results[call_id]},
                    }], feedback_generation)
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
                if session.closed or session.stop_reason is not None:
                    return
                call_id = str(call.get("id") or "")
                name = str(call.get("name") or "unknown")
                if call_id in session.cancelled:
                    continue
                started = _now_ms()
                progress_task = None
                try:
                    if call_id and call_id in session.tool_results:
                        result = session.tool_results[call_id]
                    else:
                        if (call_id and name in session.non_blocking_tools
                                and feedback_generation == session.tool_feedback_generation):
                            progress_task = asyncio.create_task(self._tool_progress_after_delay(
                                session, name=name, call_id=call_id,
                                feedback_generation=feedback_generation, started=time.monotonic(),
                            ), name=f"live-tool-progress-{session.id}")
                            session.tool_progress_task = progress_task
                            session.tool_progress_call_id = call_id
                        result = await _maybe_await(self.adapter.execute_tool(session, call))
                        # Cancellation of voice feedback cannot erase the real
                        # mutation receipt or cause a duplicate to execute again.
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
                    responses.append({"name": name, "id": call_id, "response": {"result": result}})
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
                finally:
                    if progress_task:
                        progress_task.cancel()
                        await asyncio.gather(progress_task, return_exceptions=True)
                        if session.tool_progress_task is progress_task:
                            session.tool_progress_task = None
                            session.tool_progress_call_id = None
            self._send_tool_completions(session, responses, feedback_generation)

    async def input(
        self,
        *,
        session_id: str,
        resource_id: str,
        message: dict[str, Any],
        actor: Any = None,
        received_at: int | None = None,
        _audio_projection=None,
        _delivery_guard=None,
    ) -> dict[str, Any]:
        session = self._get(session_id, resource_id, actor)
        self._touch(session)
        if _delivery_guard:
            _delivery_guard()
        if _audio_projection is not None:
            # The socket's server commit path constructs this object. It is not
            # a client message flag and never enters the provider JSON bridge.
            from .audio_v2 import DurablePCMProjection
            if (not isinstance(_audio_projection, DurablePCMProjection)
                    or message != {"audio_base64": base64.b64encode(_audio_projection.pcm16).decode("ascii")}):
                raise LiveError("LIVE_AUDIO_PROJECTION", "Invalid durable PCM projection")
            if hasattr(self.adapter, "input_audio"):
                admitted = await _maybe_await(self.adapter.input_audio(session, _audio_projection))
                if admitted is False:
                    return {"ok": True, "session_id": session.id, "already_projected": True}
        elif hasattr(self.adapter, "input"):
            await _maybe_await(self.adapter.input(session, message))
        if _delivery_guard:
            _delivery_guard()
        received_at = received_at or _now_ms()
        written_at = None

        if message.get("activity_start"):
            if not session.manual_activity_detection:
                raise LiveError("INVALID_ARGUMENT", "Manual activity is not enabled")
            if session.activity_open:
                raise LiveError("INVALID_ARGUMENT", "Activity is already open")
            generation = session.audio_turn_generation + 1
            written_at = self._write(session, {"type": "activity_start", "audio_turn_generation": generation})
            session.activity_open = True
            session.audio_turn_generation = generation
            session.audio_turn_open = True
            self._advance_tool_feedback(session)
        if "audio_base64" in message:
            audio = message.get("audio_base64")
            if not isinstance(audio, str) or len(audio) > 16_000:
                raise LiveError("INVALID_ARGUMENT", "Audio chunk is invalid")
            if session.manual_activity_detection and not session.activity_open:
                raise LiveError("INVALID_ARGUMENT", "activity_start is required before buffered audio")
            generation = session.audio_turn_generation + int(not session.audio_turn_open)
            written_at = self._write(session, {"type": "audio", "data": audio, "audio_turn_generation": generation})
            if not session.manual_activity_detection and not session.audio_turn_open:
                session.audio_turn_generation = generation
                session.audio_turn_open = True
                self._advance_tool_feedback(session)
        if message.get("audio_stream_end"):
            if session.manual_activity_detection:
                raise LiveError("INVALID_ARGUMENT", "Use activity_end when manual activity is enabled")
            self._write(session, {"type": "audio_stream_end", "audio_turn_generation": session.audio_turn_generation})
            if session.audio_turn_open:
                session.audio_end_awaiting_ack.append(session.audio_turn_generation)
                session.audio_turn_open = False
        if message.get("activity_end"):
            if not session.manual_activity_detection:
                raise LiveError("INVALID_ARGUMENT", "Manual activity is not enabled")
            if not session.activity_open:
                raise LiveError("INVALID_ARGUMENT", "No activity is open")
            self._write(session, {"type": "activity_end", "audio_turn_generation": session.audio_turn_generation})
            session.activity_open = False
            if session.audio_turn_open:
                session.audio_end_awaiting_ack.append(session.audio_turn_generation)
                session.audio_turn_open = False
        if "text" in message:
            text = message.get("text")
            if not isinstance(text, str) or not text.strip() or len(text) > 4_000:
                raise LiveError("INVALID_ARGUMENT", "Text turn is invalid")
            self._write(session, {"type": "text", "text": text.strip()})
            self._advance_tool_feedback(session)
        return {
            "ok": True,
            "session_id": session.id,
            "timing": {"received_at": received_at, "provider_input_at": written_at, "handled_at": _now_ms()},
        }

    async def input_pcm(self, *, session_id, resource_id, projection, actor=None, received_at=None, **transport):
        from .audio_v2 import DurablePCMProjection
        if not isinstance(projection, DurablePCMProjection):
            raise LiveError("LIVE_AUDIO_PROJECTION", "A server durable PCM projection is required")
        return await self.input(session_id=session_id, resource_id=resource_id, actor=actor,
                                received_at=received_at, _audio_projection=projection,
                                message={"audio_base64": base64.b64encode(projection.pcm16).decode("ascii")},
                                **transport)

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

    async def stop(
        self, *, session_id: str, resource_id: str, actor: Any = None,
        reason: LiveStopReason = "explicit_stop",
    ) -> dict[str, Any]:
        if reason not in ("explicit_stop", "detached_reclaim"):
            raise LiveError("INVALID_ARGUMENT", "Unknown Live stop reason")
        session = self._get(session_id, resource_id, actor)
        # An explicit Stop is absorbing: a concurrent internal reclaim cannot
        # later turn it back into a resumable termination.
        if session.stop_reason is None or reason == "explicit_stop":
            session.stop_reason = reason
        self._cancel_tool_progress(session)
        if not session.closed:
            try:
                self._write(session, {"type": "stop"})
            except LiveError:
                pass
        if hasattr(self.adapter, "on_stopped"):
            await _maybe_await(self.adapter.on_stopped(session))
        await self._discard(session, graceful=True)
        return {"ok": True, "session_id": session.id}

    async def _discard(self, session: _Session, graceful: bool = False) -> None:
        session.closed = True
        self._cancel_tool_progress(session)
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

"""Generic in-process Live session host for Python product adapters.

The host owns provider lifecycle, resource/actor binding, bounded input, event
pagination and ordered/deduplicated tool execution. Product adapters own domain
authorization and mutations.
"""
from __future__ import annotations

import asyncio
import inspect
import json
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


def _now_ms() -> int:
    return round(time.time() * 1000)


async def _maybe_await(value: Any) -> Any:
    return await value if inspect.isawaitable(value) else value


class LiveSessionHost:
    def __init__(
        self,
        *,
        adapter_factory: Callable[..., Any],
        key_resolver: Callable[[str, Any], str | None] | None = None,
        provider_run: Callable[..., Awaitable[None]] = provider.run,
        models: tuple[str, ...] = LIVE_SESSION_MODELS,
        ready_timeout_ms: int = 30_000,
        max_sessions: int = 2,
    ):
        self.models = tuple(models)
        self.ready_timeout_ms = ready_timeout_ms
        self.max_sessions = max_sessions
        self.provider_run = provider_run
        self.key_resolver = key_resolver or (lambda _resource_id, _actor: provider.default_key())
        self.sessions: dict[str, _Session] = {}
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
        if len(self.sessions) >= self.max_sessions:
            raise LiveError("LIVE_BUSY", "Live session limit reached")

        initialized = await _maybe_await(
            self.adapter.initialize(resource_id=resource_id, actor=actor, model=model, **args)
        )
        if not isinstance(initialized, dict):
            raise LiveError("LIVE_ADAPTER_ERROR", "Live adapter initialize() returned invalid state")

        session_id = "live_" + uuid.uuid4().hex
        reader = _QueueReader()
        loop = asyncio.get_running_loop()
        session = _Session(
            id=session_id,
            resource_id=resource_id,
            actor=actor,
            model=model,
            reader=reader,
            state=dict(initialized.get("state") or {}),
            ready=loop.create_future(),
        )
        self.sessions[session_id] = session
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
            if kind == "ready":
                self._emit(session, event)
                if session.ready and not session.ready.done():                    session.ready.set_result(event)
                return
            if kind == "error":
                self._emit(session, event)
                if session.ready and not session.ready.done():
                    session.ready.set_exception(
                        LiveError("LIVE_PROVIDER_ERROR", str(event.get("message") or "Gemini Live error")[:500])
                    )
                return
            if kind == "tool_cancelled":
                session.cancelled.update(str(v) for v in (event.get("ids") or []))
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
            self._emit(session, event)

        async def runner() -> None:
            try:
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
        response = initialized.get("response") or {}
        return {"session_id": session_id, "model": model, **response}

    async def _handle_tool_calls(self, session: _Session, calls: list[dict[str, Any]]) -> None:
        async with session.tool_lock:
            responses: list[dict[str, Any]] = []
            for call in calls if isinstance(calls, list) else []:
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
                        if call_id:
                            session.tool_results[call_id] = result
                            session.tool_order.append(call_id)
                            while len(session.tool_order) > 100:
                                old = session.tool_order.popleft()
                                session.tool_results.pop(old, None)
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
            if responses and not session.closed:
                session.tool_response_at = self._write(session, {"type": "tool_response", "responses": responses})
                session.awaiting_audio = True
                self._emit(session, {"type": "timing", "stage": "tool_response_written"})

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
        if hasattr(self.adapter, "input"):
            await _maybe_await(self.adapter.input(session, message))
        received_at = received_at or _now_ms()
        written_at = None

        if "audio_base64" in message:
            audio = message.get("audio_base64")
            if not isinstance(audio, str) or len(audio) > 16_000:
                raise LiveError("INVALID_ARGUMENT", "Audio chunk is invalid")
            written_at = self._write(session, {"type": "audio", "data": audio})
        if message.get("audio_stream_end"):
            self._write(session, {"type": "audio_stream_end"})
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
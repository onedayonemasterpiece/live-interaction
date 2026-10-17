import asyncio
import json
import unittest
from types import SimpleNamespace

from live_interaction.session_host import LiveSessionHost


async def settle(predicate):
    async with asyncio.timeout(1):
        while not predicate():
            await asyncio.sleep(0.001)


class ToolAdapter:
    def __init__(self, *, non_blocking=True, manual=True):
        self.non_blocking = non_blocking
        self.manual = manual
        self.called = []
        self.entered = {}
        self.release = {}
        self.errors = set()
        self.stop_entered = asyncio.Event()
        self.stop_release = None

    def initialize(self, **_kwargs):
        declaration = {"name": "operation.run"}
        if self.non_blocking:
            declaration["behavior"] = "NON_BLOCKING"
        return {"configuration": {
            "functions": [declaration], "manual_activity_detection": self.manual,
        }}

    async def execute_tool(self, _session, call):
        call_id = call["id"]
        self.called.append(call_id)
        self.entered.setdefault(call_id, asyncio.Event()).set()
        if call_id in self.release:
            await self.release[call_id].wait()
        if call_id in self.errors:
            raise RuntimeError("operation failed")
        return {"receipt": "committed-" + call_id, "revision": 7}

    async def on_stopped(self, _session):
        self.stop_entered.set()
        if self.stop_release:
            await self.stop_release.wait()


class ToolProvider:
    def __init__(self):
        self.messages = []
        self.events = None
        self.stop_release = None

    async def run(self, *, load_key, reader, on_event):
        self.events = on_event
        load_key()
        self.messages.append(json.loads(await reader.readline()))
        on_event({"type": "ready", "model": "gemini-3.8-live"})
        while line := await reader.readline():
            message = json.loads(line)
            self.messages.append(message)
            if message["type"] == "stop":
                if self.stop_release:
                    await self.stop_release.wait()
                return

    @property
    def responses(self):
        return [response for message in self.messages if message["type"] == "tool_response"
                for response in message["responses"]]

    @property
    def pending(self):
        return [response for response in self.responses if response.get("willContinue") is True]

    @property
    def completed(self):
        return [response for response in self.responses if response.get("willContinue") is not True]


class ToolProgressContract(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.contexts = []

    async def asyncTearDown(self):
        for ctx in self.contexts:
            for gate in ctx.adapter.release.values():
                gate.set()
            if ctx.adapter.stop_release:
                ctx.adapter.stop_release.set()
            if ctx.provider.stop_release:
                ctx.provider.stop_release.set()
            ctx.session.reader.close()
            await ctx.host.stop_all()
            await settle(lambda: not ctx.session.tool_lock.locked())

    async def start(self, *, non_blocking=True, manual=True, default_delay=False, model=None):
        adapter = ToolAdapter(non_blocking=non_blocking, manual=manual)
        provider = ToolProvider()
        host = LiveSessionHost(
            adapter_factory=lambda **_kwargs: adapter,
            key_resolver=lambda *_args: "fake-key",
            provider_run=provider.run, ready_timeout_ms=500,
            **({} if default_delay else {"tool_progress_delay_ms": 20}),
        )
        actor = {"subject": "owner", "tenant_id": "tenant"}
        started = await host.start(resource_id="project", actor=actor, model=model)
        ctx = SimpleNamespace(host=host, adapter=adapter, provider=provider, actor=actor,
                              session=host.sessions[started["session_id"]])
        self.contexts.append(ctx)
        return ctx

    def call(self, ctx, call_id="one", *, slow=True):
        if slow:
            ctx.adapter.release.setdefault(call_id, asyncio.Event())
        ctx.adapter.entered.setdefault(call_id, asyncio.Event())
        ctx.provider.events({"type": "tool_call", "calls": [
            {"id": call_id, "name": "operation.run", "args": {}},
        ]})

    async def input(self, ctx, message):
        await ctx.host.input(session_id=ctx.session.id, resource_id="project",
                             actor=ctx.actor, message=message)

    async def test_fast_call_has_only_real_completion_with_default_delay(self):
        ctx = await self.start(default_delay=True)
        self.assertEqual(ctx.host.tool_progress_delay_ms, 2500)
        self.call(ctx, slow=False)
        await settle(lambda: len(ctx.provider.responses) == 1)
        response = ctx.provider.responses[0]
        self.assertFalse(response["willContinue"])
        self.assertEqual(response["scheduling"], "WHEN_IDLE")
        self.assertEqual(response["response"]["result"], ctx.session.tool_results["one"])
        self.assertEqual(ctx.provider.pending, [])
        self.assertIsNone(ctx.session.tool_progress_task)

    async def test_long_call_sends_one_truthful_pending_then_same_id_final(self):
        ctx = await self.start()
        self.call(ctx)
        await settle(lambda: len(ctx.provider.pending) == 1)
        pending = ctx.provider.pending[0]
        self.assertEqual(pending["id"], "one")
        self.assertEqual(pending["scheduling"], "WHEN_IDLE")
        self.assertEqual(set(pending["response"]), {"status", "elapsed_ms"})
        self.assertEqual(pending["response"]["status"], "pending")
        self.assertGreaterEqual(pending["response"]["elapsed_ms"], 20)
        self.assertNotIn("one", ctx.session.tool_results)
        self.assertFalse(any(event["type"] == "tool_result" for event in ctx.session.events))
        await asyncio.sleep(0.05)
        self.assertEqual(len(ctx.provider.pending), 1)
        ctx.adapter.release["one"].set()
        await settle(lambda: len(ctx.provider.completed) == 1)
        final = ctx.provider.completed[0]
        self.assertEqual(final["id"], pending["id"])
        self.assertFalse(final["willContinue"])
        self.assertEqual(final["scheduling"], "WHEN_IDLE")
        self.assertEqual(final["response"]["result"]["receipt"], "committed-one")
        self.assertNotIn("status", ctx.session.tool_results["one"])
        self.assertIsNone(ctx.session.tool_progress_task)

    async def test_default_blocking_and_implicit_extended_behavior_are_unchanged(self):
        for model in (None, "gemini-3.8-live-extended-thinking"):
            with self.subTest(model=model):
                ctx = await self.start(non_blocking=False, model=model)
                self.call(ctx)
                await ctx.adapter.entered["one"].wait()
                await asyncio.sleep(0.05)
                self.assertEqual(ctx.provider.responses, [])
                self.assertIsNone(ctx.session.tool_progress_task)
                ctx.adapter.release["one"].set()
                await settle(lambda: len(ctx.provider.responses) == 1)
                self.assertEqual(set(ctx.provider.responses[0]), {"name", "id", "response"})

    async def test_provider_cancellation_suppresses_late_feedback_but_keeps_receipt(self):
        for after_pending in (False, True):
            with self.subTest(after_pending=after_pending):
                ctx = await self.start()
                self.call(ctx)
                await ctx.adapter.entered["one"].wait()
                if after_pending:
                    await settle(lambda: len(ctx.provider.pending) == 1)
                ctx.provider.events({"type": "tool_cancelled", "ids": ["one"]})
                await asyncio.sleep(0.05)
                ctx.adapter.release["one"].set()
                await settle(lambda: "one" in ctx.session.tool_results and not ctx.session.tool_lock.locked())
                self.call(ctx, slow=False)
                await asyncio.sleep(0.01)
                self.assertEqual(len(ctx.provider.pending), int(after_pending))
                self.assertEqual(ctx.provider.completed, [])
                self.assertEqual(ctx.adapter.called, ["one"])
                self.assertEqual(ctx.session.tool_results["one"]["receipt"], "committed-one")
                self.assertIsNone(ctx.session.tool_progress_task)

    async def test_stop_fences_timer_and_final_before_awaited_cleanup_completes(self):
        ctx = await self.start()
        ctx.adapter.stop_release = asyncio.Event()
        ctx.provider.stop_release = asyncio.Event()
        self.call(ctx)
        await ctx.adapter.entered["one"].wait()
        stop = asyncio.create_task(ctx.host.stop(
            session_id=ctx.session.id, resource_id="project", actor=ctx.actor,
        ))
        await ctx.adapter.stop_entered.wait()
        self.assertFalse(ctx.session.closed)
        await asyncio.sleep(0.05)
        ctx.adapter.release["one"].set()
        await settle(lambda: "one" in ctx.session.tool_results and not ctx.session.tool_lock.locked())
        self.assertFalse(ctx.session.closed)
        self.assertEqual(ctx.provider.responses, [])
        self.assertIsNone(ctx.session.tool_progress_task)
        ctx.adapter.stop_release.set()
        ctx.provider.stop_release.set()
        await stop

    async def test_provider_close_keeps_real_receipt_without_late_completion(self):
        ctx = await self.start()
        self.call(ctx)
        await settle(lambda: len(ctx.provider.pending) == 1)
        ctx.session.reader.close()
        await settle(lambda: ctx.session.closed)
        ctx.adapter.release["one"].set()
        await settle(lambda: "one" in ctx.session.tool_results and not ctx.session.tool_lock.locked())
        self.assertEqual(len(ctx.provider.pending), 1)
        self.assertEqual(ctx.provider.completed, [])
        self.assertEqual(ctx.session.tool_results["one"]["receipt"], "committed-one")
        self.assertIsNone(ctx.session.tool_progress_task)

    async def test_progress_backpressure_does_not_cancel_the_operation(self):
        ctx = await self.start()
        self.call(ctx)
        await ctx.adapter.entered["one"].wait()
        ctx.session.reader._max_bytes = 1
        await settle(lambda: any(event["type"] == "tool_progress_skipped" for event in ctx.session.events))
        self.assertEqual(ctx.provider.responses, [])
        self.assertNotIn("one", ctx.session.tool_results)
        ctx.session.reader._max_bytes = 768 * 1024
        ctx.adapter.release["one"].set()
        await settle(lambda: len(ctx.provider.completed) == 1)
        self.assertEqual(ctx.adapter.called, ["one"])
        self.assertEqual(ctx.provider.completed[0]["response"]["result"], ctx.session.tool_results["one"])

    async def test_new_text_or_speech_closes_result_silently_and_cancels_pending(self):
        for manual, message in ((True, {"text": "Новая реплика"}),
                                (True, {"activity_start": True}),
                                (False, {"audio_base64": "AAAA"})):
            for after_pending in (False, True):
                with self.subTest(manual=manual, message=message, after_pending=after_pending):
                    ctx = await self.start(manual=manual)
                    self.call(ctx)
                    await ctx.adapter.entered["one"].wait()
                    if after_pending:
                        await settle(lambda: len(ctx.provider.pending) == 1)
                    await self.input(ctx, message)
                    await asyncio.sleep(0.05)
                    ctx.adapter.release["one"].set()
                    await settle(lambda: len(ctx.provider.completed) == 1)
                    self.assertEqual(len(ctx.provider.pending), int(after_pending))
                    final = ctx.provider.completed[0]
                    self.assertFalse(final["willContinue"])
                    self.assertEqual(final["scheduling"], "SILENT")
                    self.assertEqual(final["response"]["result"], ctx.session.tool_results["one"])
                    self.assertIsNone(ctx.session.tool_progress_task)

    async def test_call_queued_before_new_input_keeps_its_original_feedback_generation(self):
        ctx = await self.start()
        self.call(ctx, "one")
        await ctx.adapter.entered["one"].wait()
        self.call(ctx, "two")
        await self.input(ctx, {"text": "Другой вопрос"})
        ctx.adapter.release["one"].set()
        await ctx.adapter.entered["two"].wait()
        await asyncio.sleep(0.05)
        self.assertEqual(ctx.provider.pending, [])
        ctx.adapter.release["two"].set()
        await settle(lambda: len(ctx.provider.completed) == 2)
        self.assertEqual(ctx.adapter.called, ["one", "two"])
        self.assertTrue(all(response["scheduling"] == "SILENT" for response in ctx.provider.completed))

    async def test_duplicate_during_execution_reuses_only_the_real_cached_result(self):
        ctx = await self.start()
        self.call(ctx)
        await ctx.adapter.entered["one"].wait()
        self.call(ctx)
        await settle(lambda: len(ctx.provider.pending) == 1)
        ctx.adapter.release["one"].set()
        await settle(lambda: len(ctx.provider.completed) == 2)
        self.assertEqual(ctx.adapter.called, ["one"])
        self.assertEqual(len(ctx.provider.pending), 1)
        self.assertTrue(all(response["response"] == {"result": ctx.session.tool_results["one"]}
                            for response in ctx.provider.completed))
        self.assertTrue(all(response["willContinue"] is False for response in ctx.provider.completed))

    async def test_failed_long_call_finishes_generator_with_actual_error(self):
        ctx = await self.start()
        ctx.adapter.errors.add("one")
        self.call(ctx)
        await settle(lambda: len(ctx.provider.pending) == 1)
        ctx.adapter.release["one"].set()
        await settle(lambda: len(ctx.provider.completed) == 1)
        final = ctx.provider.completed[0]
        self.assertFalse(final["willContinue"])
        self.assertEqual(final["response"]["error"]["code"], "LIVE_TOOL_ERROR")
        self.assertNotIn("one", ctx.session.tool_results)
        self.assertIsNone(ctx.session.tool_progress_task)


if __name__ == "__main__":
    unittest.main()

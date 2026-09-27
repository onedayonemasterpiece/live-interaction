import asyncio
import unittest

from live_interaction.session_host import LiveError, LiveSessionHost


class Adapter:
    def __init__(self):
        self.called = []
        self.observed = []

    def on_event(self, session, event):
        self.observed.append((session.resource_id, event.get("type"), event.get("text")))

    def initialize(self, *, resource_id, actor, model, **_args):
        return {
            "state": {},
            "context": {"resourceId": resource_id},
            "configuration": {"functions": [{"name": "story.read"}]},
            "response": {"story_id": resource_id},
        }

    async def execute_tool(self, _session, call):
        self.called.append(call["id"])
        return {"value": "story", "revision": 7}


class Provider:
    def __init__(self):
        self.inputs = []
        self.events = None

    async def run(self, *, load_key, reader, on_event):
        self.events = on_event
        self.assert_key = load_key()
        start = await reader.readline()
        self.inputs.append(start)
        on_event({"type": "ready", "model": "gemini-3.8-live"})
        while True:
            line = await reader.readline()
            if not line:
                return
            self.inputs.append(line)
            if b'"type":"stop"' in line:
                return


class SessionHostContract(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.adapter = Adapter()
        self.provider = Provider()
        self.host = LiveSessionHost(
            adapter_factory=lambda **_kw: self.adapter,
            key_resolver=lambda _resource, _actor: "test-key",
            provider_run=self.provider.run,
            ready_timeout_ms=500,
        )
        self.base_actor = {"subject": "a", "tenant_id": "t"}
        self.started = await self.host.start(resource_id="story1", actor=self.base_actor)

    async def asyncTearDown(self):
        await self.host.stop_all()

    async def test_owner_binding_input_limits_and_event_pagination(self):
        with self.assertRaises(LiveError) as forbidden:
            await self.host.input(
                resource_id="story1",
                session_id=self.started["session_id"],
                actor={"subject": "b", "tenant_id": "t"},
                message={"text": "test"},
            )
        self.assertEqual(forbidden.exception.code, "FORBIDDEN")

        with self.assertRaises(LiveError) as invalid:
            await self.host.input(
                resource_id="story1",
                session_id=self.started["session_id"],
                actor=self.base_actor,
                message={"audio_base64": "a" * 16001},
            )
        self.assertEqual(invalid.exception.code, "INVALID_ARGUMENT")

        session = self.host.sessions[self.started["session_id"]]
        for n in range(140):
            self.host._emit(session, {"type": "audio", "data": str(n)})
        page = self.host.events(
            resource_id="story1", session_id=session.id, actor=self.base_actor
        )
        audio = sum(event["type"] == "audio" for event in page["events"])
        while page["has_more"]:
            page = self.host.events(
                resource_id="story1",
                session_id=session.id,
                actor=self.base_actor,
                after=page["cursor"],
            )
            audio += sum(event["type"] == "audio" for event in page["events"])
        self.assertEqual(audio, 140)

    async def test_tools_are_serialized_deduplicated_and_cancel_before_start(self):
        session = self.host.sessions[self.started["session_id"]]
        self.provider.events(
            {
                "type": "tool_call",
                "calls": [
                    {"name": "story.read", "id": "one", "args": {}},
                    {"name": "story.read", "id": "one", "args": {}},
                    {"name": "story.write", "id": "two", "args": {}},
                ],
            }
        )
        self.provider.events({"type": "tool_cancelled", "ids": ["two"]})
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        self.assertEqual(self.adapter.called, ["one"])
        results = [e for e in session.events if e["type"] == "tool_result"]
        self.assertEqual([r["id"] for r in results], ["one", "one"])

    async def test_provider_events_are_visible_to_product_adapter(self):
        self.provider.events({"type": "input_transcript", "text": "Привет"})
        await asyncio.sleep(0)
        self.assertIn(("story1", "input_transcript", "Привет"), self.adapter.observed)

    async def test_stop_is_bounded_and_removes_session(self):
        session_id = self.started["session_id"]
        result = await self.host.stop(
            resource_id="story1", session_id=session_id, actor=self.base_actor
        )
        self.assertTrue(result["ok"])
        self.assertEqual(self.host.size(), 0)

class ManagedProvider:
    def __init__(self):
        self.calls = 0
        self.cancelled = False
        self.started = asyncio.Event()

    async def run(self, *, session, reader, on_event):
        self.calls += 1
        start = await reader.readline()
        self.started.set()
        on_event({"type": "ready", "model": session.model})
        try:
            while True:
                line = await reader.readline()
                if not line:
                    return
                if b'"type":"stop"' in line:
                    return
        except asyncio.CancelledError:
            self.cancelled = True
            raise


class ManagedHostContract(unittest.IsolatedAsyncioTestCase):
    async def test_managed_runner_does_not_resolve_legacy_key(self):
        adapter = Adapter()
        provider = ManagedProvider()
        legacy_calls = []
        host = LiveSessionHost(
            adapter_factory=lambda **_kw: adapter,
            key_resolver=lambda *_args: legacy_calls.append(True) or "legacy-key",
            managed_runner=provider.run,
            ready_timeout_ms=500,
        )
        try:
            started = await host.start(
                resource_id="search1",
                actor={"subject": "a", "tenant_id": "t"},
            )
            self.assertTrue(started["session_id"].startswith("live_"))
            self.assertEqual(provider.calls, 1)
            self.assertEqual(legacy_calls, [])
        finally:
            await host.stop_all()

    async def test_client_liveness_discards_session_and_cancels_provider(self):
        from unittest.mock import patch
        adapter = Adapter()
        provider = ManagedProvider()
        now = [1000]
        host = LiveSessionHost(
            adapter_factory=lambda **_kw: adapter,
            managed_runner=provider.run,
            ready_timeout_ms=500,
            client_liveness_timeout_ms=1000,
        )
        with patch("live_interaction.session_host._now_ms", side_effect=lambda: now[0]):
            started = await host.start(
                resource_id="search1",
                actor={"subject": "a", "tenant_id": "t"},
            )
            now[0] = 2500
            await asyncio.sleep(0.45)
            self.assertNotIn(started["session_id"], host.sessions)
            self.assertTrue(provider.cancelled)



class CapabilityAdapter(Adapter):
    def initialize(self, *, resource_id, actor, model, **_args):
        return {
            "state": {},
            "capability": "core",
            "context": {"resourceId": resource_id},
            "configuration": {
                "functions": [
                    {"name": "activate_capability"},
                    {"name": "read"},
                ]
            },
        }

    async def resolve_capability(self, _session, call):
        if call.get("name") != "activate_capability":
            return None
        return {
            "capability": "dataset",
            "configuration": {
                "system_instruction": "dataset",
                "functions": [
                    {"name": "activate_capability"},
                    {"name": "dataset.find"},
                    {"name": "dataset.read"},
                ],
            },
            "context": {"scope": "dataset"},
            "continuation": str((call.get("args") or {}).get("intent") or ""),
            "response": {"selected": "dataset"},
        }


class CapabilityProvider:
    def __init__(self):
        self.messages = []
        self.events = None

    async def run(self, *, session, reader, on_event):
        self.events = on_event
        start = await reader.readline()
        self.messages.append(start)
        on_event({"type": "ready", "model": session.model})
        while True:
            line = await reader.readline()
            if not line:
                return
            self.messages.append(line)
            message = __import__("json").loads(line)
            if message.get("type") == "reconfigure":
                on_event(
                    {
                        "type": "capability_transition_started",
                        "transition_id": message["transition_id"],
                        "capability": message["capability"],
                    }
                )
                on_event(
                    {
                        "type": "capability_ready",
                        "transition_id": message["transition_id"],
                        "capability": message["capability"],
                    }
                )
            if message.get("type") == "stop":
                return


class CapabilityHostContract(unittest.IsolatedAsyncioTestCase):
    async def test_capability_router_delegates_acknowledgement_to_provider(self):
        adapter = CapabilityAdapter()
        provider = CapabilityProvider()
        host = LiveSessionHost(
            adapter_factory=lambda **_kw: adapter,
            managed_runner=provider.run,
            ready_timeout_ms=500,
            reconfigure_timeout_ms=500,
        )
        actor = {"subject": "a", "tenant_id": "t"}
        started = await host.start(resource_id="story1", actor=actor)
        session = host.sessions[started["session_id"]]
        provider.events({
            "type": "tool_call",
            "calls": [{
                "name": "activate_capability",
                "id": "cap1",
                "args": {"capability": "dataset", "intent": "find national projects dataset"},
            }],
        })
        for _ in range(40):
            decoded = [__import__("json").loads(item) for item in provider.messages]
            if any(event.get("type") == "tool_result" and event.get("id") == "cap1" for event in session.events):
                break
            await asyncio.sleep(0)
        decoded = [__import__("json").loads(item) for item in provider.messages]
        reconfigure = next(item for item in decoded if item.get("type") == "reconfigure")
        self.assertEqual(reconfigure["capability"], "dataset")
        self.assertEqual(len(reconfigure["configuration"]["functions"]), 3)
        self.assertEqual(reconfigure["continuation"], "find national projects dataset")
        self.assertEqual(reconfigure["router_response"]["id"], "cap1")
        self.assertEqual(reconfigure["router_response"]["response"]["scheduling"], "SILENT")
        self.assertFalse(any(item.get("type") == "tool_response" for item in decoded))
        self.assertEqual(session.capability, "dataset")
        self.assertTrue(any(
            event.get("type") == "capability_transition_requested"
            and event.get("to_capability") == "dataset"
            for event in session.events
        ))
        await host.stop(resource_id="story1", session_id=started["session_id"], actor=actor)

    async def test_capability_bundle_limit_is_structured_tool_error(self):
        class TooBig(CapabilityAdapter):
            async def resolve_capability(self, _session, _call):
                return {
                    "capability": "too_big",
                    "configuration": {
                        "functions": [{"name": f"f{i}"} for i in range(10)]
                    },
                }

        adapter = TooBig()
        provider = CapabilityProvider()
        host = LiveSessionHost(
            adapter_factory=lambda **_kw: adapter,
            managed_runner=provider.run,
            ready_timeout_ms=500,
            reconfigure_timeout_ms=100,
        )
        actor = {"subject": "a", "tenant_id": "t"}
        started = await host.start(resource_id="story2", actor=actor)
        provider.events(
            {
                "type": "tool_call",
                "calls": [
                    {"name": "activate_capability", "id": "cap2", "args": {}}
                ],
            }
        )
        for _ in range(40):
            decoded = [__import__("json").loads(item) for item in provider.messages]
            if any(item.get("type") == "tool_response" for item in decoded):
                break
            await asyncio.sleep(0)
        decoded = [__import__("json").loads(item) for item in provider.messages]
        self.assertFalse(any(item.get("type") == "reconfigure" for item in decoded))
        response = next(item for item in decoded if item.get("type") == "tool_response")
        self.assertEqual(
            response["responses"][0]["response"]["error"]["code"],
            "LIVE_CAPABILITY_LIMIT",
        )
        await host.stop(
            resource_id="story2",
            session_id=started["session_id"],
            actor=actor,
        )


class SlowAdapter(Adapter):
    def __init__(self):
        super().__init__()
        self.entered = asyncio.Event()
        self.release = asyncio.Event()

    async def initialize(self, **kwargs):
        self.entered.set()
        await self.release.wait()
        return super().initialize(**kwargs)


class ConcurrentStartContract(unittest.IsolatedAsyncioTestCase):
    async def test_start_slot_is_reserved_before_async_initialization(self):
        adapter = SlowAdapter()
        provider = ManagedProvider()
        host = LiveSessionHost(
            adapter_factory=lambda **_kw: adapter,
            managed_runner=provider.run,
            max_sessions=1,
            ready_timeout_ms=500,
        )
        actor = {"subject": "a", "tenant_id": "t"}
        first = asyncio.create_task(host.start(resource_id="one", actor=actor))
        await adapter.entered.wait()
        with self.assertRaises(LiveError) as busy:
            await host.start(resource_id="two", actor=actor)
        self.assertEqual(busy.exception.code, "LIVE_BUSY")
        adapter.release.set()
        started = await first
        self.assertTrue(started["session_id"].startswith("live_"))
        await host.stop_all()


if __name__ == "__main__":
    unittest.main()
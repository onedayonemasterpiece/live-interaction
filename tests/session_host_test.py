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


if __name__ == "__main__":
    unittest.main()
import unittest

from live_interaction.transcribe import (
    MODEL,
    _provider_error_event,
    handle_server_message,
    input_payload,
    setup_config,
)


class TranscribeLiveContract(unittest.TestCase):
    def test_setup_is_text_only_smart_and_manual_boundary_compatible(self):
        setup = setup_config({
            "input_audio_transcription": {
                "languageCodes": [],
                "customVocabulary": ["Мира", "Projects Hub", "Codex"],
                "mode": "SMART",
            },
            "manual_activity_detection": True,
        })["setup"]
        self.assertEqual(setup["model"], "models/" + MODEL)
        self.assertEqual(setup["generationConfig"]["responseModalities"], ["TEXT"])
        self.assertEqual(setup["inputAudioTranscription"]["mode"], "SMART")
        self.assertEqual(
            setup["realtimeInputConfig"]["automaticActivityDetection"],
            {"disabled": True},
        )
        self.assertNotIn("tools", setup)
        self.assertNotIn("systemInstruction", setup)
        self.assertNotIn("outputAudioTranscription", setup)
        self.assertNotIn("sessionResumption", setup)

    def test_vocabulary_supports_google_live_transcribe_bound(self):
        setup = setup_config({
            "input_audio_transcription": {
                "customVocabulary": [f"term-{i}" for i in range(1000)],
            }
        })["setup"]
        self.assertEqual(len(setup["inputAudioTranscription"]["customVocabulary"]), 1000)
        with self.assertRaisesRegex(ValueError, "customVocabulary"):
            setup_config({
                "input_audio_transcription": {
                    "customVocabulary": ["x"] * 1001,
                }
            })

    def test_wire_mapping_mirrors_only_audio_boundaries(self):
        self.assertEqual(
            input_payload({"type": "activity_start"}),
            {"realtimeInput": {"activityStart": {}}},
        )
        self.assertEqual(
            input_payload({"type": "activity_end"}),
            {"realtimeInput": {"activityEnd": {}}},
        )
        self.assertEqual(
            input_payload({"type": "audio_stream_end"}),
            {"realtimeInput": {"audioStreamEnd": True}},
        )
        audio = input_payload({"type": "audio", "data": "AAAA"})
        self.assertEqual(audio["realtimeInput"]["audio"]["mimeType"], "audio/pcm;rate=16000")
        self.assertEqual(audio["realtimeInput"]["audio"]["data"], "AAAA")
        self.assertIsNone(input_payload({"type": "text", "text": "semantic request"}))
        self.assertIsNone(input_payload({"type": "snapshot", "context": {"x": 1}}))
        self.assertIsNone(input_payload({"type": "tool_response", "responses": []}))

    def test_interim_and_final_transcripts_are_distinct(self):
        seen = []
        handle_server_message({
            "serverContent": {
                "interimInputTranscription": {"text": "промежуточный"},
                "inputTranscription": {"text": "финальный"},
                "turnComplete": True,
            }
        }, seen.append)
        self.assertEqual(
            [item["type"] for item in seen],
            ["interim_input_transcript", "input_transcript", "turn_complete"],
        )
        self.assertEqual(seen[0]["text"], "промежуточный")
        self.assertEqual(seen[1]["text"], "финальный")

    def test_clean_local_stop_does_not_emit_provider_error(self):
        exc = RuntimeError("normal close")
        self.assertIsNone(_provider_error_event(exc, "fixture-key", stopped=True))
        event = _provider_error_event(exc, "fixture-key", stopped=False)
        self.assertEqual(event["type"], "error")
        self.assertEqual(event["code"], "RuntimeError")


if __name__ == "__main__":
    unittest.main()


class TranscribeRenewalContract(unittest.IsolatedAsyncioTestCase):
    async def test_go_away_renews_caption_session_and_reopens_activity(self):
        import asyncio
        from unittest.mock import patch
        from provider_test import _FakeSocket, _QueueReader
        from live_interaction.transcribe import MODEL, run
        reader = _QueueReader()
        reader.feed({"type": "start", "model": MODEL, "configuration": {}})
        events, sockets = [], []

        def connect(*_args, **_kwargs):
            incoming = [{"setupComplete": {}}]
            sockets.append(_FakeSocket(incoming))
            return sockets[-1]

        with patch("websockets.connect", side_effect=connect):
            task = asyncio.create_task(run(load_key=lambda: "fixture-key", reader=reader, on_event=events.append))
            async with asyncio.timeout(2):
                while not any(e.get("type") == "ready" for e in events):
                    await asyncio.sleep(.001)
            reader.feed({"type": "activity_start"})
            async with asyncio.timeout(2):
                while not any("realtimeInput" in m for m in sockets[0].sent[1:]):
                    await asyncio.sleep(.001)
            sockets[0].incoming.put_nowait({"goAway": {"timeLeft": "1s"}})
            async with asyncio.timeout(2):
                while not any(e.get("type") == "resumed" for e in events):
                    await asyncio.sleep(.001)
            self.assertEqual(sockets[1].sent[1], {"realtimeInput": {"activityStart": {}}})
            self.assertFalse(any(e.get("type") == "error" for e in events))
            reader.feed({"type": "stop"})
            await asyncio.wait_for(task, 1)


class TranscribeOutageContract(unittest.IsolatedAsyncioTestCase):
    async def wait_for(self, predicate):
        import asyncio
        async with asyncio.timeout(1):
            while not predicate():
                await asyncio.sleep(.001)

    async def test_stop_during_connect_does_not_wait_for_or_send_new_setup(self):
        import asyncio
        from unittest.mock import patch
        from provider_test import _FakeSocket, _QueueReader
        from live_interaction.transcribe import run
        reader, events, sockets = _QueueReader(), [], []
        reader.feed({"type": "start", "model": MODEL, "configuration": {}})
        reconnect_started, release = asyncio.Event(), asyncio.Event()

        class Delayed:
            async def __aenter__(self):
                reconnect_started.set()
                await release.wait()
                socket = _FakeSocket([{"setupComplete": {}}])
                sockets.append(socket)
                return socket
            async def __aexit__(self, *args):
                return False

        def connect(*args, **kwargs):
            if sockets:
                return Delayed()
            socket = _FakeSocket([{"setupComplete": {}}])
            sockets.append(socket)
            return socket

        with patch("websockets.connect", side_effect=connect):
            task = asyncio.create_task(run(reader=reader, load_key=lambda: "fixture-key", on_event=events.append))
            try:
                await self.wait_for(lambda: any(e["type"] == "ready" for e in events))
                sockets[0].incoming.put_nowait({"goAway": {"timeLeft": "1s"}})
                await reconnect_started.wait()
                reader.feed({"type": "stop"})
                await asyncio.wait_for(task, .2)
                self.assertEqual(len(sockets), 1)
                self.assertFalse(any(e["type"] in ("resumed", "error") for e in events))
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    async def test_outage_pcm_is_dropped_and_only_fresh_audio_reaches_renewed_provider(self):
        import asyncio
        from unittest.mock import patch
        from provider_test import _FakeSocket, _QueueReader
        from live_interaction.transcribe import run
        reader, events, sockets = _QueueReader(), [], []
        reader.feed({"type": "start", "model": MODEL, "configuration": {}})
        reconnect_started, release = asyncio.Event(), asyncio.Event()

        class Delayed:
            async def __aenter__(self):
                reconnect_started.set()
                await release.wait()
                socket = _FakeSocket([{"setupComplete": {}}])
                sockets.append(socket)
                return socket
            async def __aexit__(self, *args):
                return False

        def connect(*args, **kwargs):
            if sockets:
                return Delayed()
            socket = _FakeSocket([{"setupComplete": {}}])
            sockets.append(socket)
            return socket

        with patch("websockets.connect", side_effect=connect):
            task = asyncio.create_task(run(reader=reader, load_key=lambda: "fixture-key", on_event=events.append))
            try:
                await self.wait_for(lambda: any(e["type"] == "ready" for e in events))
                sockets[0].incoming.put_nowait({"goAway": {"timeLeft": "1s"}})
                await reconnect_started.wait()
                reader.feed({"type": "activity_start"})
                reader.feed({"type": "audio", "data": "AAAA", "queued_at": 1})
                reader.feed({"type": "activity_end"})
                await self.wait_for(lambda: reader.queue.empty())
                self.assertTrue(any(e["type"] == "input_dropped" for e in events))
                release.set()
                await self.wait_for(lambda: any(e["type"] == "resumed" for e in events))
                self.assertFalse(any("audio" in m.get("realtimeInput", {}) for m in sockets[1].sent))
                reader.feed({"type": "activity_start"})
                reader.feed({"type": "audio", "data": "AQAB", "queued_at": 10**13})
                await self.wait_for(lambda: any("audio" in m.get("realtimeInput", {}) for m in sockets[1].sent))
                payloads = [m["realtimeInput"]["audio"]["data"] for m in sockets[1].sent if "audio" in m.get("realtimeInput", {})]
                self.assertEqual(payloads, ["AQAB"])
                reader.feed({"type": "stop"})
                await asyncio.wait_for(task, 1)
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)


class TranscribeStartupContract(unittest.IsolatedAsyncioTestCase):
    async def initial_setup(self, action):
        import asyncio
        from unittest.mock import patch
        from provider_test import _FakeSocket, _QueueReader
        from live_interaction.transcribe import run
        reader, events, sockets = _QueueReader(), [], []
        reader.feed({"type": "start", "model": MODEL, "configuration": {}})
        connecting, release = asyncio.Event(), asyncio.Event()

        class Delayed:
            async def __aenter__(self):
                connecting.set()
                await release.wait()
                socket = _FakeSocket([{"setupComplete": {}}])
                sockets.append(socket)
                return socket
            async def __aexit__(self, *args):
                return False

        with patch("websockets.connect", return_value=Delayed()):
            task = asyncio.create_task(run(reader=reader, load_key=lambda: "fixture-key", on_event=events.append))
            try:
                await connecting.wait()
                prefix = [{"type": "activity_start"},
                          {"type": "audio", "data": "AAAA", "queued_at": 1},
                          {"type": "audio", "data": "AQAB", "queued_at": 2},
                          {"type": "activity_end"}]
                for message in prefix:
                    reader.feed(message)
                if action == "stop":
                    reader.feed({"type": "stop"})
                    await asyncio.wait_for(task, .2)
                    self.assertEqual(sockets, [])
                    self.assertFalse(any(e["type"] in ("ready", "error") for e in events))
                elif action == "overflow":
                    for _ in range(60):
                        reader.feed({"type": "audio", "data": "A" * 16_000})
                    await asyncio.wait_for(task, .2)
                    self.assertEqual(sockets, [])
                    self.assertTrue(any(e["type"] == "error" and e.get("message") == "caption_startup_backpressure"
                                        for e in events))
                else:
                    await asyncio.sleep(.01)  # input arrives while setup is still blocked
                    self.assertFalse(any(e["type"] == "input_dropped" for e in events))
                    release.set()
                    async with asyncio.timeout(1):
                        while not any(e["type"] == "ready" for e in events):
                            await asyncio.sleep(.001)
                    async with asyncio.timeout(1):
                        while len(sockets[0].sent) < len(prefix) + 1:
                            await asyncio.sleep(.001)
                    self.assertEqual(sockets[0].sent[1:], [input_payload(message) for message in prefix])
                    reader.feed({"type": "stop"})
                    await asyncio.wait_for(task, 1)
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)

    async def test_initial_admitted_prefix_reaches_provider_once_in_order(self):
        await self.initial_setup("preserve")

    async def test_stop_during_initial_setup_discards_prefix_without_waiting_for_connect(self):
        await self.initial_setup("stop")

    async def test_initial_prefix_has_the_same_byte_bound_as_host_input(self):
        await self.initial_setup("overflow")



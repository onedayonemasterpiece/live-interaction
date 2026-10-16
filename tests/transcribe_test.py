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

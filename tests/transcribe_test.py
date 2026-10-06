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

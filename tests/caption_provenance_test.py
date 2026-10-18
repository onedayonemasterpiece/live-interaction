import asyncio
import base64
import hashlib
import json
import unittest
from unittest.mock import patch

from live_interaction.caption_provenance import CaptionAudioLedger
from live_interaction.transcribe import MODEL, handle_server_message, input_payload, run, setup_config
from provider_test import _FakeSocket, _QueueReader


def scope(turn="A", sequence=1):
    return {"session_id": "session", "conversation_id": "conversation", "source_id": "source",
            "utterance_id": "utterance_" + turn, "turn_id": "turn_" + turn,
            "utterance_sequence": sequence}


def audio(turn="A", sequence=1, start=500_000, samples=1600):
    return {"type": "audio", "data": base64.b64encode(b"\x01\x00" * samples).decode(),
            "caption_scope": scope(turn, sequence), "capture_start_us": start,
            "capture_end_us": start + samples * 1_000_000 // 16_000}


def transcription(ledger, start="0s", end="0.1s"):
    return ledger.transcript("input_transcript", {"text": "  МИРА\nпривет ",
                                                  "startOffset": start, "endOffset": end})


class CaptionProvenanceContract(unittest.TestCase):
    def test_native_timestamps_require_verbatim_and_private_metadata_stays_off_wire(self):
        config = {"native_caption_provenance": {"offset_clock": "unverified"},
                  "input_audio_transcription": {"mode": "VERBATIM"}}
        setup = setup_config(config)["setup"]
        self.assertTrue(setup["realtimeInputConfig"]["interimTranscriptTimestampEnabled"])
        self.assertNotIn("native_caption_provenance", setup)
        self.assertNotIn("caption_scope", input_payload(audio())["realtimeInput"])
        self.assertEqual(set(input_payload(audio())["realtimeInput"]["audio"]), {"data", "mimeType"})
        config["input_audio_transcription"]["mode"] = "SMART"
        with self.assertRaisesRegex(ValueError, "require_verbatim"):
            setup_config(config)

    def test_unverified_offsets_never_assign_a_turn_even_when_they_look_plausible(self):
        ledger = CaptionAudioLedger(diagnostics=True)
        ledger.connected()
        ledger.sent(audio())
        ledger.sent(audio("B", 2, 900_000))
        event = transcription(ledger)
        self.assertNotIn("caption_scope", event)
        probe = event["caption_timing"]
        self.assertFalse(probe["assigned"])
        self.assertEqual(probe["offset_clock"], "unverified")
        self.assertEqual(probe["sent_pcm_samples"], 3200)
        self.assertEqual((probe["start_offset"], probe["end_offset"]), ("0s", "0.1s"))
        self.assertEqual(probe["text_sha256"], hashlib.sha256("мира привет".encode()).hexdigest())
        self.assertNotIn("text", probe)
        self.assertNotIn("observed_finished", probe)

    def test_explicit_synthetic_clock_maps_audio_ranges_not_latest_turn(self):
        # A synthetic contract test is not evidence of Google's actual clock.
        ledger = CaptionAudioLedger(offset_clock="connection_pcm")
        ledger.connected()
        ledger.sent(audio())
        ledger.sent({"type": "activity_end"})
        ledger.sent(audio("B", 2, 900_000))
        self.assertEqual(transcription(ledger)["caption_scope"], scope())
        self.assertEqual(transcription(ledger, "0.1s", "0.2s")["caption_scope"], scope("B", 2))
        for start, end in [(None, "0.1s"), ("0s", None), (0, .1), ("0.2s", "0.3s"),
                           ("0.09s", "0.11s"), ("-0.01s", "0s"), ("0s", "0s")]:
            self.assertNotIn("caption_scope", transcription(ledger, start, end))
        ledger.sent({"type": "audio_stream_end"})
        self.assertNotIn("caption_scope", transcription(ledger))

    def test_no_audio_missing_source_evicted_range_and_new_provider_generation_are_unassigned(self):
        ledger = CaptionAudioLedger(offset_clock="connection_pcm", max_spans=2)
        ledger.connected()
        self.assertNotIn("caption_scope", transcription(ledger))
        ledger.sent({**audio(), "caption_scope": None})
        self.assertNotIn("caption_scope", transcription(ledger))
        ledger.sent(audio("B", 2, 1_000_000))
        ledger.sent(audio("C", 3, 2_000_000))
        self.assertEqual(len(ledger._spans), 2)
        self.assertNotIn("caption_scope", transcription(ledger))
        ledger.connected()
        self.assertEqual(ledger.provider_connection_generation, 2)
        self.assertEqual(ledger.sent_pcm_samples, 0)
        self.assertNotIn("caption_scope", transcription(ledger))
        ledger.sent(audio("D", 4, 3_000_000))
        self.assertEqual(transcription(ledger)["caption_scope"], scope("D", 4))

    def test_finished_is_only_observed_when_provider_actually_sends_a_boolean(self):
        ledger, seen = CaptionAudioLedger(), []
        ledger.connected()
        handle_server_message({"serverContent": {"inputTranscription": {
            "text": "Мира", "startOffset": "0.000000001s", "endOffset": "0.1s", "finished": False,
        }}}, seen.append, caption_ledger=ledger)
        self.assertIs(seen[0]["caption_timing"]["observed_finished"], False)
        self.assertEqual(seen[0]["caption_timing"]["start_offset"], "0.000000001s")
        self.assertNotIn("caption_scope", seen[0])


class CaptionSentAudioContract(unittest.IsolatedAsyncioTestCase):
    async def wait_for(self, predicate):
        async with asyncio.timeout(2):
            while not predicate():
                await asyncio.sleep(.001)

    def reader(self):
        reader = _QueueReader()
        reader.feed({"type": "start", "model": MODEL, "configuration": {
            "input_audio_transcription": {"mode": "VERBATIM"},
            "native_caption_provenance": {"offset_clock": "unverified", "diagnostics": True},
        }})
        return reader

    async def test_actual_sender_records_startup_and_new_socket_separately(self):
        reader, events, sockets = self.reader(), [], []
        reader.feed({"type": "activity_start"})
        reader.feed(audio())

        def connect(*args, **kwargs):
            socket = _FakeSocket([{"setupComplete": {}}])
            sockets.append(socket)
            return socket

        with patch("websockets.connect", side_effect=connect):
            task = asyncio.create_task(run(reader=reader, load_key=lambda: "fixture-key", on_event=events.append))
            try:
                await self.wait_for(lambda: any(e["type"] == "caption_audio_sent" for e in events))
                first = next(e for e in events if e["type"] == "caption_audio_sent")
                self.assertEqual(first["caption_scope"], scope())
                self.assertEqual((first["provider_connection_generation"], first["stream_start_sample"],
                                  first["stream_end_sample"]), (1, 0, 1600))
                sockets[0].incoming.put_nowait({"serverContent": {"inputTranscription": {
                    "text": "Мира", "startOffset": "0s", "endOffset": "0.1s"}}})
                await self.wait_for(lambda: any(e["type"] == "input_transcript" for e in events))
                self.assertFalse(next(e for e in events if e["type"] == "input_transcript")["caption_timing"]["assigned"])
                sockets[0].incoming.put_nowait({"goAway": {"timeLeft": "1s"}})
                await self.wait_for(lambda: any(e["type"] == "resumed" for e in events))
                reader.feed(audio("B", 2, 2_000_000))
                await self.wait_for(lambda: len([e for e in events if e["type"] == "caption_audio_sent"]) == 2)
                last = [e for e in events if e["type"] == "caption_audio_sent"][-1]
                self.assertEqual((last["provider_connection_generation"], last["stream_start_sample"]), (2, 0))
                self.assertEqual(last["caption_scope"], scope("B", 2))
                self.assertNotIn("caption_scope", json.dumps(sockets[0].sent))
                self.assertNotIn("caption_scope", json.dumps(sockets[1].sent))
            finally:
                reader.feed({"type": "stop"})
                await asyncio.wait_for(task, 1)

    async def test_failed_provider_write_never_becomes_sent_audio_evidence(self):
        reader, events = self.reader(), []
        entered, release = asyncio.Event(), asyncio.Event()

        class FailingSocket(_FakeSocket):
            async def send(self, payload):
                if "audio" in (json.loads(payload).get("realtimeInput") or {}):
                    entered.set()
                    await release.wait()
                    raise ConnectionError("synthetic write failure")
                await super().send(payload)

        socket = FailingSocket([{"setupComplete": {}}])
        with patch("websockets.connect", return_value=socket):
            task = asyncio.create_task(run(reader=reader, load_key=lambda: "fixture-key", on_event=events.append))
            try:
                await self.wait_for(lambda: any(e["type"] == "ready" for e in events))
                reader.feed(audio())
                await asyncio.wait_for(entered.wait(), 1)
                self.assertFalse(any(e["type"] == "caption_audio_sent" for e in events))
                release.set()
                await self.wait_for(lambda: any(e["type"] == "input_dropped" for e in events))
                self.assertFalse(any(e["type"] == "caption_audio_sent" for e in events))
            finally:
                release.set()
                reader.feed({"type": "stop"})
                await asyncio.wait_for(task, 1)

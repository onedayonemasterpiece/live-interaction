"""Real provider parser/guarded receive/host/relay, entirely offline."""
import asyncio
import base64
from collections import deque
import json
import struct
import unittest
from unittest.mock import patch

from live_interaction import provider as proposed
from live_interaction.socket_transport import encode_output
from live_interaction.session_host import LiveError
from socket_egress_diagnostics_test import EgressDiagnostics, settle
from provider_test import _FakeSocket



class Guard:
    def __init__(self):
        self.checks = 0
    def check(self):
        self.checks += 1


class BufferedWs:
    def __init__(self, messages):
        self.messages = deque(json.dumps(message) for message in messages)
    async def recv(self):
        # Matches an already buffered ws.recv, which need not suspend.
        return self.messages.popleft()


def message(raw, *, final=False):
    return {"serverContent": {"modelTurn": {"parts": [{"inlineData": {
        "mimeType": "audio/pcm;rate=24000", "data": base64.b64encode(raw).decode()}}]},
        **({"generationComplete": True, "turnComplete": True} if final else {})}}


class ProviderEgress(EgressDiagnostics):
    async def drive(self, messages, *, fixed):
        self.guard = Guard()
        ws = BufferedWs(messages)
        for _ in range(len(messages)):
            obj = json.loads(await proposed._guarded_recv(ws, self.guard))
            if fixed:
                await proposed.handle_server_message_async(obj, emit=self.provider.events,
                    stopped=lambda: self.session.closed, resource_guard=self.guard)
            else:
                proposed.handle_server_message(obj, emit=self.provider.events)

    async def finish_success(self, raw):
        await settle(lambda: any(isinstance(f, str) and json.loads(f).get("event", {}).get("type") == "turn_complete" for f in self.sent))
        frames = [f for f in self.sent if isinstance(f, bytes)]
        self.assertEqual(b"".join(f[12:] for f in frames), raw)
        seqs = [struct.unpack("!III", f[:12])[1] for f in frames]
        self.assertEqual(seqs, sorted(set(seqs)))
        self.assertTrue(all(len(f) <= 11012 and len(f) % 2 == 0 for f in frames))
        self.assertTrue(all(struct.unpack("!III", f[:12])[::2] == (0x574C4F31, 24000) for f in frames))
        await self.inbound.put('{"type":"stop"}')
        await asyncio.wait_for(self.worker, 2)
        self.assertEqual(self.closed[-1], (1000, "closed"))
        self.assertFalse(any(r["event"] == "socket_failed" for r in self.records))

    async def test_proposed_buffered_turn_preserves_pcm_and_boundary(self):
        await self.start()
        raw = b"\x01\x00" * 1024
        await self.drive([message(raw, final=i == 128) for i in range(129)], fixed=True)
        await self.finish_success(raw * 129)
        self.assertGreaterEqual(self.guard.checks, 258)

    async def test_proposed_large_pcm_part_preserves_all_bytes(self):
        await self.start()
        raw = b"\x01\x00" * 524288
        await self.drive([message(raw, final=True)], fixed=True)
        await self.finish_success(raw)

    async def test_proposed_valid_4MiB_pcm_part_within_existing_provider_envelope(self):
        await self.start()
        raw = b"\x01\x00" * 2097152
        self.assertLess(len(json.dumps(message(raw)).encode()), 8 * 1024 * 1024)
        await self.drive([message(raw, final=True)], fixed=True)
        await self.finish_success(raw)

    async def test_proposed_slow_receiver_still_fails_closed_at_existing_bound(self):
        gate = asyncio.Event()
        async def slow_send(frame):
            self.sent.append(frame)
            if isinstance(frame, bytes):
                await gate.wait()
        await self.start(send=slow_send)
        raw = b"\x01\x00" * 2048
        await self.drive([message(raw) for _ in range(200)], fixed=True)
        record = await self.failed()
        self.assertEqual(record["egress_reject_reason"], "queued_bytes")
        self.assertLessEqual(record["queue_bytes_at_reject"], 256 * 1024)
        self.assertEqual(record["egress_limit_bytes"], 256 * 1024)
        self.assertFalse(gate.is_set())

    async def test_proposed_actual_run_buffered_parser_to_relay(self):
        # Actual provider.run is wired, with the provider WebSocket replaced by
        # an in-memory socket. No model/key/resource operation is performed.
        await self.host.stop_all()
        socket = _FakeSocket([{"setupComplete": {}}])
        with patch("websockets.connect", return_value=socket):
            self.host.provider_run = proposed.run
            self.started = await self.host.start(resource_id="fixture-story", actor={"subject": "fixture"}, attempt_id="fixture-attempt")
            self.session = self.host.sessions[self.started["session_id"]]
            self.binding = self.host.open_socket(session_id=self.session.id, resource_id="fixture-story", ticket=self.started["socket_ticket"])
            await self.start()
            raw = b"\x02\x00" * 1024
            for i in range(200):
                socket.incoming.put_nowait(message(raw, final=i == 199))
            await self.finish_success(raw * 200)
            self.assertTrue(socket.closed)
            self.assertEqual(len(socket.sent), 1)
            self.assertIn("setup", socket.sent[0])

    async def test_proposed_stop_midpart_emits_no_stale_tail_or_completion(self):
        await self.start()
        seen = []
        stopped = False
        def emit(event):
            nonlocal stopped
            seen.append(event)
            self.provider.events(event)
            stopped = event["type"] == "audio"
        await proposed.handle_server_message_async(message(b"\x01\x00" * 524288, final=True),
            emit=emit, stopped=lambda: stopped)
        self.assertEqual([event["type"] for event in seen], ["audio"])
        self.assertEqual(len(base64.b64decode(seen[0]["data"])), 11000)
        await self.inbound.put('{"type":"stop"}')
        await asyncio.wait_for(self.worker, 0.5)
        self.assertEqual(self.closed[-1], (1000, "closed"))

    async def test_proposed_resource_fence_is_rechecked_between_chunks(self):
        class Expired(Guard):
            def check(self):
                super().check()
                if self.checks == 2:
                    raise RuntimeError("fixture_resource_expired")
        seen = []
        guard = Expired()
        with self.assertRaisesRegex(RuntimeError, "fixture_resource_expired"):
            await proposed.handle_server_message_async(message(b"\x01\x00" * 524288, final=True),
                emit=seen.append, resource_guard=guard)
        self.assertEqual(len(seen), 1)
        self.assertEqual(seen[0]["type"], "audio")

    async def test_proposed_cancellation_does_not_emit_completion(self):
        seen = []
        task = None
        def emit(event):
            seen.append(event)
            task.cancel()
        task = asyncio.create_task(proposed.handle_server_message_async(
            message(b"\x01\x00" * 524288, final=True), emit=emit))
        with self.assertRaises(asyncio.CancelledError):
            await task
        self.assertEqual([event["type"] for event in seen], ["audio"])

    async def test_proposed_invalid_pcm_fails_closed(self):
        for data in ("?", base64.b64encode(b"\x01").decode()):
            bad = {"serverContent": {"modelTurn": {"parts": [{"inlineData": {
                "mimeType": "audio/pcm;rate=24000", "data": data}}]}, "turnComplete": True}}
            seen = []
            await proposed.handle_server_message_async(bad, emit=seen.append)
            self.assertEqual(seen[0]["data"], data)
            with self.assertRaises(LiveError):
                encode_output({**seen[0], "seq": 42})

    async def test_proposed_lossless_transcript_tool_usage_and_boundaries(self):
        source = message(b"\x01\x00" * 9000, final=True)
        source.update({"interactionStatus": "IDLE", "toolCallCancellation": {"ids": ["fixture"]},
                       "toolCall": {"functionCalls": [{"name": "fixture_read", "id": "read1"}]},
                       "usageMetadata": {"totalTokenCount": 37}})
        source["serverContent"].update({"inputTranscription": {"text": "u" * 5000},
                                        "outputTranscription": {"text": "m" * 5000}})
        old, new = [], []
        old = [
            {"type": "interaction_status", "status": "IDLE"},
            {"type": "tool_cancelled", "ids": ["fixture"]},
            {"type": "tool_call", "calls": source["toolCall"]["functionCalls"]},
            {"type": "usage", "metadata": {"totalTokenCount": 37}},
            {"type": "input_transcript", "text": "u" * 5000},
            {"type": "output_transcript", "text": "m" * 5000},
            {"type": "generation_complete"}, {"type": "turn_complete"},
        ]
        await proposed.handle_server_message_async(source, emit=new.append)
        self.assertEqual([e for e in old if e["type"] != "audio"], [e for e in new if e["type"] != "audio"])
        old_history, new_history = proposed.DialogueHistory(), proposed.DialogueHistory()
        for history, events in ((old_history, old), (new_history, new)):
            for event in events:
                if event["type"] in ("input_transcript", "output_transcript"):
                    history.transcript("user" if event["type"] == "input_transcript" else "model", event["text"])
                elif event["type"] in ("turn_complete", "interrupted", "tool_call"):
                    history.complete()
        self.assertEqual(old_history.snapshot(), new_history.snapshot())


if __name__ == "__main__":
    unittest.main()

"""Offline tests of exact rc2 plus proposed diagnostics; no provider or socket I/O."""
import asyncio
import base64
import json
import struct
import unittest

from session_host_test import Adapter, Provider

from live_interaction import socket_transport as transport
from live_interaction import socket_host as host_module

async def settle(predicate):
    async with asyncio.timeout(2):
        while not predicate():
            await asyncio.sleep(0)


class EgressDiagnostics(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.provider = Provider()
        self.records, self.sent, self.closed = [], [], []
        self.host = host_module.LiveSocketSessionHost(
            adapter_factory=lambda **_: Adapter(), key_resolver=lambda *_: "fixture",
            provider_run=self.provider.run, ready_timeout_ms=500, diagnostic=self.records.append)
        self.started = await self.host.start(resource_id="fixture-story", actor={"subject": "fixture"}, attempt_id="fixture-attempt")
        self.session = self.host.sessions[self.started["session_id"]]
        self.binding = self.host.open_socket(session_id=self.session.id, resource_id="fixture-story", ticket=self.started["socket_ticket"])
        self.inbound = asyncio.Queue()
        self.worker = None

    async def asyncTearDown(self):
        if self.worker and not self.worker.done():
            self.worker.cancel()
            await asyncio.gather(self.worker, return_exceptions=True)
        await self.host.stop_all()

    async def start(self, max_output_bytes=transport.MAX_OUTPUT_BYTES, send=None):
        async def capture(frame):
            self.sent.append(frame)
        async def close(code, reason):
            self.closed.append((code, reason))
        self.worker = asyncio.create_task(transport.serve_socket(
            self.binding, receive=self.inbound.get, send=send or capture, close=close,
            max_output_bytes=max_output_bytes))
        await self.inbound.put(json.dumps({"type": "hello", "protocol": host_module.SOCKET_PROTOCOL,
            "attempt_id": "fixture-attempt", "connection_generation": 1, "cursor": 0}))
        await settle(lambda: self.sent)
        await settle(lambda: self.binding.state.subscribers)
        # Drain initial backlog before constructing each deterministic control.
        await asyncio.sleep(0.01)

    def audio(self, data):
        self.provider.events({"type": "audio", "mime_type": "audio/pcm;rate=24000",
                              "data": base64.b64encode(data).decode()})

    async def failed(self):
        await asyncio.wait_for(self.worker, 2)
        self.assertEqual(self.closed[-1], (1002, "LIVE_SOCKET_EGRESS_BACKPRESSURE"))
        self.assertEqual(self.host.size(), 0)
        return next(r for r in self.records if r["event"] == "socket_failed")

    async def test_single_pcm_rejection_records_exact_encoded_size_without_audio(self):
        await self.start()
        self.audio(b"\x01\x00" * (transport.MAX_OUTPUT_BYTES // 2))
        record = await self.failed()
        self.assertEqual(record["egress_reject_reason"], "single_payload")
        self.assertEqual(record["output_event_type"], "audio")
        self.assertEqual(record["output_payload_bytes"], transport.MAX_OUTPUT_BYTES + 12)
        self.assertEqual(record["queue_bytes_at_reject"], 0)
        self.assertEqual(record["queue_count_at_reject"], 0)
        self.assertNotIn("data", record)
        self.assertNotIn("mime_type", record)

    async def test_synchronous_burst_keeps_rejection_watermark_after_drain(self):
        await self.start()
        # All callbacks run before sender gets its next event-loop turn.
        for _ in range(129):
            self.audio(b"\x01\x00" * 1024)
        record = await self.failed()
        self.assertEqual(record["egress_reject_reason"], "queued_bytes")
        self.assertEqual(record["output_payload_bytes"], 2060)
        self.assertEqual(record["queue_count_at_reject"], 127)
        self.assertEqual(record["queue_bytes_at_reject"], 127 * 2060)
        self.assertLess(record["queue_bytes"], record["queue_bytes_at_reject"])
        self.assertEqual(record["egress_limit_bytes"], 256 * 1024)
        self.assertEqual(record["egress_limit_events"], 320)

    async def test_event_count_fuse_stays_authoritative(self):
        await self.start()
        for _ in range(321):
            self.provider.events({"type": "generation_complete"})
        record = await self.failed()
        self.assertEqual(record["egress_reject_reason"], "event_count")
        self.assertEqual(record["queue_count_at_reject"], 320)
        self.assertLess(record["queue_bytes_at_reject"] + record["output_payload_bytes"], transport.MAX_OUTPUT_BYTES)

    async def test_healthy_pcm_is_lossless_ordered_once_with_stop_unchanged(self):
        await self.start()
        expected = [b"\x00\x80\xff\x7f", b"\x01\x00" * 512, b"\x02\x00" * 1536]
        seqs = []
        for raw in expected:
            seqs.append(self.session.next_seq)
            self.audio(raw)
        boundary = self.session.next_seq
        self.provider.events({"type": "generation_complete"})
        await settle(lambda: any(isinstance(f, str) and json.loads(f).get("event", {}).get("seq") == boundary for f in self.sent))
        frames = [f for f in self.sent if isinstance(f, bytes)]
        self.assertEqual([struct.unpack("!III", f[:12]) for f in frames],
                         [(transport.OUTPUT_MAGIC, seq, 24000) for seq in seqs])
        self.assertEqual([f[12:] for f in frames], expected)
        self.assertEqual(b"".join(f[12:] for f in frames), b"".join(expected))
        await self.inbound.put('{"type":"stop"}')
        await asyncio.wait_for(self.worker, 2)
        self.assertEqual(self.closed[-1], (1000, "closed"))
        self.assertEqual(self.host.size(), 0)
        self.assertFalse(any(r["event"] == "socket_failed" for r in self.records))

    async def test_stop_does_not_wait_for_blocked_output_send(self):
        gate, blocked = asyncio.Event(), asyncio.Event()
        async def send(frame):
            self.sent.append(frame)
            if isinstance(frame, bytes):
                blocked.set()
                await gate.wait()
        await self.start(send=send)
        self.audio(b"\x01\x00" * 1000)
        await asyncio.wait_for(blocked.wait(), 1)
        await self.inbound.put('{"type":"stop"}')
        await asyncio.wait_for(self.worker, 0.5)
        self.assertFalse(gate.is_set())
        self.assertEqual(self.closed[-1], (1000, "closed"))
        self.assertEqual(self.host.size(), 0)

    async def test_untrusted_event_type_is_bounded_and_payload_not_logged(self):
        await self.start(max_output_bytes=1024)
        self.provider.events({"type": "private transcript " + "x" * 1000, "text": "private-output" * 300})
        record = await self.failed()
        self.assertEqual(record["output_event_type"], "unknown")
        self.assertNotIn("private", json.dumps(record))
        self.assertNotIn("text", record)

    async def test_stale_duplicate_event_is_not_sent_twice(self):
        await self.start()
        self.audio(b"\x01\x00")
        await settle(lambda: any(isinstance(f, bytes) for f in self.sent))
        duplicate = self.session.events[-1]
        for listener in tuple(self.binding.state.subscribers):
            listener(duplicate)
        self.provider.events({"type": "generation_complete"})
        await asyncio.sleep(0.01)
        self.assertEqual(len([f for f in self.sent if isinstance(f, bytes)]), 1)
        await self.inbound.put('{"type":"stop"}')
        await asyncio.wait_for(self.worker, 2)


if __name__ == "__main__":
    unittest.main()

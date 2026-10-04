import asyncio
import base64
import json
import struct
import unittest

from live_interaction.session_host import LiveError
from live_interaction.socket_host import LiveSocketSessionHost, SOCKET_PROTOCOL
from live_interaction.socket_transport import decode_audio, encode_output, same_origin, serve_socket, socket_ticket
from session_host_test import Adapter, Provider


class ManualAdapter(Adapter):
    def initialize(self, **kwargs):
        result = super().initialize(**kwargs)
        result["configuration"]["manual_activity_detection"] = True
        return result


async def settle(predicate, timeout=1):
    async with asyncio.timeout(timeout):
        while not predicate():
            await asyncio.sleep(0.001)


class SocketHostContract(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.provider = Provider()
        self.adapter = ManualAdapter()
        self.records = []
        self.host = LiveSocketSessionHost(
            adapter_factory=lambda **_kw: self.adapter,
            key_resolver=lambda *_: "fixture-not-a-provider-key",
            provider_run=self.provider.run,
            ready_timeout_ms=500,
            diagnostic=self.records.append,
        )
        self.actor = {"subject": "author", "tenant_id": "tenant"}
        self.started = await self.host.start(resource_id="story1", actor=self.actor, attempt_id="attempt_test")
        self.sid = self.started["session_id"]
        self.session = self.host.sessions[self.sid]

    async def asyncTearDown(self):
        await self.host.stop_all()

    def open(self, ticket=None):
        return self.host.open_socket(session_id=self.sid, resource_id="story1", ticket=ticket or self.started["socket_ticket"])

    def renew(self):
        return self.host.issue_socket_ticket(session_id=self.sid, resource_id="story1", actor=self.actor)["socket_ticket"]

    async def test_ticket_is_single_use_resource_actor_bound_and_not_stored_plain(self):
        self.assertEqual(self.started["transport_protocol"], SOCKET_PROTOCOL)
        state = self.host._socket_states[self.sid]
        self.assertNotEqual(state.ticket_digest, self.started["socket_ticket"].encode())
        with self.assertRaises(LiveError):
            self.host.issue_socket_ticket(session_id=self.sid, resource_id="story1", actor={"subject": "other", "tenant_id": "tenant"})
        with self.assertRaises(LiveError):
            self.host.open_socket(session_id=self.sid, resource_id="other-story", ticket=self.started["socket_ticket"])
        binding = self.open()
        with self.assertRaises(LiveError):
            self.open()
        await binding.close()
        with self.assertRaises(LiveError) as error:
            self.open()
        self.assertEqual(error.exception.code, "LIVE_SOCKET_TICKET")
        self.open(self.renew())

    async def test_expired_ticket_and_nonincreasing_generation_fail_closed(self):
        self.host._socket_states[self.sid].ticket_expires = 0
        with self.assertRaises(LiveError):
            self.open()
        binding = self.open(self.renew())
        binding.connect(2)
        await binding.close()
        next_binding = self.open(self.renew())
        with self.assertRaises(LiveError):
            next_binding.connect(2)
        with self.assertRaises(LiveError):
            next_binding.connect(True)
        next_binding.connect(3)
        await next_binding.close()

    async def test_no_http_audio_or_text_fallback_after_wss_disconnect(self):
        binding = self.open()
        binding.connect(1)
        for disconnected in (False, True):
            if disconnected:
                await binding.close()
            with self.assertRaises(LiveError) as error:
                await self.host.input(session_id=self.sid, resource_id="story1", actor=self.actor, message={"text": "do not replay"})
            self.assertEqual(error.exception.code, "LIVE_TRANSPORT_MISMATCH")

    async def test_audio_age_sequence_and_provider_manual_boundary_are_enforced(self):
        binding = self.open()
        binding.connect(1)
        with self.assertRaises(LiveError):
            await binding.input({"audio_base64": "AAAA"}, frame_seq=1, capture_age_ms=0, pcm_bytes=2)
        await binding.input({"activity_start": True})
        await binding.input({"audio_base64": "AAAA"}, frame_seq=1, capture_age_ms=0, pcm_bytes=2)
        for seq, age in ((1, 0), (3, 0), (2, 2501)):
            with self.assertRaises(LiveError):
                await binding.input({"audio_base64": "AAAA"}, frame_seq=seq, capture_age_ms=age, pcm_bytes=2)
        await binding.input({"audio_base64": "AAAA"}, frame_seq=2, capture_age_ms=2500, pcm_bytes=2)
        await binding.input({"activity_end": True})
        await asyncio.sleep(0)
        kinds = [json.loads(line)["type"] for line in self.provider.inputs]
        self.assertEqual(kinds[-4:], ["activity_start", "audio", "audio", "activity_end"])

    async def test_startup_catchup_has_separate_bounded_age_window(self):
        binding = self.open()
        binding.connect(1)
        await binding.input({"activity_start": True})
        await binding.input(
            {"audio_base64": "AAAA"},
            frame_seq=1,
            capture_age_ms=10_000,
            pcm_bytes=2,
            startup_catchup=True,
        )
        await binding.input(
            {"audio_base64": "AAAA"},
            frame_seq=2,
            capture_age_ms=20_000,
            pcm_bytes=2,
            startup_catchup=True,
        )
        await binding.input(
            {"audio_base64": "AAAA"},
            frame_seq=3,
            capture_age_ms=10,
            pcm_bytes=2,
        )
        with self.assertRaises(LiveError) as after_steady:
            await binding.input(
                {"audio_base64": "AAAA"},
                frame_seq=4,
                capture_age_ms=10,
                pcm_bytes=2,
                startup_catchup=True,
            )
        self.assertEqual(after_steady.exception.code, "LIVE_SOCKET_STALE_AUDIO")
        with self.assertRaises(LiveError) as too_old:
            await binding.input(
                {"audio_base64": "AAAA"},
                frame_seq=4,
                capture_age_ms=20_001,
                pcm_bytes=2,
                startup_catchup=True,
            )
        self.assertEqual(too_old.exception.code, "LIVE_SOCKET_STALE_AUDIO")
        await binding.input({"activity_end": True})

    async def test_disconnect_damages_turn_until_clean_end_reaches_provider(self):
        binding = self.open()
        binding.connect(1)
        await binding.input({"activity_start": True})
        await binding.input({"audio_base64": "AAAA"}, frame_seq=1, capture_age_ms=0, pcm_bytes=2)
        await binding.close()
        state = self.host._socket_states[self.sid]
        self.assertTrue(state.damaged)
        self.assertFalse(self.session.activity_open)
        self.provider.events({"type": "input_timing", "activity_end_sent_at": 1})
        self.assertTrue(state.damaged)
        self.provider.events({"type": "tool_call", "calls": [{"name": "story.read", "id": "damaged", "args": {}}]})
        await settle(lambda: any(e.get("code") == "LIVE_INPUT_DAMAGED" for e in self.session.events))
        self.assertEqual(self.adapter.called, [])
        recovered = self.open(self.renew())
        recovered.connect(2)
        await recovered.input({"activity_start": True})
        await recovered.input({"audio_base64": "AAAA"}, frame_seq=1, capture_age_ms=0, pcm_bytes=2)
        await recovered.input({"activity_end": True})
        self.assertTrue(state.damaged, "Enqueue is not provider delivery")
        self.provider.events({"type": "input_timing", "activity_end_sent_at": 2})
        self.assertFalse(state.damaged)
        self.provider.events({"type": "tool_call", "calls": [{"name": "story.read", "id": "clean", "args": {}}]})
        await settle(lambda: self.adapter.called == ["clean"])

    async def test_push_and_idempotent_stop_do_not_require_http_polling(self):
        binding = self.open()
        binding.connect(1)
        seen = []
        unsubscribe = binding.subscribe(seen.append)
        self.provider.events({"type": "output_transcript", "text": "response"})
        self.assertEqual(seen[-1]["text"], "response")
        unsubscribe()
        before = len(seen)
        self.provider.events({"type": "turn_complete"})
        self.assertEqual(len(seen), before)
        await binding.stop()
        stopped = await self.host.stop(session_id=self.sid, resource_id="story1", actor=self.actor)
        self.assertTrue(stopped["already_closed"])
        self.assertEqual(self.host.size(), 0)
        encoded = json.dumps(self.records)
        self.assertNotIn(self.started["socket_ticket"], encoded)
        self.assertNotIn("response", encoded)
        self.assertNotIn("fixture-not-a-provider-key", encoded)

    async def test_neutral_relay_hello_binary_push_and_ack(self):
        binding = self.open()
        inbound = asyncio.Queue()
        sent, closed = [], []
        async def send(frame):
            sent.append(frame)
        async def close(code, reason):
            closed.append((code, reason))
        task = asyncio.create_task(serve_socket(binding, receive=inbound.get, send=send, close=close))
        await inbound.put(json.dumps({"type": "hello", "protocol": SOCKET_PROTOCOL,
            "attempt_id": "attempt_test", "connection_generation": 1, "cursor": 0}))
        await settle(lambda: len(sent) > 0)
        self.assertEqual(json.loads(sent[0])["type"], "hello_ack")
        await inbound.put('{"type":"input","message":{"activity_start":true}}')
        await inbound.put(struct.pack("!III", 0x574C4131, 1, 10) + b"\x01\x00\xff\x7f")
        await inbound.put('{"type":"input","message":{"activity_end":true}}')
        await inbound.put('{"type":"ping"}')
        await settle(lambda: any(isinstance(x, str) and json.loads(x).get("type") == "audio_ack" for x in sent))
        self.provider.events({"type": "audio", "data": base64.b64encode(b"\x01\x00").decode(), "mime_type": "audio/pcm;rate=24000"})
        await settle(lambda: any(isinstance(x, bytes) for x in sent))
        output = next(x for x in sent if isinstance(x, bytes))
        self.assertEqual(struct.unpack("!III", output[:12])[::2], (0x574C4F31, 24000))
        self.assertEqual(output[12:], b"\x01\x00")
        await inbound.put('{"type":"stop"}')
        await asyncio.wait_for(task, 2)
        self.assertEqual(closed[-1][0], 1000)
        self.assertEqual(self.host.size(), 0)

    async def test_relay_output_overflow_fails_instead_of_dropping_audio(self):
        binding = self.open()
        inbound = asyncio.Queue()
        sent, closed = [], []
        async def send(frame):
            sent.append(frame)
        async def close(code, reason):
            closed.append((code, reason))
        task = asyncio.create_task(serve_socket(binding, receive=inbound.get, send=send, close=close, max_output_bytes=1024))
        await inbound.put(json.dumps({"type": "hello", "protocol": SOCKET_PROTOCOL,
            "attempt_id": "attempt_test", "connection_generation": 1, "cursor": 0}))
        await settle(lambda: sent)
        self.provider.events({"type": "audio", "data": base64.b64encode(b"\0" * 2048).decode(), "mime_type": "audio/pcm;rate=24000"})
        await asyncio.wait_for(task, 2)
        self.assertEqual(closed[-1][1], "LIVE_SOCKET_EGRESS_BACKPRESSURE")
        self.assertEqual(self.host.size(), 0)

    async def test_wrong_hello_and_json_audio_are_rejected_without_provider_input(self):
        for bad in ({"type": "hello", "protocol": "other"}, {"type": "hello", "protocol": SOCKET_PROTOCOL,
                    "attempt_id": "wrong", "connection_generation": 1, "cursor": 0}):
            # A failed hello intentionally terminates its provider session.
            fresh = await self.host.start(resource_id="bad", actor=self.actor, attempt_id="right")
            binding = self.host.open_socket(session_id=fresh["session_id"], resource_id="bad", ticket=fresh["socket_ticket"])
            closed = []
            async def receive():
                return json.dumps(bad)
            async def send(_):
                pass
            async def close(code, reason):
                closed.append((code, reason))
            await serve_socket(binding, receive=receive, send=send, close=close)
            self.assertEqual(closed[0][0], 1002)


class SocketWireContract(unittest.TestCase):
    def test_wire_is_big_endian_header_little_endian_pcm(self):
        data = bytes.fromhex("574c41310000002a0000007d0100feffff7f0080")
        seq, age, startup_catchup, pcm = decode_audio(data)
        self.assertEqual((seq, age, startup_catchup), (42, 125, False))
        self.assertEqual(struct.unpack("<hhhh", pcm), (1, -2, 32767, -32768))
        encoded = encode_output({"type": "audio", "seq": 42, "mime_type": "audio/pcm;rate=24000", "data": base64.b64encode(pcm).decode()})
        self.assertEqual(encoded.hex(), "574c4f310000002a00005dc00100feffff7f0080")
        catchup = struct.pack("!III", 0x574C4131, 43, 0x80000000 | 9000) + pcm
        cseq, cage, cflag, cpcm = decode_audio(catchup)
        self.assertEqual((cseq, cage, cflag), (43, 9000, True))
        self.assertEqual(cpcm, pcm)

    def test_bad_frames_are_bounded(self):
        for value in (b"", b"0" * 13, b"0" * 11014, b"0" * 14):
            with self.assertRaises(LiveError):
                decode_audio(value)

    def test_ticket_protocol_and_origin_checks(self):
        ticket = "a" * 32
        self.assertEqual(socket_ticket([SOCKET_PROTOCOL, "wl-ticket." + ticket]), ticket)
        for values in ([], [SOCKET_PROTOCOL], [SOCKET_PROTOCOL, "wl-ticket.bad"], [SOCKET_PROTOCOL, "wl-ticket." + ticket, "wl-ticket." + ticket]):
            with self.assertRaises(LiveError):
                socket_ticket(values)
        self.assertTrue(same_origin("https://example.test", "example.test"))
        for origin in (None, "https://evil.test", "https://example.test.evil", "https://user@example.test", "https://example.test?token=x"):
            self.assertFalse(same_origin(origin, "example.test"))

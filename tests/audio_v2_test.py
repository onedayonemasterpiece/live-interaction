import asyncio
import base64
from collections import OrderedDict
import json
import struct
import threading
import unittest
from contextlib import aclosing
from dataclasses import replace
from fractions import Fraction
from pathlib import Path
from unittest.mock import patch

from live_interaction.audio_v2 import *
from live_interaction.audio_codec import PyAVAudioCodecFactory, PyAVDecoder, adts_packets, opus_samples
from live_interaction import provider as provider_module
from live_interaction.audio_socket import AudioWorkPool, SocketAudioV2
from live_interaction.socket_host import LiveSocketSessionHost
from live_interaction.socket_transport import serve_socket, socket_protocol, socket_ticket
from live_interaction.session_host import LiveError
from session_host_test import Provider, Adapter
from socket_host_test import settle

VECTORS = json.loads((Path(__file__).parent / "fixtures/wl-live-v2-vectors.json").read_text())
FACTORY = PyAVAudioCodecFactory()


def vector_chunks(source_id="local_vector"):
    return [EncodedChunk(source_id, c["epoch_id"], c["chunk_seq"], c["capture_start_us"], c["duration_us"],
                         c["flags"], base64.b64decode(c["payload_base64"])) for c in VECTORS["chunks"]]


class AudioWireConformance(unittest.TestCase):
    def test_shared_cross_language_vectors_and_transport_independent_identity(self):
        self.assertEqual(HEADER.size, 56)
        for case in VECTORS["valid"]:
            with self.subTest(case=case["name"]):
                frame = AudioFrame.decode(bytes.fromhex(case["hex"]))
                self.assertEqual(frame.encode().hex(), case["hex"])
                for key, value in case["fields"].items():
                    self.assertEqual(getattr(frame, key), value.encode() if key == "magic" else value)
        one, retry = (AudioFrame.decode(bytes.fromhex(v["hex"])) for v in VECTORS["valid"][:2])
        original = EncodedChunk("s1", "e1", one.chunk_seq, one.start_us, one.duration_us, one.flags & SOURCE_FLAGS, one.payload)
        resent = EncodedChunk("s1", "e1", retry.chunk_seq, retry.start_us, retry.duration_us, retry.flags & SOURCE_FLAGS, retry.payload)
        self.assertEqual(original.identity, resent.identity)
        for case in VECTORS["invalid"]:
            with self.subTest(case=case["name"]), self.assertRaises(LiveError):
                AudioFrame.decode(bytes.fromhex(case["hex"]))

    def test_codec_epoch_and_expansion_hard_bounds(self):
        epoch = CodecEpoch.from_dict(VECTORS["epoch"])
        for kwargs in ({"clock_rate": 16000}, {"init": b"x" * 8193}, {"channels": 2}, {"codec": "pcm"}):
            with self.assertRaises(LiveError):
                replace(epoch, **kwargs)
        frame = AudioFrame.decode(bytes.fromhex(VECTORS["valid"][0]["hex"]))
        for kwargs in ({"payload": b"x" * 8193}, {"duration_us": 240001}, {"chunk_seq": 1 << 53}, {"flags": 32}):
            with self.assertRaises(LiveError):
                replace(frame, **kwargs).encode()
        with self.assertRaises(LiveError):
            opus_samples(bytes([0xFF, 63]))
        self.assertEqual(socket_protocol(["wl-live-v2", "wl-live-v1"], allow_v2=True), "wl-live-v2")
        self.assertEqual(socket_protocol(["wl-live-v1"]), "wl-live-v1")
        with self.assertRaises(LiveError):
            socket_protocol(["wl-live-v2"])
        self.assertEqual(socket_ticket(["wl-live-v2", "wl-ticket." + "a" * 32]), "a" * 32)



class BoundedVoiceRaceContracts(unittest.IsolatedAsyncioTestCase):
    """Transport ordering and ingress without any optional local audio codecs."""

    async def test_new_live_session_does_not_autoplay_a_prior_sessions_durable_response(self):
        from types import SimpleNamespace
        diagnostic = []
        socket = SocketAudioV2.__new__(SocketAudioV2)
        socket.closed = False
        socket.binding = SimpleNamespace(stopped=False,
            session=SimpleNamespace(id="live_today", stop_reason=None),
            state=SimpleNamespace(generation=3),
            heartbeat=lambda: None,
            host=SimpleNamespace(diagnostic=lambda _session, event, **data: diagnostic.append((event, data))))
        socket.archive = SimpleNamespace(failed_recoveries=OrderedDict())
        socket.output_recovering = set()
        socket.output_recovery = asyncio.Queue(maxsize=8)
        socket.output_changed = asyncio.Event()
        socket._request_output("out_live_yesterday_1", 123)
        self.assertTrue(socket.output_recovery.empty())
        self.assertEqual(socket.output_recovering, set())
        self.assertEqual(diagnostic[0][0], "socket_historical_output_ignored")
        # Reconnecting WSS generation within today's SAME Live session still
        # recovers the actual original, at the client's true played cursor.
        socket._request_output("out_live_today_2", 314)
        self.assertEqual(socket.output_recovery.get_nowait(), ("out_live_today_2", 314))
        self.assertEqual(socket.output_recovering, {"out_live_today_2"})
        # A direct replay call on historical input does not read, mark played,
        # cancel, or change any durable source bytes.
        class ForbiddenHistoricalStore:
            async def resume_source(self, _source_id):
                raise AssertionError("historical source accessed during new Live")
        socket.output_store = ForbiddenHistoricalStore()
        await socket._replay_output("out_live_yesterday_1", 400)

    async def test_recovery_gap_is_response_scoped_and_does_not_stop_the_live_conversation(self):
        from collections import OrderedDict
        from types import SimpleNamespace
        from live_interaction.audio_v2 import OutputResume
        r = SocketAudioV2.__new__(SocketAudioV2)
        current, healthy = "out_live_current_1", "out_live_current_2"
        r.closed = False
        diagnostics, sent, recovered = [], [], []
        r.binding = SimpleNamespace(stopped=False,
            session=SimpleNamespace(id="live_current", stop_reason=None),
            state=SimpleNamespace(generation=5), heartbeat=lambda: None,
            host=SimpleNamespace(diagnostic=lambda _session, event, **data: diagnostics.append((event, data))))
        r.archive = SimpleNamespace(failed_recoveries=OrderedDict())
        r.output_recovery = asyncio.Queue(maxsize=8)
        r.output_recovering = {current, healthy}
        r.output_acknowledged = {current: 3}
        r.failed = asyncio.get_running_loop().create_future()
        async def send(payload):
            sent.append(payload)
        r.send_json = send
        async def replay(source, _played):
            if source == current:
                raise invalid("OUTPUT_SEQUENCE", "An original recovery prefix has a hole")
            recovered.append(source)
        r._replay_output = replay
        def resume(source):
            return OutputResume(BoundSource(SourceDescriptor(source, 0), "server_" + source, 5),
                                0, False, 120000, True)
        class Store:
            async def pending_outputs(self, *, limit=8):
                return [resume(current), resume(healthy)]
        r.output_store = Store()
        r.output_recovery.put_nowait((current, 0))
        r.output_recovery.put_nowait((healthy, 0))
        worker = asyncio.create_task(r._output_recovery_worker())
        try:
            await asyncio.wait_for(r.output_recovery.join(), 2)
            self.assertFalse(r.failed.done(), "A damaged reply is not a fatal Live session failure")
            self.assertEqual(recovered, [healthy])
            self.assertNotIn(current, r.output_recovering)
            self.assertEqual(list(r.archive.failed_recoveries), [current])
            self.assertTrue(any(item.get("type") == "output_replayed" and item.get("source_id") == current
                                and item.get("complete") is False and item.get("through_seq") == 3
                                for item in sent))
            self.assertTrue(any(item.get("code") == "LIVE_AUDIO_OUTPUT_GAP" for item in sent))
            self.assertEqual(diagnostics[0][0], "socket_output_recovery_gap")
            await r._output_manifest()
            self.assertEqual(sent[-1]["type"], "output_available")
            self.assertEqual([item["source_id"] for item in sent[-1]["sources"]], [healthy])
            r._request_output(current, 0)
            self.assertTrue(r.output_recovery.empty(), "A damaged source must not cause retry storms")
        finally:
            worker.cancel()
            await asyncio.gather(worker, return_exceptions=True)

    async def test_same_source_recovery_serializes_a_paused_live_writer(self):
        from collections import OrderedDict
        from types import SimpleNamespace
        from live_interaction.audio_output import ArchivedOutput
        r = SocketAudioV2.__new__(SocketAudioV2)
        source = "out_session_1"
        r.output_source_locks = {}
        r.output_source_users = {}
        r.output_recovering = set()
        r.closed = False
        r.output_bindings = OrderedDict()
        r.output_next = {}
        r.output_acknowledged = {}
        r.output_pending = {}
        r.output_bytes = [0, 0]
        r.output_duration = [0, 0]
        r.output_epoch_ref = 0
        r.limits = AudioLimits()
        r.binding = SimpleNamespace(stopped=False, session=SimpleNamespace(id="session",stop_reason=None),
                                    state=SimpleNamespace(generation=2))
        r.archive = SimpleNamespace(is_cancelled=lambda _: False)
        entered, go = asyncio.Event(), asyncio.Event()
        async def admission(_source, chunk, recovery):
            if chunk.chunk_seq == 1 and not recovery:
                entered.set()
                await go.wait()
        r._admit_output = admission
        epoch = CodecEpoch.from_dict(VECTORS["epoch"])
        originals = [replace(c, source_id=source, flags=c.flags & EPOCH_END) for c in vector_chunks()]
        record = ArchivedOutput(BoundSource(SourceDescriptor(source, 0), "server_source", 2),
                                epoch, 1, tuple(OutputChunk(c, 1) for c in originals))
        observed = []
        async def deliver(*, recovery):
            async for payload in r._frames(record, recovery=recovery):
                if isinstance(payload, bytes):
                    observed.append(AudioFrame.decode(payload).chunk_seq)
                await asyncio.sleep(0)
        live = asyncio.create_task(deliver(recovery=False))
        try:
            await asyncio.wait_for(entered.wait(), 2)
            r.output_recovering.add(source)
            rec = asyncio.create_task(deliver(recovery=True))
            await asyncio.sleep(.01)
            go.set()
            await asyncio.wait_for(asyncio.gather(live, rec), 2)
            self.assertEqual(observed, [c.chunk_seq for c in originals])
            self.assertEqual(r.output_next[source], len(originals)+1)
        finally:
            go.set()
            if not live.done():
                live.cancel()


    @staticmethod
    def _output_for_cancellation():
        from collections import OrderedDict
        from types import SimpleNamespace
        from live_interaction.audio_output import ArchivedOutput
        r = SocketAudioV2.__new__(SocketAudioV2)
        source = "out_session_1"
        r.closed = False
        r.output_source_locks = {}
        r.output_source_users = {}
        r.output_recovering = set()
        r.output_bindings = OrderedDict()
        r.output_next, r.output_acknowledged = {}, {}
        r.output_pending = {}
        r.output_bytes, r.output_duration = [0,0],[0,0]
        r.output_changed = asyncio.Event()
        r.output_epoch_ref = 0
        r.limits = AudioLimits()
        r.binding = SimpleNamespace(stopped=False,
            session=SimpleNamespace(id="session", stop_reason=None),
            state=SimpleNamespace(generation=2), heartbeat=lambda:None)
        r.archive = SimpleNamespace(is_cancelled=lambda _: False)
        epoch=CodecEpoch.from_dict(VECTORS["epoch"])
        originals=[replace(c,source_id=source,flags=c.flags & EPOCH_END) for c in vector_chunks()]
        record=ArchivedOutput(BoundSource(SourceDescriptor(source,0),"server_source",2),
                              epoch,1,tuple(OutputChunk(c,1) for c in originals))
        return r,record,source

    async def test_aborted_socket_send_closes_source_generator_and_recovery_is_not_blocked(self):
        r,record,source=self._output_for_cancellation()
        send_error=OSError("closed socket")
        try:
            async with aclosing(r._frames(record,recovery=False)) as outputs:
                header=await anext(outputs)
                self.assertIsInstance(header,str)
                first=AudioFrame.decode(await anext(outputs))
                self.assertEqual(first.chunk_seq,1)
                self.assertTrue(r.output_source_locks[source].locked())
                self.assertEqual(r.output_next.get(source,1),1)
                raise send_error
        except OSError as exc:
            self.assertIs(exc,send_error)
        self.assertFalse(r.output_source_locks[source].locked())
        self.assertFalse(r.output_pending,"A failed send cannot leak a delivery reservation")
        self.assertEqual(r.output_next.get(source,1),1,
                         "Only a completed wire send may advance the delivered sequence")
        r.output_recovering.add(source)
        observed=[]
        async with aclosing(r._frames(record,recovery=True)) as recovery:
            async for packet in recovery:
                if isinstance(packet,bytes):
                    observed.append(AudioFrame.decode(packet).chunk_seq)
        self.assertEqual(observed,[1,2])
        self.assertEqual(r.output_next[source],3)
        self.assertFalse(r.output_source_users)

    async def test_stop_after_first_completed_frame_does_not_send_late_tail(self):
        r,record,source=self._output_for_cancellation()
        frames=[]
        async with aclosing(r._frames(record,recovery=False)) as sender:
            self.assertIsInstance(await anext(sender),str)
            frames.append(AudioFrame.decode(await anext(sender)).chunk_seq)
            # The first bytes reached the device and its real receipt released
            # this reservation. User Stop arrives before the second chunk.
            r._output_ack({"source_id":source,"chunk_seq":1,"stage":"device_durable"})
            r.binding.stopped=True
            async for packet in sender:
                if isinstance(packet,bytes):
                    frames.append(AudioFrame.decode(packet).chunk_seq)
        self.assertEqual(frames,[1])
        self.assertFalse(r.output_pending)
        self.assertFalse(r.output_source_users)
        self.assertFalse(r.output_source_locks[source].locked())


    async def test_old_device_ack_is_idempotent_after_newer_durable_receipt(self):
        r,record,source=self._output_for_cancellation()
        async with aclosing(r._frames(record,recovery=False)) as sender:
            frames=[v async for v in sender]
        self.assertEqual([AudioFrame.decode(v).chunk_seq for v in frames if isinstance(v,bytes)],[1,2])
        self.assertTrue(r.output_pending)
        r._output_ack({"source_id":source,"chunk_seq":2,"stage":"device_durable"})
        r._output_ack({"source_id":source,"chunk_seq":1,"stage":"device_durable"})
        r._output_ack({"source_id":source,"chunk_seq":2,"stage":"device_durable"})
        self.assertEqual(r.output_acknowledged[source],2)
        self.assertFalse(r.output_pending)
        self.assertEqual(sum(r.output_bytes)+sum(r.output_duration),0)
        with self.assertRaises(LiveError):
            r._output_ack({"source_id":source,"chunk_seq":3,"stage":"device_durable"})


    async def test_stop_wakes_waiting_input_without_false_ack_or_storage(self):
        from time import monotonic
        from types import SimpleNamespace
        from live_interaction.audio_socket import _Source
        r=SocketAudioV2.__new__(SocketAudioV2)
        r.limits=AudioLimits()
        r.queues=[asyncio.Queue(maxsize=32),asyncio.Queue(maxsize=32)]
        r.queued_bytes=[16,0]
        r.queued_duration=[r.limits.live_window_duration_us,0]
        r.admission_changed=[asyncio.Event(),asyncio.Event()]
        r.closed=False
        source=_Source(BoundSource(SourceDescriptor("live",0),"server_live",0),False,0,monotonic())
        source.submitted=4
        r.sources={1:source}
        r.epochs={1:(1,CodecEpoch.from_dict(VECTORS["epoch"]))}
        r.live_ref=1
        r.binding=SimpleNamespace(stopped=False,session=SimpleNamespace(stop_reason=None),
                state=SimpleNamespace(generation=1),heartbeat=lambda:None,
                host=SimpleNamespace(diagnostic=lambda *_a,**_kw:None))
        r._age=lambda _src,_frame:None
        blocked=AudioFrame(UPLINK_MAGIC,0,1,1,5,800000,200000,1,0,b"abcd")
        task=asyncio.create_task(r.audio(blocked.encode(r.limits)))
        try:
            await asyncio.sleep(.03)
            self.assertFalse(task.done())
            r.binding.stopped=True
            r.admission_changed[0].set()
            with self.assertRaises(LiveError) as error:
                await asyncio.wait_for(task,.2)
            self.assertEqual(error.exception.code,"LIVE_AUDIO_STOPPED")
            self.assertEqual(r.sources[1].submitted,4)
            self.assertEqual(r.queued_duration[0],r.limits.live_window_duration_us)
            self.assertTrue(r.queues[0].empty())
        finally:
            if not task.done():
                task.cancel()
            await asyncio.gather(task,return_exceptions=True)

    async def test_bounded_audio_waits_for_slow_store_without_dropping_originals(self):
        from time import monotonic
        from types import SimpleNamespace
        from live_interaction.audio_socket import _Source
        r = SocketAudioV2.__new__(SocketAudioV2)
        r.limits = AudioLimits()
        r.queues = [asyncio.Queue(maxsize=32), asyncio.Queue(maxsize=32)]
        r.queued_bytes = [0, 0]
        r.queued_duration = [0, 0]
        r.admission_changed = [asyncio.Event(), asyncio.Event()]
        r.closed = False
        r.sources = {1:_Source(BoundSource(SourceDescriptor("live",0),"server_live",0),False,0,monotonic())}
        r.epochs = {1:(1,CodecEpoch.from_dict(VECTORS["epoch"]))}
        r.live_ref = 1
        events = []
        r.binding = SimpleNamespace(stopped=False, session=SimpleNamespace(stop_reason=None),
              state=SimpleNamespace(generation=1), heartbeat=lambda:None,
              host=SimpleNamespace(diagnostic=lambda _s,_stage,**data:events.append(data)))
        r._age=lambda _src,_frame:None
        stalled, release=asyncio.Event(),asyncio.Event()
        originals=[]
        async def worker():
            for n in range(10):
                item,size,duration=await r.queues[0].get()
                if n==0:
                    stalled.set()
                    await release.wait()
                originals.append(item.chunk_seq)
                r.queued_bytes[0]-=size
                r.queued_duration[0]-=duration
                r.queues[0].task_done()
                r.admission_changed[0].set()
        worker_task=asyncio.create_task(worker())
        async def producer():
            for seq in range(1,11):
                await r.audio(AudioFrame(UPLINK_MAGIC,0,1,1,seq,(seq-1)*200000,
                                        200000,1,0,b"abcd").encode(r.limits))
        send_task=asyncio.create_task(producer())
        try:
            await asyncio.wait_for(stalled.wait(),2)
            await asyncio.sleep(.05)
            self.assertFalse(send_task.done())
            self.assertLessEqual(r.queued_duration[0],r.limits.live_window_duration_us)
            release.set()
            await asyncio.wait_for(asyncio.gather(send_task,worker_task),2)
            self.assertEqual(originals,list(range(1,11)))
            self.assertEqual(r.sources[1].submitted,10)
            self.assertEqual(r.queued_duration[0],0)
            self.assertTrue(events)
        finally:
            release.set()
            for task in (send_task,worker_task):
                if not task.done():task.cancel()
            await asyncio.gather(send_task,worker_task,return_exceptions=True)


@unittest.skipUnless(FACTORY.av, "Install the pinned [audio] extra for native codec tests")
class NativeAudioCodecs(unittest.TestCase):
    def test_opus_original_vector_state_preskip_and_terminal_trim(self):
        decoder = FACTORY.decoder(CodecEpoch.from_dict(VECTORS["epoch"]))
        output = [decoder.decode(chunk) for chunk in vector_chunks()]
        self.assertEqual(sum(len(x.pcm16) // 2 for x in output), 1600)
        self.assertEqual(output[0].capture_start_us, 0)
        self.assertEqual(output[1].capture_start_us, round(len(output[0].pcm16) * 1_000_000 / 32000))
        with self.assertRaises(LiveError):
            decoder.decode(vector_chunks()[-1])

    def test_partial_codec_tail_returns_exact_sample_count(self):
        for samples in (1, 104, 319, 320, 523, 1600, 16000):
            with self.subTest(samples=samples):
                encoder = FACTORY.encoder(OPUS_PROFILE, "epoch", sample_rate=16000)
                decoder = FACTORY.decoder(replace(encoder.epoch, decoded_sample_rate=16000))
                chunks = encoder.encode(b"\0\0" * samples, sample_rate=16000, end=True)
                decoded, start = 0, 0
                for seq, (payload, duration, end) in enumerate(chunks, 1):
                    result = decoder.decode(EncodedChunk("source", "epoch", seq, start, duration, EPOCH_END if end else 0, payload))
                    decoded += len(result.pcm16) // 2
                    start += duration
                self.assertEqual(decoded, samples)

    def test_malformed_ogg_and_forged_duration_fail_before_native_decode(self):
        chunks = vector_chunks()
        for chunk in (replace(chunks[0], duration_us=1), replace(chunks[0], payload=chunks[0].payload[:-1] + bytes([chunks[0].payload[-1] ^ 1]))):
            decoder = FACTORY.decoder(CodecEpoch.from_dict(VECTORS["epoch"]))
            with self.assertRaises(LiveError):
                decoder.decode(chunk)
        decoder = FACTORY.decoder(CodecEpoch.from_dict(VECTORS["epoch"]))
        with self.assertRaises(LiveError):
            decoder.decode(chunks[1])  # Missing packet dependency / page sequence.

    def test_aac_lc_adts_decodes_once_with_explicit_encoder_priming(self):
        av = FACTORY.av
        encoder = av.CodecContext.create("aac", "w")
        encoder.sample_rate, encoder.layout, encoder.format = 16000, "mono", "fltp"
        encoder.time_base, encoder.bit_rate, encoder.thread_count = Fraction(1, 16000), 32000, 1
        encoder.open()
        packets = []
        for n in range(4):
            frame = av.AudioFrame(format="fltp", layout="mono", samples=1024)
            frame.sample_rate, frame.time_base, frame.pts = 16000, Fraction(1, 16000), n * 1024
            frame.planes[0].update(bytes(frame.planes[0].buffer_size))
            packets.extend(encoder.encode(frame))
        packets.extend(encoder.encode(None))
        priming = -packets[0].pts  # Measured encoder output PTS, not guessed from ADTS.
        epoch = CodecEpoch("aac_epoch", "aac", "adts", 1, 1, 16000, 16000, priming, b"")
        decoder = FACTORY.decoder(epoch)
        def adts(packet):
            data = bytes(packet); length = len(data) + 7
            return bytes([255, 241, (1 << 6) | (8 << 2), (1 << 6) | (length >> 11), (length >> 3) & 255, ((length & 7) << 5) | 31, 252]) + data
        groups = (packets[:2], packets[2:4], packets[4:])
        total = start = coded = 0
        for seq, group in enumerate(groups, 1):
            payload = b"".join(adts(packet) for packet in group)
            coded += 1024 * len(group)
            valid = max(0, coded - priming)
            duration = (valid * 1_000_000 // 16000) - start
            result = decoder.decode(EncodedChunk("s", "aac_epoch", seq, start, duration, EPOCH_END if seq == 3 else 0, payload))
            total += len(result.pcm16) // 2
            start += duration
        self.assertEqual(total, 4096)
        with self.assertRaises(LiveError):
            adts_packets(adts(packets[0]) * 4, 16000)


class AudioWorkPoolLifecycle(unittest.IsolatedAsyncioTestCase):
    async def test_shutdown_timeout_rejects_new_work_without_freeing_a_running_native_job(self):
        pool = AudioWorkPool()
        entered, release = threading.Event(), threading.Event()

        def native_job():
            entered.set()
            release.wait(1)
            return threading.current_thread()

        job = asyncio.create_task(pool.run(native_job))
        try:
            await settle(entered.is_set)
            with self.assertRaises(LiveError) as timed_out:
                await pool.close(timeout=0.01)
            self.assertEqual(timed_out.exception.code, "LIVE_AUDIO_CODEC_SHUTDOWN_TIMEOUT")
            self.assertFalse(job.done())
            with self.assertRaises(LiveError) as closed:
                await pool.run(lambda: None)
            self.assertEqual(closed.exception.code, "LIVE_AUDIO_CODEC_CLOSED")
            release.set()
            thread = await job
            await pool.close()
            await settle(lambda: not thread.is_alive())
        finally:
            release.set()
            await asyncio.gather(job, return_exceptions=True)
            await pool.close()


class MemoryEncodedStore:
    """Test product adapter: receipt after commit; exact duplicates only."""
    def __init__(self):
        self.sources, self.epochs, self.chunks, self.seals = {}, {}, {}, {}
        self.outputs, self.played, self.cancelled = {}, {}, set()
        self.block = {}
        self.entered = asyncio.Event()
        self.output_gate = None
        self.output_entered = asyncio.Event()

    async def open_source(self, source):
        previous = self.sources.setdefault(source.source_id, source)
        if previous != source:
            raise invalid("SOURCE_CONFLICT", "Changed source")
        return BoundSource(source, "server_" + source.source_id, self.prefix(source.source_id))

    def prefix(self, source_id):
        seq = 0
        while (source_id, seq + 1) in self.chunks:
            seq += 1
        return seq

    async def register_epoch(self, source, epoch):
        key = (source.source.source_id, epoch.epoch_id)
        if self.epochs.setdefault(key, epoch) != epoch:
            raise invalid("EPOCH_CONFLICT", "Changed epoch")

    async def commit_chunk(self, source, epoch, chunk):
        if chunk.source_id in self.block:
            self.entered.set()
            await self.block[chunk.source_id].wait()
        key = (chunk.source_id, chunk.chunk_seq)
        new = key not in self.chunks
        if self.chunks.setdefault(key, chunk) != chunk:
            raise invalid("CHUNK_CONFLICT", "Changed chunk")
        return DurableCommit(chunk.source_id, source.server_source_id, chunk.epoch_id, chunk.chunk_seq,
                             chunk.payload_sha256, self.prefix(chunk.source_id), new)

    async def seal_source(self, source, last_chunk_seq, capture_end_us):
        self.seals[source.source.source_id] = (last_chunk_seq, capture_end_us)
        prefix = self.prefix(source.source.source_id)
        last = self.chunks.get((source.source.source_id, last_chunk_seq))
        return {"server_durable_seq": prefix, "complete": prefix == last_chunk_seq and
                (last_chunk_seq == 0 or bool(last and last.flags & EPOCH_END))}

    async def commit_output(self, source, epoch, output):
        if self.output_gate:
            self.output_entered.set()
            await self.output_gate.wait()
        key = (output.chunk.source_id, output.chunk.chunk_seq)
        if key in self.outputs and self.outputs[key] != output:
            raise invalid("OUTPUT_CONFLICT", "Changed original event identity")
        receipt = await self.commit_chunk(source, epoch, output.chunk)
        self.outputs[key] = output
        return receipt

    async def resume_source(self, source_id):
        return await self.open_source(self.sources[source_id])

    async def epoch(self, source, epoch_id):
        return self.epochs[(source.source.source_id, epoch_id)]

    async def read_output(self, source, *, after_seq, max_encoded_bytes, max_duration_us):
        result, size, duration = [], 0, 0
        for seq in range(after_seq + 1, self.prefix(source.source.source_id) + 1):
            output = self.outputs[(source.source.source_id, seq)]
            chunk = output.chunk
            if size + len(chunk.payload) > max_encoded_bytes or duration + chunk.duration_us > max_duration_us:
                break
            size += len(chunk.payload); duration += chunk.duration_us
            result.append(output)
        return result

    async def mark_played(self, source, played_us):
        status = await self.output_status(source)
        if played_us > status.duration_us + 1000:
            raise invalid("PLAYBACK", "Played position exceeds original")
        source_id = source.source.source_id
        self.played[source_id] = max(self.played.get(source_id, 0), played_us)

    async def output_status(self, source):
        source = await self.resume_source(source.source.source_id)
        source_id = source.source.source_id
        last = self.chunks.get((source_id, source.server_durable_seq))
        duration = last.capture_start_us + last.duration_us if last else 0
        complete = self.seals.get(source_id) == (source.server_durable_seq, duration) and bool(last and last.flags & EPOCH_END)
        return OutputResume(source, self.played.get(source_id, 0), source_id in self.cancelled, duration, complete)

    async def pending_outputs(self, *, limit=8):
        result = []
        for source_id in self.sources:
            if not source_id.startswith("out_"):
                continue
            status = await self.output_status(await self.resume_source(source_id))
            if not status.cancelled and (not status.complete or status.played_us < status.duration_us):
                result.append(status)
        return result[:limit]

    async def cancel_output(self, source):
        self.cancelled.add(source.source.source_id)


class AudioAdapter(Adapter):
    def __init__(self, store):
        super().__init__()
        self.store, self.projections, self.active_sources = store, [], []
        self.projection_gate = None
        self.projection_entered = asyncio.Event()

    def initialize(self, **kwargs):
        result = super().initialize(**kwargs)
        result["configuration"]["manual_activity_detection"] = True
        return result

    async def input(self, session, message):
        if "audio_base64" in message:
            raise AssertionError("Typed v2 must never append legacy PCM again")

    async def activate_audio_source(self, session, source):
        self.active_sources.append(source.server_source_id)

    async def input_audio(self, session, projection):
        self.assert_committed(projection)
        if self.projection_gate:
            self.projection_entered.set()
            await self.projection_gate.wait()
        self.projections.append(projection)

    def assert_committed(self, projection):
        chunk = self.store.chunks[(projection.commit.source_id, projection.commit.chunk_seq)]
        assert chunk.payload_sha256 == projection.commit.payload_sha256


@unittest.skipUnless(FACTORY.av, "Install the pinned [audio] extra for native socket tests")
class CompressedSocketContract(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.store, self.provider = MemoryEncodedStore(), Provider()
        self.adapter = AudioAdapter(self.store)
        self.host = LiveSocketSessionHost(adapter_factory=lambda **_: self.adapter, key_resolver=lambda *_: "fixture-not-provider",
                                         provider_run=self.provider.run, ready_timeout_ms=500,
                                         audio_v2=AudioV2Services(lambda _: self.store, FACTORY,
                                                                output_store_factory=lambda _: self.store), diagnostic=lambda _: None)
        self.started = await self.host.start(resource_id="r1", actor="owner", attempt_id="attempt_v2")
        self.session = self.host.sessions[self.started["session_id"]]
        self.binding = self.host.open_socket(session_id=self.session.id, resource_id="r1", ticket=self.started["socket_ticket"])
        self.incoming = asyncio.Queue()
        self.sent, self.closes = [], []
        async def send(value): self.sent.append(value)
        async def close(code, reason): self.closes.append((code, reason))
        self.task = asyncio.create_task(serve_socket(self.binding, receive=self.incoming.get, send=send, close=close, protocol=PROTOCOL_V2))
        await self.json({"type": "hello", "protocol": PROTOCOL_V2, "attempt_id": "attempt_v2", "connection_generation": 1,
                         "cursor": 0, "capabilities": {"uplink_codecs": [OPUS_PROFILE], "downlink_codecs": [OPUS_PROFILE]}})
        await self.message("hello_ack")

    async def asyncTearDown(self):
        if self.store.output_gate:
            self.store.output_gate.set()
        if not self.task.done():
            self.task.cancel()
        await asyncio.gather(self.task, return_exceptions=True)
        await self.host.close()

    async def json(self, value):
        await self.incoming.put(json.dumps(value))

    async def test_session_stop_reuses_pool_and_terminal_host_close_retires_it(self):
        pool = self.host._audio_work_pool
        thread = await pool.run(threading.current_thread)
        await self.host.stop_all()
        self.assertFalse(pool.closed)
        self.assertEqual(await pool.run(lambda: 7), 7)
        await self.host.start(resource_id="r1", actor="owner", attempt_id="attempt_after_stop")
        self.assertIs(self.host._audio_work_pool, pool)
        await self.host.close()
        await self.host.close()
        await settle(lambda: not thread.is_alive())
        self.assertTrue(pool.closed)
        with self.assertRaises(LiveError) as closed:
            await self.host.start(resource_id="r1", actor="owner", attempt_id="attempt_after_close")
        self.assertEqual(closed.exception.code, "LIVE_HOST_CLOSED")

    def messages(self, kind):
        return [item for value in self.sent if isinstance(value, str) and (item := json.loads(value)).get("type") == kind]

    async def message(self, kind, **matches):
        def matching(): return [m for m in self.messages(kind) if all(m.get(key) == value for key, value in matches.items())]
        await settle(lambda: bool(matching()) or self.task.done(), timeout=2)
        self.assertTrue(matching(), self.closes)
        return matching()[-1]

    async def source(self, name, recovery=False):
        await self.json({"type": "source_open", "source_id": name, "started_at_unix_ms": 1000, "source_time_now_us": 100_000,
                         "lane": "recovery" if recovery else "live"})
        bound = await self.message("source_bound", source_id=name)
        await self.json({"type": "codec_epoch", "source_ref": bound["source_ref"], "descriptor": VECTORS["epoch"]})
        epoch = await self.message("epoch_bound", source_id=name)
        return bound["source_ref"], epoch["epoch_ref"]

    async def chunk(self, refs, chunk, recovery=False):
        await self.incoming.put(AudioFrame(UPLINK_MAGIC, chunk.flags | (RECOVERY if recovery else 0), *refs, chunk.chunk_seq,
                                          chunk.capture_start_us, chunk.duration_us, 17 if recovery else self.binding.state.generation, 0, chunk.payload).encode())

    async def test_original_commit_before_one_decode_projection_and_compressed_output_tail(self):
        refs = await self.source("live")
        for chunk in vector_chunks("live"):
            await self.chunk(refs, chunk)
        await self.message("audio_ack", source_id="live", chunk_seq=2)
        await settle(lambda: any(b'"type":"activity_end"' in line for line in self.provider.inputs))
        self.assertEqual(sum(len(p.pcm16) // 2 for p in self.adapter.projections), 1600)
        self.assertEqual([self.store.chunks[("live", n)].payload for n in (1, 2)], [c.payload for c in vector_chunks("live")])
        self.assertEqual(self.adapter.active_sources, ["server_live"])
        await self.json({"type": "source_seal", "source_ref": refs[0], "last_chunk_seq": 2, "capture_end_us": 100000})
        await self.message("source_sealed", source_id="live")
        self.provider.events({"type": "audio", "data": base64.b64encode(b"\0\0" * 2400).decode(), "mime_type": "audio/pcm;rate=24000"})
        self.provider.events({"type": "turn_complete"})
        await settle(lambda: any(isinstance(value, bytes) and AudioFrame.decode(value).flags & EPOCH_END for value in self.sent))
        epoch = CodecEpoch.from_dict((await self.message("output_epoch"))["descriptor"])
        decoder = PyAVDecoder(FACTORY.av, epoch)
        samples = 0
        for value in self.sent:
            if isinstance(value, bytes):
                frame = AudioFrame.decode(value)
                self.assertEqual(frame.magic, DOWNLINK_MAGIC)
                source_id = (await self.message("output_epoch"))["source_id"]
                original = self.store.outputs[(source_id, frame.chunk_seq)]
                self.assertEqual(original.chunk.payload, frame.payload)
                self.assertEqual(original.original_event_seq, frame.event_seq)
                result = decoder.decode(EncodedChunk("out", epoch.epoch_id, frame.chunk_seq, frame.start_us,
                                                      frame.duration_us, frame.flags & EPOCH_END, frame.payload))
                samples += len(result.pcm16) // 2
        self.assertEqual(samples, 4800)

    async def test_stalled_durable_commit_preserves_source_order_and_control_ping(self):
        refs = await self.source("live")
        self.store.block["live"] = asyncio.Event()
        chunks = vector_chunks("live")
        try:
            await self.chunk(refs, chunks[0])
            await asyncio.wait_for(self.store.entered.wait(), 1)
            await self.chunk(refs, chunks[1])
            # This control MUST stay behind both accepted original chunks.
            await self.json({"type": "source_seal", "source_ref": refs[0],
                             "last_chunk_seq": 2, "capture_end_us": 100000})
            await self.json({"type": "ping"})
            async with asyncio.timeout(.25):
                await self.message("pong")
            self.assertEqual(self.store.prefix("live"), 0)
            self.assertNotIn("live", self.store.seals)
            self.store.block["live"].set()
            await self.message("audio_ack", source_id="live", chunk_seq=2)
            sealed = await self.message("source_sealed", source_id="live")
            self.assertTrue(sealed["complete"])
            self.assertEqual([self.store.chunks[("live", n)].payload for n in (1, 2)],
                             [c.payload for c in chunks])
        finally:
            self.store.block["live"].set()

    async def reconnect(self, *, playback=None, new_host=False):
        if not self.task.done():
            await self.incoming.put(None)
            await asyncio.wait_for(self.task, 2)
        if new_host:
            await self.host.close()
            self.provider = Provider()
            self.host = LiveSocketSessionHost(adapter_factory=lambda **_: self.adapter, key_resolver=lambda *_: "fixture-not-provider",
                         provider_run=self.provider.run, ready_timeout_ms=500,
                         audio_v2=AudioV2Services(lambda _: self.store, FACTORY, output_store_factory=lambda _: self.store), diagnostic=lambda _: None)
            self.started = await self.host.start(resource_id="r1", actor="owner", attempt_id="attempt_new")
            self.session = self.host.sessions[self.started["session_id"]]
            ticket = self.started["socket_ticket"]
            generation = 1
        else:
            renewed = self.host.issue_socket_ticket(session_id=self.session.id, resource_id="r1", actor="owner")
            ticket = renewed["socket_ticket"]
            generation = self.binding.state.generation + 1
        self.binding = self.host.open_socket(session_id=self.session.id, resource_id="r1", ticket=ticket)
        self.incoming = asyncio.Queue()
        self.sent, self.closes = [], []
        async def send(value): self.sent.append(value)
        async def close(code, reason): self.closes.append((code, reason))
        self.task = asyncio.create_task(serve_socket(self.binding, receive=self.incoming.get, send=send, close=close, protocol=PROTOCOL_V2))
        hello = {"type": "hello", "protocol": PROTOCOL_V2, "attempt_id": self.binding.attempt_id,
                 "connection_generation": generation, "cursor": 0,
                 "capabilities": {"uplink_codecs": [OPUS_PROFILE], "downlink_codecs": [OPUS_PROFILE]}}
        if playback:
            hello["playback_resume"] = playback
        await self.json(hello)
        await self.message("hello_ack")

    async def test_detached_output_is_archived_but_new_live_does_not_autoplay_originals(self):
        await self.incoming.put(None)
        await asyncio.wait_for(self.task, 2)
        source_id = f"out_{self.session.id}_1"
        self.provider.events({"type": "audio", "data": base64.b64encode(bytes(4800)).decode(), "mime_type": "audio/pcm;rate=24000"})
        self.provider.events({"type": "turn_complete"})
        await settle(lambda: source_id in self.store.seals)
        originals = [self.store.outputs[(source_id, seq)] for seq in range(1, self.store.prefix(source_id) + 1)]
        await self.reconnect(playback={"source_id": source_id, "played_ms": 50}, new_host=True)
        notice = await self.message("output_available")
        self.assertEqual(notice["sources"], [], "New Start cannot advertise previous Live session")
        self.assertFalse(self.messages("output_replayed"))
        self.assertFalse(any(isinstance(v, bytes) for v in self.sent))
        self.assertEqual(self.store.played.get(source_id, 0), 0, "Ignored autoplay never fabricates playhead")
        archived = await self.store.resume_source(source_id)
        explicit = await self.store.read_output(archived, after_seq=0, max_encoded_bytes=65536, max_duration_us=2_000_000)
        self.assertEqual([o.chunk.payload for o in explicit], [o.chunk.payload for o in originals])
        self.assertEqual([o.original_event_seq for o in explicit], [o.original_event_seq for o in originals])
        self.assertFalse(any(b'"type":"audio"' in line for line in self.provider.inputs))

    async def test_output_commit_wait_does_not_block_interruption_and_cancelled_original_never_autoplays(self):
        self.store.output_gate = asyncio.Event()
        self.provider.events({"type": "audio", "data": base64.b64encode(bytes(4800)).decode(), "mime_type": "audio/pcm;rate=24000"})
        await asyncio.wait_for(self.store.output_entered.wait(), 2)
        self.assertFalse(any(isinstance(v, bytes) for v in self.sent))
        self.provider.events({"type": "interrupted"})
        await self.message("output_interrupt")
        self.store.output_gate.set()
        source_id = f"out_{self.session.id}_1"
        await settle(lambda: source_id in self.store.cancelled)
        self.assertIn(source_id, self.store.seals)
        self.assertFalse(any(isinstance(v, bytes) for v in self.sent))
        await self.reconnect(playback={"source_id": source_id, "played_ms": 0}, new_host=True)
        self.assertEqual((await self.message("output_available"))["sources"], [])
        self.assertFalse(self.messages("output_replayed"))
        self.assertIn(source_id, self.store.cancelled)
        self.assertFalse(any(isinstance(v, bytes) for v in self.sent))

    async def test_output_window_waits_for_device_durable_prefix_while_stop_remains_available(self):
        for _ in range(12):
            self.provider.events({"type": "audio", "data": base64.b64encode(bytes(4800)).decode(), "mime_type": "audio/pcm;rate=24000"})
        self.provider.events({"type": "turn_complete"})
        await settle(lambda: len([v for v in self.sent if isinstance(v, bytes)]) >= 9)
        await asyncio.sleep(.1)
        first = [AudioFrame.decode(v) for v in self.sent if isinstance(v, bytes)]
        self.assertLessEqual(sum(f.duration_us for f in first), 960000)
        bound = await self.message("output_epoch")
        await self.json({"type": "output_ack", "source_id": bound["source_id"], "stage": "device_durable", "chunk_seq": first[-1].chunk_seq})
        await settle(lambda: len([v for v in self.sent if isinstance(v, bytes)]) > len(first))
        await self.json({"type": "stop"})
        await settle(lambda: self.binding.stopped, timeout=.2)

    async def test_durable_v2_backlog_does_not_retain_provider_pcm_in_relay(self):
        await self._durable_v2_backlog()

    async def test_provider_receive_burst_waits_for_real_archive_commits(self):
        # Unlike synthetic direct on_event calls, this exercises the exact
        # provider recv parser + session ContextVar + guarded host callback.
        # Force the first *durable* store commit to pause, then send 9.6 s of
        # 320-ms PCM packets. Previously the eighth packet overflowed 2 s.
        archive = self.binding.state.output_archive
        source_id = f"out_{self.session.id}_1"
        self.store.output_gate = asyncio.Event()
        pcm = b"\x01\x00" * 7680
        item = {"serverContent": {"modelTurn": {"parts": [{"inlineData": {
            "mimeType": "audio/pcm;rate=24000",
            "data": base64.b64encode(pcm).decode("ascii"),
        }}]}}}
        token = provider_module._output_dispatch.set(
            lambda event, sink: self.host._dispatch_provider_event(self.session, event, sink))
        producer = None
        try:
            async def actual_recv():
                for _ in range(30):
                    await provider_module.handle_server_message_async(item, emit=self.provider.events)
                await provider_module.handle_server_message_async(
                    {"serverContent": {"turnComplete": True}}, emit=self.provider.events)
            producer = asyncio.create_task(actual_recv())
            await asyncio.wait_for(self.store.output_entered.wait(), 2)
            await asyncio.sleep(.05)
            self.assertFalse(producer.done(), "Producer must stop reading at bounded archive admission")
            self.assertLessEqual(archive.queued_duration_us, 2_000_000)
            self.assertIsNone(archive.error)
            self.store.output_gate.set()
            await asyncio.wait_for(producer, 8)
            await settle(lambda: source_id in self.store.seals, timeout=3)
            self.assertEqual(self.store.seals[source_id][1], 9_600_000)
            self.assertFalse(self.task.done(), self.closes)
        finally:
            self.store.output_gate.set()
            if producer is not None:
                if not producer.done():
                    producer.cancel()
                await asyncio.gather(producer, return_exceptions=True)
            provider_module._output_dispatch.reset(token)

    async def test_provider_single_oversized_event_preserves_exact_pcm_by_splitting(self):
        archive = self.binding.state.output_archive
        raw = b"\x7f\x00" * 120000
        event = {"type": "audio", "mime_type": "audio/pcm;rate=24000",
                 "data": base64.b64encode(raw).decode("ascii")}
        pieces = list(archive.provider_pieces(event))
        self.assertGreater(len(pieces), 5)
        self.assertEqual(b"".join(base64.b64decode(p["data"]) for p in pieces), raw)
        self.assertTrue(all(len(p["data"]) <= 60_000 for p in pieces))
        self.assertEqual(event["data"], base64.b64encode(raw).decode("ascii"))

    async def test_evicted_v2_backlog_recovers_originals_without_detaching(self):
        # Exercise the production eviction path under a deliberately tiny
        # cache, independently of codec compressibility and machine speed.
        with patch("live_interaction.audio_output.OUTPUT_CACHE_EVENTS", 8), \
                patch("live_interaction.audio_output.OUTPUT_CACHE_BYTES", 1024):
            await self._durable_v2_backlog(evicted=True)

    async def _durable_v2_backlog(self, *, evicted=False):
        # Hold the real device-durable ACK while the independent archive fsyncs
        # six seconds of provider output. Its compressed originals fit easily;
        # keeping the redundant base64 PCM in the relay exceeds 256 KiB.
        archive = self.binding.state.output_archive
        source_id = f"out_{self.session.id}_1"
        pcm = b"\x01\x00" * 2400
        for _ in range(60):
            seq = self.session.next_seq
            self.provider.events({"type": "audio", "data": base64.b64encode(pcm).decode(),
                                  "mime_type": "audio/pcm;rate=24000"})
            await settle(lambda: archive.processed_seq >= seq or self.task.done(), timeout=2)
            self.assertFalse(self.task.done(), self.closes)
        self.provider.events({"type": "turn_complete"})
        await settle(lambda: source_id in self.store.seals)
        self.assertEqual(self.store.seals[source_id][1], 6_000_000)
        before = [AudioFrame.decode(v) for v in self.sent if isinstance(v, bytes)]
        self.assertLessEqual(sum(f.duration_us for f in before), 960000)
        self.assertFalse(self.task.done(), self.closes)
        retained = [e for e in self.session.events if e.get("type") == "audio"]
        self.assertTrue(retained)
        self.assertEqual(retained[-1]["data"], base64.b64encode(pcm).decode(),
                         "Relay projection must not mutate the archive/ring original")
        if evicted:
            self.assertLessEqual(len(archive.results), 8)
            self.assertLessEqual(archive.result_bytes, 1024)
        # Drain only after actual ACK, and verify each wire payload equals its
        # durable original. No replay, invented progress or relaxed window.
        acknowledged = 0
        for _ in range(500 if evicted else 100):
            frames = [AudioFrame.decode(v) for v in self.sent if isinstance(v, bytes)]
            if frames and frames[-1].flags & EPOCH_END:
                break
            if frames and frames[-1].chunk_seq > acknowledged:
                acknowledged = frames[-1].chunk_seq
                await self.json({"type": "output_ack", "source_id": source_id,
                                 "stage": "device_durable", "chunk_seq": acknowledged})
            await asyncio.sleep(.01)
        self.assertTrue(frames and frames[-1].flags & EPOCH_END, self.closes)
        self.assertEqual(sum(f.duration_us for f in frames), 6_000_000)
        self.assertEqual([f.chunk_seq for f in frames], list(range(1, len(frames) + 1)))
        for frame in frames:
            self.assertEqual(frame.payload, self.store.chunks[(source_id, frame.chunk_seq)].payload)
        self.assertEqual(self.store.played.get(source_id, 0), 0,
                         "Byte receipt must not fabricate a played cursor")
        self.assertFalse(self.closes)

    async def test_interrupted_evicted_recovery_drops_admission_reservation(self):
        entered, release = asyncio.Event(), asyncio.Event()
        runtimes = []
        original_admit = SocketAudioV2._admit_output

        async def paused_admit(runtime, source_id, chunk, recovery):
            if recovery and not runtimes:
                runtimes.append(runtime)
                entered.set()
                await release.wait()
            await original_admit(runtime, source_id, chunk, recovery)

        source_id = f"out_{self.session.id}_1"
        archive = self.binding.state.output_archive
        try:
            with patch.object(SocketAudioV2, "_admit_output", paused_admit), \
                    patch("live_interaction.audio_output.OUTPUT_CACHE_EVENTS", 8), \
                    patch("live_interaction.audio_output.OUTPUT_CACHE_BYTES", 1024):
                for _ in range(30):
                    seq = self.session.next_seq
                    self.provider.events({"type": "audio", "data": base64.b64encode(bytes(4800)).decode(),
                                          "mime_type": "audio/pcm;rate=24000"})
                    await settle(lambda: archive.processed_seq >= seq)
                self.provider.events({"type": "turn_complete"})
                await settle(lambda: source_id in self.store.seals)
                frames = [AudioFrame.decode(v) for v in self.sent if isinstance(v, bytes)]
                self.assertTrue(frames)
                await self.json({"type": "output_ack", "source_id": source_id,
                                 "stage": "device_durable", "chunk_seq": frames[-1].chunk_seq})
                await asyncio.wait_for(entered.wait(), 2)
                self.provider.events({"type": "interrupted"})
                await self.message("output_interrupt")
                after_interrupt = len(self.sent)
                release.set()
                await settle(lambda: source_id in self.store.cancelled)
                await asyncio.sleep(.15)
                self.assertFalse(any(isinstance(v, bytes) for v in self.sent[after_interrupt:]),
                                 "Interrupted recovery must not send a reserved frame after admission wakes")
                self.assertFalse(runtimes[0].output_pending)
                self.assertEqual(sum(runtimes[0].output_bytes), 0)
                self.assertEqual(sum(runtimes[0].output_duration), 0)
                self.assertFalse(self.task.done(), self.closes)
        finally:
            release.set()
        await self.json({"type": "stop"})
        await settle(lambda: self.binding.stopped, timeout=.2)

    async def test_finished_live_source_can_unbind_and_rotate_without_reconnecting(self):
        old = await self.source("first_live")
        for chunk in vector_chunks("first_live"):
            await self.chunk(old, chunk)
        await self.message("audio_ack", source_id="first_live", chunk_seq=2)
        await self.json({"type": "source_seal", "source_ref": old[0], "last_chunk_seq": 2, "capture_end_us": 100000})
        self.assertTrue((await self.message("source_sealed", source_id="first_live"))["complete"])
        await self.json({"type": "source_unbind", "source_ref": old[0]})
        await self.message("source_unbound", source_id="first_live")
        new = await self.source("second_live")
        self.assertGreater(new[0], old[0]); self.assertGreater(new[1], old[1])
        self.assertEqual(self.binding.state.generation, 1)
        self.assertIn(("first_live", 2), self.store.chunks)

    async def test_recovery_refs_are_bounded_and_release_does_not_delete_sources(self):
        live = await self.source("fresh_live")
        old = [await self.source(f"old_{i}", recovery=True) for i in range(8)]
        await self.json({"type": "source_unbind", "source_ref": old[0][0]})
        await self.message("source_unbound", source_id="old_0")
        new = await self.source("old_8", recovery=True)
        self.assertGreater(new[0], old[-1][0]); self.assertGreater(new[1], old[-1][1])
        self.assertIn("old_0", self.store.sources)
        for chunk in vector_chunks("fresh_live"):
            await self.chunk(live, chunk)
        await self.message("audio_ack", source_id="fresh_live", chunk_seq=2)

    async def test_played_manifest_advances_and_interruption_after_eos_stays_cancelled(self):
        for _ in range(2):
            self.provider.events({"type": "audio", "data": base64.b64encode(bytes(4800)).decode(), "mime_type": "audio/pcm;rate=24000"})
            self.provider.events({"type": "turn_complete"})
        first = f"out_{self.session.id}_1"
        second = f"out_{self.session.id}_2"
        await settle(lambda: second in self.store.seals)
        before = len(self.messages("output_available"))
        await self.json({"type": "playback_position", "source_id": first, "played_ms": 100, "connection_generation": 1})
        await self.json({"type": "output_list"})
        await settle(lambda: len(self.messages("output_available")) > before)
        self.assertEqual([s["source_id"] for s in self.messages("output_available")[-1]["sources"]], [second])
        self.provider.events({"type": "interrupted"})
        await settle(lambda: second in self.store.cancelled)
        await self.reconnect(playback={"source_id": second, "played_ms": 0}, new_host=True)
        self.assertEqual((await self.message("output_available"))["sources"], [])
        self.assertFalse(self.messages("output_replayed"))
        self.assertIn(second, self.store.cancelled)
        self.assertFalse(any(isinstance(v, bytes) for v in self.sent))

    async def test_open_output_recovery_waits_for_real_tail_without_blocking_fresh_live(self):
        self.provider.events({"type": "audio", "data": base64.b64encode(bytes(4800)).decode(), "mime_type": "audio/pcm;rate=24000"})
        source_id = f"out_{self.session.id}_1"
        await settle(lambda: (source_id, 1) in self.store.outputs)
        await self.reconnect(playback={"source_id": source_id, "played_ms": 50})
        await settle(lambda: any(isinstance(v, bytes) for v in self.sent))
        self.assertFalse(self.messages("output_replayed"))
        refs = await self.source("new_live_during_output_recovery")
        for chunk in vector_chunks("new_live_during_output_recovery"):
            await self.chunk(refs, chunk)
        await self.message("audio_ack", source_id="new_live_during_output_recovery", chunk_seq=2)
        self.assertFalse(self.messages("output_replayed"))
        self.provider.events({"type": "audio", "data": base64.b64encode(bytes(4800)).decode(), "mime_type": "audio/pcm;rate=24000"})
        self.provider.events({"type": "turn_complete"})
        self.assertTrue((await self.message("output_replayed", source_id=source_id))["complete"])
        frames = [AudioFrame.decode(v) for v in self.sent if isinstance(v, bytes)]
        self.assertEqual(sum(f.duration_us for f in frames), 200000)
        self.assertEqual([f.chunk_seq for f in frames], list(range(1, len(frames) + 1)))

    async def test_recovery_gap_ack_is_contiguous_and_never_activates_or_forwards(self):
        refs = await self.source("old", recovery=True)
        chunks = vector_chunks("old")
        await self.chunk(refs, chunks[1], recovery=True)
        await self.message("audio_ack", source_id="old", chunk_seq=0)
        await self.chunk(refs, chunks[0], recovery=True)
        await self.message("audio_ack", source_id="old", chunk_seq=2)
        self.assertEqual(self.adapter.active_sources, [])
        self.assertEqual(self.adapter.projections, [])
        self.assertFalse(any(b'"type":"audio"' in line for line in self.provider.inputs))

    async def test_recovery_storage_wait_cannot_block_fresh_live(self):
        self.store.block["old"] = asyncio.Event()
        old = await self.source("old", recovery=True)
        await self.chunk(old, vector_chunks("old")[0], recovery=True)
        await asyncio.wait_for(self.store.entered.wait(), 1)
        live = await self.source("fresh")
        for chunk in vector_chunks("fresh"):
            await self.chunk(live, chunk)
        await self.message("audio_ack", source_id="fresh", chunk_seq=2)
        await settle(lambda: bool(self.adapter.projections))
        self.assertTrue(all(p.commit.source_id == "fresh" for p in self.adapter.projections))
        self.assertNotIn(("old", 1), self.store.chunks)

    async def test_stop_bypasses_blocked_commit(self):
        self.store.block["live"] = asyncio.Event()
        refs = await self.source("live")
        await self.chunk(refs, vector_chunks("live")[0])
        await asyncio.wait_for(self.store.entered.wait(), 1)
        await self.json({"type": "stop"})
        await settle(lambda: self.binding.stopped, timeout=.2)
        await asyncio.wait_for(self.task, 2)
        self.assertFalse(self.adapter.projections)
        self.assertFalse(any(b'"type":"audio"' in line for line in self.provider.inputs))

    async def test_stop_fences_after_awaited_projection_claim(self):
        self.adapter.projection_gate = asyncio.Event()
        refs = await self.source("live")
        await self.chunk(refs, vector_chunks("live")[0])
        await asyncio.wait_for(self.adapter.projection_entered.wait(), 1)
        await self.binding.stop()
        self.adapter.projection_gate.set()
        await settle(lambda: self.task.done(), timeout=2)
        self.assertFalse(any(b'"type":"audio"' in line for line in self.provider.inputs))

    async def test_immutable_epoch_cannot_be_redefined_in_place(self):
        refs = await self.source("live")
        changed = {**VECTORS["epoch"], "pre_skip": VECTORS["epoch"]["pre_skip"] + 1}
        await self.json({"type": "codec_epoch", "source_ref": refs[0], "descriptor": changed})
        await asyncio.wait_for(self.task, 2)
        self.assertEqual(self.closes[-1][1], "LIVE_AUDIO_EPOCH_CONFLICT")


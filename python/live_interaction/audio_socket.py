"""wl-live-v2 audio machinery for the existing socket relay and session host."""
from __future__ import annotations

import asyncio
import base64
import json
import re
import time
from collections import deque, OrderedDict
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

from .session_host import LiveError
from .audio_v2 import (AudioFrame, BoundSource, CodecEpoch, DurablePCMProjection, EncodedChunk,
                       SourceDescriptor, MAX_SAFE_INTEGER, UPLINK_MAGIC, DOWNLINK_MAGIC,
                       RECOVERY, PORTION_START, PORTION_END, EPOCH_END, EVENT_END, SOURCE_FLAGS,
                       OutputResume, identifier, integer, invalid)


class AudioWorkPool:
    """One small native pool per existing host, never one thread per stream."""
    def __init__(self):
        self.executor = ThreadPoolExecutor(max_workers=2, thread_name_prefix="live-codec")
        self.slots = asyncio.Semaphore(4)
        self.pending = set()
        self.closed = False

    async def run(self, function, *args, **kwargs):
        if self.closed:
            raise invalid("CODEC_CLOSED", "The host codec pool is closed")
        async with self.slots:
            if self.closed:
                raise invalid("CODEC_CLOSED", "The host codec pool is closed")
            future = asyncio.get_running_loop().run_in_executor(self.executor, lambda: function(*args, **kwargs))
            self.pending.add(future)
            future.add_done_callback(self.pending.discard)
            try:
                return await asyncio.shield(future)
            except asyncio.CancelledError:
                # A native call cannot be cancelled half-way. Do not free its
                # context from another task while the bounded job still uses it.
                await asyncio.shield(future)
                raise

    async def close(self, *, timeout=5.0):
        self.closed = True
        # Retire idle threads and reject new work without blocking the event
        # loop in join(). Existing bounded native calls retain their contexts.
        self.executor.shutdown(wait=False)
        if self.pending:
            _, remaining = await asyncio.wait(tuple(self.pending), timeout=timeout)
            if remaining:
                raise invalid("CODEC_SHUTDOWN_TIMEOUT", "Native codec work did not finish within the shutdown bound")


@dataclass
class _Source:
    bound: BoundSource
    recovery: bool
    source_time_now_us: int
    registered_at: float
    submitted: int = 0
    acknowledged: int = 0
    complete: bool = False


class SocketAudioV2:
    def __init__(self, binding, services, send, failed):
        self.binding, self.services = binding, services
        self.store = services.store_factory(binding.session)
        self.limits = services.limits
        self.send, self.failed = send, failed
        if not hasattr(binding.host, "_audio_work_pool"):
            if getattr(binding.host, "_host_close_task", None) is not None:
                raise invalid("CODEC_CLOSED", "The host codec pool is closed")
            binding.host._audio_work_pool = AudioWorkPool()
        self.work = binding.host._audio_work_pool
        self.sources: dict[int, _Source] = {}
        self.epochs: dict[int, tuple[int, CodecEpoch]] = {}
        self.next_source_ref = self.next_epoch_ref = 1
        self.live_ref = None
        self.decoder = None
        self.decoder_key = None
        self.decoder_ended = True
        self.portions = deque()
        self.queues = [asyncio.Queue(maxsize=32), asyncio.Queue(maxsize=32)]
        self.queued_bytes = [0, 0]
        self.queued_duration = [0, 0]
        self.next_work_at = [0.0, 0.0]
        self.tasks = []
        self.closed = False
        self.archive = binding.state.output_archive
        if self.archive is None:
            raise invalid("OUTPUT_STORE", "Compressed v2 requires a durable output archive")
        self.output_store = self.archive.store
        self.output_epoch_ref = 0
        self.output_bindings = OrderedDict()
        self.output_next = {}
        self.output_acknowledged = {}
        self.output_pending = {}
        self.output_bytes = [0, 0]
        self.output_duration = [0, 0]
        self.output_changed = asyncio.Event()
        self.output_recovery = asyncio.Queue(maxsize=8)
        self.output_recovering = set()
        self.playback_resume = None

    def negotiate(self, hello):
        capabilities = hello.get("capabilities")
        if not isinstance(capabilities, dict):
            raise invalid("CAPABILITIES", "Compressed v2 capabilities are required")
        selected = []
        for direction in ("uplink", "downlink"):
            offered = capabilities.get(direction + "_codecs")
            if not isinstance(offered, list) or not 1 <= len(offered) <= 8 or any(not isinstance(x, str) or len(x) > 96 for x in offered):
                raise invalid("CAPABILITIES", "Invalid compressed codec capabilities")
            profile = next((value for value in self.services.codecs.profiles(direction) if value in offered), None)
            if not profile:
                raise invalid("CODEC_UNAVAILABLE", f"No compressed {direction} profile is available")
            selected.append(profile)
        self.uplink_profile, self.downlink_profile = selected
        if self.downlink_profile != self.archive.profile:
            raise invalid("CODEC_UNAVAILABLE", "The original output codec is not supported by this receiver")
        playback = hello.get("playback_resume")
        if playback is not None:
            if not isinstance(playback, dict):
                raise invalid("PLAYBACK", "Invalid playback resume request")
            played = integer(playback.get("played_ms"), 0, MAX_SAFE_INTEGER, "played_ms")
            if "source_id" in playback:
                source_id = identifier(playback["source_id"], "source_id")
            else:
                response = integer(playback.get("response_id"), 1, self.binding.session.response_seq, "response_id")
                source_id = f"out_{self.binding.session.id}_{response}"
            self.playback_resume = (source_id, played)
            self.output_recovering.add(source_id)
            self.output_recovery.put_nowait(self.playback_resume)
        resume = hello.get("resume", [])
        if not isinstance(resume, list) or len(resume) > self.limits.max_sources:
            raise invalid("SOURCE_LIMIT", "Too many pending sources")
        # Source manifests bind and obtain authoritative cursors independently
        # after ready. Client-reported cursors never become server receipts.
        return {"uplink_profile": self.uplink_profile, "downlink_profile": self.downlink_profile,
                "recovery_profiles": list(self.services.codecs.profiles("uplink")),
                "audio_limits": self.limits.as_dict(), "resume": [
                    {"source_id": item.get("source_id"), "status": "awaiting_source_descriptor"}
                    for item in resume if isinstance(item, dict)]}

    def start(self):
        self.tasks = [asyncio.create_task(self._worker(0)), asyncio.create_task(self._worker(1)),
                      asyncio.create_task(self._output_recovery_worker())]
        return self.tasks

    def _current(self):
        self.binding.heartbeat()
        if self.closed or self.binding.stopped or self.binding.session.stop_reason is not None:
            raise invalid("STOPPED", "Audio delivery stopped")

    def _enqueue(self, lane, value, size, duration):
        self._current()
        byte_limit = self.limits.recovery_window_bytes if lane else self.limits.live_window_bytes
        duration_limit = self.limits.recovery_window_duration_us if lane else self.limits.live_window_duration_us
        if (self.queues[lane].full() or self.queued_bytes[lane] + size > byte_limit or
                self.queued_duration[lane] + duration > duration_limit):
            raise invalid("BACKPRESSURE", "The bounded audio delivery lane is full")
        self.queued_bytes[lane] += size
        self.queued_duration[lane] += duration
        self.queues[lane].put_nowait((value, size, duration))

    def audio(self, data):
        frame = AudioFrame.decode(data, self.limits)
        if frame.magic != UPLINK_MAGIC or frame.stream_ref not in self.sources:
            raise invalid("SOURCE", "Unknown input source reference")
        source = self.sources[frame.stream_ref]
        pair = self.epochs.get(frame.epoch_ref)
        if pair is None or pair[0] != frame.stream_ref:
            raise invalid("EPOCH", "Epoch reference is not bound to this source")
        recovery = bool(frame.flags & RECOVERY)
        if recovery != source.recovery or (not recovery and frame.delivery_generation != self.binding.state.generation):
            raise invalid("GENERATION", "Audio delivery lane or generation changed")
        if not recovery:
            if frame.stream_ref != self.live_ref or frame.chunk_seq > source.submitted + 1:
                raise invalid("SEQUENCE", "Fresh Live chunks must be contiguous")
            self._age(source, frame)
        self._enqueue(int(recovery), frame, len(frame.payload), frame.duration_us)
        source.submitted = max(source.submitted, frame.chunk_seq)

    def control(self, message):
        kind = message.get("type")
        if kind == "output_ack":
            self._output_ack(message)
            return
        if kind == "output_resume":
            self._request_output(identifier(message.get("source_id"), "source_id"),
                                 integer(message.get("played_ms"), 0, MAX_SAFE_INTEGER, "played_ms"))
            return
        if kind == "source_open":
            lane = int(message.get("lane") == "recovery")
        elif kind in ("codec_epoch", "source_seal", "source_unbind"):
            source = self.sources.get(message.get("source_ref"))
            if source is None:
                raise invalid("SOURCE", "Source must be bound before its codec or seal")
            lane = int(source.recovery)
        elif kind in ("input", "playback_position", "output_list"):
            lane = 0
        else:
            raise invalid("CONTROL", "Unsupported v2 control message")
        self._enqueue(lane, message, len(json.dumps(message).encode("utf-8")), 0)

    def _age(self, source, frame):
        now_source_us = source.source_time_now_us + round((time.monotonic() - source.registered_at) * 1_000_000)
        age_us = now_source_us - frame.start_us - frame.duration_us
        if age_us < -250_000 or age_us > self.limits.live_max_age_ms * 1000:
            raise invalid("STALE", "Fresh audio is outside the admitted capture clock window")

    async def _worker(self, lane):
        try:
            while True:
                value, size, duration = await self.queues[lane].get()
                try:
                    if duration:
                        rate = self.limits.recovery_ingest_us_per_second if lane else self.limits.live_decode_us_per_second
                        delay = self.next_work_at[lane] - time.monotonic()
                        if delay > 0:
                            await asyncio.sleep(delay)
                        self.next_work_at[lane] = max(time.monotonic(), self.next_work_at[lane]) + duration / rate
                    self._current()
                    if isinstance(value, AudioFrame):
                        await self._chunk(value)
                    else:
                        await self._control(value)
                finally:
                    self.queued_bytes[lane] -= size
                    self.queued_duration[lane] -= duration
                    self.queues[lane].task_done()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if not self.failed.done():
                self.failed.set_result(exc)

    async def _control(self, message):
        kind = message.get("type")
        if kind == "source_open":
            descriptor = SourceDescriptor.from_dict(message)
            recovery = message.get("lane") == "recovery"
            if message.get("lane") not in ("live", "recovery"):
                raise invalid("SOURCE", "Unknown source delivery lane")
            source_time = integer(message.get("source_time_now_us"), 0, MAX_SAFE_INTEGER, "source_time_now_us")
            existing = next(((ref, value) for ref, value in self.sources.items() if value.bound.source.source_id == descriptor.source_id), None)
            if existing:
                ref, source = existing
                if source.bound.source != descriptor or source.recovery != recovery:
                    raise invalid("SOURCE_CONFLICT", "Source identity or lane changed")
            else:
                if len(self.sources) >= self.limits.max_sources or (not recovery and self.live_ref is not None):
                    raise invalid("SOURCE_LIMIT", "Source reference table is full or a live source is already bound")
                bound = await self.store.open_source(descriptor)
                self._current()
                if not isinstance(bound, BoundSource) or bound.source != descriptor:
                    raise invalid("DURABILITY", "Store returned the wrong source binding")
                integer(bound.server_durable_seq, 0, MAX_SAFE_INTEGER, "server_durable_seq")
                if not recovery and bound.server_durable_seq:
                    raise invalid("SOURCE_RECOVERY", "A previously delivered source must use recovery; open a fresh live source")
                ref = integer(self.next_source_ref, 1, 0xFFFFFFFF, "source_ref")
                self.next_source_ref += 1
                source = _Source(bound, recovery, source_time, time.monotonic(), bound.server_durable_seq, bound.server_durable_seq)
                self.sources[ref] = source
                if not recovery:
                    await self.binding.activate_audio_source(bound)
                    self._current()
                    self.live_ref = ref
            await self.send_json({"type": "source_bound", "source_id": descriptor.source_id, "server_source_id": source.bound.server_source_id,
                                  "source_ref": ref, "server_durable_seq": source.acknowledged})
        elif kind == "codec_epoch":
            source_ref = integer(message.get("source_ref"), 1, 0xFFFFFFFF, "source_ref")
            source = self.sources[source_ref]
            epoch = CodecEpoch.from_dict(message.get("descriptor"))
            allowed = self.services.codecs.profiles("uplink") if source.recovery else (self.uplink_profile,)
            if epoch.profile not in allowed or epoch.decoded_sample_rate != 16000:
                raise invalid("CODEC_UNAVAILABLE", "The source codec is not available in this delivery lane")
            existing = next(((ref, pair) for ref, pair in self.epochs.items() if pair[0] == source_ref and pair[1].epoch_id == epoch.epoch_id), None)
            if existing:
                ref, (_, old_epoch) = existing
                if old_epoch != epoch:
                    raise invalid("EPOCH_CONFLICT", "Codec epoch descriptors are immutable")
            else:
                if len(self.epochs) >= self.limits.max_epochs:
                    raise invalid("EPOCH_LIMIT", "Codec epoch reference table is full")
                await self.store.register_epoch(source.bound, epoch)
                self._current()
                ref = integer(self.next_epoch_ref, 1, 0xFFFFFFFF, "epoch_ref")
                self.next_epoch_ref += 1
                self.epochs[ref] = (source_ref, epoch)
            await self.send_json({"type": "epoch_bound", "source_id": source.bound.source.source_id, "epoch_id": epoch.epoch_id, "epoch_ref": ref})
        elif kind == "source_seal":
            source = self.sources[message["source_ref"]]
            seq = integer(message.get("last_chunk_seq"), 0, MAX_SAFE_INTEGER, "last_chunk_seq")
            end = integer(message.get("capture_end_us"), 0, MAX_SAFE_INTEGER, "capture_end_us")
            receipt = await self.store.seal_source(source.bound, seq, end)
            if (not isinstance(receipt, dict) or type(receipt.get("complete")) is not bool
                    or integer(receipt.get("server_durable_seq"), 0, MAX_SAFE_INTEGER, "server_durable_seq") < source.acknowledged):
                raise invalid("DURABILITY", "Invalid source terminal receipt")
            source.complete = receipt["complete"]
            self._current()
            await self.send_json({"type": "source_sealed", "source_id": source.bound.source.source_id,
                                  "last_chunk_seq": seq, "capture_end_us": end,
                                  "server_durable_seq": receipt["server_durable_seq"], "complete": source.complete})
        elif kind == "source_unbind":
            ref = message["source_ref"]
            source = self.sources[ref]
            if not source.recovery:
                if not source.complete or not self.decoder_ended or self.portions:
                    raise invalid("SOURCE", "A live source must finish its durable codec tail before unbinding")
                if self.decoder:
                    self.decoder.close()
                    self.decoder = None
                    self.decoder_key = None
                self.live_ref = None
            # This ordered lane has completed its prior commit/ACK work. Removing
            # references has no authority to delete original source bytes.
            self.epochs = {key: pair for key, pair in self.epochs.items() if pair[0] != ref}
            del self.sources[ref]
            await self.send_json({"type": "source_unbound", "source_id": source.bound.source.source_id})
        elif kind == "input":
            body = message.get("message")
            if not isinstance(body, dict) or "audio_base64" in body:
                raise invalid("FRAME", "Compressed input must use WLA2")
            await self.binding.input(body)
        elif kind == "playback_position":
            generation = integer(message.get("connection_generation"), 1, 0xFFFFFFFF, "connection_generation")
            played = integer(message.get("played_ms"), 0, MAX_SAFE_INTEGER, "played_ms")
            if generation != self.binding.state.generation:
                raise invalid("PLAYBACK", "Playback position belongs to another response or generation")
            if "source_id" in message:
                source_id = identifier(message["source_id"], "source_id")
            else:
                response = integer(message.get("response_id"), 1, self.binding.session.response_seq, "response_id")
                source_id = f"out_{self.binding.session.id}_{response}"
            source = await self.output_store.resume_source(source_id)
            self._current()
            await self.output_store.mark_played(source, played * 1000)
        elif kind == "output_list":
            if self.output_recovery.full():
                raise invalid("OUTPUT_RECOVERY_LIMIT", "Output recovery lane is full")
            self.output_recovery.put_nowait((None, 0))

    async def _chunk(self, frame):
        source = self.sources[frame.stream_ref]
        _, epoch = self.epochs[frame.epoch_ref]
        chunk = EncodedChunk(source.bound.source.source_id, epoch.epoch_id, frame.chunk_seq, frame.start_us,
                             frame.duration_us, frame.flags & SOURCE_FLAGS, frame.payload)
        # This is the only route to a PCM projection. No packet decode occurs
        # before the store has committed these exact original encoded bytes.
        commit = await self.store.commit_chunk(source.bound, epoch, chunk)
        commit.validate_for(source.bound, chunk)
        if commit.server_durable_seq < source.acknowledged:
            raise invalid("DURABILITY", "The committed contiguous prefix moved backwards")
        source.acknowledged = commit.server_durable_seq
        self._current()
        await self.send_json({"type": "audio_ack", "stage": "server_durable", "source_id": chunk.source_id,
                              "chunk_seq": source.acknowledged})
        if source.recovery or not commit.newly_committed:
            return
        self._current()
        self._age(source, frame)
        key = (frame.stream_ref, frame.epoch_ref)
        if self.decoder_key != key:
            if self.decoder is not None and not self.decoder_ended:
                raise invalid("CODEC_SEQUENCE", "Previous live codec epoch has no acknowledged terminal chunk")
            if self.decoder:
                self.decoder.close()
            self.decoder = await self.work.run(self.services.codecs.decoder, epoch)
            self.decoder_key = key
            self.decoder_ended = False
        if chunk.flags & PORTION_START:
            if self.portions and self.portions[-1]["end"] is None or len(self.portions) >= 8:
                raise invalid("PORTION", "Speech portions overlap or exceed their bound")
            self.portions.append({"start": chunk.capture_start_us, "end": None, "started": False})
        if chunk.flags & PORTION_END:
            if not self.portions or self.portions[-1]["end"] is not None:
                raise invalid("PORTION", "Speech portion end has no start")
            self.portions[-1]["end"] = chunk.capture_start_us + chunk.duration_us
        decoded = await self.work.run(self.decoder.decode, chunk)
        self._current()
        self._age(source, frame)
        await self._project(commit, decoded)
        if chunk.flags & EPOCH_END:
            self.decoder_ended = True
            if self.portions:
                raise invalid("PORTION", "Codec epoch ended before its speech portion completed")

    async def _project(self, commit, decoded):
        pcm = decoded.pcm16
        start = decoded.capture_start_us
        end = start + round((len(pcm) // 2) * 1_000_000 / 16000)
        while self.portions:
            portion = self.portions[0]
            left = max(start, portion["start"])
            right = min(end, portion["end"]) if portion["end"] is not None else end
            if right > left:
                if not portion["started"]:
                    if self.binding.session.manual_activity_detection:
                        await self.binding.input({"activity_start": True})
                    portion["started"] = True
                first = max(0, round((left - start) * 16000 / 1_000_000)) * 2
                last = min(len(pcm) // 2, round((right - start) * 16000 / 1_000_000)) * 2
                for offset in range(first, last, 8000):
                    self._current()
                    projection = DurablePCMProjection(commit, pcm[offset:min(offset + 8000, last)], 16000,
                                                      start + round(offset * 1_000_000 / 32000))
                    await self.binding.input_pcm(projection)
            if portion["end"] is not None and end >= portion["end"]:
                if portion["started"]:
                    key = "activity_end" if self.binding.session.manual_activity_detection else "audio_stream_end"
                    await self.binding.input({key: True})
                self.portions.popleft()
            else:
                break

    async def send_json(self, value):
        await self.send(json.dumps(value, ensure_ascii=False, separators=(",", ":")))

    async def interrupt_output(self, response_id):
        source_id = f"out_{self.binding.session.id}_{response_id}"
        for key, (size, duration, lane) in tuple(self.output_pending.items()):
            if key[0] == source_id:
                # Cancellation frees a delivery window, not a durable receipt.
                self.output_bytes[lane] -= size
                self.output_duration[lane] -= duration
                del self.output_pending[key]
        self.output_changed.set()
        await self.send_json({"type": "output_interrupt", "source_id": source_id,
                              "provider_response_id": response_id,
                              "connection_generation": self.binding.state.generation})

    async def output(self, event):
        """The existing writer obtains only committed original compressed bytes."""
        kind = event.get("type")
        if kind != "interrupted":
            try:
                outputs = await self.archive.for_event(event)
            except LiveError as error:
                response = event.get("response_id")
                if (error.code != "LIVE_AUDIO_OUTPUT_HISTORY"
                        or kind not in ("audio", "turn_complete", "closed")
                        or not isinstance(response, int) or isinstance(response, bool) or response < 1):
                    raise
                # Eviction is not loss of the durable original. Reuse the
                # bounded recovery lane and its unchanged byte/ACK/played
                # contract instead of detaching a healthy socket. It preserves
                # output_next, so an already sent prefix is never sent twice.
                if not self.binding.stopped and not self.archive.is_cancelled(response):
                    source_id = f"out_{self.binding.session.id}_{response}"
                    if source_id not in self.output_recovering:
                        self._request_output(source_id, 0)
                        self.binding.host.diagnostic(self.binding.session, "socket_output_cache_recovery",
                            code=error.code, connection_generation=self.binding.state.generation)
                outputs = ()
            for archived in outputs:
                if archived.cancelled or self.archive.is_cancelled(archived.response_id):
                    continue
                source_id = archived.source.source.source_id
                if source_id in self.output_recovering:
                    continue
                expected = self.output_next.get(source_id, 1)
                if archived.chunks and archived.chunks[0].chunk.chunk_seq > expected:
                    # A fresh socket has no decoder history. Recover the original
                    # prefix independently; never remux a suffix into a new epoch.
                    self._request_output(source_id, 0)
                    continue
                async for payload in self._frames(archived, recovery=False):
                    yield payload
        if kind != "audio":
            yield json.dumps({"type": "event", "event": event}, ensure_ascii=False, separators=(",", ":"))

    def _request_output(self, source_id, played_ms):
        self._current()
        if source_id in self.output_recovering:
            return
        if self.output_recovery.full() or len(self.output_recovering) >= 8:
            raise invalid("OUTPUT_RECOVERY_LIMIT", "The bounded output recovery lane is full")
        self.output_recovering.add(source_id)
        self.output_recovery.put_nowait((source_id, played_ms))

    def _output_ack(self, message):
        self._current()
        source_id = identifier(message.get("source_id"), "source_id")
        entry = self.output_bindings.get(source_id)
        seq = integer(message.get("chunk_seq"), 0, MAX_SAFE_INTEGER, "chunk_seq")
        previous = self.output_acknowledged.get(source_id, 0)
        if (message.get("stage") != "device_durable" or entry is None or
                seq < previous or seq > entry[0].server_durable_seq):
            raise invalid("OUTPUT_ACK", "Invalid device-durable contiguous output receipt")
        self.output_acknowledged[source_id] = seq
        for key, (size, duration, lane) in tuple(self.output_pending.items()):
            if key[0] == source_id and key[1] <= seq:
                self.output_bytes[lane] -= size
                self.output_duration[lane] -= duration
                del self.output_pending[key]
        self.output_changed.set()

    async def _admit_output(self, source_id, chunk, recovery):
        lane = int(recovery)
        byte_limit = self.limits.output_recovery_window_bytes if lane else self.limits.output_window_bytes
        duration_limit = self.limits.output_recovery_window_duration_us if lane else self.limits.output_window_duration_us
        if chunk.chunk_seq <= self.output_acknowledged.get(source_id, 0):
            return
        while (self.output_bytes[lane] + len(chunk.payload) > byte_limit or
               self.output_duration[lane] + chunk.duration_us > duration_limit or
               sum(value[2] == lane for value in self.output_pending.values()) >= 32):
            self._current()
            self.output_changed.clear()
            try:
                await asyncio.wait_for(self.output_changed.wait(), 15.0 if recovery else 5.0)
            except asyncio.TimeoutError:
                raise invalid("OUTPUT_ACK_TIMEOUT", "Original output exceeded its durable receipt window") from None
        self._current()
        self.output_pending[(source_id, chunk.chunk_seq)] = (len(chunk.payload), chunk.duration_us, lane)
        self.output_bytes[lane] += len(chunk.payload)
        self.output_duration[lane] += chunk.duration_us

    async def _frames(self, archived, *, recovery):
        source_id = archived.source.source.source_id
        entry = self.output_bindings.get(source_id)
        if entry is None:
            while len(self.output_bindings) >= 32:
                removable = next((name for name in self.output_bindings if name not in self.output_recovering
                                  and not any(key[0] == name for key in self.output_pending)), None)
                if removable is None:
                    raise invalid("OUTPUT_LIMIT", "Output reference table is full")
                del self.output_bindings[removable]
                self.output_next.pop(removable, None)
                self.output_acknowledged.pop(removable, None)
            self.output_epoch_ref += 1
            integer(self.output_epoch_ref, 1, 0xFFFFFFFF, "output_epoch_ref")
            ref = self.output_epoch_ref
            self.output_bindings[source_id] = entry = (archived.source, archived.epoch, ref)
            yield json.dumps({"type": "output_epoch", "source_id": source_id,
                              "server_source_id": archived.source.server_source_id,
                              "stream_ref": ref, "epoch_ref": ref,
                              "response_id": archived.response_id, "provider_response_id": archived.response_id,
                              "server_durable_seq": archived.source.server_durable_seq,
                              "connection_generation": self.binding.state.generation,
                              "descriptor": archived.epoch.as_dict()}, separators=(",", ":"))
        elif entry[1] != archived.epoch or entry[0].server_source_id != archived.source.server_source_id:
            raise invalid("OUTPUT_IDENTITY", "Original output binding changed")
        ref = entry[2]
        # Historical recovery may reuse a numeric response id from a different
        # session. Only this host's exact source shares its interruption fence.
        owns_response = source_id == f"out_{self.binding.session.id}_{archived.response_id}"
        self.output_bindings[source_id] = (archived.source, archived.epoch, ref)
        for index, original in enumerate(archived.chunks):
            chunk = original.chunk
            expected = self.output_next.get(source_id, 1)
            if chunk.chunk_seq < expected:
                continue
            if chunk.chunk_seq != expected:
                raise invalid("OUTPUT_SEQUENCE", "Output codec dependencies are not contiguous")
            if owns_response and self.archive.is_cancelled(archived.response_id):
                return
            await self._admit_output(source_id, chunk, recovery)
            if owns_response and self.archive.is_cancelled(archived.response_id):
                pending = self.output_pending.pop((source_id, chunk.chunk_seq), None)
                if pending:
                    size, duration, lane = pending
                    self.output_bytes[lane] -= size
                    self.output_duration[lane] -= duration
                return
            frame = AudioFrame(DOWNLINK_MAGIC, chunk.flags | (EVENT_END if not recovery and index == len(archived.chunks) - 1 else 0),
                               ref, ref, chunk.chunk_seq, chunk.capture_start_us, chunk.duration_us,
                               self.binding.state.generation, original.original_event_seq, chunk.payload)
            self.output_next[source_id] = chunk.chunk_seq + 1
            yield frame.encode(self.limits)

    @staticmethod
    def _resume_dict(status):
        if not isinstance(status, OutputResume):
            raise invalid("OUTPUT_STORE", "Invalid durable output recovery status")
        return {"source_id": status.source.source.source_id, "server_source_id": status.source.server_source_id,
                "server_durable_seq": status.source.server_durable_seq, "played_us": status.played_us,
                "duration_us": status.duration_us, "complete": status.complete, "cancelled": status.cancelled}

    async def _output_recovery_worker(self):
        try:
            await self._output_manifest()
            while True:
                source_id, played_ms = await self.output_recovery.get()
                try:
                    if source_id is None:
                        await self._output_manifest()
                    else:
                        await self._replay_output(source_id, played_ms)
                finally:
                    self.output_recovering.discard(source_id)
                    self.output_recovery.task_done()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            if not self.failed.done():
                self.failed.set_result(exc)

    async def _output_manifest(self):
        statuses = await self.output_store.pending_outputs(limit=8)
        if not isinstance(statuses, list) or len(statuses) > 8:
            raise invalid("OUTPUT_STORE", "Output manifest exceeds its bound")
        self._current()
        await self.send_json({"type": "output_available", "sources": [self._resume_dict(s) for s in statuses]})

    async def _replay_output(self, source_id, played_ms):
        from .audio_output import ArchivedOutput
        source = await self.output_store.resume_source(source_id)
        await self.output_store.mark_played(source, played_ms * 1000)
        status = await self.output_store.output_status(source)
        self._resume_dict(status)
        if status.cancelled:
            await self.send_json({"type": "transport_notice", "code": "LIVE_AUDIO_OUTPUT_CANCELLED", "source_id": source_id})
            await self.send_json({"type": "output_replayed", "source_id": source_id, "through_seq": 0,
                                  "complete": False, "cancelled": True})
            return
        after = 0
        ended = False
        next_at = time.monotonic()
        while not ended:
            self._current()
            observed = self.archive.processed_seq
            chunks = await self.output_store.read_output(status.source, after_seq=after,
                        max_encoded_bytes=self.limits.output_recovery_window_bytes,
                        max_duration_us=self.limits.output_recovery_window_duration_us)
            if not chunks:
                # An open response on this host may still append. A process-crash
                # prefix has no imaginary EOS; report it as incomplete instead.
                if self.archive.owns_open_source(source_id):
                    self.archive.changed.clear()
                    if self.archive.processed_seq == observed and self.archive.owns_open_source(source_id):
                        await self.archive.changed.wait()
                    status = await self.output_store.output_status(source)
                    continue
                break
            if (len(chunks) > 32 or sum(len(c.chunk.payload) for c in chunks) > self.limits.output_recovery_window_bytes
                    or sum(c.chunk.duration_us for c in chunks) > self.limits.output_recovery_window_duration_us):
                raise invalid("OUTPUT_STORE", "Output recovery read exceeded its bounds")
            first = chunks[0].chunk
            if first.source_id != source_id or first.chunk_seq != after + 1:
                raise invalid("OUTPUT_STORE", "Output recovery read is not its committed contiguous prefix")
            epoch = await self.output_store.epoch(source, first.epoch_id)
            if epoch.profile != self.downlink_profile or any(c.chunk.source_id != source_id or c.chunk.epoch_id != epoch.epoch_id for c in chunks):
                raise invalid("OUTPUT_CODEC", "Original output epoch is unavailable on this receiver")
            status = await self.output_store.output_status(source)
            if status.cancelled:
                break
            response = integer(int(source_id.rsplit("_", 1)[-1]), 1, 0xFFFFFFFF, "provider_response_id")
            archived = ArchivedOutput(status.source, epoch, response, tuple(chunks))
            async for payload in self._frames(archived, recovery=True):
                self._current()
                await self.send(payload)
            after = chunks[-1].chunk.chunk_seq
            ended = bool(chunks[-1].chunk.flags & EPOCH_END)
            next_at += sum(c.chunk.duration_us for c in chunks) / self.limits.output_recovery_us_per_second
            if next_at > time.monotonic():
                await asyncio.sleep(next_at - time.monotonic())
        while ended and not status.complete and self.archive.owns_open_source(source_id):
            self.archive.changed.clear()
            if self.archive.owns_open_source(source_id):
                await self.archive.changed.wait()
            status = await self.output_store.output_status(source)
        status = await self.output_store.output_status(source)
        await self.send_json({"type": "output_replayed", "source_id": source_id, "through_seq": after,
                              "complete": bool(ended and status.complete), "cancelled": status.cancelled})
        if not ended and not status.cancelled:
            await self.send_json({"type": "transport_notice", "code": "LIVE_AUDIO_OUTPUT_INCOMPLETE", "source_id": source_id})

    async def close(self):
        self.closed = True
        self.output_changed.set()
        for task in self.tasks:
            task.cancel()
        if self.tasks:
            await asyncio.gather(*self.tasks, return_exceptions=True)
        if self.decoder:
            self.decoder.close()

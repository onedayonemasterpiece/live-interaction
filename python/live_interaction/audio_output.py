"""Bounded output archiving owned by the existing Live session, not its socket.

The provider still emits PCM through the existing host. One ordered codec state
commits its compressed original before any socket can obtain a deliverable chunk.
Detaching a socket does not cancel this worker or remux an existing response.
"""
from __future__ import annotations

import asyncio
import base64
import os
import re
import time
from collections import OrderedDict
from dataclasses import dataclass

from .audio_v2 import (BoundSource, CodecEpoch, EncodedChunk, OutputChunk,
                       SourceDescriptor, EPOCH_END, MAX_SAFE_INTEGER, integer, invalid)


OUTPUT_CACHE_EVENTS = 320
OUTPUT_CACHE_BYTES = 131072


@dataclass(frozen=True)
class ArchivedOutput:
    source: BoundSource
    epoch: CodecEpoch
    response_id: int
    chunks: tuple[OutputChunk, ...]
    cancelled: bool = False


class AudioOutputArchive:
    def __init__(self, host, session, services, work):
        self.host, self.session, self.services, self.work = host, session, services, work
        self.store = services.output_store_factory(session)
        profiles = services.codecs.profiles("downlink")
        if not profiles:
            raise invalid("CODEC_UNAVAILABLE", "No compressed output encoder is available")
        self.profile = profiles[0]
        self.queue = asyncio.Queue(maxsize=320)
        self.queued_bytes = self.queued_duration_us = 0
        self.results = OrderedDict()
        self.result_bytes = 0
        self.processed_seq = 0
        self.changed = asyncio.Event()
        self.admission_changed = asyncio.Event()
        self.debug_extended = os.environ.get("LIVE_AUDIO_DIAGNOSTICS") == "extended"
        self._audio_commits = 0
        self._last_encode_ms = self._last_commit_ms = 0
        self.error = None
        self.closing = False
        self.encoder = None
        self.source = self.epoch = None
        self.active_source_id = None
        self.response_id = self.rate = self.chunk_seq = self.end_us = self.last_audio_event = 0
        self.cancelled_responses = OrderedDict()
        self.task = asyncio.create_task(self._run())

    def provider_pieces(self, event):
        """Bound a single *provider* PCM item without changing sample order.

        The provider sometimes packs more than two seconds into one item.
        Split only large input events at complete 16-bit samples, before the
        host allocates event sequence/response identities; no samples are lost.
        """
        if event.get("type") != "audio":
            yield event
            return
        encoded = event.get("data") or ""
        mime = str(event.get("mime_type") or "")
        match = re.search(r"rate=(\d+)", mime)
        rate = int(match[1]) if match else 24000
        if rate not in (16000, 24000, 48000) or not isinstance(encoded, str):
            raise invalid("OUTPUT", "Invalid provider PCM metadata")
        # Normal ~320-ms provider events do not pay a second decode/encode cost.
        if len(encoded) <= 60_000:
            yield event
            return
        try:
            raw = base64.b64decode(encoded, validate=True)
        except (ValueError, TypeError):
            raise invalid("OUTPUT", "Invalid provider PCM base64") from None
        if not raw or len(raw) % 2 or len(raw) > 262144:
            raise invalid("OUTPUT", "Invalid provider PCM size")
        samples_per_piece = rate * 2 // 5  # 400 ms, well below the 2-s bound
        for offset in range(0, len(raw), samples_per_piece * 2):
            fragment = raw[offset:offset + samples_per_piece * 2]
            yield {**event, "data": base64.b64encode(fragment).decode("ascii")}

    async def admit(self, event):
        """Apply TCP backpressure *before* the provider emits its next event.

        This runs in the provider receive task; the guarded observer remains
        synchronous, so there is no task per audio item or duplicate queue.
        """
        if self.closing:
            raise asyncio.CancelledError()
        size = len(event.get("data") or "") if event.get("type") == "audio" else 0
        duration = 0
        if size:
            match = re.search(r"rate=(\d+)", str(event.get("mime_type") or ""))
            rate = int(match[1]) if match else 24000
            if rate not in (16000, 24000, 48000):
                raise invalid("OUTPUT", "Unsupported provider PCM rate")
            duration = (size * 3 // 4) * 1_000_000 // (rate * 2)
        if size > 262144 or duration > 2_000_000:
            raise invalid("OUTPUT", "Provider audio is larger than archive admission bound")
        started = time.monotonic()
        while True:
            if self.closing:
                raise asyncio.CancelledError()
            if self.error:
                raise self.error
            if self.task.done():
                raise invalid("OUTPUT_CLOSED", "Output archive worker is no longer active")
            if (not self.queue.full() and self.queued_bytes + size <= 262144
                    and self.queued_duration_us + duration <= 2_000_000):
                waited = round((time.monotonic() - started) * 1000)
                if waited >= 15:
                    self.host.diagnostic(self.session, "archive_admission_wait",
                        archive_wait_ms=waited, queue_bytes=self.queued_bytes,
                        queue_items=self.queue.qsize())
                return
            self.admission_changed.clear()
            remaining = 5 - (time.monotonic() - started)
            if remaining <= 0:
                break
            try:
                await asyncio.wait_for(self.admission_changed.wait(), remaining)
            except asyncio.TimeoutError:
                break
        self._fail(invalid("OUTPUT_ARCHIVE_STALL", "Provider output archive did not drain in time"))
        raise self.error

    def enqueue(self, event):
        if self.closing or self.error:
            return
        if event.get("type") == "interrupted":
            self.cancelled_responses[self.session.response_seq] = True
            while len(self.cancelled_responses) > 32:
                self.cancelled_responses.popitem(last=False)
        size = len(event.get("data") or "") if event.get("type") == "audio" else 0
        duration = 0
        if size:
            match = re.search(r"rate=(\d+)", str(event.get("mime_type") or ""))
            rate = int(match[1]) if match else 24000
            if rate not in (16000, 24000, 48000):
                self._fail(invalid("OUTPUT", "Unsupported provider PCM rate")); return
            duration = (size * 3 // 4) * 1_000_000 // (rate * 2)
        # Includes the item currently committing. This is a RAM/work bound,
        # not a promise that the oldest volatile sample is at most two seconds.
        if self.queue.full() or self.queued_bytes + size > 262144 or self.queued_duration_us + duration > 2_000_000:
            self._fail(invalid("OUTPUT_BACKPRESSURE", "The bounded original output archive queue is full")); return
        self.queued_bytes += size
        self.queued_duration_us += duration
        self.queue.put_nowait((event, size, duration))

    def _fail(self, error):
        if self.error:
            return
        self.error = error
        self.changed.set()
        self.admission_changed.set()
        self.host.diagnostic(self.session, "output_archive_failed", code=getattr(error, "code", "LIVE_AUDIO_OUTPUT_STORE"))
        # No audio can be advertised as durable after storage/codec failure.
        # This task is separate from the archive worker so shutdown can join it.
        if not self.session.closed:
            asyncio.create_task(self.host.stop(session_id=self.session.id, resource_id=self.session.resource_id,
                                               actor=self.session.actor))

    async def _run(self):
        try:
            while True:
                item = await self.queue.get()
                if item is None:
                    self.queue.task_done()
                    break
                event, size, duration = item
                started = time.monotonic()
                self._last_encode_ms = self._last_commit_ms = 0
                try:
                    outputs = await self._process(event)
                    seq = int(event["seq"])
                    self.results[seq] = outputs
                    self.result_bytes += sum(len(c.chunk.payload) for output in outputs for c in output.chunks)
                    while len(self.results) > OUTPUT_CACHE_EVENTS or self.result_bytes > OUTPUT_CACHE_BYTES:
                        _, old = self.results.popitem(last=False)
                        self.result_bytes -= sum(len(c.chunk.payload) for output in old for c in output.chunks)
                    self.processed_seq = seq
                    self.changed.set()
                    if event.get("type") == "audio":
                        self._audio_commits += 1
                        elapsed = round((time.monotonic() - started) * 1000)
                        if self.debug_extended or self._audio_commits == 1 or elapsed >= 250 or self._audio_commits % 16 == 0:
                            self.host.diagnostic(self.session, "archive_event_committed",
                                event_seq=seq, pcm_bytes=size * 3 // 4,
                                duration_ms=duration // 1000, process_ms=elapsed,
                                encode_ms=self._last_encode_ms, commit_ms=self._last_commit_ms,
                                queue_items=self.queue.qsize(), queue_bytes=self.queued_bytes)
                finally:
                    self.queued_bytes -= size
                    self.queued_duration_us -= duration
                    self.queue.task_done()
                    self.admission_changed.set()
            # Graceful Stop/process shutdown commits the final partial codec
            # packet. A hard process kill retains the already committed prefix.
            await self._finish(cancelled=self.session.stop_reason == "explicit_stop")
        except asyncio.CancelledError:
            raise
        except Exception as error:
            self._fail(error)
        finally:
            if self.encoder:
                self.encoder.close()
                self.encoder = None
            self.changed.set()

    async def for_event(self, event):
        seq = int(event.get("seq") or 0)
        while self.processed_seq < seq and not self.error and not self.task.done():
            self.changed.clear()
            if self.processed_seq < seq:
                await self.changed.wait()
        if self.error:
            raise self.error
        if event.get("type") in ("audio", "turn_complete", "closed", "interrupted") and seq not in self.results:
            raise invalid("OUTPUT_HISTORY", "The event is outside the bounded archive delivery cache; resume its durable source")
        return self.results.get(seq, ())

    def is_cancelled(self, response_id):
        return response_id in self.cancelled_responses

    def owns_open_source(self, source_id):
        return (self.active_source_id == source_id
                and not self.error and not self.task.done())

    async def _process(self, event):
        kind = event.get("type")
        if kind == "audio":
            try:
                raw = base64.b64decode(event.get("data", ""), validate=True)
                match = re.search(r"rate=(\d+)", str(event.get("mime_type") or ""))
                rate = int(match[1]) if match else 24000
                response = integer(event.get("response_id"), 1, 0xFFFFFFFF, "response_id")
                seq = integer(event.get("seq"), 1, MAX_SAFE_INTEGER, "event_seq")
            except (ValueError, TypeError):
                raise invalid("OUTPUT", "Invalid provider PCM metadata") from None
            if not raw or len(raw) % 2 or len(raw) > 262144 or rate not in (16000, 24000, 48000):
                raise invalid("OUTPUT", "Provider PCM exceeds its bound")
            media_start = integer(event.get("audio_start_us", 0), 0, MAX_SAFE_INTEGER, "audio_start_us")
            if self.encoder is None:
                if media_start:
                    raise invalid("OUTPUT_HISTORY", "Cannot invent a codec state for a missing response prefix")
                descriptor = SourceDescriptor(f"out_{self.session.id}_{response}", 0)
                self.active_source_id = descriptor.source_id
                self.source = await self.store.open_source(descriptor)
                if not isinstance(self.source, BoundSource) or self.source.source != descriptor or self.source.server_durable_seq:
                    raise invalid("OUTPUT_IDENTITY", "An existing output source must be replayed from its original bytes")
                self.encoder = await self.work.run(self.services.codecs.encoder, self.profile, f"out_{response}", sample_rate=rate)
                self.epoch = self.encoder.epoch
                await self.store.register_epoch(self.source, self.epoch)
                self.response_id, self.rate = response, rate
                self.chunk_seq = self.end_us = 0
            elif response != self.response_id or rate != self.rate:
                raise invalid("OUTPUT_SEQUENCE", "A response must end its single output codec epoch before a new response or PCM rate")
            self.last_audio_event = seq
            t0 = time.monotonic()
            encoded = await self.work.run(self.encoder.encode, raw, sample_rate=rate)
            self._last_encode_ms += round((time.monotonic() - t0) * 1000)
            output = await self._commit(encoded)
            return (output,) if output.chunks else ()
        if kind in ("turn_complete", "closed", "interrupted"):
            if kind == "interrupted" and self.encoder is None and self.source is not None:
                # Playback can still be in progress after provider EOS. Preserve
                # that cancellation across restart instead of reviving the tail.
                await self.store.cancel_output(self.source)
            output = await self._finish(cancelled=kind == "interrupted")
            return (output,) if output else ()
        return ()

    async def _commit(self, encoded, *, cancelled=False):
        chunks = []
        for payload, duration, ended in encoded:
            chunk = EncodedChunk(self.source.source.source_id, self.epoch.epoch_id, self.chunk_seq + 1,
                                 self.end_us, duration, EPOCH_END if ended else 0, payload)
            original = OutputChunk(chunk, self.last_audio_event)
            t0 = time.monotonic()
            receipt = await self.store.commit_output(self.source, self.epoch, original)
            self._last_commit_ms += round((time.monotonic() - t0) * 1000)
            receipt.validate_for(self.source, chunk)
            if receipt.server_durable_seq != chunk.chunk_seq:
                raise invalid("DURABILITY", "Output commit did not extend its contiguous durable prefix")
            self.chunk_seq = chunk.chunk_seq
            self.end_us += duration
            self.source = BoundSource(self.source.source, self.source.server_source_id, receipt.server_durable_seq)
            chunks.append(original)
        return ArchivedOutput(self.source, self.epoch, self.response_id, tuple(chunks), cancelled)

    async def _finish(self, *, cancelled=False):
        if self.encoder is None:
            return None
        encoded = await self.work.run(self.encoder.encode, b"", sample_rate=self.rate, end=True)
        output = await self._commit(encoded, cancelled=cancelled)
        receipt = await self.store.seal_source(self.source, self.chunk_seq, self.end_us)
        if not isinstance(receipt, dict) or receipt.get("complete") is not True or receipt.get("server_durable_seq") != self.chunk_seq:
            raise invalid("DURABILITY", "Original output tail has no complete durable source receipt")
        if cancelled:
            await self.store.cancel_output(self.source)
        self.encoder.close()
        self.encoder = None
        self.active_source_id = None
        return output

    async def close(self):
        if not self.closing:
            self.closing = True
            self.admission_changed.set()
            if not self.task.done():
                await self.queue.put(None)
        if asyncio.current_task() is not self.task:
            try:
                await asyncio.wait_for(asyncio.shield(self.task), 5.0)
            except asyncio.TimeoutError:
                self.task.cancel()
                await asyncio.gather(self.task, return_exceptions=True)
                self._fail(invalid("OUTPUT_FLUSH_TIMEOUT", "Original output tail did not finish its bounded shutdown flush"))

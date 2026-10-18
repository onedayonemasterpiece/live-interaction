"""Bounded stateful audio codecs, using the optional pinned PyAV native wheel.

No subprocess, pathname decoder, network input or automatic format probing.
The small Ogg/ADTS reader validates framing and decoded work before native decode.
Original headers and chunks remain untouched in the product's durable store.
"""
from __future__ import annotations

import secrets
import struct
from fractions import Fraction

from .audio_v2 import (AAC_PROFILE, OPUS_PROFILE, CodecEpoch, EncodedChunk, EPOCH_END,
                       DecodedPCM, invalid)

_CRC_TABLE = []
for _i in range(256):
    _v = _i << 24
    for _ in range(8):
        _v = ((_v << 1) ^ (0x04C11DB7 if _v & 0x80000000 else 0)) & 0xFFFFFFFF
    _CRC_TABLE.append(_v)


def ogg_crc(data: bytes) -> int:
    crc = 0
    for value in data:
        crc = ((crc << 8) & 0xFFFFFFFF) ^ _CRC_TABLE[((crc >> 24) ^ value) & 255]
    return crc


def ogg_page(packets: list[bytes], *, serial: int, sequence: int, granule: int, flags: int = 0) -> bytes:
    lacing = bytearray()
    for packet in packets:
        if not packet or len(packet) > 8192:
            raise invalid("OGG", "Invalid bounded Ogg packet")
        lacing.extend([255] * (len(packet) // 255))
        lacing.append(len(packet) % 255)
    if len(lacing) > 255:
        raise invalid("OGG", "Too many Ogg packet segments")
    page = bytearray(struct.pack("<4sBBQIIIB", b"OggS", 0, flags, granule, serial, sequence, 0, len(lacing)))
    page.extend(lacing)
    page.extend(b"".join(packets))
    struct.pack_into("<I", page, 22, ogg_crc(page))
    return bytes(page)


def opus_samples(packet: bytes) -> int:
    """RFC 6716 TOC duration, in the Ogg Opus 48 kHz clock (max 120 ms)."""
    if not packet or len(packet) > 8192:
        raise invalid("OPUS", "Invalid Opus packet size")
    config, code = packet[0] >> 3, packet[0] & 3
    size = (480, 960, 1920, 2880)[config & 3] if config < 12 else (
        (480, 960)[config & 1] if config < 16 else (120, 240, 480, 960)[config & 3])
    if code == 3:
        if len(packet) < 2:
            raise invalid("OPUS", "Truncated Opus count")
        count = packet[1] & 63
    else:
        count = 1 if code == 0 else 2
    if not count or size * count > 5760:
        raise invalid("OPUS", "Opus packet duration exceeds 120 ms")
    return size * count


class OggOpusReader:
    """One logical Ogg stream; whole pages per chunk, packet continuation allowed."""
    def __init__(self, epoch: CodecEpoch):
        self.serial = None
        self.sequence = 0
        self.partial = bytearray()
        self.ended = False
        self.coded_samples = 0
        self.valid_samples = 0
        self.pre_skip = epoch.pre_skip
        headers, _ = self._pages(epoch.init, headers=True)
        if len(headers) != 2 or len(headers[0]) != 19 or not headers[0].startswith(b"OpusHead") or not headers[1].startswith(b"OpusTags"):
            raise invalid("OGG", "Exactly two original Ogg Opus headers are required")
        head = headers[0]
        if head[8] != 1 or head[9] != 1 or head[18] != 0 or struct.unpack_from("<H", head, 10)[0] != epoch.pre_skip:
            raise invalid("OGG", "Opus header does not match the immutable epoch")
        if self.partial or self.ended:
            raise invalid("OGG", "Incomplete or ended Ogg initialization")
        self.head = head

    def _pages(self, data: bytes, *, headers=False):
        offset = 0
        packets = []
        previous_valid = self.valid_samples
        work = 0
        pages = 0
        while offset < len(data):
            if self.ended or len(data) - offset < 27:
                raise invalid("OGG", "Truncated page or bytes after Ogg EOS")
            magic, version, flags, granule, serial, seq, crc, count = struct.unpack_from("<4sBBQIIIB", data, offset)
            if magic != b"OggS" or version or flags & ~7 or offset + 27 + count > len(data):
                raise invalid("OGG", "Invalid Ogg page header")
            lacing = data[offset + 27:offset + 27 + count]
            end = offset + 27 + count + sum(lacing)
            if end > len(data):
                raise invalid("OGG", "v2 chunks must contain complete Ogg pages")
            page = bytearray(data[offset:end]); page[22:26] = b"\0" * 4
            if ogg_crc(page) != crc:
                raise invalid("OGG", "Ogg checksum mismatch")
            if self.serial is None:
                if seq != 0 or flags != 2:
                    raise invalid("OGG", "Missing Ogg beginning of stream")
                self.serial = serial
            elif serial != self.serial or flags & 2:
                raise invalid("OGG", "Ogg logical stream changed inside an epoch")
            if seq != self.sequence or bool(flags & 1) != bool(self.partial):
                raise invalid("OGG", "Ogg page sequence or continuation mismatch")
            self.sequence = (self.sequence + 1) & 0xFFFFFFFF
            cursor = offset + 27 + count
            completed = 0
            for size in lacing:
                self.partial.extend(data[cursor:cursor + size]); cursor += size
                if len(self.partial) > 8192:
                    raise invalid("OGG", "Continued Opus packet exceeds its bound")
                if size < 255:
                    packet = bytes(self.partial); self.partial.clear()
                    if not packet:
                        raise invalid("OGG", "Empty Opus packet")
                    if not headers:
                        samples = opus_samples(packet)
                        work += samples
                        if work > 11_520:
                            raise invalid("DURATION", "Encoded Opus work exceeds 240 ms")
                        self.coded_samples += samples
                    packets.append(packet); completed += 1
                    if len(packets) > 96:
                        raise invalid("OGG", "Ogg packet count exceeds its bound")
            if headers:
                if granule != 0 or flags & 4 or len(packets) > 2:
                    raise invalid("OGG", "Invalid initialization granule")
            elif completed:
                if granule > self.coded_samples or granule < self.valid_samples + self.pre_skip:
                    # The first page may still be wholly inside codec pre-skip.
                    if not (self.valid_samples == 0 and granule <= self.pre_skip and granule <= self.coded_samples):
                        raise invalid("OGG", "Invalid Opus granule")
                if not flags & 4 and granule != self.coded_samples:
                    raise invalid("OGG", "Nonterminal Opus granule does not match packet duration")
                self.valid_samples = max(0, granule - self.pre_skip)
            elif granule != 0xFFFFFFFFFFFFFFFF:
                raise invalid("OGG", "Ogg page without a completed packet needs the unset granule")
            if flags & 4:
                if self.partial:
                    raise invalid("OGG", "Ogg EOS contains an unfinished packet")
                self.ended = True
            offset = end
            pages += 1
            if pages > 64:
                raise invalid("OGG", "Ogg page count exceeds its bound")
        return packets, self.valid_samples - previous_valid

    def packets(self, chunk: EncodedChunk):
        packets, valid = self._pages(chunk.payload)
        if self.ended != bool(chunk.flags & EPOCH_END):
            raise invalid("OGG", "Epoch end and Ogg EOS disagree")
        if abs(valid * 1_000_000 - chunk.duration_us * 48000) > 48000:
            raise invalid("DURATION", "Declared duration does not match valid Opus samples")
        return packets, self.valid_samples


_AAC_RATES = (96000, 88200, 64000, 48000, 44100, 32000, 24000, 22050, 16000, 12000, 11025, 8000, 7350)


def adts_packets(payload: bytes, sample_rate: int):
    offset, work = 0, 0
    result = []
    while offset < len(payload):
        if len(payload) - offset < 7:
            raise invalid("ADTS", "Truncated ADTS header")
        h = payload[offset:offset + 7]
        index, channels = (h[2] >> 2) & 15, ((h[2] & 1) << 2) | (h[3] >> 6)
        length = ((h[3] & 3) << 11) | (h[4] << 3) | (h[5] >> 5)
        header = 7 if h[1] & 1 else 9
        if (h[0] != 255 or h[1] & 0xF6 != 0xF0 or h[2] >> 6 != 1 or index >= len(_AAC_RATES)
                or _AAC_RATES[index] != sample_rate or channels != 1 or h[6] & 3
                or length <= header or offset + length > len(payload)):
            raise invalid("ADTS", "Only complete mono AAC-LC ADTS frames in the declared clock are supported")
        work += 1024
        if work * 1_000_000 > sample_rate * 240_000:
            raise invalid("DURATION", "Encoded AAC work exceeds 240 ms")
        # avcodec receives a raw AAC packet plus the AudioSpecificConfig derived
        # from the checked ADTS header; transport bytes remain stored unchanged.
        result.append(payload[offset + header:offset + length])
        offset += length
    return result, work


class PyAVDecoder:
    def __init__(self, av, epoch: CodecEpoch):
        self.av, self.epoch = av, epoch
        self.reader = OggOpusReader(epoch) if epoch.codec == "opus" else None
        self.context = av.CodecContext.create(epoch.codec, "r")
        self.context.thread_count = 1
        if self.reader:
            # Apply pre-skip exactly once ourselves, after stateful resampling.
            self.context.extradata = self.reader.head[:10] + b"\0\0" + self.reader.head[12:]
        else:
            index = _AAC_RATES.index(epoch.clock_rate)
            self.context.extradata = struct.pack("!H", (2 << 11) | (index << 7) | (1 << 3))
        self.context.open()
        self.resampler = av.AudioResampler(format="s16", layout="mono", rate=epoch.decoded_sample_rate)
        self.skip = round(epoch.pre_skip * epoch.decoded_sample_rate / epoch.clock_rate)
        self.emitted = 0
        self.coded = 0
        self.valid = 0
        self.origin = None
        self.next_capture_us = None
        self.last_seq = None
        self.ended = False

    def decode(self, chunk: EncodedChunk) -> DecodedPCM:
        if self.ended or chunk.epoch_id != self.epoch.epoch_id or (self.last_seq is not None and chunk.chunk_seq != self.last_seq + 1):
            raise invalid("CODEC_SEQUENCE", "Decoder requires ordered chunks of one epoch")
        if self.next_capture_us is not None and abs(chunk.capture_start_us - self.next_capture_us) > 1:
            raise invalid("TIMELINE", "Capture timeline changed inside an epoch")
        if self.origin is None:
            self.origin = chunk.capture_start_us
        if self.reader:
            packets, total = self.reader.packets(chunk)
            valid = round(total * self.epoch.decoded_sample_rate / 48000)
        else:
            packets, work = adts_packets(chunk.payload, self.epoch.clock_rate)
            self.coded += work
            available = max(0, self.coded - self.epoch.pre_skip)
            declared = round(chunk.duration_us * self.epoch.clock_rate / 1_000_000)
            if not chunk.flags & EPOCH_END and declared != available - self.valid:
                raise invalid("DURATION", "Declared AAC duration does not match valid samples")
            if declared < 0 or declared > available - self.valid:
                raise invalid("DURATION", "Invalid AAC terminal trim")
            valid = self.valid + declared
        self.valid = valid
        # All structural/duration checks above run before entering the native
        # codec. A single context is used in order; callers serialize it.
        output = bytearray()
        try:
            for packet in packets:
                for frame in self.context.decode(self.av.Packet(packet)):
                    if frame.samples > 5760 or len(frame.layout.channels) != 1:
                        raise invalid("DECODE", "Native decoder exceeded its admitted work")
                    for pcm in self.resampler.resample(frame):
                        output.extend(bytes(pcm.planes[0])[:pcm.samples * 2])
            if chunk.flags & EPOCH_END:
                for frame in self.context.decode(None):
                    for pcm in self.resampler.resample(frame):
                        output.extend(bytes(pcm.planes[0])[:pcm.samples * 2])
                for pcm in self.resampler.resample(None):
                    output.extend(bytes(pcm.planes[0])[:pcm.samples * 2])
                self.ended = True
        except Exception as exc:
            if hasattr(exc, "code"):
                raise
            raise invalid("DECODE", "Native audio decoder rejected the chunk") from None
        drop = min(self.skip, len(output) // 2)
        del output[:drop * 2]; self.skip -= drop
        allowed = max(0, self.valid - self.emitted)
        if len(output) // 2 > allowed:
            if not self.ended:
                raise invalid("DECODE", "Decoded audio exceeds its valid duration")
            del output[allowed * 2:]
        if len(output) > self.epoch.decoded_sample_rate * 2 * 241 // 1000:
            raise invalid("DECODE", "Decoded PCM exceeds the bounded frame plus resampler tail")
        position = self.origin + round(self.emitted * 1_000_000 / self.epoch.decoded_sample_rate)
        self.emitted += len(output) // 2
        if self.ended and self.emitted != self.valid:
            raise invalid("DECODE", "Codec ended without its complete valid tail")
        self.next_capture_us = chunk.capture_start_us + chunk.duration_us
        self.last_seq = chunk.chunk_seq
        return DecodedPCM(bytes(output), position)

    def close(self):
        self.ended = True
        self.context = None
        self.resampler = None


class PyAVOpusEncoder:
    def __init__(self, av, epoch_id: str, sample_rate: int):
        if sample_rate not in (16000, 24000, 48000):
            raise invalid("CODEC", "Unsupported provider PCM rate")
        self.av, self.sample_rate = av, sample_rate
        self.context = av.CodecContext.create("libopus", "w")
        self.context.sample_rate, self.context.layout, self.context.format = sample_rate, "mono", "s16"
        self.context.bit_rate = 32000
        self.context.time_base = Fraction(1, sample_rate)
        self.context.thread_count = 1
        self.context.options = {"application": "audio", "frame_duration": "20", "compression_level": "5"}
        self.context.open()
        head = bytes(self.context.extradata or b"")
        if len(head) != 19 or not head.startswith(b"OpusHead"):
            raise invalid("CODEC", "Native encoder did not provide Opus initialization")
        self.pre_skip = struct.unpack_from("<H", head, 10)[0]
        self.serial, self.sequence = secrets.randbits(32), 2
        tags = b"OpusTags" + struct.pack("<I", 16) + b"live-interaction" + struct.pack("<I", 0)
        init = ogg_page([head], serial=self.serial, sequence=0, granule=0, flags=2) + ogg_page([tags], serial=self.serial, sequence=1, granule=0)
        self.epoch = CodecEpoch(epoch_id, "opus", "ogg", 1, 1, 48000, 48000, self.pre_skip, init)
        self.buffer = bytearray()
        self.packets = []
        self.pts, self.granule, self.valid = 0, 0, 0
        self.ended = False

    def _page(self, end=False):
        payload = ogg_page(self.packets, serial=self.serial, sequence=self.sequence, granule=self.granule, flags=4 if end else 0)
        self.sequence = (self.sequence + 1) & 0xFFFFFFFF
        current = max(0, self.granule - self.pre_skip)
        duration = round((current - self.valid) * 1_000_000 / 48000)
        self.valid = current
        self.packets.clear()
        if not 0 <= duration <= 240_000 or len(payload) > 8192:
            raise invalid("ENCODE", "Native encoded output exceeds its bound")
        return payload, duration, end

    def encode(self, pcm16: bytes, *, sample_rate: int, end=False):
        if self.ended or sample_rate != self.sample_rate or not isinstance(pcm16, bytes) or len(pcm16) % 2 or len(pcm16) > 262144:
            raise invalid("ENCODE", "Invalid PCM input or closed encoder")
        self.buffer.extend(pcm16)
        result = []
        size = self.context.frame_size * 2
        while len(self.buffer) >= size or (end and self.buffer):
            count = min(size, len(self.buffer))
            frame = self.av.AudioFrame(format="s16", layout="mono", samples=count // 2)
            frame.sample_rate, frame.pts, frame.time_base = sample_rate, self.pts, Fraction(1, sample_rate)
            frame.planes[0].update(bytes(self.buffer[:count])); del self.buffer[:count]
            self.pts += count // 2
            for packet in self.context.encode(frame):
                self.packets.append(bytes(packet))
                self.granule = round((packet.pts + packet.duration) * 48000 / sample_rate) + self.pre_skip
                if len(self.packets) == 5:
                    result.append(self._page())
        if end:
            for packet in self.context.encode(None):
                self.packets.append(bytes(packet))
                self.granule = round((packet.pts + packet.duration) * 48000 / sample_rate) + self.pre_skip
            if self.packets:
                result.append(self._page(True))
            self.ended = True
        return result

    def close(self):
        self.ended = True
        self.context = None
        self.buffer.clear()
        self.packets.clear()


class PyAVAudioCodecFactory:
    def __init__(self):
        try:
            import av
        except ImportError:
            av = None
        self.av = av

    def profiles(self, direction):
        if self.av is None:
            return ()
        profiles = []
        for profile, codec, mode in ((OPUS_PROFILE, "opus" if direction == "uplink" else "libopus", "r" if direction == "uplink" else "w"),
                                     (AAC_PROFILE, "aac", "r")):
            if direction == "downlink" and profile == AAC_PROFILE:
                continue
            try:
                self.av.Codec(codec, mode)
                profiles.append(profile)
            except (ValueError, RuntimeError):
                pass
        return tuple(profiles)

    def decoder(self, epoch):
        if epoch.profile not in self.profiles("uplink") or epoch.decoded_sample_rate != 16000:
            raise invalid("CODEC_UNAVAILABLE", "This compressed uplink profile is unavailable")
        return PyAVDecoder(self.av, epoch)

    def encoder(self, profile, epoch_id, *, sample_rate):
        if profile not in self.profiles("downlink"):
            raise invalid("CODEC_UNAVAILABLE", "This compressed downlink profile is unavailable")
        return PyAVOpusEncoder(self.av, epoch_id, sample_rate)

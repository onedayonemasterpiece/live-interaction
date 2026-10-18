"""Typed, bounded wl-live-v2 wire and durable-source integration contract.

This module owns no product store, provider or credentials. Transport references
are deliberately absent from durable chunk identity: a retry may use a new
socket, reference table or recovery lane without changing the captured source.
"""
from __future__ import annotations

import base64
import hashlib
import re
import struct
from dataclasses import dataclass
from typing import Any, Callable, Literal, Protocol, TypedDict

from .session_host import LiveError

PROTOCOL_V2 = "wl-live-v2"
UPLINK_MAGIC = b"WLA2"
DOWNLINK_MAGIC = b"WLO2"
HEADER = struct.Struct("!4sBBHIIQQIIIIQ")
HEADER_BYTES = 56
MAX_SAFE_INTEGER = (1 << 53) - 1
RECOVERY = 1
PORTION_START = 2
PORTION_END = 4
EPOCH_END = 8
EVENT_END = 16
SOURCE_FLAGS = PORTION_START | PORTION_END | EPOCH_END
OPUS_PROFILE = "opus/ogg;channels=1"
AAC_PROFILE = "aac/adts;rate=16000;channels=1"
_ID = re.compile(r"[A-Za-z0-9][A-Za-z0-9_.:-]{0,95}\Z")


def invalid(code: str, message: str) -> LiveError:
    return LiveError("LIVE_AUDIO_" + code, message)


def integer(value: Any, low: int, high: int, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or not low <= value <= high:
        raise invalid("METADATA", f"Invalid {name}")
    return value


def identifier(value: Any, name: str) -> str:
    if not isinstance(value, str) or not _ID.fullmatch(value):
        raise invalid("METADATA", f"Invalid {name}")
    return value


@dataclass(frozen=True)
class AudioLimits:
    max_encoded_bytes: int = 8192
    max_duration_us: int = 240_000
    max_init_bytes: int = 8192
    live_window_bytes: int = 32_768
    live_window_duration_us: int = 960_000
    live_max_age_ms: int = 2500
    recovery_window_bytes: int = 16_384
    recovery_window_duration_us: int = 480_000
    max_sources: int = 9
    max_epochs: int = 32
    # Work admission is audio duration, not the compressed byte count. Separate
    # lanes stop a small highly compressed recovery stream monopolising decode.
    live_decode_us_per_second: int = 1_500_000
    recovery_decode_us_per_second: int = 250_000
    recovery_ingest_us_per_second: int = 4_000_000
    output_window_bytes: int = 32_768
    output_window_duration_us: int = 960_000
    output_recovery_window_bytes: int = 16_384
    output_recovery_window_duration_us: int = 480_000
    output_recovery_us_per_second: int = 4_000_000

    def __post_init__(self):
        for key, value in vars(self).items():
            integer(value, 1, MAX_SAFE_INTEGER, key)
        if self.max_encoded_bytes > 65_536 or self.max_duration_us > 240_000 or self.max_init_bytes > 16_384:
            raise invalid("LIMIT", "Codec frame bounds exceed the v2 hard ceiling")

    def as_dict(self):
        return dict(vars(self))


@dataclass(frozen=True)
class SourceDescriptor:
    source_id: str
    started_at_unix_ms: int

    def __post_init__(self):
        identifier(self.source_id, "source_id")
        integer(self.started_at_unix_ms, 0, MAX_SAFE_INTEGER, "started_at_unix_ms")

    @classmethod
    def from_dict(cls, value):
        if not isinstance(value, dict):
            raise invalid("SOURCE", "Source descriptor must be an object")
        return cls(value.get("source_id"), value.get("started_at_unix_ms"))


@dataclass(frozen=True)
class CodecEpoch:
    epoch_id: str
    codec: Literal["opus", "aac"]
    container: Literal["ogg", "adts"]
    version: int
    channels: int
    decoded_sample_rate: int
    clock_rate: int
    pre_skip: int
    init: bytes

    def __post_init__(self):
        identifier(self.epoch_id, "epoch_id")
        integer(self.version, 1, 1, "codec version")
        integer(self.channels, 1, 1, "channels")
        if self.decoded_sample_rate not in (16000, 24000, 48000) or isinstance(self.decoded_sample_rate, bool):
            raise invalid("CODEC", "Unsupported decoded PCM rate")
        if not isinstance(self.init, bytes) or len(self.init) > 8192:
            raise invalid("CODEC", "Decoder initialization exceeds its bound")
        integer(self.pre_skip, 0, 65535, "pre_skip")
        if (self.codec, self.container) == ("opus", "ogg"):
            if self.clock_rate != 48000:
                raise invalid("CODEC", "Ogg Opus uses a 48 kHz clock")
            if not self.init:
                raise invalid("CODEC", "Original Ogg Opus headers are required")
        elif (self.codec, self.container) == ("aac", "adts"):
            if self.clock_rate != self.decoded_sample_rate or self.init:
                raise invalid("CODEC", "AAC-LC ADTS uses its declared sample clock and in-band headers")
        else:
            raise invalid("CODEC", "Unsupported compressed codec/container")

    @property
    def profile(self) -> str:
        return OPUS_PROFILE if self.codec == "opus" else f"aac/adts;rate={self.decoded_sample_rate};channels=1"

    def as_dict(self):
        return {**{key: value for key, value in vars(self).items() if key != "init"},
                "init_base64": base64.b64encode(self.init).decode("ascii")}

    @classmethod
    def from_dict(cls, value):
        if not isinstance(value, dict):
            raise invalid("CODEC", "Codec epoch must be an object")
        encoded = value.get("init_base64", "")
        if not isinstance(encoded, str) or len(encoded) > 10_924:
            raise invalid("CODEC", "Decoder initialization exceeds its bound")
        try:
            init = base64.b64decode(encoded, validate=True)
        except (ValueError, TypeError):
            raise invalid("CODEC", "Invalid decoder initialization") from None
        return cls(*(value.get(key) for key in (
            "epoch_id", "codec", "container", "version", "channels", "decoded_sample_rate", "clock_rate", "pre_skip"
        )), init)


@dataclass(frozen=True)
class EncodedChunk:
    source_id: str
    epoch_id: str
    chunk_seq: int
    capture_start_us: int
    duration_us: int
    flags: int
    payload: bytes

    def __post_init__(self):
        identifier(self.source_id, "source_id")
        identifier(self.epoch_id, "epoch_id")
        integer(self.chunk_seq, 1, MAX_SAFE_INTEGER, "chunk_seq")
        integer(self.capture_start_us, 0, MAX_SAFE_INTEGER, "capture_start_us")
        integer(self.duration_us, 0, 240_000, "duration_us")
        integer(self.flags, 0, SOURCE_FLAGS, "source flags")
        if self.flags & ~SOURCE_FLAGS:
            raise invalid("FRAME", "Transport flags are not durable source metadata")
        if not isinstance(self.payload, bytes) or not 1 <= len(self.payload) <= 8192:
            raise invalid("FRAME", "Encoded payload exceeds its bound")
        if not self.duration_us and not (self.flags & EPOCH_END):
            raise invalid("FRAME", "Only an epoch tail may have zero valid duration")

    @property
    def payload_sha256(self) -> str:
        return hashlib.sha256(self.payload).hexdigest()

    @property
    def identity(self) -> tuple:
        return (self.source_id, self.epoch_id, self.chunk_seq, self.capture_start_us,
                self.duration_us, self.flags, self.payload_sha256)


@dataclass(frozen=True)
class OutputChunk:
    """Original encoded output plus its original host event, across reconnects."""
    chunk: EncodedChunk
    original_event_seq: int

    def __post_init__(self):
        if not isinstance(self.chunk, EncodedChunk) or self.chunk.flags & ~EPOCH_END:
            raise invalid("OUTPUT", "Invalid original output chunk")
        integer(self.original_event_seq, 1, MAX_SAFE_INTEGER, "original_event_seq")


@dataclass(frozen=True)
class AudioFrame:
    magic: bytes
    flags: int
    stream_ref: int
    epoch_ref: int
    chunk_seq: int
    start_us: int
    duration_us: int
    delivery_generation: int
    event_seq: int
    payload: bytes

    def validate(self, limits: AudioLimits = AudioLimits()):
        if self.magic not in (UPLINK_MAGIC, DOWNLINK_MAGIC):
            raise invalid("FRAME", "Unknown audio frame magic")
        integer(self.flags, 0, 31, "flags")
        integer(self.stream_ref, 1, 0xFFFFFFFF, "stream_ref")
        integer(self.epoch_ref, 1, 0xFFFFFFFF, "epoch_ref")
        integer(self.chunk_seq, 1, MAX_SAFE_INTEGER, "chunk_seq")
        integer(self.start_us, 0, MAX_SAFE_INTEGER, "start_us")
        integer(self.duration_us, 0, limits.max_duration_us, "duration_us")
        integer(self.delivery_generation, 1, 0xFFFFFFFF, "delivery_generation")
        integer(self.event_seq, 0, MAX_SAFE_INTEGER, "event_seq")
        if self.magic == UPLINK_MAGIC and (self.event_seq or self.flags & EVENT_END):
            raise invalid("FRAME", "Output cursor flags are invalid on input")
        if self.magic == DOWNLINK_MAGIC and (self.flags & (RECOVERY | PORTION_START | PORTION_END) or not self.event_seq):
            raise invalid("FRAME", "Invalid output stream metadata")
        if not self.duration_us and not self.flags & EPOCH_END:
            raise invalid("FRAME", "Only an epoch tail may have zero valid duration")
        if not isinstance(self.payload, bytes) or not 1 <= len(self.payload) <= limits.max_encoded_bytes:
            raise invalid("FRAME", "Encoded payload exceeds its bound")
        return self

    def encode(self, limits: AudioLimits = AudioLimits()) -> bytes:
        self.validate(limits)
        return HEADER.pack(self.magic, self.flags, 0, HEADER_BYTES, self.stream_ref,
                           self.epoch_ref, self.chunk_seq, self.start_us, self.duration_us,
                           len(self.payload), self.delivery_generation, 0, self.event_seq) + self.payload

    @classmethod
    def decode(cls, data: bytes, limits: AudioLimits = AudioLimits()):
        if not isinstance(data, bytes) or not HEADER_BYTES < len(data) <= HEADER_BYTES + limits.max_encoded_bytes:
            raise invalid("FRAME", "Invalid encoded frame length")
        magic, flags, reserved, header_bytes, stream, epoch, seq, start, duration, size, generation, reserved2, event = HEADER.unpack_from(data)
        if reserved or reserved2 or header_bytes != HEADER_BYTES or size != len(data) - HEADER_BYTES:
            raise invalid("FRAME", "Invalid encoded frame header")
        return cls(magic, flags, stream, epoch, seq, start, duration, generation, event, data[HEADER_BYTES:]).validate(limits)


@dataclass(frozen=True)
class BoundSource:
    source: SourceDescriptor
    server_source_id: str
    server_durable_seq: int


@dataclass(frozen=True)
class OutputResume:
    source: BoundSource
    played_us: int
    cancelled: bool
    duration_us: int
    complete: bool

    def __post_init__(self):
        if not isinstance(self.source, BoundSource) or type(self.cancelled) is not bool or type(self.complete) is not bool:
            raise invalid("OUTPUT", "Invalid output recovery status")
        integer(self.played_us, 0, MAX_SAFE_INTEGER, "played_us")
        integer(self.duration_us, 0, MAX_SAFE_INTEGER, "duration_us")


@dataclass(frozen=True)
class DurableCommit:
    """Created by the server store after the atomic durable commit, never JSON."""
    source_id: str
    server_source_id: str
    epoch_id: str
    chunk_seq: int
    payload_sha256: str
    server_durable_seq: int
    newly_committed: bool

    def validate_for(self, source: BoundSource, chunk: EncodedChunk):
        if (self.source_id, self.server_source_id, self.epoch_id, self.chunk_seq, self.payload_sha256) != (
                chunk.source_id, source.server_source_id, chunk.epoch_id, chunk.chunk_seq, chunk.payload_sha256):
            raise invalid("DURABILITY", "Store receipt does not identify the committed chunk")
        integer(self.server_durable_seq, 0, MAX_SAFE_INTEGER, "server_durable_seq")
        if not isinstance(self.newly_committed, bool):
            raise invalid("DURABILITY", "Invalid store receipt")
        return self


@dataclass(frozen=True)
class DurablePCMProjection:
    commit: DurableCommit
    pcm16: bytes
    sample_rate: int
    capture_start_us: int

    def __post_init__(self):
        if not isinstance(self.commit, DurableCommit) or self.sample_rate != 16000:
            raise invalid("PROJECTION", "Provider projection requires a typed durable receipt and 16 kHz PCM")
        if not isinstance(self.pcm16, bytes) or not self.pcm16 or len(self.pcm16) % 2 or len(self.pcm16) > 8000:
            raise invalid("PROJECTION", "Decoded PCM projection exceeds its bound")
        integer(self.capture_start_us, 0, MAX_SAFE_INTEGER, "capture_start_us")


@dataclass(frozen=True)
class DecodedPCM:
    pcm16: bytes
    capture_start_us: int


class SourceSeal(TypedDict):
    server_durable_seq: int
    complete: bool


class EncodedAudioStore(Protocol):
    async def open_source(self, source: SourceDescriptor) -> BoundSource: ...
    async def register_epoch(self, source: BoundSource, epoch: CodecEpoch) -> None: ...
    async def commit_chunk(self, source: BoundSource, epoch: CodecEpoch, chunk: EncodedChunk) -> DurableCommit: ...
    async def seal_source(self, source: BoundSource, last_chunk_seq: int, capture_end_us: int) -> SourceSeal: ...


class EncodedOutputStore(EncodedAudioStore, Protocol):
    async def commit_output(self, source: BoundSource, epoch: CodecEpoch, output: OutputChunk) -> DurableCommit: ...
    async def resume_source(self, source_id: str) -> BoundSource: ...
    async def epoch(self, source: BoundSource, epoch_id: str) -> CodecEpoch: ...
    async def read_output(self, source: BoundSource, *, after_seq: int, max_encoded_bytes: int,
                          max_duration_us: int) -> list[OutputChunk]: ...
    async def mark_played(self, source: BoundSource, played_us: int) -> None: ...
    async def output_status(self, source: BoundSource) -> OutputResume: ...
    async def pending_outputs(self, *, limit: int = 8) -> list[OutputResume]: ...
    async def cancel_output(self, source: BoundSource) -> None: ...


class AudioDecoder(Protocol):
    def decode(self, chunk: EncodedChunk) -> DecodedPCM: ...
    def close(self) -> None: ...


class AudioEncoder(Protocol):
    epoch: CodecEpoch
    def encode(self, pcm16: bytes, *, sample_rate: int, end: bool = False) -> list[tuple[bytes, int, bool]]: ...
    def close(self) -> None: ...


class AudioCodecFactory(Protocol):
    def profiles(self, direction: Literal["uplink", "downlink"]) -> tuple[str, ...]: ...
    def decoder(self, epoch: CodecEpoch) -> AudioDecoder: ...
    def encoder(self, profile: str, epoch_id: str, *, sample_rate: int) -> AudioEncoder: ...


@dataclass(frozen=True)
class AudioV2Services:
    store_factory: Callable[[Any], EncodedAudioStore]
    codecs: AudioCodecFactory
    limits: AudioLimits = AudioLimits()
    output_store_factory: Callable[[Any], EncodedOutputStore] | None = None


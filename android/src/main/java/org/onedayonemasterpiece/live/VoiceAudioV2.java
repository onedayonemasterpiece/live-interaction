package org.onedayonemasterpiece.live;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Arrays;
import java.util.Objects;

/** Immutable source DTOs and the exact compressed wl-live-v2 binary frame. */
public final class VoiceAudioV2 {
    private VoiceAudioV2() { }
    public static final String PROTOCOL = "wl-live-v2";
    public static final String OPUS_PROFILE = "opus/ogg;channels=1";
    public static final String AAC_PROFILE = "aac/adts;rate=16000;channels=1";
    public static final long MAX_SAFE_INTEGER = (1L << 53) - 1;
    public static final int HEADER_BYTES = 56;
    public static final int INPUT_MAGIC = 0x574c4132;
    public static final int OUTPUT_MAGIC = 0x574c4f32;
    public static final int RECOVERY = 1, PORTION_START = 2, PORTION_END = 4, EPOCH_END = 8, EVENT_END = 16;
    public static final int SOURCE_FLAGS = PORTION_START | PORTION_END | EPOCH_END;
    public static final int MAX_ENCODED_BYTES = 8192, MAX_DURATION_US = 240000, MAX_INIT_BYTES = 8192;

    public static String id(String value) {
        if (value == null || !value.matches("[A-Za-z0-9][A-Za-z0-9_.:-]{0,95}")) throw invalid("METADATA");
        return value;
    }
    public static long integer(long value, long low, long high) {
        if (value < low || value > high) throw invalid("METADATA");
        return value;
    }
    public static IllegalArgumentException invalid(String code) { return new IllegalArgumentException("LIVE_AUDIO_" + code); }

    public record SourceDescriptor(String sourceId, long startedAtUnixMs) {
        public SourceDescriptor { id(sourceId); integer(startedAtUnixMs, 0, MAX_SAFE_INTEGER); }
    }
    public record CodecEpoch(String epochId, String codec, String container, int version, int channels,
                             int decodedSampleRate, int clockRate, int preSkip, byte[] init) {
        public CodecEpoch {
            id(epochId);
            if (version != 1 || channels != 1 || (decodedSampleRate != 16000 && decodedSampleRate != 24000 && decodedSampleRate != 48000)
                    || init == null || init.length > MAX_INIT_BYTES) throw invalid("CODEC");
            integer(preSkip, 0, 65535);
            if ("opus".equals(codec) && "ogg".equals(container)) {
                if (clockRate != 48000 || init.length == 0) throw invalid("CODEC");
            } else if ("aac".equals(codec) && "adts".equals(container)) {
                if (clockRate != decodedSampleRate || init.length != 0) throw invalid("CODEC");
            } else throw invalid("CODEC");
            init = init.clone();
        }
        @Override public byte[] init() { return init.clone(); }
        public String profile() { return "opus".equals(codec) ? OPUS_PROFILE : "aac/adts;rate=" + decodedSampleRate + ";channels=1"; }
        @Override public boolean equals(Object other) {
            return other instanceof CodecEpoch e && epochId.equals(e.epochId) && codec.equals(e.codec) && container.equals(e.container)
                && version == e.version && channels == e.channels && decodedSampleRate == e.decodedSampleRate
                && clockRate == e.clockRate && preSkip == e.preSkip && Arrays.equals(init, e.init);
        }
        @Override public int hashCode() {
            return 31 * Objects.hash(epochId, codec, container, version, channels, decodedSampleRate, clockRate, preSkip) + Arrays.hashCode(init);
        }
    }
    public record EncodedChunk(String sourceId, String epochId, long chunkSeq, long captureStartUs,
                               int durationUs, int flags, byte[] payload) {
        public EncodedChunk {
            id(sourceId); id(epochId); integer(chunkSeq, 1, MAX_SAFE_INTEGER); integer(captureStartUs, 0, MAX_SAFE_INTEGER);
            integer(durationUs, 0, MAX_DURATION_US);
            if ((flags & ~SOURCE_FLAGS) != 0 || flags < 0 || (durationUs == 0 && (flags & EPOCH_END) == 0)
                    || payload == null || payload.length == 0 || payload.length > MAX_ENCODED_BYTES) throw invalid("FRAME");
            payload = payload.clone();
        }
        @Override public byte[] payload() { return payload.clone(); }
        public int encodedBytes() { return payload.length; }
        public String payloadSha256() { return sha256(payload); }
        @Override public boolean equals(Object other) {
            return other instanceof EncodedChunk c && sourceId.equals(c.sourceId) && epochId.equals(c.epochId)
                && chunkSeq == c.chunkSeq && captureStartUs == c.captureStartUs && durationUs == c.durationUs
                && flags == c.flags && Arrays.equals(payload, c.payload);
        }
        @Override public int hashCode() {
            return 31 * Objects.hash(sourceId, epochId, chunkSeq, captureStartUs, durationUs, flags) + Arrays.hashCode(payload);
        }
    }
    public record DurablePrefix(String sourceId, long chunkSeq) {
        public DurablePrefix { id(sourceId); integer(chunkSeq, 0, MAX_SAFE_INTEGER); }
    }
    /** References are socket-local; source/epoch IDs and the bytes remain stable. */
    public record OutputBinding(String sourceId, String serverSourceId, long streamRef,
                                long providerResponseId, long epochRef, CodecEpoch epoch) {
        public OutputBinding {
            id(sourceId); id(serverSourceId); integer(streamRef, 1, 0xffffffffL);
            integer(providerResponseId, 1, 0xffffffffL); integer(epochRef, 1, 0xffffffffL);
            Objects.requireNonNull(epoch, "epoch");
        }
    }
    /** EVENT_END, delivery generation and transport references never enter this identity. */
    public record OutputChunk(EncodedChunk chunk, long originalEventSeq) {
        public OutputChunk {
            Objects.requireNonNull(chunk, "chunk"); integer(originalEventSeq, 1, MAX_SAFE_INTEGER);
            if ((chunk.flags() & ~EPOCH_END) != 0) throw invalid("OUTPUT");
        }
    }
    public record Frame(int magic, int flags, long streamRef, long epochRef, long chunkSeq,
                        long startUs, int durationUs, long deliveryGeneration, long eventSeq, byte[] payload) {
        public Frame {
            if (magic != INPUT_MAGIC && magic != OUTPUT_MAGIC) throw invalid("FRAME");
            integer(flags, 0, 31); integer(streamRef, 1, 0xffffffffL); integer(epochRef, 1, 0xffffffffL);
            integer(chunkSeq, 1, MAX_SAFE_INTEGER); integer(startUs, 0, MAX_SAFE_INTEGER);
            integer(durationUs, 0, MAX_DURATION_US); integer(deliveryGeneration, 1, 0xffffffffL); integer(eventSeq, 0, MAX_SAFE_INTEGER);
            if (magic == INPUT_MAGIC && (eventSeq != 0 || (flags & EVENT_END) != 0)) throw invalid("FRAME");
            if (magic == OUTPUT_MAGIC && (eventSeq == 0 || (flags & (RECOVERY | PORTION_START | PORTION_END)) != 0)) throw invalid("FRAME");
            if ((durationUs == 0 && (flags & EPOCH_END) == 0) || payload == null || payload.length == 0 || payload.length > MAX_ENCODED_BYTES) throw invalid("FRAME");
            payload = payload.clone();
        }
        @Override public byte[] payload() { return payload.clone(); }
        public byte[] encode() {
            return ByteBuffer.allocate(HEADER_BYTES + payload.length).order(ByteOrder.BIG_ENDIAN)
                .putInt(magic).put((byte) flags).put((byte) 0).putShort((short) HEADER_BYTES)
                .putInt((int) streamRef).putInt((int) epochRef).putLong(chunkSeq).putLong(startUs)
                .putInt(durationUs).putInt(payload.length).putInt((int) deliveryGeneration).putInt(0).putLong(eventSeq).put(payload).array();
        }
        public static Frame decode(byte[] data) {
            if (data == null || data.length <= HEADER_BYTES || data.length > HEADER_BYTES + MAX_ENCODED_BYTES) throw invalid("FRAME");
            ByteBuffer b = ByteBuffer.wrap(data).order(ByteOrder.BIG_ENDIAN);
            int magic = b.getInt(), flags = Byte.toUnsignedInt(b.get()), reserved = Byte.toUnsignedInt(b.get()), length = Short.toUnsignedInt(b.getShort());
            long stream = Integer.toUnsignedLong(b.getInt()), epoch = Integer.toUnsignedLong(b.getInt()), seq = b.getLong(), start = b.getLong();
            int duration = b.getInt(), bytes = b.getInt(); long generation = Integer.toUnsignedLong(b.getInt());
            int reserved2 = b.getInt(); long event = b.getLong();
            if (reserved != 0 || reserved2 != 0 || length != HEADER_BYTES || bytes != data.length - HEADER_BYTES) throw invalid("FRAME");
            byte[] payload = new byte[bytes]; b.get(payload);
            return new Frame(magic, flags, stream, epoch, seq, start, duration, generation, event, payload);
        }
    }
    public static String sha256(byte[] value) {
        try {
            byte[] hash = MessageDigest.getInstance("SHA-256").digest(value);
            char[] result = new char[hash.length * 2], alphabet = "0123456789abcdef".toCharArray();
            for (int i = 0; i < hash.length; i++) { result[2 * i] = alphabet[(hash[i] >>> 4) & 15]; result[2 * i + 1] = alphabet[hash[i] & 15]; }
            return new String(result);
        }
        catch (NoSuchAlgorithmException impossible) { throw new IllegalStateException(impossible); }
    }
}

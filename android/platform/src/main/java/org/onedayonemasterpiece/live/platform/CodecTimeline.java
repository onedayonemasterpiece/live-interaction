package org.onedayonemasterpiece.live.platform;

/** Coded sample clock to valid original span; independent of MediaCodec buffer/PTS batching. */
final class CodecTimeline {
    record Span(long startUs, int durationUs, long granule, long validSamples, int endTrimSamples) { }
    private final int clockRate, preSkip;
    private long coded, valid;
    CodecTimeline(int clockRate, int preSkip) {
        if (clockRate < 1 || preSkip < 0 || preSkip > 65535) throw CompressedAudio.invalid("CLOCK");
        this.clockRate = clockRate; this.preSkip = preSkip;
    }
    /** True once the next coded packet contains the last original captured sample. */
    boolean covers(int packetSamples, long capturedSamples) {
        if (packetSamples < 1 || capturedSamples < 0) throw CompressedAudio.invalid("PACKET_CLOCK");
        return coded + packetSamples - preSkip >= capturedSamples;
    }
    Span advance(int packetSamples, long capturedSamples, boolean terminal) {
        if (packetSamples < 1 || packetSamples * 1000000L > clockRate * 240000L || capturedSamples < 0)
            throw CompressedAudio.invalid("PACKET_CLOCK");
        coded += packetSamples;
        long decodedEnd = Math.max(0, coded - preSkip);
        long end = terminal ? Math.min(capturedSamples, decodedEnd) : decodedEnd;
        if (end < valid) throw CompressedAudio.invalid("TIMELINE_REGRESS");
        if (!terminal && end > capturedSamples) throw CompressedAudio.invalid("TIMELINE_AHEAD");
        long startUs = valid * 1000000L / clockRate, endUs = end * 1000000L / clockRate;
        if (endUs - startUs > 240000L) throw CompressedAudio.invalid("FRAME_DURATION");
        valid = end;
        return new Span(startUs, (int) (endUs - startUs), terminal ? Math.min(coded, capturedSamples + preSkip) : coded,
            valid, terminal ? Math.toIntExact(Math.max(0, decodedEnd - capturedSamples)) : 0);
    }
    long validSamples() { return valid; }
}

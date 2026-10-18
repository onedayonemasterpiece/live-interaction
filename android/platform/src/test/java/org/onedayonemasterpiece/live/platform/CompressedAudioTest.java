package org.onedayonemasterpiece.live.platform;

import org.junit.Test;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import static org.junit.Assert.*;

/** Real framing/clock tests; Android hardware and acoustic acceptance remain separate gates. */
public class CompressedAudioTest {
    private static byte[] head(int skip) {
        return ByteBuffer.allocate(19).order(ByteOrder.LITTLE_ENDIAN)
            .put("OpusHead".getBytes(StandardCharsets.US_ASCII)).put((byte) 1).put((byte) 1).putShort((short) skip)
            .putInt(48000).putShort((short) 0).put((byte) 0).array();
    }
    private static byte[] tagged(String tag, byte[] data) {
        return ByteBuffer.allocate(16 + data.length).order(ByteOrder.nativeOrder())
            .put(tag.getBytes(StandardCharsets.US_ASCII)).putLong(data.length).put(data).array();
    }
    @Test public void androidUnifiedCsdPreservesHeadAndChecksDelay() {
        byte[] original = head(312);
        byte[] csd = CompressedAudio.join(tagged("AOPUSHDR", original),
            CompressedAudio.join(tagged("AOPUSDLY", ByteBuffer.allocate(8).order(ByteOrder.nativeOrder()).putLong(6500000L).array()),
                tagged("AOPUSPRL", ByteBuffer.allocate(8).order(ByteOrder.nativeOrder()).putLong(80000000L).array())));
        assertArrayEquals(original, CompressedAudio.opusHead(csd));
        csd[csd.length - 24 + 16] = 1; // seek pre-roll is opaque for encoding; original pre-skip is unchanged.
        assertEquals(312, CompressedAudio.preSkip(CompressedAudio.opusHead(csd)));
        byte[] conflict = CompressedAudio.join(tagged("AOPUSHDR", original),
            tagged("AOPUSDLY", ByteBuffer.allocate(8).order(ByteOrder.nativeOrder()).putLong(12000000L).array()));
        assertThrows(IllegalArgumentException.class, () -> CompressedAudio.opusHead(conflict));
    }
    @Test public void invalidCsdNeverBecomesZeroPreskip() {
        assertThrows(IllegalArgumentException.class, () -> CompressedAudio.opusHead(new byte[0]));
        assertThrows(IllegalArgumentException.class, () -> CompressedAudio.opusHead(tagged("AOPUSHDR", new byte[10])));
        byte[] overflow = tagged("AOPUSHDR", head(312));
        ByteBuffer.wrap(overflow).order(ByteOrder.nativeOrder()).putLong(8, Long.MAX_VALUE);
        assertThrows(IllegalArgumentException.class, () -> CompressedAudio.opusHead(overflow));
    }
    @Test public void separateAndroidDelayCannotOverrideTheImmutableOriginalSilently() {
        CompressedAudio.requireOpusDelay(head(312), ByteBuffer.allocate(8).order(ByteOrder.nativeOrder()).putLong(6500000L).array());
        assertThrows(IllegalArgumentException.class, () -> CompressedAudio.requireOpusDelay(head(312), new byte[8]));
        assertThrows(IllegalArgumentException.class, () -> CompressedAudio.requireOpusDelay(head(312),
            ByteBuffer.allocate(8).order(ByteOrder.nativeOrder()).putLong(Long.MAX_VALUE).array()));
    }
    @Test public void oggPagesRoundTripKeepPacketSequenceGranuleAndEos() {
        CompressedAudio.OggMux mux = new CompressedAudio.OggMux(0x6b794a21);
        byte[] init = mux.headers(head(312));
        byte[] packet = new byte[] {(byte) 0xf8, (byte) 0xff, (byte) 0xfe}; // valid Opus 20-ms silence packet
        CompressedAudio.OggDemux reader = new CompressedAudio.OggDemux();
        List<CompressedAudio.OggPage> headers = reader.pages(init);
        assertArrayEquals(head(312), headers.get(0).packets().get(0));
        assertTrue(CompressedAudio.starts(headers.get(1).packets().get(0), 0, "OpusTags"));
        CompressedAudio.OggPage first = reader.pages(mux.packet(packet, 960, false)).get(0);
        assertEquals(960, first.granule()); assertFalse(first.eos()); assertArrayEquals(packet, first.packets().get(0));
        CompressedAudio.OggPage end = reader.pages(mux.packet(packet, 1272, true)).get(0);
        assertTrue(end.eos()); assertEquals(1272, end.granule());
        assertThrows(IllegalArgumentException.class, () -> reader.pages(packet));
    }
    @Test public void oggCorruptionAndMissingPrefixAreRejected() {
        CompressedAudio.OggMux mux = new CompressedAudio.OggMux(1); byte[] init = mux.headers(head(312));
        byte[] packet = mux.packet(new byte[] {(byte) 0xf8, (byte) 0xff, (byte) 0xfe}, 960, false);
        assertThrows(IllegalArgumentException.class, () -> new CompressedAudio.OggDemux().pages(packet));
        CompressedAudio.OggDemux reader = new CompressedAudio.OggDemux(); reader.pages(init);
        packet[packet.length - 1] ^= 1;
        assertThrows(IllegalArgumentException.class, () -> reader.pages(packet));
    }
    @Test public void exact255BytePacketGetsTerminatingLace() {
        byte[] packet = new byte[255]; packet[0] = (byte) 0xf8;
        CompressedAudio.OggMux mux = new CompressedAudio.OggMux(1);
        CompressedAudio.OggDemux reader = new CompressedAudio.OggDemux(); reader.pages(mux.headers(head(0)));
        assertArrayEquals(packet, reader.pages(mux.packet(packet, 960, true)).get(0).packets().get(0));
    }
    @Test public void opusTocUsesCodecClockAndRejectsExcessWork() {
        assertEquals(960, CompressedAudio.opusSamples(new byte[] {(byte) 0xf8}));
        assertEquals(120, CompressedAudio.opusSamples(new byte[] {(byte) 0x80}));
        assertEquals(5760, CompressedAudio.opusSamples(new byte[] {(byte) 0x1b, 2}));
        assertThrows(IllegalArgumentException.class, () -> CompressedAudio.opusSamples(new byte[] {(byte) 0xfb, 63}));
        assertThrows(IllegalArgumentException.class, () -> CompressedAudio.opusSamples(new byte[] {(byte) 0xfb}));
    }
    @Test public void adtsIsMonoLcWithExplicitClockAndFrameLength() {
        byte[] packet = {1, 2, 3, 4};
        for (int rate : new int[] {16000, 24000, 48000}) {
            byte[] framed = CompressedAudio.adts(packet, rate);
            assertArrayEquals(packet, CompressedAudio.adtsPackets(framed, rate).get(0));
            assertThrows(IllegalArgumentException.class, () -> CompressedAudio.adtsPackets(framed, rate == 16000 ? 24000 : 16000));
            framed[6] |= 1;
            assertThrows(IllegalArgumentException.class, () -> CompressedAudio.adtsPackets(framed, rate));
        }
    }
    @Test public void original100msIncludesPreskipAndDelayedTailExactlyOnce() {
        CodecTimeline timeline = new CodecTimeline(48000, 312);
        long totalUs = 0;
        for (int i = 0; i < 5; i++) {
            CodecTimeline.Span span = timeline.advance(960, (i + 1) * 960, false);
            assertEquals(totalUs, span.startUs());
            if (i == 0) assertEquals(13500, span.durationUs());
            totalUs += span.durationUs();
        }
        assertEquals(93500, totalUs);
        CodecTimeline.Span tail = timeline.advance(960, 4800, true);
        assertEquals(6500, tail.durationUs()); assertEquals(648, tail.endTrimSamples());
        assertEquals(5112, tail.granule()); assertEquals(100000, totalUs + tail.durationUs());
        assertEquals(4800, timeline.validSamples());
    }
    @Test public void aacPrimingIsNotMistakenForCapturedAudio() {
        CodecTimeline timeline = new CodecTimeline(16000, 2112);
        assertEquals(0, timeline.advance(1024, 4096, false).durationUs());
        assertEquals(0, timeline.advance(1024, 4096, false).durationUs());
        assertEquals(60000, timeline.advance(1024, 4096, false).durationUs());
        assertEquals(64000, timeline.advance(1024, 4096, false).durationUs());
        assertEquals(64000, timeline.advance(1024, 4096, false).durationUs());
        assertEquals(64000, timeline.advance(1024, 4096, false).durationUs());
        CodecTimeline.Span tail = timeline.advance(1024, 4096, true);
        assertEquals(4000, tail.durationUs()); assertEquals(960, tail.endTrimSamples());
        assertEquals(4096, timeline.validSamples());
    }
    @Test public void finalCodecPaddingIsTrimmedAtTheFirstPacketCoveringOriginalAudio() {
        // Includes partial reads, an exact frame boundary and a 60-second source.
        for (int[] format : new int[][] {{48000, 312, 960}, {16000, 2112, 1024}}) {
            int rate = format[0], delay = format[1], packet = format[2];
            for (long captured : new long[] {1, 311, 312, 313, 647, 648, 649, 959, 960,
                    packet * 8L, packet * 8L + 649, rate * 60L + 311}) {
                CodecTimeline timeline = new CodecTimeline(rate, delay);
                long previousGranule = 0;
                boolean completed = false;
                for (int i = 0; i < (captured + delay) / packet + 4; i++) {
                    boolean finalPacket = timeline.covers(packet, captured);
                    CodecTimeline.Span span = timeline.advance(packet, captured, finalPacket);
                    assertTrue("monotone sample clock", span.granule() >= previousGranule);
                    previousGranule = span.granule();
                    if (finalPacket) {
                        assertEquals("exact original sample count", captured, timeline.validSamples());
                        assertEquals("original plus immutable encoder delay", captured + delay, span.granule());
                        assertTrue("codec-only samples trimmed", span.endTrimSamples() >= 0);
                        assertTrue("terminal trim cannot exceed frame", span.endTrimSamples() < packet);
                        completed = true;
                        break;
                    }
                    assertTrue("never claim unrecorded samples", timeline.validSamples() < captured);
                }
                assertTrue("padded packet must cover original", completed);
            }
        }
    }
    @Test public void aheadAndRegressionAreDistinctFromAnUnderproducedTail() {
        CodecTimeline ahead = new CodecTimeline(48000, 312);
        assertEquals("LIVE_CODEC_TIMELINE_AHEAD", assertThrows(IllegalArgumentException.class,
                () -> ahead.advance(960, 100, false)).getMessage());
        CodecTimeline past = new CodecTimeline(48000, 0);
        past.advance(960, 960, false);
        assertEquals("LIVE_CODEC_TIMELINE_REGRESS", assertThrows(IllegalArgumentException.class,
                () -> past.advance(960, 0, true)).getMessage());
    }
    @Test public void nonterminalPaddingOrReorderedCaptureIsAnError() {
        CodecTimeline timeline = new CodecTimeline(48000, 312);
        assertThrows(IllegalArgumentException.class, () -> timeline.advance(960, 100, false));
        assertThrows(IllegalArgumentException.class, () -> new CodecTimeline(48000, 0).advance(20000, 30000, false));
    }
}

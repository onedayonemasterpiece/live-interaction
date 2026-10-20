package org.onedayonemasterpiece.live.platform;

import org.junit.Test;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.onedayonemasterpiece.live.DurableOutputStore;
import static org.onedayonemasterpiece.live.VoiceAudioV2.*;
import static org.junit.Assert.*;

public class DurablePlaybackPumpTest {
    private static byte[] head() {
        return ByteBuffer.allocate(19).order(ByteOrder.LITTLE_ENDIAN).put("OpusHead".getBytes(StandardCharsets.US_ASCII))
            .put((byte) 1).put((byte) 1).putShort((short) 0).putInt(48000).putShort((short) 0).put((byte) 0).array();
    }
    private static final class Journal implements DurableOutputStore {
        final Map<String, List<OutputChunk>> sources = new LinkedHashMap<>();
        final CodecEpoch epoch = new CodecEpoch("epoch", "opus", "ogg", 1, 1, 16000, 48000, 0, new CompressedAudio.OggMux(1).headers(head()));
        void source(String id, int count, boolean sealed) {
            var mux = new CompressedAudio.OggMux(1); mux.headers(head());
            List<OutputChunk> chunks = new ArrayList<>();
            for (int i = 1; i <= count; i++) {
                boolean end = sealed && i == count;
                chunks.add(new OutputChunk(new EncodedChunk(id, "epoch", i, (i - 1) * 20000L, 20000, end ? EPOCH_END : 0,
                    mux.packet(new byte[] {(byte) 0xf8, (byte) 0xff, (byte) 0xfe}, i * 960L, end)), i));
            }
            sources.put(id, chunks);
        }
        public List<OutputChunk> readOutput(String sourceId, long after, int bytes, long duration) {
            List<OutputChunk> result = new ArrayList<>(); int size = 0; long time = 0;
            for (OutputChunk value : sources.get(sourceId)) {
                EncodedChunk chunk = value.chunk(); if (chunk.chunkSeq() <= after) continue;
                if (size + chunk.payload().length > bytes || time + chunk.durationUs() > duration) break;
                result.add(value); size += chunk.payload().length; time += chunk.durationUs();
            }
            return result;
        }
        public CodecEpoch outputEpoch(String sourceId, String epochId) { return epoch; }
        public void openOutput(SourceDescriptor value) { throw new AssertionError("playback may not create originals"); }
        public void registerOutputEpoch(String id, CodecEpoch value) { throw new AssertionError("playback may not change originals"); }
        public DurablePrefix appendOutput(OutputChunk value) { throw new AssertionError("playback may not invent EOS or receipts"); }
        public void markPlayed(String id, long value) { throw new AssertionError("submission is not actual playback"); }
        public void cancelPlayback(String id) { throw new AssertionError("pump may not invent cancellation"); }
        public List<PlaybackPosition> playbackPositions() { throw new AssertionError("unused"); }
    }
    private static final class Player implements DurablePlaybackPump.Sink {
        int free = 20;
        final List<String> epochs = new ArrayList<>(), audio = new ArrayList<>(), prefixes = new ArrayList<>();
        public boolean epoch(String id, CodecEpoch epoch) { epochs.add(id); return true; }
        public boolean audio(EncodedChunk chunk) {
            if (free == 0) return false;
            free--; audio.add(chunk.sourceId() + ":" + chunk.chunkSeq()); return true;
        }
        public boolean finishPrefix(String sourceId) { prefixes.add(sourceId); return true; }
    }
    @Test public void fullPlayerRetainsJournalCursorAndRetriesExactlyTheUnsubmittedChunk() throws Exception {
        Journal journal = new Journal(); journal.source("out_a", 3, true);
        Player player = new Player(); player.free = 1;
        DurablePlaybackPump pump = new DurablePlaybackPump(journal, player); pump.register("out_a", journal.epoch);
        pump.received("out_a", 3); pump.pump(Set.of());
        assertEquals(List.of("out_a:1"), player.audio);
        pump.pump(Set.of()); assertEquals(List.of("out_a:1"), player.audio);
        player.free = 2; pump.pump(Set.of());
        assertEquals(List.of("out_a:1", "out_a:2", "out_a:3"), player.audio);
        assertEquals(List.of("out_a"), player.epochs);
    }
    @Test public void nextSourceWaitsForHardwareCompletionInsteadOfByteReceiptOrQueueSubmission() throws Exception {
        Journal journal = new Journal(); journal.source("out_a", 1, true); journal.source("out_b", 1, true);
        Player player = new Player(); DurablePlaybackPump pump = new DurablePlaybackPump(journal, player);
        pump.register("out_a", journal.epoch); pump.register("out_b", journal.epoch);
        pump.received("out_a", 1); pump.pump(Set.of()); pump.pump(Set.of());
        assertEquals(List.of("out_a:1"), player.audio);
        pump.played("out_a", true); pump.pump(Set.of());
        assertEquals(List.of("out_a:1", "out_b:1"), player.audio);
    }
    @Test public void crashedPrefixDrainsAfterAcceptedBytesWithoutForgingOriginalEos() throws Exception {
        Journal journal = new Journal(); journal.source("out_partial", 2, false);
        Player player = new Player(); player.free = 1;
        DurablePlaybackPump pump = new DurablePlaybackPump(journal, player); pump.register("out_partial", journal.epoch);
        pump.received("out_partial", 2); pump.incomplete("out_partial", 2); pump.pump(Set.of());
        assertTrue(player.prefixes.isEmpty());
        player.free = 1; pump.pump(Set.of()); assertEquals(List.of("out_partial"), player.prefixes);
        assertTrue(journal.sources.get("out_partial").stream().allMatch(value -> value.chunk().flags() == 0));
    }
    @Test public void interruptedSourceDoesNotResumeWhenCapacityReturnsAndRebindingDoesNotRepeatPlayedSource() throws Exception {
        Journal journal = new Journal(); journal.source("out_a", 2, true); journal.source("out_b", 1, true);
        Player player = new Player(); player.free = 1;
        DurablePlaybackPump pump = new DurablePlaybackPump(journal, player);
        pump.register("out_a", journal.epoch); pump.register("out_b", journal.epoch); pump.pump(Set.of());
        player.free = 10; pump.pump(Set.of("out_a"));
        assertEquals(List.of("out_a:1", "out_b:1"), player.audio);
        pump.played("out_b", true); pump.register("out_b", journal.epoch); pump.pump(Set.of("out_a"));
        assertEquals(List.of("out_a:1", "out_b:1"), player.audio);
    }
}

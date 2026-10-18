package org.onedayonemasterpiece.live.platform;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import org.onedayonemasterpiece.live.DurableOutputStore;
import static org.onedayonemasterpiece.live.VoiceAudioV2.*;

/** Serial device-journal reader. WSS receipt and decoder submission are independent cursors. */
final class DurablePlaybackPump {
    interface Sink {
        boolean epoch(String sourceId, CodecEpoch epoch);
        boolean audio(EncodedChunk chunk);
        boolean finishPrefix(String sourceId);
    }
    private static final class Feed {
        final CodecEpoch epoch;
        long receivedThrough, submittedThrough, prefixThrough = -1;
        boolean epochQueued, inputEnded, completed, complete;
        Feed(CodecEpoch epoch) { this.epoch = epoch; }
    }
    private final DurableOutputStore store;
    private final Sink sink;
    private final Map<String, Feed> feeds = new LinkedHashMap<>();
    private String current;
    DurablePlaybackPump(DurableOutputStore store, Sink sink) { this.store = store; this.sink = sink; }
    void register(String sourceId, CodecEpoch epoch) {
        Feed before = feeds.get(sourceId);
        if (before == null || before.completed && !before.complete) feeds.put(sourceId, new Feed(epoch));
        else if (!before.epoch.epochId().equals(epoch.epochId())) throw CompressedAudio.invalid("OUTPUT_EPOCH_CHANGED");
    }
    void received(String sourceId, long through) {
        Feed feed = feeds.get(sourceId);
        if (feed != null) feed.receivedThrough = Math.max(feed.receivedThrough, through);
    }
    void incomplete(String sourceId, long through) {
        Feed feed = feeds.get(sourceId);
        if (feed != null) feed.prefixThrough = Math.max(feed.receivedThrough, through);
    }
    void played(String sourceId, boolean complete) {
        Feed feed = feeds.get(sourceId);
        if (feed != null) { feed.completed = true; feed.complete = complete; }
        if (sourceId.equals(current)) current = null;
    }
    void pump(Set<String> suppressed) throws IOException {
        Feed feed = current == null ? null : feeds.get(current);
        if (feed == null || feed.completed || suppressed.contains(current)) {
            current = null;
            for (var entry : feeds.entrySet()) {
                if (!entry.getValue().completed && !suppressed.contains(entry.getKey())) {
                    current = entry.getKey(); feed = entry.getValue(); break;
                }
            }
        }
        if (current == null || feed == null || feed.inputEnded) return;
        if (!feed.epochQueued) {
            if (!sink.epoch(current, feed.epoch)) return;
            feed.epochQueued = true;
        }
        for (OutputChunk output : store.readOutput(current, feed.submittedThrough, 16384, 480000)) {
            EncodedChunk chunk = output.chunk();
            if (!sink.audio(chunk)) return; // byte remains durable; cursor does not advance until accepted
            feed.submittedThrough = chunk.chunkSeq();
            if ((chunk.flags() & EPOCH_END) != 0) { feed.inputEnded = true; return; }
        }
        if (feed.prefixThrough >= 0 && feed.submittedThrough >= feed.prefixThrough && sink.finishPrefix(current))
            feed.inputEnded = true; // decoder drain only; no invented EPOCH_END or durable completion
    }
}

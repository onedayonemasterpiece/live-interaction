package org.onedayonemasterpiece.live.platform;

import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.List;
import org.onedayonemasterpiece.live.DurableOutputStore;
import java.util.Set;
import static org.onedayonemasterpiece.live.VoiceAudioV2.id;

/** Bounded original-source recovery; receiving bytes never advances the played cursor. */
final class OutputResumeQueue {
    record Resume(String sourceId, long playedMs) { }
    private final Map<String, Long> pending = new LinkedHashMap<>();
    private final Set<String> attempted = new LinkedHashSet<>();
    private String selected;

    synchronized void offer(String sourceId, long playedUs, long durationUs, boolean complete, boolean cancelled) {
        id(sourceId);
        if (playedUs < 0 || durationUs < 0) throw CompressedAudio.invalid("OUTPUT_POSITION");
        if (cancelled || complete && playedUs >= durationUs) { finished(sourceId); return; }
        if (attempted.contains(sourceId) || pending.size() >= 8 && !pending.containsKey(sourceId)) return;
        pending.merge(sourceId, playedUs, Math::max);
    }
    synchronized Resume next(Map<String, Long> playedPositions) {
        if (selected != null || pending.isEmpty()) return null;
        var entry = pending.entrySet().iterator().next(); selected = entry.getKey();
        return new Resume(selected, Math.max(entry.getValue(), playedPositions.getOrDefault(selected, 0L)) / 1000);
    }
    synchronized void finished(String sourceId) {
        boolean known = pending.remove(sourceId) != null || sourceId.equals(selected);
        if (sourceId.equals(selected)) selected = null;
        // A truncated source is retried after reconnect, not forever in the same socket.
        if (known) {
            attempted.add(sourceId);
            while (attempted.size() > 32) attempted.remove(attempted.iterator().next());
        }
    }
    synchronized void reconnect(String activeSource, long playedUs) {
        attempted.clear(); selected = null;
        if (activeSource != null) {
            Map<String, Long> remaining = new LinkedHashMap<>(pending);
            pending.clear(); pending.put(activeSource, playedUs);
            for (var entry : remaining.entrySet()) if (pending.size() < 8) pending.putIfAbsent(entry.getKey(), entry.getValue());
        }
    }
    synchronized String selected() { return selected; }
    synchronized Set<String> pendingSources() { return Set.copyOf(pending.keySet()); }

    /** Reopening the app restores cancellation independently of actual playback. */
    void restore(List<DurableOutputStore.PlaybackPosition> positions,
                 Map<String, Long> playedPositions, Set<String> suppressed) {
        for (var position : positions) {
            String id = position.source().sourceId();
            playedPositions.merge(id, position.playedUs(), Math::max);
            if (position.playbackCancelled()) suppressed.add(id);
            if (position.receivedThrough() > 0)
                offer(id, position.playedUs(), position.durationUs(), position.sealed(), suppressed.contains(id));
        }
    }

    /** An output finished during a network outage still owes its played cursor.
     * Emit positions on every new transport, never re-enqueue already heard audio.
     * IDs are scoped to this native Run; server receipts remain monotone/idempotent.
     */
    static Map<String, Long> completedPlayheads(Set<String> completed, Map<String, Long> playedUs) {
        Map<String, Long> result = new LinkedHashMap<>();
        for (String sourceId : completed) {
            Long position = playedUs.get(sourceId);
            if (position != null && position > 0) result.put(sourceId, position / 1000);
        }
        return result;
    }
}

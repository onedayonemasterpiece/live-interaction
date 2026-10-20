package org.onedayonemasterpiece.live.platform;

import org.junit.Test;
import java.util.Map;
import java.util.Set;
import static org.junit.Assert.*;

public class OutputResumeQueueTest {
    @Test public void freshRunRestoresCancellationWithoutTurningReceivedBytesIntoPlayedAudio() {
        var source = new org.onedayonemasterpiece.live.VoiceAudioV2.SourceDescriptor("out_cancelled", 0);
        var pending = new org.onedayonemasterpiece.live.VoiceAudioV2.SourceDescriptor("out_pending", 0);
        var positions = java.util.List.of(
            new org.onedayonemasterpiece.live.DurableOutputStore.PlaybackPosition(source, 5, 11, 20000, 100000, true, true),
            new org.onedayonemasterpiece.live.DurableOutputStore.PlaybackPosition(pending, 5, 12, 40000, 100000, true, false));
        var played = new java.util.HashMap<String, Long>();
        var suppressed = new java.util.HashSet<String>();
        OutputResumeQueue queue = new OutputResumeQueue();
        queue.restore(positions, played, suppressed);
        assertEquals(Set.of("out_cancelled"), suppressed);
        assertEquals(Long.valueOf(20000), played.get("out_cancelled"));
        assertEquals(Set.of("out_pending"), queue.pendingSources());
        assertEquals(new OutputResumeQueue.Resume("out_pending", 40), queue.next(played));
        // An older server manifest cannot undo the on-device cancellation.
        queue.offer("out_cancelled", 0, 100000, true, suppressed.contains("out_cancelled"));
        queue.finished("out_pending");
        assertNull(queue.next(played));
        queue.reconnect(null, 0);
        queue.restore(positions, played, suppressed);
        assertEquals(Set.of("out_pending"), queue.pendingSources());
    }

    @Test public void cancellationOfEmptySourceStillFencesItsLaterDelivery() {
        var source = new org.onedayonemasterpiece.live.VoiceAudioV2.SourceDescriptor("out_empty", 0);
        var positions = java.util.List.of(new org.onedayonemasterpiece.live.DurableOutputStore.PlaybackPosition(
            source, 0, 0, 0, 0, false, true));
        var suppressed = new java.util.HashSet<String>();
        OutputResumeQueue queue = new OutputResumeQueue();
        queue.restore(positions, new java.util.HashMap<>(), suppressed);
        assertEquals(Set.of("out_empty"), suppressed);
        assertTrue(queue.pendingSources().isEmpty());
        assertNull(queue.next(Map.of()));
    }

    @Test public void completedAudioDuringSocketOutageOwesAReplaySafePlayheadReceipt() {
        Map<String, Long> positions = OutputResumeQueue.completedPlayheads(
            Set.of("out_offline", "out_incomplete", "out_zero"),
            Map.of("out_offline", 1854321L, "out_incomplete", 100000L, "out_zero", 0L,
                   "out_unrelated", 920000L));
        assertEquals(Map.of("out_offline", 1854L, "out_incomplete", 100L), positions);
        // Resending the same monotone position after another reconnect is idempotent.
        assertEquals(positions, OutputResumeQueue.completedPlayheads(
            Set.of("out_offline", "out_incomplete", "out_zero"),
            Map.of("out_offline", 1854321L, "out_incomplete", 100000L, "out_zero", 0L)));
    }
    @Test public void receivedSourceDoesNotBecomePlayedOrStartEveryReplayAtOnce() {
        OutputResumeQueue queue = new OutputResumeQueue();
        queue.offer("out_first", 100000, 900000, true, false);
        queue.offer("out_next", 0, 200000, true, false);
        assertEquals(new OutputResumeQueue.Resume("out_first", 100), queue.next(Map.of()));
        assertNull(queue.next(Map.of())); // receipt/manifest is not player completion
        queue.finished("out_first");
        assertEquals(new OutputResumeQueue.Resume("out_next", 0), queue.next(Map.of()));
    }
    @Test public void actualLocalPositionWinsOverOlderServerReceipt() {
        OutputResumeQueue queue = new OutputResumeQueue();
        queue.offer("out_first", 100000, 900000, true, false);
        assertEquals(350, queue.next(Map.of("out_first", 350000L)).playedMs());
    }
    @Test public void completeAndCancelledSourcesAreNotReplayed() {
        OutputResumeQueue queue = new OutputResumeQueue();
        queue.offer("out_done", 400000, 400000, true, false);
        queue.offer("out_cancelled", 0, 400000, true, true);
        assertNull(queue.next(Map.of()));
        queue.offer("out_selected", 0, 400000, true, false); queue.next(Map.of());
        queue.offer("out_selected", 0, 400000, true, true);
        queue.offer("out_next", 0, 400000, true, false);
        assertEquals("out_next", queue.next(Map.of()).sourceId());
    }
    @Test public void incompletePrefixIsTriedOncePerConnectionAndPreservesActualPosition() {
        OutputResumeQueue queue = new OutputResumeQueue();
        queue.offer("out_partial", 400000, 400000, false, false);
        assertEquals(400, queue.next(Map.of()).playedMs());
        queue.finished("out_partial");
        queue.offer("out_partial", 400000, 400000, false, false);
        assertNull(queue.next(Map.of()));
        queue.reconnect("out_partial", 400000);
        assertEquals(400, queue.next(Map.of()).playedMs());
    }
    @Test public void manifestReservationIsBoundedAndReconnectPrioritizesInterruptedSource() {
        OutputResumeQueue queue = new OutputResumeQueue();
        for (int i = 0; i < 40; i++) queue.offer("out_" + i, 0, 400000, true, false);
        queue.reconnect("out_interrupted", 123000);
        assertEquals(new OutputResumeQueue.Resume("out_interrupted", 123), queue.next(Map.of()));
        queue.finished("out_interrupted");
        int rest = 0;
        for (OutputResumeQueue.Resume next; (next = queue.next(Map.of())) != null;) { rest++; queue.finished(next.sourceId()); }
        assertEquals(7, rest);
    }
}

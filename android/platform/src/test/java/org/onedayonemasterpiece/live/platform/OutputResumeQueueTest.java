package org.onedayonemasterpiece.live.platform;

import org.junit.Test;
import java.util.Map;
import java.util.Set;
import static org.junit.Assert.*;

public class OutputResumeQueueTest {
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

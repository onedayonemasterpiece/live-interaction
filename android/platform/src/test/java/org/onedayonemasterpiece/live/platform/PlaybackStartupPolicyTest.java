package org.onedayonemasterpiece.live.platform;

import org.junit.Test;
import static org.junit.Assert.*;

/** Silence-at-start and short-response prebuffer invariants; device waveform is a separate gate. */
public class PlaybackStartupPolicyTest {
    @Test public void prebufferMustBeExactlyBoundedAndRateAware() {
        assertEquals(9600, PlaybackStartupPolicy.prefillFrames(48000));
        assertEquals(3200, PlaybackStartupPolicy.prefillFrames(16000));
        assertEquals(19200, PlaybackStartupPolicy.nextWriteBytes(48000, 0, 50000));
        assertEquals(3200, PlaybackStartupPolicy.nextWriteBytes(48000, 8000, 50000));
        assertEquals(200, PlaybackStartupPolicy.nextWriteBytes(48000, 8000, 200));
        assertEquals(1024, PlaybackStartupPolicy.nextWriteBytes(48000, 9600, 1024));
        assertFalse(PlaybackStartupPolicy.canStart(48000, 9599, false));
        assertTrue(PlaybackStartupPolicy.canStart(48000, 9600, false));
    }
    @Test public void platformStartThresholdCannotSilentlyExceedPrebufferCapacity() {
        // 400 ms requested, 200 ms prefill; if a vendor supplies a smaller
        // effective buffer, use strictly less than half its actual capacity.
        assertEquals(9600, PlaybackStartupPolicy.boundedPrefillFrames(48000, 19200, 19200));
        assertEquals(3600, PlaybackStartupPolicy.boundedPrefillFrames(48000, 7200, 24000));
        assertEquals(300, PlaybackStartupPolicy.boundedPrefillFrames(16000, 600, 1024));
        assertEquals(7200, PlaybackStartupPolicy.nextWriteBytes(48000, 0, 30000, 3600));
        assertEquals(400, PlaybackStartupPolicy.nextWriteBytes(48000, 3400, 30000, 3600));
        assertTrue(PlaybackStartupPolicy.canStart(48000, 3600, false, 3600));
        assertFalse(PlaybackStartupPolicy.canStart(48000, 3599, false, 3600));
        assertTrue(PlaybackStartupPolicy.canStart(48000, 5760, true, 9600)); // 120-ms reply
        assertFalse(PlaybackStartupPolicy.canStart(48000, 0, true, 9600));
        assertThrows(IllegalArgumentException.class,
                () -> PlaybackStartupPolicy.boundedPrefillFrames(48000, 1, 9600));
    }
    @Test public void shortResponseStartsOnlyAfterRealPcmAndNeverFabricatesPlayhead() {
        assertFalse(PlaybackStartupPolicy.canStart(48000, 0, true));
        assertFalse(PlaybackStartupPolicy.canStart(16000, 0, false));
        assertTrue(PlaybackStartupPolicy.canStart(48000, 240, true));
        assertTrue(PlaybackStartupPolicy.canStart(16000, 512, true));
        assertFalse(PlaybackStartupPolicy.canStart(16000, 512, false));
        assertThrows(IllegalArgumentException.class,
                () -> PlaybackStartupPolicy.nextWriteBytes(48000, -1, 1024));
        assertThrows(IllegalArgumentException.class,
                () -> PlaybackStartupPolicy.nextWriteBytes(48000, 0, 7));
        assertThrows(IllegalArgumentException.class,
                () -> PlaybackStartupPolicy.prefillFrames(0));
    }
}

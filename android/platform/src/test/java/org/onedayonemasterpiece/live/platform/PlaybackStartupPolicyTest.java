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

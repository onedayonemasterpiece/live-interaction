package org.onedayonemasterpiece.live.platform;

import org.junit.Test;
import static org.junit.Assert.*;

/** Silence-at-start and short-response prebuffer invariants; device waveform is a separate gate. */
public class PlaybackStartupPolicyTest {
    @Test public void prebufferMustBeExactlyBoundedAndRateAware() {
        assertEquals(14400, PlaybackStartupPolicy.prefillFrames(48000));
        assertEquals(4800, PlaybackStartupPolicy.prefillFrames(16000));
        assertEquals(28800, PlaybackStartupPolicy.nextWriteBytes(48000, 0, 50000));
        assertEquals(12800, PlaybackStartupPolicy.nextWriteBytes(48000, 8000, 50000));
        assertEquals(200, PlaybackStartupPolicy.nextWriteBytes(48000, 8000, 200));
        assertEquals(1024, PlaybackStartupPolicy.nextWriteBytes(48000, 14400, 1024));
        assertFalse(PlaybackStartupPolicy.canStart(48000, 14399, false));
        assertTrue(PlaybackStartupPolicy.canStart(48000, 14400, false));
    }
    @Test public void platformStartThresholdCannotSilentlyExceedPrebufferCapacity() {
        // 600 ms requested, 300 ms prefill; if a vendor supplies a smaller
        // effective buffer, use at most half its actual capacity.
        assertEquals(14400, PlaybackStartupPolicy.boundedPrefillFrames(48000, 28800, 28800));
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
    @Test public void terminalUnderrunUsesRemainingFramesNotCumulativeWrittenSamples() {
        // One second has played; only the last 120 ms remain after starvation.
        assertEquals(5760, PlaybackStartupPolicy.terminalThresholdFrames(53760, 48000, 9600));
        assertEquals(4800, PlaybackStartupPolicy.terminalThresholdFrames(53760, 48960, 5760));
        assertEquals(5760, PlaybackStartupPolicy.terminalThresholdFrames(5760, 0, 9600));
        assertEquals(9600, PlaybackStartupPolicy.terminalThresholdFrames(57600, 48000, 9600));
        assertEquals(9600, PlaybackStartupPolicy.terminalThresholdFrames(72000, 48000, 9600));
        assertEquals(1, PlaybackStartupPolicy.terminalThresholdFrames(48001, 48000, 9600));
        assertEquals(9600, PlaybackStartupPolicy.terminalThresholdFrames(48000, 48000, 9600));
        assertEquals(9600, PlaybackStartupPolicy.terminalThresholdFrames(0, 0, 9600));
        long wrap = 1L << 32;
        assertEquals(5760, PlaybackStartupPolicy.terminalThresholdFrames(wrap + 53760, wrap + 48000, 9600));
        assertThrows(IllegalArgumentException.class,
                () -> PlaybackStartupPolicy.terminalThresholdFrames(1, 2, 9600));
        assertThrows(IllegalArgumentException.class,
                () -> PlaybackStartupPolicy.terminalThresholdFrames(-1, 0, 9600));
        assertThrows(IllegalArgumentException.class,
                () -> PlaybackStartupPolicy.terminalThresholdFrames(1, 0, 0));
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

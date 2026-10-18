package org.onedayonemasterpiece.live.platform;

/** AudioTrack must have PCM queued before PLAYING; otherwise slow HAL startup
 * consumes an empty stream and can distort/drop the first audible syllable.
 * This is only a mechanical byte/frame threshold, not an audio processor.
 */
final class PlaybackStartupPolicy {
    private PlaybackStartupPolicy() { }
    static int prefillFrames(int sampleRate) {
        if (sampleRate < 8000 || sampleRate > 192000) throw new IllegalArgumentException("LIVE_CODEC_OUTPUT_RATE");
        return sampleRate / 5; // 200 ms; the native track allocates >= 400 ms
    }
    static int nextWriteBytes(int sampleRate, long queuedFrames, int remainingBytes) {
        if (queuedFrames < 0 || remainingBytes < 0 || (remainingBytes & 1) != 0)
            throw new IllegalArgumentException("LIVE_CODEC_OUTPUT_WRITE");
        int untilStart = (int) Math.max(0, prefillFrames(sampleRate) - queuedFrames);
        return untilStart == 0 ? remainingBytes : Math.min(remainingBytes, untilStart * 2);
    }
    static boolean canStart(int sampleRate, long queuedFrames, boolean finalAudio) {
        return queuedFrames > 0 && (finalAudio || queuedFrames >= prefillFrames(sampleRate));
    }
}

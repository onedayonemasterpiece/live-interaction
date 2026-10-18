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
    static int boundedPrefillFrames(int sampleRate, int bufferFrames, int capacityFrames) {
        if (bufferFrames < 2 || capacityFrames < 2) throw new IllegalArgumentException("LIVE_CODEC_OUTPUT_CAPACITY");
        return Math.max(1, Math.min(prefillFrames(sampleRate), Math.min(bufferFrames, capacityFrames) / 2));
    }
    static int nextWriteBytes(int sampleRate, long queuedFrames, int remainingBytes) {
        return nextWriteBytes(sampleRate, queuedFrames, remainingBytes, prefillFrames(sampleRate));
    }
    static int nextWriteBytes(int sampleRate, long queuedFrames, int remainingBytes, int targetFrames) {
        if (queuedFrames < 0 || remainingBytes < 0 || (remainingBytes & 1) != 0)
            throw new IllegalArgumentException("LIVE_CODEC_OUTPUT_WRITE");
        if (targetFrames < 1 || targetFrames > sampleRate) throw new IllegalArgumentException("LIVE_CODEC_OUTPUT_THRESHOLD");
        int untilStart = (int) Math.max(0, targetFrames - queuedFrames);
        return untilStart == 0 ? remainingBytes : Math.min(remainingBytes, untilStart * 2);
    }
    static boolean canStart(int sampleRate, long queuedFrames, boolean finalAudio) {
        return canStart(sampleRate, queuedFrames, finalAudio, prefillFrames(sampleRate));
    }
    static boolean canStart(int sampleRate, long queuedFrames, boolean finalAudio, int targetFrames) {
        if (targetFrames < 1 || targetFrames > sampleRate) throw new IllegalArgumentException("LIVE_CODEC_OUTPUT_THRESHOLD");
        return queuedFrames > 0 && (finalAudio || queuedFrames >= targetFrames);
    }
}

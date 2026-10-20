package org.onedayonemasterpiece.live;

import java.io.IOException;
import java.util.List;

/** Product-scoped persistence of original compressed output and actual playback. */
public interface DurableOutputStore {
    record PlaybackPosition(VoiceAudioV2.SourceDescriptor source, long receivedThrough,
            long originalEventSeq, long playedUs, long durationUs, boolean sealed, boolean playbackCancelled) {
        public PlaybackPosition(VoiceAudioV2.SourceDescriptor source, long receivedThrough,
                long originalEventSeq, long playedUs, long durationUs, boolean sealed) {
            this(source, receivedThrough, originalEventSeq, playedUs, durationUs, sealed, false);
        }
    }

    void openOutput(VoiceAudioV2.SourceDescriptor source) throws IOException;
    void registerOutputEpoch(String sourceId, VoiceAudioV2.CodecEpoch epoch) throws IOException;
    VoiceAudioV2.CodecEpoch outputEpoch(String sourceId, String epochId) throws IOException;

    /** Sync original encoded bytes and identity before ACK or playback. */
    VoiceAudioV2.DurablePrefix appendOutput(VoiceAudioV2.OutputChunk chunk) throws IOException;

    /** Read an ordered prefix within BOTH budgets; receiving never marks it played. */
    List<VoiceAudioV2.OutputChunk> readOutput(String sourceId, long afterChunkSeq,
            int maxEncodedBytes, long maxDurationUs) throws IOException;

    /** Actual AudioTrack progress in the source's valid media timeline, never scheduled/received time. */
    void markPlayed(String sourceId, long playedUs) throws IOException;
    /** Sync a monotone playback-cancellation marker for an existing output source.
     * Retain original bytes and the ACTUAL played cursor. A late chunk cannot undo it.
     * Implementations must return this marker from playbackPositions after reopening.
     */
    void cancelPlayback(String sourceId) throws IOException;
    List<PlaybackPosition> playbackPositions() throws IOException;
}

package org.onedayonemasterpiece.live;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * Product persistence for immutable compressed sources. An instance is already
 * bound to the authenticated product scope; the SDK cannot widen that scope.
 *
 * A successful mutating call is durable across process death. In particular,
 * append must sync the original encoded bytes and their identity before it
 * returns a device receipt. A relay ACK must never call markServerCommitted.
 * No implementation may silently evict an uncommitted source to make room.
 */
public interface DurableSourceStore extends Closeable {
    record SourceStatus(
            VoiceAudioV2.SourceDescriptor source,
            long deviceDurableThrough,
            long serverCommittedThrough,
            boolean sealed,
            boolean serverSealed,
            long validEndUs,
            int endTrimSamples,
            String disposition) {}

    List<SourceStatus> sources() throws IOException;

    /** Idempotent only when the existing descriptor is exactly equal. */
    SourceStatus open(VoiceAudioV2.SourceDescriptor source) throws IOException;

    /** An epoch id is immutable within its source, including the init bytes. */
    void registerEpoch(String sourceId, VoiceAudioV2.CodecEpoch epoch) throws IOException;

    VoiceAudioV2.CodecEpoch epoch(String sourceId, String epochId) throws IOException;

    /**
     * Source-wide sequence starts at one and is contiguous. An exact retry is
     * idempotent; reuse with different metadata or bytes is an error. Delivery
     * lane/RECOVERY flags are not part of EncodedChunk or its stored identity.
     */
    VoiceAudioV2.DurablePrefix append(VoiceAudioV2.EncodedChunk chunk) throws IOException;

    /**
     * Read, without consuming, an ordered prefix after both the supplied cursor
     * and the stored server-committed cursor. Neither byte nor duration budget
     * may be exceeded. The next chunk is never skipped to fit a later one.
     */
    List<VoiceAudioV2.EncodedChunk> readPending(
            String sourceId, long afterChunkSeq, int maxEncodedBytes, long maxDurationUs)
            throws IOException;

    /** Only an authenticated server-durable, contiguous source receipt qualifies. */
    VoiceAudioV2.DurablePrefix markServerCommitted(VoiceAudioV2.DurablePrefix prefix)
            throws IOException;

    /**
     * End the immutable source at the last valid capture sample. endTrimSamples
     * is in the final epoch's clock rate; it is never an epoch descriptor edit.
     * Repeating the same seal is safe; changing it or appending later is not.
     */
    void seal(String sourceId, long validEndUs, int endTrimSamples) throws IOException;

    /** Persist an authenticated source_sealed receipt separately from chunk commits. */
    void markServerSealed(String sourceId) throws IOException;

    /** Persist the product's explicit terminal outcome, such as saved_as_note. */
    void disposition(String sourceId, String disposition) throws IOException;

    /**
     * Release only a source sealed locally and on server, with every chunk server committed and an
     * explicit disposition. This is never an automatic quota eviction policy.
     */
    void release(String sourceId) throws IOException;
}

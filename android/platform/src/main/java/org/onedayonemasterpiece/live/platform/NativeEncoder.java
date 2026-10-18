package org.onedayonemasterpiece.live.platform;

import android.media.MediaCodec;
import android.media.MediaCodecInfo;
import android.media.MediaCodecList;
import android.media.MediaFormat;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.UUID;
import static org.onedayonemasterpiece.live.VoiceAudioV2.*;

/** One continuous codec epoch. Called only on the serial encoder/storage thread. */
final class NativeEncoder implements AutoCloseable {
    record Choice(String profile, String codecName, int rate) { }
    interface Sink {
        void epoch(CodecEpoch epoch) throws IOException;
        void chunk(EncodedChunk chunk) throws IOException;
        default void speechStarted() { }
    }
    private record Packet(byte[] bytes, int samples, long pts) { }
    private record VoiceMark(long sample, boolean active) { }
    private final Choice choice;
    private final String sourceId, epochId = UUID.randomUUID().toString();
    private final MediaCodec codec;
    private final Sink sink;
    private final ArrayDeque<VoiceMark> voice = new ArrayDeque<>();
    private final MediaCodec.BufferInfo info = new MediaCodec.BufferInfo();
    private CodecEpoch epoch;
    private CompressedAudio.OggMux ogg;
    private CodecTimeline timeline;
    private Packet pending;
    private byte[] prefix = new byte[0];
    private long capturedSamples, submittedSamples, validSamples, chunkSeq, lastPts = Long.MIN_VALUE;
    private boolean eos, closed, finishing, terminalPublished, activeInput, activePublished, waitForQuiet;
    private double noise = 100;
    private long quietSamples;
    private MediaFormat outputFormat;
    private int trim;

    static MediaFormat format(Choice c) {
        MediaFormat f = MediaFormat.createAudioFormat(OPUS_PROFILE.equals(c.profile()) ? "audio/opus" : "audio/mp4a-latm", c.rate(), 1);
        f.setInteger(MediaFormat.KEY_BIT_RATE, OPUS_PROFILE.equals(c.profile()) ? 24000 : 32000);
        f.setInteger(MediaFormat.KEY_MAX_INPUT_SIZE, c.rate() / 50 * 2);
        f.setInteger(MediaFormat.KEY_PCM_ENCODING, 2); // ENCODING_PCM_16BIT
        if (!OPUS_PROFILE.equals(c.profile())) f.setInteger(MediaFormat.KEY_AAC_PROFILE, MediaCodecInfo.CodecProfileLevel.AACObjectLC);
        return f;
    }
    static Choice find(String profile) {
        int rate = OPUS_PROFILE.equals(profile) ? 48000 : 16000;
        Choice template = new Choice(profile, "", rate);
        String name = new MediaCodecList(MediaCodecList.REGULAR_CODECS).findEncoderForFormat(format(template));
        return name == null ? null : new Choice(profile, name, rate);
    }
    /** Real native codec preflight, synthetic silence only: never requests microphone permission. */
    static boolean probe(Choice choice) {
        if (choice == null) return false;
        try (NativeEncoder encoder = new NativeEncoder(choice, "capability-probe", new Sink() {
            public void epoch(CodecEpoch e) { }
            public void chunk(EncodedChunk c) { }
        })) {
            short[] silence = new short[choice.rate() / 50];
            for (int i = 0; i < 16; i++) encoder.accept(silence, silence.length);
            encoder.finish();
            return encoder.epoch != null && encoder.validSamples == encoder.capturedSamples;
        } catch (Exception error) { return false; }
    }
    NativeEncoder(Choice choice, String sourceId, Sink sink) throws IOException {
        this.choice = choice; this.sourceId = sourceId; this.sink = sink;
        codec = MediaCodec.createByCodecName(choice.codecName());
        try { codec.configure(format(choice), null, null, MediaCodec.CONFIGURE_FLAG_ENCODE); codec.start(); }
        catch (RuntimeException error) { codec.release(); throw error; }
    }
    int rate() { return choice.rate(); }
    long validEndUs() { return samplesUs(validSamples); }
    int endTrimSamples() { return trim; }
    long lastSeq() { return chunkSeq; }
    CodecEpoch epoch() { return epoch; }
    /** Close the current portion at the next encoded durable boundary; original recording continues. */
    void finishTurn() {
        if (activeInput) voice.add(new VoiceMark(capturedSamples, false));
        activeInput = false; waitForQuiet = true; quietSamples = 0;
    }

    void accept(short[] pcm, int count) throws IOException {
        if (closed || eos || count < 1 || count > pcm.length) throw CompressedAudio.invalid("ENCODER_STATE");
        observeVoice(pcm, count);
        int offset = 0; long deadline = System.nanoTime() + 1500000000L;
        while (offset < count) {
            int index = codec.dequeueInputBuffer(10000);
            if (index < 0) { drain(false); if (System.nanoTime() > deadline) throw CompressedAudio.invalid("ENCODER_STALLED"); continue; }
            ByteBuffer b = codec.getInputBuffer(index);
            if (b == null || b.capacity() < 2) throw CompressedAudio.invalid("INPUT_BUFFER");
            b.clear(); b.order(ByteOrder.LITTLE_ENDIAN); int n = Math.min(count - offset, b.remaining() / 2);
            for (int i = 0; i < n; i++) b.putShort(pcm[offset + i]);
            long pts = samplesUs(submittedSamples);
            capturedSamples += n;
            submittedSamples += n;
            codec.queueInputBuffer(index, 0, n * 2, pts, 0); offset += n; drain(false);
        }
    }
    void finish() throws IOException {
        if (closed || eos) return;
        // Encoder delay plus a partial final AudioRecord read can leave the last
        // original samples unrepresented at EOS. Queue codec-only silence, never
        // count it as captured or persist it as an original source sample.
        finishing = true;
        long deadline = System.nanoTime() + 3500000000L;
        int pad = choice.rate() / 3; // bounded 333 ms: AAC delay <=160 ms, Opus pre-skip <=120 ms
        while (pad > 0 && System.nanoTime() < deadline) {
            int index = codec.dequeueInputBuffer(10000);
            if (index < 0) { drain(true); continue; }
            ByteBuffer b = codec.getInputBuffer(index);
            if (b == null || b.capacity() < 2) throw CompressedAudio.invalid("INPUT_BUFFER");
            b.clear(); b.order(ByteOrder.LITTLE_ENDIAN);
            int n = Math.min(pad, b.remaining() / 2);
            for (int i = 0; i < n; i++) b.putShort((short) 0);
            codec.queueInputBuffer(index, 0, n * 2, samplesUs(submittedSamples), 0);
            submittedSamples += n; pad -= n;
            drain(false);
        }
        if (pad > 0) throw CompressedAudio.invalid("ENCODER_PADDING_TIMEOUT");
        boolean submitted = false;
        while (!eos && System.nanoTime() < deadline) {
            if (!submitted) {
                int index = codec.dequeueInputBuffer(10000);
                if (index >= 0) {
                    codec.queueInputBuffer(index, 0, 0, samplesUs(submittedSamples), MediaCodec.BUFFER_FLAG_END_OF_STREAM); submitted = true;
                }
            }
            drain(true);
        }
        if (!eos) throw CompressedAudio.invalid("ENCODER_EOS_TIMEOUT");
        if (capturedSamples > 0 && (epoch == null || validSamples < capturedSamples))
            throw CompressedAudio.invalid("ENCODER_TAIL_SHORT");
        if (validSamples > capturedSamples) throw CompressedAudio.invalid("ENCODER_TAIL_OVERRUN");
    }
    private void drain(boolean waiting) throws IOException {
        while (!eos) {
            int index = codec.dequeueOutputBuffer(info, waiting ? 10000 : 0);
            if (index == MediaCodec.INFO_TRY_AGAIN_LATER) return;
            if (index == MediaCodec.INFO_OUTPUT_FORMAT_CHANGED) {
                outputFormat = codec.getOutputFormat(); readFormat(); continue;
            }
            if (index < 0) continue;
            byte[] data = new byte[info.size];
            long pts = info.presentationTimeUs; int flags = info.flags;
            try {
                ByteBuffer b = codec.getOutputBuffer(index);
                if (b == null && data.length != 0) throw CompressedAudio.invalid("OUTPUT_BUFFER");
                if (b != null) { b.position(info.offset); b.limit(info.offset + info.size); b.get(data); }
            } finally { codec.releaseOutputBuffer(index, false); }
            if ((flags & MediaCodec.BUFFER_FLAG_CODEC_CONFIG) != 0) {
                if (OPUS_PROFILE.equals(choice.profile()) && data.length > 0) registerOpus(data);
                else readFormat();
            } else if (data.length > 0) {
                if (epoch == null) readFormat();
                if (epoch == null) throw CompressedAudio.invalid("DELAY_METADATA_UNAVAILABLE");
                if (lastPts != Long.MIN_VALUE && pts < lastPts) throw CompressedAudio.invalid("OUTPUT_TIMESTAMP");
                lastPts = pts;
                if (pending != null && !terminalPublished) {
                    // Once padding begins, close the stream at the first packet
                    // covering all captured samples. Remaining codec-only packets
                    // are drained but never published as a new speech portion.
                    boolean lastOriginal = finishing && timeline.covers(pending.samples(), capturedSamples);
                    emit(pending, lastOriginal);
                    if (lastOriginal) terminalPublished = true;
                }
                int samples = OPUS_PROFILE.equals(choice.profile()) ? CompressedAudio.opusSamples(data) : 1024;
                pending = terminalPublished ? null : new Packet(data, samples, pts);
            }
            if ((flags & MediaCodec.BUFFER_FLAG_END_OF_STREAM) != 0) {
                eos = true;
                if (!terminalPublished && pending != null) { emit(pending, true); pending = null; terminalPublished = true; }
                else if (!terminalPublished && capturedSamples > 0) throw CompressedAudio.invalid("EMPTY_EOS");
            }
        }
    }
    private void readFormat() throws IOException {
        if (outputFormat == null) return;
        if (OPUS_PROFILE.equals(choice.profile())) {
            ByteBuffer csd = outputFormat.getByteBuffer("csd-0");
            if (csd != null) { byte[] bytes = new byte[csd.remaining()]; csd.duplicate().get(bytes); registerOpus(bytes); }
        } else {
            if (epoch != null) return;
            ByteBuffer csd = outputFormat.getByteBuffer("csd-0");
            if (csd == null) return;
            byte[] config = new byte[csd.remaining()]; csd.duplicate().get(config);
            byte[] expected = CompressedAudio.aacConfig(choice.rate());
            if (config.length < 2 || config[0] != expected[0] || (config[1] & 0xf8) != (expected[1] & 0xf8)) throw CompressedAudio.invalid("AAC_PROFILE");
            // Public output metadata is the only authority for AAC priming. Missing is not zero.
            if (!outputFormat.containsKey("encoder-delay")) return;
            int delay = outputFormat.getInteger("encoder-delay");
            if (delay < 0 || delay > choice.rate() * 16 / 100) throw CompressedAudio.invalid("AAC_DELAY");
            epoch = new CodecEpoch(epochId, "aac", "adts", 1, 1, choice.rate(), choice.rate(), delay, new byte[0]);
            timeline = new CodecTimeline(choice.rate(), delay);
            sink.epoch(epoch);
        }
    }
    private void registerOpus(byte[] csd) throws IOException {
        byte[] head = CompressedAudio.opusHead(csd); int delay = CompressedAudio.preSkip(head);
        if (delay > 5760) throw CompressedAudio.invalid("OPUS_DELAY");
        if (outputFormat != null && outputFormat.containsKey("csd-1")) {
            ByteBuffer metadata = outputFormat.getByteBuffer("csd-1");
            if (metadata == null) throw CompressedAudio.invalid("OPUS_DELAY");
            byte[] bytes = new byte[metadata.remaining()]; metadata.duplicate().get(bytes);
            CompressedAudio.requireOpusDelay(head, bytes); // MediaCodec's separate delay overrides CSD-0; conflicting originals are unsupported.
        }
        if (epoch != null) {
            CompressedAudio.OggDemux d = new CompressedAudio.OggDemux();
            byte[] original = d.pages(epoch.init()).get(0).packets().get(0);
            if (!Arrays.equals(head, original)) throw CompressedAudio.invalid("EPOCH_CHANGED");
            return;
        }
        ogg = new CompressedAudio.OggMux(UUID.randomUUID().hashCode());
        epoch = new CodecEpoch(epochId, "opus", "ogg", 1, 1, 16000, 48000, delay, ogg.headers(head));
        timeline = new CodecTimeline(48000, delay);
        sink.epoch(epoch);
    }
    private void emit(Packet packet, boolean terminal) throws IOException {
        CodecTimeline.Span span = timeline.advance(packet.samples(), capturedSamples, terminal);
        long end = span.validSamples(), granule = span.granule();
        byte[] payload = OPUS_PROFILE.equals(choice.profile()) ? ogg.packet(packet.bytes(), granule, terminal) : CompressedAudio.adts(packet.bytes(), choice.rate());
        payload = CompressedAudio.join(prefix, payload);
        if (payload.length > MAX_ENCODED_BYTES) throw CompressedAudio.invalid("FRAME_SIZE");
        if (end == validSamples && !terminal) { prefix = payload; return; }
        prefix = new byte[0];
        long startUs = samplesUs(validSamples), endUs = samplesUs(end);
        int flags = portionFlags(end, terminal) | (terminal ? EPOCH_END : 0);
        if (endUs - startUs > MAX_DURATION_US) throw CompressedAudio.invalid("FRAME_DURATION");
        sink.chunk(new EncodedChunk(sourceId, epochId, ++chunkSeq, startUs, (int) (endUs - startUs), flags, payload));
        validSamples = end;
        if (terminal) trim = span.endTrimSamples();
    }
    private void observeVoice(short[] pcm, int count) {
        double sum = 0; for (int i = 0; i < count; i++) { double v = pcm[i]; sum += v * v; }
        double rms = Math.sqrt(sum / count);
        boolean speech = rms >= Math.max(500, noise * 3.2);
        if (!activeInput && !speech) noise = 0.98 * noise + 0.02 * Math.min(rms, 800);
        if (speech) quietSamples = 0; else quietSamples += count;
        if (waitForQuiet) {
            if (quietSamples < choice.rate() * 48 / 100) return;
            waitForQuiet = false;
        }
        boolean next = speech || activeInput && quietSamples < choice.rate() * 48 / 100;
        if (next != activeInput) {
            voice.add(new VoiceMark(capturedSamples, next)); activeInput = next;
            if (next) sink.speechStarted();
        }
    }
    private int portionFlags(long endSample, boolean terminal) {
        int flags = 0;
        while (!voice.isEmpty() && voice.peek().sample() < endSample) {
            boolean next = voice.remove().active();
            if (next && !activePublished) flags |= PORTION_START;
            if (!next && activePublished) flags |= PORTION_END;
            activePublished = next;
        }
        if (terminal && activePublished) { flags |= PORTION_END; activePublished = false; }
        return flags;
    }
    private long samplesUs(long samples) { return samples * 1000000L / choice.rate(); }
    @Override public void close() {
        if (closed) return; closed = true;
        try { codec.stop(); } catch (RuntimeException ignored) { }
        codec.release();
    }
}

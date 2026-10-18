package org.onedayonemasterpiece.live.platform;

import android.media.AudioAttributes;
import android.media.AudioFormat;
import android.media.AudioTrack;
import android.media.MediaCodec;
import android.media.MediaCodecList;
import android.media.MediaFormat;
import android.os.Build;
import android.util.Log;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import static org.onedayonemasterpiece.live.VoiceAudioV2.*;

/** Native compressed decoder and AudioTrack; no PCM or encoded audio crosses a UI bridge. */
final class NativePlayer implements AutoCloseable {
    interface Listener {
        void playing(boolean playing);
        void played(String sourceId, long playedUs);
        void timing(String stage);
        void failure(String code);
        void completed(String sourceId, boolean complete);
    }
    private record Item(long version, String response, CodecEpoch epoch, EncodedChunk chunk, boolean prefixEnd, int bytes) { }
    record PlaybackResume(String sourceId, long playedMs) { }
    private final Object hardware = new Object(), receiptsLock = new Object();
    private final LinkedBlockingQueue<Item> queue = new LinkedBlockingQueue<>(256);
    private final Map<String, Long> pendingEnds = new LinkedHashMap<>();
    private final Map<String, Long> playedPositions;
    private final Listener listener;
    private final AtomicLong version = new AtomicLong();
    private final Thread worker;
    private volatile boolean closed, playing;
    private volatile String activeSource;
    private AudioTrack track;
    private String trackResponse;
    private long trackOriginUs, writtenSamples, lastHead, headWrap, lastReportMs;
    private int trackRate, trackPrefillFrames;
    private boolean trackStarted; // guarded by hardware: queued PCM is not yet physically playing
    private long queuedDuration;
    private int queuedBytes, queuedItems;
    private Decoder decoder;

    NativePlayer(Listener listener, Map<String, Long> playedPositions) {
        this.listener = listener; this.playedPositions = playedPositions;
        worker = new Thread(this::work, "live-native-playback"); worker.setDaemon(true); worker.start();
    }
    static boolean decoderAvailable(String profile) {
        String mime = OPUS_PROFILE.equals(profile) ? "audio/opus" : "audio/mp4a-latm";
        MediaFormat f = MediaFormat.createAudioFormat(mime, OPUS_PROFILE.equals(profile) ? 48000 : 16000, 1);
        String name = new MediaCodecList(MediaCodecList.REGULAR_CODECS).findDecoderForFormat(f);
        if (name == null) return false;
        try { MediaCodec c = MediaCodec.createByCodecName(name); c.release(); return true; }
        catch (IOException | RuntimeException error) { return false; }
    }
    boolean playing() { return playing; }
    String activeSource() { return activeSource; }
    boolean epoch(String response, CodecEpoch epoch) {
        if (closed) return false;
        return offer(new Item(version.get(), response, epoch, null, false, 0));
    }
    boolean audio(EncodedChunk chunk) {
        if (closed) return false;
        return offer(new Item(version.get(), chunk.sourceId(), null, chunk, false, chunk.payload().length));
    }
    /** Drain the available archived prefix without claiming an original EPOCH_END. */
    boolean finishAvailablePrefix(String sourceId) {
        return offer(new Item(version.get(), sourceId, null, null, true, 0));
    }
    private boolean offer(Item item) {
        synchronized (receiptsLock) {
            long duration = item.chunk() == null ? 0 : item.chunk().durationUs();
            if (closed || item.version() != version.get() || queuedItems >= 256
                    || queuedBytes + item.bytes() > 1048576 || queuedDuration + duration > 30000000L) return false;
            if (!queue.offer(item)) return false;
            queuedItems++; queuedBytes += item.bytes(); queuedDuration += duration;
            // Source identity and actual played position are independent of the text/event cursor.
            if (item.chunk() != null) pendingEnds.merge(item.response(), item.chunk().captureStartUs() + duration, Math::max);
            return true;
        }
    }
    PlaybackResume resume() {
        reportPosition();
        synchronized (receiptsLock) {
            if (pendingEnds.isEmpty()) return null;
            String response = pendingEnds.keySet().iterator().next();
            return new PlaybackResume(response, playedPositions.getOrDefault(response, 0L) / 1000);
        }
    }
    /** Barge-in and explicit Stop interrupt hardware before draining anything else. */
    void interrupt() {
        reportPosition();
        synchronized (receiptsLock) {
            version.incrementAndGet(); releaseTrack(); activeSource = null; queue.clear();
            queuedItems = queuedBytes = 0; queuedDuration = 0; pendingEnds.clear();
        }
        worker.interrupt();
    }
    void stopImmediately() { reportPosition(); closed = true; interrupt(); }
    void reportPosition() {
        String response; long position; boolean pending;
        synchronized (hardware) {
            if (track == null || !trackStarted) return; // Never mark prefilling PCM as played.
            try {
                long head = track.getPlaybackHeadPosition() & 0xffffffffL;
                if (head < lastHead) headWrap += 1L << 32;
                lastHead = head;
                long samples = Math.min(writtenSamples, headWrap + head);
                response = trackResponse; position = trackOriginUs + samples * 1000000L / trackRate;
                pending = samples < writtenSamples;
            } catch (RuntimeException ignored) { return; }
        }
        setPlaying(pending);
        playedPositions.merge(response, position, Math::max);
        synchronized (receiptsLock) {
            Long end = pendingEnds.get(response);
            if (end != null && end <= position + 1) pendingEnds.remove(response);
        }
        long now = System.nanoTime() / 1000000L;
        if (now - lastReportMs >= 100) { lastReportMs = now; listener.played(response, position); }
    }
    private void work() {
        ArrayDeque<Item> deferred = new ArrayDeque<>();
        long observedVersion = version.get();
        try {
            while (!closed) {
                if (observedVersion != version.get()) {
                    observedVersion = version.get(); closeDecoder();
                    long keepVersion = observedVersion;
                    deferred.removeIf(item -> item.version() != keepVersion);
                }
                Item item = null;
                for (Iterator<Item> it = deferred.iterator(); it.hasNext();) {
                    Item candidate = it.next();
                    if (decoder == null || decoder.response.equals(candidate.response())) { item = candidate; it.remove(); break; }
                }
                if (item == null) {
                    try { item = queue.poll(50, TimeUnit.MILLISECONDS); }
                    catch (InterruptedException ignored) { continue; }
                }
                reportPosition();
                try {
                    if (item == null) { if (decoder != null) decoder.drain(false); continue; }
                    if (item.version() != version.get()) continue;
                    // Network replay and fresh output may interleave. Keep each original codec state ordered.
                    // Deferred items retain the same global byte/duration/item reservations as queued items.
                    if (decoder != null && !decoder.response.equals(item.response())) { deferred.addLast(item); continue; }
                    consumed(item);
                    if (item.epoch() != null) {
                        if (decoder != null) {
                            if (decoder.epoch.epochId().equals(item.epoch().epochId())) continue; // durable store already checked immutable bytes
                            throw CompressedAudio.invalid("OUTPUT_EPOCH_INCOMPLETE");
                        }
                        decoder = new Decoder(item.response(), item.epoch(), item.version());
                        activeSource = item.response();
                    } else if (item.prefixEnd()) {
                        if (decoder != null) decoder.finishPrefix();
                        completed(item, false);
                    } else {
                        if (decoder == null || !decoder.response.equals(item.response())) throw CompressedAudio.invalid("OUTPUT_EPOCH_MISSING");
                        decoder.audio(item.chunk());
                        if (decoder.ended) completed(item, true);
                    }
                } catch (Exception error) {
                    if ((item == null ? observedVersion : item.version()) == version.get() && !closed) {
                        interrupt(); listener.failure(safeCode(error));
                    }
                }
            }
        } finally { releaseTrack(); closeDecoder(); }
    }
    private void consumed(Item item) {
        synchronized (receiptsLock) {
            if (item.version() != version.get()) return;
            queuedItems--; queuedBytes -= item.bytes();
            if (item.chunk() != null) queuedDuration -= item.chunk().durationUs();
        }
    }
    private void completed(Item item, boolean complete) throws InterruptedException {
        awaitPlayed(item.version());
        if (item.version() != version.get() || closed) return;
        reportPosition(); closeDecoder(); releaseTrack(); activeSource = null;
        synchronized (receiptsLock) { pendingEnds.remove(item.response()); }
        listener.completed(item.response(), complete);
    }
    private void awaitPlayed(long expectedVersion) throws InterruptedException {
        // Very short replies never reach the 200-ms startup threshold. Their
        // terminal decoder event still starts precisely the PCM already queued.
        AudioTrack pending;
        synchronized (hardware) {
            pending = track;
            // Empty decoded output must never get a successful played receipt.
            if (pending != null && writtenSamples == 0)
                throw CompressedAudio.invalid("OUTPUT_EMPTY");
        }
        if (pending != null) startBufferedTrack(pending, expectedVersion, true);
        long deadline = System.nanoTime() + 35000000000L;
        while (!closed && expectedVersion == version.get()) {
            reportPosition();
            synchronized (hardware) {
                if (track == null || (headWrap + lastHead) >= writtenSamples) return;
            }
            if (System.nanoTime() > deadline) throw CompressedAudio.invalid("PLAYBACK_STALLED");
            Thread.sleep(10);
        }
    }
    private void write(byte[] pcm, int rate, String response, long startUs, long expectedVersion) throws IOException {
        if (pcm.length == 0 || closed || expectedVersion != version.get()) return;
        long alreadyPlayedUs = playedPositions.getOrDefault(response, 0L);
        int skip = (int) Math.min(pcm.length / 2, Math.max(0, alreadyPlayedUs - startUs) * rate / 1000000L);
        int offset = skip * 2;
        if (offset == pcm.length) return;
        AudioTrack selected;
        synchronized (hardware) { selected = track; }
        if (selected == null) {
            int minimum = AudioTrack.getMinBufferSize(rate, AudioFormat.CHANNEL_OUT_MONO, AudioFormat.ENCODING_PCM_16BIT);
            if (minimum <= 0) throw CompressedAudio.invalid("OUTPUT_UNSUPPORTED");
            AudioTrack created = new AudioTrack.Builder()
                .setAudioAttributes(new AudioAttributes.Builder().setUsage(AudioAttributes.USAGE_VOICE_COMMUNICATION).setContentType(AudioAttributes.CONTENT_TYPE_SPEECH).build())
                .setAudioFormat(new AudioFormat.Builder().setEncoding(AudioFormat.ENCODING_PCM_16BIT).setSampleRate(rate).setChannelMask(AudioFormat.CHANNEL_OUT_MONO).build())
                .setTransferMode(AudioTrack.MODE_STREAM).setBufferSizeInBytes(Math.max(minimum, rate * 4 / 5)).build(); // >=400 ms
            int startup;
            try {
                startup = PlaybackStartupPolicy.boundedPrefillFrames(rate, created.getBufferSizeInFrames(), created.getBufferCapacityInFrames());
                if (Build.VERSION.SDK_INT >= 31) {
                    // Android S defaults its streaming start threshold to the *whole*
                    // track capacity. Without this explicit bound a short voice
                    // response can remain buffered forever despite play().
                    int actual = created.setStartThresholdInFrames(startup);
                    if (actual < 1 || actual >= created.getBufferSizeInFrames())
                        throw CompressedAudio.invalid("OUTPUT_START_THRESHOLD");
                    startup = Math.max(startup, actual);
                }
                Log.i("LiveNativeAudioTrack", "buffer_frames=" + created.getBufferSizeInFrames()
                        + " capacity_frames=" + created.getBufferCapacityInFrames()
                        + " start_threshold_frames=" + startup);
            } catch (RuntimeException error) {
                created.release();
                throw CompressedAudio.invalid("OUTPUT_START_THRESHOLD");
            }
            synchronized (hardware) {
                if (closed || expectedVersion != version.get()) { created.release(); return; }
                track = selected = created; trackResponse = response; trackOriginUs = startUs + skip * 1000000L / rate;
                trackRate = rate; trackPrefillFrames = startup; writtenSamples = lastHead = headWrap = 0; trackStarted = false;
            }
            // Do not call play() on an empty AudioTrack. Queue 200 ms first;
            // the output device can otherwise underrun before codec data arrives.
        }
        while (offset < pcm.length && !closed && expectedVersion == version.get()) {
            int writeBytes;
            synchronized (hardware) {
                if (track != selected) return;
                writeBytes = trackStarted ? pcm.length - offset :
                    PlaybackStartupPolicy.nextWriteBytes(rate, writtenSamples, pcm.length - offset, trackPrefillFrames);
            }
            int count;
            try { count = selected.write(pcm, offset, writeBytes, AudioTrack.WRITE_BLOCKING); }
            catch (IllegalStateException error) { if (closed || expectedVersion != version.get()) return; throw error; }
            if (count <= 0 || count % 2 != 0) {
                if (closed || expectedVersion != version.get()) return;
                throw CompressedAudio.invalid("OUTPUT_WRITE");
            }
            offset += count;
            synchronized (hardware) {
                if (track != selected) return;
                writtenSamples += count / 2;
            }
            startBufferedTrack(selected, expectedVersion, false);
            reportPosition();
        }
    }
    private void startBufferedTrack(AudioTrack selected, long expectedVersion, boolean finalAudio) {
        boolean started = false;
        synchronized (hardware) {
            if (track == selected && !trackStarted && !closed && expectedVersion == version.get()
                    && PlaybackStartupPolicy.canStart(trackRate, writtenSamples, finalAudio, trackPrefillFrames)) {
                if (Build.VERSION.SDK_INT >= 31 && finalAudio && writtenSamples < trackPrefillFrames) {
                    // Every queued short-response sample is original, but there
                    // are fewer than the normal start threshold. Lower Android's
                    // native threshold too; changing only Java policy stalls.
                    int actual = selected.setStartThresholdInFrames((int) writtenSamples);
                    if (actual > writtenSamples || actual < 1)
                        throw CompressedAudio.invalid("OUTPUT_SHORT_START_THRESHOLD");
                }
                selected.play();
                trackStarted = true;
                setPlaying(true); // ordered with Stop under hardware, no stale true after release
                started = true;
            }
        }
        if (started) listener.timing("audio_scheduled");
    }
    private void releaseTrack() {
        AudioTrack selected;
        synchronized (hardware) { selected = track; track = null; trackStarted = false; trackPrefillFrames = 0; }
        if (selected != null) {
            try { selected.pause(); } catch (RuntimeException ignored) { }
            try { selected.flush(); } catch (RuntimeException ignored) { }
            selected.release();
        }
        setPlaying(false);
    }
    private void setPlaying(boolean next) { if (playing != next) { playing = next; listener.playing(next); } }
    private void closeDecoder() { if (decoder != null) { decoder.close(); decoder = null; } }
    private static String safeCode(Exception e) { String s = e.getMessage(); return s != null && s.matches("LIVE_[A-Z0-9_]+") ? s : "LIVE_NATIVE_PLAYBACK_FAILED"; }
    @Override public void close() { stopImmediately(); }

    private final class Decoder implements AutoCloseable {
        final String response;
        final long ownVersion;
        final CodecEpoch epoch;
        final MediaCodec codec;
        final MediaCodec.BufferInfo info = new MediaCodec.BufferInfo();
        final CompressedAudio.OggDemux ogg;
        long codedSamples, validUs, outputSamples, originUs = -1, nextStart = -1, lastSeq;
        int rate, manualSkip;
        boolean ended, inputEos, firstAudio;
        Decoder(String response, CodecEpoch epoch, long ownVersion) throws IOException {
            this.response = response; this.epoch = epoch; this.ownVersion = ownVersion;
            rate = epoch.codec().equals("opus") ? 48000 : epoch.decodedSampleRate();
            MediaFormat f = MediaFormat.createAudioFormat(epoch.codec().equals("opus") ? "audio/opus" : "audio/mp4a-latm", rate, 1);
            f.setInteger(MediaFormat.KEY_PCM_ENCODING, AudioFormat.ENCODING_PCM_16BIT);
            f.setInteger(MediaFormat.KEY_MAX_INPUT_SIZE, MAX_ENCODED_BYTES);
            if (epoch.codec().equals("opus")) {
                ogg = new CompressedAudio.OggDemux(); List<byte[]> headers = new ArrayList<>();
                for (CompressedAudio.OggPage page : ogg.pages(epoch.init())) headers.addAll(page.packets());
                if (headers.size() != 2 || !CompressedAudio.starts(headers.get(1), 0, "OpusTags")) throw CompressedAudio.invalid("OPUS_HEADERS");
                byte[] head = CompressedAudio.opusHead(headers.get(0));
                if (CompressedAudio.preSkip(head) != epoch.preSkip()) throw CompressedAudio.invalid("OPUS_DELAY");
                f.setByteBuffer("csd-0", ByteBuffer.wrap(head));
                // MediaCodec applies this pre-skip itself; never trim it a second time.
                ByteBuffer delay = ByteBuffer.allocate(8).order(ByteOrder.nativeOrder()).putLong(epoch.preSkip() * 1000000000L / 48000);
                ByteBuffer preroll = ByteBuffer.allocate(8).order(ByteOrder.nativeOrder()).putLong(80000000L);
                delay.flip(); preroll.flip();
                f.setByteBuffer("csd-1", delay); f.setByteBuffer("csd-2", preroll);
            } else {
                ogg = null; f.setByteBuffer("csd-0", ByteBuffer.wrap(CompressedAudio.aacConfig(rate)));
                // Decoder gets raw AAC (no encoder-delay instruction); this layer owns AAC priming.
                manualSkip = epoch.preSkip();
            }
            codec = MediaCodec.createDecoderByType(f.getString(MediaFormat.KEY_MIME));
            try { codec.configure(f, null, null, 0); codec.start(); }
            catch (RuntimeException e) { codec.release(); throw e; }
        }
        void audio(EncodedChunk frame) throws IOException {
            if (frame.chunkSeq() <= lastSeq) return; // immutable duplicate was checked before enqueueing
            if (frame.chunkSeq() != lastSeq + 1) throw CompressedAudio.invalid("OUTPUT_PREFIX_MISSING");
            if (ended || frame.captureStartUs() != nextStart && nextStart >= 0) throw CompressedAudio.invalid("OUTPUT_TIMELINE");
            lastSeq = frame.chunkSeq();
            if (originUs < 0) originUs = frame.captureStartUs();
            nextStart = frame.captureStartUs() + frame.durationUs(); validUs = nextStart - originUs;
            boolean terminal = (frame.flags() & EPOCH_END) != 0;
            List<byte[]> packets = new ArrayList<>();
            if (ogg != null) {
                List<CompressedAudio.OggPage> pages = ogg.pages(frame.payload());
                for (int i = 0; i < pages.size(); i++) {
                    CompressedAudio.OggPage page = pages.get(i);
                    for (byte[] packet : page.packets()) { codedSamples += CompressedAudio.opusSamples(packet); packets.add(packet); }
                    if (page.eos() != (terminal && i == pages.size() - 1)) throw CompressedAudio.invalid("OUTPUT_EOS");
                    if (!page.packets().isEmpty() && (page.granule() > codedSamples || !page.eos() && page.granule() != codedSamples)) throw CompressedAudio.invalid("OUTPUT_GRANULE");
                    if (i == pages.size() - 1) {
                        long expected = Math.max(0, page.granule() - epoch.preSkip());
                        if (Math.abs(expected * 1000000L - validUs * 48000) > 48000) throw CompressedAudio.invalid("OUTPUT_DURATION");
                    }
                }
            } else packets = CompressedAudio.adtsPackets(frame.payload(), rate);
            long work = 0;
            for (byte[] packet : packets) work += ogg != null ? CompressedAudio.opusSamples(packet) : 1024;
            if (work * 1000000L > rate * 240000L) throw CompressedAudio.invalid("OUTPUT_DECODE_WORK");
            if (!firstAudio) { firstAudio = true; listener.timing("first_output_audio"); }
            for (int i = 0; i < packets.size(); i++) {
                byte[] packet = packets.get(i);
                int samples = ogg != null ? CompressedAudio.opusSamples(packet) : 1024;
                // Decoder timestamp is coded packet time, not a fabricated fixed PCM chunk size.
                queuePacket(packet, inputPacketSamples * 1000000L / rate, terminal && i == packets.size() - 1);
                inputPacketSamples += samples;
            }
            if (terminal && packets.isEmpty()) queuePacket(new byte[0], inputPacketSamples * 1000000L / rate, true);
            if (terminal) awaitDecoderEnd();
        }
        void finishPrefix() throws IOException {
            if (!inputEos) queuePacket(new byte[0], inputPacketSamples * 1000000L / rate, true);
            awaitDecoderEnd();
        }
        private void awaitDecoderEnd() throws IOException {
            long deadline = System.nanoTime() + 2000000000L;
            while (!ended && !closed && ownVersion == version.get() && System.nanoTime() < deadline) drain(true);
            if (!ended && !closed && ownVersion == version.get()) throw CompressedAudio.invalid("DECODER_EOS_TIMEOUT");
            long expected = Math.round(validUs * rate / 1000000.0);
            if (ended && Math.abs(outputSamples - expected) > 1) throw CompressedAudio.invalid("OUTPUT_TAIL_MISSING");
        }
        private long inputPacketSamples;
        private void queuePacket(byte[] packet, long pts, boolean terminal) throws IOException {
            long deadline = System.nanoTime() + 2000000000L;
            while (!closed && ownVersion == version.get()) {
                int index = codec.dequeueInputBuffer(10000);
                if (index < 0) { drain(false); if (System.nanoTime() > deadline) throw CompressedAudio.invalid("DECODER_STALLED"); continue; }
                ByteBuffer b = codec.getInputBuffer(index);
                if (b == null || b.capacity() < packet.length) throw CompressedAudio.invalid("DECODER_BUFFER");
                b.clear(); b.put(packet);
                codec.queueInputBuffer(index, 0, packet.length, pts, terminal ? MediaCodec.BUFFER_FLAG_END_OF_STREAM : 0);
                if (terminal) inputEos = true;
                drain(false); return;
            }
        }
        private void drain(boolean waiting) throws IOException {
            while (!closed && ownVersion == version.get()) {
                int index = codec.dequeueOutputBuffer(info, waiting ? 10000 : 0);
                if (index == MediaCodec.INFO_TRY_AGAIN_LATER) return;
                if (index == MediaCodec.INFO_OUTPUT_FORMAT_CHANGED) {
                    MediaFormat f = codec.getOutputFormat();
                    if (f.getInteger(MediaFormat.KEY_CHANNEL_COUNT) != 1 || f.getInteger(MediaFormat.KEY_SAMPLE_RATE) != rate
                            || f.containsKey(MediaFormat.KEY_PCM_ENCODING) && f.getInteger(MediaFormat.KEY_PCM_ENCODING) != AudioFormat.ENCODING_PCM_16BIT) throw CompressedAudio.invalid("DECODER_FORMAT");
                    continue;
                }
                if (index < 0) continue;
                byte[] bytes = new byte[info.size]; boolean terminal = (info.flags & MediaCodec.BUFFER_FLAG_END_OF_STREAM) != 0;
                try {
                    ByteBuffer b = codec.getOutputBuffer(index);
                    if (b != null) { b.position(info.offset); b.limit(info.offset + info.size); b.get(bytes); }
                } finally { codec.releaseOutputBuffer(index, false); }
                if (bytes.length % 2 != 0 || bytes.length > rate * 2 / 4 + 2048) throw CompressedAudio.invalid("DECODED_SIZE");
                int skip = Math.min(manualSkip, bytes.length / 2); manualSkip -= skip;
                long allowed = Math.max(0, Math.round(validUs * rate / 1000000.0) - outputSamples);
                int take = (int) Math.min(bytes.length / 2 - skip, allowed);
                if (!inputEos && bytes.length / 2 - skip > allowed + 1) throw CompressedAudio.invalid("DECODED_DURATION");
                byte[] valid = java.util.Arrays.copyOfRange(bytes, skip * 2, (skip + take) * 2);
                write(valid, rate, response, originUs + outputSamples * 1000000L / rate, ownVersion);
                outputSamples += take;
                if (terminal) { ended = true; return; }
            }
        }
        @Override public void close() { try { codec.stop(); } catch (RuntimeException ignored) { } codec.release(); }
    }
}

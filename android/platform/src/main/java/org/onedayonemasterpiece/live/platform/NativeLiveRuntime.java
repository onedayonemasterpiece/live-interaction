package org.onedayonemasterpiece.live.platform;

import android.Manifest;
import android.content.Context;
import android.content.pm.PackageManager;
import android.media.AudioAttributes;
import android.media.AudioDeviceCallback;
import android.media.AudioDeviceInfo;
import android.media.AudioFocusRequest;
import android.media.AudioFormat;
import android.media.AudioManager;
import android.media.AudioRecord;
import android.media.AudioTimestamp;
import android.media.MediaRecorder;
import android.media.audiofx.AcousticEchoCanceler;
import android.media.audiofx.NoiseSuppressor;
import android.os.Build;
import android.os.Handler;
import android.os.Looper;
import android.os.Process;
import android.util.Log;
import com.google.gson.JsonObject;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import okhttp3.OkHttpClient;
import org.onedayonemasterpiece.live.DurableSourceStore;
import org.onedayonemasterpiece.live.DurableOutputStore;
import org.onedayonemasterpiece.live.LiveSocketTransportV2;
import static org.onedayonemasterpiece.live.VoiceAudioV2.*;

/**
 * Android-owned microphone, VAD, compressed source journal, WSS and playback.
 * A WebView is only an observer/controller. Its lifetime never owns these resources.
 */
public final class NativeLiveRuntime implements AutoCloseable {
    public record Capabilities(boolean supported, List<String> inputProfiles, List<String> outputProfiles, String errorCode) {
        public Capabilities { inputProfiles = List.copyOf(inputProfiles); outputProfiles = List.copyOf(outputProfiles); }
    }
    public record Snapshot(String state, String conversationId, String sessionId, String sourceId, String serverSourceId,
            String codecProfile, boolean capturing, boolean playing, boolean connected, boolean recovering,
            long deviceDurableThrough, long serverCommittedThrough, String errorCode) { }
    public interface Listener {
        void onState(Snapshot state);
        void onError(String code, boolean recoverable);
        default void onEvent(String json) { }
        default void onDeviceDurable(DurablePrefix prefix) { }
        default void onServerDurable(DurablePrefix prefix) { }
        default void onPlayedPosition(String responseId, long playedMs) { }
    }
    private record Pcm(short[] samples, int count, long firstSample, long recordingUnixMs, long rotation) { }
    private record LocalSource(SourceDescriptor descriptor, long firstSample, int connectionGeneration) { }
    private static volatile Capabilities cachedCapabilities;
    private static final Map<String, NativeEncoder.Choice> codecChoices = new ConcurrentHashMap<>();
    private final Context context;
    private final DurableSourceStore store;
    private final DurableOutputStore outputStore;
    private final SessionApi sessionApi;
    private final Listener listener;
    private final Handler callbacks = new Handler(Looper.getMainLooper());
    private final Object lifecycle = new Object(), hardware = new Object();
    private final AtomicLong generation = new AtomicLong();
    private final ExecutorService control = Executors.newSingleThreadExecutor(r -> daemon(r, "live-native-control"));
    private final ScheduledExecutorService network = Executors.newSingleThreadScheduledExecutor(r -> daemon(r, "live-native-network"));
    private final OkHttpClient http = new OkHttpClient.Builder().connectTimeout(5, TimeUnit.SECONDS)
            .readTimeout(0, TimeUnit.MILLISECONDS).writeTimeout(5, TimeUnit.SECONDS).build();
    private volatile Run current;
    private volatile Snapshot terminalSnapshot = new Snapshot("idle", null, null, null, null, null, false, false, false, false, 0, 0, null);
    private volatile boolean closed;
    private String boundConversation;
    private AudioRecord recorder;
    private AcousticEchoCanceler echo;
    private NoiseSuppressor noise;
    private AudioManager audioManager;
    private AudioFocusRequest focus;
    private AudioDeviceCallback routeCallback;
    private boolean chosenSpeaker, legacySpeakerChanged, legacySpeakerBefore;
    private int previousAudioMode;
    private CompletableFuture<Void> finalizing = CompletableFuture.completedFuture(null);

    private final class Run {
        final long token;
        final StartContext start;
        final AtomicBoolean stopped = new AtomicBoolean();
        final AtomicLong finishTurnAt = new AtomicLong(-1);
        final AtomicLong rotation = new AtomicLong();
        final ArrayBlockingQueue<Pcm> pcm = new ArrayBlockingQueue<>(100); // <= 2 seconds of PCM, never an unbounded backlog
        final CompletableFuture<Snapshot> started = new CompletableFuture<>();
        final CompletableFuture<Void> localDone = new CompletableFuture<>();
        final CompletableFuture<Void> outputDone = new CompletableFuture<>();
        final Object outputSubmission = new Object();
        final ThreadPoolExecutor outputIo = new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(256), r -> daemon(r, "live-native-output-store"));
        final Map<String, Long> playedPositions = new ConcurrentHashMap<>();
        final Map<Long, OutputBinding> outputBindings = new ConcurrentHashMap<>();
        final Set<String> suppressedOutputs = ConcurrentHashMap.newKeySet();
        final Set<String> liveOutputs = ConcurrentHashMap.newKeySet();
        final Set<String> completedOutputs = ConcurrentHashMap.newKeySet();
        final OutputResumeQueue outputResumes = new OutputResumeQueue();
        final AtomicBoolean playbackPumpQueued = new AtomicBoolean();
        DurablePlaybackPump playback; // outputIo thread only after prepare
        final AtomicBoolean playedFlush = new AtomicBoolean();
        int outputTasks;
        boolean outputSealed;
        Throwable outputFailure;
        final Map<String, LiveSocketTransportV2.SourceBinding> bindings = new HashMap<>();
        final Map<String, Long> submitted = new HashMap<>();
        final Set<String> opening = new HashSet<>(), registering = new HashSet<>(), epochs = new HashSet<>(), sealing = new HashSet<>(), sealed = new HashSet<>(), unbinding = new HashSet<>();
        volatile boolean captureDone, capturing, connected, recovering;
        volatile String state = "starting", error, sessionId, liveSourceId, serverSourceId, stopReason = "explicit_stop";
        volatile int connectionGeneration;
        volatile long receivedCursor, capturedSamples, encodedSamples, deviceDurable, serverDurable, lastFreshAckNs;
        volatile LocalSource source;
        volatile NativeEncoder.Choice codec;
        volatile NativePlayer player;
        volatile LiveSocketTransportV2 transport;
        volatile NativePlayer.PlaybackResume playbackResume;
        volatile String lastOutputSource;
        volatile boolean connectPending;
        int reconnectCount;
        Run(long token, StartContext start) { this.token = token; this.start = start; }
    }
    public NativeLiveRuntime(Context context, DurableSourceStore store, SessionApi sessionApi, Listener listener) {
        this(context, store, requireOutputStore(store), sessionApi, listener);
    }
    public NativeLiveRuntime(Context context, DurableSourceStore store, DurableOutputStore outputStore, SessionApi sessionApi, Listener listener) {
        this.context = Objects.requireNonNull(context).getApplicationContext();
        this.store = Objects.requireNonNull(store); this.outputStore = Objects.requireNonNull(outputStore);
        this.sessionApi = Objects.requireNonNull(sessionApi); this.listener = Objects.requireNonNull(listener);
    }
    private static DurableOutputStore requireOutputStore(DurableSourceStore store) {
        if (!(store instanceof DurableOutputStore output)) throw new IllegalArgumentException("LIVE_NATIVE_OUTPUT_STORE_REQUIRED");
        return output;
    }
    private static Thread daemon(Runnable r, String name) { Thread t = new Thread(r, name); t.setDaemon(true); return t; }
    /** Actual codec probe, cached per process. It never starts AudioRecord. Call off the UI thread. */
    public static synchronized Capabilities capabilities(Context context) {
        if (cachedCapabilities != null) return cachedCapabilities;
        List<String> inputs = new ArrayList<>(), outputs = new ArrayList<>();
        try {
            if (!context.getPackageManager().hasSystemFeature(PackageManager.FEATURE_MICROPHONE))
                return cachedCapabilities = new Capabilities(false, inputs, outputs, "LIVE_NATIVE_MICROPHONE_UNSUPPORTED");
            for (String profile : List.of(OPUS_PROFILE, AAC_PROFILE)) {
                NativeEncoder.Choice choice = NativeEncoder.find(profile);
                if (NativeEncoder.probe(choice)) { codecChoices.put(profile, choice); inputs.add(profile); }
                if (NativePlayer.decoderAvailable(profile)) outputs.add(profile);
            }
        } catch (RuntimeException error) {
            return cachedCapabilities = new Capabilities(false, inputs, outputs, "LIVE_NATIVE_CODEC_PROBE_FAILED");
        }
        // v2 server downlink is Opus. AAC alone must not masquerade as complete support.
        boolean supported = !inputs.isEmpty() && outputs.contains(OPUS_PROFILE);
        return cachedCapabilities = new Capabilities(supported, inputs, outputs, supported ? null : "LIVE_NATIVE_COMPRESSED_UNSUPPORTED");
    }
    public CompletableFuture<Snapshot> start(StartContext start) {
        Objects.requireNonNull(start);
        final Run run;
        synchronized (lifecycle) {
            if (closed) return failed("LIVE_NATIVE_CLOSED");
            if (boundConversation != null && !boundConversation.equals(start.conversationId())) return failed("LIVE_NATIVE_SCOPE_CHANGE");
            if (current != null && !current.stopped.get()) return current.started;
            if (!finalizing.isDone()) return finalizing.thenCompose(ignored -> start(start));
            boundConversation = start.conversationId();
            run = new Run(generation.incrementAndGet(), start); current = run;
        }
        publish(run); control.execute(() -> prepare(run)); return run.started;
    }
    private void prepare(Run run) {
        try {
            Capabilities caps = capabilities(context);
            if (!active(run)) { run.localDone.complete(null); return; }
            if (!caps.supported()) throw new IllegalStateException(caps.errorCode());
            String profile = caps.inputProfiles().contains(OPUS_PROFILE) ? OPUS_PROFILE
                    : run.start.allowAacCompatibility() && caps.inputProfiles().contains(AAC_PROFILE) ? AAC_PROFILE : null;
            if (profile == null) throw new IllegalStateException("LIVE_NATIVE_COMPRESSED_UNSUPPORTED");
            run.codec = codecChoices.get(profile);
            run.outputResumes.restore(outputStore.playbackPositions(), run.playedPositions, run.suppressedOutputs);
            if (context.checkSelfPermission(Manifest.permission.RECORD_AUDIO) != PackageManager.PERMISSION_GRANTED)
                throw new IllegalStateException("LIVE_NATIVE_MICROPHONE_PERMISSION");
            run.player = player(run);
            run.playback = new DurablePlaybackPump(outputStore, new DurablePlaybackPump.Sink() {
                public boolean epoch(String sourceId, CodecEpoch epoch) { return run.player.epoch(sourceId, epoch); }
                public boolean audio(EncodedChunk chunk) { return run.player.audio(chunk); }
                public boolean finishPrefix(String sourceId) { return run.player.finishAvailablePrefix(sourceId); }
            });
            if (!acquireCapture(run)) { run.localDone.complete(null); return; }
            Thread encoderThread = daemon(() -> encode(run), "live-native-encode-store"); encoderThread.start();
            Thread captureThread = daemon(() -> capture(run), "live-native-capture"); captureThread.start();
            if (active(run)) { run.state = "recording_only"; publish(run); run.started.complete(snapshot()); }
            network.execute(() -> { connect(run); playbackPump(run); });
        } catch (Exception error) {
            fail(run, safeCode(error, "LIVE_NATIVE_START_FAILED"), false);
            run.captureDone = true; run.localDone.complete(null);
        }
    }
    private NativePlayer player(Run run) {
        return new NativePlayer(new NativePlayer.Listener() {
            public void playing(boolean playing) { if (active(run)) publish(run); }
            public void played(String sourceId, long playedUs) {
                if (!active(run)) return;
                // Persist what hardware played, independently of encoded-byte receipt acknowledgement.
                if (run.playedFlush.compareAndSet(false, true)) submitOutput(run, () -> {
                    try {
                        for (var position : run.playedPositions.entrySet()) outputStore.markPlayed(position.getKey(), position.getValue());
                    } finally { run.playedFlush.set(false); }
                });
                LiveSocketTransportV2 t = run.transport; if (t != null) t.playbackPosition(sourceId, playedUs / 1000);
                dispatch(() -> { if (active(run)) listener.onPlayedPosition(sourceId, playedUs / 1000); });
            }
            public void timing(String stage) { nativeTiming(run, stage); }
            public void failure(String code) { fail(run, code, false); }
            public void completed(String sourceId, boolean complete) {
                run.completedOutputs.add(sourceId);
                executeNetwork(() -> outputPlayed(run, sourceId, complete));
            }
        }, run.playedPositions);
    }
    private boolean acquireCapture(Run run) {
        AudioManager manager = (AudioManager) context.getSystemService(Context.AUDIO_SERVICE);
        if (manager == null) throw new IllegalStateException("LIVE_NATIVE_AUDIO_MANAGER");
        AudioFocusRequest request = new AudioFocusRequest.Builder(AudioManager.AUDIOFOCUS_GAIN_TRANSIENT_EXCLUSIVE)
            .setAudioAttributes(new AudioAttributes.Builder().setUsage(AudioAttributes.USAGE_VOICE_COMMUNICATION).setContentType(AudioAttributes.CONTENT_TYPE_SPEECH).build())
            .setOnAudioFocusChangeListener(change -> {
                if (change == AudioManager.AUDIOFOCUS_LOSS || change == AudioManager.AUDIOFOCUS_LOSS_TRANSIENT) {
                    if (active(run)) { dispatch(() -> { if (visible(run)) listener.onError("LIVE_NATIVE_AUDIO_FOCUS_LOST", false); }); stop("audio_focus_lost"); }
                }
            }, callbacks).build();
        int rate = run.codec.rate();
        int minimum = AudioRecord.getMinBufferSize(rate, AudioFormat.CHANNEL_IN_MONO, AudioFormat.ENCODING_PCM_16BIT);
        if (minimum <= 0) throw new IllegalStateException("LIVE_NATIVE_CAPTURE_UNSUPPORTED");
        AudioRecord created = new AudioRecord.Builder().setAudioSource(MediaRecorder.AudioSource.VOICE_COMMUNICATION)
            .setAudioFormat(new AudioFormat.Builder().setEncoding(AudioFormat.ENCODING_PCM_16BIT).setSampleRate(rate).setChannelMask(AudioFormat.CHANNEL_IN_MONO).build())
            .setBufferSizeInBytes(Math.max(minimum, rate * 2 / 5)).build();
        if (created.getState() != AudioRecord.STATE_INITIALIZED) { created.release(); throw new IllegalStateException("LIVE_NATIVE_CAPTURE_UNSUPPORTED"); }
        synchronized (hardware) {
            if (!active(run)) { created.release(); return false; }
            int result = manager.requestAudioFocus(request);
            if (result != AudioManager.AUDIOFOCUS_REQUEST_GRANTED) { created.release(); throw new IllegalStateException("LIVE_NATIVE_AUDIO_FOCUS_DENIED"); }
            audioManager = manager; focus = request; previousAudioMode = manager.getMode(); manager.setMode(AudioManager.MODE_IN_COMMUNICATION);
            // AudioTrack uses VOICE_COMMUNICATION. Many handset defaults route that
            // usage to the quiet earpiece even when Gemini has emitted audio.
            // Prefer loudspeaker only with no wired/Bluetooth/USB route available.
            // The callback relinquishes our request when external audio appears.
            routeCallback = new AudioDeviceCallback() {
                @Override public void onAudioDevicesAdded(AudioDeviceInfo[] devices) { updateCommunicationRoute(run); }
                @Override public void onAudioDevicesRemoved(AudioDeviceInfo[] devices) { updateCommunicationRoute(run); }
            };
            try { manager.registerAudioDeviceCallback(routeCallback, callbacks); }
            catch (RuntimeException unavailable) {
                routeCallback = null;
                Log.w("LiveNativeAudioRoute", "device_callback_unavailable");
            }
            try { updateCommunicationRouteLocked(run, manager); }
            catch (RuntimeException unavailable) {
                Log.w("LiveNativeAudioRoute", "speaker_route_query_failed");
                nativeTiming(run, "speaker_route_query_failed");
            }
            recorder = created;
            if (AcousticEchoCanceler.isAvailable()) { echo = AcousticEchoCanceler.create(created.getAudioSessionId()); if (echo != null) echo.setEnabled(true); }
            if (NoiseSuppressor.isAvailable()) { noise = NoiseSuppressor.create(created.getAudioSessionId()); if (noise != null) noise.setEnabled(true); }
            created.startRecording();
            if (created.getRecordingState() != AudioRecord.RECORDSTATE_RECORDING) throw new IllegalStateException("LIVE_NATIVE_CAPTURE_START_FAILED");
            run.capturing = true; return true;
        }
    }
    private void capture(Run run) {
        Process.setThreadPriority(Process.THREAD_PRIORITY_AUDIO);
        int rate = run.codec.rate(); long startUnix = System.currentTimeMillis(), firstSample = 0; boolean timestampBound = false;
        try {
            while (active(run)) {
                AudioRecord selected;
                synchronized (hardware) { selected = recorder; }
                if (selected == null) break;
                short[] frame = new short[rate / 50];
                int count;
                try { count = selected.read(frame, 0, frame.length, AudioRecord.READ_BLOCKING); }
                catch (RuntimeException error) { if (!active(run)) break; throw error; }
                if (count <= 0) { if (!active(run)) break; throw new IllegalStateException("LIVE_NATIVE_CAPTURE_READ_FAILED"); }
                if (!timestampBound) {
                    AudioTimestamp ts = new AudioTimestamp();
                    try {
                        if (selected.getTimestamp(ts, AudioTimestamp.TIMEBASE_MONOTONIC) == AudioRecord.SUCCESS) {
                            long anchor = ts.nanoTime - ts.framePosition * 1000000000L / rate;
                            startUnix = System.currentTimeMillis() + (anchor - System.nanoTime()) / 1000000L;
                        }
                    } catch (RuntimeException ignored) { /* Stop may release hardware after a successful final read. Keep that read. */ }
                    timestampBound = true;
                }
                Pcm pcm = new Pcm(frame, count, firstSample, startUnix, run.rotation.get());
                firstSample += count; run.capturedSamples = firstSample;
                if (!run.pcm.offer(pcm)) {
                    // One already captured frame is kept while hardware is stopped. Never discard a claimed original.
                    fail(run, "LIVE_NATIVE_STORAGE_BACKPRESSURE", false);
                    if (!run.pcm.offer(pcm, 2, TimeUnit.SECONDS))
                        dispatch(() -> { if (visible(run)) listener.onError("LIVE_NATIVE_CAPTURE_TAIL_NOT_DURABLE", false); });
                    break;
                }
            }
        } catch (InterruptedException error) { Thread.currentThread().interrupt(); fail(run, "LIVE_NATIVE_CAPTURE_INTERRUPTED", false); }
        catch (Exception error) { if (active(run)) fail(run, safeCode(error, "LIVE_NATIVE_CAPTURE_FAILED"), false); }
        finally { run.capturing = false; run.captureDone = true; publish(run); }
    }
    private void encode(Run run) {
        NativeEncoder encoder = null; long rotation = -1;
        try {
            while (!run.captureDone || !run.pcm.isEmpty()) {
                Pcm frame = run.pcm.poll(50, TimeUnit.MILLISECONDS);
                if (frame == null) continue;
                if (encoder == null || rotation != frame.rotation()) {
                    if (encoder != null) { finishSource(run, encoder); encoder.close(); }
                    rotation = frame.rotation();
                    SourceDescriptor descriptor = new SourceDescriptor(UUID.randomUUID().toString(), frame.recordingUnixMs() + frame.firstSample() * 1000L / run.codec.rate());
                    boolean fresh = run.connected && frame.rotation() == run.rotation.get()
                            && run.capturedSamples - frame.firstSample() <= run.codec.rate() / 2;
                    LocalSource source = new LocalSource(descriptor, frame.firstSample(), fresh ? run.connectionGeneration : 0);
                    store.open(descriptor); run.source = source; run.deviceDurable = run.serverDurable = 0; run.serverSourceId = null;
                    run.lastFreshAckNs = System.nanoTime();
                    run.liveSourceId = fresh ? descriptor.sourceId() : null;
                    encoder = new NativeEncoder(run.codec, descriptor.sourceId(), new NativeEncoder.Sink() {
                        public void epoch(CodecEpoch epoch) throws IOException { store.registerEpoch(descriptor.sourceId(), epoch); }
                        public void speechStarted() {
                            if (active(run) && source.connectionGeneration() == run.connectionGeneration && run.connected)
                                interruptOutput(run, "local_speech_started");
                        }
                        public void chunk(EncodedChunk chunk) throws IOException {
                            DurablePrefix receipt = store.append(chunk); // fsync before any local/device acknowledgement or wire submission
                            if (run.source == source) run.deviceDurable = receipt.chunkSeq();
                            dispatch(() -> { if (visible(run)) listener.onDeviceDurable(receipt); });
                            if ((chunk.flags() & PORTION_START) != 0) nativeTiming(run, "speech_start");
                            if ((chunk.flags() & PORTION_END) != 0) nativeTiming(run, "speech_end");
                        }
                    });
                    publish(run);
                }
                encoder.accept(frame.samples(), frame.count());
                run.encodedSamples = frame.firstSample() + frame.count();
                if (run.connected && run.source.connectionGeneration() == 0 && frame.rotation() == run.rotation.get()
                        && run.capturedSamples - frame.firstSample() <= run.codec.rate() / 2)
                    run.rotation.incrementAndGet();
                long finishAt = run.finishTurnAt.get();
                if (finishAt >= 0 && frame.firstSample() + frame.count() >= finishAt && run.finishTurnAt.compareAndSet(finishAt, -1))
                    encoder.finishTurn();
            }
            if (encoder != null) finishSource(run, encoder);
        } catch (Exception error) {
            fail(run, safeCode(error, "LIVE_NATIVE_SOURCE_STORE_FAILED"), false);
            run.localDone.completeExceptionally(new IllegalStateException(safeCode(error, "LIVE_NATIVE_SOURCE_STORE_FAILED")));
            return;
        } finally { if (encoder != null) encoder.close(); }
        run.localDone.complete(null);
    }
    private void finishSource(Run run, NativeEncoder encoder) throws IOException {
        encoder.finish();
        LocalSource source = run.source;
        if (source != null) store.seal(source.descriptor().sourceId(), encoder.validEndUs(), encoder.endTrimSamples());
    }
    private void connect(Run run) {
        if (!active(run) || run.connectPending) return;
        run.connectPending = true; int connectionGeneration = ++run.connectionGeneration;
        String previousSession = run.sessionId;
        try {
            sessionApi.connect(run.start, previousSession, run.receivedCursor, connectionGeneration, false).whenComplete((connection, error) -> {
                if (connection != null && !active(run)) { stopRemote(connection.sessionId(), run.stopReason); return; }
                executeNetwork(() -> {
                    if (!active(run)) {
                        if (connection != null) stopRemote(connection.sessionId(), run.stopReason); return;
                    }
                    if (connectionGeneration != run.connectionGeneration) return; // Expired attachment, not authority to stop the newer same session.
                    if (error != null || connection == null) {
                        SessionApi.ConnectFailure known = connectionFailure(error);
                        String code = known == null ? "LIVE_NATIVE_SESSION_UNAVAILABLE" : known.code();
                        if (known != null && !known.recoverable()) {
                            Log.w("LiveNativeSession", "terminal_connect code=" + code
                                    + " generation=" + connectionGeneration);
                            fail(run, code, false); // No fresh session/token budget on terminal denial.
                        } else {
                            Log.w("LiveNativeSession", "retry_connect code=" + code
                                    + " generation=" + connectionGeneration);
                            disconnected(run, code);
                        }
                        return;
                    }
                    if (connection.connectionGeneration() != connectionGeneration || connection.recoveryOnly()) {
                        stopRemote(connection.sessionId(), "invalid_connection"); fail(run, "LIVE_NATIVE_SESSION_CONFIGURATION", false); return;
                    }
                    if (!Objects.equals(previousSession, connection.sessionId())) {
                        run.receivedCursor = 0;
                    }
                    run.sessionId = connection.sessionId();
                    LiveSocketTransportV2 transport = transport(run, connectionGeneration);
                    run.transport = transport;
                    OutputResumeQueue.Resume resume = run.outputResumes.next(run.playedPositions);
                    run.playbackResume = resume == null ? null : new NativePlayer.PlaybackResume(resume.sourceId(), resume.playedMs());
                    if (run.playbackResume != null) transport.setPlaybackResume(run.playbackResume.sourceId(), run.playbackResume.playedMs());
                    transport.connect(connection.baseUrl(), connection.socketPath(), connection.attemptId(), connection.ticket(),
                            connectionGeneration, run.receivedCursor, List.of(run.codec.profile()), List.of(OPUS_PROFILE), connection.headers())
                        .whenComplete((hello, failure) -> executeNetwork(() -> {
                            if (!active(run) || run.transport != transport) { transport.close(false); return; }
                            if (failure != null) { disconnected(run, "LIVE_NATIVE_SOCKET_UNAVAILABLE"); return; }
                            run.connected = true; run.connectPending = false; run.reconnectCount = 0; run.error = null;
                            run.bindings.clear(); run.submitted.clear(); run.opening.clear(); run.registering.clear(); run.epochs.clear(); run.sealing.clear(); run.sealed.clear(); run.unbinding.clear();
                            run.liveSourceId = null; run.serverSourceId = null; run.rotation.incrementAndGet();
                            // Playback can finish locally while WSS is down. Its
                            // output source is then no longer eligible for audio
                            // recovery, but its last hardware cursor still needs
                            // an idempotent monotone server-side receipt. Replay
                            // the completed-source positions on every new socket
                            // before asking for another output manifest.
                            for (var completed : OutputResumeQueue.completedPlayheads(run.completedOutputs, run.playedPositions).entrySet())
                                transport.playbackPosition(completed.getKey(), completed.getValue());
                            run.state = "delayed"; publish(run); requestNextOutput(run); pump(run, connectionGeneration);
                        }));
                });
            });
        } catch (RuntimeException error) { disconnected(run, "LIVE_NATIVE_SESSION_UNAVAILABLE"); }
    }
    private LiveSocketTransportV2 transport(Run run, int connectionGeneration) {
        return new LiveSocketTransportV2(http, new LiveSocketTransportV2.Listener() {
            public void onDurableAck(DurablePrefix prefix) {
                executeNetwork(() -> {
                    if (!active(run) || connectionGeneration != run.connectionGeneration) return;
                    try {
                        DurablePrefix durable = store.markServerCommitted(prefix);
                        if (run.source != null && run.source.descriptor().sourceId().equals(prefix.sourceId())) {
                            run.serverDurable = durable.chunkSeq(); run.lastFreshAckNs = System.nanoTime();
                        }
                        dispatch(() -> { if (visible(run)) listener.onServerDurable(durable); }); publish(run);
                    } catch (IOException e) { fail(run, "LIVE_NATIVE_RECEIPT_STORE_FAILED", false); }
                });
            }
            public void onSourceBound(LiveSocketTransportV2.SourceBinding source) {
                executeNetwork(() -> {
                    if (active(run) && connectionGeneration == run.connectionGeneration && source.sourceId().equals(run.liveSourceId)) {
                        run.serverSourceId = source.serverSourceId(); publish(run);
                    }
                });
            }
            public void onOutputEpoch(long responseId, long generation, CodecEpoch epoch) {
                // v2 archive identity is mandatory. This legacy callback is never a durable output source.
                if (active(run)) fail(run, "LIVE_NATIVE_OUTPUT_IDENTITY_REQUIRED", false);
            }
            public void onOutputBinding(OutputBinding binding, long generation) {
                if (!active(run) || connectionGeneration != run.connectionGeneration) return;
                run.outputBindings.put(binding.epochRef(), binding);
                if (!binding.sourceId().equals(run.outputResumes.selected())) run.liveOutputs.add(binding.sourceId());
                submitOutput(run, () -> {
                    outputStore.openOutput(new SourceDescriptor(binding.sourceId(), 0)); // unknown origin time is explicit and stable
                    outputStore.registerOutputEpoch(binding.sourceId(), binding.epoch());
                    // An interrupt may fence this binding before its queued open runs.
                    if (run.stopped.get() || run.suppressedOutputs.contains(binding.sourceId())) {
                        run.suppressedOutputs.add(binding.sourceId());
                        outputStore.cancelPlayback(binding.sourceId());
                    }
                    run.playback.register(binding.sourceId(), binding.epoch());
                    drainPlayback(run);
                });
            }
            public void onEncodedAudio(Frame frame) {
                if (!active(run) || connectionGeneration != run.connectionGeneration) return;
                OutputBinding binding = run.outputBindings.get(frame.epochRef());
                if (binding == null || binding.streamRef() != frame.streamRef()) { fail(run, "LIVE_NATIVE_OUTPUT_IDENTITY_REQUIRED", false); return; }
                run.lastOutputSource = binding.sourceId();
                submitOutput(run, () -> {
                    EncodedChunk original = new EncodedChunk(binding.sourceId(), binding.epoch().epochId(), frame.chunkSeq(),
                        frame.startUs(), frame.durationUs(), frame.flags() & EPOCH_END, frame.payload());
                    DurablePrefix receipt = outputStore.appendOutput(new OutputChunk(original, frame.eventSeq()));
                    if (run.stopped.get() || run.suppressedOutputs.contains(binding.sourceId())) {
                        run.suppressedOutputs.add(binding.sourceId());
                        outputStore.cancelPlayback(binding.sourceId());
                    }
                    run.playback.received(binding.sourceId(), receipt.chunkSeq());
                    if (active(run) && connectionGeneration == run.connectionGeneration) {
                        LiveSocketTransportV2 transport = run.transport;
                        // A duplicate may reveal a longer existing device prefix. This connection acknowledges only delivered frames.
                        if (transport != null) transport.outputAcknowledged(binding.sourceId(), Math.min(receipt.chunkSeq(), frame.chunkSeq()));
                    }
                    drainPlayback(run);
                });
            }
            public void onEvent(JsonObject event) {
                if (!active(run) || connectionGeneration != run.connectionGeneration) return;
                String type = event.has("type") ? event.get("type").getAsString() : "";
                if ("interrupted".equals(type)) interruptOutput(run, "provider_interrupted");
                if ("error".equals(type) && event.has("code")) {
                    String code = event.get("code").getAsString();
                    if (code.matches("RESOURCE_[A-Z0-9_]+")
                            || "LIVE_SESSION_CLOSED".equals(code)
                            || "LIVE_SESSION_NOT_FOUND".equals(code)) {
                        Log.w("LiveNativeSession", "terminal_socket code=" + code
                                + " generation=" + connectionGeneration);
                        fail(run, code, false);
                        return;
                    }
                }
                if ("output_available".equals(type) || "output_replayed".equals(type)) {
                    JsonObject copy = event.deepCopy();
                    executeNetwork(() -> { if (active(run) && connectionGeneration == run.connectionGeneration) outputEvent(run, copy, connectionGeneration); });
                }
                // Audio has a binary-only path; no base64/raw PCM can be observed by the UI.
                if (!"audio".equals(type) && !event.has("audio_base64")) {
                    String json = event.toString(); dispatch(() -> { if (active(run) && connectionGeneration == run.connectionGeneration) listener.onEvent(json); });
                }
            }
            public void onFailure(String code) { executeNetwork(() -> {
                if (active(run) && connectionGeneration == run.connectionGeneration) disconnected(run, code);
            }); }
        });
    }
    private void interruptOutput(Run run, String reason) {
        NativePlayer current = run.player;
        Log.i("LiveNativeInterrupt", "stage=interrupt reason=" + reason
                + " connection_generation=" + run.connectionGeneration
                + " source_id=" + (current == null ? "none" : current.activeSource())
                + " thread=" + Thread.currentThread().getName());
        // Fence delayed callbacks immediately; fsync is on the existing output worker.
        fenceOutputPlayback(run);
        NativePlayer player = run.player; if (player != null) player.interrupt();
        submitOutput(run, () -> persistOutputCancellations(run));
        executeNetwork(() -> {
            if (!active(run)) return;
            for (String sourceId : run.suppressedOutputs) run.outputResumes.finished(sourceId);
            run.playbackResume = null;
            // A new user portion has priority. Continue old pending output after the next actual output completes.
        });
    }
    private void fenceOutputPlayback(Run run) {
        for (OutputBinding binding : run.outputBindings.values()) run.suppressedOutputs.add(binding.sourceId());
        NativePlayer.PlaybackResume resume = run.playbackResume;
        if (resume != null) run.suppressedOutputs.add(resume.sourceId());
    }
    private void persistOutputCancellations(Run run) throws IOException {
        // Metadata only. Queued opens/chunks independently recheck the same fence.
        for (var position : outputStore.playbackPositions())
            if (!position.playbackCancelled() && run.suppressedOutputs.contains(position.source().sourceId()))
                outputStore.cancelPlayback(position.source().sourceId());
    }
    private void outputEvent(Run run, JsonObject event, int connectionGeneration) {
        try {
            if ("output_available".equals(event.get("type").getAsString())) {
                if (!event.has("sources") || !event.get("sources").isJsonArray() || event.getAsJsonArray("sources").size() > 8)
                    throw CompressedAudio.invalid("OUTPUT_MANIFEST");
                for (var value : event.getAsJsonArray("sources")) {
                    JsonObject item = value.getAsJsonObject(); String sourceId = id(item.get("source_id").getAsString());
                    if (run.liveOutputs.contains(sourceId)) continue; // original bytes are already arriving on the current live stream
                    long played = Math.max(item.get("played_us").getAsLong(), run.playedPositions.getOrDefault(sourceId, 0L));
                    boolean cancelled = item.get("cancelled").getAsBoolean() || run.suppressedOutputs.contains(sourceId);
                    if (cancelled && run.suppressedOutputs.add(sourceId))
                        submitOutput(run, () -> persistOutputCancellations(run));
                    run.outputResumes.offer(sourceId, played, item.get("duration_us").getAsLong(), item.get("complete").getAsBoolean(), cancelled);
                }
                requestNextOutput(run);
            } else {
                String sourceId = id(event.get("source_id").getAsString());
                boolean cancelled = event.has("cancelled") && event.get("cancelled").getAsBoolean();
                if (cancelled || run.suppressedOutputs.contains(sourceId)) {
                    run.suppressedOutputs.add(sourceId); run.outputResumes.finished(sourceId);
                    submitOutput(run, () -> persistOutputCancellations(run));
                    if (run.playbackResume != null && run.playbackResume.sourceId().equals(sourceId)) run.playbackResume = null;
                    requestNextOutput(run);
                } else if (!event.get("complete").getAsBoolean()) {
                    if (event.get("through_seq").getAsLong() == 0) {
                        run.outputResumes.finished(sourceId); run.playbackResume = null; requestNextOutput(run);
                    } else submitOutput(run, () -> {
                        run.playback.incomplete(sourceId, event.get("through_seq").getAsLong());
                        drainPlayback(run);
                    });
                }
            }
        } catch (Exception error) { fail(run, safeCode(error, "LIVE_NATIVE_OUTPUT_MANIFEST"), false); }
    }
    private void outputPlayed(Run run, String sourceId, boolean complete) {
        if (!active(run)) return;
        run.liveOutputs.remove(sourceId);
        run.outputResumes.finished(sourceId);
        if (run.playbackResume != null && run.playbackResume.sourceId().equals(sourceId)) run.playbackResume = null;
        long playedUs = run.playedPositions.getOrDefault(sourceId, 0L);
        int connectionGeneration = run.connectionGeneration;
        submitOutput(run, () -> {
            outputStore.markPlayed(sourceId, playedUs);
            run.playback.played(sourceId, complete);
            drainPlayback(run);
            executeNetwork(() -> {
                if (!active(run) || connectionGeneration != run.connectionGeneration || !run.connected || run.transport == null) return;
                run.transport.playbackPosition(sourceId, playedUs / 1000);
                run.transport.requestOutputs(); // server persists the preceding position before listing the next bounded manifest
                requestNextOutput(run);
            });
        });
    }
    private void requestNextOutput(Run run) {
        if (!active(run) || !run.connected || run.transport == null) return;
        OutputResumeQueue.Resume resume = run.outputResumes.next(run.playedPositions);
        if (resume == null) return;
        run.playbackResume = new NativePlayer.PlaybackResume(resume.sourceId(), resume.playedMs());
        run.transport.resumeOutput(resume.sourceId(), resume.playedMs());
    }
    private void playbackPump(Run run) {
        if (!active(run)) return;
        if (run.playbackPumpQueued.compareAndSet(false, true)) {
            if (!submitOutput(run, () -> { try { drainPlayback(run); } finally { run.playbackPumpQueued.set(false); } }))
                run.playbackPumpQueued.set(false);
        }
        network.schedule(() -> playbackPump(run), 40, TimeUnit.MILLISECONDS);
    }
    /** Pull only a bounded amount from already synced originals; a full player queue never loses durable output or stops the mic. */
    private void drainPlayback(Run run) throws IOException {
        if (active(run) && run.playback != null) run.playback.pump(run.suppressedOutputs);
    }
    private void pump(Run run, int connectionGeneration) {
        if (!active(run) || connectionGeneration != run.connectionGeneration || !run.connected || run.transport == null) return;
        try {
            List<DurableSourceStore.SourceStatus> sources = new ArrayList<>(store.sources());
            sources.sort(Comparator.comparingInt((DurableSourceStore.SourceStatus s) -> s.source().sourceId().equals(run.liveSourceId) ? 0 : 1)
                    .thenComparingLong(s -> s.source().startedAtUnixMs()));
            boolean recovering = false; int slots = run.bindings.size() + run.opening.size();
            for (DurableSourceStore.SourceStatus status : sources) {
                String id = status.source().sourceId(); boolean recovery = !id.equals(run.liveSourceId);
                if (recovery && (status.deviceDurableThrough() > status.serverCommittedThrough() || status.sealed() && !status.serverSealed())) recovering = true;
                if (status.deviceDurableThrough() == 0) continue;
                if (recovery && status.sealed() && status.serverSealed() && status.serverCommittedThrough() == status.deviceDurableThrough()) {
                    if (run.bindings.containsKey(id) && run.unbinding.add(id)) {
                        LiveSocketTransportV2 transport = run.transport;
                        transport.unbindSource(id).whenComplete((ignored, error) -> executeNetwork(() -> {
                            if (!active(run) || run.transport != transport) return;
                            run.unbinding.remove(id);
                            if (error != null) { disconnected(run, "LIVE_NATIVE_SOURCE_UNBIND_FAILED"); return; }
                            run.bindings.remove(id); run.submitted.remove(id); run.sealed.remove(id);
                            run.epochs.removeIf(key -> key.startsWith(id + "/"));
                        }));
                    }
                    continue;
                }
                LiveSocketTransportV2.SourceBinding binding = run.bindings.get(id);
                if (binding == null) {
                    // Eight recovery sources leave the ninth slot for fresh speech.
                    if (slots >= (recovery ? 8 : 9) || !run.opening.add(id)) continue;
                    slots++;
                    long nowUs = recovery ? status.validEndUs() : sourceNowUs(run, id);
                    LiveSocketTransportV2 transport = run.transport;
                    transport.openSource(status.source(), nowUs, recovery).whenComplete((bound, error) -> executeNetwork(() -> {
                        if (!active(run) || run.transport != transport) return;
                        run.opening.remove(id);
                        if (error != null) { disconnected(run, "LIVE_NATIVE_SOURCE_OPEN_FAILED"); return; }
                        if (bound.serverDurableSeq() < status.serverCommittedThrough()) { fail(run, "LIVE_NATIVE_SERVER_DURABLE_REGRESSION", false); return; }
                        try {
                            // The authenticated bind also repairs a receipt lost after server fsync but before device fsync.
                            DurablePrefix receipt = store.markServerCommitted(new DurablePrefix(id, bound.serverDurableSeq()));
                            if (id.equals(run.liveSourceId)) { run.serverDurable = receipt.chunkSeq(); run.lastFreshAckNs = System.nanoTime(); }
                            run.bindings.put(id, bound); run.submitted.put(id, bound.serverDurableSeq());
                        } catch (IOException failure) { fail(run, "LIVE_NATIVE_RECEIPT_STORE_FAILED", false); }
                    }));
                    continue;
                }
                long cursor = run.submitted.getOrDefault(id, binding.serverDurableSeq());
                List<EncodedChunk> chunks = store.readPending(id, cursor, recovery ? 16384 : 32768, recovery ? 480000 : 960000);
                for (EncodedChunk chunk : chunks) {
                    if (!recovery && sourceNowUs(run, id) - chunk.captureStartUs() - chunk.durationUs() > 2000000L) {
                        disconnected(run, "LIVE_NATIVE_LIVE_BACKLOG_RECOVERY"); return;
                    }
                    String key = id + "/" + chunk.epochId();
                    if (!run.epochs.contains(key)) {
                        if (!run.registering.add(key)) break;
                        CodecEpoch epoch = store.epoch(id, chunk.epochId());
                        LiveSocketTransportV2 transport = run.transport;
                        transport.registerEpoch(binding, epoch).whenComplete((ref, error) -> executeNetwork(() -> {
                            if (!active(run) || run.transport != transport) return;
                            run.registering.remove(key);
                            if (error != null) disconnected(run, "LIVE_NATIVE_EPOCH_FAILED"); else run.epochs.add(key);
                        }));
                        break;
                    }
                    if (!run.transport.submit(chunk, recovery, run.connectionGeneration)) break;
                    run.submitted.put(id, chunk.chunkSeq());
                }
                if (status.sealed() && !status.serverSealed() && run.submitted.getOrDefault(id, 0L) == status.deviceDurableThrough() && run.sealing.add(id)) {
                    LiveSocketTransportV2 transport = run.transport;
                    transport.sealSource(id, status.deviceDurableThrough(), status.validEndUs()).whenComplete((ignored, error) -> executeNetwork(() -> {
                        if (!active(run) || run.transport != transport) return;
                        run.sealing.remove(id);
                        if (error != null) { disconnected(run, "LIVE_NATIVE_SOURCE_SEAL_FAILED"); return; }
                        try { store.markServerSealed(id); run.sealed.add(id); }
                        catch (IOException failure) { fail(run, "LIVE_NATIVE_SEAL_RECEIPT_STORE_FAILED", false); }
                    }));
                }
            }
            String state = run.liveSourceId == null || run.capturedSamples - run.encodedSamples > run.codec.rate() / 2
                || run.deviceDurable > run.serverDurable && System.nanoTime() - run.lastFreshAckNs > 500000000L ? "delayed" : "live";
            if (run.recovering != recovering || !state.equals(run.state)) { run.recovering = recovering; run.state = state; publish(run); }
            run.receivedCursor = run.transport.cursor(); run.player.reportPosition();
        } catch (Exception error) { fail(run, safeCode(error, "LIVE_NATIVE_SOURCE_RECOVERY_FAILED"), false); return; }
        network.schedule(() -> pump(run, connectionGeneration), 40, TimeUnit.MILLISECONDS);
    }
    private long sourceNowUs(Run run, String id) {
        LocalSource source = run.source;
        return source == null || !source.descriptor().sourceId().equals(id) ? 0 : Math.max(0, run.capturedSamples - source.firstSample()) * 1000000L / run.codec.rate();
    }
    private void disconnected(Run run, String code) {
        if (!active(run)) return;
        boolean alreadyWaiting = !run.connected && !run.connectPending;
        if (alreadyWaiting && run.transport == null) return;
        run.connected = false; run.connectPending = false; run.liveSourceId = null; run.serverSourceId = null;
        LiveSocketTransportV2 t = run.transport;
        if (t != null) { run.receivedCursor = t.cursor(); t.close(false); run.transport = null; }
        NativePlayer player = run.player;
        if (player != null) {
            NativePlayer.PlaybackResume resume = player.resume();
            String sourceId = resume == null ? run.lastOutputSource : resume.sourceId();
            if (sourceId != null && (run.suppressedOutputs.contains(sourceId) || run.completedOutputs.contains(sourceId))) sourceId = null;
            run.outputResumes.reconnect(sourceId, sourceId == null ? 0 : run.playedPositions.getOrDefault(sourceId, 0L));
            run.playbackResume = null;
        }
        run.outputBindings.clear(); run.liveOutputs.clear();
        // Native playback continues from the synced device prefix while WSS reconnects. Replayed bytes are immutable duplicates.
        run.error = code; run.state = "recording_only"; run.recovering = true; publish(run);
        dispatch(() -> { if (active(run)) listener.onError(code, true); });
        if (!alreadyWaiting) {
            int waitMs = Math.min(10000, 500 << Math.min(5, run.reconnectCount++));
            network.schedule(() -> connect(run), waitMs, TimeUnit.MILLISECONDS);
        }
    }
    public void finishTurn() { Run run = current; if (run != null && active(run)) run.finishTurnAt.set(run.capturedSamples); }
    /** Hardware release is synchronous. The future is only the local EOS/fsync/seal barrier. */
    public CompletableFuture<Void> stop(String reason) {
        Run run;
        synchronized (lifecycle) {
            run = current;
            if (run == null) return finalizing;
            if (!run.stopped.compareAndSet(false, true)) return finalizing;
            run.stopReason = normalizedReason(reason); generation.incrementAndGet();
            run.state = "stopping"; run.connected = false;
            finalizing = CompletableFuture.allOf(run.localDone, run.outputDone);
        }
        fenceOutputPlayback(run);
        run.suppressedOutputs.addAll(run.outputResumes.pendingSources());
        releaseCapture();
        if (run.player != null) run.player.stopImmediately();
        LiveSocketTransportV2 transport = run.transport; if (transport != null) transport.close(true);
        if (run.sessionId != null) executeNetwork(() -> stopRemote(run.sessionId, run.stopReason));
        submitOutput(run, () -> {
            persistOutputCancellations(run);
            for (var position : run.playedPositions.entrySet()) outputStore.markPlayed(position.getKey(), position.getValue());
        });
        synchronized (run.outputSubmission) {
            run.outputSealed = true;
            if (run.outputTasks == 0) finishOutputStore(run);
        }
        run.started.completeExceptionally(new IllegalStateException("LIVE_NATIVE_STOPPED"));
        publish(run);
        finalizing.whenComplete((ignored, error) -> {
            run.outputIo.shutdown();
            long stopGeneration;
            Snapshot terminal;
            synchronized (lifecycle) {
                if (current != run) return;
                run.state = error != null || run.stopReason.startsWith("LIVE_") ? "error" : "idle";
                if (run.error != null && run.error.endsWith("_UNSUPPORTED")) run.state = "unsupported";
                terminal = new Snapshot(run.state, run.start.conversationId(), run.sessionId,
                    run.source == null ? null : run.source.descriptor().sourceId(), run.serverSourceId,
                    run.codec == null ? null : run.codec.profile(), false, false, false, run.recovering, run.deviceDurable, run.serverDurable, run.error);
                terminalSnapshot = terminal; current = null; stopGeneration = generation.get();
            }
            dispatch(() -> { if (current == null && generation.get() == stopGeneration) listener.onState(terminal); });
        });
        return finalizing;
    }
    private void releaseCapture() {
        AudioRecord selected; AcousticEchoCanceler oldEcho; NoiseSuppressor oldNoise; AudioManager manager; AudioFocusRequest request; AudioDeviceCallback callback;
        boolean restoreLegacy, originalLegacy, clearRoute; int mode;
        synchronized (hardware) {
            selected = recorder; recorder = null; oldEcho = echo; echo = null; oldNoise = noise; noise = null;
            manager = audioManager; audioManager = null; request = focus; focus = null; mode = previousAudioMode;
            callback = routeCallback; routeCallback = null;
            restoreLegacy = legacySpeakerChanged; originalLegacy = legacySpeakerBefore;
            clearRoute = chosenSpeaker;
            legacySpeakerChanged = chosenSpeaker = false;
        }
        if (selected != null) {
            try { selected.stop(); } catch (RuntimeException ignored) { }
            selected.release();
        }
        if (oldEcho != null) oldEcho.release(); if (oldNoise != null) oldNoise.release();
        if (manager != null) {
            if (callback != null) manager.unregisterAudioDeviceCallback(callback);
            if (Build.VERSION.SDK_INT >= 31 && clearRoute) manager.clearCommunicationDevice();
            if (Build.VERSION.SDK_INT < 31 && restoreLegacy) manager.setSpeakerphoneOn(originalLegacy);
            if (request != null) manager.abandonAudioFocusRequest(request);
            manager.setMode(mode);
        }
    }
    /** Route selection is a platform lifecycle decision, never a second audio engine. */
    private void updateCommunicationRoute(Run run) {
        synchronized (hardware) {
            if (active(run) && audioManager != null) {
                try { updateCommunicationRouteLocked(run, audioManager); }
                catch (RuntimeException unavailable) {
                    Log.w("LiveNativeAudioRoute", "route_change_failed");
                    nativeTiming(run, "route_change_failed");
                }
            }
        }
    }
    private void updateCommunicationRouteLocked(Run run, AudioManager manager) {
        List<AudioDeviceInfo> devices;
        if (Build.VERSION.SDK_INT >= 31) devices = manager.getAvailableCommunicationDevices();
        else devices = List.of(manager.getDevices(AudioManager.GET_DEVICES_OUTPUTS));
        List<Integer> types = new ArrayList<>(devices.size());
        AudioDeviceInfo speaker = null;
        for (AudioDeviceInfo d : devices) {
            if (!d.isSink()) continue;
            types.add(d.getType());
            if (d.getType() == AudioDeviceInfo.TYPE_BUILTIN_SPEAKER) speaker = d;
        }
        int current = 0;
        if (Build.VERSION.SDK_INT >= 31) {
            AudioDeviceInfo selected = manager.getCommunicationDevice();
            if (selected != null) current = selected.getType();
        } else current = manager.isSpeakerphoneOn()
                ? AudioDeviceInfo.TYPE_BUILTIN_SPEAKER : AudioDeviceInfo.TYPE_BUILTIN_EARPIECE;
        boolean external = types.stream().anyMatch(CommunicationRoutePolicy::isExternal);
        String decision = "system_default";
        if (external && chosenSpeaker) {
            if (Build.VERSION.SDK_INT >= 31) manager.clearCommunicationDevice();
            else if (legacySpeakerChanged) manager.setSpeakerphoneOn(legacySpeakerBefore);
            chosenSpeaker = legacySpeakerChanged = false;
            decision = "released_to_external";
        } else if (CommunicationRoutePolicy.preferSpeaker(current, types)) {
            if (Build.VERSION.SDK_INT >= 31) {
                if (speaker != null && manager.setCommunicationDevice(speaker)) {
                    chosenSpeaker = true;
                    decision = "speaker_selected";
                } else decision = "speaker_unavailable";
            } else {
                legacySpeakerBefore = manager.isSpeakerphoneOn();
                manager.setSpeakerphoneOn(true);
                chosenSpeaker = legacySpeakerChanged = !legacySpeakerBefore;
                decision = "speaker_selected_legacy";
            }
        } else if (chosenSpeaker && current == AudioDeviceInfo.TYPE_BUILTIN_SPEAKER) {
            decision = "speaker_retained";
        }
        AudioDeviceInfo actual = Build.VERSION.SDK_INT >= 31 ? manager.getCommunicationDevice() : null;
        int observed = actual == null ? current : actual.getType();
        Log.i("LiveNativeAudioRoute", "decision=" + decision + " device_type=" + observed);
        if (active(run)) {
            JsonObject event = new JsonObject();
            event.addProperty("type", "native_audio_route");
            event.addProperty("decision", decision);
            event.addProperty("device_type", observed);
            dispatch(() -> { if (active(run)) listener.onEvent(event.toString()); });
        }
    }
    private void fail(Run run, String code, boolean recoverable) {
        if (!active(run)) return;
        run.error = code;
        if (!recoverable) run.started.completeExceptionally(new IllegalStateException(code));
        dispatch(() -> { if (visible(run)) listener.onError(code, recoverable); });
        if (recoverable) executeNetwork(() -> disconnected(run, code)); else stop(code);
    }
    private void stopRemote(String sessionId, String reason) {
        // The product API has its own bounded HTTP timeouts. This never delays local Stop or closes the store.
        try { sessionApi.stop(sessionId, reason).exceptionally(error -> null); } catch (RuntimeException ignored) { }
    }
    @FunctionalInterface private interface OutputWrite { void run() throws IOException; }
    private boolean submitOutput(Run run, OutputWrite write) {
        synchronized (run.outputSubmission) {
            if (run.outputSealed) return false;
            run.outputTasks++;
        }
        try {
            run.outputIo.execute(() -> {
                try { write.run(); }
                catch (Exception error) {
                    synchronized (run.outputSubmission) { if (run.outputFailure == null) run.outputFailure = new IOException(safeCode(error, "LIVE_NATIVE_OUTPUT_STORE_FAILED")); }
                    fail(run, safeCode(error, "LIVE_NATIVE_OUTPUT_STORE_FAILED"), false);
                } finally { outputTaskFinished(run); }
            });
            return true;
        } catch (RejectedExecutionException error) {
            synchronized (run.outputSubmission) { run.outputFailure = new IOException("LIVE_NATIVE_OUTPUT_STORAGE_BACKPRESSURE"); }
            outputTaskFinished(run); fail(run, "LIVE_NATIVE_OUTPUT_STORAGE_BACKPRESSURE", false); return false;
        }
    }
    private void outputTaskFinished(Run run) {
        synchronized (run.outputSubmission) {
            run.outputTasks--;
            if (run.outputSealed && run.outputTasks == 0) finishOutputStore(run);
        }
    }
    private void finishOutputStore(Run run) {
        if (run.outputFailure == null) run.outputDone.complete(null); else run.outputDone.completeExceptionally(run.outputFailure);
    }
    private boolean active(Run run) { return !closed && current == run && !run.stopped.get() && generation.get() == run.token; }
    private boolean visible(Run run) { return current == run || current == null && generation.get() == run.token + 1; }
    public Snapshot snapshot() {
        Run run = current;
        if (run == null) return terminalSnapshot;
        return new Snapshot(run.state, run.start.conversationId(), run.sessionId, run.source == null ? null : run.source.descriptor().sourceId(),
            run.serverSourceId, run.codec == null ? null : run.codec.profile(), run.capturing && !run.stopped.get(),
            run.player != null && run.player.playing() && !run.stopped.get(), run.connected, run.recovering, run.deviceDurable, run.serverDurable, run.error);
    }
    private void publish(Run run) { if (current == run) dispatch(() -> { if (current == run) listener.onState(snapshot()); }); }
    private void nativeTiming(Run run, String stage) {
        if (!active(run)) return;
        JsonObject event = new JsonObject(); event.addProperty("type", "native_timing"); event.addProperty("stage", stage);
        event.addProperty("at_monotonic_ms", System.nanoTime() / 1000000L);
        dispatch(() -> { if (active(run)) listener.onEvent(event.toString()); });
    }
    private void dispatch(Runnable callback) { callbacks.post(() -> { try { callback.run(); } catch (RuntimeException ignored) { } }); }
    private void executeNetwork(Runnable work) { if (!network.isShutdown()) { try { network.execute(work); } catch (RuntimeException ignored) { } } }
    // CompletableFuture wraps the product's typed failure; do not lose its
    // resource/quota/authorization code behind a generic reconnect symptom.
    static SessionApi.ConnectFailure connectionFailure(Throwable failure) {
        while (failure instanceof CompletionException || failure instanceof ExecutionException) {
            if (failure.getCause() == null || failure.getCause() == failure) break;
            failure = failure.getCause();
        }
        return failure instanceof SessionApi.ConnectFailure known ? known : null;
    }

    private static String normalizedReason(String reason) { return reason != null && reason.matches("[A-Za-z][A-Za-z0-9_]{0,63}") ? reason : "explicit_stop"; }
    private static String safeCode(Exception error, String fallback) { String value = error.getMessage(); return value != null && value.matches("LIVE_[A-Z0-9_]+") ? value : fallback; }
    private static <T> CompletableFuture<T> failed(String code) { CompletableFuture<T> f = new CompletableFuture<>(); f.completeExceptionally(new IllegalStateException(code)); return f; }
    @Override public void close() {
        CompletableFuture<Void> done = stop("runtime_closed");
        synchronized (lifecycle) { closed = true; }
        done.whenComplete((ignored, error) -> {
            control.shutdownNow(); network.shutdownNow(); http.dispatcher().executorService().shutdown(); http.connectionPool().evictAll();
        });
    }
}

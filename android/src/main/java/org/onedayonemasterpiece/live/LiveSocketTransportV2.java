package org.onedayonemasterpiece.live;

import com.google.gson.*;
import okhttp3.*;
import okio.ByteString;
import java.util.*;
import java.util.concurrent.*;
import static org.onedayonemasterpiece.live.VoiceAudioV2.*;

/** One authenticated compressed socket. Device persistence stays with its caller. */
public final class LiveSocketTransportV2 implements AutoCloseable {
    public record Hello(String uplinkProfile, String downlinkProfile, long generation, JsonObject limits) { }
    public record SourceBinding(String sourceId, String serverSourceId, long sourceRef, long serverDurableSeq, boolean recovery) { }
    public interface Listener {
        default void onReady(Hello hello) { }
        default void onSourceBound(SourceBinding source) { }
        void onDurableAck(DurablePrefix prefix);
        default void onOutputEpoch(long responseId, long generation, CodecEpoch epoch) { }
        default void onOutputEpoch(long responseId, long generation, long epochRef, CodecEpoch epoch) {
            onOutputEpoch(responseId, generation, epoch);
        }
        default void onOutputBinding(OutputBinding binding, long generation) {
            onOutputEpoch(binding.providerResponseId(), generation, binding.epochRef(), binding.epoch());
        }
        void onEncodedAudio(Frame frame);
        void onEvent(JsonObject event);
        void onFailure(String code);
    }
    private record EpochKey(String sourceId, String epochId) { }
    private record ChunkKey(String sourceId, long seq) { }
    private record Pending(int bytes, int durationUs, boolean recovery, long sentAt) { }
    private final Object lock = new Object();
    private final OkHttpClient client;
    private final Listener listener;
    private final Gson gson = new Gson();
    private final ScheduledExecutorService timer = Executors.newSingleThreadScheduledExecutor(r -> { Thread t = new Thread(r, "live-v2-clock"); t.setDaemon(true); return t; });
    private final Map<String, SourceBinding> sources = new HashMap<>();
    private final Map<String, SourceDescriptor> sourceDescriptors = new HashMap<>();
    private final Map<String, Boolean> sourceLanes = new HashMap<>();
    private final Map<String, CompletableFuture<SourceBinding>> opening = new HashMap<>();
    private final Map<EpochKey, CompletableFuture<Long>> registering = new HashMap<>();
    private final Map<EpochKey, Long> epochs = new HashMap<>();
    private final Map<EpochKey, CodecEpoch> epochDescriptors = new HashMap<>();
    private final Map<String, CompletableFuture<Void>> sealing = new HashMap<>();
    private final Map<String, CompletableFuture<Void>> unbinding = new HashMap<>();
    private final Set<String> sourceComplete = new HashSet<>();
    private final Map<String, Long> acknowledged = new HashMap<>(), submitted = new HashMap<>();
    private final Map<ChunkKey, Pending> pending = new LinkedHashMap<>();
    private final Map<Long, OutputBinding> outputEpochs = new HashMap<>();
    private final Map<Long, Long> outputSequences = new HashMap<>();
    private final Map<String, Long> outputDurable = new LinkedHashMap<>();
    private WebSocket socket;
    private CompletableFuture<Hello> connected;
    private long generation, eventCursor, openedAt, lastPing, lastPong;
    private boolean ready, closed;
    private int liveBytes, recoveryBytes, liveDuration, recoveryDuration;
    private int liveByteLimit = 32768, recoveryByteLimit = 16384, liveDurationLimit = 960000, recoveryDurationLimit = 480000;
    private Set<String> offeredUp, offeredDown;
    private Hello hello;
    private JsonObject playbackResume;

    public LiveSocketTransportV2(OkHttpClient client, Listener listener) {
        // An auth header may never follow a redirect to another host.
        this.client = Objects.requireNonNull(client).newBuilder().followRedirects(false).followSslRedirects(false).build();
        this.listener = Objects.requireNonNull(listener);
    }
    public CompletableFuture<Hello> connect(String baseUrl, String socketPath, String attemptId, String ticket,
            int generation, long cursor, List<String> uplinkProfiles, List<String> downlinkProfiles) {
        return connect(baseUrl, socketPath, attemptId, ticket, generation, cursor, uplinkProfiles, downlinkProfiles, Map.of());
    }
    public CompletableFuture<Hello> connect(String baseUrl, String socketPath, String attemptId, String ticket,
            int generation, long cursor, List<String> uplinkProfiles, List<String> downlinkProfiles, Map<String, String> extraHeaders) {
        synchronized (lock) {
            if (connected != null || closed) throw new IllegalStateException("LIVE_SOCKET_SINGLE_USE");
            HttpUrl base = HttpUrl.get(baseUrl);
            if (!"https".equals(base.scheme()) || !base.username().isEmpty() || !base.password().isEmpty()
                    || base.query() != null || base.fragment() != null || (!base.encodedPath().equals("/") && !base.encodedPath().isEmpty())
                    || ticket == null || !ticket.matches("[A-Za-z0-9_-]{20,512}")) throw invalid("CONNECTION");
            id(attemptId); integer(generation, 1, 0xffffffffL); integer(cursor, 0, MAX_SAFE_INTEGER);
            if (uplinkProfiles == null || downlinkProfiles == null || uplinkProfiles.isEmpty() || downlinkProfiles.isEmpty()
                    || uplinkProfiles.size() > 8 || downlinkProfiles.size() > 8) throw invalid("CAPABILITIES");
            this.generation = generation; this.eventCursor = cursor;
            offeredUp = Set.copyOf(uplinkProfiles); offeredDown = Set.copyOf(downlinkProfiles);
            connected = new CompletableFuture<>(); openedAt = now();
            HttpUrl url = base.resolve(socketPath);
            if (url == null || !url.scheme().equals(base.scheme()) || !url.host().equals(base.host()) || url.port() != base.port()
                    || !url.username().isEmpty() || !url.password().isEmpty() || url.query() != null || url.fragment() != null) throw invalid("ORIGIN");
            Request.Builder request = new Request.Builder().url(url)
                .header("Origin", base.scheme() + "://" + base.host() + (base.port() == 443 ? "" : ":" + base.port()))
                .header("Sec-WebSocket-Protocol", PROTOCOL + ", wl-ticket." + ticket);
            for (var entry : extraHeaders.entrySet()) {
                if (!entry.getKey().equalsIgnoreCase("Cookie") && !entry.getKey().equalsIgnoreCase("Authorization")) throw invalid("AUTH_HEADER");
                request.header(entry.getKey(), entry.getValue());
            }
            socket = client.newWebSocket(request.build(), new WebSocketListener() {
                @Override public void onOpen(WebSocket ws, Response response) {
                    synchronized (lock) {
                        if (closed) { ws.cancel(); return; }
                        socket = ws;
                        if (!PROTOCOL.equals(response.header("Sec-WebSocket-Protocol"))) { fail("LIVE_SOCKET_PROTOCOL"); return; }
                        JsonObject message = object("type", "hello", "protocol", PROTOCOL, "attempt_id", attemptId);
                        message.addProperty("connection_generation", generation); message.addProperty("cursor", cursor);
                        JsonObject caps = new JsonObject(); caps.add("uplink_codecs", gson.toJsonTree(uplinkProfiles)); caps.add("downlink_codecs", gson.toJsonTree(downlinkProfiles));
                        message.add("capabilities", caps);
                        if (playbackResume != null) message.add("playback_resume", playbackResume.deepCopy());
                        send(message);
                    }
                }
                @Override public void onMessage(WebSocket ws, String text) { receiveText(text); }
                @Override public void onMessage(WebSocket ws, ByteString bytes) { receiveAudio(bytes.toByteArray()); }
                @Override public void onFailure(WebSocket ws, Throwable error, Response response) { fail("LIVE_SOCKET_IO"); }
                @Override public void onClosed(WebSocket ws, int code, String reason) { fail("LIVE_SOCKET_CLOSED"); }
            });
            timer.scheduleAtFixedRate(this::tick, 100, 100, TimeUnit.MILLISECONDS);
            return connected;
        }
    }
    public CompletableFuture<SourceBinding> openSource(SourceDescriptor source, long sourceTimeNowUs, boolean recovery) {
        synchronized (lock) {
            requireReady(); integer(sourceTimeNowUs, 0, MAX_SAFE_INTEGER);
            if (sourceDescriptors.containsKey(source.sourceId()) && (!sourceDescriptors.get(source.sourceId()).equals(source)
                    || sourceLanes.get(source.sourceId()) != recovery)) throw invalid("SOURCE_CONFLICT");
            if (sources.containsKey(source.sourceId())) return CompletableFuture.completedFuture(sources.get(source.sourceId()));
            if (opening.containsKey(source.sourceId())) return opening.get(source.sourceId());
            if (sources.size() + opening.size() >= 9) throw invalid("SOURCE_LIMIT");
            CompletableFuture<SourceBinding> result = new CompletableFuture<>(); opening.put(source.sourceId(), result); sourceLanes.put(source.sourceId(), recovery);
            sourceDescriptors.put(source.sourceId(), source);
            JsonObject m = object("type", "source_open", "source_id", source.sourceId(), "lane", recovery ? "recovery" : "live");
            m.addProperty("started_at_unix_ms", source.startedAtUnixMs()); m.addProperty("source_time_now_us", sourceTimeNowUs);
            send(m); return result;
        }
    }
    public CompletableFuture<Long> registerEpoch(SourceBinding source, CodecEpoch epoch) {
        synchronized (lock) {
            requireReady(); EpochKey key = new EpochKey(source.sourceId(), epoch.epochId());
            if (!sources.containsKey(source.sourceId())) throw invalid("SOURCE");
            if (epochDescriptors.containsKey(key) && !epochDescriptors.get(key).equals(epoch)) throw invalid("EPOCH_CONFLICT");
            if (epochs.containsKey(key)) return CompletableFuture.completedFuture(epochs.get(key));
            if (registering.containsKey(key)) return registering.get(key);
            if (epochs.size() + registering.size() >= 32) throw invalid("EPOCH_LIMIT");
            CompletableFuture<Long> result = new CompletableFuture<>(); registering.put(key, result);
            epochDescriptors.put(key, epoch);
            JsonObject m = object("type", "codec_epoch"); m.addProperty("source_ref", source.sourceRef()); m.add("descriptor", epochJson(epoch));
            send(m); return result;
        }
    }
    /** False leaves the already durable chunk with the caller; no volatile queue replay. */
    public boolean submit(EncodedChunk chunk, boolean recovery, long recoveryGeneration) {
        synchronized (lock) {
            if (!ready || closed) return false;
            SourceBinding source = sources.get(chunk.sourceId()); Long epoch = epochs.get(new EpochKey(chunk.sourceId(), chunk.epochId()));
            if (source == null || epoch == null || source.recovery() != recovery) return false;
            ChunkKey key = new ChunkKey(chunk.sourceId(), chunk.chunkSeq());
            if (pending.containsKey(key)) return true;
            long previous = submitted.getOrDefault(chunk.sourceId(), source.serverDurableSeq());
            if (chunk.chunkSeq() <= acknowledged.getOrDefault(chunk.sourceId(), 0L)) return false;
            if (chunk.chunkSeq() != previous + 1) throw invalid("SEQUENCE");
            if ((recovery ? recoveryBytes : liveBytes) + chunk.encodedBytes() > (recovery ? recoveryByteLimit : liveByteLimit)
                    || (recovery ? recoveryDuration : liveDuration) + chunk.durationUs() > (recovery ? recoveryDurationLimit : liveDurationLimit)
                    || pending.size() >= 32) return false;
            Frame frame = new Frame(INPUT_MAGIC, chunk.flags() | (recovery ? RECOVERY : 0), source.sourceRef(), epoch,
                chunk.chunkSeq(), chunk.captureStartUs(), chunk.durationUs(), recovery ? recoveryGeneration : generation, 0, chunk.payload());
            if (!socket.send(ByteString.of(frame.encode()))) { fail("LIVE_SOCKET_IO"); return false; }
            pending.put(key, new Pending(chunk.encodedBytes(), chunk.durationUs(), recovery, now())); submitted.put(chunk.sourceId(), chunk.chunkSeq());
            if (recovery) { recoveryBytes += chunk.encodedBytes(); recoveryDuration += chunk.durationUs(); }
            else { liveBytes += chunk.encodedBytes(); liveDuration += chunk.durationUs(); }
            return true;
        }
    }
    public CompletableFuture<Void> sealSource(String sourceId, long lastSeq, long captureEndUs) {
        synchronized (lock) {
            requireReady(); SourceBinding source = sources.get(sourceId); if (source == null) throw invalid("SOURCE");
            if (sealing.containsKey(sourceId)) return sealing.get(sourceId);
            integer(lastSeq, 0, MAX_SAFE_INTEGER); integer(captureEndUs, 0, MAX_SAFE_INTEGER);
            CompletableFuture<Void> result = new CompletableFuture<>(); sealing.put(sourceId, result);
            JsonObject m = object("type", "source_seal"); m.addProperty("source_ref", source.sourceRef()); m.addProperty("last_chunk_seq", lastSeq); m.addProperty("capture_end_us", captureEndUs);
            send(m); return result;
        }
    }
    public boolean sendInput(JsonObject input) {
        synchronized (lock) {
            requireReady(); if (input.has("audio_base64") || input.has("_audio_projection")) throw invalid("FRAME");
            JsonObject m = object("type", "input"); m.add("message", input.deepCopy()); return send(m);
        }
    }
    /** Release only socket references. Durable source bytes/disposition stay with the product. */
    public CompletableFuture<Void> unbindSource(String sourceId) {
        synchronized (lock) {
            requireReady(); SourceBinding source = sources.get(sourceId);
            if (source == null || (!source.recovery() && !sourceComplete.contains(sourceId))
                    || pending.keySet().stream().anyMatch(k -> k.sourceId().equals(sourceId))) throw invalid("SOURCE_PENDING");
            if (unbinding.containsKey(sourceId)) return unbinding.get(sourceId);
            CompletableFuture<Void> result = new CompletableFuture<>(); unbinding.put(sourceId, result);
            JsonObject m = object("type", "source_unbind"); m.addProperty("source_ref", source.sourceRef()); send(m); return result;
        }
    }
    public void requestOutputs() {
        synchronized (lock) { requireReady(); send(object("type", "output_list")); }
    }
    public void playbackPosition(long responseId, long playedMs) {
        synchronized (lock) {
            if (!ready || closed) return;
            integer(responseId, 1, 0xffffffffL); integer(playedMs, 0, MAX_SAFE_INTEGER);
            JsonObject m = object("type", "playback_position"); m.addProperty("response_id", responseId); m.addProperty("played_ms", playedMs); m.addProperty("connection_generation", generation); send(m);
        }
    }
    public void playbackPosition(String sourceId, long playedMs) {
        synchronized (lock) {
            if (!ready || closed) return;
            id(sourceId); integer(playedMs, 0, MAX_SAFE_INTEGER);
            JsonObject m = object("type", "playback_position", "source_id", sourceId);
            m.addProperty("played_ms", playedMs); m.addProperty("connection_generation", generation); send(m);
        }
    }
    /** Caller has synced the original bytes; receiving or enqueueing playback is not enough. */
    public void outputAcknowledged(String sourceId, long contiguousDeviceSeq) {
        synchronized (lock) {
            if (!ready || closed) return;
            id(sourceId); integer(contiguousDeviceSeq, 0, outputDurable.getOrDefault(sourceId, -1L));
            JsonObject m = object("type", "output_ack", "source_id", sourceId, "stage", "device_durable");
            m.addProperty("chunk_seq", contiguousDeviceSeq); send(m);
        }
    }
    public void setPlaybackResume(String sourceId, long playedMs) {
        synchronized (lock) {
            if (connected != null || closed) throw new IllegalStateException("LIVE_SOCKET_SINGLE_USE");
            id(sourceId); integer(playedMs, 0, MAX_SAFE_INTEGER);
            playbackResume = object("source_id", sourceId); playbackResume.addProperty("played_ms", playedMs);
        }
    }
    /** Bounded additional pending responses use the same recovery lane after hello. */
    public void resumeOutput(String sourceId, long playedMs) {
        synchronized (lock) {
            requireReady(); id(sourceId); integer(playedMs, 0, MAX_SAFE_INTEGER);
            JsonObject m = object("type", "output_resume", "source_id", sourceId); m.addProperty("played_ms", playedMs); send(m);
        }
    }
    /** Actual playback position, independent of the received text/event cursor. */
    public void setPlaybackResume(long responseId, long playedMs) {
        synchronized (lock) {
            if (connected != null || closed) throw new IllegalStateException("LIVE_SOCKET_SINGLE_USE");
            integer(responseId, 1, 0xffffffffL); integer(playedMs, 0, MAX_SAFE_INTEGER);
            playbackResume = new JsonObject(); playbackResume.addProperty("response_id", responseId); playbackResume.addProperty("played_ms", playedMs);
        }
    }
    private void receiveText(String text) {
        try {
            if (text.length() > 262144) throw invalid("OUTPUT_SIZE");
            JsonObject m = JsonParser.parseString(text).getAsJsonObject(); String type = string(m, "type");
            synchronized (lock) {
                if (closed) return;
                if ("hello_ack".equals(type)) {
                    if (ready || !PROTOCOL.equals(string(m, "protocol")) || number(m, "connection_generation", 1, 0xffffffffL) != generation) throw invalid("PROTOCOL");
                    String up = string(m, "uplink_profile"), down = string(m, "downlink_profile");
                    if (!offeredUp.contains(up) || !offeredDown.contains(down)) throw invalid("CODEC");
                    JsonObject limits = m.getAsJsonObject("audio_limits");
                    liveByteLimit = (int) number(limits, "live_window_bytes", 1, 32768); recoveryByteLimit = (int) number(limits, "recovery_window_bytes", 1, 16384);
                    liveDurationLimit = (int) number(limits, "live_window_duration_us", 1, 960000); recoveryDurationLimit = (int) number(limits, "recovery_window_duration_us", 1, 480000);
                    hello = new Hello(up, down, generation, limits.deepCopy()); ready = true; lastPing = lastPong = now(); connected.complete(hello); listener.onReady(hello); return;
                }
                requireReady();
                if ("pong".equals(type)) { lastPong = now(); return; }
                if ("source_bound".equals(type)) {
                    String id = string(m, "source_id"); CompletableFuture<SourceBinding> future = opening.remove(id); if (future == null) throw invalid("SOURCE");
                    SourceBinding source = new SourceBinding(id, string(m, "server_source_id"), number(m, "source_ref", 1, 0xffffffffL), number(m, "server_durable_seq", 0, MAX_SAFE_INTEGER), sourceLanes.get(id));
                    sources.put(id, source); acknowledged.put(id, source.serverDurableSeq()); submitted.put(id, source.serverDurableSeq());
                    future.complete(source); listener.onSourceBound(source); listener.onDurableAck(new DurablePrefix(id, source.serverDurableSeq())); return;
                }
                if ("epoch_bound".equals(type)) {
                    EpochKey key = new EpochKey(string(m, "source_id"), string(m, "epoch_id")); CompletableFuture<Long> future = registering.remove(key);
                    if (future == null) throw invalid("EPOCH"); long ref = number(m, "epoch_ref", 1, 0xffffffffL); epochs.put(key, ref); future.complete(ref); return;
                }
                if ("audio_ack".equals(type)) {
                    String id = string(m, "source_id"); long seq = number(m, "chunk_seq", 0, MAX_SAFE_INTEGER);
                    if (!"server_durable".equals(string(m, "stage")) || !sources.containsKey(id) || seq < acknowledged.get(id) || seq > submitted.get(id)) throw invalid("ACK_SEQUENCE");
                    acknowledged.put(id, seq);
                    var iterator = pending.entrySet().iterator();
                    while (iterator.hasNext()) { var entry = iterator.next(); if (entry.getKey().sourceId().equals(id) && entry.getKey().seq() <= seq) {
                        Pending p = entry.getValue(); if (p.recovery()) { recoveryBytes -= p.bytes(); recoveryDuration -= p.durationUs(); } else { liveBytes -= p.bytes(); liveDuration -= p.durationUs(); } iterator.remove();
                    } }
                    listener.onDurableAck(new DurablePrefix(id, seq)); return;
                }
                if ("source_sealed".equals(type)) {
                    String sourceId = string(m, "source_id");
                    if (m.has("complete") && m.get("complete").getAsBoolean()) sourceComplete.add(sourceId);
                    CompletableFuture<Void> f = sealing.remove(sourceId); if (f != null) f.complete(null); return;
                }
                if ("source_unbound".equals(type)) {
                    String sourceId = string(m, "source_id"); CompletableFuture<Void> future = unbinding.remove(sourceId);
                    if (future == null) throw invalid("SOURCE");
                    sources.remove(sourceId); sourceDescriptors.remove(sourceId); sourceLanes.remove(sourceId);
                    acknowledged.remove(sourceId); submitted.remove(sourceId);
                    sourceComplete.remove(sourceId);
                    epochs.keySet().removeIf(k -> k.sourceId().equals(sourceId));
                    epochDescriptors.keySet().removeIf(k -> k.sourceId().equals(sourceId));
                    future.complete(null); return;
                }
                if ("output_epoch".equals(type)) {
                    if (number(m, "connection_generation", 1, 0xffffffffL) != generation) throw invalid("OUTPUT_GENERATION");
                    long ref = number(m, "epoch_ref", 1, 0xffffffffL), response = number(m, "provider_response_id", 1, 0xffffffffL);
                    long streamRef = number(m, "stream_ref", 1, 0xffffffffL);
                    CodecEpoch epoch = epochFromJson(m.getAsJsonObject("descriptor"));
                    if (!epoch.profile().equals(hello.downlinkProfile()) || outputEpochs.containsKey(ref) || outputEpochs.size() >= 32) throw invalid("EPOCH");
                    OutputBinding output = new OutputBinding(string(m, "source_id"), string(m, "server_source_id"), streamRef, response, ref, epoch);
                    outputEpochs.put(ref, output);
                    outputDurable.put(output.sourceId(), number(m, "server_durable_seq", 0, MAX_SAFE_INTEGER));
                    while (outputDurable.size() > 64) outputDurable.remove(outputDurable.keySet().iterator().next());
                    listener.onOutputBinding(output, generation); return;
                }
                if ("event".equals(type)) {
                    JsonObject event = m.getAsJsonObject("event"); long seq = number(event, "seq", 1, MAX_SAFE_INTEGER);
                    if (seq > eventCursor) {
                        eventCursor = seq;
                        listener.onEvent(event);
                    } return;
                }
                if ("transport_notice".equals(type) || "output_available".equals(type) || "output_replayed".equals(type)) { listener.onEvent(m); return; }
                if ("output_interrupt".equals(type)) { JsonObject event = m.deepCopy(); event.addProperty("type", "interrupted"); listener.onEvent(event); return; }
                throw invalid("PROTOCOL");
            }
        } catch (Exception error) { fail(error.getMessage() != null && error.getMessage().startsWith("LIVE_") ? error.getMessage() : "LIVE_SOCKET_PROTOCOL"); }
    }
    private void receiveAudio(byte[] data) {
        try {
            Frame frame = Frame.decode(data);
            synchronized (lock) {
                if (closed) return; requireReady(); OutputBinding epoch = outputEpochs.get(frame.epochRef());
                if (frame.magic() != OUTPUT_MAGIC || frame.deliveryGeneration() != generation || epoch == null || epoch.streamRef() != frame.streamRef()) throw invalid("OUTPUT_GENERATION");
                long previous = outputSequences.getOrDefault(frame.epochRef(), 0L);
                if (frame.chunkSeq() != previous + 1) throw invalid("OUTPUT_SEQUENCE");
                outputSequences.put(frame.epochRef(), frame.chunkSeq());
                outputDurable.put(epoch.sourceId(), Math.max(outputDurable.getOrDefault(epoch.sourceId(), 0L), frame.chunkSeq()));
                listener.onEncodedAudio(frame);
                if ((frame.flags() & EVENT_END) != 0) eventCursor = Math.max(eventCursor, frame.eventSeq());
                if ((frame.flags() & EPOCH_END) != 0) { outputEpochs.remove(frame.epochRef()); outputSequences.remove(frame.epochRef()); }
            }
        } catch (Exception error) { fail("LIVE_SOCKET_OUTPUT_FRAME"); }
    }
    private void tick() {
        synchronized (lock) {
            if (closed) return; long at = now();
            if (!ready && at - openedAt > 5000) { fail("LIVE_SOCKET_HELLO_TIMEOUT"); return; }
            if (!ready) return;
            for (Pending p : pending.values()) if (at - p.sentAt() > (p.recovery() ? 15000 : 5000)) { fail("LIVE_SOCKET_ACK_TIMEOUT"); return; }
            if (at - lastPong > 45000) { fail("LIVE_SOCKET_HEARTBEAT_TIMEOUT"); return; }
            if (at - lastPing >= 15000) { lastPing = at; send(object("type", "ping")); }
        }
    }
    private boolean send(JsonObject message) {
        String text = gson.toJson(message); if (text.length() > 16384) throw invalid("CONTROL_SIZE");
        if (socket == null || !socket.send(text)) { fail("LIVE_SOCKET_IO"); return false; } return true;
    }
    private void requireReady() { if (closed || !ready) throw new IllegalStateException("LIVE_SOCKET_NOT_READY"); }
    private static long now() { return System.nanoTime() / 1000000L; }
    private void fail(String code) {
        synchronized (lock) { if (closed) return; closeInternal(false, code); }
        try { listener.onFailure(code); } catch (Exception ignored) { }
    }
    public void close(boolean sendStop) { synchronized (lock) { closeInternal(sendStop, "LIVE_STOPPED"); } }
    private void closeInternal(boolean sendStop, String code) {
        if (closed) return; closed = true; ready = false; timer.shutdownNow();
        IllegalStateException error = new IllegalStateException(code);
        if (connected != null) connected.completeExceptionally(error);
        opening.values().forEach(f -> f.completeExceptionally(error)); registering.values().forEach(f -> f.completeExceptionally(error)); sealing.values().forEach(f -> f.completeExceptionally(error)); unbinding.values().forEach(f -> f.completeExceptionally(error));
        opening.clear(); registering.clear(); sealing.clear(); unbinding.clear(); pending.clear(); outputEpochs.clear(); outputSequences.clear(); outputDurable.clear();
        liveBytes = recoveryBytes = liveDuration = recoveryDuration = 0;
        if (socket != null) { if (sendStop) { socket.send("{\"type\":\"stop\"}"); socket.close(1000, "client_stop"); } else socket.cancel(); }
    }
    @Override public void close() { close(true); }
    public long cursor() { synchronized (lock) { return eventCursor; } }
    public Hello hello() { synchronized (lock) { return hello; } }
    public boolean ready() { synchronized (lock) { return ready && !closed; } }
    private static JsonObject object(String... pairs) { JsonObject result = new JsonObject(); for (int i = 0; i < pairs.length; i += 2) result.addProperty(pairs[i], pairs[i+1]); return result; }
    private static String string(JsonObject object, String key) { JsonElement e = object.get(key); if (e == null || !e.isJsonPrimitive() || !e.getAsJsonPrimitive().isString()) throw invalid("METADATA"); return e.getAsString(); }
    private static long number(JsonObject object, String key, long low, long high) { JsonElement e = object.get(key); if (e == null || !e.isJsonPrimitive() || !e.getAsJsonPrimitive().isNumber() || !e.getAsString().matches("[0-9]+")) throw invalid("METADATA"); return integer(e.getAsLong(), low, high); }
    public static JsonObject epochJson(CodecEpoch e) {
        JsonObject m = object("epoch_id", e.epochId(), "codec", e.codec(), "container", e.container(), "init_base64", Base64.getEncoder().encodeToString(e.init()));
        m.addProperty("version", e.version()); m.addProperty("channels", e.channels()); m.addProperty("decoded_sample_rate", e.decodedSampleRate()); m.addProperty("clock_rate", e.clockRate()); m.addProperty("pre_skip", e.preSkip()); return m;
    }
    private static CodecEpoch epochFromJson(JsonObject m) { return new CodecEpoch(string(m,"epoch_id"), string(m,"codec"), string(m,"container"), (int)number(m,"version",1,1), (int)number(m,"channels",1,1), (int)number(m,"decoded_sample_rate",16000,48000), (int)number(m,"clock_rate",16000,48000), (int)number(m,"pre_skip",0,65535), Base64.getDecoder().decode(string(m,"init_base64"))); }
}

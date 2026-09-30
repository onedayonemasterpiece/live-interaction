package org.onedayonemasterpiece.live;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayDeque;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import okhttp3.HttpUrl;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.WebSocket;
import okhttp3.WebSocketListener;
import okio.ByteString;

/** Native wl-live-v1 binding. No Android, product, credential or provider logic. */
public final class LiveSocketTransport implements AutoCloseable {
    public static final String PROTOCOL = "wl-live-v1";
    public static final String VERSION = "0.3.6-rc.1";
    public static final int BATCH_BYTES = 3200;
    public static final int MAX_PENDING_PCM_BYTES = 48000;
    public static final long MAX_AGE_MS = 2500;
    public static final int ACK_WINDOW = 4;
    private static final int INPUT_MAGIC = 0x574c4131;
    private static final int OUTPUT_MAGIC = 0x574c4f31;
    public interface Listener {
        void onEvent(JsonObject event);
        void onAudio(long sequence, byte[] pcm, int sampleRate);
        void onFailure(String code);
        default void onDiagnostic(Map<String, Object> fields) {}
    }
    private static final class Frame {
        final byte[] pcm; final String control; final long captured;
        Frame(byte[] pcm, String control, long captured) {
            this.pcm = pcm; this.control = control; this.captured = captured;
        }
    }
    private static final class Pending {
        final long sent; final int bytes;
        Pending(long sent, int bytes) { this.sent = sent; this.bytes = bytes; }
    }
    private final Object lock = new Object();
    private final Gson gson = new Gson();
    private final Listener listener;
    private final OkHttpClient client;
    private final ArrayDeque<Frame> queue = new ArrayDeque<>();
    private final LinkedHashMap<Long, Pending> pending = new LinkedHashMap<>();
    private final ByteArrayOutputStream batch = new ByteArrayOutputStream(BATCH_BYTES);
    private final ScheduledExecutorService timers = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "shared-live-clock"); t.setDaemon(true); return t;
    });
    private WebSocket socket;
    private CompletableFuture<Void> connected;
    private Thread sender;
    private boolean closed, ready, speechOpen, started;
    private int generation, queuedPcmBytes, pendingPcmBytes;
    private long audioSequence, acknowledged, eventCursor;
    private long batchCaptured, helloDeadline, lastPing, lastPong;
    public LiveSocketTransport(OkHttpClient client, Listener listener) {
        this.client = client; this.listener = listener;
    }
    private static long now() { return System.nanoTime() / 1_000_000; }

    public CompletableFuture<Void> connect(String baseUrl, String socketPath, String ticket,
                                           String attemptId, int connectionGeneration, long cursor) {
        synchronized (lock) {
            if (started) throw new IllegalStateException("Transport instances are single-use");
            started = true;
            connected = new CompletableFuture<>();
            try {
                if (closed || ticket == null || !ticket.matches("[A-Za-z0-9_-]{20,512}") || connectionGeneration < 1 || cursor < 0)
                    throw new IllegalArgumentException("LIVE_SOCKET_CONFIGURATION");
                HttpUrl base = HttpUrl.get(baseUrl);
                HttpUrl target = base.resolve(socketPath);
                if (target == null || !base.scheme().equals(target.scheme()) || !base.host().equals(target.host())
                    || base.port() != target.port() || !target.username().isEmpty() || !target.password().isEmpty()
                    || target.query() != null || target.fragment() != null)
                    throw new IllegalArgumentException("LIVE_SOCKET_ORIGIN");
                generation = connectionGeneration;
                eventCursor = cursor;
                String authority = base.newBuilder().encodedPath("/").query(null).fragment(null).build().toString();
                authority = authority.substring(0, authority.length() - 1);
                Request request = new Request.Builder().url(target).header("Origin", authority)
                    .header("Sec-WebSocket-Protocol", PROTOCOL + ", wl-ticket." + ticket).build();
                socket = client.newWebSocket(request, new WebSocketListener() {
                    @Override public void onOpen(WebSocket ws, Response response) {
                        synchronized (lock) {
                            if (closed) { ws.cancel(); return; }
                            if (!PROTOCOL.equals(response.header("Sec-WebSocket-Protocol"))) { fail("LIVE_SOCKET_PROTOCOL"); return; }
                            JsonObject hello = new JsonObject();
                            hello.addProperty("type", "hello"); hello.addProperty("protocol", PROTOCOL);
                            hello.addProperty("attempt_id", attemptId); hello.addProperty("cursor", cursor);
                            hello.addProperty("connection_generation", generation);
                            helloDeadline = now() + 2000;
                            if (!ws.send(gson.toJson(hello))) fail("LIVE_SOCKET_CLOSED");
                        }
                    }
                    @Override public void onMessage(WebSocket ws, String text) { receiveText(text); }
                    @Override public void onMessage(WebSocket ws, ByteString bytes) { receiveAudio(bytes.toByteArray()); }
                    @Override public void onFailure(WebSocket ws, Throwable error, Response response) { fail("LIVE_SOCKET_IO"); }
                    @Override public void onClosing(WebSocket ws, int code, String reason) { ws.close(code, null); }
                    @Override public void onClosed(WebSocket ws, int code, String reason) { fail("LIVE_SOCKET_CLOSED"); }
                });
                sender = new Thread(this::sendLoop, "shared-live-sender"); sender.setDaemon(true); sender.start();
                timers.scheduleAtFixedRate(this::tick, 100, 100, TimeUnit.MILLISECONDS);
            } catch (Exception error) {
                fail(error instanceof IllegalArgumentException ? error.getMessage() : "LIVE_SOCKET_CONFIGURATION");
            }
            return connected;
        }
    }
    public void submitPcm(short[] samples) {
        synchronized (lock) {
            if (!ready || closed) return;
            if (samples == null || samples.length > 32000) { fail("LIVE_AUDIO_FRAME_SIZE"); return; }
            if (queuedPcmBytes + pendingPcmBytes + batch.size() + samples.length * 2 > MAX_PENDING_PCM_BYTES) {
                fail("LIVE_AUDIO_BACKPRESSURE"); return;
            }
            if (!speechOpen) {
                enqueue(new Frame(null, "{\"type\":\"input\",\"message\":{\"activity_start\":true}}", now()));
                speechOpen = true;
            }
            for (short sample : samples) {
                if (batch.size() == 0) batchCaptured = now();
                batch.write(sample & 255); batch.write((sample >>> 8) & 255);
                if (batch.size() == BATCH_BYTES) flush();
            }
        }
    }
    public void endSpeech() {
        synchronized (lock) {
            if (!ready || closed || !speechOpen) return;
            flush();
            enqueue(new Frame(null, "{\"type\":\"input\",\"message\":{\"activity_end\":true}}", now()));
            speechOpen = false;
        }
    }
    public void sendText(String text) {
        synchronized (lock) {
            if (!ready || closed) return;
            if (text == null || text.trim().isEmpty() || text.length() > 4000) { fail("LIVE_TEXT_INVALID"); return; }
            endSpeech();
            JsonObject body = new JsonObject(); body.addProperty("text", text.trim());
            JsonObject envelope = new JsonObject(); envelope.addProperty("type", "input"); envelope.add("message", body);
            enqueue(new Frame(null, gson.toJson(envelope), now()));
        }
    }
    private void enqueue(Frame frame) {
        if (closed) return;
        if (queue.size() >= 256) { fail("LIVE_AUDIO_BACKPRESSURE"); return; }
        queue.addLast(frame);
        if (frame.pcm != null) queuedPcmBytes += frame.pcm.length;
        lock.notifyAll();
    }
    private void flush() {
        if (batch.size() == 0) return;
        enqueue(new Frame(batch.toByteArray(), null, batchCaptured)); batch.reset();
    }
    private void sendLoop() {
        try {
            synchronized (lock) {
                while (!closed) {
                    if (!ready || queue.isEmpty() || pending.size() >= ACK_WINDOW) { lock.wait(100); continue; }
                    Frame frame = queue.removeFirst();
                    long age = Math.max(0, now() - frame.captured);
                    if (age > MAX_AGE_MS || socket.queueSize() > MAX_PENDING_PCM_BYTES) { fail("LIVE_AUDIO_STALE"); return; }
                    if (frame.pcm != null) {
                        long seq = ++audioSequence;
                        queuedPcmBytes -= frame.pcm.length; pendingPcmBytes += frame.pcm.length;
                        pending.put(seq, new Pending(now(), frame.pcm.length));
                        if (!socket.send(ByteString.of(encodeAudio(frame.pcm, seq, age)))) { fail("LIVE_SOCKET_CLOSED"); return; }
                        if (seq == 1 || seq % 16 == 0) diagnostic("audio_sent", age);
                    } else if (!socket.send(frame.control)) { fail("LIVE_SOCKET_CLOSED"); return; }
                }
            }
        } catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); }
        catch (Exception error) { fail("LIVE_SOCKET_SEND"); }
    }
    private void tick() {
        synchronized (lock) {
            if (closed) return;
            long at = now();
            if (!ready && helloDeadline > 0 && at > helloDeadline) { fail("LIVE_SOCKET_HELLO_TIMEOUT"); return; }
            if (!ready) return;
            if (batch.size() > 0 && at - batchCaptured >= 100) flush();
            if (!pending.isEmpty() && at - pending.values().iterator().next().sent > MAX_AGE_MS) { fail("LIVE_SOCKET_ACK_TIMEOUT"); return; }
            if (at - lastPong > 45000) { fail("LIVE_SOCKET_HEARTBEAT_TIMEOUT"); return; }
            if (at - lastPing >= 15000) {
                lastPing = at;
                if (!socket.send("{\"type\":\"ping\"}")) fail("LIVE_SOCKET_CLOSED");
            }
        }
    }
    private void receiveText(String text) {
        try {
            if (text.length() > 262144) { fail("LIVE_SOCKET_OUTPUT_SIZE"); return; }
            JsonObject message = gson.fromJson(text, JsonObject.class);
            String kind = message.get("type").getAsString();
            JsonObject event = null;
            synchronized (lock) {
                if (closed) return;
                if ("hello_ack".equals(kind)) {
                    if (!PROTOCOL.equals(message.get("protocol").getAsString()) || generation != message.get("connection_generation").getAsInt()) {
                        fail("LIVE_SOCKET_PROTOCOL"); return;
                    }
                    ready = true; lastPing = lastPong = now(); connected.complete(null); lock.notifyAll();
                    diagnostic("hello_ack", 0); return;
                }
                if (!ready) { fail("LIVE_SOCKET_PROTOCOL"); return; }
                if ("audio_ack".equals(kind)) {
                    long ack = message.get("seq").getAsLong();
                    if (ack < acknowledged || ack > audioSequence) { fail("LIVE_SOCKET_ACK_SEQUENCE"); return; }
                    acknowledged = ack;
                    var iterator = pending.entrySet().iterator();
                    while (iterator.hasNext()) {
                        var entry = iterator.next();
                        if (entry.getKey() <= ack) { pendingPcmBytes -= entry.getValue().bytes; iterator.remove(); }
                    }
                    lock.notifyAll(); return;
                }
                if ("pong".equals(kind)) { lastPong = now(); return; }
                if ("event".equals(kind)) {
                    event = message.getAsJsonObject("event");
                    long seq = event.get("seq").getAsLong();
                    if (seq <= eventCursor) return;
                    eventCursor = seq;
                }
            }
            if (event != null) listener.onEvent(event);
        } catch (Exception error) { fail("LIVE_SOCKET_PROTOCOL"); }
    }
    private void receiveAudio(byte[] frame) {
        try {
            byte[] pcm; int rate; long seq;
            synchronized (lock) {
                if (closed) return;
                if (!ready || frame.length < 14 || frame.length > 262144 || frame.length % 2 != 0) { fail("LIVE_SOCKET_OUTPUT_FRAME"); return; }
                ByteBuffer header = ByteBuffer.wrap(frame).order(ByteOrder.BIG_ENDIAN);
                if (header.getInt() != OUTPUT_MAGIC) { fail("LIVE_SOCKET_OUTPUT_FRAME"); return; }
                seq = Integer.toUnsignedLong(header.getInt()); rate = header.getInt();
                if (rate < 8000 || rate > 96000) { fail("LIVE_SOCKET_OUTPUT_RATE"); return; }
                if (seq <= eventCursor) return;
                eventCursor = seq;
                pcm = new byte[frame.length - 12]; header.get(pcm);
            }
            listener.onAudio(seq, pcm, rate);
        } catch (Exception error) { fail("LIVE_SOCKET_OUTPUT_FRAME"); }
    }
    public static byte[] encodeAudio(byte[] pcm, long sequence, long ageMs) {
        if (pcm == null || pcm.length == 0 || pcm.length > 11000 || pcm.length % 2 != 0 || sequence <= 0 || sequence > 0xffffffffL || ageMs < 0 || ageMs > MAX_AGE_MS)
            throw new IllegalArgumentException("LIVE_SOCKET_FRAME");
        return ByteBuffer.allocate(12 + pcm.length).order(ByteOrder.BIG_ENDIAN)
            .putInt(INPUT_MAGIC).putInt((int) sequence).putInt((int) ageMs).put(pcm).array();
    }
    private void diagnostic(String event, long age) {
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("event", event); fields.put("transport", "wss"); fields.put("connection_generation", generation);
        fields.put("frame_seq", audioSequence); fields.put("ack_seq", acknowledged); fields.put("capture_age_ms", age);
        fields.put("pending_pcm_bytes", queuedPcmBytes + pendingPcmBytes + batch.size()); fields.put("unacked_frames", pending.size());
        try { listener.onDiagnostic(fields); } catch (Exception ignored) {}
    }
    private void fail(String code) {
        synchronized (lock) {
            if (closed) return;
            if (connected != null && !connected.isDone()) connected.completeExceptionally(new IllegalStateException(code));
            closeInternal(false);
        }
        listener.onFailure(code);
    }
    public void close(boolean sendStop) { synchronized (lock) { closeInternal(sendStop); } }
    private void closeInternal(boolean sendStop) {
        if (closed) return;
        closed = true; ready = false; speechOpen = false;
        queue.clear(); pending.clear(); batch.reset(); queuedPcmBytes = pendingPcmBytes = 0;
        lock.notifyAll(); timers.shutdownNow();
        if (connected != null && !connected.isDone()) connected.completeExceptionally(new IllegalStateException("LIVE_STOPPED"));
        if (socket != null) {
            if (sendStop) { socket.send("{\"type\":\"stop\"}"); socket.close(1000, "client_stop"); }
            else socket.cancel();
        }
        if (sender != null) sender.interrupt();
    }
    @Override public void close() { close(true); }
    public long cursor() { synchronized (lock) { return eventCursor; } }
}

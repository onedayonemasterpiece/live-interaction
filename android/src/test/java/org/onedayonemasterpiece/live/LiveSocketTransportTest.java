package org.onedayonemasterpiece.live;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import okhttp3.OkHttpClient;
import okhttp3.Response;
import okhttp3.WebSocket;
import okhttp3.WebSocketListener;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okio.ByteString;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import static org.junit.Assert.*;

public class LiveSocketTransportTest {
    private MockWebServer server;
    private OkHttpClient http;
    private LiveSocketTransport transport;
    private final Gson gson = new Gson();
    private final BlockingQueue<String> controls = new LinkedBlockingQueue<>();
    private final BlockingQueue<byte[]> audio = new LinkedBlockingQueue<>();
    private final BlockingQueue<byte[]> played = new LinkedBlockingQueue<>();
    private final BlockingQueue<JsonObject> events = new LinkedBlockingQueue<>();
    private final BlockingQueue<String> failures = new LinkedBlockingQueue<>();
    private volatile WebSocket peer;
    private boolean acknowledge = true;

    @Before public void setup() throws Exception {
        server = new MockWebServer();
        server.enqueue(new MockResponse().addHeader("Sec-WebSocket-Protocol", "wl-live-v1")
            .withWebSocketUpgrade(new WebSocketListener() {
                @Override public void onOpen(WebSocket socket, Response response) { peer = socket; }
                @Override public void onMessage(WebSocket socket, String text) {
                    JsonObject msg = gson.fromJson(text, JsonObject.class);
                    if ("hello".equals(msg.get("type").getAsString())) {
                        socket.send("{\"type\":\"hello_ack\",\"protocol\":\"wl-live-v1\",\"connection_generation\":1}");
                    } else if ("ping".equals(msg.get("type").getAsString())) {
                        socket.send("{\"type\":\"pong\"}");
                    } else controls.add(text);
                }
                @Override public void onMessage(WebSocket socket, ByteString value) {
                    byte[] data = value.toByteArray(); audio.add(data);
                    long sequence = Integer.toUnsignedLong(ByteBuffer.wrap(data).getInt(4));
                    if (acknowledge) socket.send("{\"type\":\"audio_ack\",\"seq\":" + sequence + "}");
                }
                @Override public void onClosing(WebSocket socket, int code, String reason) { socket.close(code, null); }
            }));
        server.start();
        http = new OkHttpClient.Builder().connectTimeout(2, TimeUnit.SECONDS).build();
        transport = new LiveSocketTransport(http, new LiveSocketTransport.Listener() {
            public void onEvent(JsonObject event) { events.add(event); }
            public void onAudio(long seq, byte[] pcm, int rate) { assertEquals(24000, rate); played.add(pcm); }
            public void onFailure(String code) { failures.add(code); }
        });
    }
    @After public void cleanup() throws Exception {
        transport.close(); server.shutdown(); http.dispatcher().executorService().shutdownNow(); http.connectionPool().evictAll();
    }
    private void connect() throws Exception {
        transport.connect(server.url("/").toString(), "/socket", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "attempt_test", 1, 0).get(3, TimeUnit.SECONDS);
    }
    @Test public void pcmIsBinaryOrderedAndTextNeverOvertakesSpeech() throws Exception {
        connect();
        var request = server.takeRequest(1, TimeUnit.SECONDS);
        assertEquals("/socket", request.getPath());
        assertNull(request.getHeader("Authorization"));
        assertTrue(request.getHeader("Sec-WebSocket-Protocol").contains("wl-ticket."));
        short[] samples = new short[1600]; samples[0] = 1; samples[1] = -2;
        transport.submitPcm(samples); transport.sendText("Edit current draft");
        String start = controls.poll(2, TimeUnit.SECONDS);
        assertTrue(start.contains("activity_start"));
        byte[] frame = audio.poll(2, TimeUnit.SECONDS); assertNotNull(frame);
        assertEquals(3212, frame.length);
        assertEquals(0x574c4131, ByteBuffer.wrap(frame).getInt());
        assertArrayEquals(new byte[]{1,0,-2,-1}, Arrays.copyOfRange(frame, 12, 16));
        assertTrue(controls.poll(2, TimeUnit.SECONDS).contains("activity_end"));
        assertTrue(controls.poll(2, TimeUnit.SECONDS).contains("Edit current draft"));
        assertTrue(failures.isEmpty());
    }
    @Test public void outputIsPushedAndDuplicateSequenceIsIgnored() throws Exception {
        connect();
        peer.send("{\"type\":\"event\",\"event\":{\"type\":\"output_transcript\",\"seq\":1,\"text\":\"hello\"}}");
        assertEquals("hello", events.poll(2, TimeUnit.SECONDS).get("text").getAsString());
        byte[] frame = ByteBuffer.allocate(16).order(ByteOrder.BIG_ENDIAN).putInt(0x574c4f31).putInt(2).putInt(24000).put(new byte[]{1,0,2,0}).array();
        peer.send(ByteString.of(frame)); peer.send(ByteString.of(frame));
        assertArrayEquals(new byte[]{1,0,2,0}, played.poll(2, TimeUnit.SECONDS));
        assertNull(played.poll(150, TimeUnit.MILLISECONDS));
        assertEquals(2, transport.cursor());
    }
    @Test public void immediateStopDiscardsPendingInput() throws Exception {
        connect(); transport.close(true); transport.submitPcm(new short[1600]); transport.sendText("do not send");
        assertTrue(controls.poll(2, TimeUnit.SECONDS).contains("stop"));
        assertNull(audio.poll(150, TimeUnit.MILLISECONDS)); assertNull(controls.poll(150, TimeUnit.MILLISECONDS));
        assertTrue(failures.isEmpty());
    }
    @Test public void missingAcknowledgementHasBoundedVisibleFailure() throws Exception {
        acknowledge = false; connect(); transport.submitPcm(new short[1600]); transport.endSpeech();
        assertEquals("LIVE_SOCKET_ACK_TIMEOUT", failures.poll(4, TimeUnit.SECONDS));
    }
    @Test public void crossOriginSocketIsRejectedBeforeNetwork() throws Exception {
        try {
            transport.connect(server.url("/").toString(), "https://another.invalid/socket", "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "attempt_test", 1, 0).get(1, TimeUnit.SECONDS);
            fail("expected failure");
        } catch (java.util.concurrent.ExecutionException expected) {
            assertEquals("LIVE_SOCKET_ORIGIN", expected.getCause().getMessage());
        }
        assertEquals(0, server.getRequestCount());
    }
    @Test public void sharedWireVectorMatchesPythonAndBrowser() {
        byte[] pcm = new byte[]{1,0,-2,-1,-1,127,0,-128};
        byte[] value = LiveSocketTransport.encodeAudio(pcm,42,125);
        assertEquals("574c41310000002a0000007d0100feffff7f0080", ByteString.of(value).hex());
        try { LiveSocketTransport.encodeAudio(new byte[2], 1, 2501); fail(); } catch (IllegalArgumentException expected) {}
    }
}

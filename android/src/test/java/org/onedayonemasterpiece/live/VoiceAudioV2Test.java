package org.onedayonemasterpiece.live;

import com.google.gson.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Base64;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.onedayonemasterpiece.live.VoiceAudioV2.*;

/** The very same checked-in original codec/wire vectors are consumed by Python. */
public class VoiceAudioV2Test {
    private static JsonObject vectors() throws Exception {
        return JsonParser.parseString(Files.readString(Path.of("../tests/fixtures/wl-live-v2-vectors.json"))).getAsJsonObject();
    }
    private static byte[] hex(String text) {
        byte[] result = new byte[text.length() / 2];
        for (int i = 0; i < result.length; i++) result[i] = (byte) Integer.parseInt(text.substring(i * 2, i * 2 + 2), 16);
        return result;
    }
    @Test public void exactPythonFramesAndInvalidHeaderVectors() throws Exception {
        assertEquals(56, HEADER_BYTES);
        for (JsonElement value : vectors().getAsJsonArray("valid")) {
            JsonObject v = value.getAsJsonObject(), f = v.getAsJsonObject("fields");
            byte[] bytes = hex(v.get("hex").getAsString()); Frame frame = Frame.decode(bytes);
            assertArrayEquals(v.get("name").getAsString(), bytes, frame.encode());
            assertEquals(f.get("magic").getAsString(), new String(bytes, 0, 4, StandardCharsets.US_ASCII));
            assertEquals(f.get("flags").getAsInt(), frame.flags());
            assertEquals(f.get("stream_ref").getAsLong(), frame.streamRef());
            assertEquals(f.get("epoch_ref").getAsLong(), frame.epochRef());
            assertEquals(f.get("chunk_seq").getAsLong(), frame.chunkSeq());
            assertEquals(f.get("start_us").getAsLong(), frame.startUs());
            assertEquals(f.get("duration_us").getAsInt(), frame.durationUs());
            assertEquals(f.get("delivery_generation").getAsLong(), frame.deliveryGeneration());
            assertEquals(f.get("event_seq").getAsLong(), frame.eventSeq());
        }
        for (JsonElement value : vectors().getAsJsonArray("invalid")) {
            JsonObject v = value.getAsJsonObject();
            try { Frame.decode(hex(v.get("hex").getAsString())); fail(v.get("name").getAsString()); }
            catch (IllegalArgumentException expected) { assertTrue(expected.getMessage().startsWith("LIVE_AUDIO_")); }
        }
    }
    @Test public void retryLaneAndReferencesNeverChangeDurableIdentity() throws Exception {
        JsonArray cases = vectors().getAsJsonArray("valid");
        Frame live = Frame.decode(hex(cases.get(0).getAsJsonObject().get("hex").getAsString()));
        Frame recovery = Frame.decode(hex(cases.get(1).getAsJsonObject().get("hex").getAsString()));
        EncodedChunk a = new EncodedChunk("source", "epoch", live.chunkSeq(), live.startUs(), live.durationUs(), live.flags() & SOURCE_FLAGS, live.payload());
        EncodedChunk b = new EncodedChunk("source", "epoch", recovery.chunkSeq(), recovery.startUs(), recovery.durationUs(), recovery.flags() & SOURCE_FLAGS, recovery.payload());
        assertEquals(a, b); assertEquals(a.payloadSha256(), b.payloadSha256());
        byte[] read = a.payload(); read[0] ^= 1; assertEquals(a, b);
        try { new EncodedChunk("source", "epoch", 1, 0, 100000, RECOVERY, a.payload()); fail(); }
        catch (IllegalArgumentException expected) { }
    }
    @Test public void epochAndExpansionBoundsAreExplicitAndImmutable() throws Exception {
        JsonObject e = vectors().getAsJsonObject("epoch");
        byte[] init = Base64.getDecoder().decode(e.get("init_base64").getAsString());
        CodecEpoch epoch = new CodecEpoch("e", "opus", "ogg", 1, 1, 16000, 48000, e.get("pre_skip").getAsInt(), init);
        byte[] original = epoch.init(); init[0] ^= 1; assertArrayEquals(original, epoch.init());
        try { new CodecEpoch("e", "opus", "ogg", 1, 1, 16000, 16000, 0, original); fail(); }
        catch (IllegalArgumentException expected) { }
        try { new Frame(INPUT_MAGIC, 0, 1, 1, 1L << 53, 0, 100000, 1, 0, new byte[]{1}); fail(); }
        catch (IllegalArgumentException expected) { }
        try { new EncodedChunk("s", "e", 1, 0, 240001, 0, new byte[]{1}); fail(); }
        catch (IllegalArgumentException expected) { }
        try { new EncodedChunk("s", "e", 1, 0, 1000, 0, new byte[8193]); fail(); }
        catch (IllegalArgumentException expected) { }
        EncodedChunk output = new EncodedChunk("out_session_1", "e", 1, 0, 1000, EPOCH_END, new byte[]{1});
        assertEquals(output, new OutputChunk(output, 7).chunk());
        try { new OutputChunk(new EncodedChunk("s", "e", 1, 0, 1000, PORTION_START, new byte[]{1}), 7); fail(); }
        catch (IllegalArgumentException expected) { }
    }
}

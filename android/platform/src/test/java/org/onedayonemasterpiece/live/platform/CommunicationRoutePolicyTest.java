package org.onedayonemasterpiece.live.platform;

import android.media.AudioDeviceInfo;
import org.junit.Test;
import java.util.List;
import static org.junit.Assert.*;

public class CommunicationRoutePolicyTest {
    private static final int SPEAKER = AudioDeviceInfo.TYPE_BUILTIN_SPEAKER;
    private static final int EARPIECE = AudioDeviceInfo.TYPE_BUILTIN_EARPIECE;

    @Test public void selectsSpeakerWhenInCallDefaultsToEarpieceWithoutExternalDevice() {
        assertTrue(CommunicationRoutePolicy.preferSpeaker(EARPIECE, List.of(EARPIECE, SPEAKER)));
        assertTrue(CommunicationRoutePolicy.preferSpeaker(0, List.of(EARPIECE, SPEAKER)));
    }
    @Test public void neverOverridesAnAlreadySelectedSpeakerOrMissingSpeaker() {
        assertFalse(CommunicationRoutePolicy.preferSpeaker(SPEAKER, List.of(EARPIECE, SPEAKER)));
        assertFalse(CommunicationRoutePolicy.preferSpeaker(EARPIECE, List.of(EARPIECE)));
    }
    @Test public void neverStealsWiredBluetoothOrUsbAudio() {
        int[] external = {AudioDeviceInfo.TYPE_BLUETOOTH_SCO,
                          AudioDeviceInfo.TYPE_WIRED_HEADPHONES,
                          AudioDeviceInfo.TYPE_WIRED_HEADSET,
                          AudioDeviceInfo.TYPE_USB_HEADSET,
                          AudioDeviceInfo.TYPE_USB_DEVICE,
                          AudioDeviceInfo.TYPE_BLE_HEADSET};
        for (int route : external) {
            assertTrue(CommunicationRoutePolicy.isExternal(route));
            assertFalse(CommunicationRoutePolicy.preferSpeaker(EARPIECE,
                List.of(EARPIECE, SPEAKER, route)));
            assertFalse(CommunicationRoutePolicy.preferSpeaker(route,
                List.of(EARPIECE, SPEAKER, route)));
        }
    }
    @Test public void ignoredSpeakerSafeTypeIsNotAnExternalHeadset() {
        assertFalse(CommunicationRoutePolicy.isExternal(AudioDeviceInfo.TYPE_BUILTIN_SPEAKER_SAFE));
        assertTrue(CommunicationRoutePolicy.preferSpeaker(EARPIECE,
            List.of(EARPIECE, SPEAKER, AudioDeviceInfo.TYPE_BUILTIN_SPEAKER_SAFE)));
    }
}

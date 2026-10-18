package org.onedayonemasterpiece.live.platform;

import android.media.AudioDeviceInfo;
import java.util.List;

/** Device-type-only routing policy; never overrides an attached external call route. */
final class CommunicationRoutePolicy {
    private CommunicationRoutePolicy() { }

    static boolean preferSpeaker(int currentType, List<Integer> availableTypes) {
        boolean speaker = false;
        boolean external = false;
        for (int type : availableTypes) {
            if (type == AudioDeviceInfo.TYPE_BUILTIN_SPEAKER) speaker = true;
            else if (type != AudioDeviceInfo.TYPE_BUILTIN_EARPIECE
                    && type != AudioDeviceInfo.TYPE_BUILTIN_SPEAKER_SAFE) external = true;
        }
        return speaker && !external
                && (currentType == 0 || currentType == AudioDeviceInfo.TYPE_BUILTIN_EARPIECE);
    }

    static boolean isExternal(int type) {
        return type != AudioDeviceInfo.TYPE_BUILTIN_SPEAKER
                && type != AudioDeviceInfo.TYPE_BUILTIN_EARPIECE
                && type != AudioDeviceInfo.TYPE_BUILTIN_SPEAKER_SAFE;
    }
}

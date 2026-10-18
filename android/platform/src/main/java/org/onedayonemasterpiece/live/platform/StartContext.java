package org.onedayonemasterpiece.live.platform;

import org.onedayonemasterpiece.live.VoiceAudioV2;

/** Product has already authenticated and bound the supplied store to this conversation. */
public record StartContext(String conversationId, boolean allowAacCompatibility) {
    public StartContext { VoiceAudioV2.id(conversationId); }
    public StartContext(String conversationId) { this(conversationId, true); }
}

# Native voice loudspeaker routing — Android RC 0.4.0-rc.3

18 October 2026. Related Projects Hub incident: `inc_a6d3286ccf0620e09f450bdb`.

A handset can show a Gemini output transcript and retain 12 provider PCM events while
its native `AudioTrack`, configured with `USAGE_VOICE_COMMUNICATION` under
`MODE_IN_COMMUNICATION`, plays through the quiet receiver/earpiece instead of the
loudspeaker. This is a **hypothesis for the physical Samsung**, not an established
cause. The existing managed emulator had positive waveform/playback receipts
but no direct physical speaker-route evidence.

The shared platform now owns one bounded routing policy (not a second recorder or
audio engine). At user-started voice capture, if only internal routes are available
and the OS currently selects the receiver (or reports no selected device), request
the built-in loudspeaker. Android 12+ uses `setCommunicationDevice`; legacy OS
uses `setSpeakerphoneOn`. Do not override Bluetooth, wired or USB routes.
Relinquish an app-selected speaker route when an external output appears, and
reselect the built-in speaker only when the external route disappears. Undo the
app's own request/legacy state after Stop or audio-focus loss. Routing failures
are observable, not permission to discard recorded audio or crash capture.

Sanitized Android logcat event `LiveNativeAudioRoute` includes a fixed routing
decision and numeric output device type. The framework emits `native_audio_route`
events with the same metadata through the existing typed native bridge. Neither
contains microphone data, transcript, credentials or user commands.

Native `CommunicationRoutePolicyTest` covers default receiver, lack of speaker,
existing loudspeaker, Bluetooth SCO/BLE, wired and USB, and speaker-safe profiles.

Acceptance still open: a signed Projects Hub candidate pinned to this immutable
framework build, direct Samsung handset with and without Bluetooth, observed
route type and audible playback, normal Stop/cleanup, codec EOS stability, and
provider-verified speech. A green emulator alone does not close this incident.

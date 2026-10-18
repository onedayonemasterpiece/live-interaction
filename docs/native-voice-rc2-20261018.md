# Native voice 0.4.0-rc.2 candidate

This continues the original `claude/voice-continuity-v0.4-20261016` candidate at
`c564f0c44c6fe783fd4932bd4633fada34e05c73`. It is a release candidate; a build or
unit-test pass does not establish real microphone/provider acceptance.

## Capability-transition deadline

The Python host sends one absolute transition deadline to its provider runner.
Admission, the resumption checkpoint and replacement setup consume that same
deadline. A failed transition closes the inconsistent session and cancels its
runner; a late provider event cannot install tools after the host has rejected
the transition. Normal Stop still drains permitted final events through the
existing lifecycle contract.

An in-flight grant timeout remains an unknown authority outcome. It must not be
reported as the preceding definitive budget denial, retried with a smaller
amount, or routed to another credential. The resource SDK remains the sole
authority client; this framework does not create or alter quota policy.

## Optional native caption provenance

The native caption path retains raw provider Duration offsets and a bounded
ledger of PCM ranges that were actually written successfully to that provider
connection. Accepted audio supplies the session, conversation, source and
utterance identity. Arrival order and the currently visible user bubble are
never evidence of ownership of a transcript.

The default offset clock is `unverified`. `connection_pcm` is an explicit
server-owned opt-in that requires a real, exact-provider receipt with multiple
distinct utterances and connection changes. An `audio_stream_end` invalidates
this clock until a separately verified contract exists. Missing, overlapping,
out-of-range or evicted ranges produce no attributed native caption. Caption
failure does not stop the main Live voice session or erase canonical history.

Diagnostic capture is opt-in and contains scalar timing/range metadata and
digests, not transcript text, audio payloads, credentials or resumption handles.
The native probe requests VERBATIM transcription and interim timestamps; the
legacy PWA transcription configuration is preserved.

## Acceptance still required

Projects Hub's Android CI exercises real AudioRecord, MediaCodec, TLS WSS,
compressed durable storage, AudioTrack, outage/recovery and Stop. Failures of the
emulator sound device are recorded and cannot be passed by substituting text or
directly injecting PCM into the application.

Real Google acceptance must additionally verify compressed-input recognition,
the proposed caption clock, capability transition and spoken progress before a
slow tool completes. Record the exact source/package revisions and failures as
well as successful receipts before a production rollout.

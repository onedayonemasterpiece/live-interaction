# Native voice 0.4.0-rc.1 — source candidate

This checkpoint continues the original Claude Code branch
`claude/voice-continuity-v0.4-20261016` (original `599dc1caad8e0995ab65dc6b3fe2f684073af8f6`),
through the reviewed `3c72aba58d22076790ba9a8469eb501937cde797` checkpoint.
It is an integrated source candidate, not a production release or handset acceptance.
The common source version is `0.4.0-rc.1`; Python packaging normalizes it to `0.4.0rc1`.
Existing stable consumers and deployed applications remain unchanged. The dependent
Projects Hub candidate must pin the immutable commit containing this document.

## Delivered boundaries

- Negotiated `wl-live-v2`, compressed originals in both directions, stable source/epoch/chunk identity,
  independent durable/forwarded/played receipts, bounded fresh and recovery lanes.
- Opus/Ogg with original initialization, pre-skip and terminal trim; AAC/ADTS requires actual encoder
  priming metadata. Python uses the pinned optional `[audio]` dependency `av==18.1.0`.
- Host-owned output encoding/archive continues across WSS detach; reconnect reads exact originals,
  keeps original event cursors and separate actual playback position. Interrupted originals remain stored.
- Shared Android platform runtime owns AudioRecord, VAD, MediaCodec, AudioTrack and WSS independently
  of a presentation WebView. Product supplies authentication, foreground service, durable store and UI.
- One bounded pending response for explicitly declared `NON_BLOCKING` tools, honest final receipt,
  cancellation/generation guards and silent completion after new speech. No tool rerun for a pending hint.
- Terminal `await host.close()` drains sessions and the native codec pool; ordinary `stop_all()` is reusable.

Compatibility: existing `wl-live-v1`/PCM APIs remain explicit legacy paths. They do not satisfy compressed
native acceptance. There is no silent codec/protocol downgrade. Consumers must install `[audio]` for v2.
Android minSdk remains 26; devices lacking a verified compressed encoder report unsupported.

## Verification on 17 October 2026

| Gate | Result | Scope |
|---|---|---|
| Full Python unittest, real PyAV 18.1.0 | 127 PASS, 50.588 s, no skips | Includes 21 audio and 11 progress tests |
| Node `npm test` | 12 test files PASS, no skips | Each file executes the existing browser/Node contracts |
| Java core | 9 PASS | Wire vectors and legacy/native transport contracts |
| Android platform | 20 PASS | Codec/timeline, output selection and durable playback submission |
| Projects Hub Android paired build | 31 unit tests PASS; `assembleDebug` succeeds | SDK 35, AGP 8.7.3, Gradle 8.10.2, JDK 17 |
| Version metadata after RC label | PASS | Python/package/runtime label consistency |

Complete source hashes and test logs are recorded by the dependent Projects Hub candidate.
These are synthetic/local integration checks. The APK is a debug candidate with separate package ID.

## Remaining acceptance

The real bounded provider invocation on 17 October completed its realtime/burst cases, but skipped
resume because no fresh checkpoint after audio end was observed; the idle case timed out. It does not
prove long-session resumption or capability-transition behavior. No repeat invocation is implied here.

The consumer still needs a physical Android 90-minute conversation, screen-off/background lifecycle,
route/permission/interruption/Stop tests, bandwidth/latency/loss/outage/handover trials, restart/power-loss
durability, audible progress with the actual provider/resource wait, and signed update/rollback evidence.
Installed iOS PWA qualification is the next separate milestone. CPU/RSS/PSS/battery/capacity and Russian
ASR quality under compression require measurements; queue limits are not supported-user counts.

Promotion requires consumer evidence and the existing release process. A green shared CI alone cannot
promote this candidate; no tag, production deployment, merge or signing is performed by this checkpoint.

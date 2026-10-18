---
name: live-interaction
description: Integrate or change interactive Live voice/video in Wonderful Lections, Street Story, KenigEvents or future products. Use the shared runtime and platform bindings; ordinary durable voice-review is a separate flow.
---

# Shared Live interaction

Canonical implementation: `onedayonemasterpiece/live-interaction` (public), local
checkout `/home/dev/projects/live-interaction`. Read the
[architecture](../../docs/live-agent-architecture.md) and
[integration contract](../../docs/integration.md) before integration. For native v2,
also read [voice continuity](../../docs/voice-continuity-contract.md), the
[RC2 contract](../../docs/native-voice-rc2-20261018.md) and
[Android platform binding](../../android/platform/README.md).
[Native WSS v1](../../docs/native-wss.md) records the earlier 0.3.x candidate;
do not apply its Stop/Start-only recovery limitation to v2.

Use semantic release versions with archive digests/lockfile integrity. Never
silently update a running product/session. Framework fixes belong here, not in
copies of transport code inside consumers. Release candidates are not production
acceptance. Keep existing stable consumers pinned while candidate acceptance runs.
At this 18 October 2026 source checkpoint, published stable is `0.3.28`;
`0.4.0-rc.2` is the native source candidate, with its prerelease publication still
pending. Record an explicit source candidate's exact commit/tree; move to the
versioned archive and digest when published, without relabeling it stable.

Python products use `LiveSocketSessionHost` (an extension of the existing host)
and the framework-neutral `serve_socket` relay. FastAPI/aiohttp products only
provide authorized HTTP routes and socket I/O callbacks. Do not add a Node
sidecar merely because the first WSS consumer was Node. Node products keep the
existing Node binding and shared Python provider. Common protocol/conformance
checks cover both; backend language choice is not a performance guarantee.

Consume the Android contracts and platform projects from the same immutable
archive through the existing composite build; never edit a fork in the product.
For native v2, shared `NativeLiveRuntime` owns AudioRecord, VAD, MediaCodec,
AudioTrack and WSS. Supply product authorization, SessionApi, durable stores,
foreground service, permission flow and UI in the consumer. Keep browser products
on the shared AudioWorklet client and generated assets.

Use authenticated HTTP bootstrap/renewal and a 15-second one-use ticket in the
subprotocol. Negotiate `wl-live-v2` and verified compressed profiles for native
voice; preserve original source/epoch/chunk identity and separate durable,
forwarded and played receipts. Renew the same session's socket generation without
replaying old speech as commands; recovery of durable originals remains a separate
lane. Preserve immediate Stop across every await boundary. Keep `wl-live-v1`/PCM
only as an explicit legacy binding, with no silent codec/protocol/HTTP downgrade.
Keep credentials, tickets and resumption handles out of URLs. Do not start an
events poller in a migrated WSS session. A damaged input turn cannot authorize
tools until a later clean provider boundary is proven.

Treat native captions as an optional projection with default clock `unverified`.
Require accepted source/utterance provenance and verified provider offsets before
attribution; never guess from arrival order or a visible bubble. Surface unavailable
captions without stopping primary voice or changing canonical history. Keep caption
diagnostics opt-in and qualify them separately; a primary audio PASS does not
qualify captions, and missing offsets must not be invented to pass a probe.

Keep resource/actor authorization, product functions, immutable revision binding,
state, prompts/persona and credential selection in product adapters. Preserve
small prompt layers, progressive capabilities and context continuity. No raw
provider calls outside the shared resource controller. The model/key binding is
sticky after ready; quota failures are not an excuse for key/provider hopping.
Missing credentials and authority failures remain visible and fail closed.

When a browser product needs a local control-plane recognizer while Live owns the microphone, use the shared browser client's optional `captureTap(pcm,rms)` after playback suppression. Keep that tap synchronous, bounded and local-only; do not open a second `getUserMedia`, create a competing audio transport, or delay provider delivery. Copy frames before retaining/transferring them and keep product command semantics in the consumer adapter.

Preserve one bounded ordered sender, preroll/tail, immediate local Stop, no stale
speech replay, complete already-received playback, separate confirmed spoken Stop,
serialized authorized tools and mutation deduplication/readback. A button Stop
needs no voice confirmation. Do not start a new conversation just to change a
capability or recover transport; refresh authoritative product state and never
replay an accepted write.

Deliberate durable recordings are distinct from reconnect replay: use manual
activity_start -> ordered PCM -> activity_end. A trusted durable sink consumes
full provider transcription before bounded UI projection; do not reconstruct an
archive from the event ring. Interactive packet-loss recovery never replays old
recordings automatically.

Surface waits truthfully as transport, provider, resource or tool/action wait;
after 15 seconds show elapsed time and immediate Stop. After 120 seconds allow
explicit Stop/Start without replaying the pending command. Do not promise external
provider latency. Routine diagnostics contain only bounded identifiers, timings,
counts and codes, never credentials, raw media or full transcripts/tool payloads.

Verify Node/Python contracts, native mock-WebSocket contracts, affected consumer
regressions and actual public TLS upgrade. Real voice acceptance needs consecutive
audio turns, application read/write tools, search when requested, Stop/restart,
full playback and result readback. Distinguish prepared PCM from a physical mic.
Long-session/resumption checks and real provider receipts are required before
claiming those properties; a unit test, setup handshake or APK build is not Live
acceptance. Keep managed artifacts and exact source/dependency versions.
An explicitly labeled consumer RC may retain named physical-handset and 90-minute
acceptance gates as open. Record Android emulator audio and real-provider receipts
separately; neither substitutes for those physical/long-session claims or promotes
the candidate to stable. Record failures and degraded optional captions as well
as passing gates.


Browser socket send/ACK failures recover through bounded same-session ticket
renewal before any full setup retry. Stop capture and drop stale PCM; never send
provider Stop solely for a recoverable socket gap. Retain typed `RESOURCE_*`
startup errors and `retry_after_ms` through HTTP/UI; a resource refusal is not a
provider outage and must not trigger a transport retry loop. Apply the linked
source/receipt recovery contract to native v2. Require real transport acceptance
for lifecycle releases.

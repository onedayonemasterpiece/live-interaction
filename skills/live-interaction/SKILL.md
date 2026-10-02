---
name: live-interaction
description: Integrate or change interactive Live voice/video in Wonderful Lections, Street Story, KenigEvents or future products. Use the shared runtime and platform bindings; ordinary durable voice-review is a separate flow.
---

# Shared Live interaction

Canonical implementation: `onedayonemasterpiece/live-interaction` (public), local
checkout `/home/dev/projects/live-interaction`. Read `docs/live-agent-architecture.md`
and `docs/integration.md` before integration. For native/Python WSS also read
`docs/native-wss.md`, including the multi-project runtime decision and RC limitations.

Use semantic release versions with archive digests/lockfile integrity. Never
silently update a running product/session. Framework fixes belong here, not in
copies of transport code inside consumers. Release candidates are not production
acceptance. Keep existing stable consumers pinned while candidate acceptance runs.

Python products use `LiveSocketSessionHost` (an extension of the existing host)
and the framework-neutral `serve_socket` relay. FastAPI/aiohttp products only
provide authorized HTTP routes and socket I/O callbacks. Do not add a Node
sidecar merely because the first WSS consumer was Node. Node products keep the
existing Node binding and shared Python provider. Common protocol/conformance
checks cover both; backend language choice is not a performance guarantee.

Android products consume the native Java socket binding from the same immutable
archive as a generated source set; never edit or copy a fork into a product.
The app retains capture/VAD/playback/UI. Browser products use the existing shared
AudioWorklet client and generated assets, not a product-local microphone engine.

WSS: authenticated HTTP bootstrap/renewal, 15-second one-use ticket in subprotocol,
matching `wl-live-v1` hello_ack before readiness, binary PCM, pushed events/audio,
bounded ordered sender and ACK pacing. No credential/ticket/resumption handle in
URLs. No polling or silent HTTP-audio fallback in a migrated session. A damaged
input turn cannot authorize tools until a later clean provider boundary is proven.
Native RC reconnect is explicit Stop/Start with no speech replay. Python browser
RC does not accept 20-second startup replay; disable captureDuringStart and show
setup state until that catch-up path has independent acceptance.

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

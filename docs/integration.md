# Integration contract

This document covers transport/package integration. The mandatory product
architecture for prompt layering, progressive capability disclosure, session
continuity, modality-aware resource accounting, logging and acceptance lives in
[Live agent architecture and operating standard](live-agent-architecture.md).
A new or materially changed consumer MUST follow both documents.

## Pin and import

The first consumer uses a release tarball in its private `vendor/` directory.
Build it with `npm pack` from a clean, committed framework checkout. Record its
release version and archive SHA-256 in the consumer's `liveFramework` package metadata,
use a `file:vendor/live-interaction-<version>.tgz` dependency, and commit the npm
lockfile (SHA-512 integrity). CI verifies version metadata, archive digest, lockfile
and installed/generated assets. This is an immutable distribution artifact, not
an editable source fork. Update it from the next versioned framework release; keep the source commit only in release receipts.
This also works in container builds without granting a cross-repository key to CI.
Do not distribute a private package inside a public product without authorization.

The framework repository is public. Consumers preferring Git version resolution may use this form:

Node: `@onedayonemasterpiece/live-interaction` dependency set to
`git+https://github.com/onedayonemasterpiece/live-interaction.git#semver:^0.1.0`.
Commit the lockfile. Build browser assets with
`installBrowserAssets(destination)` from the package `/assets` export; include
these generated modules in the application's content hash/cache manifest. Never
edit or commit generated copies. Import `createLiveClient` from the generated
`client.js`; server imports `createLiveSessionHost` from the `/node` export.

Python: install `live-interaction @ git+https://github.com/onedayonemasterpiece/live-interaction.git@v0.1.2`.
Use `live_interaction.provider.run(load_key=server_credential_resolver)` for the
worker entrypoint. Default resolver reads only `LIVE_API_KEY`. Node packages
include the same Python module, so a Node consumer need not perform a separate
pip install of the project (websockets must already be installed).

`run(reader=asyncio.StreamReader(...), on_event=callback, load_key=resolver)` is
also available for a Python host. Feed bounded NDJSON commands to the reader;
callbacks receive events. Product endpoints and tool authorization belong in
the host. Python applications should reuse this transport; they do not need the
Wonderful Lections presentation service or Node to connect to Google.

For in-process Python products, `live_interaction.session_host.LiveSessionHost` is the
matching generic host. Construct it with an `adapter_factory` and a server-owned
`key_resolver(resource_id, actor)`. The adapter implements `initialize(...)` and
`execute_tool(session, call)`; optional `input`, `on_started`, `on_resumed`, and
`on_stopped` hooks mirror the Node host semantics. Optional `on_event(session, event)` observes\nprovider events before host projection, so a product can keep bounded conversation context\nwithout moving domain state into the provider. The host binds every session to
resource+actor, bounds provider input to 768 KiB, caps audio chunks at 16k base64
characters, paginates a 320-event ring, serializes tools, deduplicates successful
provider call IDs, honors cancellation before execution, and never replays a mutation.
Applications still authenticate every HTTP route before calling the host.

`configuration.manual_activity_detection=true` creates a session with provider automatic
activity detection disabled. In that mode the host accepts `activity_start`, one or more
`audio_base64` chunks, then `activity_end`; `audio_stream_end` is rejected. This is for
deliberate buffered turns, not normal realtime reconnect replay. Provider transcript events
remain complete for the trusted adapter `on_event` observer; only the polling/UI ring projects
transcript text to a bounded 2000-character preview. A durable observer must persist or
durably enqueue the trusted event before returning.

## Multimodal tool results (Python candidate 0.3.11-rc.1)

Python adapters return `with_live_tool_parts(result, parts)` from `execute_tool`.
It mirrors Node's `withLiveToolParts`: one or two JPEG/PNG/WebP `inlineData`
parts, each at most 512 KiB decoded, with a bounded `displayName`. Reference the
image in the JSON result as `{"image":{"$ref":"comparison.jpg"}}`. Image bytes
belong in `FunctionResponse.parts`, never in a JSON text field. Preserve the
wrapped result through product response projection; `dict(result)` discards the
attachment. Both first execution and cached call-ID replay carry the same parts;
browser events contain no image bytes. Standalone realtime video snapshots do
not guarantee that a tool verdict uses that frame.

An immutable Python/native candidate may be distributed as a consumer-owned
versioned Git source archive. Verify its SHA-256 and package/Python versions
before installing it and generating native assets. Include `pyproject.toml` in
that distribution. Local candidate acceptance does not publish or promote the
framework release, and does not change other consumers' pins.

## WSS transport in 0.3.x

A migrated consumer constructs `createLiveClient({transport:'wss', ...})`. The
session POST is still authenticated with the product's normal HTTP authorization and
must return `{session_id, transport_protocol:'wl-live-v1', socket_ticket, socket_url}`.
The ticket is random, stored server-side only as a digest, expires after 15 seconds,
and is consumed once through the WebSocket subprotocol
`wl-ticket.<ticket>`; never put bearer credentials, review capabilities, tickets or
resumption handles in a WebSocket URL.

The browser sends `hello` with a pre-session `attempt_id`, the event cursor and a
monotonic connection generation. Audio frames are binary PCM16/16 kHz with a bounded
header carrying frame sequence and capture age. The server may send asynchronous
`audio_ack` receipt telemetry, but the next audio frame never waits for that
acknowledgement. Provider events are pushed immediately; output PCM is binary. A new
WSS consumer must not start the legacy events poller or automatically fall back to
HTTP input if WSS setup/reconnect fails.

Release 0.3.8 adds optional `captureTap(pcm,rms)` on the browser client. The tap runs synchronously on each accepted microphone frame after `suppressCaptureDuringPlayback` has rejected frames while model audio is playing and before the ordered provider sender receives the frame. It is intended for bounded local-only control processing that reuses the already-owned PCM16/16 kHz capture. The callback must not perform network I/O, acquire another microphone, block, mutate provider state or throw through the Live client; callback failures are isolated and reported only as bounded `capture_tap_error` timing telemetry. The native WSS binding remains the separately documented `0.3.7-rc.1` candidate. Release 0.3.6 serializes capability-level `media_resolution` as the scalar Gemini Live `generationConfig.mediaResolution` enum. Release 0.3.5 introduced the consumer setting, but object-wrapping the scalar is rejected by the Live setup schema. Consumers can therefore use low-resolution vision for routine verification and high resolution for dedicated image inspection without changing transport semantics.

Release 0.3.4 makes each browser WSS audio send complete only after the relay ACK for that frame. The ordered audio sender therefore naturally paces startup/catch-up audio instead of dumping a valid multi-second handoff into WebSocket.bufferedAmount. ACK timeout remains fail-closed with bounded socket metrics.

Release 0.3.3 keeps steady-state PCM byte/age bounds authoritative over AudioWorklet render-quantum object count, retains a large pathological-fragmentation fuse, assigns stable sender failure codes, and emits safe playback telemetry (PCM peak/RMS, sample rates, AudioContext running state and output/base latency). No transcript or credential data is included in these metrics.

Release 0.3.2 preserves the 20-second startup PCM byte bound but raises the
independent catch-up fragment-count fuse so a provider setup lasting several
seconds cannot fail merely because AudioWorklet delivered many small quanta.
Steady-state queue/age guards are unchanged, and startup PCM beyond the byte
budget still fails closed.

Release 0.3.1 treats the versioned `hello_ack`, not merely TCP/WebSocket `open`, as
transport readiness. Browser-origin upgrades are same-origin by default, server
output buffering is bounded, and a reconnect backlog is de-duplicated against live
push by event sequence. The server accepts bounded RFC WebSocket fragmentation while
preserving message ordering. Consumers behind a reverse proxy must keep the public
Host/Origin relationship intact and explicitly validate their proxy's Upgrade and
idle-timeout behaviour.

Reconnect obtains a fresh one-use ticket through the authenticated HTTP session
resource. The browser discards its pending sender queue and restarts capture rather
than replaying stale speech. If a socket disappears while an audio turn is open, the
host closes that provider audio boundary, marks the accepted fragment damaged and
rejects provider tool calls with `LIVE_INPUT_DAMAGED` until a later clean turn reaches
the provider boundary. This prevents a truncated command from becoming a mutation.

Shared capture is AudioWorklet-only for 0.3.x. One MediaStream/AudioContext is handed
from startup or wake capture into active Live by replacing the frame callback; it is
not reacquired. The resampler carries state across worklet blocks; 0.3.1 adds an exact long-run
duration regression at 44.1 kHz so block boundaries cannot accumulate drift. Consumers should
surface unsupported AudioWorklet as a microphone capability failure rather than
secretly selecting the deprecated ScriptProcessor path.

## Browser API

`createLiveClient({request?, onEvent, onState, onNotice, onTiming, onWait,
voiceControl?, captureTap?})` returns `start`, `stop`, `input` and read-only `sessionId`,
`starting`, `generation`, `playingCount`.

`start({url,body,authorize,takeMicrophoneHandoff?,microphone?,captureDuringStart?})`: same-origin authenticated collection URL, application
start arguments (model/context IDs/history, never a key), async authorization. A
microphone handoff reuses the existing MediaStream and may seed buffered PCM captured
while the server/provider session was starting; it must not open a second microphone.
For a manual push-to-toggle start with no already-open microphone,
`captureDuringStart:true` opens local capture immediately after local authorization,
buffers up to the existing 20s startup bound while the session POST is pending,
then reuses that same MediaStream and buffered PCM when Live becomes ready.
Default HTTP protocol: POST collection -> `{session_id,model,...app metadata}`;
POST `/:id/input` -> `{ok:true}`; GET `/:id/events?after=N` ->
`{events,cursor,has_more,gap,closed}`; POST `/:id/stop` -> `{ok:true}`.
The host authenticates **every** route before dispatch, including event polling.
A custom `request` must honor abort signals and the same bounded timeouts.

`stop({reason,keepalive,preservePlayback})` synchronously stops capture/tracks,
invalidates the epoch, clears pending speech, aborts requests and calls
`onState('off')`. Remote cleanup has its own 2.5s bound. Preserve received playback
only on provider closure/transport failure, never for a user's explicit Stop.
`input({text})` starts a turn; `input({...product context})` is adapter-specific.

`captureTap(pcm,rms)` is optional and receives the same PCM16/16 kHz microphone frame after playback suppression and before transport enqueue. Keep it synchronous, bounded and local-only; copy the frame before transferring or retaining it. Throwing from the tap is isolated from Live transport and reported as `capture_tap_error`. This hook is for local control-plane sidecars, not a second conversational pipeline.

`onEvent(event,generation)` must not block playback with a long domain refresh.
Before applying asynchronous UI results, compare the generation to the current
client generation. Tools and mutation busy/readback indicators remain app policy.
`onState`: starting, started, listening, answering, budget_wait, budget_ready, reconnecting, off,
microphone_unavailable, connection_error, start_error.
`onNotice`: voice_stop_confirmation_requested, voice_stop_cancelled,
voice_stop_expired, event_gap, transport_error, microphone_error,
connection_error, start_error; second argument may be an Error.
`onTiming` contains bounded numeric diagnostics, not speech/secret payloads.
`onWait(null | {elapsed_ms,stage,can_restart})` stays hidden below 15 seconds;
show mm:ss, a gentle pulse honoring reduced motion, and an immediate Stop.
Stage is transport until worker-send/ASR evidence, then provider; outstanding tool calls use action so a slow application is not blamed on Google. A rolling resource grant refusal uses resource: capture is paused until the same unsent control message is admitted, while Stop remains immediate. Optional video frames are dropped on budget refusal. Extended intermediate turnComplete does not complete an outstanding wait. At 120 seconds,
offer explicit Stop+Start without replaying the old command. Never auto-restart
or automatically retry a mutation. Already accepted writes may still finish;
refresh authoritative product state after reconnect.

Russian voice Stop uses two separate utterances, a 30s confirmation window and
900ms fragment debounce. Supply another `voiceControl` parser or `false` for a
different language/policy. The **application system instruction must also ask
for confirmation aloud** and must not claim the microphone is off before it is.
A button Stop always bypasses confirmation. No generic edit/call permissions are
introduced by voice control.

## Node adapter

`createLiveSessionHost({adapterFactory,createWorker,ErrorClass?,models?,
readyTimeoutMs?,reconfigureTimeoutMs?,maxSessions?})` returns
start/input/events/stop/stopAll/size.
The public host args are `resourceId`, `actor`, and, after setup, `sessionId`.
Start also takes model/history and adapter-specific arguments. Authenticate before
calling the host; it additionally binds subject+tenant+resource to each session.
Do not expose the internal actor-omitting stopAll path over HTTP.

`adapterFactory({emit,write,measure,timing})` returns:

- `initialize({resourceId,actor,model,...args})` validates access and returns
  `{state,capability?,context,configuration,response}`. No mutation during initialization.
- `executeTool(session,{id,name,args})` authorizes each ordinary tool, validates arguments,
  and runs the product's normal prepare/apply/readback semantics.
- Optional `resolveCapability(session,call)` (Python: `resolve_capability`) is a
  side-effect-free router resolver. Return null for an ordinary tool or
  `{capability,configuration,context?,continuation?,response?}` for a capability
  transition. `continuation` is a bounded application-owned summary of the
  already pending intent; when omitted, hosts may use the router call's bounded
  `intent` argument. It is context, not permission.
  Capability IDs are bounded identifiers; a transition bundle contains at most
  nine functions and at most 256 KiB serialized configuration.
- Optional `input`, `onStarted`, `onResumed`, `onStopped` for product context and
  latest-only frame updates. Clear timers on Stop, resend latest image on resume.

`configuration` is server-owned: `system_instruction`, `context_instruction`,
`functions` (Gemini declarations), `voice` (default Aoede), `search_enabled`
(default false). Never accept arbitrary declarations/instructions from a browser.
An image-reading tool may return `withLiveToolParts(result, [{inlineData:{mimeType:'image/jpeg',displayName:'preview.jpg',data:base64}}])` from the Node host. The host sends `result` directly as the provider FunctionResponse `response` alongside the bounded image parts and preserves both on deduplicated calls; the product still owns image access and authorization.

The host caches successful tool results by provider call ID and serializes tools;
application idempotency must still survive process/session restarts. If
`resolveCapability` selects a transition, that router call must be the only tool
call in its provider batch. The host sends one bounded `reconfigure` worker
command containing the server-owned configuration, a router acknowledgement and
the bounded continuation intent. For Gemini the worker sends that acknowledgement
only after any already-started microphone turn has reached its `audio_stream_end`
at the provider. A 30-second bound fails the router call visibly if the turn
never ends; the host does not silently drop its remaining PCM. The worker then sends that acknowledgement
on the old connection first, waits for a **fresh post-response** resumable handle,
then resumes the same model/key/session with the new bundle. If Gemini does not issue a fresh checkpoint within the bounded wait, the worker discards the old handle, opens a new connection with the same model/key, restores bounded completed turns through `historyConfig.initialHistoryInClientContent`, and delivers the one accepted continuation. A checkpoint resume does not replay application history. After
`capability_ready` the host updates active capability/configuration state but
must not send a duplicate FunctionResponse. Transition failure is emitted as
structured transition/tool metadata and closes the provider session so a late
ready cannot change the active allowlist; it is never silently converted into a
successful fresh conversation. `onEvent(session,event)` is a trusted optional
adapter observer invoked when an event is emitted, even without browser polling;
do not persist raw conversation text, media, handles or tool arguments from it.

`createWorker({model,actor,resourceId})` returns a child with stdin/stdout/stderr
and kill(). Spawn the shared Python provider or a **thin credential adapter**.
Only the server chooses an approved credential binding; do not send it through
setup JSON, browser APIs, transcripts or receipts. Whitelist the child environment.
Different product instances can select different server-side keys. Resolve once
per session; resumption keeps the same key/model. Missing credentials fail closed;
quota failure never triggers key hopping. No shared global default credential pool.

## Worker wire and limits

Start: `{type:'start',model,context,configuration,history}`. Inputs: audio
(base64 PCM16, 16kHz mono), audio_stream_end, text, snapshot (JPEG+context),
tool_response, `reconfigure`, stop. `reconfigure` carries a server-owned
transition ID, capability ID, full bounded configuration, optional context,
bounded `continuation`, and the exact router FunctionResponse acknowledgement.
For Gemini the worker writes that acknowledgement on the current WebSocket,
invalidates the old resumption token, waits for a fresh resumable handle issued
after the acknowledgement, closes only that WebSocket intentionally, resumes
the same model/session with the new configuration, emits `capability_ready`,
and sends one bounded `LIVE_CONTINUATION` application turn. Inputs queued
during the handoff are dropped rather than replayed; the audio-end barrier
keeps the speech that triggered a transition ahead of that boundary. Writes include
numeric `queued_at` for delay measurement.
Snapshot uses video input only, never an implicit user text turn. A product may
mark a replaceable frame `optional:true`: a denied rolling image grant drops that
frame and emits `input_dropped`, preserving the Live conversation. Required
frames retain fail-closed semantics. Already executed tools are never rerun
while their unsent FunctionResponse waits for an available grant. Product images
must be current and bounded before transport. Do not replay captured audio on
recovery. Tools are cancelled only before starting; accepted writes require
normal domain reconciliation.

Current 0.3.2 defaults: 80ms browser batch; <=11000 PCM bytes per browser-to-server
request (binary only when the consumer opts in with `binaryAudio: true` and
supports `application/octet-stream`); then <=16000 base64 characters on the server-to-provider JSON wire;
1.5s buffered PCM and 2.5s item-age steady-state guards; 2.5s non-audio input request bound; 10s absolute audio/audio_stream_end HTTP ceiling; one in-flight sender;
250ms preroll; conservative 0.008 RMS onset and 0.003 RMS continuation gates;
2s quiet tail. An intentional startup
microphone handoff may seed at most 20s of PCM and temporarily uses a separate
bounded catch-up ceiling (seed + the ordinary 1.5s queue). Once the backlog isA new adapter/release requires real browser voice acceptance: 10 turns, authorized
product actions, navigation/context changes, immediate Stop/no later audio POST,
restart, full playback and voice confirmation. For connection/recovery changes,
include long session and genuine provider resumption receipts. Keep provider
failures visible and distinguish fixtures from real acceptance.

Wonderful Lections Show should have a separate **read-only cohost tool adapter**,
not reuse Review's mutation permissions by default. Street-story / idea-hub need
their own context, authorization and UI; none is implemented by this extraction.

## Updating a consumer by version

For the initial npm archive distribution, use the source repository helper:

```sh
node scripts/update-consumer.mjs /path/to/product 0.1.0 /path/to/managed/artifacts
# Or resolve the current stable release once:
node scripts/update-consumer.mjs /path/to/product latest /path/to/managed/artifacts
```

It downloads the versioned public release, checks package identity, updates the
versioned archive/manifest and lockfile. Then run npm ci/build and acceptance.
`latest` is resolved during this explicit update; it never changes the code of
an already running Live session. Keep previous package archives only if needed
for your source history/release policy. The helper neither commits nor deploys.

## Shared resource guard in 0.1.3

A consumer that reserves Live capacity through a shared controller passes its
lease to the provider as resource_guard. The transport checks the lease before
opening or resuming a provider connection and before/after provider receives.
It calls before_send(payload) before setup and every outbound provider message,
then rechecks the lease before bytes are written.

The key is resolved once from resource_guard.key() and remains pinned through
session resumption. An exception marked resource_failure is terminal for the
provider session: it is propagated to the resource controller and cannot cause
reconnect, Search capability downgrade, or key rotation.

The parameter is optional only for compatibility with consumers that have not
yet migrated. New or changed managed consumers must supply the shared guard.

## Future provider research

The 0.1.x provider transport is Gemini-specific. Potential alternatives are kept
in [the Live provider research backlog](provider-research-backlog.md), including
Qwen Audio/Omni Realtime, StepAudio, GLM-Realtime, SeedRealtime, OpenAI
Realtime/GPT-Live, Grok Voice, Nova Sonic, Hume EVI and self-hosted research
options such as MiniCPM-o and PersonaPlex.

This list is intentionally **not** a fallback chain. A new provider needs a thin
adapter plus provider-specific real acceptance. Do not switch providers inside a
running/recovering session merely because Gemini is slow, unavailable or out of
quota.

## Browser durable capture

Release 0.2.6 exposes `createMicrophoneCapture` and
`createDurableMicrophoneCapture` from the existing `./browser` entry point. This is
the reusable capture layer for consumers that need an offline/restart-safe source;
do not copy `getUserMedia`, PCM conversion, silence gating or pre-roll into a product.

`createLiveClient({persistAudio})` passes the same optional durability callback into
the shared audio sender. For accepted speech/pre-roll/tail, the callback receives
either `{pcm:Int16Array,sample_rate:16000,captured_at_ms}` or an
`{audio_stream_end:true,captured_at_ms}` marker. Provider transport is not allowed
to see that accepted PCM until the callback resolves. A persistence failure stops the
sender and surfaces the transport error; it is never converted into a provider retry.

For completely offline capture, `createDurableMicrophoneCapture({persist})` runs the
same microphone conversion and shared sender/VAD path with no provider POST. Its
async `stop()` stops hardware, drains already captured frames, seals the source, waits
for durable receipts, then closes the sender. The product owns the durable store,
source IDs/manifests, retention and later upload/replay policy. When connectivity is
available again, feed the durable source through the product's deliberate buffered
Live turn (`activityStart → PCM → activityEnd`); ordinary reconnect still never
replays old speech automatically.

## Resource waits in 0.3.11-rc.2

A rolling resource wait for an unsent tool response or explicit text keeps queued text in FIFO order until admission succeeds. It never invokes the tool again. Only realtime capture is discarded by the resource recovery cutoff. A genuine capability transition retains its separate old-stage input fence. Stop, lease/key binding and the bounded admission deadline remain unchanged. This candidate addresses the retained Street Story selection failure after a tool-response token wait; actual consumer acceptance is recorded separately.

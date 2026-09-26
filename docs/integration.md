# Integration contract

## Pin and import

Node: `@onedayonemasterpiece/live-interaction` dependency set to
`git+https://github.com/onedayonemasterpiece/live-interaction.git#<full commit SHA>`.
Commit the lockfile. Build browser assets with
`installBrowserAssets(destination)` from the package `/assets` export; include
these generated modules in the application's content hash/cache manifest. Never
edit or commit generated copies. Import `createLiveClient` from the generated
`client.js`; server imports `createLiveSessionHost` from the `/node` export.

Python: install `live-interaction @ git+https://github.com/onedayonemasterpiece/live-interaction.git@<same SHA>`.
Use `live_interaction.provider.run(load_key=server_credential_resolver)` for the
worker entrypoint. Default resolver reads only `LIVE_API_KEY`. Node packages
include the same Python module, so a Node consumer need not perform a separate
pip install of the project (websockets must already be installed).

`run(reader=asyncio.StreamReader(...), on_event=callback, load_key=resolver)` is
also available for a Python host. Feed bounded NDJSON commands to the reader;
callbacks receive events. Product endpoints and tool authorization belong in
the host. Python applications should reuse this transport; they do not need the
Wonderful Lections presentation service or Node to connect to Google.

## Browser API

`createLiveClient({request?, onEvent, onState, onNotice, onTiming, onWait,
voiceControl?})` returns `start`, `stop`, `input` and read-only `sessionId`,
`starting`, `generation`, `playingCount`.

`start({url,body,authorize})`: same-origin authenticated collection URL, application
start arguments (model/context IDs/history, never a key), async authorization.
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

`onEvent(event,generation)` must not block playback with a long domain refresh.
Before applying asynchronous UI results, compare the generation to the current
client generation. Tools and mutation busy/readback indicators remain app policy.
`onState`: starting, started, listening, answering, reconnecting, off,
microphone_unavailable, connection_error, start_error.
`onNotice`: voice_stop_confirmation_requested, voice_stop_cancelled,
voice_stop_expired, event_gap, transport_error, microphone_error,
connection_error, start_error; second argument may be an Error.
`onTiming` contains bounded numeric diagnostics, not speech/secret payloads.
`onWait(null | {elapsed_ms,stage,can_restart})` stays hidden below 15 seconds;
show mm:ss, a gentle pulse honoring reduced motion, and an immediate Stop.
Stage is transport until worker-send/ASR evidence, then provider. At 120 seconds,
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
readyTimeoutMs?,maxSessions?})` returns start/input/events/stop/stopAll/size.
The public host args are `resourceId`, `actor`, and, after setup, `sessionId`.
Start also takes model/history and adapter-specific arguments. Authenticate before
calling the host; it additionally binds subject+tenant+resource to each session.
Do not expose the internal actor-omitting stopAll path over HTTP.

`adapterFactory({emit,write,measure,timing})` returns:

- `initialize({resourceId,actor,model,...args})` validates access and returns
  `{state,context,configuration,response}`. No mutation during initialization.
- `executeTool(session,{id,name,args})` authorizes each tool, validates arguments,
  and runs the product's normal prepare/apply/readback semantics.
- Optional `input`, `onStarted`, `onResumed`, `onStopped` for product context and
  latest-only frame updates. Clear timers on Stop, resend latest image on resume.

`configuration` is server-owned: `system_instruction`, `context_instruction`,
`functions` (Gemini declarations), `voice` (default Aoede), `search_enabled`
(default false). Never accept arbitrary declarations/instructions from a browser.
The host caches successful tool results by provider call ID and serializes tools;
application idempotency must still survive process/session restarts.

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
tool_response, stop. Writes include numeric `queued_at` for delay measurement.
Snapshot uses video input only, never an implicit user text turn. Product images
must be current and bounded before transport. Do not replay captured audio on
recovery. Tools are cancelled only before starting; accepted writes require
normal domain reconciliation.

Current measured defaults: 256ms batch; <=11000 PCM bytes / <=16000 base64
characters; 1.5s buffered PCM, 2.5s age/request bound; one in-flight sender;
250ms preroll; conservative RMS gate; 2s quiet tail. Silence is suppressed after
the tail. PCM is required by this provider transport; AAC/OGG would need a measured
server decoder and new acceptance, not just a MIME rename. Poll 160ms, drain
has_more immediately, report gaps, and play every received audio buffer before
considering a provider closure finished. Model mic energy alone is not barge-in.

## Release gate and ownership

Core changes go here and run Node/Python tests. A consumer pins the reviewed
commit and verifies generated assets match the installed package. Existing
Wonderful Lections CI also rejects known duplicate transport implementations.
Agent instructions/skill route new integrations here, but no skill can prevent
all future deliberate divergence; code review and dependency checks enforce it.

A new adapter/release requires real browser voice acceptance: 10 turns, authorized
product actions, navigation/context changes, immediate Stop/no later audio POST,
restart, full playback and voice confirmation. For connection/recovery changes,
include long session and genuine provider resumption receipts. Keep provider
failures visible and distinguish fixtures from real acceptance.

Wonderful Lections Show should have a separate **read-only cohost tool adapter**,
not reuse Review's mutation permissions by default. Street-story / idea-hub need
their own context, authorization and UI; none is implemented by this extraction.

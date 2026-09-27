# Live Interaction

Shared Live runtime for Wonderful Lections and other product adapters.
It owns tested audio/provider lifecycle and provider-neutral capability transitions;
domain agents, permissions and product tools remain in consumer adapters.

- `browser/client.js`: microphone, bounded ordered PCM sender, conservative silence
  gate, playback, polling/cursor drain, local Stop, confirmed Russian voice Stop,
  delay notices, and restart isolation. No DOM or product tools.
- `node/sessions.mjs`: owner/resource-bound worker sessions, input limits, event
  pagination, serialized/cancelled/deduplicated tool execution, bounded cleanup.
- `python/live_interaction/provider.py`: one real Gemini WebSocket, setup,
  compression/resumption, same-model bounded recovery, provider event translation.
- `python/live_interaction/session_host.py`: in-process Python session host with
  resource/actor binding, bounded NDJSON input, event cursors and ordered/deduplicated tools.
- Product adapter: authentication, resource authorization, function declarations,
  instructions, tools, mutation/readback, images, UI and credential resolution.

Use versioned releases (current release `0.2.6`). Read the canonical
[Live agent architecture](docs/live-agent-architecture.md) before integrating a
consumer, then follow the [integration contract](docs/integration.md).
Run `npm test` and `npm run test:python`. Runtime Python requires websockets 15–16.
No browser key, alternate-model fallback, key rotation, or automatic replay of
speech/mutations. Release 0.2.6 adds a shared browser microphone capture primitive
and an optional durability-first audio sink. Accepted speech/pre-roll/tail can be
persisted before provider transport, and the same shared capture/VAD path can run
offline without a provider session. Release 0.2.5 adds lossless trusted transcription delivery plus session-level manual activity boundaries (`activityStart`/`activityEnd`) for deliberate buffered audio turns, while keeping the UI event ring bounded. Release 0.2.4 keeps catch-up active until half the steady PCM watermark so a subsequent in-flight HTTP batch cannot overflow the queue immediately after handoff. The normal 1.5s queue and 2.5s age limits still apply after catch-up. Release 0.2.3 restores bounded dialogue history and the accepted intent on a new same-key provider connection if Gemini never issues a fresh post-router checkpoint. It does not reuse a pre-call handle. Release 0.2.2 corrects the router acknowledgement wire envelope: `scheduling: SILENT` and `willContinue: false` are FunctionResponse fields, not model-visible result data. Release 0.2.1 hardens Gemini progressive transitions: the router FunctionResponse is completed on the old connection, pre-call resumption state is discarded, a fresh post-response handle is required before reconnect, and the resumed bundle receives one bounded continuation intent without a duplicate FunctionResponse. Release 0.2.0 introduced progressive capability transitions and bounded capability bundles (maximum 9 function declarations). Configuration digests/function counts/schema bytes are observable without logging prompts or tool arguments. Native Google Search and an application search function are mutually exclusive in one provider setup. Release 0.1.9 distinguishes provider-native Google Search from an application-provided search function so disabling or losing native search does not suppress a product search tool. Release 0.1.8 can capture and buffer the first user speech locally while a manually started Live session is still being created, closing the push-to-toggle startup gap without opening a second microphone. Release 0.1.7 keeps the strict steady-state audio queue/age guards while giving audio HTTP delivery a 10s absolute ceiling so a bounded startup handoff is not falsely aborted by the older 2.5s generic request timeout. Release 0.1.6 separates an intentional bounded microphone-handoff catch-up from the normal 1.5s network queue guard; after catch-up, the original steady-state queue and age limits apply unchanged. Release 0.1.5 keeps the 0.1.4 runtime while aligning Node and Python package metadata. Release 0.1.3 adds an optional provider resource guard. A
guarded consumer checks its lease before connect/setup/send/receive, charges
outbound provider payloads before transmission, and keeps the same guarded key
through provider resumption. Resource-controller failures are terminal and are
never converted into key hopping or provider reconnect. Consumers that omit the
guard keep the earlier compatibility contract until they migrate deliberately. Repository visibility is public by owner request. Versioned archives are published as GitHub Releases; npm registry publishing is not configured.

## Measured boundary

Extraction source: Wonderful Lections `bd4e3fa` (+ small numeric send timings).
Its real-browser evidence includes 10 voice turns with 3 mutations, immediate
Stop/restart, complete 63-buffer playback, spoken Stop cancellation/confirmation,
and genuine provider resumption followed by mutation. A 20-minute v1alpha
comparison completed 30 turns plus mutation and restart, but included an 80.7s
provider response outlier. v1beta had 1011 failures in longer sessions.

**A reused framework does not guarantee Google latency or successful recovery.**
The production endpoint remains v1beta; one alpha run is not evidence to switch.
Consumers must surface delay, provider closure and missing capabilities honestly.
Post-extraction acceptance evidence and exact versions belong in consumer release
reports. Unit/fixture tests are not substitutes for real provider acceptance.

Internet search is deferred. A function calling a separate lightweight search
model is an unverified hypothesis, not an implemented capability or dependency.
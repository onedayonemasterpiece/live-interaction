# Live Interaction

Shared Live runtime for Wonderful Lections and future product adapters.
The first consumer is Wonderful Lections Review. This is a small extraction of
its tested audio/lifecycle paths, not a general agent framework.

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

Use versioned releases (current release `0.1.10`); see [integration](docs/integration.md).
Run `npm test` and `npm run test:python`. Runtime Python requires websockets 15–16.
No browser key, alternate-model fallback, key rotation, or automatic replay of
speech/mutations. Release 0.1.10 makes provider-native Google Search and an application fallback search function mutually exclusive in each provider setup: native search is exposed first, and the application search function appears only after native search is disabled/unavailable. Release 0.1.9 distinguishes provider-native Google Search from an application-provided search function so disabling or losing native search does not suppress a product search tool. Release 0.1.8 can capture and buffer the first user speech locally while a manually started Live session is still being created, closing the push-to-toggle startup gap without opening a second microphone. Release 0.1.7 keeps the strict steady-state audio queue/age guards while giving audio HTTP delivery a 10s absolute ceiling so a bounded startup handoff is not falsely aborted by the older 2.5s generic request timeout. Release 0.1.6 separates an intentional bounded microphone-handoff catch-up from the normal 1.5s network queue guard; after catch-up, the original steady-state queue and age limits apply unchanged. Release 0.1.5 keeps the 0.1.4 runtime while aligning Node and Python package metadata. Release 0.1.3 adds an optional provider resource guard. A
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

Internet search can be provider-native or application-provided. When an application declares a fallback search function, the provider exposes exactly one search mechanism at a time: native Google Search first, then the application function if native search becomes unavailable.
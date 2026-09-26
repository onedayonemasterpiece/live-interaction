# Live Interaction

Private shared Live runtime for Wonderful Lections and future product adapters.
The first consumer is Wonderful Lections Review. This is a small extraction of
its tested audio/lifecycle paths, not a general agent framework.

- `browser/client.js`: microphone, bounded ordered PCM sender, conservative silence
  gate, playback, polling/cursor drain, local Stop, confirmed Russian voice Stop,
  delay notices, and restart isolation. No DOM or product tools.
- `node/sessions.mjs`: owner/resource-bound worker sessions, input limits, event
  pagination, serialized/cancelled/deduplicated tool execution, bounded cleanup.
- `python/live_interaction/provider.py`: one real Gemini WebSocket, setup,
  compression/resumption, same-model bounded recovery, provider event translation.
- Product adapter: authentication, resource authorization, function declarations,
  instructions, tools, mutation/readback, images, UI and credential resolution.

Use immutable Git commit dependencies; see [integration](docs/integration.md).
Run `npm test` and `npm run test:python`. Runtime Python requires websockets 15–16.
No browser key, alternate-model fallback, key rotation, or automatic replay of
speech/mutations. Repository visibility is private; no npm public publishing.

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

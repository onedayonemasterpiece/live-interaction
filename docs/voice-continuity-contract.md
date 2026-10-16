# Voice continuity contract (draft for wl-live-v2)

Status: **draft, 16 October 2026**. Working contract for the native Android + PWA voice
delivery (projects-hub task `docs/prompts/projects-hub-native-voice-pwa-delivery-20261016.md`).
Numbers marked *hypothesis* are starting values for the network bench, not accepted limits.
`wl-live-v1` remains supported; v2 is negotiated, never assumed.

## 1. Independent state owners

| Entity | Owner | Lifetime |
|---|---|---|
| Logical conversation | product backend | until explicit Stop / product end; survives socket loss, provider renewal, backend restart |
| Device capture | Android native service / browser binding | user Start → Stop; independent of socket and WebView |
| Socket binding | shared framework | one WSS connection generation; disposable |
| Provider session | shared framework (Python host) | many provider connections; checkpoint handle and journal survive connections |
| Durable source | product (device store + server store) | until terminal disposition and retention |
| Semantic command | product tools | idempotent by command id; outcome only by receipt/readback |

None of these implies another: an open socket is not a live provider; an ACK is not durability;
a provider transcript is not a command outcome.

## 2. Audio stages

```
captured (volatile) → device-durable → relay-admitted → server-durable → provider-forwarded
→ turn-complete → semantic-outcome
```

| Stage | Who records it | Meaning | Loss window |
|---|---|---|---|
| captured | device | PCM frame from the microphone | volatile until device-durable; bounded by device fsync interval (*hypothesis* ≤ 250 ms) |
| device-durable | device store | encoded chunk with stable `source_id/chunk_seq`, original capture time, hash | survives app restart / process death |
| relay-admitted | server socket | frame passed protocol checks | transport-only; not durable |
| server-durable | server store | compressed chunk appended and fsynced | survives backend restart |
| provider-forwarded | provider transport | decoded PCM sent on the current provider connection | lost if the provider connection drops before forwarding; reported as `input_dropped` |
| turn-complete | provider | provider ended the turn | — |
| semantic-outcome | product | tool receipt/readback or explicit no-action | — |

**v2 ACK** is cumulative per source and states the highest stage reached:
`{"type":"audio_ack","source_id":…,"chunk_seq":N,"stage":"server_durable"}`. The device may
release its copy only after `server_durable` *and* the product's terminal disposition rules.

Durable barrier: a chunk is forwarded to the provider only after `server-durable`.

## 3. Conversation and socket states

```
attached ──transient I/O──▶ detached ──reattach (fresh ticket, generation+1)──▶ attached
   │                          │
   │                          └─ no client for dormant_after ──▶ dormant (provider connection closed,
   │                                                              handle + journal kept)
   └─ explicit Stop / auth / protocol violation ──▶ stopped (fail-closed)
dormant ──speech or reattach──▶ renewing ──▶ attached
```

- `detached`: provider session continues; output that becomes stale is not replayed as audio.
- `dormant` (*hypothesis* after 60 s detached or N minutes idle): saves provider budget; resume
  by checkpoint handle while valid (provider documents 2 h), otherwise journal restore with an
  explicit `context_restored: degraded` event.
- Provider renewal: rolling-window budget with jittered backoff, not a lifetime count.
- Backend restart: logical conversation and handle are persisted; the client reattaches; this is
  not a new conversation for the user.

## 4. Outage behaviour (store-and-forward)

| Gap | Client behaviour | Server behaviour |
|---|---|---|
| short (*hypothesis* ≤ 5 s) | keep capturing; resend device-durable chunks after reattach in the same turn | dedupe by `chunk_seq`; turn stays clean if no chunk is missing |
| medium (*hypothesis* ≤ 2 min) | audible + visible "saving, will deliver": counter of undelivered seconds | logical session detached, provider kept or dormant |
| long | recording continues as a source; Live returns after delivery and processing | deferred portions processed in order with original timestamps |

Rules:
- Only identified device-durable chunks are delivered after a gap; an unknown realtime queue
  tail is never replayed.
- Deferred speech carries its original capture time to the model. The model (not a phrase list)
  judges freshness and cancellation; externally visible or irreversible actions from deferred
  speech require current confirmation. Deterministic code enforces state, idempotency and fences.
- A turn is **damaged** only when audio is actually missing (device loss before durability,
  `input_dropped`, missing chunk); a damaged turn cannot execute mutations.
- Live speech after recovery is queued behind deferred portions unless the user explicitly
  skips them ("save as note").

## 5. Wire v2 additions (negotiated)

Subprotocols offered by the client: `["wl-live-v2", "wl-live-v1", "wl-ticket.<ticket>"]`.

`hello` adds `capabilities`:
```json
{"type":"hello","protocol":"wl-live-v2","attempt_id":"…","cursor":42,"connection_generation":3,
 "capabilities":{"uplink_codecs":["opus/ogg;rate=16000;channels=1","aac/adts;rate=16000;channels=1","pcm16;rate=16000"],
                 "downlink_codecs":["opus/ogg;rate=24000","pcm16;rate=24000"]},
 "resume":[{"source_id":"src_…","last_server_durable_seq":118}]}
```
`hello_ack` selects one uplink and one downlink codec, returns per-source
`server_durable_seq`, `event_gap`, and the bounds below. The client resends device-durable
chunks after `server_durable_seq`.

Binary uplink frame v2 (`WLA2`): magic, version, flags (`deferred`, `portion_start`,
`portion_end`), `source_id` hash/index, `chunk_seq`, original capture start (ms, server-clock
offset from hello), duration (ms), codec id, payload. Payload is the encoded bytes exactly as the
server stores them (Ogg pages or ADTS frames); no transcoding before storage.

Bounds announced in `hello_ack` (all *hypothesis* until measured): max encoded bytes per frame,
max decoded duration per frame, max unacknowledged duration in flight, max decode work per
second, deferred backlog duration accepted per reattach.

Downlink: binary `WLO2` with generation and response id. On barge-in the client reports the
played position (`{"type":"playback_position","response_id":…,"played_ms":…}`) so the host can
align the agent context where the provider supports it; otherwise the UX must not claim the
model knows where it was interrupted.

## 6. What stays product-owned

Permissions, durable source rows and retention, domain tools and their idempotency, deferred
command policy wording, UI texts, Android permission/service wiring. Codec framing, decode,
reconnect, store-and-forward mechanics and interruption mechanics stay in this framework.

## 7. Open decisions

Tracked in the projects-hub delivery PR; numeric values above are not accepted until the
network bench and handset runs measure them.

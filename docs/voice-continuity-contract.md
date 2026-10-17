# Voice continuity contract (draft for wl-live-v2)

Status: **draft, 16 October 2026; clarified 17 October 2026**. Working contract for the native Android + PWA voice
delivery (projects-hub task `docs/prompts/projects-hub-native-voice-pwa-delivery-20261016.md`).
Numbers marked *hypothesis* are starting values for the network bench, not accepted limits.
Native/mobile v2 requires codec compression in **both directions**. `wl-live-v1` remains
supported as an explicitly selected PCM compatibility mode. v2 is negotiated, never assumed;
an unsupported profile is explicit, with no silent PCM/v1 fallback.

## 1. Independent state owners

| Entity | Owner | Lifetime |
|---|---|---|
| Logical conversation | product backend | durable product timeline; survives socket loss, provider renewal, backend restart and capture Stop. Stop ends the current Live/capture session, not its journal or saved sources |
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
| captured | device | PCM frame from the microphone | volatile until device-durable; measure the age of the oldest uncommitted captured sample |
| device-durable | device store | encoded chunk with stable `source_id/chunk_seq`, codec epoch descriptor, original capture time, hash | survives app restart / process death |
| relay-admitted | server socket | frame passed protocol checks | transport-only; not durable |
| server-durable | server store | compressed chunk and matching source/epoch metadata durably committed | survives backend restart |
| provider-forwarded | provider transport | decoded PCM sent on the current provider connection | provider receipt/processing can still be unknown after a send; locally dropped input is reported as `input_dropped`, and source durability is tracked separately |
| turn-complete | provider | provider ended the turn | — |
| semantic-outcome | product | tool receipt/readback or explicit no-action | — |

**v2 ACK** is cumulative per source and stage:
`{"type":"audio_ack","source_id":…,"chunk_seq":N,"stage":"server_durable"}` means that
**every chunk through N** is committed with its required metadata. N is the contiguous
committed prefix, not the largest received or individually stored sequence; an out-of-order
chunk cannot advance it across a hole. Persist enough commit information to recover the
acknowledged prefix after restart. The device may release its copy only after `server_durable`
*and* the product's terminal disposition rules. `chunk_seq` does not reset within a source
when its codec changes.

Durable barrier: a chunk is forwarded to the provider only after `server-durable`.

A device fsync cadence (*hypothesis* ≤ 250 ms) is a flush-scheduling input, **not the total
volatile-loss bound**. Measure oldest volatile sample age from capture through encoding,
packet/container assembly, writer queues and completed commit, including under load and
storage stalls. Bound the bytes and audio duration in those stages. Finish partial codec input
as a valid final packet, record true captured duration/end trimming, and flush/seal pending
container data and the final chunk when a source/codec epoch ends. A quiet or stopped tail
must not wait indefinitely for another full chunk. Local Stop stops hardware immediately;
tail commit may finish asynchronously, and its durable/failure
status must remain explicit. Source completion is recorded only after the tail and its
terminal metadata are committed.

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
  by checkpoint handle while valid ([provider session-management documentation](https://ai.google.dev/gemini-api/docs/live-api/session-management)
  gives a two-hour resumption window after the last session termination), otherwise journal
  restore with an explicit `context_restored: degraded` event.
- Provider renewal: rolling-window budget with jittered backoff, not a lifetime count.
- Backend restart: logical conversation and handle are persisted; the client reattaches; this is
  not a new conversation for the user.

## 4. Outage behaviour (store-and-forward)

| Gap | Client behaviour | Server behaviour |
|---|---|---|
| short (*hypothesis* ≤ 5 s) | keep capturing; deliver identified device-durable chunks after reattach under a recovery source/epoch | dedupe by `chunk_seq`; preserve the gap's mutation fence until a later clean provider boundary |
| medium (*hypothesis* ≤ 2 min) | audible + visible "saving, will deliver": counter of undelivered seconds | logical session detached, provider kept or dormant |
| long | recording continues as a bounded durable source; fresh Live can return while backlog remains pending | deferred portions processed in order with original timestamps under a separate recovery budget |

Rules:
- Only identified device-durable chunks are delivered after a gap; an unknown realtime queue
  tail is never replayed.
- Deferred speech carries its original capture time to the model. The model (not a phrase list)
  judges freshness and cancellation; externally visible or irreversible actions from deferred
  speech require current confirmation. Deterministic code enforces state, idempotency and fences.
- A transport gap during an open speech turn fences that turn's mutations. Missing audio,
  `input_dropped` and missing chunks also damage their correlated turn, including a loss reported
  after its end was enqueued. Only a later clean turn actually reaching the provider boundary
  can lift this fence; source completeness does not retroactively authorize the old command.
- Complete recovered chunks retain their source identity, original timestamps and recovery
  epoch. Their integrity and the authority to execute a deferred instruction are independent.
- Fresh Live readiness depends on its negotiated transport, provider readiness and available
  live budget, not completion of older source delivery or semantic processing. Each fresh
  chunk still crosses the durable barrier. Pending recovery cannot consume the entire live
  in-flight, write or decode budget; its progress/status remains visible independently.
- Local Stop bypasses every delivery/analysis queue and stops hardware immediately; saved source
  bytes remain available. Fresh speech is captured on its own timeline and is not trapped behind
  the whole deferred backlog. At a bounded processing boundary it takes priority over further
  deferred semantic processing, with pending-source context visible to the same central Mira.
- New speech detection may reversibly pause replay/playback. Mira decides the meaning of an
  interruption, cancellation or skip; no phrase list or second conversational agent does so.
  Resuming or disposing of a saved source is explicit and does not replay accepted mutations.

## 5. Wire v2 additions (negotiated)

Subprotocols offered by the client: `["wl-live-v2", "wl-live-v1", "wl-ticket.<ticket>"]`.

`hello` adds `capabilities`:
```json
{"type":"hello","protocol":"wl-live-v2","attempt_id":"…","cursor":42,"connection_generation":3,
 "capabilities":{"uplink_codecs":["opus/ogg;channels=1","aac/adts;rate=16000;channels=1"],
                 "downlink_codecs":["opus/ogg;channels=1"]},
 "resume":[{"source_id":"src_…","last_server_durable_seq":118,"pending_codec_epochs":["ce_1"]}]}
```
These codec choices are bench candidates, not an accepted quality profile. `hello_ack`
selects one compressed uplink and one compressed downlink profile, returns per-source
`server_durable_seq`, `event_gap`, and the bounds below. Its durable cursors come from the
server's committed prefixes, not the client's last-observed ACK. The client resends
device-durable chunks after `server_durable_seq` under the recovery budget.

Each source codec epoch has an immutable descriptor: epoch id, codec/container and version,
channels, codec timestamp timebase, decoded PCM rate, and required decoder initialization
(including stream headers and pre-skip where applicable). Persist it with the source and
reference it from every chunk. New profile negotiation starts a new codec epoch; it does not
reinterpret or transcode stored originals. Socket/provider generations and recovery scheduling
epochs are separate from this codec identity.

Pending old-epoch descriptors remain available across reconnect/renegotiation. Negotiate
their recovery support separately from the new live profile and decode them with their own
ordered state. An unsupported old epoch stays explicitly pending/failed with original bytes
retained under the source disposition rules; it cannot be silently counted as delivered or
block an otherwise ready fresh Live profile.

For Opus in Ogg, granule positions and pre-skip use a **48 kHz clock**, independent of encoder
input rate, requested decoded PCM rate and device playback rate. The current provider boundary
takes decoded 16 kHz PCM and emits 24 kHz PCM; this does not change the Ogg timebase or require
24 kHz decoding on the phone. Do not label these as interchangeable `rate` values or derive
capture duration from compressed byte count. Page or storage/transport chunk boundaries do
not imply independently decodable audio: Ogg packets may span pages, and decoding requires
stream headers plus ordered codec state or a defined restart/pre-roll boundary. Preserve those
dependencies during tail flush and partial recovery
([Ogg Opus specification](https://www.rfc-editor.org/rfc/rfc7845)).

Binary uplink frame v2 (`WLA2`): magic, version, flags (`deferred`, `portion_start`,
`portion_end`), `source_id` hash/index, `chunk_seq`, original capture start (ms, server-clock
offset from hello), duration (ms), codec epoch id, payload. Payload is the encoded bytes exactly
as the server stores them (Ogg stream segments or ADTS frames); no transcoding before storage.

Bounds announced in `hello_ack` (numeric values remain *hypothesis* until measured): max
encoded bytes and decoded duration per frame; live unacknowledged bytes/duration and oldest
age; decode work per second; separate recovery in-flight/work and per-reattach limits.
Device and server also enforce finite volatile RAM and disk-spool byte/duration limits,
oldest volatile sample age, and source retention. Local limits remain authoritative alongside
negotiated peer limits. At a bound, surface backpressure/full/error status; never accumulate
unbounded audio or silently discard an undelivered durable source. Account for live and
recovery work separately so fresh input and Stop retain their priority.

Downlink: compressed binary `WLO2` with generation, response id and codec epoch id.
On barge-in the client reports the played position
(`{"type":"playback_position","response_id":…,"played_ms":…}`) so the host can
align the agent context where the provider supports it; otherwise the UX must not claim the
model knows where it was interrupted.

## 6. What stays product-owned

Permissions, durable source rows and retention, domain tools and their idempotency, deferred
command policy wording, UI texts, Android permission/service wiring. Codec framing, decode,
reconnect, store-and-forward mechanics and interruption mechanics stay in this framework.

## 7. Open decisions

Tracked in the projects-hub delivery PR; numeric values above are not accepted until the
network bench and handset runs measure them.


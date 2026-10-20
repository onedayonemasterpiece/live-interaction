# Voice continuity contract (draft for wl-live-v2)

Status: **implementation candidate, 17 October 2026; physical/provider acceptance pending**. Continuation of the 16 October draft for native Android + PWA voice
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

- `detached`: the provider session and its bounded output archive continue. A receiver can
  request original compressed output from its actual played position; cancelled responses
  remain saved and are excluded from automatic playback.
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

The native v2 candidate offers `["wl-live-v2", "wl-ticket.<ticket>"]`. Explicitly selecting
the legacy compatibility client instead offers `wl-live-v1`; a failed v2 handshake never
silently changes the codec or transport version. Existing Cookie, exact Origin and one-use
ticket checks still apply to both protocols.

`hello` adds `capabilities`:
```json
{"type":"hello","protocol":"wl-live-v2","attempt_id":"…","cursor":42,"connection_generation":3,
 "capabilities":{"uplink_codecs":["opus/ogg;channels=1","aac/adts;rate=16000;channels=1"],
                 "downlink_codecs":["opus/ogg;channels=1"]},
 "resume":[{"source_id":"local_…","last_server_durable_seq":118,"pending_codec_epochs":["ce_1"]}],
 "playback_resume":{"source_id":"out_session_7","played_ms":420}}
```
These codec choices are bench candidates, not an accepted quality profile. `hello_ack`
selects one compressed uplink and one compressed downlink profile and returns `event_gap`,
`recovery_profiles`, and the bounds below. Pending input sources are initially
`awaiting_source_descriptor`; subsequent `source_bound` returns the authoritative
`server_durable_seq` after actor/conversation-scoped source binding. The server never
promotes a client's last-observed ACK into a durable receipt. Source binding and old-source
delivery do not delay the hello acknowledgement or readiness of a fresh Live lane.

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

### 5.1 Executable frame and input hooks

Both `WLA2` and `WLO2` have exactly **56 header bytes**, big-endian integers, followed by
the original encoded payload. The magic identifies version and direction.

| Offset | Bytes | Field |
|---:|---:|---|
| 0 | 4 | ASCII `WLA2` / `WLO2` |
| 4 | 1 | flags |
| 5 | 1 | reserved, zero |
| 6 | 2 | header length, 56 |
| 8 | 4 | socket-local source/stream reference, nonzero |
| 12 | 4 | socket-local codec epoch reference, nonzero |
| 16 | 8 | source-wide chunk sequence, first 1, at most 2^53−1 |
| 24 | 8 | original source-relative capture/media start, microseconds |
| 32 | 4 | valid captured/media duration, microseconds |
| 36 | 4 | encoded payload length |
| 40 | 4 | delivery generation; distinct from source and codec identity |
| 44 | 4 | reserved, zero |
| 48 | 8 | original host event sequence for output; zero for input |

Flags are `RECOVERY=1`, `PORTION_START=2`, `PORTION_END=4`, `EPOCH_END=8`, and
output-only `EVENT_END=16`. `RECOVERY`, delivery generations, references and `EVENT_END`
are not part of durable chunk identity. Stable identity includes source/epoch IDs, source-wide
sequence, original source-relative start and duration, portion/EPOCH_END flags, and SHA-256
of the unchanged payload. A retry can change its transport lane without changing its original.

`source_open` carries `source_id`, `started_at_unix_ms`, `source_time_now_us`, and
`lane: live|recovery`. The product assigns `server_source_id`; client metadata never selects
the actor or conversation. `source_bound` returns both IDs, `source_ref` and committed prefix.
`codec_epoch` carries `source_ref` and the immutable descriptor; `epoch_bound` returns
`epoch_ref`. A socket admits one new live source and up to eight recovery sources. A source
with a previously committed prefix uses recovery; fresh Live starts its own new source, so
an old missing chunk cannot block it. Sequence numbers stay source-wide across codec epochs.

`source_seal` records `source_ref`, `last_chunk_seq` and `capture_end_us`. This intent can
precede still-missing recovery chunks. Completion requires the contiguous prefix through
that final sequence, exact valid end, and terminal chunks for every used codec epoch.
Acknowledging a seal request alone is not a completed source receipt.

`source_sealed` includes the store's `server_durable_seq` and `complete` result.
`source_unbind` / `source_unbound` release only socket references, never original bytes or
their disposition. Recovery references can be recycled; a former live source can be unbound
after its complete durable seal and decoder EOS. New references increase monotonically,
so freed slots cannot reinterpret late frames. This permits further sources without a
reconnect that would interrupt fresh capture.

The entire captured original, including silence, is encoded and stored. Portion flags limit
which decoded spans reach the Live provider; the decoder still receives the ordered complete
codec stream. The typed `EncodedAudioStore` hook commits original bytes before one stateful
decode. `DurablePCMProjection` then enters `LiveSessionHost.input_pcm` and the product's
`input_audio` hook. Its receipt is an internal Python value, never a JSON bypass flag.
Legacy `adapter.input(audio_base64)` is not called again for that projection. The same decoded
PCM can feed captions. Stop/socket generation are checked after awaited product hooks before
provider delivery. Recovery commits/ACKs never automatically replay old speech into fresh Live.

Bounds announced in `hello_ack` (numeric values remain *hypothesis* until measured): max
encoded bytes and decoded duration per frame; live unacknowledged bytes/duration and oldest
age; decode work per second; separate recovery in-flight/work and per-reattach limits.
Device and server also enforce finite volatile RAM and disk-spool byte/duration limits,
oldest volatile sample age, and source retention. Local limits remain authoritative alongside
negotiated peer limits. At a bound, surface backpressure/full/error status; never accumulate
unbounded audio or silently discard an undelivered durable source. Account for live and
recovery work separately so fresh input and Stop retain their priority.

The executable candidate caps each payload at 8,192 bytes and valid duration at 240 ms;
decoder work and codec initialization have separate hard caps. Input windows are 32,768 bytes /
960 ms for live and 16,384 bytes / 480 ms for recovery. Source/disk retention limits remain
product-owned and finite. These are implemented admission limits, not measured throughput,
quality or maximum volatile-loss guarantees.

### 5.2 Original output, received durability and actual playback

`AudioV2Services.output_store_factory` supplies an actor/conversation-scoped
`EncodedOutputStore`. The existing host owns one ordered, bounded archive worker and native
codec state across socket loss. Each provider response is one stable `out_<session>_<response>`
source and one codec epoch in this candidate. A PCM-rate change inside a response fails
explicitly. Output is encoded once, committed with its original event sequence, and sealed
after the real codec tail. Detached sockets do not cancel archiving. Interrupted/Stopped tails
can finish persistence while playback cancellation remains immediate; saved cancelled output
is marked as such and cannot automatically resume as speech.

Android rc12 persists the same local playback decision through
`DurableOutputStore.cancelPlayback` and `PlaybackPosition.playbackCancelled`.
The normal output worker commits it after immediate hardware interruption and
before the Stop completion barrier. A queued source open or late original chunk
rechecks the fence, so a delayed write cannot resurrect cancelled speech. A new
Run restores these markers separately from actual played positions. Received
bytes, canonical audio and already-played counters remain unchanged; existing
unmarked journals remain valid and are not guessed to be cancelled. This prevents
one identified local restart/replay path; it does not by itself establish the
origin of any historical handset incident or replace provider/device testing.

`output_epoch` binds `source_id`, `server_source_id`, `stream_ref`, `provider_response_id`,
`epoch_ref`, `server_durable_seq`, `connection_generation` and the original descriptor.
`WLO2` carries unchanged compressed chunks. Android syncs each original output chunk before
both playback and `output_ack {source_id,chunk_seq,stage:"device_durable"}`. This cumulative
prefix frees the bounded delivery window; it is not a played position. Output live/recovery
windows have independent byte/duration caps, with the same candidate sizes as input.

The receiver reports `playback_position {source_id,played_ms,connection_generation}` from
actual AudioTrack progress. It persists this separately from received event/chunk cursors.
The provider's understanding of an interruption still depends on its supported context
alignment API; recording a played position does not imply that the model knows it.

After hello, `output_available.sources` is a bounded manifest (at most eight pending sources)
with IDs, committed prefix, `played_us`, `duration_us`, `complete` and `cancelled`. It is an
availability notice, not automatic playback. The optional hello `playback_resume`, or later
`output_resume {source_id,played_ms}`, requests one source in the independent recovery lane.
The current decoder replays original headers and the ordered compressed prefix and discards
already played samples; it does not invent random-access codec state. Prefix transfer/decoding
is bounded per batch and paced independently of fresh Live. Efficient indexed Opus seeking
with preroll remains a performance gate, not an implemented promise.

Replay preserves original event sequence metadata but omits `EVENT_END`, so old-session audio
does not advance a new session's text cursor. `output_replayed` reports `through_seq` and
`complete`. A persisted prefix lacking a true terminal chunk reports incomplete recovery;
neither server nor device invents EOS, a played receipt or source completion. Cancelled
originals remain stored and are excluded from the pending playback manifest.

After playback progress, `output_list` requests the next bounded pending manifest. It is
ordered after the prior played-position commit, allowing the ninth and later saved response
to progress without a socket reset. A recovery reader waits for an original still being
produced by this host; an empty read batch is not mistaken for a crashed/incomplete source.

The Python implementation uses the optional pinned `audio` extra (`av==18.1.0`) and runtime
codec capability checks. Opus/Ogg is the primary profile; mono 16 kHz AAC-LC/ADTS is explicit
uplink compatibility when native capability and encoder delay are available. The single shared
decode supplies 16 kHz provider/caption PCM; Opus clock/pre-skip stay 48 kHz. There is no Go/Rust
port, per-chunk ffmpeg process, second caption decoder, or second provider host.

`tests/fixtures/wl-live-v2-vectors.json` is consumed by Python and pure Java conformance tests.
Native codec tests exercise pre-skip, partial tails, CRC/sequence/expansion rejection; WSS tests
exercise durable barriers, separate lanes, Stop fences, output windows, detached archiving,
and byte-identical replay after a new host session. These tests do not replace handset,
network, power-loss, load, or real-provider release acceptance.

## 6. What stays product-owned

Permissions, durable source rows and retention, domain tools and their idempotency, deferred
command policy wording, UI texts, Android permission/service wiring. Codec framing, decode,
reconnect, store-and-forward mechanics and interruption mechanics stay in this framework.

## 7. Open decisions

Tracked in the projects-hub delivery PR; numeric values above are not accepted until the
network bench and handset runs measure them.


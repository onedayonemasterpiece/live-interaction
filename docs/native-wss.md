# Native WSS integration and runtime decision

Status: 0.3.6-rc.1 integration candidate, not a stable production acceptance receipt.
Decision date: 2026-09-30. Replaces the provisional Node-sidecar plan.

## Decision

Keep each existing product in its native runtime. Both Street Story (FastAPI)
and KenigEvents Live search (aiohttp) already own domain policy in Python and
use the same Python provider/session host. Wonderful Lections owns its domain
adapter in Node. Add one reusable Python socket binding to this package; do not
add a Node sidecar to each Python product and do not rewrite unrelated domains.
New products choose their backend runtime deliberately; Live must not dictate it.

The alternative of porting all Street Story to Node is feasible, not inherently
prohibitively difficult. It is rejected here because it does not eliminate the
existing Python provider or the Python KenigEvents host, while it would require
re-verifying SQLite migration/recovery, research grounding, revisions, idempotency
and social publication. A Node sidecar retains all those Python responsibilities
and adds process/IPC deployment coupling. This decision is about long-lived
failure boundaries, not sunk source effort or an unmeasured language benchmark.

The cost we accept is maintaining two small framework socket bindings. The
provider implementation, wire contract, limits, release policy and conformance
vectors stay common. Native and browser capture remain platform-specific; they
must not be reimplemented separately inside every product.

## Ownership

- Framework: ticket lifecycle, handshake, ordered bounded transport, PCM framing,
  event push, damage detection, provider lifecycle and protocol tests.
- Product: user authentication, origin policy, resource authorization, tools,
  current context, durable state, explicit publish consent and readback.
- Android: capture/VAD and UI through the shared Java socket transport; no API
  keys or provider calls. Browser: existing shared AudioWorklet client.
- Deploy one isolated runtime per product. Do not share a running Wonderful
  Lections process, database, credentials or outage domain with Street Story.

## Python HTTP integration

Construct `LiveSocketSessionHost` with the existing adapter/managed runner.
Authenticated session start returns a one-use 15-second ticket, protocol and
attempt_id. The HTTP adapter adds a same-origin relative socket_url. Renewal
uses `issue_socket_ticket` with the authenticated actor and resource. Origin
and query-string rejection run BEFORE `open_socket` consumes the ticket.
No bearer credential, ticket or resumption handle belongs in the socket URL.

After accepting the negotiated `wl-live-v1` subprotocol, call `serve_socket`
from `live_interaction.socket_transport`, supplying async receive/send/close
callbacks. This is identical for FastAPI and aiohttp; neither product copies the
protocol parser. TLS remains at the product's existing reverse proxy.

Binary audio uses a 12-byte network-order header: WLA1 magic, frame sequence,
original capture age in milliseconds, followed by signed PCM16 little-endian,
16 kHz mono. Output uses WLO1, event sequence, sample rate. Audio payload is at
most 11000 bytes. The relay rejects stale (>2500 ms) or out-of-order audio.
The native sender batches at 100 ms, uses a bounded four-frame ACK window, and
caps total queued/unacknowledged PCM at 1.5 seconds. ACK proves relay admission,
NOT provider processing, comprehension or tool success.

Readiness requires hello_ack, not merely a TCP/WebSocket open. Socket events
are pushed without polling. JSON ping/pong refreshes application liveness;
protocol/TLS failures never silently fall back to HTTP audio. Tickets are never
reused. In this initial native binding, reconnect is an explicit Stop/Start:
pending speech is discarded and authoritative product state is refreshed.
Automatic native reconnect/20-second startup capture is NOT claimed.

For Python browser consumers disable captureDuringStart until the shared
startup-catchup admission path is separately accepted; do not enlarge the normal
capture-age limit to conceal old speech. A missing setup-ready indication must
be visible so users do not believe their speech was accepted.

A gap during an open turn marks input damaged and closes its normal provider
activity boundary. New product tools are blocked until a subsequent clean turn
boundary actually reaches the provider. Existing accepted mutations are never
replayed. Unknown write outcomes require authoritative readback.

## Android distribution

`android/src/main/java/org/onedayonemasterpiece/live/LiveSocketTransport.java`
is the product-neutral native binding (Java 17, OkHttp 4.12, Gson). Products
consume it from this versioned, digest-verified source archive as a generated
sourceSet. Never edit a copied SDK inside a product. Capture and playback remain
native Android responsibilities. Stop must stop hardware immediately; provider
closure must not truncate already received playback.

## Release/acceptance

Required: Node and Python wire vectors; bad ticket/origin/resource; sequence,
age and byte limits; Stop; concurrent push; damaged-turn tool refusal/recovery;
Android mock WebSocket tests; real TLS upgrade through the public reverse proxy;
real prepared-audio turns, application tools/search and result readback.
Physical microphone, long-running availability and subjective voice quality are
separate evidence. A green build or provider setup alone is not acceptance.

Measure setup, capture-to-relay, relay-to-provider boundary, first transcript,
first audio, tool duration, disconnect reason and clean recovery. Keep only
bounded sanitized identifiers/timings/codes, not credentials, full transcripts
or PCM in routine logs. Do not promise external provider response latency.

A new shared release requires all affected consumers' regression gates; changes
are promoted explicitly, never as a runtime auto-update. Rollback restores the
previous immutable release without changing durable data or auth.

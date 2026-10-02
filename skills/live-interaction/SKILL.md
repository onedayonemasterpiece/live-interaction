---
name: live-interaction
description: Integrate or change interactive Live voice/video sessions in Wonderful Lections, street-story, idea-hub or another product using the shared live-interaction framework. Use for microphone transport, model playback, Live lifecycle, recovery or new Live product adapters; ordinary recorded voice-review is a separate flow.
---

# Shared Live interaction

Canonical implementation: `onedayonemasterpiece/live-interaction` (public),
local checkout `/home/dev/projects/live-interaction`. Read its
[adapter contract](https://github.com/onedayonemasterpiece/live-interaction/blob/main/docs/integration.md)
from the local checkout or versioned dependency before implementation. The repository and its versioned GitHub release archives are public.

Use semantic release versions. For npm consumers, the framework source helper
`scripts/update-consumer.mjs <product> <version|latest> <managed-artifacts>` updates
the release archive and lockfile; run build/acceptance before deploying. Never
auto-update a running session.

Use the shared browser client and Python provider; Node hosts also use the shared
session host. Add product tools/context/UI/authorization as adapters. Improve a
missing transport capability in the shared repository, then update the consumer's
versioned release dependency and lockfile. Do not fork/copy an independent audio queue,
provider websocket, playback or Stop implementation into a product.

For 0.3.x migrations prefer the versioned WSS contract: authenticated HTTP session
bootstrap, one-use socket ticket in the WebSocket subprotocol, matching
`wl-live-v1` hello acknowledgement, binary PCM, pushed provider events/audio and no
silent fallback to the legacy HTTP audio/polling path. Keep browser/server/provider
connection generations separate and reject damaged-turn mutations after reconnect.

Build browser modules from the installed package; generated copies are ignored
and covered by the consumer's asset hash/cache manifest. Enforce semantic release versioning, release archive digest,
lockfile provenance and generated-asset equality in the consumer's CI/build.
Wonderful Lections has `scripts/verify-live-framework.mjs` as the first example.
This guard plus review enforces known integration paths; a skill alone does not
prove arbitrary future code cannot diverge.

Keep key resolution server-side and specific to the product/deployment. Different
products may use different keys. Resolve once per session, keep the same binding
on resume, whitelist child environment, and never rotate keys to mask quota failure.
Do not ask the user to paste credentials or put them in browser config/receipts.

When a product needs a local control-plane recognizer while Live owns the microphone, use the shared browser client's optional `captureTap(pcm,rms)` after playback suppression. Keep that tap synchronous, bounded and local-only; do not open a second `getUserMedia`, create a competing audio transport, or delay provider delivery. Copy frames before retaining/transferring them and keep product command semantics outside the shared runtime.

Preserve one ordered bounded sender, silence preroll/tail, immediate local Stop,
no stale speech replay, full received output playback, separate spoken Stop
confirmation, owner/resource checks and serialized authorized tools. An intentional
same-microphone startup handoff may use the framework's bounded catch-up seed; do not
weaken the normal steady-state queue/age limits to accommodate it. Audio HTTP delivery
may have a longer absolute ceiling than the steady queue/age limits; the sender remains
the primary steady-state liveness guard. A Show/cohost
adapter should start read-only; do not inherit Review mutation permissions.

A deliberate buffered source is different from transport reconnect replay. When a
product must deliver one already-durable long recording as one logical Live turn,
use the shared manual-activity session contract: enable
`configuration.manual_activity_detection`, send `activity_start`, ordered PCM,
then `activity_end`; do not substitute `audio_stream_end` or a product-local
transport. Trusted product `on_event` observers receive the full provider
transcription before the bounded polling/UI projection. A durable source sink must
persist or durably enqueue that trusted event before returning; never reconstruct an
archive from the 320-event ring or browser transcript preview.

Surface waits after 15s with elapsed mm:ss, truthful transport/provider stage,
Stop, and explicit restart after 120s. Restart must not replay a pending mutation.
Do not promise external provider latency or resumption success.

Run shared Node/Python contracts and affected consumer regressions. For transport
changes run real browser speech acceptance (10 turns plus product actions,
context changes, Stop/restart, full playback and spoken confirmation), and long
session/recovery checks where relevant. Keep receipts with exact source receipts and dependency
versions, timings and provider failures. Unit tests and setup success are not Live
acceptance. Use managed artifact storage where provided by the environment.

Internet search via a separate lightweight model remains an unverified technical
debt hypothesis. Do not implement or advertise it as available unless the user
requests it and a real provider probe verifies capability.
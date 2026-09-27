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

Preserve one ordered bounded sender, silence preroll/tail, immediate local Stop,
no stale speech replay, full received output playback, separate spoken Stop
confirmation, owner/resource checks and serialized authorized tools. An intentional
same-microphone startup handoff may use the framework's bounded catch-up seed; do not
weaken the normal steady-state queue/age limits to accommodate it. A Show/cohost
adapter should start read-only; do not inherit Review mutation permissions.

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

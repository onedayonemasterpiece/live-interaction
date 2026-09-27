# Live agent architecture and operating standard

Status: canonical shared design for products using `@onedayonemasterpiece/live-interaction`.

Last reviewed: 2026-09-27.

This document defines how a product should expose prompts, tools, capabilities,
conversation state, visual context, resource accounting and diagnostics to a
realtime model. It is deliberately product-neutral. Wonderful Lections, Street
Story and future Live consumers keep domain semantics in their adapters while
following this shared interaction contract.

## Why this exists

Realtime models are not conventional request/response agents. A persistent
audio session mixes latency-sensitive conversation, tool choice, context
growth, interruption, background speech and provider/session limits. Reliability
degrades when a product sends one large system prompt plus every available tool
schema on every session.

The shared design therefore uses **progressive capability disclosure**:
the realtime model receives a small stable core and only the domain capability
bundle needed for the current task.

This is not a deterministic intent classifier replacing the model. The Live
model remains the conversational controller and chooses when it needs a
capability. The runtime only enforces the capability transition and permissions.

## External guidance behind the design

The contract is based on the current provider guidance, not a product-specific
preference:

- OpenAI Realtime recommends short, labelled prompt sections, precise
  non-conflicting instructions, explicit tool use/avoid rules, and keeping tool
  availability synchronized with the prompt.
  https://developers.openai.com/api/docs/guides/voice-prompting
- OpenAI Tool Search loads deferred tool definitions only when needed and
  recommends clear namespaces with fewer than 10 functions for token efficiency
  and model performance.
  https://developers.openai.com/api/docs/guides/tools-tool-search
- OpenAI Realtime allows tools at session or response scope and recommends a
  narrow allowed tool surface for MCP integrations.
  https://developers.openai.com/api/docs/guides/realtime-mcp
- Gemini Live recommends one persona/role at a time, explicit invocation
  conditions, prompt chaining instead of lengthy multi-page prompts, and notes
  that Live performs best on tasks involving a small number of function calls.
  https://ai.google.dev/gemini-api/docs/live-api/best-practices
- Gemini Live configuration cannot change on an open WebSocket, but Google
  explicitly permits changing configuration parameters other than the model
  when pausing/resuming through session resumption.
  https://ai.google.dev/api/live
- Gemini Live recommends context-window compression and session resumption for
  long-running sessions.
  https://ai.google.dev/gemini-api/docs/live-api/session-management

Provider documentation is evidence for the architecture, not a substitute for
our production acceptance. Every provider/model upgrade still needs real Live
tests.

## 1. Prompt layers

A Live session prompt is assembled from small layers. Never maintain one
monolithic product prompt.

### 1.1 Core layer

The core is stable across the entire product session and must remain small.
It contains only:

- identity/persona and output language;
- conversational style and brevity;
- interruption and unclear-audio behaviour;
- continuity rule: reconnect/resume is not a new conversation;
- truthfulness about tool results: never claim success before a successful
  result/readback;
- the rule that only tools in the current tool list exist;
- the capability-router contract;
- any cross-product safety invariant that cannot live in a tool boundary.

The core must not contain detailed instructions for datasets, media, slide
geometry, publishing, maps, search providers, database schemas or other
capabilities that are not currently loaded.

Use short labelled sections and bullets. Avoid duplicated MUST/NEVER rules.
When two rules can conflict, specify priority explicitly.

### 1.2 Mode overlay

A mode adds only rules that are genuinely different for that product mode.

Examples:

- `show`: cohost behaviour, background speech, semantic yield, no mutations;
- `review`: continuous authoring conversation, explicit user edit intent,
  reversible low-risk edits;
- `capture`: listening/transcription only;
- `guide`: location/story conversation.

Changing mode is a product state transition, not a new persona unless the
product genuinely changes agents.

### 1.3 Capability overlay

Only one primary domain capability should normally be active at a time.
The overlay contains the short workflow and the invocation/avoid rules for the
functions in that bundle.

A good capability overlay looks like:

- Objective
- Use when
- Do not use when
- Read path
- Write path / confirmation boundary
- Success condition
- Failure recovery

Do not copy generic persona/conversation rules into a capability overlay.

## 2. Capability router

Every interactive authoring session starts with a tiny router surface plus the
core read context required to understand the user's request.

The model chooses a named capability by calling a router function such as:

`activate_capability({capability, intent})`

The server validates that the capability is allowed for the current actor,
resource and mode. The model does not supply tool definitions or permissions.

Recommended capability IDs are product-owned, for example:

- `slide_edit`
- `media`
- `dataset`
- `deck`
- `verification`
- `search`

Each bundle should normally contain **fewer than 10 functions**. Prefer 3–6.
If a bundle grows beyond that, split it by user intent rather than by backend
module layout.

The router remains available inside every bundle so the model can switch when
the user's intent changes. A `release_capability`/return-to-core transition may
be used after the task completes.

## 3. Provider-neutral deferred loading

The shared runtime models capability loading as a state transition even though
providers expose different primitives.

### OpenAI Realtime

Use provider-native session/response tool scoping or deferred tool search when
available. Do not send definitions for unrelated namespaces.

### Gemini Live

Gemini does not accept configuration changes on an open WebSocket. Capability
switching therefore uses a **safe session-resumption boundary**:

1. The model requests a capability.
2. The application validates it and records a pending transition.
3. Do not reconnect while a tool call or model generation is in flight.
4. Wait for a resumable session handle / safe turn boundary.
5. Pause the provider connection.
6. Resume the same model with:
   - the latest resumption handle;
   - the same core prompt;
   - the current mode overlay;
   - the new capability overlay;
   - only the router + selected bundle functions.
7. Emit `capability_ready`.
8. Continue the pending user intent without requiring the user to repeat it.

Google documents that configuration parameters except the model can change
while pausing/resuming. We still require provider acceptance because preview
behaviour can change.

If no resumable handle becomes available within a bounded transition window,
the runtime may open a new provider session only if the product can restore
bounded dialogue history and the pending intent without replaying a mutation.
The UI conversation remains continuous; transport identity is an
implementation detail.

Never switch model or API key merely to load a capability.

## 4. Conversation continuity

The product conversation is longer-lived than a provider WebSocket.

Maintain a bounded application conversation summary/history outside the
provider session. A Stop/pause, capability transition, GoAway reconnect or
provider resumption must not automatically insert user-visible messages such as
"conversation ended" or reset the task.

The user should experience:

`Space ON -> work -> Space OFF -> Space ON -> continue where we left off`.

A new provider transport is not a new user conversation.

Do not replay previously accepted write commands after reconnect. Preserve
idempotency IDs and reconcile authoritative product state instead.

## 5. Tool contracts

A tool description must state when it is appropriate and when it is not.

Read tools can be proactive when intent and identifiers are clear. Write tools
must follow the product's explicit confirmation policy. A user's direct request
to edit an object can itself be sufficient confirmation for low-risk,
reversible authoring if the product contract says so; destructive or
high-impact actions require a stronger boundary.

After any mutation:

1. execute the normal domain mutation path;
2. obtain authoritative readback;
3. only then let the model state that the change succeeded.

A failed tool call must produce a bounded structured error code. Do not expose
raw stack traces, provider credentials or transport internals to the model.

The prompt must never mention a tool that is absent from the current bundle.
The tool list and instructions are one versioned capability contract.

## 6. Visual and multimodal context

Do not stream a visual snapshot merely because a UI has a current screen.

A capability declares whether it needs visual context:

- `none`: no frame;
- `on_activate`: one current snapshot after capability activation;
- `after_mutation`: refresh only after a successful change;
- `on_demand`: model explicitly requests a current snapshot.

Continuous/repeated frames are permitted only for product scenarios that have
measured evidence that they are required.

Images and video must have modality-aware resource accounting. Base64 transport
bytes are an encoding detail and must never be counted as text tokens.

## 7. Long-session policy

For Gemini Live:

- enable context-window compression for long sessions;
- retain the latest session-resumption handle;
- handle GoAway before forced disconnect;
- treat `generationComplete` and `turnComplete` distinctly;
- preserve playback through provider closure when appropriate;
- never replay captured speech or mutations on recovery.

The framework owns provider transport mechanics. Consumers own the semantic
decision to stay active, pause, yield or stop.

## 8. Resource accounting

Resource control is a separate trust boundary from prompt/tool selection.

Required invariants:

- one lease/key remains sticky after provider readiness;
- no post-ready key hopping;
- provider quota/capacity decisions are not bypassed by local fallback;
- local fallback is only for an unavailable central authority according to the
  shared resource-control contract;
- estimates are **modality-aware**.

At minimum log and account separately for:

- setup/system/tool schema envelope;
- text turns;
- PCM audio duration;
- images/video frames;
- tool responses.

Never estimate an image by serializing its base64 bytes and charging those bytes
as text. The resource estimator must remove/bypass binary/base64 bodies and use
a calibrated image/video estimate.

Provider-reported usage is authoritative evidence for calibration; local
estimates are conservative admission controls, not a substitute tokenizer.

## 9. Observability contract

Every production consumer must expose bounded sanitized Live audit logs.
Debugging a screenshot without correlated runtime evidence is not acceptable.

### Required correlation fields

- `session_id`
- product/resource ID
- consumer
- provider/model
- mode
- active capability
- configuration digest/version
- capability transition ID when applicable

### Required lifecycle events

- session start requested / ready / failed / stopped;
- provider ready / resumed / GoAway / closed;
- session-resumption handle availability as a boolean (never the handle);
- capability requested / transition started / ready / failed / released;
- tool call start / ok / error with tool name and error code;
- turn/generation boundaries;
- audio stream end;
- visual snapshot sent, including only dimensions/encoded byte count/estimated
  resource units;
- resource grant requested / granted / denied, including modality and units;
- resource-control fallback activation;
- stop/yield reason.

### Forbidden log content

Do not log:

- API keys, wrapped keys, bearer tokens or review capabilities;
- resumption handles;
- raw audio or image/base64 payloads;
- full transcripts by default;
- tool arguments that may contain user data;
- tool results that may contain user data;
- system prompts in full.

For prompt/config diagnostics log a stable version/digest plus counts:
instruction characters, function count, schema bytes and capability names.

Production log retention should be short and bounded. Consumers may add a
separate explicitly authorized diagnostic mode for transcript capture, but it
is not the default audit path.

## 10. Failure semantics

Keep failures attributable:

- `provider_error`: provider/session failure;
- `resource_error`: shared quota/lease/budget failure;
- `tool_error`: product capability call failed;
- `transition_error`: capability reconfiguration/resumption failed;
- `transport_error`: product-to-worker/browser transport;
- `authorization_error`: actor/resource permission failure.

Do not collapse these into "Live failed".

Do not automatically retry the same write tool with the same arguments after an
unknown failure. Reconcile first.

## 11. Acceptance and eval matrix

A framework or capability release is not complete with unit tests alone.

### Static/contract tests

- prompt mentions only current tools;
- each capability bundle is within its tool-count budget;
- tool schemas are bounded;
- Show/read-only modes cannot obtain mutation bundles;
- configuration digest is stable for equivalent input;
- audit sanitizer rejects transcripts, args, payloads and secrets.

### Model/tool evals

Maintain representative conversations for:

- happy path;
- ambiguous intent;
- capability switch;
- switch back to core;
- tool failure;
- repeated failure;
- interruption/barge-in;
- background speech/no-op;
- user changes task midway;
- reconnect/resume during a long conversation.

Score the actual tool sequence and final product state, not only prose quality.

### Real provider acceptance

For every provider/model/framework combination that matters in production:

- real microphone/audio path;
- immediate speech after activation;
- several consecutive turns;
- capability activation;
- at least one read and one permitted write;
- readback after mutation;
- Stop/restart continuity;
- interruption;
- provider session resumption;
- context compression / long-session evidence;
- resource accounting readback;
- audit-log correlation for the entire run.

Record exact framework, consumer SHA, provider model and timestamps.

## 12. Consumer adoption contract

A consumer must:

1. pin a released `live-interaction` version;
2. link this document from its Live architecture/runbook;
3. declare its modes and capability bundles in product source;
4. keep domain authorization/mutations in its adapter;
5. use the shared resource-control library;
6. implement the observability contract;
7. run shared contract tests plus product-specific acceptance;
8. avoid private forks of transport, capability routing or resource accounting.

Consumer-specific prompt text belongs in the consumer repository. The reusable
rules for layering, capability disclosure, lifecycle, logging and acceptance
belong here.

## 13. Initial Wonderful Lections decomposition

This is an example, not framework-owned product policy.

`core`
- get_current_slide
- get_slide_context
- activate_capability
- wait/yield only when the mode needs it

`slide_edit`
- get_current_slide
- get_design_options
- prepare_slide_change
- apply_prepared_change
- read_slide_after_change
- activate_capability

`dataset`
- get_current_slide
- open_dataset_chooser
- preview_authoring_choice
- apply_authoring_choice
- read_dataset_rows
- activate_capability

`media`
- get_current_slide
- get_design_options
- prepare_slide_change
- apply_prepared_change
- read_slide_after_change
- open_media_chooser
- preview_authoring_choice
- apply_authoring_choice
- activate_capability

`deck` and `verification` should remain separate bundles.

Show mode does not expose the capability router for mutation capabilities. Its
tool surface remains intentionally tiny.

## 14. Rollout sequence

Do not big-bang this architecture into every consumer.

1. Fix shared resource-accounting defects and add budget audit evidence.
2. Add provider-neutral capability state/digests/audit primitives.
3. Add Gemini safe reconfiguration via session resumption and prove it with a
   real provider test.
4. Migrate one Wonderful Lections capability (dataset) end to end.
5. Add media, then slide edit.
6. Run long-session/interrupt/reconnect acceptance.
7. Release a new shared framework version.
8. Update Street Story and other consumers by pinned release, adopting the
   standard without copying Wonderful-specific tools.

Each stage must be independently deployable and reversible.

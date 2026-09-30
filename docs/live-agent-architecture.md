# Live agent architecture and operating standard

Status: canonical shared design for products using `@onedayonemasterpiece/live-interaction`.

Last reviewed: 2026-09-28.

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
- OpenAI Tool Search loads deferred definitions in the **Responses API** and
  recommends clear namespaces with fewer than 10 functions. This is design
  evidence for small bundles, not an API feature of Gemini Live or GPT-Live.
  https://developers.openai.com/api/docs/guides/tools-tool-search
- OpenAI Realtime allows function tools at session or response scope. GPT-Live
  instead keeps the voice prompt short and delegates detailed work to a
  backend; its supported session updates and instruction appends have different
  semantics. Neither API is the transport used by this Gemini implementation.
  https://developers.openai.com/api/docs/guides/realtime-mcp
  https://developers.openai.com/api/docs/guides/live-prompting
  https://developers.openai.com/api/docs/guides/live-delegation
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

### OpenAI APIs

For an actual Realtime consumer, use its documented session/response tool
scoping. For GPT-Live, keep the conversation prompt short and configure or
update tools in its delegated backend within that API's supported fields.
Responses `tool_search` is a separate mechanism; do not assume it can update
an active voice model or reuse it as a Gemini implementation detail.

### Gemini Live

Gemini does not accept configuration changes on an open WebSocket. Capability
switching therefore uses a **safe session-resumption boundary**:

1. The model requests a capability through the small router and supplies a
   bounded `intent` summary for continuation.
2. The application validates the requested capability, permissions and bounded
   configuration, then records a pending transition.
3. Gemini must first complete the router function call on the **current**
   connection. The worker sends a bounded acknowledgement with
   `scheduling=SILENT`; this acknowledgement grants no new authority.
4. Discard any pre-call resumption handle. Wait for a **new resumable handle
   issued after that router response**. Google explicitly marks function-call
   and generation states as non-resumable, so reusing a handle from before the
   router response can lose the pending tool-call state.
5. If the current microphone turn is still open, wait for its audio end to be
   sent to the provider before acknowledging the router call. Only after that
   and a fresh handle exists, pause the provider connection. Input captured
   during the bounded handoff is not replayed as stale speech.
6. Resume the same model and key with:
   - that fresh resumption handle;
   - the same core prompt;
   - the current mode overlay;
   - the new capability overlay;
   - only the router + selected bundle functions.
7. Emit `capability_ready`, then inject one bounded application-owned
   continuation turn derived from the already accepted `intent`. This is
   continuity context, not a new authorization or a replay of a mutation.
8. Continue the pending user intent without requiring the user to repeat it.

The host must not send a second FunctionResponse after `capability_ready`;
the provider worker owns the two-phase router acknowledgement. A duplicate
provider call ID is answered from the bounded host result cache rather than
starting a second transition.

If no fresh post-response resumable handle becomes available within the
bounded transition window, open a new provider connection with the same model
and key, bounded completed dialogue turns and the one accepted intent. The
cold connection uses Gemini `historyConfig.initialHistoryInClientContent`: it
loads text-only history after setup without triggering an old command, then
sends the accepted continuation once. Native checkpoint resumption does not
replay application history. A cold restore cannot recreate provider-internal
audio, tool or reasoning state; product context must carry authoritative
selected IDs, accepted revision and confirmed mutation receipt where relevant.
Gemini does not guarantee input transcription order against other messages, so
late ASR fragments are best-effort additions to the last user turn.
Never reuse the pre-call handle or replay a mutation. Emit explicit recovery
metadata. If a transition misses its deadline or retry limit, close that
provider session and require an explicit restart; a late `capability_ready`
cannot revive an old allowlist. Stop remains immediate locally. The UI
conversation remains continuous only for a successful transition.

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

Context compression and resumption solve context/connection lifetime, not a
rolling token-admission refusal. When the resource authority returns
`RESOURCE_TOKEN_BUDGET`, keep the same lease and key. An optional visual frame
may be dropped without ending the conversation. For a required, already
constructed text or tool response, wait within a bounded deadline and retry
the **same unsent provider message** after the authority's retry interval;
never invoke the product tool or mutation again. Pause browser capture during
this wait, discard speech captured before recovery, and resume capture only
after admission succeeds. Show the wait and keep Stop immediate. Realtime PCM
cannot wait behind a minute-scale quota without losing its timing: if its
grant is denied, stop visibly rather than replaying stale audio. A bounded
wait that expires remains a truthful resource error, not a silent restart.

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
Repeated `usageMetadata` snapshots within a turn are not additive. Keep the
current-turn maximum and accumulate completed turns separately; do not call
one snapshot or the maximum over a session its total bill.
Progressive disclosure reduces the active tool schema, but it is not a quota
guarantee: Gemini Live re-processes the retained multimodal context on later
turns, and a capability change sends a new setup. Measure actual provider
usage and separate setup, audio, image and tool-response admission events.
https://ai.google.dev/gemini-api/docs/live-api/best-practices#pricing-and-billing

## 9. Observability contract

Every production consumer must expose bounded sanitized Live audit logs.
Debugging a screenshot without correlated runtime evidence is not acceptable.

### Required correlation fields

- `session_id`
- pre-session `attempt_id`
- product/resource ID
- consumer
- provider/model
- mode
- browser/server/provider connection generation or epoch where applicable
- active capability
- configuration digest/version
- capability transition ID when applicable

### Required lifecycle events

- session start requested / ready / failed / stopped;
- provider ready / resumed / GoAway / closed;
- WSS hello/version, reconnect/gap, socket buffer and original capture-age watermarks;
- AudioContext state and effective microphone settings, where the browser exposes them;
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
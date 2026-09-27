# Live provider research backlog

Status: research backlog only. Last reviewed: 2026-09-27.

The production framework currently has a measured Gemini transport. The entries
below are **potential alternatives for future experiments**, not configured
fallbacks, compatibility promises, or evidence that another provider is suitable
for a consumer. Model names, pricing, language support, endpoints and quotas must
be re-verified against current provider documentation immediately before each
experiment.

A provider may be promoted only after it has a separate thin adapter, preserves
the framework's product/security boundaries, and passes the same real-browser /
real-microphone acceptance expected from connection or recovery changes. Do not
silently route an existing session to another provider after quota, latency or
transport failure.

## Candidates

| Provider / family | Why it is worth researching | Known boundary / question to verify |
|---|---|---|
| Google Gemini Live | Current measured baseline for this framework. Audio, multimodal context and tool calling are already integrated by the existing transport. | Provider latency, session/recovery behavior and quota remain external runtime conditions. |
| Alibaba Qwen Audio Realtime — current family includes `qwen-audio-3.1-realtime-plus` | Realtime speech interaction, function calling, semantic turn handling; official documentation currently lists Russian support. Strong candidate for voice-first assistants. | Re-verify exact Russian ASR/TTS quality, interruption behavior, session limits, regions, tool semantics and production quotas. |
| Alibaba Qwen Omni Realtime — current family includes `qwen3.8-omni-flash-realtime` | Realtime audio plus image/video context, function calling/MCP; candidate when a consumer needs to discuss a slide, screen or camera context. | Re-verify Russian input/output support separately, video cost/latency, tool behavior and exact transport/API contract. |
| StepFun StepAudio 3 Realtime | Described as full-duplex conversational audio with interruption/backchannel behavior and tool use. | Russian support was not confirmed in the reviewed material; verify language quality, API maturity, regions, limits and billing. |
| Zhipu / Z.ai GLM-Realtime family | Voice/video realtime sessions, interruptions and function calling make it a plausible multimodal alternative. | Verify current Russian support, whether tool calling is available in the required video mode, session semantics and non-China deployment constraints. |
| ByteDance SeedRealtime | Native audiovisual full-duplex research/model direction; interesting for multi-speaker and temporal audiovisual context. | Treat as research until a stable public API, Russian support, pricing and production access appropriate to our deployment are confirmed. |
| OpenAI GPT Realtime family — current docs include GPT-Realtime-2.1 and Mini | Speech-to-speech, tool calling and mature realtime agent APIs; relevant as a cloud provider alternative. | Measure Russian quality, interruption/turn detection, provider-specific session limits, data controls, cost and adapter complexity. |
| OpenAI GPT-Live family | Native full-duplex conversation with delegation to another agent is architecturally interesting when the conversational layer should stay responsive during longer work. | Different architecture from the existing Gemini transport; verify public API contract, audio-only/multimodal boundary, delegation semantics and total cost. |
| xAI Grok Voice | Bidirectional voice, tool calling and official Russian support make it a practical API candidate. | Verify Russian quality in noisy rooms, interruption/background speech behavior, session/token security, regions, quotas and measured latency. |
| Amazon Nova 2 Sonic | Managed speech-to-speech on AWS/Bedrock with tool integration; relevant for AWS-heavy deployments. | Russian is not in the currently reviewed official supported-language set; do not trial for a Russian product unless this changes. |
| Hume EVI 4-mini | Expressive voice layer with Russian support; interesting where prosody/turn-taking matters and a separate reasoning model is acceptable. | It is a voice-interface layer rather than a drop-in single-model Gemini replacement; integration would require an explicit LLM/tool architecture. |
| OpenBMB / ModelBest MiniCPM-o 4.5 | Open-weight multimodal/full-duplex direction; candidate for future self-hosted experiments and dependency reduction. | Current reviewed conversational language support is English/Chinese; Russian, hardware sizing, latency and ops burden require proof before product use. |
| NVIDIA PersonaPlex | Open conversational voice research with published weights/code; useful as a self-hosted research reference. | Not a drop-in cloud Live API. Russian capability, production support, tool architecture, GPU footprint and operational reliability all require separate evaluation. |

## Official starting points

- Alibaba Qwen Omni / Realtime: <https://www.alibabacloud.com/help/en/model-studio/omni/>
- Alibaba Qwen Audio 3.1 Realtime Plus: <https://www.alibabacloud.com/help/en/model-studio/qwen-audio-3-1-realtime-plus>
- StepFun StepAudio 3 Realtime: <https://platform.stepfun.ai/docs/en/guides/models/stepaudio-3-realtime>
- Zhipu / Z.ai GLM-Realtime: <https://docs.bigmodel.cn/cn/guide/models/sound-and-video/glm-realtime>
- ByteDance SeedRealtime: <https://seed.bytedance.com/en/SeedRealtime>
- OpenAI GPT Realtime: <https://developers.openai.com/api/docs/models/gpt-realtime-2.1>
- OpenAI GPT-Live: <https://developers.openai.com/api/docs/models/gpt-live-1>
- xAI Voice: <https://docs.x.ai/docs/guides/voice>
- Amazon Nova 2 Sonic: <https://aws.amazon.com/blogs/aws/introducing-amazon-nova-2-sonic-next-generation-speech-to-speech-model-for-conversational-ai/>
- Hume EVI: <https://dev.hume.ai/docs/speech-to-speech-evi/overview>
- MiniCPM-o 4.5: <https://huggingface.co/openbmb/MiniCPM-o-4_5>
- NVIDIA PersonaPlex: <https://research.nvidia.com/labs/adlr/personaplex/>

## Common experiment matrix

Every provider experiment should use the same scenario set and record provider-
specific behavior instead of judging from a demo call:

1. Russian speech understanding and Russian speech output on the same microphone
   and room/noise profiles used by the target consumer.
2. Cold-start and warm-turn latency distributions, including long-tail latency,
   not only the fastest response.
3. True full-duplex/barge-in behavior, echo/self-trigger resistance, long silence,
   background speech, audience speech and semantic turn detection.
4. Tool calling: schema fidelity, cancellation before execution, duplicate call
   behavior, idempotency/readback boundaries and failure reporting.
5. Audio plus image/video support where relevant, including whether visual updates
   interrupt speech or change turn semantics.
6. Session lifetime, context limits, resumption/reconnect behavior and what state
   is retained after network loss.
7. Browser/server transport (WebRTC/WebSocket), ephemeral credential support and
   whether a server-only credential path can preserve current trust boundaries.
8. Quota/concurrency behavior, exact billing model and measured cost per comparable
   real conversation rather than nominal token-price comparison.
9. Data retention/privacy/region controls and whether they fit the consumer's
   deployment and content sensitivity.
10. SDK/API stability, observability and provider error taxonomy.

A successful experiment still does not make a provider an automatic fallback.
Provider selection remains explicit at session start; a quota or network failure
must fail visibly unless a separately designed and accepted cross-provider policy
is introduced.

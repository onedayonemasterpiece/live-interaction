# Shared Live runtime

This repository is the canonical implementation of browser Live audio,
Gemini session transport and cross-product Live interaction architecture.
Read `docs/live-agent-architecture.md` first, then `docs/integration.md`, before
changing a public interface or integrating a consumer.

Keep domain tools, permissions, persona, imagery and key selection in product
adapters, but enforce the shared prompt-layering, progressive capability disclosure,
conversation-continuity and sanitized observability contracts centrally. Do not send
a monolithic prompt plus every domain tool. Preserve immediate local Stop, bounded
ordered capture, complete output, confirmed voice Stop, and mutation idempotency.
Never mask a provider error with another model/key. Run Node/Python contracts plus
affected consumer browser tests and real provider capability-transition acceptance.
Real Google acceptance receipts are required for transport/lifecycle releases;
record failures and provenance, not just passing fixtures. Use versioned releases for
consumers and update the integration skill when the public contract changes.

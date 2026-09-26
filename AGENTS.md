# Shared Live runtime

This private repository is the canonical implementation of browser Live audio
and Gemini session transport for our products. Read `docs/integration.md` before
changing a public interface or integrating a consumer.

Keep domain tools, permissions, persona, imagery and key selection in product
adapters. Preserve immediate local Stop, bounded ordered capture, complete output,
confirmed voice Stop, and mutation idempotency. Never mask a provider error with
another model/key. Run Node/Python contracts plus affected consumer browser tests.
Real Google acceptance receipts are required for transport/lifecycle releases;
record failures and provenance, not just passing fixtures. Use versioned releases for
consumers and update the integration skill when the public contract changes.

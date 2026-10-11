# Live interaction 0.3.28

Promotes the unchanged functional code of candidate `0.3.28-rc.1`, commit `f1ef688de8d2ce3813ce50716727e443771c2b45`.

## Fixes

Browser send/ACK gaps renew the same session ticket before provider Stop, discard stale PCM, and preserve immediate explicit Stop. Replaced socket callbacks cannot damage new ACKs, heartbeat or events. Python readiness and browser HTTP errors preserve typed resource codes and bounded `retry_after_ms`. Quotas, provider/key binding, damaged-turn guards, durable persistence failure semantics and native behavior are unchanged.

## Evidence

- Complete local suites: Node 97 passed; Python 63 passed.
- Exact candidate GitHub contracts and native JVM jobs passed: https://github.com/onedayonemasterpiece/live-interaction/actions/runs/38137612771
- First six new browser regressions fail base `557532aa6277b5fc06424eb171d99e96f1445387` and pass candidate.
- Real ProjectsHub provider canary passed on deployed product commit `458259295e7530a7b3526922ac45f3e016c2e01d`, installed candidate source above. Evidence job `job_3ea4352630604f0d8154a941`: exit 0, 10.4 seconds. Verified same Live session through initial completed turn, controlled socket close, ticket rotation, generation-2 hello, fresh PCM ACK, fresh provider transcript/audio/completion, Stop and server closure.
- Canary used prepared PCM, not a physical microphone. This verifies bounded reconnect; it does not claim new long-session or physical-device acceptance.

Other products remain on existing immutable pins. ProjectsHub owns its backend/Android rollout and rollback. This framework release does not deploy other consumers.

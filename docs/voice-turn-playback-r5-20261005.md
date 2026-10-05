# 0.3.23 — explicit utterance boundary and playback lifecycle

Scope: Projects Hub voice continuation, 2026-10-05. No new provider, model, ASR, semantic filtering, credential path or copied product VAD.

## Reproduced defects

On 0.3.22, executable WSS browser fixtures reproduced playback created after `interrupted` while `AudioContext.resume()` was pending, and UI remaining `answering` after the final reply buffer ended. Manual sender `finish()` also failed to pump `activity_end` when PCM had already drained. User Stop already invalidated the session epoch correctly and stays immediate. These are code-level reproductions, not proof of the exact cause of a physical-device incident.

## Changes

- `createLiveClient().finishTurn(): boolean` seals an admitted active utterance through the same ordered sender, without stopping the mic or session. True means queued locally, NOT provider/durable acknowledgement. Idle, unconfirmed onset, double click, stopped session and unsupported continuous-capture mode return false and create no empty semantic turns.
- `createLiveAudioSender().endTurn()` is the narrow synchronous primitive. Existing `finish()` retains offline/durable-finalization semantics and now schedules a manual end even after PCM drains.
- Separate playback generation invalidates audio awaiting resume on interruption, suppression, Stop or new session. Cancellation does not wait for AudioContext.
- UI returns to listening only after provider turn completion AND all scheduled/pending playback is drained, with a running mic and no pending user reply/resource pause.
- AudioContext resume is bounded at 2500 ms. Playback errors are exposed as `onNotice('playback_error', error)` instead of silent failure/unhandled WSS rejection; transcripts remain available. No automatic retry/replay of speech or tools.

## Verification and limits

New fixtures first failed on baseline for cancellation, listening status and idle-queue finalization, then passed after changes. Full Node suite: 86/86. Independent OpenCode read-only review dvt_55b6200462cb4af282e8ddb5e7524e3a confirmed cancellation and missing listening transition; its proposed `playing.size`-only guard is intentionally NOT used because it would discard the first legitimate chunk.

Consumer must check full tests, build, real same-provider WSS and user-device acoustic acceptance. No claim of solving conversational Gemini interim input transcription. Shortening silence tails is not substituted for genuine interim text; current 2 s tail and adaptive echo / 180 ms speech admission remain unchanged.

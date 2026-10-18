# Android platform runtime candidate

This Android library extends the existing pure Java contracts in `android/`. It owns actual microphone capture, native VAD, compressed encoding, the authenticated v2 WSS lifecycle, original-source recovery, native decoding and AudioTrack playback. The product owns the foreground service, permissions, scoped durable journals, authentication and UI. A WebView observes state and sends controls; it never owns this audio path.

The module uses the existing Android baseline: **minSdk 26, compileSdk 35, AGP 8.7.3 and Java 17**. `coreLibraryDesugaring` is enabled for Java collection APIs on older supported Android versions. Candidate integration uses the existing composite build; no published dependency version or release claim is introduced here.

## Product API

The product constructs `NativeLiveRuntime(Context, DurableSourceStore, SessionApi, Listener)`. The supplied store must also implement `DurableOutputStore`. A five-argument constructor permits separate input and output store adapters. Both adapters must already be scoped to the authenticated actor, workspace and conversation; the runtime never changes that scope or closes the injected stores.

`SessionApi.connect(StartContext, previousSessionId, eventCursor, connectionGeneration, recoveryOnly)` returns the authenticated connection: base origin, product-supplied socket path, session/attempt identity, one-use ticket, connection generation, recovery mode and opaque auth headers. `SessionApi.stop(sessionId, reason)` performs the product's bounded server cleanup. The product supplies cookie/authentication material directly to this adapter, never through a JavaScript bridge. The core transport rechecks exact origin, disables redirects, and permits only Cookie/Authorization as extra request headers.

The runtime exposes:

- `capabilities(Context)`: a cached native codec preflight, with no microphone capture. Run this off the UI thread. The result contains `supported`, input/output profiles and an explicit error code.
- `start(new StartContext(conversationId))`: idempotent while the same run is active. A scoped runtime cannot be reused for another conversation.
- `finishTurn()`: closes the current VAD portion at a subsequent encoded durable boundary. Capture and original spooling continue. It does not interpret phrases or route product commands.
- `stop(reason)`: synchronously stops/releases microphone and playback hardware before socket, storage or backlog work. Its future completes after the local input EOS/fsync/seal and already accepted output writes/played-position barrier. Server cleanup is separate and cannot delay a new local Start.
- `snapshot()`: state and authoritative capture/playback/connectivity/recovery flags, conversation/session/source identities, codec profile and device/server durable prefixes.
- `close()`: final Stop and release of SDK executors/network resources. Product cleanup waits for the Stop future before closing its journals.

The state vocabulary is `idle`, `starting`, `live`, `delayed`, `recording_only`, `stopping`, `error`, `unsupported`. The independent `capturing` and `playing` flags describe hardware state. A connected run becomes `delayed` while a new live source is being established, the encoder is over 500 ms behind capture, or an unacknowledged fresh prefix has waited over 500 ms. Disconnection enters `recording_only`; original capture continues locally.

Listener callbacks carry snapshots, typed error codes, semantic event JSON, durable prefix receipts and actual played positions. Native timing events have `type: native_timing` and stages `speech_start`, `speech_end`, `first_output_audio`, `audio_scheduled`. Encoded audio and PCM are excluded from UI callbacks. Queued callbacks are fenced after Stop/new Start and, for socket semantic events, after attachment replacement.

## Input original and codec clock

AudioRecord reads mono PCM in 20 ms buffers. The in-memory encoder queue is bounded to approximately two seconds. If the product journal cannot keep up, hardware stops with an explicit error; an already captured final buffer is retained for bounded tail finalization. The runtime never acknowledges a volatile buffer as durable.

The whole captured original is compressed, including silence. Native energy VAD marks `PORTION_START`/`PORTION_END`; it does not remove silent samples or break codec history. The server derives provider activity controls from those boundaries. There is no second stream of duplicate client activity commands. `finishTurn()` shares the same boundary path and waits for a quiet interval before opening a subsequent portion.

Opus uses the actual Android encoder at 48 kHz and 24 kbit/s. The original Opus identification header and pre-skip are retained, and complete Ogg pages carry ordered packets and valid granules. The protocol's decoded PCM projection can be 16 kHz while the Opus granule/pre-skip clock remains **48 kHz**. Packet duration is read from Opus framing; codec buffer timestamps are checked for order rather than assumed to equal captured PCM duration. Valid original duration excludes pre-skip and final padding exactly once.

AAC compatibility uses mono AAC-LC at 16 kHz and 32 kbit/s with ADTS framing. It is offered only when the actual encoder exports trustworthy priming metadata and passes the synthetic native codec probe. Missing encoder delay is **not** treated as zero. A device with neither a verified Opus encoder nor this verified AAC path reports compressed capture as unsupported. The runtime does not switch to a browser microphone or raise minSdk to hide that limitation.

Each local source has a stable source ID, original capture origin, monotonically increasing source sequence and immutable codec epoch. Every encoded chunk is journaled and fsynced before a device receipt or WSS submission. Recovery uses the same original bytes and sequence. Audio captured before a successful attachment, old sources, and a fresh source that falls over two seconds behind are recovered through the non-command lane; they are not silently replayed as new provider input.

The server's durable prefix and its final source seal receipt are persisted independently. A source with all audio acknowledged but no server seal receipt still resends the seal after restart. An authenticated source binding also repairs a lost device-side acknowledgement after server fsync. Completed source references are explicitly unbound from the socket without deleting the durable original or deciding its product disposition.

## Output original and playback

The v2 output binding identifies an immutable output source and codec epoch. Incoming original compressed chunks are written to `DurableOutputStore` before device acknowledgement. Source-global sequence and original event sequence remain distinct from temporary socket references and delivery generation.

`DurablePlaybackPump` reads bounded batches from that same journal using a separate decoder-submission cursor. A full player queue leaves the cursor on the unsubmitted chunk and retries after capacity returns. A fast replay of a long response therefore uses durable storage instead of growing a volatile queue or stopping microphone capture. The player accepts canonical original chunks, not reconstructed transport frames.

MediaCodec decodes compressed packets natively; AudioTrack supplies actual playback head position. The device stores actual played microseconds independently of the received event cursor. Reconnect does not rewind text/tool events. The existing native decoder/player can continue through the available synced prefix while WSS reconnects, and immutable replay duplicates do not re-enter the decoder. After process restart, the authorized source is replayed from its original codec prefix and already played samples are discarded locally.

Only one pending output replay is requested at a time. The next source waits for actual playback completion. A source interrupted by a new speech portion is suppressed for the active run; late encoded callbacks cannot restart it. A crashed, incomplete source drains only its available prefix and receives an explicit incomplete notice. Decoder finalization of that prefix does not invent an original EPOCH_END, a server receipt or a completed source. Further bounded output manifests are requested over the existing socket after played-position persistence.

## Validation and remaining gates

On the local Android toolchain (JDK 17, Gradle 8.10.2, SDK/build tools 35), the final `:platform:testDebugUnitTest` run passed **20 tests**: eleven codec/framing/timeline regressions, five output recovery-selection regressions and four durable playback/backpressure regressions. Android javac compiled the actual platform API. The consumer agent separately runs the complete composite consumer unit-test and APK assemble gates; the consumer's build record identifies the final APK and source checkpoint.

Reproduction from `framework/android` with the installed toolchain:

```sh
gradle --no-daemon :platform:testDebugUnitTest --console=plain
```

These checks do not demonstrate real AudioRecord/MediaCodec/AudioTrack behavior on a handset. Physical acceptance remains required for codec capability, AAC priming where offered, first/last sample behavior, acoustic echo/noise, routing/headsets, barge-in, immediate Stop, permission cancellation, WebView recreation, foreground/background transitions, network loss and process recovery. Signed APK/release delivery is also separate. No provider calls or production changes were made by the platform tests.

The replay implementation restores decoder state from the original prefix and discards already played decoded samples. An optimized Opus seek/preroll path is not claimed; long-prefix cost and end-to-end mobile timing remain measurement gates. The native VAD is an explicit energy/hysteresis implementation and still needs acoustic tuning on actual devices.

## Codec references

- [Android MediaCodec codec-specific data](https://developer.android.com/reference/android/media/MediaCodec): Opus identification data, native-order pre-skip and seek pre-roll fields; separate decoder delay overrides identification-header delay.
- [AOSP C2SoftOpusEnc](https://android.googlesource.com/platform/frameworks/av/+/1401a72389/media/codec2/components/opus/C2SoftOpusEnc.cpp): actual encoder lookahead and unified Opus header metadata.
- [AOSP C2SoftOpusDec](https://android.googlesource.com/platform/frameworks/av/+/1401a72389/media/codec2/components/opus/C2SoftOpusDec.cpp): native 48 kHz decoding and decoder-owned pre-skip removal.
- [AOSP C2SoftAacEnc](https://android.googlesource.com/platform/frameworks/av/+/master/media/codec2/components/aac/C2SoftAacEnc.cpp): encoder configuration/output handling; this implementation does not establish a universal public AAC priming guarantee.

The SDK retains the original header and rejects conflicting separate delay metadata. It never fabricates an original header merely to make one decoder accept a stream.

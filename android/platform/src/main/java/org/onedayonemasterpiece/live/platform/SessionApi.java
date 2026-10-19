package org.onedayonemasterpiece.live.platform;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;

/** Product-owned, authenticated same-origin session/ticket API. No provider credentials. */
public interface SessionApi {
    CompletableFuture<Connection> connect(StartContext context, String previousSessionId,
            long eventCursor, int connectionGeneration, boolean recoveryOnly);
    CompletableFuture<Void> stop(String sessionId, String reason);

    /** Typed product API failure. Only a safe machine code crosses this boundary. */
    final class ConnectFailure extends IOException {
        private final String code;
        private final boolean recoverable;

        public ConnectFailure(String code, boolean recoverable) {
            super(code != null && code.matches("(?:RESOURCE|LIVE|NATIVE|HTTP)_[A-Z0-9_]+")
                ? code : "LIVE_NATIVE_SESSION_UNAVAILABLE");
            this.code = getMessage();
            this.recoverable = recoverable;
        }
        public String code() { return code; }
        public boolean recoverable() { return recoverable; }
    }

    /** Opaque credentials intentionally have no generated record/toString representation. */
    final class Connection {
        private final String baseUrl, socketPath, sessionId, attemptId, ticket;
        private final int connectionGeneration;
        private final boolean recoveryOnly;
        private final Map<String, String> headers;
        public Connection(String baseUrl, String socketPath, String sessionId, String attemptId, String ticket,
                int connectionGeneration, boolean recoveryOnly, Map<String, String> headers) {
            if (baseUrl == null || socketPath == null || sessionId == null || attemptId == null || ticket == null
                    || connectionGeneration < 1) throw new IllegalArgumentException("LIVE_SESSION_CONFIGURATION");
            this.baseUrl = baseUrl; this.socketPath = socketPath; this.sessionId = sessionId; this.attemptId = attemptId;
            this.ticket = ticket; this.connectionGeneration = connectionGeneration; this.recoveryOnly = recoveryOnly;
            this.headers = Collections.unmodifiableMap(new LinkedHashMap<>(headers == null ? Map.of() : headers));
        }
        public Connection(String baseUrl, String socketPath, String sessionId, String attemptId, String ticket,
                int connectionGeneration, boolean recoveryOnly) {
            this(baseUrl, socketPath, sessionId, attemptId, ticket, connectionGeneration, recoveryOnly, Map.of());
        }
        public String baseUrl() { return baseUrl; }
        public String socketPath() { return socketPath; }
        public String sessionId() { return sessionId; }
        public String attemptId() { return attemptId; }
        public String ticket() { return ticket; }
        public int connectionGeneration() { return connectionGeneration; }
        public boolean recoveryOnly() { return recoveryOnly; }
        public Map<String, String> headers() { return headers; }
    }
}

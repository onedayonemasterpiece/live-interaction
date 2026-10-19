package org.onedayonemasterpiece.live.platform;

import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import org.junit.Test;
import static org.junit.Assert.*;

public class SessionFailureTest {
    @Test public void terminalBudgetIsVisibleThroughFutureFailureWithoutSensitiveMessage() {
        SessionApi.ConnectFailure budget = new SessionApi.ConnectFailure("RESOURCE_TOKEN_BUDGET", false);
        SessionApi.ConnectFailure received =
            NativeLiveRuntime.connectionFailure(new CompletionException(budget));
        assertSame(budget, received);
        assertEquals("RESOURCE_TOKEN_BUDGET", received.code());
        assertFalse(received.recoverable());
        assertEquals("RESOURCE_TOKEN_BUDGET", received.getMessage());
    }

    @Test public void recoverableNetworkFailureStaysRetryableAndUnrelatedFailuresStayGeneric() {
        SessionApi.ConnectFailure offline = new SessionApi.ConnectFailure("LIVE_SOCKET_IO", true);
        assertSame(offline, NativeLiveRuntime.connectionFailure(new CompletionException(offline)));
        assertTrue(offline.recoverable());
        assertNull(NativeLiveRuntime.connectionFailure(new RuntimeException("unexpected")));
    }

    @Test public void rejectedCodesNeverLeakProviderPayloadOrCredentials() {
        SessionApi.ConnectFailure invalid = new SessionApi.ConnectFailure("cookie bearer secret", false);
        assertEquals("LIVE_NATIVE_SESSION_UNAVAILABLE", invalid.code());
        assertFalse(invalid.recoverable());
        assertEquals("LIVE_NATIVE_SESSION_UNAVAILABLE", invalid.getMessage());
    }
}

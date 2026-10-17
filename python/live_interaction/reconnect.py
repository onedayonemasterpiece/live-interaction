"""Rate-bounded provider reconnect policy shared by Live and caption transports.

A long conversation renews its provider connection many times (the provider
closes connections on its own schedule). A lifetime attempt count would end every
long session; an unbounded loop would storm the provider. The budget therefore
limits attempts per rolling window and backs off only on consecutive failures.
"""
from __future__ import annotations

import random
import time
from collections import deque


class ReconnectBudget:
    def __init__(self, *, max_attempts: int = 6, window_seconds: float = 300.0,
                 base_delay: float = 0.5, max_delay: float = 8.0,
                 clock=time.monotonic, rng=random.random):
        if max_attempts < 1 or window_seconds <= 0:
            raise ValueError("invalid reconnect budget")
        self.max_attempts = max_attempts
        self.window_seconds = window_seconds
        self.base_delay = base_delay
        self.max_delay = max_delay
        self._clock = clock
        self._rng = rng
        self._attempts: deque[float] = deque()
        self.consecutive_failures = 0

    def _trim(self, now: float) -> None:
        while self._attempts and now - self._attempts[0] >= self.window_seconds:
            self._attempts.popleft()

    def allow(self) -> bool:
        """Record an attempt if the rolling window still has room."""
        now = self._clock()
        self._trim(now)
        if len(self._attempts) >= self.max_attempts:
            return False
        self._attempts.append(now)
        return True

    def delay(self) -> float:
        """Backoff before the next attempt: immediate after a clean renewal."""
        if self.consecutive_failures <= 0:
            return 0.0
        raw = min(self.max_delay, self.base_delay * 2 ** (self.consecutive_failures - 1))
        # Full jitter in [0.5, 1.5) avoids synchronized retries across sessions.
        return raw * (0.5 + self._rng())

    def failed(self) -> None:
        self.consecutive_failures += 1

    def succeeded(self) -> None:
        self.consecutive_failures = 0

    def recent_attempts(self) -> int:
        self._trim(self._clock())
        return len(self._attempts)

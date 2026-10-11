import asyncio
import unittest

from live_interaction.session_host import LiveError, LiveSessionHost


class Adapter:
    def initialize(self, **_kwargs):
        return {"state": {}, "context": {}, "configuration": {"functions": []}, "response": {}}


class ResourceStartupErrorContract(unittest.IsolatedAsyncioTestCase):
    async def run_failure(self, runner, code, retry=None):
        host = LiveSessionHost(adapter_factory=lambda **_: Adapter(), managed_runner=runner, ready_timeout_ms=100)
        try:
            with self.assertRaises(LiveError) as raised:
                await host.start(resource_id="conversation-1", actor={"subject": "a", "tenant_id": "t"})
            self.assertEqual(raised.exception.code, code)
            self.assertEqual(getattr(raised.exception, "retry_after_ms", None), retry)
            self.assertEqual(len(host.sessions), 0, "failed starts must release local capacity")
        finally:
            await host.stop_all()

    async def test_resource_event_preserves_code_and_retry(self):
        async def runner(**kwargs):
            kwargs["on_event"]({"type": "error", "code": "RESOURCE_TOKEN_BUDGET", "message": "budget", "retry_after_ms": 60000})
        await self.run_failure(runner, "RESOURCE_TOKEN_BUDGET", 60000)

    async def test_resource_exception_preserves_code_and_retry(self):
        class ResourceFailure(RuntimeError):
            code = "RESOURCE_TOKEN_BUDGET"
            retry_after_ms = 3000
        async def runner(**_kwargs):
            raise ResourceFailure("budget")
        await self.run_failure(runner, "RESOURCE_TOKEN_BUDGET", 3000)

    async def test_untyped_provider_error_stays_provider_error(self):
        async def runner(**kwargs):
            kwargs["on_event"]({"type": "error", "message": "provider failed"})
        await self.run_failure(runner, "LIVE_PROVIDER_ERROR")

    async def test_untyped_exception_stays_provider_error(self):
        async def runner(**_kwargs):
            raise RuntimeError("provider failed")
        await self.run_failure(runner, "LIVE_PROVIDER_ERROR")

    def test_retry_metadata_is_finite_bounded_numeric(self):
        for bad in (True, "3000", -1, float("nan"), float("inf")):
            self.assertFalse(hasattr(LiveError("RESOURCE_TEST", "test", retry_after_ms=bad), "retry_after_ms"))
        self.assertEqual(LiveError("RESOURCE_TEST", "test", retry_after_ms=1e12).retry_after_ms, 86400000)

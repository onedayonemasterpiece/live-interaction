import asyncio
import json
import time
import unittest
from unittest.mock import patch

from live_interaction import provider

class Reader:
    def __init__(self):self.queue=asyncio.Queue()
    def feed(self,message):self.queue.put_nowait((json.dumps(message)+"\n").encode())
    async def readline(self):return await self.queue.get()

class Socket:
    CLOSED=object()
    def __init__(self):
        self.incoming=asyncio.Queue();self.incoming.put_nowait({"setupComplete":{}})
        self.sent=[];self.closed=False;self.two_texts=asyncio.Event()
    async def __aenter__(self):return self
    async def __aexit__(self,*args):return False
    async def send(self,payload):
        self.sent.append(json.loads(payload))
        if sum("clientContent" in item for item in self.sent)>=2:self.two_texts.set()
        if "toolResponse" in self.sent[-1]:
            self.incoming.put_nowait({"sessionResumptionUpdate":{"resumable":True,"newHandle":"fixture-fresh"}})
    async def recv(self):
        item=await self.incoming.get()
        if item is self.CLOSED:raise ConnectionError("fixture closed")
        return json.dumps(item)
    async def close(self):
        if not self.closed:self.closed=True;self.incoming.put_nowait(self.CLOSED)

class BudgetFailure(RuntimeError):
    resource_failure=True
    code="RESOURCE_TOKEN_BUDGET"
    retry_after_ms=50

class Guard:
    def __init__(self):self.calls=0;self.retry_entered=asyncio.Event();self.admit=asyncio.Event()
    def check(self):pass
    def key(self):return "fixture-key"
    async def before_send(self,payload):
        if "toolResponse" in payload:
            self.calls+=1
            if self.calls==1:raise BudgetFailure("fixture wait")
            self.retry_entered.set();await self.admit.wait()

class ResourceWaitTextFifo(unittest.IsolatedAsyncioTestCase):
    async def test_blocked_tool_response_preserves_queued_text_once_fifo_and_drops_stale_capture(self):
        reader=Reader();reader.feed({"type":"start","model":"gemini-3.8-live","configuration":{"functions":[{"name":"read_topic"}]}})
        socket=Socket();guard=Guard();events=[]
        with patch("websockets.connect",return_value=socket):
            task=asyncio.create_task(provider.run(reader=reader,on_event=events.append,resource_guard=guard))
            try:
                for _ in range(1000):
                    if any(e.get("type")=="ready" for e in events):break
                    await asyncio.sleep(.001)
                reader.feed({"type":"tool_response","responses":[{"id":"saved-once","name":"read_topic","response":{"saved":True}}]})
                await asyncio.wait_for(guard.retry_entered.wait(),1)
                queued_at=round(time.time()*1000)-10
                reader.feed({"type":"text","text":"first accepted clarification","queued_at":queued_at})
                reader.feed({"type":"audio","data":"STALE_PCM","queued_at":queued_at})
                reader.feed({"type":"snapshot","data":"STALE_JPEG","queued_at":queued_at})
                reader.feed({"type":"activity_start","queued_at":queued_at})
                reader.feed({"type":"activity_end","queued_at":queued_at})
                reader.feed({"type":"audio_stream_end","queued_at":queued_at})
                reader.feed({"type":"text","text":"second accepted clarification","queued_at":queued_at})
                guard.admit.set()
                for _ in range(300):
                    if len([e for e in events if e.get("type")=="input_dropped"])>=5:break
                    await asyncio.sleep(.001)
                text=[s["clientContent"]["turns"][0]["parts"][0]["text"] for s in socket.sent if "clientContent" in s]
                self.assertEqual(text,["first accepted clarification","second accepted clarification"],"accepted text queued before budget-ready must remain FIFO exactly once")
                self.assertEqual(sum("toolResponse" in s for s in socket.sent),1,"unsent admission retry must not repeat tool response")
                self.assertEqual(guard.calls,2)
                self.assertFalse(any("realtimeInput" in s for s in socket.sent))
                drops=[e for e in events if e.get("type")=="input_dropped"]
                self.assertEqual({e["input_type"] for e in drops},{"audio","snapshot","activity_start","activity_end","audio_stream_end"})
                self.assertTrue(all(e["reason"]=="resource_budget" for e in drops))
                self.assertTrue(any(e.get("type")=="resource_budget_ready" for e in events))
                self.assertFalse(any(str(e.get("type","")).startswith("capability_transition") or e.get("type")=="capability_ready" for e in events))
            finally:
                guard.admit.set();reader.feed({"type":"stop"});await asyncio.wait_for(task,1)
    async def test_real_capability_transition_still_fences_old_stage_text(self):
        reader=Reader();reader.feed({"type":"start","model":"gemini-3.8-live","configuration":{"functions":[{"name":"activate_capability"}]}})
        first=Socket();second=Socket();sockets=[first,second];events=[]
        with patch("websockets.connect",side_effect=lambda *args,**kwargs:sockets.pop(0)):
            task=asyncio.create_task(provider.run(load_key=lambda:"fixture-key",reader=reader,on_event=events.append))
            try:
                for _ in range(1000):
                    if any(e.get("type")=="ready" for e in events):break
                    await asyncio.sleep(.001)
                reader.feed({"type":"reconfigure","transition_id":"transition-fixture","capability":"selection","configuration":{"functions":[{"name":"select_facts"}]},"router_response":{"name":"activate_capability","id":"router","response":{}},"continuation":"finish accepted selection"})
                for _ in range(1000):
                    if any(e.get("type")=="capability_ready" for e in events):break
                    await asyncio.sleep(.001)
                self.assertTrue(any(e.get("type")=="capability_ready" for e in events))
                reader.feed({"type":"text","text":"stale prior stage intent","queued_at":1})
                for _ in range(1000):
                    if any(e.get("type")=="input_dropped" for e in events):break
                    await asyncio.sleep(.001)
                self.assertTrue(any(e.get("type")=="input_dropped" and e.get("reason")=="capability_transition" and e.get("input_type")=="text" for e in events))
                self.assertFalse(any("stale prior stage intent" in json.dumps(s) for s in second.sent))
            finally:reader.feed({"type":"stop"});await asyncio.wait_for(task,1)

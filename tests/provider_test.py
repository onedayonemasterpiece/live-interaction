import asyncio
import json
import unittest
from unittest.mock import patch
from live_interaction.provider import (
    _guarded_recv,
    _guarded_send,
    _is_resource_failure,
    handle_server_message,
    run,
    setup_config,
)

class _Guard:
    def __init__(self):
        self.checks = 0
        self.payloads = []
    def check(self):
        self.checks += 1
    def key(self):
        return 'fixture-key'
    async def before_send(self, payload):
        self.payloads.append(payload)

class _Ws:
    def __init__(self, incoming='{"ok":true}'):
        self.sent = []
        self.incoming = incoming
    async def send(self, payload):
        self.sent.append(payload)
    async def recv(self):
        return self.incoming

class _ResourceFailure(RuntimeError):
    resource_failure = True

class ProviderContract(unittest.TestCase):
    def test_configuration_is_per_application_not_global(self):
        a = setup_config('gemini-3.8-live', {'id': 'one'}, configuration={'system_instruction': 'cohost', 'functions': [{'name':'read_story'}], 'voice':'Aoede'})['setup']
        b = setup_config('gemini-3.8-live', {'id': 'two'}, configuration={'system_instruction':'ideas'})['setup']
        self.assertEqual(a['tools'][0]['functionDeclarations'][0]['name'], 'read_story')
        self.assertEqual(b['tools'], [])
        self.assertNotIn('read_story', str(b))
        self.assertNotIn('thinkingConfig', a['generationConfig'])

    def test_application_search_function_remains_available_without_native_search(self):
        setup = setup_config(
            'gemini-3.8-live',
            {},
            configuration={
                'system_instruction': 'street-story',
                'functions': [
                    {'name': 'read_topic'},
                    {'name': 'search_web'},
                ],
                'application_search_function': 'search_web',
            },
            search=False,
        )['setup']
        system = setup['systemInstruction']['parts'][0]['text']
        self.assertIn('search_web', system)
        self.assertIn('Provider-native Google Search', system)
        self.assertNotIn('Интернет-поиск сейчас недоступен', system)
        self.assertEqual(
            [item['name'] for item in setup['tools'][0]['functionDeclarations']],
            ['read_topic', 'search_web'],
        )

    def test_native_search_hides_application_search_function(self):
        setup = setup_config(
            'gemini-3.8-live',
            {},
            configuration={
                'functions': [{'name':'read_topic'},{'name':'search_web'}],
                'application_search_function':'search_web',
            },
            search=True,
        )['setup']
        self.assertEqual(setup['tools'][0]['functionDeclarations'], [{'name':'read_topic'}])
        self.assertEqual(setup['tools'][1], {'googleSearch': {}})

    def test_unknown_application_search_function_does_not_claim_search(self):
        setup = setup_config(
            'gemini-3.8-live',
            {},
            configuration={
                'functions': [{'name': 'read_topic'}],
                'application_search_function': 'search_web',
            },
            search=False,
        )['setup']
        system = setup['systemInstruction']['parts'][0]['text']
        self.assertIn('Интернет-поиск сейчас недоступен', system)
        self.assertNotIn('приложение предоставляет функцию search_web', system)

    def test_extended_nonblocking_and_resumption(self):
        s = setup_config('gemini-3.8-live-extended-thinking', {}, configuration={'functions':[{'name':'read'}]}, handle='test-handle')['setup']
        self.assertEqual(s['tools'][0]['functionDeclarations'][0]['behavior'], 'NON_BLOCKING')
        self.assertEqual(s['sessionResumption']['handle'], 'test-handle')
        self.assertIn('slidingWindow', s['contextWindowCompression'])

    def test_audio_tail_is_emitted_before_completion(self):
        events=[]
        handle_server_message({'serverContent':{'modelTurn':{'parts':[{'inlineData':{'mimeType':'audio/pcm;rate=24000','data':'AAAA'}}]},'turnComplete':True}},emit=events.append)
        self.assertEqual([e['type'] for e in events], ['audio','turn_complete'])

    def test_cancellation_and_idle_are_preserved(self):
        events=[]
        handle_server_message({'toolCallCancellation':{'ids':['write1']},'interactionStatus':'IDLE'},emit=events.append)
        self.assertEqual(events[0]['status'],'IDLE')
        self.assertEqual(events[1]['ids'],['write1'])

    def test_resource_guard_is_public_transport_parameter(self):
        import inspect
        self.assertIn('resource_guard', inspect.signature(run).parameters)

    def test_resource_failure_marker_is_preserved(self):
        self.assertTrue(_is_resource_failure(_ResourceFailure('expired')))
        self.assertFalse(_is_resource_failure(RuntimeError('provider')))


class _QueueReader:
    def __init__(self):
        self.queue=asyncio.Queue()
    def feed(self,payload):
        self.queue.put_nowait((json.dumps(payload,separators=(',',':'))+'\n').encode())
    async def readline(self):
        return await self.queue.get()

class _FakeSocket:
    _CLOSED=object()
    def __init__(self,incoming):
        self.incoming=asyncio.Queue()
        for item in incoming:self.incoming.put_nowait(item)
        self.sent=[]
        self.closed=False
    async def __aenter__(self):
        return self
    async def __aexit__(self,*_args):
        return False
    async def send(self,payload):
        self.sent.append(json.loads(payload))
    async def recv(self):
        item=await self.incoming.get()
        if item is self._CLOSED:
            raise ConnectionError('closed')
        return json.dumps(item)
    async def close(self):
        if self.closed:return
        self.closed=True
        self.incoming.put_nowait(self._CLOSED)

class ProviderReconfigureContract(unittest.IsolatedAsyncioTestCase):
    async def test_reconfigure_completes_router_call_before_using_fresh_resumption_handle(self):
        reader=_QueueReader()
        reader.feed({
            'type':'start',
            'model':'gemini-3.8-live',
            'context':{'slide':'one'},
            'configuration':{
                'system_instruction':'core',
                'functions':[{'name':'activate_capability'},{'name':'read'}],
            },
        })
        first=_FakeSocket([
            {'setupComplete':{}},
            {'sessionResumptionUpdate':{'resumable':True,'newHandle':'old-handle'}},
        ])
        second=_FakeSocket([{'setupComplete':{}}])
        sockets=[first,second]
        connect_calls=[]
        def fake_connect(*args,**kwargs):
            connect_calls.append((args,kwargs))
            if not sockets:
                raise AssertionError('unexpected third provider connection')
            return sockets.pop(0)
        events=[]
        task=asyncio.create_task(run(load_key=lambda:'fixture-key',reader=reader,on_event=events.append))
        with patch('websockets.connect',side_effect=fake_connect):
            for _ in range(100):
                if any(e.get('type')=='resumption_state' and e.get('resumable') for e in events):
                    break
                await asyncio.sleep(0.001)
            self.assertTrue(any(e.get('type')=='ready' for e in events))
            reader.feed({
                'type':'reconfigure',
                'transition_id':'tr-1',
                'capability':'dataset',
                'configuration':{
                    'system_instruction':'dataset',
                    'functions':[{'name':'activate_capability'},{'name':'dataset.find'}],
                },
                'context':{'slide':'one','scope':'dataset'},
                'continuation':'find national projects dataset',
                'router_response':{
                    'name':'activate_capability',
                    'id':'cap-1',
                    'response':{'result':{'capability':'dataset','accepted':True},'scheduling':'SILENT'},
                },
            })
            for _ in range(100):
                if any('toolResponse' in item for item in first.sent):
                    break
                await asyncio.sleep(0.001)
            self.assertTrue(any('toolResponse' in item for item in first.sent))
            self.assertEqual(len(connect_calls),1)
            first.incoming.put_nowait({'sessionResumptionUpdate':{'resumable':True,'newHandle':'fresh-handle'}})
            for _ in range(200):
                if any(e.get('type')=='capability_ready' and e.get('transition_id')=='tr-1' for e in events):
                    break
                await asyncio.sleep(0.001)
            self.assertEqual(len(connect_calls),2)
            self.assertEqual(first.sent[0]['setup']['sessionResumption'],{})
            self.assertEqual(second.sent[0]['setup']['sessionResumption'],{'handle':'fresh-handle'})
            names=[x['name'] for x in second.sent[0]['setup']['tools'][0]['functionDeclarations']]
            self.assertEqual(names,['activate_capability','dataset.find'])
            continuation=[item for item in second.sent if 'clientContent' in item]
            self.assertEqual(len(continuation),1)
            self.assertIn('find national projects dataset',continuation[0]['clientContent']['turns'][0]['parts'][0]['text'])
            self.assertFalse(any(e.get('type')=='reconnecting' for e in events))
            self.assertTrue(any(e.get('type')=='capability_transition_acknowledged' for e in events))
            self.assertTrue(any(e.get('type')=='capability_transition_started' for e in events))
            self.assertTrue(any(e.get('type')=='capability_ready' for e in events))
            self.assertTrue(any(e.get('type')=='capability_continuation_sent' for e in events))
            reader.feed({'type':'stop'})
            await asyncio.wait_for(task,1)

class ProviderResourceGuardContract(unittest.IsolatedAsyncioTestCase):
    async def test_guarded_send_charges_before_provider_write(self):
        guard = _Guard()
        ws = _Ws()
        payload = {'clientContent': {'turns': []}}
        await _guarded_send(ws, payload, guard)
        self.assertEqual(guard.payloads, [payload])
        self.assertEqual(json.loads(ws.sent[0]), payload)
        self.assertGreaterEqual(guard.checks, 3)

    async def test_guarded_receive_checks_before_and_after_wait(self):
        guard = _Guard()
        ws = _Ws('{"serverContent":{}}')
        raw = await _guarded_recv(ws, guard)
        self.assertEqual(raw, '{"serverContent":{}}')
        self.assertEqual(guard.checks, 2)

if __name__ == '__main__':
    unittest.main()
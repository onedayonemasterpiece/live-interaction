import asyncio
import json
import unittest
from unittest.mock import patch
from live_interaction.provider import (
    DialogueHistory,
    _guarded_recv,
    _guarded_send,
    _send_with_budget_wait,
    _is_resource_failure,
    handle_server_message,
    run,
    setup_config,
)

class DialogueHistoryContract(unittest.TestCase):
    def test_streamed_reply_keeps_prior_user_constraint_as_one_turn(self):
        history=DialogueHistory()
        history.input_started()
        history.transcript('user','Не меняй выбранную фотографию; сначала проверь ревизию 17.')
        for index in range(12):
            history.transcript('model',f'Фрагмент {index}.')
        history.complete()
        turns=history.snapshot()
        self.assertEqual([item['role'] for item in turns],['user','model'])
        self.assertIn('Не меняй выбранную фотографию',turns[0]['text'])
        self.assertIn('Фрагмент 11',turns[1]['text'])
        history.transcript('user','И сохрани ограничение по лицензии.')
        self.assertIn('лицензии',history.snapshot()[0]['text'])

    def test_long_utterance_keeps_both_constraint_and_selected_object(self):
        history=DialogueHistory()
        history.input_started()
        history.transcript('user','Только с моим подтверждением. ' + 'Детали. '*100 + 'Выбран asset-42, revision 17.')
        turns=history.snapshot()
        self.assertLessEqual(len(turns[0]['text']),700)
        self.assertIn('Только с моим подтверждением',turns[0]['text'])
        self.assertIn('asset-42, revision 17',turns[0]['text'])

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

    def test_transcription_hints_are_application_scoped_and_bounded(self):
        hinted = setup_config(
            'gemini-3.8-live',
            {},
            configuration={
                'input_audio_transcription': {
                    'languageCodes': ['ru-RU'],
                    'customVocabulary': ['Мира', 'Projects Hub', 'Codex', 'DevCoveer'],
                },
            },
        )['setup']
        plain = setup_config('gemini-3.8-live', {}, configuration={})['setup']
        self.assertEqual(hinted['inputAudioTranscription']['languageCodes'], ['ru-RU'])
        self.assertIn('Мира', hinted['inputAudioTranscription']['customVocabulary'])
        self.assertEqual(plain['inputAudioTranscription'], {})
        with self.assertRaisesRegex(ValueError, 'languageCodes'):
            setup_config('gemini-3.8-live', {}, configuration={
                'input_audio_transcription': {'languageCodes': ['ru-RU'] * 9},
            })
        with self.assertRaisesRegex(ValueError, 'customVocabulary'):
            setup_config('gemini-3.8-live', {}, configuration={
                'input_audio_transcription': {'customVocabulary': ['x'] * 101},
            })

    def test_media_resolution_is_explicit_per_live_configuration(self):
        low=setup_config('gemini-3.8-live',{},configuration={'media_resolution':'MEDIA_RESOLUTION_LOW'})['setup']
        high=setup_config('gemini-3.8-live',{},configuration={'media_resolution':'MEDIA_RESOLUTION_HIGH'})['setup']
        default=setup_config('gemini-3.8-live',{},configuration={})['setup']
        self.assertEqual(low['generationConfig']['mediaResolution'],'MEDIA_RESOLUTION_LOW')
        self.assertEqual(high['generationConfig']['mediaResolution'],'MEDIA_RESOLUTION_HIGH')
        self.assertNotIn('mediaResolution',default['generationConfig'])
        with self.assertRaisesRegex(ValueError,'Unsupported media_resolution'):
            setup_config('gemini-3.8-live',{},configuration={'media_resolution':'LOWISH'})

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

    def test_manual_activity_setup_and_lossless_transcript(self):
        setup = setup_config(
            'gemini-3.8-live',
            {},
            configuration={'manual_activity_detection': True},
        )['setup']
        self.assertTrue(setup['realtimeInputConfig']['automaticActivityDetection']['disabled'])
        events = []
        text = 'x' * 5000
        handle_server_message({'serverContent': {'inputTranscription': {'text': text}}}, emit=events.append)
        self.assertEqual(events[0]['text'], text)

    def test_automatic_activity_detection_configuration_is_bounded(self):
        setup = setup_config(
            'gemini-3.8-live',
            {},
            configuration={'automatic_activity_detection': {
                'end_of_speech_sensitivity': 'END_SENSITIVITY_LOW',
                'silence_duration_ms': 5000,
                'prefix_padding_ms': 250,
            }},
        )['setup']
        automatic = setup['realtimeInputConfig']['automaticActivityDetection']
        self.assertFalse(automatic['disabled'])
        self.assertEqual(automatic['endOfSpeechSensitivity'], 'END_SENSITIVITY_LOW')
        self.assertEqual(automatic['silenceDurationMs'], 5000)
        self.assertEqual(automatic['prefixPaddingMs'], 250)

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
    async def test_transition_retries_denied_setup_grant_before_aborting(self):
        reader=_QueueReader()
        reader.feed({'type':'start','model':'gemini-3.8-live',
            'configuration':{'system_instruction':'core','functions':[{'name':'activate_capability'}]}})
        class ClosingAfterAck(_FakeSocket):
            async def send(self,payload):
                await super().send(payload)
                if 'toolResponse' in json.loads(payload):
                    self.incoming.put_nowait(self._CLOSED)
        class BudgetFailure(_ResourceFailure):
            code='RESOURCE_TOKEN_BUDGET'
        class Guard(_Guard):
            setup_attempts=0
            async def before_send(self,payload):
                if 'setup' in payload:
                    self.setup_attempts += 1
                    if self.setup_attempts == 2:raise BudgetFailure('denied')
                self.payloads.append(payload)
        first=ClosingAfterAck([{'setupComplete':{}}])
        denied=_FakeSocket([])
        resumed=_FakeSocket([{'setupComplete':{}}])
        sockets=[first,denied,resumed];events=[]
        with patch('websockets.connect',side_effect=lambda *_args,**_kwargs:sockets.pop(0)), \
             patch('live_interaction.provider.FRESH_HANDLE_WAIT_SECONDS',.01), \
             patch('live_interaction.provider.TRANSITION_BUDGET_RETRY_SECONDS',.01):
            task=asyncio.create_task(run(reader=reader,on_event=events.append,resource_guard=Guard()))
            for _ in range(100):
                if any(e.get('type')=='ready' for e in events):break
                await asyncio.sleep(.001)
            reader.feed({'type':'reconfigure','transition_id':'tr-budget','capability':'slide_edit',
                'configuration':{'system_instruction':'edit','functions':[{'name':'prepare_slide_change'}]},
                'continuation':'add one item',
                'router_response':{'name':'activate_capability','id':'route-budget',
                    'response':{'result':{'accepted':True}},'scheduling':'SILENT','willContinue':False}})
            for _ in range(300):
                if any(e.get('type')=='capability_ready' for e in events):break
                await asyncio.sleep(.001)
            self.assertTrue(any(e.get('type')=='capability_budget_wait' and e.get('retry')==1 for e in events))
            self.assertTrue(any(e.get('type')=='resource_budget_wait' and e.get('input_type')=='setup' for e in events))
            self.assertTrue(any(e.get('type')=='resource_budget_ready' and e.get('input_type')=='setup' for e in events))
            self.assertTrue(any(e.get('type')=='capability_ready' and e.get('capability')=='slide_edit' for e in events))
            self.assertFalse(any(e.get('type')=='error' for e in events))
            self.assertEqual(len(sockets),0)
            reader.feed({'type':'stop'})
            await asyncio.wait_for(task,1)

    async def test_delayed_setup_cannot_emit_late_ready_after_transition_deadline(self):
        reader=_QueueReader()
        reader.feed({'type':'start','model':'gemini-3.8-live',
            'configuration':{'system_instruction':'core','functions':[{'name':'activate_capability'}]}})
        class ClosingAfterAck(_FakeSocket):
            async def send(self,payload):
                await super().send(payload)
                if 'toolResponse' in json.loads(payload):
                    self.incoming.put_nowait(self._CLOSED)
        first=ClosingAfterAck([{'setupComplete':{}}])
        delayed=_FakeSocket([])
        sockets=[first,delayed]
        events=[]
        with patch('websockets.connect',side_effect=lambda *_args,**_kwargs:sockets.pop(0)), \
             patch('live_interaction.provider.TRANSITION_DEADLINE_SECONDS',.05), \
             patch('live_interaction.provider.MAX_TRANSITION_CONNECTION_ATTEMPTS',1):
            task=asyncio.create_task(run(load_key=lambda:'fixture-key',reader=reader,on_event=events.append))
            for _ in range(100):
                if any(e.get('type')=='ready' for e in events):break
                await asyncio.sleep(.001)
            reader.feed({'type':'reconfigure','transition_id':'tr-timeout','capability':'dataset',
                'configuration':{'system_instruction':'dataset','functions':[{'name':'dataset.find'}]},
                'continuation':'read the dataset',
                'router_response':{'name':'activate_capability','id':'route-timeout',
                    'response':{'result':{'accepted':True}},'scheduling':'SILENT','willContinue':False}})
            await asyncio.wait_for(task,1)
            delayed.incoming.put_nowait({'setupComplete':{}})
            self.assertTrue(any(e.get('type')=='capability_transition_error' and
                e.get('transition_id')=='tr-timeout' and e.get('code')=='LIVE_CAPABILITY_TIMEOUT' for e in events))
            self.assertFalse(any(e.get('type')=='capability_ready' for e in events))
            self.assertEqual(len(sockets),0)

    async def test_repeated_transition_connection_failures_are_bounded(self):
        reader=_QueueReader()
        reader.feed({'type':'start','model':'gemini-3.8-live',
            'configuration':{'system_instruction':'core','functions':[{'name':'activate_capability'}]}})
        class ClosingAfterAck(_FakeSocket):
            async def send(self,payload):
                await super().send(payload)
                if 'toolResponse' in json.loads(payload):
                    self.incoming.put_nowait(self._CLOSED)
        first=ClosingAfterAck([{'setupComplete':{}}]); calls=0; events=[]
        def connect(*_args,**_kwargs):
            nonlocal calls
            calls+=1
            if calls==1:return first
            raise ConnectionError('fixture setup failure')
        with patch('websockets.connect',side_effect=connect), \
             patch('live_interaction.provider.MAX_TRANSITION_CONNECTION_ATTEMPTS',2):
            task=asyncio.create_task(run(load_key=lambda:'fixture-key',reader=reader,on_event=events.append))
            for _ in range(100):
                if any(e.get('type')=='ready' for e in events):break
                await asyncio.sleep(.001)
            reader.feed({'type':'reconfigure','transition_id':'tr-retries','capability':'media',
                'configuration':{'system_instruction':'media','functions':[{'name':'open_media_chooser'}]},
                'continuation':'choose image',
                'router_response':{'name':'activate_capability','id':'route-retries',
                    'response':{'result':{'accepted':True}},'scheduling':'SILENT','willContinue':False}})
            await asyncio.wait_for(task,1)
            self.assertEqual(calls,3)
            self.assertTrue(any(e.get('type')=='capability_transition_error' and
                e.get('transition_id')=='tr-retries' for e in events))
            self.assertFalse(any(e.get('type')=='capability_ready' for e in events))

    async def test_router_socket_close_after_ack_restores_intent_without_provider_error(self):
        reader=_QueueReader()
        reader.feed({'type':'start','model':'gemini-3.8-live-extended-thinking',
            'configuration':{'system_instruction':'core','functions':[{'name':'activate_capability'}]}})
        class ClosingAfterAckSocket(_FakeSocket):
            async def send(self,payload):
                await super().send(payload)
                if 'toolResponse' in json.loads(payload):
                    self.incoming.put_nowait(self._CLOSED)
        first=ClosingAfterAckSocket([{'setupComplete':{}},
            {'sessionResumptionUpdate':{'resumable':True,'newHandle':'pre-call-handle'}}])
        second=_FakeSocket([{'setupComplete':{}}])
        sockets=[first,second]
        events=[]
        with patch('websockets.connect',side_effect=lambda *_args,**_kwargs:sockets.pop(0)):
            task=asyncio.create_task(run(load_key=lambda:'fixture-key',reader=reader,on_event=events.append))
            for _ in range(100):
                if any(e.get('type')=='resumption_state' for e in events):break
                await asyncio.sleep(.001)
            reader.feed({'type':'reconfigure','transition_id':'tr-ack-closed','capability':'asset_library',
                'configuration':{'system_instruction':'asset_library','functions':[{'name':'inspect_media_asset'}]},
                'continuation':'describe the image on this slide',
                'router_response':{'name':'activate_capability','id':'route-image',
                    'response':{'result':{'accepted':True}},'scheduling':'SILENT','willContinue':False}})
            for _ in range(200):
                if any(e.get('type')=='capability_ready' for e in events):break
                await asyncio.sleep(.001)
            self.assertTrue(any(e.get('type')=='capability_transition_acknowledged' for e in events))
            self.assertTrue(any(e.get('type')=='capability_transition_recovered' and
                e.get('reason')=='router_connection_closed' for e in events))
            self.assertTrue(any(e.get('type')=='capability_ready' for e in events))
            self.assertEqual(second.sent[0]['setup']['sessionResumption'],{})
            self.assertFalse(any(e.get('type')=='error' for e in events))
            reader.feed({'type':'stop'})
            await asyncio.wait_for(task,1)

    async def test_router_socket_close_restores_intent_instead_of_ending_live(self):
        reader=_QueueReader()
        reader.feed({'type':'start','model':'gemini-3.8-live-extended-thinking',
            'configuration':{'system_instruction':'core','functions':[{'name':'activate_capability'}]}})
        class ClosingRouterSocket(_FakeSocket):
            async def send(self,payload):
                if 'toolResponse' in json.loads(payload):
                    raise ConnectionError('closed before router acknowledgement')
                await super().send(payload)
        first=ClosingRouterSocket([{'setupComplete':{}},
            {'sessionResumptionUpdate':{'resumable':True,'newHandle':'pre-call-handle'}}])
        second=_FakeSocket([{'setupComplete':{}}])
        sockets=[first,second]
        events=[]
        with patch('websockets.connect',side_effect=lambda *_args,**_kwargs:sockets.pop(0)):
            task=asyncio.create_task(run(load_key=lambda:'fixture-key',reader=reader,on_event=events.append))
            for _ in range(100):
                if any(e.get('type')=='resumption_state' for e in events):break
                await asyncio.sleep(.001)
            reader.feed({'type':'reconfigure','transition_id':'tr-closed','capability':'asset_library',
                'configuration':{'system_instruction':'asset_library','functions':[{'name':'inspect_media_asset'}]},
                'continuation':'describe the image on this slide',
                'router_response':{'name':'activate_capability','id':'route-image',
                    'response':{'result':{'accepted':True}},'scheduling':'SILENT','willContinue':False}})
            for _ in range(200):
                if any(e.get('type')=='capability_ready' for e in events):break
                await asyncio.sleep(.001)
            self.assertTrue(any(e.get('type')=='capability_transition_recovered' and
                e.get('reason')=='router_connection_closed' for e in events))
            self.assertTrue(any(e.get('type')=='capability_ready' and
                e.get('capability')=='asset_library' for e in events))
            self.assertEqual(second.sent[0]['setup']['sessionResumption'],{})
            self.assertTrue(any('clientContent' in item for item in second.sent))
            self.assertFalse(any(e.get('type')=='error' for e in events))
            reader.feed({'type':'stop'})
            await asyncio.wait_for(task,1)

    async def test_missing_post_response_checkpoint_restores_bounded_history_and_intent(self):
        reader=_QueueReader()
        reader.feed({'type':'start','model':'gemini-3.8-live','history':[
            {'role':'user','text':'previous topic'}],
            'configuration':{'system_instruction':'core','functions':[{'name':'activate_capability'}]}})
        first=_FakeSocket([{'setupComplete':{}},
            {'sessionResumptionUpdate':{'resumable':True,'newHandle':'pre-call-handle'}}])
        second=_FakeSocket([{'setupComplete':{}}])
        sockets=[first,second]
        def fake_connect(*args,**kwargs):
            return sockets.pop(0)
        events=[]
        with patch('websockets.connect',side_effect=fake_connect), patch(
            'live_interaction.provider.FRESH_HANDLE_WAIT_SECONDS',0.025):
            task=asyncio.create_task(run(load_key=lambda:'fixture-key',reader=reader,on_event=events.append))
            for _ in range(100):
                if any(e.get('type')=='resumption_state' for e in events):break
                await asyncio.sleep(.001)
            self.assertTrue(any(e.get('type')=='resumption_state' for e in events))
            reader.feed({'type':'text','text':'attach a dataset; do not edit the selected image','queued_at':1})
            for _ in range(100):
                if any(e.get('type')=='input_timing' for e in events):break
                await asyncio.sleep(.001)
            for index in range(12):
                first.incoming.put_nowait({'serverContent':{'outputTranscription':{'text':f'Ответ {index}. '}}})
            first.incoming.put_nowait({'serverContent':{'turnComplete':True}})
            for _ in range(100):
                if any(e.get('type')=='turn_complete' for e in events):break
                await asyncio.sleep(.001)
            reader.feed({'type':'reconfigure','transition_id':'tr-restore','capability':'dataset',
                'configuration':{'system_instruction':'dataset','functions':[{'name':'dataset.find'}]},
                'continuation':'attach the national projects dataset',
                'router_response':{'name':'activate_capability','id':'cap-restore',
                    'response':{'result':{'accepted':True}},'scheduling':'SILENT','willContinue':False}})
            for _ in range(200):
                if any(e.get('type')=='capability_continuation_sent' for e in events):break
                await asyncio.sleep(.001)
            self.assertEqual(len(sockets),0)
            self.assertTrue(first.closed)
            self.assertEqual(second.sent[0]['setup']['sessionResumption'],{})
            self.assertEqual(second.sent[0]['setup']['historyConfig'],{'initialHistoryInClientContent':True})
            system=second.sent[0]['setup']['systemInstruction']['parts'][0]['text']
            self.assertNotIn('previous topic',system)
            client_turns=[m['clientContent'] for m in second.sent if 'clientContent' in m]
            self.assertEqual(len(client_turns),2)
            restored=json.dumps(client_turns[0],ensure_ascii=False)
            self.assertIn('previous topic',restored)
            self.assertIn('attach a dataset; do not edit the selected image',restored)
            self.assertIn('Ответ 11',restored)
            self.assertIn('attach the national projects dataset',json.dumps(client_turns[1],ensure_ascii=False))
            self.assertTrue(any(e.get('type')=='capability_transition_recovered' and
                e.get('reason')=='fresh_handle_unavailable' for e in events))
            self.assertTrue(any(e.get('type')=='resumed' and
                e.get('resumption_mode')=='history_restore' for e in events))
            reader.feed({'type':'stop'})
            await asyncio.wait_for(task,1)

    async def test_reconfigure_completes_router_call_before_using_fresh_resumption_handle(self):
        reader=_QueueReader()
        reader.feed({
            'type':'start',
            'model':'gemini-3.8-live',
            'context':{'slide':'one'},
            'history':[{'role':'user','text':'earlier constraint from previous connection'}],
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
                    'response':{'result':{'capability':'dataset','accepted':True}},
                    'scheduling':'SILENT',
                    'willContinue':False,
                },
            })
            for _ in range(100):
                if any('toolResponse' in item for item in first.sent):
                    break
                await asyncio.sleep(0.001)
            self.assertTrue(any('toolResponse' in item for item in first.sent))
            response=next(item['toolResponse']['functionResponses'][0] for item in first.sent if 'toolResponse' in item)
            self.assertEqual(response['scheduling'],'SILENT')
            self.assertIs(response['willContinue'],False)
            self.assertNotIn('scheduling',response['response'])
            self.assertEqual(len(connect_calls),1)
            first.incoming.put_nowait({'sessionResumptionUpdate':{'resumable':True,'newHandle':'fresh-handle'}})
            for _ in range(200):
                if any(e.get('type')=='capability_ready' and e.get('transition_id')=='tr-1' for e in events):
                    break
                await asyncio.sleep(0.001)
            self.assertEqual(len(connect_calls),2)
            self.assertEqual(first.sent[0]['setup']['sessionResumption'],{})
            self.assertEqual(second.sent[0]['setup']['sessionResumption'],{'handle':'fresh-handle'})
            self.assertNotIn('earlier constraint from previous connection',second.sent[0]['setup']['systemInstruction']['parts'][0]['text'])
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
    async def test_optional_frame_budget_denial_does_not_close_session(self):
        class BudgetFailure(_ResourceFailure):
            code='RESOURCE_TOKEN_BUDGET'
        class Guard(_Guard):
            async def before_send(self,payload):
                raise BudgetFailure('denied')
        ws=_Ws();events=[];state={'stopped':False,'ws':ws,'drop_inputs_before':0}
        sent=await _send_with_budget_wait(ws,{'realtimeInput':{'video':{'data':'AAAA'}}},Guard(),events.append,'snapshot',state,optional=True)
        self.assertFalse(sent)
        self.assertEqual(ws.sent,[])
        self.assertEqual(events,[{'type':'input_dropped','reason':'resource_budget','input_type':'snapshot'}])
        with self.assertRaises(BudgetFailure):
            await _send_with_budget_wait(ws,{'realtimeInput':{'video':{'data':'AAAA'}}},Guard(),events.append,'snapshot',state)

    async def test_tool_result_waits_for_grant_without_rerunning_tool(self):
        class BudgetFailure(_ResourceFailure):
            code='RESOURCE_TOKEN_BUDGET'
            retry_after_ms=1
        class Guard(_Guard):
            attempts=0
            async def before_send(self,payload):
                self.attempts+=1
                if self.attempts==1:raise BudgetFailure('denied')
                self.payloads.append(payload)
        guard=Guard();ws=_Ws();events=[];state={'stopped':False,'ws':ws,'drop_inputs_before':0}
        payload={'toolResponse':{'functionResponses':[{'name':'apply','id':'one','response':{'result':{'revision':19}}}]}}
        sent=await _send_with_budget_wait(ws,payload,guard,events.append,'tool_response',state,deadline_seconds=1)
        self.assertTrue(sent)
        self.assertEqual(guard.attempts,2)
        self.assertEqual(len(ws.sent),1)
        self.assertEqual(json.loads(ws.sent[0]),payload)
        self.assertEqual([e['type'] for e in events],['resource_budget_wait','resource_budget_ready'])
        self.assertGreater(state['drop_inputs_before'],0)

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

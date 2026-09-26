import unittest
from live_interaction.provider import setup_config, handle_server_message

class ProviderContract(unittest.TestCase):
    def test_configuration_is_per_application_not_global(self):
        a = setup_config('gemini-3.8-live', {'id': 'one'}, configuration={'system_instruction': 'cohost', 'functions': [{'name':'read_story'}], 'voice':'Aoede'})['setup']
        b = setup_config('gemini-3.8-live', {'id': 'two'}, configuration={'system_instruction':'ideas'})['setup']
        self.assertEqual(a['tools'][0]['functionDeclarations'][0]['name'], 'read_story')
        self.assertEqual(b['tools'], [])
        self.assertNotIn('read_story', str(b))
        self.assertNotIn('thinkingConfig', a['generationConfig'])
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

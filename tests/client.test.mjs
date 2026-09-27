import test from 'node:test';
import assert from 'node:assert/strict';
import {createLiveClient} from '../browser/client.js';
const tick=()=>new Promise(r=>setImmediate(r));
test('Stop during setup is local; late setup is cleaned without starting a microphone',async()=>{
 let release;const calls=[],states=[];
 const client=createLiveClient({request:(url)=>{calls.push(url);return url==='/live'?new Promise(r=>release=r):Promise.resolve({});},onState:s=>states.push(s)});
 const start=client.start({url:'/live'});await tick();client.stop();
 assert.equal(client.starting,false);assert.equal(client.sessionId,null);assert.equal(states.at(-1),'off');
 release({session_id:'old'});await start;assert.ok(calls.includes('/live/old/stop'));assert.equal(client.sessionId,null);
});
test('delay is hidden for 15 seconds, distinguishes delivery from provider, and offers restart at two minutes',async t=>{
 t.mock.timers.enable({apis:['setInterval','Date'],now:1000});
 let release;const waits=[];
 const client=createLiveClient({request:url=>url==='/live'?Promise.resolve({session_id:'one'}):url.includes('/events')?new Promise(r=>release=r):Promise.resolve({}),onWait:s=>waits.push(s)});
 await client.start({url:'/live'});await client.input({text:'hello'});t.mock.timers.tick(14000);assert.equal(waits.filter(Boolean).length,0);
 t.mock.timers.tick(1000);assert.equal(waits.at(-1).stage,'transport');assert.equal(waits.at(-1).can_restart,false);
 release({events:[{seq:1,type:'input_timing',audio_stream_end_sent_at:999}],cursor:1});await tick();
 t.mock.timers.tick(105000);assert.equal(waits.at(-1).stage,'provider');assert.equal(waits.at(-1).elapsed_ms,120000);assert.equal(waits.at(-1).can_restart,true);
 client.stop();assert.equal(waits.at(-1),null);assert.equal(client.sessionId,null);
});
test('an Extended tool wait survives intermediate turnComplete and clears on IDLE',async t=>{
 t.mock.timers.enable({apis:['setInterval','Date'],now:1000});let release;const waits=[];
 const client=createLiveClient({request:url=>url==='/live'?Promise.resolve({session_id:'one',model:'gemini-3.8-live-extended-thinking'}):url.includes('/events')?new Promise(r=>release=r):Promise.resolve({}),onWait:s=>waits.push(s)});
 await client.start({url:'/live'});await client.input({text:'do the action'});
 release({events:[{seq:1,type:'tool_call',calls:[{id:'mutation'}]},{seq:2,type:'input_timing',text_sent_at:1001},{seq:3,type:'turn_complete'}],cursor:3});await tick();
 t.mock.timers.tick(16000);assert.equal(waits.at(-1).stage,'action');
 await new Promise(r=>setTimeout(r,180));release({events:[{seq:4,type:'tool_result',id:'mutation'},{seq:5,type:'interaction_status',status:'IDLE'}],cursor:5});await tick();
 assert.equal(waits.at(-1),null);client.stop();
});


test('microphone handoff reuses the existing stream and sends buffered PCM before live capture',async()=>{
  const originalNavigator=globalThis.navigator,originalAudio=globalThis.AudioContext;
  let getUserMediaCalls=0,stopCalls=0,handoffCalls=0;
  const track={readyState:'live',stop(){stopCalls++;this.readyState='ended';}};
  const handedStream={getTracks:()=>[track]};
  class FakeNode{connect(){return this;}disconnect(){}}
  class FakeProcessor extends FakeNode{onaudioprocess=null;}
  class FakeContext{
    sampleRate=48000;state='running';destination={};
    createMediaStreamSource(stream){assert.equal(stream,handedStream);return new FakeNode();}
    createScriptProcessor(){return new FakeProcessor();}
    async resume(){}
    async close(){this.state='closed';}
  }
  Object.defineProperty(globalThis,'navigator',{value:{mediaDevices:{getUserMedia:async()=>{getUserMediaCalls++;throw new Error('must not open a second microphone');}}},configurable:true});
  globalThis.AudioContext=FakeContext;
  const inputs=[];
  const client=createLiveClient({request:async(url,options={})=>{
    if(url==='/live')return {session_id:'one',model:'gemini-3.8-live'};
    if(url==='/live/one/input'){inputs.push(JSON.parse(options.body));await new Promise(r=>setTimeout(r,25));return {ok:true};}
    if(url.startsWith('/live/one/events'))return {events:[],cursor:0,closed:false};
    if(url==='/live/one/stop')return {ok:true};
    throw new Error('unexpected '+url);
  }});
  try{
    const chunk=new Float32Array(4096).fill(.2);
    const started=await client.start({
      url:'/live',
      takeMicrophoneHandoff:async()=>{handoffCalls++;return {stream:handedStream,sampleRate:48000,chunks:Array.from({length:64},()=>chunk)};}
    });
    assert.equal(started.session_id,'one');
    assert.equal(handoffCalls,1);
    assert.equal(getUserMediaCalls,0);
    for(let i=0;i<30&&!inputs.some(item=>item.audio_base64);i++)await new Promise(r=>setTimeout(r,20));
    assert.ok(inputs.some(item=>typeof item.audio_base64==='string'&&item.audio_base64.length>100));
    assert.equal(client.sessionId,'one');
    assert.equal(track.readyState,'live');
    client.stop();
    assert.equal(stopCalls,1);
    assert.equal(track.readyState,'ended');
  }finally{
    if(originalNavigator===undefined)delete globalThis.navigator;else Object.defineProperty(globalThis,'navigator',{value:originalNavigator,configurable:true});
    if(originalAudio===undefined)delete globalThis.AudioContext;else globalThis.AudioContext=originalAudio;
  }
});

test('text-only start does not request microphone and microphone can be enabled later in the same session',async()=>{
  const originalNavigator=globalThis.navigator,originalAudio=globalThis.AudioContext;
  let getUserMediaCalls=0,stopCalls=0;
  const track={stop(){stopCalls++;}};
  const liveStream={getTracks:()=>[track]};
  class FakeNode{connect(){return this;}disconnect(){}}
  class FakeProcessor extends FakeNode{onaudioprocess=null;}
  class FakeContext{
    sampleRate=48000;state='running';destination={};
    createMediaStreamSource(stream){assert.equal(stream,liveStream);return new FakeNode();}
    createScriptProcessor(){return new FakeProcessor();}
    async resume(){}
    async close(){this.state='closed';}
  }
  Object.defineProperty(globalThis,'navigator',{value:{mediaDevices:{getUserMedia:async()=>{getUserMediaCalls++;return liveStream;}}},configurable:true});
  globalThis.AudioContext=FakeContext;
  const client=createLiveClient({request:async(url)=>{
    if(url==='/live')return {session_id:'one',model:'gemini-3.8-live'};
    if(url.startsWith('/live/one/events'))return {events:[],cursor:0,closed:false};
    if(url==='/live/one/stop')return {ok:true};
    return {ok:true};
  }});
  try{
    await client.start({url:'/live',microphone:false});
    assert.equal(client.sessionId,'one');
    assert.equal(client.microphoneEnabled,false);
    assert.equal(getUserMediaCalls,0);
    assert.equal(await client.enableMicrophone(),true);
    assert.equal(client.microphoneEnabled,true);
    assert.equal(getUserMediaCalls,1);
    client.stop();
    assert.equal(stopCalls,1);
  }finally{
    if(originalNavigator===undefined)delete globalThis.navigator;else Object.defineProperty(globalThis,'navigator',{value:originalNavigator,configurable:true});
    if(originalAudio===undefined)delete globalThis.AudioContext;else globalThis.AudioContext=originalAudio;
  }
});

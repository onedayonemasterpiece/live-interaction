import test from 'node:test';
import assert from 'node:assert/strict';
import {createLiveClient} from '../browser/client.js';
const decodedInput=options=>options.headers?.['content-type']==='application/octet-stream'
  ?{pcm:new Uint8Array(options.body)}:JSON.parse(options.body);
const tick=()=>new Promise(r=>setImmediate(r));
test('Stop while getUserMedia is pending leaves no live track after late permission',async()=>{
 const originalNavigator=globalThis.navigator,originalAudio=globalThis.AudioContext;
 let grant,stops=0,contexts=0;
 const track={readyState:'live',stop(){stops++;this.readyState='ended';}};
 Object.defineProperty(globalThis,'navigator',{value:{mediaDevices:{getUserMedia:()=>new Promise(resolve=>{grant=resolve;})}},configurable:true});
 globalThis.AudioContext=class{constructor(){contexts++;}async resume(){}};
 const client=createLiveClient({request:url=>url==='/live'?Promise.resolve({session_id:'one',model:'gemini-3.8-live'}):url.includes('/events')?new Promise(()=>{}):Promise.resolve({ok:true})});
 try{
  const pending=client.start({url:'/live'});
  for(let i=0;i<20&&!grant;i++)await tick();
  assert.equal(typeof grant,'function');
  client.stop();
  grant({getTracks:()=>[track]});
  await pending;
  assert.equal(track.readyState,'ended');
  assert.equal(stops,1);
  assert.equal(contexts,1,'playback context may start, but capture must not');
  assert.equal(client.sessionId,null);
 }finally{
  client.stop();
  if(originalNavigator===undefined)delete globalThis.navigator;else Object.defineProperty(globalThis,'navigator',{value:originalNavigator,configurable:true});
  if(originalAudio===undefined)delete globalThis.AudioContext;else globalThis.AudioContext=originalAudio;
 }
});
test('terminal provider error releases the active browser microphone before closed poll flag',async()=>{
 const originalNavigator=globalThis.navigator,originalAudio=globalThis.AudioContext;
 let releaseEvents,stopCalls=0;const states=[];
 const track={readyState:'live',stop(){stopCalls++;this.readyState='ended';}};
 class FakeNode{connect(){}disconnect(){}}
 class FakeContext{sampleRate=48000;state='running';destination={};createMediaStreamSource(){return new FakeNode();}createScriptProcessor(){return {connect(){},disconnect(){},onaudioprocess:null};}async resume(){}async close(){this.state='closed';}}
 Object.defineProperty(globalThis,'navigator',{value:{mediaDevices:{getUserMedia:async()=>({getTracks:()=>[track]})}},configurable:true});
 globalThis.AudioContext=FakeContext;
 const client=createLiveClient({request:url=>url==='/live'?Promise.resolve({session_id:'one',model:'gemini-3.8-live'}):url.includes('/events')?new Promise(resolve=>{releaseEvents=resolve;}):Promise.resolve({ok:true}),onState:s=>states.push(s)});
 try{
  await client.start({url:'/live'});
  for(let i=0;i<30&&!releaseEvents;i++)await tick();
  assert.equal(track.readyState,'live');
  releaseEvents({events:[{seq:1,type:'error',code:'RESOURCE_TOKEN_BUDGET',message:'Live capability transition failed'}],cursor:1,closed:false});
  for(let i=0;i<30&&track.readyState==='live';i++)await tick();
  assert.equal(track.readyState,'ended');assert.equal(stopCalls,1);
  assert.equal(client.sessionId,null);assert.equal(states.at(-1),'off');
 }finally{
  client.stop();
  if(originalNavigator===undefined)delete globalThis.navigator;else Object.defineProperty(globalThis,'navigator',{value:originalNavigator,configurable:true});
  if(originalAudio===undefined)delete globalThis.AudioContext;else globalThis.AudioContext=originalAudio;
 }
});
test('capability resume reuses the active microphone and Stop ends its only track',async()=>{
 const originalNavigator=globalThis.navigator,originalAudio=globalThis.AudioContext;
 let releaseEvents,getUserMediaCalls=0,stopCalls=0;
 const track={readyState:'live',stop(){stopCalls++;this.readyState='ended';}};
 class FakeNode{connect(){}disconnect(){}}
 class FakeContext{sampleRate=48000;state='running';destination={};createMediaStreamSource(){return new FakeNode();}createScriptProcessor(){return {connect(){},disconnect(){},onaudioprocess:null};}async resume(){}async close(){this.state='closed';}}
 Object.defineProperty(globalThis,'navigator',{value:{mediaDevices:{getUserMedia:async()=>{getUserMediaCalls++;return {getTracks:()=>[track]};}}},configurable:true});
 globalThis.AudioContext=FakeContext;
 const client=createLiveClient({request:url=>url==='/live'?Promise.resolve({session_id:'one',model:'gemini-3.8-live'}):url.includes('/events')?new Promise(resolve=>{releaseEvents=resolve;}):Promise.resolve({ok:true})});
 try{
  await client.start({url:'/live'});
  for(let i=0;i<30&&!releaseEvents;i++)await tick();
  assert.equal(getUserMediaCalls,1);
  releaseEvents({events:[{seq:1,type:'resumed',capability:'slide_edit'}],cursor:1,closed:false});
  for(let i=0;i<30;i++)await tick();
  assert.equal(getUserMediaCalls,1,'a capability switch must not orphan the existing stream');
  client.stop();assert.equal(stopCalls,1);assert.equal(track.readyState,'ended');
 }finally{
  client.stop();
  if(originalNavigator===undefined)delete globalThis.navigator;else Object.defineProperty(globalThis,'navigator',{value:originalNavigator,configurable:true});
  if(originalAudio===undefined)delete globalThis.AudioContext;else globalThis.AudioContext=originalAudio;
 }
});
test('browser sends PCM as binary and reports HTTP versus server receive-to-ack time',async()=>{
  const requests=[],timings=[];let releaseEvents;
  const client=createLiveClient({binaryAudio:true,request:async(url,options={})=>{
    if(url==='/live')return {session_id:'one',model:'gemini-3.8-live'};
    if(url.startsWith('/live/one/events'))return new Promise(resolve=>{releaseEvents=resolve;});
    if(url==='/live/one/input'){requests.push(options);return {timing:{received_at:100,handled_at:112}};}
    if(url==='/live/one/stop')return {ok:true};
    throw Error('Unexpected request');
  },onTiming:(event,metrics)=>timings.push({event,...metrics})});
  await client.start({url:'/live',microphone:false});
  await client.input({pcm:new Int16Array([0,32767,-32768,123])});
  assert.equal(requests[0].headers['content-type'],'application/octet-stream');
  assert.deepEqual([...requests[0].body],[0,0,255,127,0,128,123,0]);
  assert.equal(timings.find(item=>item.event==='audio_http')?.server_receive_to_ack_ms,12);
  client.stop();releaseEvents?.({events:[],cursor:0,closed:true});
});
test('short first provider audio chunk is buffered until following audio can play continuously',async()=>{
 const originalAudio=globalThis.AudioContext,starts=[],created=performance.now();
 let releasePoll,polls=0;
 class FakeAudioContext{
  state='running';destination={};
  get currentTime(){return (performance.now()-created)/1000;}
  createBuffer(_channels,length,rate){const samples=new Float32Array(length);return {duration:length/rate,getChannelData:()=>samples};}
  createBufferSource(){const context=this;return {buffer:null,onended:null,connect(){},disconnect(){},start(at){starts.push({at,receivedAt:context.currentTime,duration:this.buffer.duration});},stop(){}};}
  async resume(){}
 }
 globalThis.AudioContext=FakeAudioContext;
 const client=createLiveClient({request:url=>{
  if(url==='/live')return Promise.resolve({session_id:'one',model:'gemini-3.8-live-extended-thinking'});
  if(url.includes('/events')){polls++;return new Promise(resolve=>{releasePoll=resolve;});}
  return Promise.resolve({ok:true});
 }});
 const audio=(seq,length)=>({seq,type:'audio',data:Buffer.alloc(length*2).toString('base64'),mime_type:'audio/pcm;rate=24000'});
 try{
  await client.start({url:'/live',microphone:false});
  for(let i=0;i<30&&!releasePoll;i++)await tick();
  releasePoll({events:[audio(1,1200)],cursor:1});
  for(let i=0;i<30&&starts.length<1;i++)await tick();
  assert.ok(starts[0].at-starts[0].receivedAt>=.38,'50 ms first chunk starts with jitter reserve');
  await new Promise(r=>setTimeout(r,300));
  for(let i=0;i<30&&polls<2;i++)await tick();
  releasePoll({events:[audio(2,9600)],cursor:2});
  for(let i=0;i<30&&starts.length<2;i++)await tick();
  assert.equal(starts.length,2);
  assert.ok(Math.abs(starts[1].at-(starts[0].at+starts[0].duration))<.005,'following chunk starts exactly at the first chunk end');
 }finally{client.stop();if(originalAudio===undefined)delete globalThis.AudioContext;else globalThis.AudioContext=originalAudio;}
});
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


test('captureDuringStart preserves speech spoken while the Live session is still being created',async()=>{
  const originalNavigator=globalThis.navigator,originalAudio=globalThis.AudioContext;
  let getUserMediaCalls=0,stopCalls=0,releaseStart;const processors=[],contexts=[],inputs=[];
  const track={readyState:'live',stop(){stopCalls++;this.readyState='ended';}};
  const liveStream={getTracks:()=>[track]};
  class FakeNode{connect(){return this;}disconnect(){}}
  class FakeProcessor extends FakeNode{onaudioprocess=null;}
  class FakeContext{
    sampleRate=48000;state='running';destination={};
    constructor(){contexts.push(this);}
    createMediaStreamSource(stream){assert.equal(stream,liveStream);return new FakeNode();}
    createScriptProcessor(){const p=new FakeProcessor();processors.push(p);return p;}
    async resume(){}
    async close(){this.state='closed';}
  }
  Object.defineProperty(globalThis,'navigator',{value:{mediaDevices:{getUserMedia:async()=>{getUserMediaCalls++;return liveStream;}}},configurable:true});
  globalThis.AudioContext=FakeContext;
  const client=createLiveClient({binaryAudio:true,request:async(url,options={})=>{
    if(url==='/live')return new Promise(resolve=>releaseStart=resolve);
    if(url==='/live/one/input'){inputs.push(decodedInput(options));return {ok:true};}
    if(url.startsWith('/live/one/events'))return {events:[],cursor:0,closed:false};
    if(url==='/live/one/stop')return {ok:true};
    throw new Error('unexpected '+url);
  }});
  try{
    const starting=client.start({url:'/live',captureDuringStart:true});
    for(let i=0;i<20&&!releaseStart;i++)await tick();
    assert.equal(getUserMediaCalls,1,'microphone capture starts before session creation resolves');
    assert.equal(processors.length,1);
    const chunk=new Float32Array(4096).fill(.2);
    for(let i=0;i<4;i++)processors[0].onaudioprocess({inputBuffer:{getChannelData:()=>chunk}});
    releaseStart({session_id:'one',model:'gemini-3.8-live'});
    const started=await starting;assert.equal(started.session_id,'one');
    assert.equal(getUserMediaCalls,1,'the startup capture stream is reused after session creation');
    for(let i=0;i<30&&!inputs.some(item=>item.pcm);i++)await new Promise(r=>setTimeout(r,10));
    assert.ok(inputs.some(item=>item.pcm?.byteLength>100),'buffered startup speech reaches Live as binary PCM');
    assert.equal(contexts[0].state,'closed','temporary startup AudioContext is retired after handoff');
    client.stop();assert.equal(stopCalls,1);assert.equal(track.readyState,'ended');
  }finally{
    if(originalNavigator===undefined)delete globalThis.navigator;else Object.defineProperty(globalThis,'navigator',{value:originalNavigator,configurable:true});
    if(originalAudio===undefined)delete globalThis.AudioContext;else globalThis.AudioContext=originalAudio;
  }
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
  const client=createLiveClient({binaryAudio:true,request:async(url,options={})=>{
    if(url==='/live')return {session_id:'one',model:'gemini-3.8-live'};
    if(url==='/live/one/input'){inputs.push(decodedInput(options));await new Promise(r=>setTimeout(r,25));return {ok:true};}
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
    for(let i=0;i<30&&!inputs.some(item=>item.pcm);i++)await new Promise(r=>setTimeout(r,20));
    assert.ok(inputs.some(item=>item.pcm?.byteLength>100));
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

test('audio input has a catch-up-safe HTTP ceiling while text stays fast-bounded',async()=>{
  const descriptor=Object.getOwnPropertyDescriptor(AbortSignal,'timeout');
  const observed=[];
  Object.defineProperty(AbortSignal,'timeout',{configurable:true,writable:true,value:ms=>{
    observed.push(ms);
    return new AbortController().signal;
  }});
  let releaseEvents;
  const client=createLiveClient({request:async(url)=>{
    if(url==='/live')return {session_id:'one',model:'gemini-3.8-live'};
    if(url.startsWith('/live/one/events'))return new Promise(resolve=>{releaseEvents=resolve;});
    if(url==='/live/one/input')return {ok:true};
    if(url==='/live/one/stop')return {ok:true};
    throw new Error('unexpected '+url);
  }});
  try{
    await client.start({url:'/live',microphone:false});
    observed.length=0;
    await client.input({text:'hello'});
    await client.input({audio_base64:'AAAA'});
    await client.input({audio_stream_end:true});
    assert.deepEqual(observed,[2500,10000,10000]);
    client.stop();
    releaseEvents?.({events:[],cursor:0,closed:true});
  }finally{
    if(descriptor)Object.defineProperty(AbortSignal,'timeout',descriptor);
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

test('createLiveClient persists accepted microphone PCM before HTTP transport',async()=>{
  const originalNavigator=globalThis.navigator,originalAudio=globalThis.AudioContext;
  const processors=[];let releasePersist;const order=[];
  const track={stop(){}},stream={getTracks:()=>[track]};
  class Node{connect(){return this;}disconnect(){}}
  class Processor extends Node{onaudioprocess=null;}
  class Context{
    sampleRate=16000;state='running';destination={};
    createMediaStreamSource(){return new Node();}
    createScriptProcessor(){const p=new Processor();processors.push(p);return p;}
    async resume(){}
    async close(){this.state='closed';}
  }
  Object.defineProperty(globalThis,'navigator',{value:{mediaDevices:{getUserMedia:async()=>stream}},configurable:true});
  globalThis.AudioContext=Context;
  let releaseEvents;
  const client=createLiveClient({
    persistAudio:message=>{
      order.push(message.pcm?'persist_pcm':'persist_end');
      return new Promise(resolve=>{releasePersist=resolve;});
    },
    request:async(url,options={})=>{
      if(url==='/live')return {session_id:'one',model:'gemini-3.8-live'};
      if(url.startsWith('/live/one/events'))return new Promise(resolve=>{releaseEvents=resolve;});
      if(url==='/live/one/input'){order.push('http_audio');return {ok:true};}
      if(url==='/live/one/stop')return {ok:true};
      throw new Error('unexpected '+url);
    }
  });
  try{
    await client.start({url:'/live'});
    const speech=new Float32Array(1600).fill(.2);
    processors[0].onaudioprocess({inputBuffer:{getChannelData:()=>speech}});
    for(let i=0;i<20&&!releasePersist;i++)await tick();
    assert.deepEqual(order,['persist_pcm']);
    releasePersist();
    for(let i=0;i<30&&!order.includes('http_audio');i++)await new Promise(resolve=>setTimeout(resolve,20));
    assert.deepEqual(order.slice(0,2),['persist_pcm','http_audio']);
    client.stop();
    releaseEvents?.({events:[],cursor:0,closed:true});
  }finally{
    if(originalNavigator===undefined)delete globalThis.navigator;else Object.defineProperty(globalThis,'navigator',{value:originalNavigator,configurable:true});
    if(originalAudio===undefined)delete globalThis.AudioContext;else globalThis.AudioContext=originalAudio;
  }
});

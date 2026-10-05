import test from 'node:test';
import assert from 'node:assert/strict';
import {createLiveAudioSender} from '../browser/live-audio.js';
import {createLiveClient} from '../browser/client.js';

const tick=()=>new Promise(resolve=>setImmediate(resolve));
const drain=async()=>{for(let i=0;i<20;i++)await tick();};

test('explicit turn end drains ordered admitted PCM, never closes microphone sender or creates empty turns',async()=>{
  const sent=[];let release;
  const sender=createLiveAudioSender({manualActivityDetection:true,speechStartMs:180,
    send:message=>{sent.push(message);return message.pcm&&!release?new Promise(resolve=>{release=resolve;}):Promise.resolve();}});
  try{
    assert.equal(sender.endTurn(),false);
    sender.push(new Int16Array(320).fill(3000),.1);
    assert.equal(sender.endTurn(),false,'unconfirmed noise cannot become a turn');
    for(let i=0;i<12;i++)sender.push(new Int16Array(320).fill(3000),.1);
    await drain();
    assert.equal(sender.endTurn(),true);
    assert.equal(sender.endTurn(),false,'double click is idempotent');
    assert.equal(sent.some(x=>x.activity_end),false,'end cannot overtake pending PCM');
    release();await drain();
    assert.equal(sent[0].activity_start,true);
    assert.equal(sent.at(-1).activity_end,true);
    assert.equal(sent.filter(x=>x.pcm).reduce((n,x)=>n+x.pcm.length,0),13*320);
    for(let i=0;i<12;i++)sender.push(new Int16Array(320).fill(4000),.1);
    assert.equal(sender.endTurn(),true);await drain();
    assert.equal(sent.filter(x=>x.activity_start).length,2);
    assert.equal(sent.filter(x=>x.activity_end).length,2);
  }finally{release?.();sender.stop();}
});

test('manual finish pumps the final boundary even after the PCM queue is already empty',async()=>{
  const sent=[];
  const sender=createLiveAudioSender({manualActivityDetection:true,send:async x=>sent.push(x)});
  try{
    sender.push(new Int16Array(1600).fill(3000),.1);await drain();
    await sender.finish();await drain();
    assert.equal(sent.at(-1).activity_end,true);
  }finally{sender.stop();}
});

class FakeSocket{
  static OPEN=1;static instance;
  constructor(){FakeSocket.instance=this;this.readyState=0;this.bufferedAmount=0;queueMicrotask(()=>{this.readyState=1;this.onopen?.();});}
  send(data){if(typeof data==='string'){const value=JSON.parse(data);if(value.type==='hello')queueMicrotask(()=>this.onmessage({data:JSON.stringify({type:'hello_ack',protocol:'wl-live-v1',connection_generation:value.connection_generation})}));}}
  emit(event){this.onmessage({data:JSON.stringify({type:'event',event})});}
  close(){this.readyState=3;this.onclose?.({code:1000,reason:'test'});}
}

async function harness(run){
  const oldAudio=globalThis.AudioContext,oldNavigator=globalThis.navigator;
  const contexts=[],nodes=[],states=[],notices=[],events=[];let holdResume=false,releaseResume;
  class Audio{
    state='running';sampleRate=16000;currentTime=0;destination={};
    constructor(){contexts.push(this);}
    resume(){if(holdResume)return new Promise(resolve=>{releaseResume=()=>{this.state='running';resolve();};});this.state='running';return Promise.resolve();}
    createBuffer(_channels,length,rate){const data=new Float32Array(length);return {duration:length/rate,getChannelData:()=>data};}
    createBufferSource(){const node={connect(){},disconnect(){},start(){node.started=true;},stop(){node.cancelled=true;}};nodes.push(node);return node;}
  }
  globalThis.AudioContext=Audio;
  Object.defineProperty(globalThis,'navigator',{value:{mediaDevices:{}},configurable:true});
  const capture={running:true,setOnFrame(){},stop(){this.running=false;}};
  const client=createLiveClient({transport:'wss',WebSocketImpl:FakeSocket,manualActivityDetection:true,
    request:async url=>url==='/live'?{session_id:'one',model:'gemini-3.8-live',transport_protocol:'wl-live-v1',socket_ticket:'abcdefghijklmnopqrstuvwxyz',socket_url:'wss://example.test/live/socket'}:{ok:true},
    onState:state=>states.push(state),onNotice:kind=>notices.push(kind),onEvent:event=>events.push(event)});
  try{
    await client.start({url:'/live',takeMicrophoneHandoff:async()=>({capture,frames:[]})});
    await run({client,capture,nodes,states,notices,events,emit:event=>FakeSocket.instance.emit(event),hold:()=>{holdResume=true;contexts[0].state='suspended';},release:()=>releaseResume?.()});
  }finally{client.stop();releaseResume?.();if(oldAudio===undefined)delete globalThis.AudioContext;else globalThis.AudioContext=oldAudio;if(oldNavigator===undefined)delete globalThis.navigator;else Object.defineProperty(globalThis,'navigator',{value:oldNavigator,configurable:true});}
}
const audio=seq=>({seq,type:'audio',data:Buffer.alloc(4800).toString('base64'),mime_type:'audio/pcm;rate=24000'});

test('WSS interruption invalidates audio waiting for AudioContext resume',async()=>harness(async h=>{
  h.hold();h.emit(audio(1));await tick();h.emit({seq:2,type:'interrupted'});h.release();await drain();
  assert.equal(h.nodes.length,0,'cancelled pending playback must not resurrect');
}));

test('reply returns to listening only after its final queued audio ends',async()=>harness(async h=>{
  h.emit(audio(1));await drain();h.emit({seq:2,type:'turn_complete'});await drain();
  assert.equal(h.states.at(-1),'answering');
  h.nodes[0].onended();
  assert.equal(h.states.at(-1),'listening');
  assert.equal(h.capture.running,true);
}));

test('turn_complete received while audio resume is pending does not switch to listening early',async()=>harness(async h=>{
  h.hold();h.emit(audio(1));await tick();h.emit({seq:2,type:'turn_complete'});await tick();
  assert.equal(h.states.at(-1),'answering');
  h.release();await drain();assert.equal(h.nodes.length,1);assert.equal(h.states.at(-1),'answering');
  h.nodes[0].onended();assert.equal(h.states.at(-1),'listening');
}));

test('explicit Stop during pending playback remains immediate and late resume cannot revive it',async()=>harness(async h=>{
  h.hold();h.emit(audio(1));await tick();h.client.stop();h.release();await drain();
  assert.equal(h.nodes.length,0);assert.equal(h.client.sessionId,null);assert.equal(h.states.at(-1),'off');
}));

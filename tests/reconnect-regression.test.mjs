import test from 'node:test';
import assert from 'node:assert/strict';
import {createLiveClient,liveJson} from '../browser/client.js';
import {createLiveSocketTransport} from '../browser/socket-transport.js';
const tick=()=>new Promise(resolve=>setImmediate(resolve));
const ticket='abcdefghijklmnopqrstuvwxyz';
class Peer {
  static OPEN=1;static instances=[];
  readyState=0;bufferedAmount=0;sent=[];closed=[];failSend=false;
  constructor(){Peer.instances.push(this);queueMicrotask(()=>{this.readyState=1;this.onopen?.();});}
  send(value){
    if(this.failSend)throw new Error('underlying socket send failed');
    this.sent.push(value);
    if(typeof value==='string'){
      const m=JSON.parse(value);
      if(m.type==='hello')queueMicrotask(()=>this.onmessage?.({data:JSON.stringify({type:'hello_ack',protocol:'wl-live-v1',connection_generation:m.connection_generation})}));
    }
  }
  // Closing handshake deliberately delayed, like a browser/network close race.
  close(code,reason){this.readyState=3;this.closed.push({code,reason});}
  finishClose(){this.onclose?.({code:1006,reason:'network gap'});}
}
function fixture({request:override,persistAudio=null}={}){
  const original={navigator:globalThis.navigator,AudioContext:globalThis.AudioContext,AudioWorkletNode:globalThis.AudioWorkletNode};
  const worklets=[],tracks=[],requests=[],states=[],notices=[],timings=[];
  Peer.instances=[];
  Object.defineProperty(globalThis,'navigator',{configurable:true,value:{mediaDevices:{getUserMedia:async()=>{
    const track={stopped:false,stop(){this.stopped=true;}};tracks.push(track);return {getTracks:()=>[track]};
  }}}});
  globalThis.AudioContext=class{state='running';sampleRate=16000;destination={};audioWorklet={addModule:async()=>{}};async resume(){}async close(){}createMediaStreamSource(){return {connect(){},disconnect(){}};}};
  globalThis.AudioWorkletNode=class{constructor(){this.port={};worklets.push(this);}connect(){}disconnect(){}};
  const client=createLiveClient({transport:'wss',WebSocketImpl:Peer,continuousCapture:true,persistAudio,
    request:async(url,options)=>{requests.push({url,options});if(override){const v=override(url,options);if(v!==undefined)return v;}
      if(url==='/live')return {session_id:'same',model:'gemini-3.8-live',transport_protocol:'wl-live-v1',socket_ticket:ticket,socket_url:'/socket'};
      if(url.endsWith('/socket-ticket'))return {socket_ticket:ticket,socket_url:'/socket'};
      return {ok:true};},onState:(state,detail)=>states.push({state,detail}),onNotice:(kind,error)=>notices.push({kind,error}),onTiming:(event,detail)=>timings.push({event,detail})});
  const frame=(index=worklets.length-1)=>worklets[index].port.onmessage?.({data:new Float32Array(640).fill(.1)});
  return {client,requests,states,notices,timings,tracks,frame,async clean(){client.stop();for(const peer of Peer.instances)peer.finishClose();await tick();for(const [k,v] of Object.entries(original))Object.defineProperty(globalThis,k,{configurable:true,writable:true,value:v});}};
}
for(const kind of ['send failure before close','backpressure before close','remote close with unacked audio'])test(kind+' renews one ticket without stopping the provider session or replaying PCM',async()=>{
  const f=fixture();
  try{
    await f.client.start({url:'/live'});const old=Peer.instances[0];
    if(kind==='send failure before close')old.failSend=true;
    if(kind==='backpressure before close')old.bufferedAmount=100000;
    f.frame();await tick();
    if(kind==='remote close with unacked audio'){old.readyState=3;old.finishClose();}
    for(let i=0;i<20&&!f.timings.some(x=>x.event==='socket_reconnect_ready');i++)await tick();
    assert.equal(f.client.sessionId,'same');
    assert.equal(f.requests.filter(x=>x.url==='/live').length,1);
    assert.equal(f.requests.filter(x=>x.url.endsWith('/socket-ticket')).length,1);
    assert.equal(f.requests.some(x=>x.url.endsWith('/stop')),false);
    assert.equal(f.notices.some(x=>x.kind==='transport_error'),false);
    assert.equal(Peer.instances.length,2);
    assert.equal(Peer.instances[1].sent.some(x=>typeof x!=='string'),false,'stale PCM is not replayed');
    assert.ok(f.tracks[0].stopped,'old microphone stopped before recovery');
    const next=Peer.instances[1];f.frame();await tick();
    const pcm=next.sent.find(x=>typeof x!=='string');assert.ok(pcm,'new microphone sends fresh PCM');
    old.finishClose();old.onerror?.(); // late close/error cannot damage fresh in-flight PCM
    next.onmessage({data:JSON.stringify({type:'audio_ack',seq:new DataView(pcm.buffer).getUint32(4,false)})});
    await tick();assert.equal(f.requests.filter(x=>x.url.endsWith('/socket-ticket')).length,1);
    assert.equal(f.client.sessionId,'same');
  }finally{await f.clean();}
});
test('Stop during ticket renewal stays off after late ticket response',async()=>{
  let resolveTicket;const f=fixture({request:url=>url.endsWith('/socket-ticket')?new Promise(resolve=>{resolveTicket=resolve;}):undefined});
  try{
    await f.client.start({url:'/live'});Peer.instances[0].failSend=true;f.frame();
    for(let i=0;i<20&&!resolveTicket;i++)await tick();assert.equal(typeof resolveTicket,'function');
    f.client.stop();const stateCount=f.states.length;
    resolveTicket({socket_ticket:ticket,socket_url:'/socket'});await tick();await tick();
    assert.equal(f.client.sessionId,null);assert.equal(Peer.instances.length,1);
    assert.equal(f.states.length,stateCount);assert.equal(f.states.at(-1).state,'off');
    assert.equal(f.requests.filter(x=>x.url.endsWith('/stop')).length,1);
  }finally{await f.clean();}
});
test('replaced socket close cannot reject new ACK promise or inject stale events',async()=>{
  Peer.instances=[];const events=[],errors=[];
  const transport=createLiveSocketTransport({WebSocketImpl:Peer,onEvent:e=>events.push(e),onError:e=>errors.push(e)});
  try{
    await transport.connect({url:'/socket',ticket,connection_generation:1});const old=Peer.instances[0];
    await transport.connect({url:'/socket',ticket,connection_generation:2});const next=Peer.instances[1];
    const sent=transport.send({pcm:new Int16Array([1,2])});let settled=false;sent.finally(()=>{settled=true;}).catch(()=>{});
    old.finishClose();old.onerror?.();old.onmessage({data:JSON.stringify({type:'event',event:{type:'error',code:'stale'}})});
    await tick();assert.equal(settled,false);assert.deepEqual(events,[]);assert.deepEqual(errors,[]);
    next.onmessage({data:JSON.stringify({type:'audio_ack',seq:1})});await sent;
    assert.equal(transport.open,true);
  }finally{transport.close();for(const peer of Peer.instances)peer.finishClose();}
});
test('liveJson preserves FastAPI and standard resource errors and retry interval',async()=>{
  const original=globalThis.fetch;
  try{for(const key of ['detail','error']){
    globalThis.fetch=async()=>({ok:false,status:503,json:async()=>({[key]:{message:'budget refused',code:'RESOURCE_TOKEN_BUDGET',retry_after_ms:60000,secret:'do not copy'}})});
    await assert.rejects(liveJson('/live'),e=>e.status===503&&e.code==='RESOURCE_TOKEN_BUDGET'&&e.retry_after_ms===60000&&e.message==='budget refused'&&!('secret' in e));
  }}finally{globalThis.fetch=original;}
});
test('resource denial during renewal terminates once with its typed retry, without transport recovery',async()=>{
  const denial=Object.assign(new Error('budget'),{code:'RESOURCE_TOKEN_BUDGET',retry_after_ms:60000});
  const f=fixture({request:url=>url.endsWith('/socket-ticket')?Promise.reject(denial):undefined});
  try{
    await f.client.start({url:'/live'});Peer.instances[0].failSend=true;f.frame();
    for(let i=0;i<20&&!f.notices.some(x=>x.kind==='resource_denial');i++)await tick();
    assert.equal(f.requests.filter(x=>x.url.endsWith('/socket-ticket')).length,1);
    assert.equal(f.client.sessionId,null);
    assert.equal(f.states.at(-1).detail.reason,'resource_denial');
    assert.equal(f.notices.at(-1).error.retry_after_ms,60000);
    assert.equal(f.notices.some(x=>x.kind==='transport_error'),false);
  }finally{await f.clean();}
});
test('terminal provider resource event never renews a socket',async()=>{
  const f=fixture();
  try{
    await f.client.start({url:'/live',microphone:false});
    // A terminal provider/resource error must not enter reconnect, either.
    Peer.instances[0].onmessage({data:JSON.stringify({type:'event',event:{type:'error',code:'RESOURCE_TOKEN_BUDGET',retry_after_ms:60000}})});
    await tick();assert.equal(f.client.sessionId,null);
    assert.equal(f.requests.some(x=>x.url.endsWith('/socket-ticket')),false);
    assert.equal(f.notices.at(-1).kind,'resource_denial');assert.equal(f.notices.at(-1).error.retry_after_ms,60000);
  }finally{await f.clean();}
});

test('durability failure is not retried as a transport gap',async()=>{
  const f=fixture({persistAudio:async()=>{throw Object.assign(new Error('local persistence failed'),{code:'LIVE_AUDIO_DURABLE_BYTES'});}});
  try{
    await f.client.start({url:'/live'});f.frame();
    for(let i=0;i<20&&f.client.sessionId;i++)await tick();
    assert.equal(f.client.sessionId,null);
    assert.equal(f.requests.some(x=>x.url.endsWith('/socket-ticket')),false);
    assert.equal(f.notices.at(-1).kind,'transport_error');
    assert.equal(f.notices.at(-1).error.code,'LIVE_AUDIO_DURABLE_BYTES');
  }finally{await f.clean();}
});

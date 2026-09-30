import test from 'node:test';
import assert from 'node:assert/strict';
import {createLiveSocketTransport,LIVE_SOCKET_PROTOCOL} from '../browser/socket-transport.js';

const tick=()=>new Promise(resolve=>setImmediate(resolve));

class FakeWebSocket {
  static OPEN=1;
  static instances=[];
  constructor(url,protocols){
    this.url=url;this.protocols=protocols;this.readyState=0;this.bufferedAmount=0;this.sent=[];this.closed=[];
    FakeWebSocket.instances.push(this);
    queueMicrotask(()=>{this.readyState=FakeWebSocket.OPEN;this.onopen?.();});
  }
  send(value){this.sent.push(value);}
  close(code=1000,reason=''){this.closed.push({code,reason});this.readyState=3;this.onclose?.({code,reason});}
}

test('browser WSS start waits for matching versioned hello acknowledgement',async()=>{
  FakeWebSocket.instances=[];
  const timing=[];
  const transport=createLiveSocketTransport({WebSocketImpl:FakeWebSocket,onTiming:(event,data)=>timing.push({event,...data}),helloTimeoutMs:1000});
  let resolved=false;
  const connecting=transport.connect({url:'https://example.test/live/socket',ticket:'abcdefghijklmnopqrstuvwxyz',attempt_id:'attempt_12345678',cursor:0,connection_generation:1}).then(()=>{resolved=true;});
  await tick();await tick();
  const ws=FakeWebSocket.instances[0];
  assert.equal(resolved,false,'TCP/WebSocket open alone is not protocol readiness');
  assert.deepEqual(ws.protocols,[LIVE_SOCKET_PROTOCOL,'wl-ticket.abcdefghijklmnopqrstuvwxyz']);
  const hello=JSON.parse(ws.sent[0]);assert.equal(hello.type,'hello');assert.equal(hello.protocol,LIVE_SOCKET_PROTOCOL);
  ws.onmessage({data:JSON.stringify({type:'hello_ack',protocol:LIVE_SOCKET_PROTOCOL,connection_generation:1})});
  await connecting;
  assert.equal(resolved,true);assert.equal(transport.open,true);
  assert.ok(timing.some(item=>item.event==='socket_hello_ack'));
  transport.close();
});

test('browser WSS rejects incompatible hello acknowledgement before media starts',async()=>{
  FakeWebSocket.instances=[];
  const errors=[];
  const transport=createLiveSocketTransport({WebSocketImpl:FakeWebSocket,onError:error=>errors.push(error),helloTimeoutMs:1000});
  const connecting=transport.connect({url:'https://example.test/live/socket',ticket:'abcdefghijklmnopqrstuvwxyz',attempt_id:'attempt_12345678',cursor:0,connection_generation:3});
  await tick();await tick();
  const ws=FakeWebSocket.instances[0];
  ws.onmessage({data:JSON.stringify({type:'hello_ack',protocol:'wl-live-v0',connection_generation:3})});
  await assert.rejects(connecting,error=>error.code==='LIVE_SOCKET_PROTOCOL');
  assert.equal(ws.closed[0]?.code,1002);
  assert.equal(errors.at(-1)?.code,'LIVE_SOCKET_PROTOCOL');
});


test('browser WSS audio send waits for server acknowledgement before resolving',async()=>{
  FakeWebSocket.instances=[];
  const transport=createLiveSocketTransport({WebSocketImpl:FakeWebSocket,helloTimeoutMs:1000,maxUnackedAgeMs:1000});
  const connecting=transport.connect({url:'https://example.test/live/socket',ticket:'abcdefghijklmnopqrstuvwxyz',attempt_id:'attempt_12345678',cursor:0,connection_generation:1});
  await tick();await tick();
  const ws=FakeWebSocket.instances[0];
  ws.onmessage({data:JSON.stringify({type:'hello_ack',protocol:LIVE_SOCKET_PROTOCOL,connection_generation:1})});
  await connecting;
  let resolved=false;
  const sending=transport.send({pcm:new Int16Array(200),age_ms:7}).then(()=>{resolved=true;});
  await tick();
  assert.equal(resolved,false,'audio sender must not outrun server ACK');
  const frame=ws.sent.find(value=>value instanceof Uint8Array);
  const seq=new DataView(frame.buffer,frame.byteOffset,frame.byteLength).getUint32(4,false);
  assert.equal(transport.metrics().unacked_audio_frames,1);
  ws.onmessage({data:JSON.stringify({type:'audio_ack',seq,server_received_at:Date.now()})});
  await sending;
  assert.equal(resolved,true);
  assert.equal(transport.metrics().unacked_audio_frames,0);
  transport.close();
});

test('browser WSS audio ACK timeout exposes bounded backpressure metrics',async()=>{
  FakeWebSocket.instances=[];
  let now=0;
  const transport=createLiveSocketTransport({WebSocketImpl:FakeWebSocket,helloTimeoutMs:1000,maxUnackedAgeMs:10,now:()=>now});
  const connecting=transport.connect({url:'https://example.test/live/socket',ticket:'abcdefghijklmnopqrstuvwxyz',attempt_id:'attempt_12345678',cursor:0,connection_generation:1});
  await tick();await tick();
  const ws=FakeWebSocket.instances[0];
  ws.onmessage({data:JSON.stringify({type:'hello_ack',protocol:LIVE_SOCKET_PROTOCOL,connection_generation:1})});
  await connecting;
  const sending=transport.send({pcm:new Int16Array(100),age_ms:0});
  now=12;
  await assert.rejects(sending,error=>error.code==='LIVE_SOCKET_BACKPRESSURE'&&error.metrics?.unacked_audio_frames===1);
  transport.close();
});

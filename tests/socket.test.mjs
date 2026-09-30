import test from 'node:test';
import assert from 'node:assert/strict';
import {EventEmitter} from 'node:events';
import {PassThrough} from 'node:stream';
import {createLiveSessionHost} from '../node/sessions.mjs';
import {encodeLiveAudioFrame} from '../browser/socket-transport.js';

const tick=()=>new Promise(resolve=>setImmediate(resolve));
function worker(writes=[]){
  const c=new EventEmitter();c.stdin=new PassThrough();c.stdout=new PassThrough();c.stderr=new PassThrough();c.kill=()=>c.emit('close',0);
  c.stdin.on('data',buf=>{for(const line of buf.toString().split('\n').filter(Boolean)){const msg=JSON.parse(line);writes.push(msg);if(msg.type==='start')queueMicrotask(()=>c.stdout.write('{"type":"ready"}\n'));}});
  return c;
}
test('binary browser frame carries sequence, age and raw PCM without base64 expansion',()=>{
  const pcm=new Int16Array([1,-2,32767,-32768]),frame=encodeLiveAudioFrame(pcm,{seq:17,age_ms:83}),view=new DataView(frame.buffer,frame.byteOffset,frame.byteLength);
  assert.equal(frame.byteLength,20);assert.equal(view.getUint32(4,false),17);assert.equal(view.getUint32(8,false),83);
  assert.deepEqual([...frame.slice(12)],[1,0,254,255,255,127,0,128]);
});
test('socket ticket is single-use, pushes events and blocks tools from a transport-damaged turn',async()=>{
  const writes=[],calls=[],c=worker(writes);
  const host=createLiveSessionHost({createWorker:()=>c,adapterFactory:()=>({
    initialize:()=>({state:{},context:{},configuration:{functions:[{name:'mutate'}]}}),
    executeTool:async(_session,call)=>{calls.push(call.id);return {ok:true};}
  })});
  const actor={subject:'a',tenant_id:'t'},started=await host.start({resourceId:'r',actor,attemptId:'attempt_12345678'});
  const observed=[],socket=host.openSocket({sessionId:started.session_id,ticket:started.socket_ticket});
  const unsubscribe=socket.subscribe(event=>observed.push(event));
  assert.throws(()=>host.openSocket({sessionId:started.session_id,ticket:started.socket_ticket}),{code:'LIVE_SOCKET_TICKET'});
  socket.transportConnected({connection_generation:1});
  socket.input({audio_base64:'AAAA'},{frame_seq:1,capture_age_ms:20,connection_generation:1});
  socket.transportGap({connection_generation:1,reason:'test_drop'});
  c.stdout.write(JSON.stringify({type:'tool_call',calls:[{name:'mutate',id:'danger'}]})+'\n');
  for(let i=0;i<20&&!observed.some(event=>event.id==='danger');i++)await tick();
  assert.deepEqual(calls,[]);
  assert.ok(observed.some(event=>event.type==='transport_gap'&&event.audio_turn_open===true));
  assert.ok(observed.some(event=>event.type==='tool_result'&&event.id==='danger'&&event.code==='LIVE_INPUT_DAMAGED'));
  const replacement=host.issueSocketTicket({resourceId:'r',sessionId:started.session_id,actor});
  assert.match(replacement.socket_ticket,/^[A-Za-z0-9_-]+$/);
  unsubscribe();await host.stop({resourceId:'r',sessionId:started.session_id,actor});
});

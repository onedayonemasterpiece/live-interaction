import test from 'node:test';
import assert from 'node:assert/strict';
import {EventEmitter} from 'node:events';
import {PassThrough} from 'node:stream';
import {createLiveSessionHost} from '../node/sessions.mjs';
import {createLiveWebSocketUpgrade} from '../node/websocket.mjs';
import {encodeLiveAudioFrame} from '../browser/socket-transport.js';

const tick=()=>new Promise(resolve=>setImmediate(resolve));
class FakeSocket extends EventEmitter{
  writes=[];destroyed=false;writableLength=0;
  write(value){this.writes.push(Buffer.from(value));return true;}
  end(){this.emit('close');}
  destroy(){this.destroyed=true;this.emit('close');}
  setNoDelay(){}
}
function clientFrame(payload,{opcode=1,fin=true}={}){
  const body=Buffer.isBuffer(payload)?payload:Buffer.from(payload),mask=Buffer.from([1,2,3,4]);
  if(body.length>=126)throw new Error('fixture too large');
  const out=Buffer.alloc(2+4+body.length);out[0]=(fin?0x80:0)|opcode;out[1]=0x80|body.length;mask.copy(out,2);
  for(let i=0;i<body.length;i++)out[6+i]=body[i]^mask[i&3];
  return out;
}
const request=({origin='https://example.test',protocols='wl-live-v1, wl-ticket.abcdefghijklmnopqrstuvwxyz'}={})=>({
  url:'/live/session/socket',
  headers:{
    host:'example.test',origin,
    'sec-websocket-key':Buffer.alloc(16,7).toString('base64'),
    'sec-websocket-protocol':protocols
  }
});
function countText(socket,needle){return Buffer.concat(socket.writes).toString('utf8').split(needle).length-1;}
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
test('startup catchup bit shares the age field without changing frame size',()=>{
  const pcm=new Int16Array([7,8]),frame=encodeLiveAudioFrame(pcm,{seq:3,age_ms:9000,startup_catchup:true}),view=new DataView(frame.buffer,frame.byteOffset,frame.byteLength);
  assert.equal(frame.byteLength,16);
  assert.equal(view.getUint32(4,false),3);
  assert.equal(view.getUint32(8,false),0x80000000+9000);
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
test('upgrade rejects cross-origin browser sockets before consuming a ticket',async()=>{
  let opened=0;const socket=new FakeSocket();
  const upgrade=createLiveWebSocketUpgrade({
    host:{openSocket(){opened++;throw new Error('must not run');}},
    matchPath:()=>({sessionId:'session'})
  });
  assert.equal(await upgrade(request({origin:'https://evil.test'}),socket),true);
  assert.equal(opened,0);assert.match(Buffer.concat(socket.writes).toString('utf8'),/403 Forbidden/);
});
test('upgrade replays backlog once, accepts fragmented hello and does not duplicate subscribed events',async()=>{
  const socket=new FakeSocket(),event={seq:1,type:'configuration_ready'};
  let listener=null,connected=0;
  const binding={
    attemptId:'attempt_12345678',
    subscribe(fn){listener=fn;fn(event);return()=>{listener=null;};},
    events(after){return {events:after<1?[event]:[],cursor:1,has_more:false,gap:false,closed:false};},
    transportConnected(){connected++;},
    input(){},transportGap(){},close(){},async stop(){}
  };
  const upgrade=createLiveWebSocketUpgrade({
    host:{openSocket(){return binding;}},
    matchPath:()=>({sessionId:'session'})
  });
  assert.equal(await upgrade(request(),socket),true);
  const hello=Buffer.from(JSON.stringify({type:'hello',protocol:'wl-live-v1',attempt_id:'attempt_12345678',cursor:0,connection_generation:1}));
  const cut=Math.floor(hello.length/2);
  socket.emit('data',clientFrame(hello.subarray(0,cut),{opcode:1,fin:false}));
  socket.emit('data',clientFrame(hello.subarray(cut),{opcode:0,fin:true}));
  for(let i=0;i<20&&!countText(socket,'"type":"event"');i++)await tick();
  assert.equal(connected,1);
  assert.equal(countText(socket,'"type":"event"'),1);
  assert.equal(countText(socket,'"type":"hello_ack"'),1);
  socket.emit('close');assert.equal(listener,null);
});

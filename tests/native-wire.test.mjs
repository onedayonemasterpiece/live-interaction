import test from 'node:test';
import assert from 'node:assert/strict';
import {EventEmitter} from 'node:events';
import {PassThrough} from 'node:stream';
import {createLiveSessionHost} from '../node/sessions.mjs';
import {encodeLiveAudioFrame,decodeLiveOutputFrame} from '../browser/socket-transport.js';

test('shared Python/Java/browser PCM conformance vector',()=>{
 const input=encodeLiveAudioFrame(new Int16Array([1,-2,32767,-32768]),{seq:42,age_ms:125});
 assert.equal(Buffer.from(input).toString('hex'),'574c41310000002a0000007d0100feffff7f0080');
 const output=decodeLiveOutputFrame(Buffer.from('574c4f310000002a00005dc00100feffff7f0080','hex'));
 assert.equal(output.seq,42);assert.equal(output.rate,24000);
 assert.equal(Buffer.from(output.pcm).toString('hex'),'0100feffff7f0080');
});

test('Node host preserves native manual activity boundaries',async()=>{
 const child=new EventEmitter(),writes=[];
 child.stdin=new PassThrough();child.stdout=new PassThrough();child.stderr=new PassThrough();
 child.kill=()=>child.emit('close',0);
 child.stdin.on('data',buffer=>{for(const line of buffer.toString().split('\n').filter(Boolean)){
  const value=JSON.parse(line);writes.push(value);
  if(value.type==='start')queueMicrotask(()=>child.stdout.write('{"type":"ready"}\n'));
 }});
 const host=createLiveSessionHost({createWorker:()=>child,adapterFactory:()=>({
  initialize:()=>({state:{},context:{},configuration:{manual_activity_detection:true,functions:[]}}),executeTool:async()=>({})
 })});
 const actor={subject:'author',tenant_id:'tenant'},started=await host.start({resourceId:'native',actor});
 const base={resourceId:'native',actor,sessionId:started.session_id};
 try{
  assert.throws(()=>host.input({...base,message:{audio_base64:'AAAA'}}),{code:'INVALID_ARGUMENT'});
  host.input({...base,message:{activity_start:true}});
  assert.throws(()=>host.input({...base,message:{activity_start:true}}),{code:'INVALID_ARGUMENT'});
  host.input({...base,message:{audio_base64:'AAAA'}});
  assert.throws(()=>host.input({...base,message:{audio_stream_end:true}}),{code:'INVALID_ARGUMENT'});
  host.input({...base,message:{activity_end:true}});
  assert.deepEqual(writes.slice(-3).map(x=>x.type),['activity_start','audio','activity_end']);
  assert.throws(()=>host.input({...base,message:{activity_end:true}}),{code:'INVALID_ARGUMENT'});
 }finally{await host.stop(base);}
});

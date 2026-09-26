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

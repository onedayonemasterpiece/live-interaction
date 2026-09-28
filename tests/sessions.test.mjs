import test from 'node:test';
import assert from 'node:assert/strict';
import {EventEmitter} from 'node:events';
import {PassThrough} from 'node:stream';
import {createLiveSessionHost,withLiveToolParts} from '../node/sessions.mjs';
const tick=()=>new Promise(r=>setImmediate(r));
function worker(){const c=new EventEmitter();c.stdin=new PassThrough();c.stdout=new PassThrough();c.stderr=new PassThrough();c.kill=()=>c.emit('close',0);c.stdin.on('data',b=>{if(JSON.parse(b).type==='start')queueMicrotask(()=>c.stdout.write('{"type":"ready"}\n'));});return c;}
test('trusted event observer receives worker failures without browser polling',async()=>{
 const c=worker(),observed=[];
 const host=createLiveSessionHost({createWorker:()=>c,adapterFactory:()=>({
  initialize:()=>({state:{},context:{},configuration:{functions:[]}}),
  executeTool:async()=>({}),onEvent:(_session,event)=>observed.push({type:event.type,code:event.code})
 })});
 const actor={subject:'a',tenant_id:'t'},started=await host.start({resourceId:'observed',actor});
 c.stdout.write('{"type":"error","code":"PROVIDER_TEST"}\n');
 await tick();
 assert.ok(observed.some(event=>event.type==='error'&&event.code==='PROVIDER_TEST'));
 await host.stop({resourceId:'observed',sessionId:started.session_id,actor});
});
test('independent product adapters preserve ordered tools, deduplication, owner isolation and cancel-before-start',async()=>{
 const c=worker(),called=[];const host=createLiveSessionHost({createWorker:()=>c,adapterFactory:()=>({initialize:({resourceId})=>({state:{},context:{resourceId},configuration:{functions:[{name:'story.read'}]}}),executeTool:async(_,call)=>{called.push(call.id);return {value:'story'};}})});
 const start=await host.start({resourceId:'story1',actor:{subject:'a',tenant_id:'t'}}),base={resourceId:'story1',sessionId:start.session_id,actor:{subject:'a',tenant_id:'t'}};
 const send=e=>c.stdout.write(JSON.stringify(e)+'\n');
 send({type:'tool_call',calls:[{name:'story.read',id:'one'},{name:'story.read',id:'one'},{name:'story.write',id:'two'}]});send({type:'tool_cancelled',ids:['two']});await tick();
 assert.deepEqual(called,['one']);assert.throws(()=>host.input({...base,actor:{subject:'b',tenant_id:'t'},message:{text:'test'}}),{code:'FORBIDDEN'});
 assert.throws(()=>host.input({...base,message:{audio_base64:'a'.repeat(16001)}}),{code:'INVALID_ARGUMENT'});
 for(let n=0;n<140;n++)send({type:'audio',data:'AAAA'});c.emit('close',0);
 let page=host.events(base),audio=page.events.filter(e=>e.type==='audio').length;assert.equal(page.closed,false);
 while(page.has_more){page=host.events({...base,after:page.cursor});audio+=page.events.filter(e=>e.type==='audio').length;}
 assert.equal(audio,140);assert.equal(page.closed,true);await host.stop(base);
});

test('image returned by a tool stays in its multimodal FunctionResponse on first call and deduplication',async()=>{
 const c=worker(),writes=[];c.stdin.on('data',b=>{for(const line of b.toString().split('\n').filter(Boolean))writes.push(JSON.parse(line));});
 const image=Buffer.from('small-image-fixture').toString('base64');let calls=0;
 const host=createLiveSessionHost({createWorker:()=>c,adapterFactory:()=>({
  initialize:()=>({state:{},context:{},configuration:{functions:[{name:'inspect_image'}]}}),
  executeTool:async()=>{calls++;return withLiveToolParts({image:{$ref:'preview.jpg'},preview_delivered:true},[{inlineData:{mimeType:'image/jpeg',displayName:'preview.jpg',data:image}}]);}
 })});
 const actor={subject:'a',tenant_id:'t'},started=await host.start({resourceId:'r1',actor});
 const base={resourceId:'r1',sessionId:started.session_id,actor};
 const call=()=>c.stdout.write(JSON.stringify({type:'tool_call',calls:[{name:'inspect_image',id:'same-image'}]})+'\n');
 call();for(let i=0;i<30&&writes.filter(x=>x.type==='tool_response').length<1;i++)await tick();
 call();for(let i=0;i<30&&writes.filter(x=>x.type==='tool_response').length<2;i++)await tick();
 const responses=writes.filter(x=>x.type==='tool_response').map(x=>x.responses[0]);
 assert.equal(calls,1);
 assert.equal(responses.length,2);
 for(const response of responses){
  assert.deepEqual(response.response.image,{$ref:'preview.jpg'});
  assert.equal(response.response.preview_delivered,true);
  assert.equal(response.parts[0].inlineData.data,image);
  assert.equal(JSON.stringify(response.response).includes(image),false,'raw image is not placed inside text result');
 }
 assert.equal(JSON.stringify(host.events(base).events).includes(image),false,'browser events contain no image bytes');
 await host.stop(base);
});

test('capability router delegates safe acknowledgement and continuation to provider worker',async()=>{
 const c=new EventEmitter();c.stdin=new PassThrough();c.stdout=new PassThrough();c.stderr=new PassThrough();c.kill=()=>c.emit('close',0);
 const writes=[];
 c.stdin.on('data',b=>{
   for(const line of b.toString().split('\n').filter(Boolean)){
     const message=JSON.parse(line);writes.push(message);
     if(message.type==='start')queueMicrotask(()=>c.stdout.write('{"type":"ready"}\n'));
     if(message.type==='reconfigure')queueMicrotask(()=>{
       c.stdout.write(JSON.stringify({type:'capability_transition_started',transition_id:message.transition_id,capability:message.capability})+'\n');
       c.stdout.write(JSON.stringify({type:'capability_ready',transition_id:message.transition_id,capability:message.capability})+'\n');
     });
   }
 });
 const adapter={
   initialize:()=>({state:{},capability:'core',context:{},configuration:{functions:[{name:'activate_capability'},{name:'read'}]}}),
   resolveCapability:async(_,call)=>call.name==='activate_capability'?{
     capability:'dataset',
     configuration:{system_instruction:'dataset',functions:[{name:'activate_capability'},{name:'dataset.find'},{name:'dataset.read'}]},
     context:{scope:'dataset'},
     continuation:call.args.intent,
     response:{selected:'dataset'}
   }:null,
   executeTool:async()=>({ok:true})
 };
 const host=createLiveSessionHost({createWorker:()=>c,adapterFactory:()=>adapter,reconfigureTimeoutMs:1000});
 const started=await host.start({resourceId:'r1',actor:{subject:'a',tenant_id:'t'}});
 const base={resourceId:'r1',sessionId:started.session_id,actor:{subject:'a',tenant_id:'t'}};
 c.stdout.write(JSON.stringify({type:'tool_call',calls:[{name:'activate_capability',id:'cap1',args:{capability:'dataset',intent:'find national projects dataset'}}]})+'\n');
 for(let i=0;i<40&&!host.events(base).events.some(e=>e.type==='tool_result'&&e.id==='cap1');i++)await tick();
 const reconfigure=writes.find(x=>x.type==='reconfigure');
 assert.equal(reconfigure.capability,'dataset');
 assert.equal(reconfigure.configuration.functions.length,3);
 assert.equal(reconfigure.continuation,'find national projects dataset');
 assert.equal(reconfigure.router_response.id,'cap1');
 assert.equal(reconfigure.router_response.response.result.accepted,true);
 assert.equal(reconfigure.router_response.scheduling,'SILENT');
 assert.equal(reconfigure.router_response.willContinue,false);
 assert.equal(reconfigure.router_response.response.scheduling,undefined);
 assert.equal(writes.some(x=>x.type==='tool_response'),false,'provider worker owns router acknowledgement');
 const events=host.events(base).events;
 assert.ok(events.some(e=>e.type==='capability_transition_requested'&&e.to_capability==='dataset'));
 assert.ok(events.some(e=>e.type==='capability_ready'&&e.capability==='dataset'));
 assert.ok(events.some(e=>e.type==='tool_result'&&e.id==='cap1'&&e.status==='ok'));
 await host.stop(base);
});

test('capability bundles reject more than nine functions without reconfiguring provider',async()=>{
 const c=worker(),writes=[];c.stdin.on('data',b=>{for(const line of b.toString().split('\n').filter(Boolean))writes.push(JSON.parse(line));});
 const adapter={
   initialize:()=>({state:{},context:{},configuration:{functions:[{name:'activate_capability'}]}}),
   resolveCapability:async()=>({capability:'too_big',configuration:{functions:Array.from({length:10},(_,i)=>({name:'f'+i}))}}),
   executeTool:async()=>({ok:true})
 };
 const host=createLiveSessionHost({createWorker:()=>c,adapterFactory:()=>adapter,reconfigureTimeoutMs:100});
 const started=await host.start({resourceId:'r2',actor:{subject:'a',tenant_id:'t'}});
 c.stdout.write(JSON.stringify({type:'tool_call',calls:[{name:'activate_capability',id:'cap2',args:{}}]})+'\n');
 for(let i=0;i<20&&!writes.some(x=>x.type==='tool_response');i++)await tick();
 assert.equal(writes.some(x=>x.type==='reconfigure'),false);
 const response=writes.find(x=>x.type==='tool_response');
 assert.equal(response.responses[0].response.error.code,'LIVE_CAPABILITY_LIMIT');
 await host.stop({resourceId:'r2',sessionId:started.session_id,actor:{subject:'a',tenant_id:'t'}});
});

test('timed-out capability switch closes worker and ignores a late ready',async()=>{
 const c=worker();let kills=0,transitionId;
 c.kill=()=>{kills++;c.emit('close',0);};
 c.stdin.on('data',chunk=>{for(const line of chunk.toString().split('\n').filter(Boolean)){
  const message=JSON.parse(line);if(message.type==='reconfigure')transitionId=message.transition_id;
 }});
 const actor={subject:'a',tenant_id:'t'};
 const host=createLiveSessionHost({createWorker:()=>c,reconfigureTimeoutMs:20,adapterFactory:()=>({
  initialize:()=>({state:{},context:{},configuration:{functions:[{name:'activate_capability'}]}}),
  resolveCapability:()=>({capability:'dataset',configuration:{functions:[{name:'activate_capability'},{name:'dataset.find'}]}}),
  executeTool:async()=>({ok:true})
 })});
 const started=await host.start({resourceId:'r3',actor});
 const base={resourceId:'r3',sessionId:started.session_id,actor};
 c.stdout.write(JSON.stringify({type:'tool_call',calls:[{name:'activate_capability',id:'late',args:{intent:'read dataset'}}]})+'\n');
 await new Promise(resolve=>setTimeout(resolve,45));
 assert.equal(kills,1);
 assert.ok(host.events(base).events.some(e=>e.type==='capability_transition_failed'&&e.code==='LIVE_CAPABILITY_TIMEOUT'));
 c.stdout.write(JSON.stringify({type:'capability_ready',transition_id:transitionId,capability:'dataset'})+'\n');
 await tick();
 const events=host.events(base).events;
 assert.equal(events.some(e=>e.type==='capability_ready'),false);
 assert.equal(events.some(e=>e.type==='tool_result'&&e.id==='late'&&e.status==='ok'),false);
 assert.throws(()=>host.input({...base,message:{text:'continue'}}),{code:'LIVE_SESSION_CLOSED'});
 await host.stop(base);
});

test('resource denial during capability setup remains the transition failure code',async()=>{
 const c=worker(),actor={subject:'a',tenant_id:'t'};
 const host=createLiveSessionHost({createWorker:()=>c,reconfigureTimeoutMs:500,adapterFactory:()=>({
  initialize:()=>({state:{},context:{},configuration:{functions:[{name:'activate_capability'}]}}),
  resolveCapability:()=>({capability:'media',configuration:{functions:[{name:'activate_capability'},{name:'open_media_chooser'}]}}),
  executeTool:async()=>({ok:true})
 })});
 const started=await host.start({resourceId:'denial',actor});
 const base={resourceId:'denial',sessionId:started.session_id,actor};
 c.stdout.write(JSON.stringify({type:'tool_call',calls:[{name:'activate_capability',id:'denied',args:{intent:'choose image'}}]})+'\n');
 await tick();
 c.stdout.write('{"type":"error","code":"RESOURCE_TOKEN_BUDGET","message":"RESOURCE_TOKEN_BUDGET"}\n');
 for(let i=0;i<20&&!host.events(base).events.some(e=>e.type==='capability_transition_failed');i++)await tick();
 assert.ok(host.events(base).events.some(e=>e.type==='capability_transition_failed'&&e.code==='RESOURCE_TOKEN_BUDGET'));
 assert.equal(host.events(base).events.some(e=>e.type==='tool_result'&&e.id==='denied'&&e.status==='ok'),false);
 await host.stop(base);
});

test('Stop remains immediate during a pending capability switch',async()=>{
 const c=worker();const actor={subject:'a',tenant_id:'t'};
 const host=createLiveSessionHost({createWorker:()=>c,reconfigureTimeoutMs:1000,adapterFactory:()=>({
  initialize:()=>({state:{},context:{},configuration:{functions:[{name:'activate_capability'}]}}),
  resolveCapability:()=>({capability:'media',configuration:{functions:[{name:'activate_capability'},{name:'open_media_chooser'}]}}),
  executeTool:async()=>({ok:true})
 })});
 const started=await host.start({resourceId:'r4',actor});
 const base={resourceId:'r4',sessionId:started.session_id,actor};
 c.stdout.write(JSON.stringify({type:'tool_call',calls:[{name:'activate_capability',id:'stopped',args:{intent:'choose image'}}]})+'\n');
 await tick();
 const before=Date.now();await host.stop(base);
 assert.ok(Date.now()-before<100);
 assert.equal(host.size(),0);
});

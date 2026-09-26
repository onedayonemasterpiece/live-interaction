import test from 'node:test';
import assert from 'node:assert/strict';
import {EventEmitter} from 'node:events';
import {PassThrough} from 'node:stream';
import {createLiveSessionHost} from '../node/sessions.mjs';
const tick=()=>new Promise(r=>setImmediate(r));
function worker(){const c=new EventEmitter();c.stdin=new PassThrough();c.stdout=new PassThrough();c.stderr=new PassThrough();c.kill=()=>c.emit('close',0);c.stdin.on('data',b=>{if(JSON.parse(b).type==='start')queueMicrotask(()=>c.stdout.write('{"type":"ready"}\n'));});return c;}
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

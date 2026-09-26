import {randomUUID} from 'node:crypto';
export const LIVE_SESSION_MODELS=Object.freeze(['gemini-3.8-live','gemini-3.8-live-extended-thinking']);
export class LiveError extends Error {constructor(code,message){super(message);this.code=code;}}
const trimText=(value,max=1200)=>typeof value==='string'?value.slice(0,max):value;

// The host owns transport, ordering and event cursors; adapters own domain policy.
export function createLiveSessionHost({adapterFactory,createWorker,models=LIVE_SESSION_MODELS,ErrorClass=LiveError,readyTimeoutMs=30000,maxSessions=2}={}){
  const DomainError=ErrorClass,sessions=new Map();
  const emit=(session,event)=>{
    session.events.push({seq:session.nextSeq++,at:new Date().toISOString(),...event});
    while(session.events.length>320)session.events.shift();
  };
  const timing=(session,stage,started)=>emit(session,{type:'timing',stage,duration_ms:Date.now()-started});
  const measure=async(session,stage,fn)=>{const at=Date.now();try{return await fn();}finally{timing(session,stage,at);}};
  const write=(session,message)=>{
    if(session.closed||!session.child?.stdin?.writable)throw new DomainError('LIVE_SESSION_CLOSED','Live-сессия закрыта');
    if(session.child.stdin.writableLength>768*1024)throw new DomainError('LIVE_BACKPRESSURE','Live provider input is not keeping up');
    const queuedAt=Date.now();
    session.child.stdin.write(`${JSON.stringify({...message,queued_at:queuedAt})}\n`);
    return queuedAt;
  };
  const handleToolCalls=async(session,calls)=>{
    const responses=[];
    for(const call of Array.isArray(calls)?calls:[]){
      if(session.closed)return;
      if(session.cancelled.has(call.id))continue;
      const toolAt=Date.now();
      try{
        const result=session.toolResults.has(call.id)?session.toolResults.get(call.id):await adapter.executeTool(session,call);
        session.toolResults.set(call.id,result);while(session.toolResults.size>100)session.toolResults.delete(session.toolResults.keys().next().value);
        emit(session,{type:'tool_result',name:call.name,id:call.id,status:'ok',duration_ms:Date.now()-toolAt,revision:result?.revision??result?.result_revision??null});
        responses.push({name:call.name,id:call.id,response:{result}});
      }catch(error){
        emit(session,{type:'tool_result',name:call?.name,id:call?.id,status:'error',code:error.code??'LIVE_TOOL_ERROR',message:trimText(error.message,240),change_shapes:Array.isArray(call?.args?.changes)?call.args.changes.slice(0,16).map(c=>({kind:c?.kind,keys:Object.keys(c??{})})):undefined});
        responses.push({name:call?.name??'unknown',id:call?.id,response:{error:{code:error.code??'LIVE_TOOL_ERROR',message:String(error.message??error).slice(0,500)}}});
      }
    }
    if(responses.length&&!session.closed){session.toolResponseAt=write(session,{type:'tool_response',responses});session.awaitingAudio=true;emit(session,{type:'timing',stage:'tool_response_written'});}
  };
  const adapter=adapterFactory({emit,write,measure,timing});
  const start=async({resourceId,actor,model=models[0],history=[],...args}={})=>{
    if(typeof resourceId!=='string'||!resourceId||resourceId.length>240)throw new DomainError('INVALID_ARGUMENT','resourceId is required');
    if(!models.includes(model))throw new DomainError('INVALID_INPUT','Unknown Live model');
    if(sessions.size>=maxSessions)throw new DomainError('LIVE_BUSY','Live session limit reached');
    const initialized=adapter.initialize({resourceId,actor,model,...args});
    const id=`live_${randomUUID().replaceAll('-','')}`;
    const child=createWorker({model,actor,resourceId});
    const session={...initialized.state,id,resourceId,actor,model,child,events:[],nextSeq:1,buffer:'',closed:false,toolChain:Promise.resolve(),cancelled:new Set(),toolResults:new Map()};
    sessions.set(id,session);
    let readyResolve,readyReject;
    const ready=new Promise((resolve,reject)=>{readyResolve=resolve;readyReject=reject;});
    const parseLine=line=>{
      if(!line.trim())return;
      let event;
      try{event=JSON.parse(line);}catch{return;}
      if(event.type==='ready'){emit(session,event);readyResolve(event);return;}
      if(event.type==='error'){emit(session,event);readyReject(new DomainError('LIVE_PROVIDER_ERROR',trimText(event.message??'Gemini Live error',500)));return;}
      if(event.type==='tool_cancelled'){for(const id of event.ids??[])session.cancelled.add(id);}
      if(event.type==='resumed'){adapter.onResumed?.(session);}
      if(event.type==='audio'&&session.awaitingAudio){timing(session,'first_audio_after_tool_response',session.toolResponseAt);session.awaitingAudio=false;}
      if(event.type==='tool_call'){
        emit(session,{type:'tool_call',provider_at:event.provider_at,calls:(event.calls??[]).map(call=>({name:call.name,id:call.id}))});
        session.toolChain=session.toolChain.then(()=>handleToolCalls(session,event.calls)).catch(error=>emit(session,{type:'error',code:error.code??'LIVE_TOOL_ERROR',message:trimText(error.message,500)}));
        return;
      }
      emit(session,event);
    };
    child.stdout.on('data',chunk=>{
      session.buffer+=chunk.toString('utf8');
      for(;;){const newline=session.buffer.indexOf('\n');if(newline<0)break;const line=session.buffer.slice(0,newline);session.buffer=session.buffer.slice(newline+1);parseLine(line);}
      if(session.buffer.length>1024*1024){session.buffer='';emit(session,{type:'error',code:'LIVE_EVENT_LIMIT',message:'Live worker output exceeded limit'});}
    });
    child.stderr.on('data',()=>{});
    child.on('error',()=>readyReject(new DomainError('LIVE_UNAVAILABLE','Не удалось запустить Live worker')));
    child.on('close',code=>{readyReject(new DomainError('LIVE_PROVIDER_CLOSED','Live provider closed during setup'));session.closed=true;emit(session,{type:'closed',code});});
    write(session,{type:'start',model,configuration:initialized.configuration,context:initialized.context,history:Array.isArray(history)?history.slice(-8).filter(m=>['user','model'].includes(m?.role)&&typeof m.text==='string').map(m=>({role:m.role,text:m.text.slice(-700)})):[]});
    let timer;
    try{
      await Promise.race([ready,new Promise((_,reject)=>{timer=setTimeout(()=>reject(new DomainError('LIVE_TIMEOUT','Gemini Live setup timeout')),readyTimeoutMs);})]);
    }catch(error){
      sessions.delete(id);try{child.kill('SIGKILL');}catch{}throw error;
    }finally{clearTimeout(timer);}
    adapter.onStarted?.(session);
    return {session_id:id,model,...initialized.response};
  };
  const getSession=(sessionId,resourceId,actor)=>{
    const session=sessions.get(sessionId);
    if(!session||session.resourceId!==resourceId)throw new DomainError('LIVE_SESSION_NOT_FOUND','Live-сессия не найдена');
    if(actor&&(actor.subject!==session.actor?.subject||actor.tenant_id!==session.actor?.tenant_id))throw new DomainError('FORBIDDEN','Live-сессия принадлежит другому пользователю');
    return session;
  };
  const input=({sessionId,resourceId,message,receivedAt=Date.now(),actor}={})=>{
    let writtenAt=null;
    const session=getSession(sessionId,resourceId,actor);
    adapter.input?.(session,message);
    if(message?.audio_base64!==undefined){
      if(typeof message.audio_base64!=='string'||message.audio_base64.length>16000)throw new DomainError('INVALID_ARGUMENT','Audio chunk is invalid');
      writtenAt=write(session,{type:'audio',data:message.audio_base64});
    }
    if(message?.audio_stream_end)write(session,{type:'audio_stream_end'});
    if(message?.text!==undefined){
      if(typeof message.text!=='string'||!message.text.trim()||message.text.length>4000)throw new DomainError('INVALID_ARGUMENT','Text turn is invalid');
      write(session,{type:'text',text:message.text.trim()});
    }
    return {ok:true,session_id:session.id,timing:{received_at:receivedAt,worker_stdin_at:writtenAt,handled_at:Date.now()}};
  };
  const events=({sessionId,resourceId,after=0,actor}={})=>{
    const session=getSession(sessionId,resourceId,actor);
    const items=session.events.filter(event=>event.seq>after).slice(0,64);
    const cursor=items.at(-1)?.seq??after,hasMore=session.events.some(event=>event.seq>cursor);
    return {session_id:session.id,events:items,cursor,has_more:hasMore,gap:Boolean(session.events.length&&after<session.events[0].seq-1),closed:session.closed&&!hasMore};
  };
  const stop=async({sessionId,resourceId,actor}={})=>{
    const session=getSession(sessionId,resourceId,actor);
    if(!session.closed){try{write(session,{type:'stop'});}catch{}setTimeout(()=>{try{session.child.kill('SIGTERM');}catch{}},1200).unref?.();}
    session.closed=true;adapter.onStopped?.(session);sessions.delete(session.id);
    return {ok:true,session_id:session.id};
  };
  const stopAll=async()=>{await Promise.allSettled([...sessions.values()].map(session=>stop({sessionId:session.id,resourceId:session.resourceId})));};
  return {start,input,events,stop,stopAll,size:()=>sessions.size};
}

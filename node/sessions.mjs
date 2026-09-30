import {createHash,randomBytes,randomUUID,timingSafeEqual} from 'node:crypto';
export const LIVE_SESSION_MODELS=Object.freeze(['gemini-3.8-live','gemini-3.8-live-extended-thinking']);
export class LiveError extends Error {constructor(code,message){super(message);this.code=code;}}
const TOOL_PARTS=Symbol('live_tool_response_parts');
export function withLiveToolParts(result,parts){
  if(!result||typeof result!=='object'||Array.isArray(result)||!Array.isArray(parts)||parts.length<1||parts.length>2)throw new TypeError('Invalid Live tool response parts');
  for(const part of parts){
    const blob=part?.inlineData;
    if(!['image/jpeg','image/png','image/webp'].includes(blob?.mimeType)||typeof blob?.data!=='string'||blob.data.length>700000||Buffer.from(blob.data,'base64').length>512*1024||typeof blob?.displayName!=='string'||!/^[a-zA-Z0-9_.-]{1,80}$/.test(blob.displayName))throw new TypeError('Invalid Live image response part');
  }
  return Object.defineProperty({...result},TOOL_PARTS,{value:parts});
}
const trimText=(value,max=1200)=>typeof value==='string'?value.slice(0,max):value;
const stable=value=>Array.isArray(value)?value.map(stable):value&&typeof value==='object'?Object.fromEntries(Object.keys(value).sort().map(key=>[key,stable(value[key])])):value;
const configurationMeta=configuration=>{
  const functions=Array.isArray(configuration?.functions)?configuration.functions:[];
  const encoded=JSON.stringify(stable(configuration??{}));
  return {
    configuration_digest:createHash('sha256').update(encoded).digest('hex'),
    function_count:functions.length,
    schema_bytes:Buffer.byteLength(JSON.stringify(functions))
  };
};
const validateCapabilitySpec=(spec,DomainError)=>{
  if(!spec||typeof spec!=='object')throw new DomainError('LIVE_CAPABILITY_INVALID','Capability resolver returned invalid state');
  const capability=String(spec.capability??'');
  if(!/^[a-z][a-z0-9_-]{0,63}$/.test(capability))throw new DomainError('LIVE_CAPABILITY_INVALID','Capability id is invalid');
  if(!spec.configuration||typeof spec.configuration!=='object'||Array.isArray(spec.configuration))throw new DomainError('LIVE_CAPABILITY_INVALID','Capability configuration is required');
  const meta=configurationMeta(spec.configuration);
  if(meta.function_count>9||Buffer.byteLength(JSON.stringify(spec.configuration))>262144)throw new DomainError('LIVE_CAPABILITY_LIMIT','Capability bundle exceeds shared limits');
  if(spec.context!==undefined&&(!spec.context||typeof spec.context!=='object'||Array.isArray(spec.context)))throw new DomainError('LIVE_CAPABILITY_INVALID','Capability context is invalid');
  if(spec.continuation!==undefined&&(typeof spec.continuation!=='string'||spec.continuation.length>1200))throw new DomainError('LIVE_CAPABILITY_INVALID','Capability continuation is invalid');
  return {...spec,capability,continuation:typeof spec.continuation==='string'?spec.continuation.trim():undefined,...meta};
};

// The host owns transport, ordering, capability transitions and event cursors; adapters own domain policy.
export function createLiveSessionHost({adapterFactory,createWorker,models=LIVE_SESSION_MODELS,ErrorClass=LiveError,readyTimeoutMs=30000,reconfigureTimeoutMs=105000,maxSessions=2}={}){
  const DomainError=ErrorClass,sessions=new Map();
  let adapter;
  const emit=(session,event)=>{
    const observed={seq:session.nextSeq++,at:new Date().toISOString(),...event};
    session.events.push(observed);
    while(session.events.length>320)session.events.shift();
    for(const listener of session.subscribers??[])try{listener(observed);}catch{}
    try{adapter?.onEvent?.(session,observed);}catch{}
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
  const rememberToolResult=(session,id,result)=>{
    if(!id)return;
    session.toolResults.set(id,result);
    while(session.toolResults.size>100)session.toolResults.delete(session.toolResults.keys().next().value);
  };
  const functionResponse=(call,result)=>({name:call.name??'unknown',id:call.id,response:result?.[TOOL_PARTS]?result:{result},...(result?.[TOOL_PARTS]?{parts:result[TOOL_PARTS]}:{})});
  const sendToolResponses=(session,responses)=>{
    if(responses.length&&!session.closed){
      session.toolResponseAt=write(session,{type:'tool_response',responses});
      session.awaitingAudio=true;
      emit(session,{type:'timing',stage:'tool_response_written'});
    }
  };
  const settleAudioEndWaiters=(session,error=null)=>{
    for(const waiter of [...session.audioEndWaiters]){
      if(!error&&session.audioEndSentGeneration<waiter.generation)continue;
      clearTimeout(waiter.timer);
      session.audioEndWaiters.delete(waiter);
      if(error)waiter.reject(error);
      else waiter.resolve();
    }
  };
  const waitForAudioTurnEnd=async session=>{
    const generation=session.audioTurnGeneration;
    if(!generation||session.audioEndSentGeneration>=generation)return;
    const started=Date.now();
    emit(session,{type:'capability_waiting_for_audio_end'});
    await new Promise((resolve,reject)=>{
      const waiter={generation,resolve,reject,timer:null};
      waiter.timer=setTimeout(()=>{
        session.audioEndWaiters.delete(waiter);
        reject(new DomainError('LIVE_AUDIO_TURN_PENDING','Speech did not finish before capability transition'));
      },30000);
      session.audioEndWaiters.add(waiter);
    });
    if(session.closed)throw new DomainError('LIVE_SESSION_CLOSED','Live-сессия закрыта');
    emit(session,{type:'capability_audio_end_ready',duration_ms:Date.now()-started});
  };
  const transitionCapability=async(session,call,spec)=>{
    const resolved=validateCapabilitySpec(spec,DomainError);
    await waitForAudioTurnEnd(session);
    const transitionId='cap_'+randomUUID().replaceAll('-','');
    let resolveReady,rejectReady;
    const ready=new Promise((resolve,reject)=>{resolveReady=resolve;rejectReady=reject;});
    session.pendingTransition={id:transitionId,capability:resolved.capability,resolve:resolveReady,reject:rejectReady};
    emit(session,{type:'capability_transition_requested',transition_id:transitionId,
      from_capability:session.capability,to_capability:resolved.capability,
      configuration_digest:resolved.configuration_digest,function_count:resolved.function_count,
      schema_bytes:resolved.schema_bytes});
    const callName=call?.name??'unknown',callId=call?.id;
    const fallbackIntent=typeof call?.args?.intent==='string'?call.args.intent.trim():'';
    const continuation=resolved.continuation??fallbackIntent;
    if(continuation.length>1200)throw new DomainError('LIVE_CAPABILITY_INVALID','Capability continuation is invalid');
    const acknowledgement={capability:resolved.capability,accepted:true};
    let sent=false;
    try{
      write(session,{type:'reconfigure',transition_id:transitionId,capability:resolved.capability,
        configuration:resolved.configuration,context:resolved.context??{},continuation,
        router_response:{name:callName,id:callId,response:{result:acknowledgement},scheduling:'SILENT',willContinue:false}});
      sent=true;
      let timer;
      try{
        await Promise.race([
          ready,
          new Promise((_,reject)=>{timer=setTimeout(()=>reject(new DomainError('LIVE_CAPABILITY_TIMEOUT','Capability transition timeout')),reconfigureTimeoutMs);})
        ]);
      }finally{clearTimeout(timer);}
      session.capability=resolved.capability;
      session.configurationDigest=resolved.configuration_digest;
      const result={capability:resolved.capability,ready:true,...(resolved.response&&typeof resolved.response==='object'?resolved.response:{})};
      rememberToolResult(session,callId,result);
      emit(session,{type:'tool_result',name:callName,id:callId,status:'ok',capability:resolved.capability});
    }catch(error){
      emit(session,{type:'capability_transition_failed',transition_id:transitionId,
        from_capability:session.capability,to_capability:resolved.capability,
        code:error.code??'LIVE_CAPABILITY_ERROR'});
      emit(session,{type:'tool_result',name:callName,id:callId,status:'error',code:error.code??'LIVE_CAPABILITY_ERROR'});
      if(sent&&!session.closed){
        // The worker may already be reconnecting with the new tool set. Once
        // the transition is rejected, it cannot safely serve the old allowlist.
        session.closed=true;
        adapter.onStopped?.(session);
        session.stoppedNotified=true;
        emit(session,{type:'error',code:error.code??'LIVE_CAPABILITY_ERROR',message:'Live capability transition failed; restart the session'});
        try{session.child.kill('SIGTERM');}catch{}
      }
    }finally{
      if(session.pendingTransition?.id===transitionId)session.pendingTransition=null;
    }
  };
  const handleToolCalls=async(session,calls)=>{
    calls=Array.isArray(calls)?calls:[];
    if(!calls.length||session.closed)return;
    if(session.inputDamaged){
      const responses=calls.map(call=>({name:call?.name??'unknown',id:call?.id,response:{error:{code:'LIVE_INPUT_DAMAGED',message:'The preceding speech was interrupted by transport loss; ask the user to repeat the request.'}}}));
      for(const call of calls)emit(session,{type:'tool_result',name:call?.name,id:call?.id,status:'error',code:'LIVE_INPUT_DAMAGED'});
      sendToolResponses(session,responses);
      return;
    }
    if(calls.length===1&&calls[0]?.id&&session.toolResults.has(calls[0].id)){
      const call=calls[0],result=session.toolResults.get(call.id);
      sendToolResponses(session,[functionResponse(call,result)]);
      return;
    }
    if(typeof adapter.resolveCapability==='function'){
      const resolved=[];
      for(const call of calls)resolved.push(await adapter.resolveCapability(session,call));
      const transitions=resolved.map((spec,index)=>({spec,index})).filter(item=>item.spec);
      if(transitions.length){
        if(calls.length!==1||transitions.length!==1){
          const responses=calls.map(call=>({name:call?.name??'unknown',id:call?.id,response:{error:{code:'LIVE_CAPABILITY_CONFLICT',message:'Capability transition must be the only tool call in its batch'}}}));
          for(const call of calls)emit(session,{type:'tool_result',name:call?.name,id:call?.id,status:'error',code:'LIVE_CAPABILITY_CONFLICT'});
          sendToolResponses(session,responses);
          return;
        }
        try{
          await transitionCapability(session,calls[0],transitions[0].spec);
        }catch(error){
          const response={error:{code:error.code??'LIVE_CAPABILITY_ERROR',message:String(error.message??error).slice(0,500)}};
          emit(session,{type:'capability_transition_rejected',from_capability:session.capability,
            to_capability:String(transitions[0].spec?.capability??'').slice(0,80),code:response.error.code});
          emit(session,{type:'tool_result',name:calls[0]?.name,id:calls[0]?.id,status:'error',code:response.error.code});
          sendToolResponses(session,[{name:calls[0]?.name??'unknown',id:calls[0]?.id,response}]);
        }
        return;
      }
    }
    const responses=[];
    for(const call of calls){
      if(session.closed)return;
      if(session.cancelled.has(call.id))continue;
      const toolAt=Date.now();
      try{
        const result=session.toolResults.has(call.id)?session.toolResults.get(call.id):await adapter.executeTool(session,call);
        rememberToolResult(session,call.id,result);
        emit(session,{type:'tool_result',name:call.name,id:call.id,status:'ok',duration_ms:Date.now()-toolAt,revision:result?.revision??result?.result_revision??null});
        responses.push(functionResponse(call,result));
      }catch(error){
        emit(session,{type:'tool_result',name:call?.name,id:call?.id,status:'error',code:error.code??'LIVE_TOOL_ERROR',message:trimText(error.message,240),change_shapes:Array.isArray(call?.args?.changes)?call.args.changes.slice(0,16).map(c=>({kind:c?.kind,keys:Object.keys(c??{})})):undefined});
        responses.push({name:call?.name??'unknown',id:call?.id,response:{error:{code:error.code??'LIVE_TOOL_ERROR',message:String(error.message??error).slice(0,500)}}});
      }
    }
    sendToolResponses(session,responses);
  };
  adapter=adapterFactory({emit,write,measure,timing});
  const ticketDigest=value=>createHash('sha256').update(value).digest();
  const issueTicketForSession=session=>{
    const ticket=randomBytes(32).toString('base64url');
    session.socketTicketHash=ticketDigest(ticket);session.socketTicketExpiresAt=Date.now()+15000;session.socketTicketUsed=false;
    return ticket;
  };
  const start=async({resourceId,actor,model=models[0],history=[],attemptId=null,...args}={})=>{
    if(typeof resourceId!=='string'||!resourceId||resourceId.length>240)throw new DomainError('INVALID_ARGUMENT','resourceId is required');
    if(!models.includes(model))throw new DomainError('INVALID_INPUT','Unknown Live model');
    if(attemptId!==null&&(typeof attemptId!=='string'||!/^attempt_[A-Za-z0-9_-]{8,80}$/.test(attemptId)))throw new DomainError('INVALID_ARGUMENT','Live attempt id is invalid');
    if(sessions.size>=maxSessions)throw new DomainError('LIVE_BUSY','Live session limit reached');
    const initialized=adapter.initialize({resourceId,actor,model,...args});
    const id=`live_${randomUUID().replaceAll('-','')}`;
    const child=createWorker({model,actor,resourceId});
    const initialMeta=configurationMeta(initialized.configuration??{});
    const session={...initialized.state,id,resourceId,actor,model,attemptId,child,events:[],nextSeq:1,buffer:'',closed:false,toolChain:Promise.resolve(),cancelled:new Set(),toolResults:new Map(),subscribers:new Set(),
      audioTurnGeneration:0,audioTurnOpen:false,audioEndSentGeneration:0,audioEndAwaitingAck:[],audioEndWaiters:new Set(),
      inputDamaged:false,damageRecoveryGeneration:0,connectionGeneration:0,socketTicketHash:null,socketTicketExpiresAt:0,socketTicketUsed:false,
      manualActivityDetection:Boolean(initialized.configuration?.manual_activity_detection),activityOpen:false,
      capability:initialized.capability??initialized.state?.capability??'core',configurationDigest:initialMeta.configuration_digest,pendingTransition:null};
    sessions.set(id,session);
    let readyResolve,readyReject;
    const ready=new Promise((resolve,reject)=>{readyResolve=resolve;readyReject=reject;});
    const parseLine=line=>{
      if(!line.trim())return;
      let event;
      try{event=JSON.parse(line);}catch{return;}
      if(session.closed)return;
      if(event.type==='capability_ready'){
        emit(session,event);
        if(session.pendingTransition?.id===event.transition_id)session.pendingTransition.resolve(event);
        return;
      }
      if(event.type==='capability_transition_error'){
        emit(session,event);
        if(session.pendingTransition?.id===event.transition_id)session.pendingTransition.reject(new DomainError(event.code??'LIVE_CAPABILITY_ERROR','Capability transition failed'));
        return;
      }
      if(event.type==='ready'){emit(session,event);readyResolve(event);return;}
      if(event.type==='error'){
        emit(session,event);
        const failure=new DomainError(event.code??'LIVE_PROVIDER_ERROR',trimText(event.message??'Gemini Live error',500));
        session.pendingTransition?.reject?.(failure);
        readyReject(failure);
        return;
      }
      if(event.type==='tool_cancelled'){for(const id of event.ids??[])session.cancelled.add(id);}
      if(event.type==='input_timing'&&(event.audio_stream_end_sent_at||event.activity_end_sent_at)){
        const generation=session.audioEndAwaitingAck.shift();
        if(generation)session.audioEndSentGeneration=Math.max(session.audioEndSentGeneration,generation);
        if(generation&&session.inputDamaged&&session.damageRecoveryGeneration&&generation>=session.damageRecoveryGeneration){
          session.inputDamaged=false;session.damageRecoveryGeneration=0;
          emit(session,{type:'transport_recovered',audio_turn_generation:generation});
        }
        settleAudioEndWaiters(session);
      }
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
    child.on('close',code=>{
      readyReject(new DomainError('LIVE_PROVIDER_CLOSED','Live provider closed during setup'));
      session.pendingTransition?.reject?.(new DomainError('LIVE_PROVIDER_CLOSED','Live provider closed during capability transition'));
      session.pendingTransition=null;
      session.closed=true;emit(session,{type:'closed',code});
      settleAudioEndWaiters(session,new DomainError('LIVE_PROVIDER_CLOSED','Live provider closed while speech was finishing'));
    });
    write(session,{type:'start',model,configuration:initialized.configuration,context:initialized.context,history:Array.isArray(history)?history.slice(-8).filter(m=>['user','model'].includes(m?.role)&&typeof m.text==='string').map(m=>({role:m.role,text:m.text.slice(-700)})):[]});
    let timer;
    try{
      await Promise.race([ready,new Promise((_,reject)=>{timer=setTimeout(()=>reject(new DomainError('LIVE_TIMEOUT','Gemini Live setup timeout')),readyTimeoutMs);})]);
    }catch(error){
      sessions.delete(id);try{child.kill('SIGKILL');}catch{}throw error;
    }finally{clearTimeout(timer);}
    adapter.onStarted?.(session);
    emit(session,{type:'configuration_ready',capability:session.capability,
      configuration_digest:session.configurationDigest,function_count:initialMeta.function_count,schema_bytes:initialMeta.schema_bytes});
    const socketTicket=issueTicketForSession(session);
    return {session_id:id,model,capability:session.capability,configuration_digest:session.configurationDigest,attempt_id:attemptId,transport_protocol:'wl-live-v1',socket_ticket:socketTicket,...initialized.response};
  };
  const getSession=(sessionId,resourceId,actor)=>{
    const session=sessions.get(sessionId);    if(!session||session.resourceId!==resourceId)throw new DomainError('LIVE_SESSION_NOT_FOUND','Live-сессия не найдена');
    if(actor&&(actor.subject!==session.actor?.subject||actor.tenant_id!==session.actor?.tenant_id))throw new DomainError('FORBIDDEN','Live-сессия принадлежит другому пользователю');
    return session;
  };
  const input=({sessionId,resourceId,message,receivedAt=Date.now(),actor,frameSeq=null,captureAgeMs=null,connectionGeneration=null}={})=>{
    let writtenAt=null;
    const session=getSession(sessionId,resourceId,actor);
    adapter.input?.(session,message);
    if(message?.activity_start){
      if(!session.manualActivityDetection||session.activityOpen)throw new DomainError('INVALID_ARGUMENT','Manual activity cannot start');
      write(session,{type:'activity_start'});session.activityOpen=true;
    }
    if(message?.audio_base64!==undefined){
      if(typeof message.audio_base64!=='string'||message.audio_base64.length>16000)throw new DomainError('INVALID_ARGUMENT','Audio chunk is invalid');
      if(session.manualActivityDetection&&!session.activityOpen)throw new DomainError('INVALID_ARGUMENT','activity_start is required');
      writtenAt=write(session,{type:'audio',data:message.audio_base64});
      if(!session.audioTurnOpen){
        session.audioTurnGeneration++;session.audioTurnOpen=true;
        if(session.inputDamaged&&!session.damageRecoveryGeneration)session.damageRecoveryGeneration=session.audioTurnGeneration;
      }
      if(Number.isInteger(frameSeq)&&frameSeq>0&&frameSeq%10===0)emit(session,{type:'browser_audio_progress',frame_seq:frameSeq,capture_age_ms:Number.isFinite(captureAgeMs)?Math.round(captureAgeMs):null,connection_generation:Number.isInteger(connectionGeneration)?connectionGeneration:null,worker_stdin_delay_ms:writtenAt?Math.max(0,writtenAt-receivedAt):null});
    }
    if(message?.audio_stream_end){
      if(session.manualActivityDetection)throw new DomainError('INVALID_ARGUMENT','Use activity_end for manual activity');
      write(session,{type:'audio_stream_end'});
      if(session.audioTurnOpen){session.audioEndAwaitingAck.push(session.audioTurnGeneration);session.audioTurnOpen=false;}
    }
    if(message?.activity_end){
      if(!session.manualActivityDetection||!session.activityOpen)throw new DomainError('INVALID_ARGUMENT','No manual activity is open');
      write(session,{type:'activity_end'});session.activityOpen=false;
      if(session.audioTurnOpen){session.audioEndAwaitingAck.push(session.audioTurnGeneration);session.audioTurnOpen=false;}
    }
    if(message?.text!==undefined){
      if(typeof message.text!=='string'||!message.text.trim()||message.text.length>4000)throw new DomainError('INVALID_ARGUMENT','Text turn is invalid');
      write(session,{type:'text',text:message.text.trim()});
    }
    return {ok:true,session_id:session.id,timing:{received_at:receivedAt,worker_stdin_at:writtenAt,handled_at:Date.now()}};
  };
  const issueSocketTicket=({sessionId,resourceId,actor}={})=>{
    const session=getSession(sessionId,resourceId,actor);
    return {socket_ticket:issueTicketForSession(session),transport_protocol:'wl-live-v1',expires_in_ms:15000,attempt_id:session.attemptId};
  };
  const openSocket=({sessionId,ticket}={})=>{
    const session=sessions.get(sessionId);
    if(!session)throw new DomainError('LIVE_SESSION_NOT_FOUND','Live-сессия не найдена');
    if(typeof ticket!=='string'||!session.socketTicketHash||session.socketTicketUsed||Date.now()>session.socketTicketExpiresAt)throw new DomainError('LIVE_SOCKET_TICKET','Live socket ticket expired or already used');
    const actual=ticketDigest(ticket),expected=session.socketTicketHash;
    if(actual.length!==expected.length||!timingSafeEqual(actual,expected))throw new DomainError('LIVE_SOCKET_TICKET','Live socket ticket is invalid');
    session.socketTicketUsed=true;session.socketTicketHash=null;
    return {
      attemptId:session.attemptId,
      subscribe(listener){session.subscribers.add(listener);return()=>session.subscribers.delete(listener);},
      events(after=0){return events({sessionId:session.id,resourceId:session.resourceId,after,actor:session.actor});},
      input(message,meta={}){return input({sessionId:session.id,resourceId:session.resourceId,message,receivedAt:meta.receivedAt??Date.now(),actor:session.actor,frameSeq:meta.frame_seq,captureAgeMs:meta.capture_age_ms,connectionGeneration:meta.connection_generation});},
      transportConnected(meta={}){session.connectionGeneration=Number(meta.connection_generation)||session.connectionGeneration+1;emit(session,{type:'transport_connected',transport:'wss',connection_generation:session.connectionGeneration});},
      transportGap(meta={}){
        emit(session,{type:'transport_gap',transport:'wss',connection_generation:Number(meta.connection_generation)||session.connectionGeneration,reason:String(meta.reason??'socket_closed').slice(0,80),audio_turn_open:session.audioTurnOpen});
        if(session.audioTurnOpen&&!session.closed){
          try{write(session,{type:session.manualActivityDetection?'activity_end':'audio_stream_end'});}catch{}
          session.activityOpen=false;
          session.audioEndAwaitingAck.push(session.audioTurnGeneration);session.audioTurnOpen=false;session.inputDamaged=true;session.damageRecoveryGeneration=0;
        }
      },
      stop(){return stop({sessionId:session.id,resourceId:session.resourceId,actor:session.actor});},
      close(){}
    };
  };
  const events=({sessionId,resourceId,after=0,actor}={})=>{
    const session=getSession(sessionId,resourceId,actor);
    const items=session.events.filter(event=>event.seq>after).slice(0,64);
    const cursor=items.at(-1)?.seq??after,hasMore=session.events.some(event=>event.seq>cursor);
    return {session_id:session.id,events:items,cursor,has_more:hasMore,gap:Boolean(session.events.length&&after<session.events[0].seq-1),closed:session.closed&&!hasMore};
  };
  const stop=async({sessionId,resourceId,actor}={})=>{
    // WSS Stop can remove the session on the socket before the browser's
    // best-effort HTTP cleanup arrives. That cleanup is intentionally
    // idempotent; an unknown already-closed id exposes no resource data.
    if(!sessions.has(sessionId))return {ok:true,session_id:sessionId,already_closed:true};
    const session=getSession(sessionId,resourceId,actor);
    if(!session.closed){try{write(session,{type:'stop'});}catch{}setTimeout(()=>{try{session.child.kill('SIGTERM');}catch{}},1200).unref?.();}
    session.pendingTransition?.reject?.(new DomainError('LIVE_SESSION_CLOSED','Live session stopped during capability transition'));
    session.pendingTransition=null;
    settleAudioEndWaiters(session,new DomainError('LIVE_SESSION_CLOSED','Live-сессия закрыта'));
    session.closed=true;if(!session.stoppedNotified)adapter.onStopped?.(session);sessions.delete(session.id);
    return {ok:true,session_id:session.id};
  };
  const stopAll=async()=>{await Promise.allSettled([...sessions.values()].map(session=>stop({sessionId:session.id,resourceId:session.resourceId})));};
  return {start,input,events,issueSocketTicket,openSocket,stop,stopAll,size:()=>sessions.size};
}

import {createLiveAudioSender} from './live-audio.js';
import {createMicrophoneCapture,createDurableMicrophoneCapture,pcm16,frameRms,microphoneConstraints} from './capture.js';
import {isLiveStopCommand,liveStopConfirmation} from './live-commands.js';
import {createLiveSocketTransport} from './socket-transport.js';

export {createLiveAudioSender} from './live-audio.js';
export {createMicrophoneCapture,createDurableMicrophoneCapture,pcm16,frameRms,microphoneConstraints} from './capture.js';

export async function liveJson(url,options){
  const response=await fetch(url,{cache:'no-store',credentials:'same-origin',signal:AbortSignal.timeout(30000),...options});
  const payload=await response.json().catch(()=>({}));
  if(!response.ok){const error=new Error(payload?.error?.message??`HTTP ${response.status}`);error.status=response.status;throw error;}
  return payload;
}
function base64(bytes){let text='';for(let i=0;i<bytes.length;i+=0x8000)text+=String.fromCharCode(...bytes.subarray(i,i+0x8000));return btoa(text);}
// UI, authentication and domain tools are host concerns. All audio/lifecycle paths
// go through this client, including Stop while setup or a poll is still pending.
export function createLiveClient({request=liveJson,onEvent=()=>{},onState=()=>{},onNotice=()=>{},onTiming=()=>{},onWait=()=>{},voiceControl={isStop:isLiveStopCommand,confirmation:liveStopConfirmation},persistAudio=null,binaryAudio=false,transport='http',WebSocketImpl=globalThis.WebSocket,suppressCaptureDuringPlayback=false,speechEndSilenceMs=2000,captureTap=null}={}){
  if(!['http','wss'].includes(transport))throw new TypeError('Unknown Live browser transport');
  let model=null,sessionId=null,starting=false,generation=0,root=null,abort=null,cursor=0,pollTimer=null,attemptId=null;
  let socketTransport=null,socketUrl=null,connectionGeneration=0,reconnectPromise=null;
  let microphone=null,sender=null,microphoneEnabled=false,startupCapture=null,budgetPaused=false;
  let playContext=null,nextPlayAt=0,playing=new Set(),inputTranscript='',transcriptAt=0;
  let stopPending=false,stopExpiry=null,stopConfirmTimer=null;
  let waitAt=null,waitTimer=null,waitStage='transport',awaitingReply=false,lastPlaybackSuppressionAt=0,suppressResponseReason=null;
  const pendingTools=new Set();
  const Audio=()=>globalThis.AudioContext||globalThis.webkitAudioContext;
  function clearWait(){waitAt=null;clearInterval(waitTimer);waitTimer=null;onWait(null);}
  function beginWait(){
    if(waitAt!==null)return;
    waitAt=Date.now();waitStage='transport';
    waitTimer=setInterval(()=>{const elapsed=Date.now()-waitAt;if(elapsed>=15000)onWait({elapsed_ms:elapsed,stage:waitStage,can_restart:elapsed>=120000});},1000);
  }
  function clearConfirmation(){stopPending=false;clearTimeout(stopExpiry);clearTimeout(stopConfirmTimer);stopExpiry=stopConfirmTimer=null;}
  function voice(){
    if(!voiceControl)return;
    if(stopPending){
      clearTimeout(stopConfirmTimer);stopConfirmTimer=null;
      const reply=voiceControl.confirmation(inputTranscript);
      if(reply==='cancel'){clearConfirmation();onTiming('voice_stop_cancelled');onNotice('voice_stop_cancelled');}
      else if(reply==='confirm'){
        const epoch=generation;
        stopConfirmTimer=setTimeout(()=>{if(epoch!==generation||!stopPending||voiceControl.confirmation(inputTranscript)!=='confirm')return;onTiming('voice_stop_confirmed');stop({reason:'voice_command'});},900);
      }
      return;
    }
    if(!voiceControl.isStop(inputTranscript))return;
    stopPending=true;inputTranscript='';onTiming('voice_stop_confirmation_requested');onNotice('voice_stop_confirmation_requested');
    stopExpiry=setTimeout(()=>{clearConfirmation();onTiming('voice_stop_expired');onNotice('voice_stop_expired');},30000);
  }
  function suppressCurrentResponse(reason='local_control'){
    if(!sessionId)return false;
    suppressResponseReason=String(reason||'local_control').slice(0,80);
    stopPlayback('response_suppressed');
    onTiming('response_suppression_started',{reason:suppressResponseReason});
    return true;
  }
  function stopPlayback(reason='user_stop'){
    if(playing.size)onTiming('playback_cancelled',{reason,queued_buffers:playing.size,remaining_ms:Math.max(0,(nextPlayAt-playContext.currentTime)*1000)});
    for(const node of playing){try{node.liveCancelled=true;node.stop();}catch{}}
    playing.clear();nextPlayAt=0;
  }
  async function play(event,epoch){
    const Context=Audio();if(!Context)return;
    playContext??=new Context();if(playContext.state==='suspended')await playContext.resume().catch(()=>{});
    if(epoch!==generation||!sessionId)return;
    const bytes=event.pcm instanceof Uint8Array?event.pcm:(()=>{const raw=atob(event.data);return Uint8Array.from(raw,c=>c.charCodeAt(0));})();
    const rate=Number(/rate=(\d+)/.exec(event.mime_type??'')?.[1]??24000),samples=new Int16Array(bytes.buffer,bytes.byteOffset,Math.floor(bytes.byteLength/2));
    let peak=0,sumSquares=0;
    const buffer=playContext.createBuffer(1,samples.length,rate),channel=buffer.getChannelData(0);
    for(let i=0;i<samples.length;i++){const value=samples[i]/32768;channel[i]=value;const magnitude=Math.abs(value);if(magnitude>peak)peak=magnitude;sumSquares+=value*value;}
    const playbackFacts={
      seq:event.seq,pcm_bytes:bytes.byteLength,duration_ms:buffer.duration*1000,sample_rate:rate,
      pcm_peak:peak,pcm_rms:samples.length?Math.sqrt(sumSquares/samples.length):0,
      audio_context_running:playContext.state==='running',
      audio_context_sample_rate:Number(playContext.sampleRate)||0,
      audio_base_latency_ms:Number(playContext.baseLatency)*1000||0,
      audio_output_latency_ms:Number(playContext.outputLatency)*1000||0
    };
    const source=playContext.createBufferSource();source.buffer=buffer;source.connect(playContext.destination);
    // Gemini can emit a very short first chunk (for example 50 ms) hundreds of
    // milliseconds before the next one. Hold the start of each playback run so
    // normal provider/poll jitter does not become an audible gap.
    const runStart=nextPlayAt<=playContext.currentTime;
    const at=Math.max(playContext.currentTime+(runStart?.4:.02),nextPlayAt||0);nextPlayAt=at+buffer.duration;playing.add(source);
    if(runStart)onTiming('playback_buffering',{buffer_ms:400,first_chunk_ms:buffer.duration*1000});
    onTiming('audio_scheduled',{...playbackFacts,starts_at:Date.now()+(at-playContext.currentTime)*1000,buffered_ms:(nextPlayAt-playContext.currentTime)*1000});
    source.onended=()=>{playing.delete(source);source.disconnect();onTiming(source.liveCancelled?'audio_cancelled':'audio_played',playbackFacts);};source.start(at);
  }
  function input(message){
    if(!sessionId)return Promise.resolve();
    if(message.text){awaitingReply=true;beginWait();}
    if(transport==='wss'){
      if(!socketTransport?.open)return Promise.reject(Object.assign(new Error('Live WebSocket is not connected'),{code:'LIVE_SOCKET_CLOSED'}));
      return socketTransport.send(message);
    }
    const pcm=message.pcm instanceof Int16Array?new Uint8Array(message.pcm.buffer,message.pcm.byteOffset,message.pcm.byteLength):null;
    const audio=pcm!==null||message.audio_base64!==undefined||message.audio_stream_end===true;
    // The ordered audio sender owns the tight steady-state liveness bound:
    // 1.5s queued PCM / 2.5s item age. A longer absolute HTTP ceiling here
    // lets an intentional startup handoff drain without a false local timeout,
    // while a genuinely stalled steady stream still fails via the sender first.
    const timeoutMs=audio?10000:2500;
    const startedAt=performance.now();
    return request(`${root}/${encodeURIComponent(sessionId)}/input`,{method:'POST',headers:{'content-type':pcm?'application/octet-stream':'application/json'},body:pcm??JSON.stringify(message),signal:AbortSignal.any([abort.signal,AbortSignal.timeout(timeoutMs)])})
      .then(result=>{
        if(pcm)onTiming('audio_http',{duration_ms:performance.now()-startedAt,pcm_bytes:pcm.byteLength,server_receive_to_ack_ms:Math.max(0,(result?.timing?.handled_at??0)-(result?.timing?.received_at??0))});
        return result;
      },error=>{
        if(pcm)onTiming('audio_http_error',{duration_ms:performance.now()-startedAt,pcm_bytes:pcm.byteLength});
        throw error;
      });
  }
  function releaseStartupCapture({stopTracks=true,event='startup_capture_cancelled'}={}){
    const capture=startupCapture;startupCapture=null;
    if(!capture)return null;
    if(stopTracks){capture.microphone.stop();onTiming(event,{buffered_chunks:capture.frames.length,buffered_samples:capture.samples,sample_rate:16000});return null;}
    onTiming(event,{buffered_chunks:capture.frames.length,buffered_samples:capture.samples,sample_rate:16000});
    return {capture:capture.microphone,frames:capture.frames};
  }
  async function beginStartupCapture(epoch){
    if(startupCapture||!microphoneEnabled)return startupCapture;
    const capture={epoch,microphone:null,frames:[],samples:0,maxSamples:16000*20};
    const microphone=createMicrophoneCapture({
      onFrame:(pcm,rms,meta)=>{
        if(epoch!==generation||startupCapture!==capture)return;
        const copy=new Int16Array(pcm);
        capture.frames.push({pcm:copy,rms,captured_at_ms:meta?.capture_at_ms});capture.samples+=copy.length;
        while(capture.samples>capture.maxSamples&&capture.frames.length>1){const removed=capture.frames.shift();capture.samples-=removed.pcm.length;}
      },
      onTiming,
      onError:error=>{if(epoch!==generation)return;releaseStartupCapture();onState('microphone_unavailable');onNotice('microphone_error',error);}
    });
    capture.microphone=microphone;startupCapture=capture;
    const started=await microphone.start({constraints:microphoneConstraints});
    if(!started||epoch!==generation||startupCapture!==capture){microphone.stop();if(startupCapture===capture)startupCapture=null;return null;}
    onTiming('startup_capture_started',{sample_rate:16000,max_buffer_ms:20000,processor:'audio-worklet'});
    return capture;
  }
  function takeStartupCapture(epoch){
    if(!startupCapture||startupCapture.epoch!==epoch)return null;
    return releaseStartupCapture({stopTracks:false,event:'startup_capture_handoff'});
  }

  function closeMic({stopTracks=true}={}){
    sender?.stop();sender=null;
    const retained=microphone?.stop({stopTracks})??null;microphone=null;
    return retained;
  }
  async function startMic(epoch,handoff=null){
    if(!microphoneEnabled||budgetPaused)return false;
    if(microphone?.running){onTiming('microphone_reused_after_resume');return true;}
    if(!handoff&&!navigator.mediaDevices?.getUserMedia){onState('microphone_unavailable');return false;}
    try{
      sender=createLiveAudioSender({
        send:message=>input(transport==='wss'?message:message.pcm&&!binaryAudio?{audio_base64:base64(new Uint8Array(message.pcm.buffer,message.pcm.byteOffset,message.pcm.byteLength))}:message),
        persist:persistAudio,
        onTiming:(event,metrics)=>{
          onTiming(event,metrics);if(event==='speech_start'){inputTranscript='';awaitingReply=true;clearWait();}if(event==='speech_end'&&awaitingReply&&!playing.size)beginWait();
        },
        onError:error=>{if(epoch!==generation)return;stop({reason:'transport_error',preservePlayback:true});onNotice('transport_error',error);},
        speechEndMs:speechEndSilenceMs
      });
      const onCapturedFrame=(pcm,rms)=>{
        if(epoch!==generation||!sessionId)return;
        const suppressPlayback=typeof suppressCaptureDuringPlayback==='function'?suppressCaptureDuringPlayback():suppressCaptureDuringPlayback;
        if(suppressPlayback&&playing.size){const at=Date.now();if(at-lastPlaybackSuppressionAt>1000){lastPlaybackSuppressionAt=at;onTiming('capture_suppressed_playback',{playing_buffers:playing.size});}return;}
        if(typeof captureTap==='function')try{captureTap(pcm,rms);}catch(error){onTiming('capture_tap_error',{error_code:String(error?.code??error?.message??'CAPTURE_TAP_ERROR').slice(0,80)});}
        sender?.push(pcm,rms);
      };
      const handoffFrames=[...(handoff?.frames??[])];
      for(const chunk of handoff?.chunks??[]){
        const samples=chunk instanceof Float32Array?chunk:new Float32Array(chunk);
        handoffFrames.push({pcm:pcm16(samples,handoff.sampleRate),rms:frameRms(samples)});
      }
      if(handoffFrames.length&&!sender.seed(handoffFrames))return false;
      if(handoff?.capture?.running){
        microphone=handoff.capture;microphone.setOnFrame(onCapturedFrame);
        onTiming('microphone_handoff_reused',{processor:'audio-worklet',seed_frames:handoffFrames.length});onState('listening');return true;
      }
      const capture=createMicrophoneCapture({
        onFrame:onCapturedFrame,        onTiming,
        onError:error=>{if(epoch!==generation)return;closeMic();onState('microphone_unavailable');onNotice('microphone_error',error);}
      });
      microphone=capture;
      const started=await capture.start({stream:handoff?.stream??null,constraints:microphoneConstraints});
      if(epoch!==generation||!microphoneEnabled){capture.stop();if(microphone===capture)microphone=null;return false;}
      if(started){onState('listening');return true;}
      return false;
    }catch(error){if(epoch!==generation)return false;closeMic();onState('microphone_unavailable');onNotice('microphone_error',error);return false;}
  }
  async function handleEvent(event,epoch){
    if(epoch!==generation)return false;
    if(['input_transcript','tool_call','tool_result','turn_complete','input_timing'].includes(event.type))onTiming(event.type,{provider_at:event.provider_at,server_at:event.at,name:event.name,duration_ms:event.duration_ms,...(event.type==='input_timing'?{max_stdin_delay_ms:event.max_stdin_delay_ms,max_ws_send_ms:event.max_ws_send_ms}: {})});
    const suppressOutput=Boolean(suppressResponseReason&&['audio','output_transcript'].includes(event.type));
    if(event.type==='input_transcript'){
      waitStage=pendingTools.size?'action':'provider';
      if(Date.now()-transcriptAt>2000)inputTranscript='';
      transcriptAt=Date.now();inputTranscript=(inputTranscript+' '+event.text).trim().slice(-1000);voice();
    }else if(event.type==='audio'){
      awaitingReply=false;clearWait();
      if(suppressOutput)onTiming('response_output_suppressed',{reason:suppressResponseReason,event_type:'audio',pcm_bytes:Number(event?.pcm?.byteLength??0)||undefined});
      else{if(!playing.size)onTiming('first_output_audio',{server_at:event.at,provider_at:event.provider_at});onState('answering');await play(event,epoch);}
    }else if(event.type==='input_timing'){
      // Compare events within the server clock only; browser clock may differ.
      if(waitAt!==null&&(event.audio_stream_end_sent_at||event.text_sent_at))waitStage=pendingTools.size?'action':'provider';
    }else if(event.type==='interrupted'){clearWait();onTiming('provider_interrupted');stopPlayback('provider_interrupted');}    else if(event.type==='tool_call'){for(const call of event.calls??[])pendingTools.add(call.id);waitStage='action';}
    else if(event.type==='tool_result'){pendingTools.delete(event.id);if(!pendingTools.size)waitStage='provider';}
    else if(event.type==='tool_cancelled'){for(const id of event.ids??[])pendingTools.delete(id);if(!pendingTools.size)waitStage='provider';}
    else if(event.type==='interaction_status'&&event.status==='IDLE'){awaitingReply=false;clearWait();}
    else if(event.type==='turn_complete'){
      if(!model?.endsWith('-extended-thinking')){awaitingReply=false;clearWait();}if(stopPending&&inputTranscript&&!stopConfirmTimer)clearConfirmation();if(!stopConfirmTimer)inputTranscript='';
      if(suppressResponseReason){onTiming('response_suppression_ended',{reason:suppressResponseReason});suppressResponseReason=null;}
    }else if(event.type==='reconnecting'){closeMic();onState('reconnecting');}
    else if(event.type==='resource_budget_wait'){
      beginWait();waitStage='resource';
      if(!budgetPaused){budgetPaused=true;closeMic();onState('budget_wait');onNotice('resource_budget_wait');}
    }else if(event.type==='resource_budget_ready'){      budgetPaused=false;waitStage='provider';onState('budget_ready');
      if(microphoneEnabled)void startMic(epoch);
    }else if(event.type==='resumed'&&microphoneEnabled&&!budgetPaused){void startMic(epoch);}
    if(epoch!==generation)return false;
    if(suppressOutput){if(event.type==='output_transcript')onTiming('response_output_suppressed',{reason:suppressResponseReason,event_type:'output_transcript'});cursor=event.seq??cursor;return true;}
    onEvent(event,epoch);cursor=event.seq??cursor;
    if(event.type==='error'){
      // A terminal provider error may precede the server's closed flag.
      // Release the browser microphone as soon as the error is observed.
      stop({reason:'provider_error',preservePlayback:true});
      return false;
    }
    return epoch===generation;
  }
  async function poll(){
    if(!sessionId)return;const epoch=generation;
    try{
      const result=await request(`${root}/${encodeURIComponent(sessionId)}/events?after=${cursor}`,{signal:AbortSignal.any([abort.signal,AbortSignal.timeout(5000)])});
      if(epoch!==generation)return;
      if(result.gap)onNotice('event_gap');
      for(const event of result.events??[])if(!await handleEvent(event,epoch))return;
      cursor=result.cursor??cursor;
      if(epoch!==generation)return;
      if(result.closed){stop({reason:'provider_closed',preservePlayback:true});return;}
      if(result.has_more){pollTimer=setTimeout(()=>void poll(),0);return;}
    }catch(error){if(epoch!==generation)return;onState('connection_error');onNotice('connection_error',error);}
    if(epoch===generation)pollTimer=setTimeout(()=>void poll(),160);
  }
  async function reconnectSocket(epoch){
    if(transport!=='wss'||epoch!==generation||!sessionId)return false;
    if(reconnectPromise)return reconnectPromise;
    reconnectPromise=(async()=>{
      closeMic();onState('reconnecting');onTiming('socket_reconnect_started',{connection_generation:connectionGeneration});
      const deadline=Date.now()+10000;
      let attempt=0,lastError=null;
      while(epoch===generation&&sessionId&&Date.now()<deadline&&attempt<3){
        attempt++;
        try{
          const result=await request(`${root}/${encodeURIComponent(sessionId)}/socket-ticket`,{method:'POST',headers:{'content-type':'application/json'},body:'{}',signal:AbortSignal.any([abort.signal,AbortSignal.timeout(3000)])});
          if(epoch!==generation||!sessionId)return false;
          connectionGeneration++;
          await socketTransport.connect({url:result.socket_url??socketUrl,ticket:result.socket_ticket,attempt_id:attemptId,cursor,connection_generation:connectionGeneration});
          onTiming('socket_reconnect_ready',{attempt,connection_generation:connectionGeneration});
          if(microphoneEnabled&&!budgetPaused)await startMic(epoch);
          return true;
        }catch(error){lastError=error;onTiming('socket_reconnect_attempt_failed',{attempt,code:error?.code??'LIVE_SOCKET_RECONNECT'});if(attempt<3)await new Promise(resolve=>setTimeout(resolve,Math.min(1000,150*2**attempt)));}
      }
      if(epoch===generation){stop({reason:'transport_error',preservePlayback:true});onNotice('transport_error',lastError??new Error('Live WebSocket reconnect failed'));}
      return false;
    })().finally(()=>{reconnectPromise=null;});
    return reconnectPromise;
  }
  function remoteStop(url,keepalive=false){onTiming('stop_request');void request(url,{method:'POST',headers:{'content-type':'application/json'},body:'{}',keepalive,signal:AbortSignal.timeout(2500)}).then(()=>onTiming('stop_response')).catch(()=>onTiming('stop_cleanup_timeout'));}
  function stop({keepalive=false,reason='user_stop',preservePlayback=false,returnMicrophoneHandoff=false}={}){
    onTiming('stop_click',{reason});const url=sessionId?`${root}/${encodeURIComponent(sessionId)}/stop`:null;
    socketTransport?.close({sendStop:true,reason});socketTransport=null;reconnectPromise=null;
    ++generation;abort?.abort();abort=null;clearConfirmation();clearWait();releaseStartupCapture();
    const returnedStream=returnMicrophoneHandoff?closeMic({stopTracks:false}):(closeMic(),null);
    clearTimeout(pollTimer);pollTimer=null;
    if(!preservePlayback)stopPlayback(reason);inputTranscript='';transcriptAt=0;suppressResponseReason=null;sessionId=null;model=null;pendingTools.clear();cursor=0;starting=false;microphoneEnabled=false;budgetPaused=false;attemptId=null;socketUrl=null;connectionGeneration=0;
    const microphone_handoff=returnedStream?{stream:returnedStream}:null;
    onState('off',{reason,microphone_handoff});onTiming('local_ui_off',{microphone_handoff:Boolean(microphone_handoff)});if(url)remoteStop(url,keepalive);
    return microphone_handoff;
  }
  async function start({url,body={},authorize=async()=>{},takeMicrophoneHandoff=null,microphone=true,captureDuringStart=false}){
    if(sessionId||starting)return;
    stopPlayback('new_session');const epoch=++generation;abort=new AbortController();starting=true;root=url;microphoneEnabled=Boolean(microphone);attemptId=`attempt_${globalThis.crypto?.randomUUID?.()?.replaceAll('-','')??Math.random().toString(36).slice(2)+Date.now().toString(36)}`;onState('starting',{attempt_id:attemptId,transport});
    try{
      await authorize();if(epoch!==generation)return;
      if(microphoneEnabled&&captureDuringStart&&typeof takeMicrophoneHandoff!=='function')await beginStartupCapture(epoch);
      if(epoch!==generation)return;
      const started=await request(url,{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({...body,attempt_id:attemptId,transport})});
      if(epoch!==generation){remoteStop(`${url}/${encodeURIComponent(started.session_id)}/stop`);return;}
      sessionId=started.session_id;model=started.model;cursor=0;onTiming('model_started',{model:started.model,attempt_id:attemptId,transport});
      if(transport==='wss'){
        if(started.transport_protocol!=='wl-live-v1'||typeof started.socket_ticket!=='string'||typeof started.socket_url!=='string')throw Object.assign(new Error('Server did not provide the required WSS transport'),{code:'LIVE_SOCKET_REQUIRED'});
        socketUrl=started.socket_url;connectionGeneration=1;
        socketTransport=createLiveSocketTransport({WebSocketImpl,onTiming,
          onEvent:event=>{void handleEvent(event,epoch);},
          onError:error=>{if(epoch===generation)onNotice('connection_error',error);},
          onClose:detail=>{if(epoch!==generation||detail.expected)return;void reconnectSocket(epoch);}
        });
        await socketTransport.connect({url:socketUrl,ticket:started.socket_ticket,attempt_id:attemptId,cursor,connection_generation:connectionGeneration});
      }
      const {socket_ticket,...startedPublic}=started;onState('started',startedPublic);
      const Context=Audio();if(Context){playContext??=new Context();await playContext.resume().catch(()=>{});}
      if(epoch!==generation)return;
      starting=false;if(transport==='http')void poll();
      const handoff=microphoneEnabled
        ?typeof takeMicrophoneHandoff==='function'
          ?await takeMicrophoneHandoff()
          :captureDuringStart
            ?takeStartupCapture(epoch)
            :null
        :null;
      if(epoch!==generation){handoff?.stream?.getTracks?.().forEach(track=>track.stop());return;}
      if(microphoneEnabled)await startMic(epoch,handoff);return startedPublic;
    }catch(error){if(epoch!==generation)return;const microphoneFailure=['MICROPHONE_UNAVAILABLE','AUDIO_WORKLET_UNAVAILABLE'].includes(error?.code);stop();onState(microphoneFailure?'microphone_unavailable':'start_error');onNotice(microphoneFailure?'microphone_error':'start_error',error);}
  }
  async function enableMicrophone({takeMicrophoneHandoff=null}={}){
    if(!sessionId)return false;
    microphoneEnabled=true;
    if(microphone?.running)return true;
    const epoch=generation;
    const handoff=typeof takeMicrophoneHandoff==='function'?await takeMicrophoneHandoff():null;
    if(epoch!==generation){handoff?.stream?.getTracks?.().forEach(track=>track.stop());return false;}
    return startMic(epoch,handoff);
  }
  function disableMicrophone(){microphoneEnabled=false;closeMic();}
  return {start,stop,input,enableMicrophone,disableMicrophone,suppressCurrentResponse,get sessionId(){return sessionId;},get starting(){return starting;},get generation(){return generation;},get playingCount(){return playing.size;},get microphoneEnabled(){return microphoneEnabled;},get responseSuppressed(){return Boolean(suppressResponseReason);}};
}
import {createLiveAudioSender} from './live-audio.js';
import {isLiveStopCommand,liveStopConfirmation} from './live-commands.js';

export async function liveJson(url,options){
  const response=await fetch(url,{cache:'no-store',credentials:'same-origin',signal:AbortSignal.timeout(30000),...options});
  const payload=await response.json().catch(()=>({}));
  if(!response.ok){const error=new Error(payload?.error?.message??`HTTP ${response.status}`);error.status=response.status;throw error;}
  return payload;
}
function base64(bytes){let text='';for(let i=0;i<bytes.length;i+=0x8000)text+=String.fromCharCode(...bytes.subarray(i,i+0x8000));return btoa(text);}
function pcm16(samples,fromRate){
  const ratio=fromRate/16000,count=Math.max(1,Math.floor(samples.length/ratio)),out=new Int16Array(count);
  for(let i=0;i<count;i++){const value=Math.max(-1,Math.min(1,samples[Math.min(samples.length-1,Math.floor(i*ratio))]));out[i]=value<0?value*32768:value*32767;}
  return out;
}

// UI, authentication and domain tools are host concerns. All audio/lifecycle paths
// go through this client, including Stop while setup or a poll is still pending.
export function createLiveClient({request=liveJson,onEvent=()=>{},onState=()=>{},onNotice=()=>{},onTiming=()=>{},onWait=()=>{},voiceControl={isStop:isLiveStopCommand,confirmation:liveStopConfirmation}}={}){
  let model=null,sessionId=null,starting=false,generation=0,root=null,abort=null,cursor=0,pollTimer=null;
  let stream=null,micContext=null,processor=null,inputSource=null,sender=null,microphoneEnabled=false,startupCapture=null;
  let playContext=null,nextPlayAt=0,playing=new Set(),inputTranscript='',transcriptAt=0;
  let stopPending=false,stopExpiry=null,stopConfirmTimer=null;
  let waitAt=null,waitTimer=null,waitStage='transport',awaitingReply=false;
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
  function stopPlayback(reason='user_stop'){
    if(playing.size)onTiming('playback_cancelled',{reason,queued_buffers:playing.size,remaining_ms:Math.max(0,(nextPlayAt-playContext.currentTime)*1000)});
    for(const node of playing){try{node.liveCancelled=true;node.stop();}catch{}}
    playing.clear();nextPlayAt=0;
  }
  async function play(event,epoch){
    const Context=Audio();if(!Context)return;
    playContext??=new Context();if(playContext.state==='suspended')await playContext.resume().catch(()=>{});
    if(epoch!==generation||!sessionId)return;
    const raw=atob(event.data),bytes=Uint8Array.from(raw,c=>c.charCodeAt(0));
    const rate=Number(/rate=(\d+)/.exec(event.mime_type??'')?.[1]??24000),samples=new Int16Array(bytes.buffer,bytes.byteOffset,Math.floor(bytes.byteLength/2));
    const buffer=playContext.createBuffer(1,samples.length,rate),channel=buffer.getChannelData(0);for(let i=0;i<samples.length;i++)channel[i]=samples[i]/32768;
    const source=playContext.createBufferSource();source.buffer=buffer;source.connect(playContext.destination);
    const at=Math.max(playContext.currentTime+.02,nextPlayAt||0);nextPlayAt=at+buffer.duration;playing.add(source);
    onTiming('audio_scheduled',{seq:event.seq,pcm_bytes:bytes.byteLength,duration_ms:buffer.duration*1000,starts_at:Date.now()+(at-playContext.currentTime)*1000,buffered_ms:(nextPlayAt-playContext.currentTime)*1000});
    source.onended=()=>{playing.delete(source);source.disconnect();onTiming(source.liveCancelled?'audio_cancelled':'audio_played',{seq:event.seq,duration_ms:buffer.duration*1000});};source.start(at);
  }
  function input(message){
    if(!sessionId)return Promise.resolve();
    if(message.text){awaitingReply=true;beginWait();}
    const audio=message.audio_base64!==undefined||message.audio_stream_end===true;
    // The ordered audio sender owns the tight steady-state liveness bound:
    // 1.5s queued PCM / 2.5s item age. A longer absolute HTTP ceiling here
    // lets an intentional startup handoff drain without a false local timeout,
    // while a genuinely stalled steady stream still fails via the sender first.
    const timeoutMs=audio?10000:2500;
    return request(`${root}/${encodeURIComponent(sessionId)}/input`,{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(message),signal:AbortSignal.any([abort.signal,AbortSignal.timeout(timeoutMs)])});
  }
  const microphoneConstraints={audio:{channelCount:1,echoCancellation:true,noiseSuppression:true,autoGainControl:true},video:false};
  function releaseStartupCapture({stopTracks=true,event='startup_capture_cancelled'}={}){
    const capture=startupCapture;startupCapture=null;
    if(!capture)return null;
    capture.processor.onaudioprocess=null;
    try{capture.processor.disconnect();capture.inputSource.disconnect();}catch{}
    if(stopTracks)for(const track of capture.stream.getTracks())track.stop();
    void capture.context.close().catch(()=>{});
    onTiming(event,{buffered_chunks:capture.chunks.length,buffered_samples:capture.samples,sample_rate:capture.sampleRate});
    return stopTracks?null:{stream:capture.stream,sampleRate:capture.sampleRate,chunks:capture.chunks};
  }
  async function beginStartupCapture(epoch){
    if(startupCapture||!microphoneEnabled)return startupCapture;
    if(!navigator.mediaDevices?.getUserMedia){const error=new Error('Microphone unavailable');error.code='MICROPHONE_UNAVAILABLE';throw error;}
    const captured=await navigator.mediaDevices.getUserMedia(microphoneConstraints);
    if(epoch!==generation||!microphoneEnabled){for(const track of captured.getTracks())track.stop();return null;}
    const Context=Audio();if(!Context){for(const track of captured.getTracks())track.stop();const error=new Error('AudioContext unavailable');error.code='MICROPHONE_UNAVAILABLE';throw error;}
    const context=new Context(),inputSource=context.createMediaStreamSource(captured),processor=context.createScriptProcessor(4096,1,1);
    const capture={epoch,stream:captured,context,inputSource,processor,chunks:[],samples:0,sampleRate:context.sampleRate,maxSamples:Math.max(1,Math.floor(context.sampleRate*20))};
    startupCapture=capture;
    processor.onaudioprocess=event=>{
      if(epoch!==generation||startupCapture!==capture)return;
      const chunk=new Float32Array(event.inputBuffer.getChannelData(0));
      capture.chunks.push(chunk);capture.samples+=chunk.length;
      while(capture.samples>capture.maxSamples&&capture.chunks.length>1){const removed=capture.chunks.shift();capture.samples-=removed.length;}
    };
    inputSource.connect(processor);processor.connect(context.destination);
    await context.resume().catch(()=>{});
    onTiming('startup_capture_started',{sample_rate:capture.sampleRate,max_buffer_ms:20000});
    return capture;
  }
  function takeStartupCapture(epoch){
    if(!startupCapture||startupCapture.epoch!==epoch)return null;
    return releaseStartupCapture({stopTracks:false,event:'startup_capture_handoff'});
  }

  function closeMic(){
    sender?.stop();sender=null;
    if(processor)processor.onaudioprocess=null;
    try{processor?.disconnect();inputSource?.disconnect();}catch{}
    for(const track of stream?.getTracks?.()??[]){track.stop();onTiming('microphone_track_stopped');}
    const context=micContext;processor=inputSource=stream=micContext=null;
    if(context)void context.close().catch(()=>{});
  }
  async function startMic(epoch,handoff=null){
    if(!microphoneEnabled)return false;
    if(!handoff&&!navigator.mediaDevices?.getUserMedia){onState('microphone_unavailable');return false;}
    try{
      const captured=handoff?.stream??await navigator.mediaDevices.getUserMedia(microphoneConstraints);
      if(epoch!==generation||!microphoneEnabled){for(const track of captured.getTracks())track.stop();return false;}
      stream=captured;const Context=Audio();if(!Context)throw new Error('AudioContext unavailable');
      micContext=new Context();inputSource=micContext.createMediaStreamSource(stream);processor=micContext.createScriptProcessor(4096,1,1);
      sender=createLiveAudioSender({send:message=>input(message.pcm?{audio_base64:base64(new Uint8Array(message.pcm.buffer))}:message),onTiming:(event,metrics)=>{
        onTiming(event,metrics);if(event==='speech_start'){inputTranscript='';awaitingReply=true;clearWait();}if(event==='speech_end'&&awaitingReply&&!playing.size)beginWait();
      },onError:error=>{if(epoch!==generation)return;stop({reason:'transport_error',preservePlayback:true});onNotice('transport_error',error);}});
      const handoffFrames=[];
      for(const chunk of handoff?.chunks??[]){
        const samples=chunk instanceof Float32Array?chunk:new Float32Array(chunk),pcm=pcm16(samples,handoff.sampleRate);
        let energy=0;for(let i=0;i<samples.length;i++)energy+=samples[i]*samples[i];
        handoffFrames.push({pcm,rms:Math.sqrt(energy/Math.max(1,samples.length))});
      }
      if(handoffFrames.length&&!sender.seed(handoffFrames))return false;
      processor.onaudioprocess=event=>{
        if(epoch!==generation||!sessionId)return;
        const samples=event.inputBuffer.getChannelData(0);let energy=0;for(let i=0;i<samples.length;i++)energy+=samples[i]*samples[i];
        // Only provider interruption or an explicit Stop cancels model playback.
        sender.push(pcm16(samples,micContext.sampleRate),Math.sqrt(energy/samples.length));
      };
      inputSource.connect(processor);processor.connect(micContext.destination);onState('listening');return true;
    }catch(error){if(epoch!==generation)return false;closeMic();onState('microphone_unavailable');onNotice('microphone_error',error);return false;}
  }
  async function poll(){
    if(!sessionId)return;const epoch=generation;
    try{
      const result=await request(`${root}/${encodeURIComponent(sessionId)}/events?after=${cursor}`,{signal:AbortSignal.any([abort.signal,AbortSignal.timeout(5000)])});
      if(epoch!==generation)return;
      if(result.gap)onNotice('event_gap');
      for(const event of result.events??[]){
        if(epoch!==generation)return;
        if(['input_transcript','tool_call','tool_result','turn_complete','input_timing'].includes(event.type))onTiming(event.type,{provider_at:event.provider_at,server_at:event.at,name:event.name,duration_ms:event.duration_ms,...(event.type==='input_timing'?{max_stdin_delay_ms:event.max_stdin_delay_ms,max_ws_send_ms:event.max_ws_send_ms}: {})});
        if(event.type==='input_transcript'){
          waitStage=pendingTools.size?'action':'provider';
          if(Date.now()-transcriptAt>2000)inputTranscript='';
          transcriptAt=Date.now();inputTranscript=(inputTranscript+' '+event.text).trim().slice(-1000);voice();
        }else if(event.type==='audio'){
          awaitingReply=false;clearWait();if(!playing.size)onTiming('first_output_audio',{server_at:event.at,provider_at:event.provider_at});onState('answering');await play(event,epoch);
        }else if(event.type==='input_timing'){
          // Compare events within the server clock only; browser clock may differ.
          if(waitAt!==null&&(event.audio_stream_end_sent_at||event.text_sent_at))waitStage=pendingTools.size?'action':'provider';
        }else if(event.type==='interrupted'){clearWait();onTiming('provider_interrupted');stopPlayback('provider_interrupted');}
        else if(event.type==='tool_call'){for(const call of event.calls??[])pendingTools.add(call.id);waitStage='action';}
        else if(event.type==='tool_result'){pendingTools.delete(event.id);if(!pendingTools.size)waitStage='provider';}
        else if(event.type==='tool_cancelled'){for(const id of event.ids??[])pendingTools.delete(id);if(!pendingTools.size)waitStage='provider';}
        else if(event.type==='interaction_status'&&event.status==='IDLE'){awaitingReply=false;clearWait();}
        else if(event.type==='turn_complete'){
          if(!model?.endsWith('-extended-thinking')){awaitingReply=false;clearWait();}if(stopPending&&inputTranscript&&!stopConfirmTimer)clearConfirmation();if(!stopConfirmTimer)inputTranscript='';
        }else if(event.type==='reconnecting'){closeMic();onState('reconnecting');}
        else if(event.type==='resumed'&&microphoneEnabled){void startMic(epoch);}
        if(epoch!==generation)return;
        onEvent(event,epoch);cursor=event.seq??cursor;
      }
      cursor=result.cursor??cursor;
      if(epoch!==generation)return;
      if(result.closed){stop({reason:'provider_closed',preservePlayback:true});return;}
      if(result.has_more){pollTimer=setTimeout(()=>void poll(),0);return;}
    }catch(error){if(epoch!==generation)return;onState('connection_error');onNotice('connection_error',error);}
    if(epoch===generation)pollTimer=setTimeout(()=>void poll(),160);
  }
  function remoteStop(url,keepalive=false){onTiming('stop_request');void request(url,{method:'POST',headers:{'content-type':'application/json'},body:'{}',keepalive,signal:AbortSignal.timeout(2500)}).then(()=>onTiming('stop_response')).catch(()=>onTiming('stop_cleanup_timeout'));}
  function stop({keepalive=false,reason='user_stop',preservePlayback=false}={}){
    onTiming('stop_click',{reason});const url=sessionId?`${root}/${encodeURIComponent(sessionId)}/stop`:null;
    ++generation;abort?.abort();abort=null;clearConfirmation();clearWait();releaseStartupCapture();closeMic();clearTimeout(pollTimer);pollTimer=null;
    if(!preservePlayback)stopPlayback(reason);inputTranscript='';transcriptAt=0;sessionId=null;model=null;pendingTools.clear();cursor=0;starting=false;microphoneEnabled=false;
    onState('off',{reason});onTiming('local_ui_off');if(url)remoteStop(url,keepalive);
  }
  async function start({url,body={},authorize=async()=>{},takeMicrophoneHandoff=null,microphone=true,captureDuringStart=false}){
    if(sessionId||starting)return;
    stopPlayback('new_session');const epoch=++generation;abort=new AbortController();starting=true;root=url;microphoneEnabled=Boolean(microphone);onState('starting');
    try{
      await authorize();if(epoch!==generation)return;
      if(microphoneEnabled&&captureDuringStart&&typeof takeMicrophoneHandoff!=='function')await beginStartupCapture(epoch);
      if(epoch!==generation)return;
      const started=await request(url,{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify(body)});
      if(epoch!==generation){remoteStop(`${url}/${encodeURIComponent(started.session_id)}/stop`);return;}
      sessionId=started.session_id;model=started.model;cursor=0;onTiming('model_started',{model:started.model});onState('started',started);
      const Context=Audio();if(Context){playContext??=new Context();await playContext.resume().catch(()=>{});}
      if(epoch!==generation)return;
      starting=false;void poll();
      const handoff=microphoneEnabled
        ?typeof takeMicrophoneHandoff==='function'
          ?await takeMicrophoneHandoff()
          :captureDuringStart
            ?takeStartupCapture(epoch)
            :null
        :null;
      if(epoch!==generation){handoff?.stream?.getTracks?.().forEach(track=>track.stop());return;}
      if(microphoneEnabled)await startMic(epoch,handoff);return started;
    }catch(error){if(epoch!==generation)return;const microphoneFailure=error?.code==='MICROPHONE_UNAVAILABLE';stop();onState(microphoneFailure?'microphone_unavailable':'start_error');onNotice(microphoneFailure?'microphone_error':'start_error',error);}
  }
  async function enableMicrophone({takeMicrophoneHandoff=null}={}){
    if(!sessionId)return false;
    microphoneEnabled=true;
    if(stream)return true;
    const epoch=generation;
    const handoff=typeof takeMicrophoneHandoff==='function'?await takeMicrophoneHandoff():null;
    if(epoch!==generation){handoff?.stream?.getTracks?.().forEach(track=>track.stop());return false;}
    return startMic(epoch,handoff);
  }
  function disableMicrophone(){microphoneEnabled=false;closeMic();}
  return {start,stop,input,enableMicrophone,disableMicrophone,get sessionId(){return sessionId;},get starting(){return starting;},get generation(){return generation;},get playingCount(){return playing.size;},get microphoneEnabled(){return microphoneEnabled;}};
}

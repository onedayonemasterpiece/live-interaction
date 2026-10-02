import {createLiveAudioSender} from './live-audio.js';

export const microphoneConstraints={audio:{channelCount:1,echoCancellation:true,noiseSuppression:true,autoGainControl:true},video:false};

export function createPcm16Resampler(fromRate,targetRate=16000){
  if(!Number.isFinite(fromRate)||fromRate<=0||!Number.isFinite(targetRate)||targetRate<=0)throw new TypeError('Invalid sample rate');
  let totalInput=0,outputSamples=0,phase=0;
  function push(samples){
    if(!(samples instanceof Float32Array))samples=new Float32Array(samples??[]);
    const values=[];
    for(let i=0;i<samples.length;i++){
      phase+=targetRate;
      while(phase>=fromRate){
        const value=Math.max(-1,Math.min(1,samples[i]));
        values.push(value<0?Math.round(value*32768):Math.round(value*32767));
        phase-=fromRate;
      }
    }
    totalInput+=samples.length;outputSamples+=values.length;
    return Int16Array.from(values);
  }
  function reset(){totalInput=0;outputSamples=0;phase=0;}
  return {push,reset,get inputSamples(){return totalInput;},get outputSamples(){return outputSamples;}};
}

export function pcm16(samples,fromRate){
  return createPcm16Resampler(fromRate).push(samples);
}

export function frameRms(samples){
  let energy=0;
  for(let i=0;i<samples.length;i++)energy+=samples[i]*samples[i];
  return Math.sqrt(energy/Math.max(1,samples.length));
}

export function createMicrophoneCapture({
  onFrame:initialOnFrame=()=>{},
  onTiming=()=>{},
  onError=()=>{},
  now=()=>globalThis.performance?.now?.()??Date.now(),
  maxPendingFrames=32,
  workletUrl=new URL('./capture-worklet.js',import.meta.url),
  WorkletNode=globalThis.AudioWorkletNode,
}={}){
  let stream=null,context=null,inputSource=null,processor=null,running=false,failed=false,pending=0,generation=0,resampler=null;
  let frameHandler=initialOnFrame,chain=Promise.resolve();
  const Audio=()=>globalThis.AudioContext||globalThis.webkitAudioContext;

  function closeHardware(stopTracks=true){
    if(processor?.port)processor.port.onmessage=null;
    else if(processor)processor.onaudioprocess=null;
    try{processor?.disconnect();inputSource?.disconnect();}catch{}
    if(stopTracks)for(const track of stream?.getTracks?.()??[])track.stop();
    const old=context;
    processor=inputSource=stream=context=null;
    if(old)void old.close().catch(()=>{});
  }

  function fail(error){
    if(failed)return;
    failed=true;running=false;closeHardware(true);onTiming('microphone_capture_error');
    onError(error);
  }

  async function start({stream:provided=null,constraints=microphoneConstraints}={}){
    if(running)return true;
    const epoch=++generation;
    failed=false;
    if(!provided&&!globalThis.navigator?.mediaDevices?.getUserMedia){
      const error=new Error('Microphone unavailable');error.code='MICROPHONE_UNAVAILABLE';throw error;
    }
    const captured=provided??await globalThis.navigator.mediaDevices.getUserMedia(constraints);
    if(epoch!==generation){for(const track of captured.getTracks?.()??[])track.stop();return false;}
    const Context=Audio();
    if(!Context){
      for(const track of captured.getTracks?.()??[])track.stop();
      const error=new Error('AudioContext unavailable');error.code='MICROPHONE_UNAVAILABLE';throw error;
    }
    stream=captured;context=new Context();
    const startedContext=context;
    if(!context.audioWorklet?.addModule||typeof WorkletNode!=='function'){
      closeHardware(true);
      const error=new Error('AudioWorklet unavailable');error.code='AUDIO_WORKLET_UNAVAILABLE';throw error;
    }
    await context.audioWorklet.addModule(String(workletUrl));
    if(epoch!==generation){closeHardware(true);return false;}
    inputSource=context.createMediaStreamSource(stream);
    processor=new WorkletNode(context,'live-microphone-capture-v1',{numberOfInputs:1,numberOfOutputs:1,outputChannelCount:[1]});
    resampler=createPcm16Resampler(context.sampleRate);
    running=true;
    processor.port.onmessage=event=>{
      if(!running)return;
      const samples=event.data instanceof Float32Array?event.data:new Float32Array(event.data??[]);
      const pcm=resampler.push(samples),rms=frameRms(samples),capturedAt=now();
      if(!pcm.length)return;
      pending++;
      if(pending>maxPendingFrames){
        pending--;
        const error=new Error('Durable microphone consumer is not keeping up with capture.');
        error.code='MICROPHONE_CAPTURE_BACKPRESSURE';
        fail(error);return;
      }
      chain=chain.then(()=>frameHandler(pcm,rms,{capture_at_ms:capturedAt,sample_rate:16000,input_sample_rate:context?.sampleRate??0}))
        .catch(error=>fail(error))
        .finally(()=>{pending=Math.max(0,pending-1);});
    };
    inputSource.connect(processor);processor.connect(context.destination);
    await startedContext.resume().catch(()=>{});
    if(epoch!==generation){
      if(stream===captured)closeHardware();
      else{for(const track of captured.getTracks?.()??[])track.stop();void startedContext.close().catch(()=>{});}
      return false;
    }
    const track=stream?.getAudioTracks?.()?.[0]??stream?.getTracks?.()?.[0];
    const settings=track?.getSettings?.()??{};
    onTiming('microphone_capture_started',{sample_rate:context.sampleRate,context_state:context.state,processor:'audio-worklet',max_pending_frames:maxPendingFrames,
      device_sample_rate:Number(settings.sampleRate)||undefined,channel_count:Number(settings.channelCount)||undefined,echo_cancellation:settings.echoCancellation,noise_suppression:settings.noiseSuppression,auto_gain_control:settings.autoGainControl});
    return true;
  }

  function stop({stopTracks=true}={}){
    ++generation;
    if(!running&&!stream)return null;
    const retained=stopTracks?null:stream;
    running=false;closeHardware(stopTracks);onTiming('microphone_capture_stopped',{pending_frames:pending,tracks_retained:Boolean(retained)});
    return retained;
  }

  async function drain(){await chain;}
  function setOnFrame(callback){if(typeof callback!=='function')throw new TypeError('onFrame callback is required');frameHandler=callback;}

  return {
    start,stop,drain,setOnFrame,
    get running(){return running;},
    get stream(){return stream;},
    get pendingFrames(){return pending;},
  };
}

export function createDurableMicrophoneCapture({
  persist,
  onTiming=()=>{},
  onError=()=>{},
  now=()=>globalThis.performance?.now?.()??Date.now(),
  batchMs=256,
  maxQueueMs=1500,
  maxAgeMs=2500,
}={}){
  if(typeof persist!=='function')throw new TypeError('persist callback is required');
  let microphone=null;
  const fail=error=>{microphone?.stop();onError(error);};
  const sender=createLiveAudioSender({
    send:async()=>{},
    persist,
    onTiming,
    onError:fail,
    now,
    batchMs,
    maxQueueMs,
    maxAgeMs,
  });
  microphone=createMicrophoneCapture({
    onFrame:(pcm,rms)=>sender.push(pcm,rms),
    onTiming,
    onError:error=>{sender.stop();onError(error);},
    now,
  });
  let stopping=null;
  async function stop(){
    if(stopping)return stopping;
    stopping=(async()=>{
      microphone.stop();
      await microphone.drain();
      await sender.finish();
      await sender.drainDurable();
      sender.stop();
      onTiming('durable_microphone_capture_stopped');
    })();
    return stopping;
  }
  return {
    start:options=>microphone.start(options),
    stop,
    stats:()=>({...sender.stats(),microphone_pending_frames:microphone.pendingFrames}),
    get running(){return microphone.running;},
  };
}
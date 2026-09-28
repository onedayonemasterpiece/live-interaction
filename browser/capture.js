import {createLiveAudioSender} from './live-audio.js';

export const microphoneConstraints={audio:{channelCount:1,echoCancellation:true,noiseSuppression:true,autoGainControl:true},video:false};

export function pcm16(samples,fromRate){
  const ratio=fromRate/16000,count=Math.max(1,Math.floor(samples.length/ratio)),out=new Int16Array(count);
  for(let i=0;i<count;i++){
    const value=Math.max(-1,Math.min(1,samples[Math.min(samples.length-1,Math.floor(i*ratio))]));
    out[i]=value<0?value*32768:value*32767;
  }
  return out;
}

export function frameRms(samples){
  let energy=0;
  for(let i=0;i<samples.length;i++)energy+=samples[i]*samples[i];
  return Math.sqrt(energy/Math.max(1,samples.length));
}

export function createMicrophoneCapture({
  onFrame=()=>{},
  onTiming=()=>{},
  onError=()=>{},
  now=()=>globalThis.performance?.now?.()??Date.now(),
  maxPendingFrames=32,
}={}){
  let stream=null,context=null,inputSource=null,processor=null,running=false,failed=false,pending=0,generation=0;
  let chain=Promise.resolve();
  const Audio=()=>globalThis.AudioContext||globalThis.webkitAudioContext;

  function closeHardware(stopTracks=true){
    if(processor)processor.onaudioprocess=null;
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
    stream=captured;context=new Context();inputSource=context.createMediaStreamSource(stream);processor=context.createScriptProcessor(4096,1,1);
    running=true;
    processor.onaudioprocess=event=>{
      if(!running)return;
      const source=event.inputBuffer.getChannelData(0);
      const samples=new Float32Array(source);
      const pcm=pcm16(samples,context.sampleRate),rms=frameRms(samples),capturedAt=now();
      pending++;
      if(pending>maxPendingFrames){
        pending--;
        const error=new Error('Durable microphone consumer is not keeping up with capture.');
        error.code='MICROPHONE_CAPTURE_BACKPRESSURE';
        fail(error);return;
      }
      chain=chain.then(()=>onFrame(pcm,rms,{capture_at_ms:capturedAt,sample_rate:16000}))
        .catch(error=>fail(error))
        .finally(()=>{pending=Math.max(0,pending-1);});
    };
    inputSource.connect(processor);processor.connect(context.destination);
    const startedContext=context;
    await startedContext.resume().catch(()=>{});
    if(epoch!==generation){
      if(stream===captured)closeHardware();
      else{for(const track of captured.getTracks?.()??[])track.stop();void startedContext.close().catch(()=>{});}
      return false;
    }
    onTiming('microphone_capture_started',{sample_rate:context.sampleRate,max_pending_frames:maxPendingFrames});
    return true;
  }

  function stop({stopTracks=true}={}){
    ++generation;
    if(!running&&!stream)return;
    running=false;closeHardware(stopTracks);onTiming('microphone_capture_stopped',{pending_frames:pending});
  }

  async function drain(){await chain;}

  return {
    start,stop,drain,
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

// One ordered sender; at most 1.5 seconds of steady-state PCM waiting, never a promise per frame.
// PCM16/16kHz is the Gemini Live wire format. Keep each batch below 16000 base64 bytes.
// A microphone handoff may intentionally contain up to 20 seconds captured while Live starts.
// That seed gets a separate bounded catch-up window; once drained back to the normal queue
// watermark, the strict steady-state queue/age guards resume automatically.
export function createLiveAudioSender({
  send,
  onTiming=()=>{},
  onError=()=>{},
  now=()=>performance.now(),
  batchMs=256,
  maxQueueMs=1500,
  maxAgeMs=2500,
  maxBootstrapMs=20000
}={}){
  let queue=[],preRoll=[],bytes=0,busy=false,closed=false,active=false,quietMs=0,captured=0,suppressed=0,timer=null;
  let catchup=false,catchupSealed=false,catchupSeedBytes=0;
  const bytesPerSecond=32000;
  const steadyByteLimit=bytesPerSecond*maxQueueMs/1000;
  const catchupByteLimit=bytesPerSecond*(maxBootstrapMs+maxQueueMs)/1000;
  const maxCatchupItems=512;

  const stats=()=>({
    captured_chunks:captured,
    suppressed_chunks:suppressed,
    queued_chunks:queue.filter(x=>x.pcm).length,
    in_flight:busy?1:0,
    queued_pcm_bytes:bytes,
    queued_base64_bytes:Math.ceil(bytes/3)*4,
    oldest_age_ms:queue.some(x=>x.pcm)?Math.round(now()-queue.find(x=>x.pcm).at):0,
    catchup,
    catchup_seed_pcm_bytes:catchupSeedBytes
  });
  const report=(event,extra={})=>onTiming(event,{...stats(),...extra});
  const rebaseQueuedAge=()=>{
    const at=now();
    for(const item of queue)if(item.pcm)item.at=at;
    for(const item of preRoll)if(item.pcm)item.at=at;
  };
  const maybeFinishCatchup=()=>{
    if(!catchup||!catchupSealed||bytes>steadyByteLimit)return false;
    catchup=false;catchupSealed=false;rebaseQueuedAge();
    report('catchup_end',{seed_pcm_bytes:catchupSeedBytes});
    return true;
  };
  const stop=()=>{
    closed=true;clearTimeout(timer);timer=null;
    queue=[];preRoll=[];bytes=0;catchup=false;catchupSealed=false;
  };
  const fail=error=>{report('transport_error');stop();onError(error);};

  async function pump(){
    if(closed||busy||!queue.length)return;
    clearTimeout(timer);timer=null;
    maybeFinishCatchup();
    const first=queue[0],parts=[];let size=0;
    if(first.end)queue.shift();
    else{
      if(first.pcm&&!catchup&&now()-first.at>maxAgeMs){
        fail(new Error('Сеть не успевает передавать речь. Запустите Live снова.'));
        return;
      }
      while(queue.length&&queue[0].pcm&&size+queue[0].pcm.byteLength<=11000){
        const item=queue.shift();parts.push(item.pcm);size+=item.pcm.byteLength;bytes-=item.pcm.byteLength;
      }
    }
    const pcm=new Int16Array(size/2);let offset=0;
    for(const part of parts){pcm.set(part,offset);offset+=part.length;}
    busy=true;const at=now();
    report('post_start',{capture_at_ms:first.at,batch_chunks:parts.length,pcm_bytes:size,queue_age_ms:at-first.at,stream_end:Boolean(first.end)});
    try{
      await send(first.end?{audio_stream_end:true}:{pcm});
      report('post_end',{duration_ms:now()-at,pcm_bytes:size});
    }catch(error){
      if(!closed)fail(error);
    }finally{
      busy=false;
      maybeFinishCatchup();
      if(!closed&&queue.length)schedule();
    }
  }

  function enqueue(item){queue.push(item);bytes+=item.pcm?.byteLength??0;}
  function schedule(){
    if(closed)return;
    maybeFinishCatchup();
    const byteLimit=catchup?catchupByteLimit:steadyByteLimit;
    const itemLimit=catchup?maxCatchupItems:80;
    if(bytes>byteLimit||queue.length>itemLimit){
      fail(new Error('Сеть не успевает передавать речь. Запустите Live снова.'));
      return;
    }
    if(busy)return;
    if(queue.some(x=>x.end)||bytes>=bytesPerSecond*batchMs/1000)void pump();
    else if(queue.length&&!timer)timer=setTimeout(()=>{timer=null;void pump();},Math.max(0,batchMs-(now()-queue[0].at)));
  }

  function push(pcm,rms){
    if(closed)return;
    captured++;const at=now(),duration=pcm.length/16,item={pcm,at};
    // Conservative energy gate: 250ms pre-roll preserves onsets; a 2s tail covers
    // provider VAD (700/1200ms tails failed real Gemini audio acceptance).
    // Responses can start before the tail finishes; prolonged idle silence is not sent.
    // Server VAD still decides turns; stream_end flushes the final tail before idle silence.
    if(rms>=0.008){
      if(!active){
        active=true;
        for(const previous of preRoll)enqueue(previous);
        preRoll=[];
        report('speech_start',{capture_at_ms:at});
      }
      quietMs=0;enqueue(item);
    }else if(active){
      enqueue(item);quietMs+=duration;
      if(quietMs>=2000){
        active=false;enqueue({end:true,at});report('speech_end',{capture_at_ms:at});
      }
    }else{
      suppressed++;preRoll.push(item);
      while(preRoll.reduce((n,x)=>n+x.pcm.length/16,0)>250+duration)preRoll.shift();
    }
    if(captured%12===0)report('capture',{capture_at_ms:at});
    schedule();
  }

  function seed(frames=[]){
    if(closed||catchup)return false;
    if(!Array.isArray(frames))return false;
    const seedBytes=frames.reduce((total,frame)=>total+(frame?.pcm?.byteLength??0),0);
    if(seedBytes>bytesPerSecond*maxBootstrapMs/1000){
      fail(new Error('Стартовый буфер речи превышает допустимый предел.'));
      return false;
    }
    catchup=true;catchupSealed=false;catchupSeedBytes=seedBytes;
    report('catchup_start',{seed_pcm_bytes:seedBytes,max_bootstrap_ms:maxBootstrapMs});
    for(const frame of frames){
      if(closed)return false;
      if(!(frame?.pcm instanceof Int16Array)||!Number.isFinite(frame?.rms)){
        fail(new Error('Стартовый буфер речи повреждён.'));
        return false;
      }
      push(frame.pcm,frame.rms);
    }
    catchupSealed=true;
    maybeFinishCatchup();
    schedule();
    return !closed;
  }

  return {push,seed,stop,stats};
}

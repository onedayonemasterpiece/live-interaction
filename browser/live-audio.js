// One ordered sender; at most 2.5 seconds of steady-state PCM waiting, never a promise per frame.
// PCM16/16kHz is the Gemini Live wire format. Keep each batch below 16000 base64 bytes.
// A microphone handoff may intentionally contain up to 20 seconds captured while Live starts.
// That seed gets a separate bounded catch-up window; once drained below half the
// normal byte and fragment watermarks, the strict steady-state guards resume.
export function createLiveAudioSender({
  send,
  onTiming=()=>{},
  onError=()=>{},
  now=()=>performance.now(),
  batchMs=80,
  maxQueueMs=1500,
  maxAgeMs=2500,
  maxBootstrapMs=20000,
  persist=null
}={}){
  let queue=[],preRoll=[],bytes=0,busy=false,closed=false,active=false,quietMs=0,captured=0,suppressed=0,timer=null;
  let catchup=false,catchupSealed=false,catchupSeedBytes=0;
  let durableBytes=0,durableItems=0,durableChain=Promise.resolve(),durableError=null,lastStagedWasEnd=false;
  const bytesPerSecond=32000;
  const steadyByteLimit=bytesPerSecond*maxQueueMs/1000;
  const catchupByteLimit=bytesPerSecond*(maxBootstrapMs+maxQueueMs)/1000;
  // Startup handoff can contain thousands of tiny AudioWorklet quanta even when
  // the bounded PCM duration/byte budget is healthy. Keep a separate generous
  // object-count fuse for pathological fragmentation, but let the byte ceiling
  // remain the primary catch-up bound.
  // Render-quantum count is not a duration signal: Chrome can emit hundreds of tiny
  // 16 kHz fragments per second after a 48 kHz AudioWorklet resample. Bytes and
  // age are the authoritative liveness guards; keep only a large fragmentation fuse.
  const steadyItemLimit=4096;
  const maxCatchupItems=32768;
  const catchupExitItemLimit=Math.floor(steadyItemLimit/2);

  const stats=()=>({
    captured_chunks:captured,
    suppressed_chunks:suppressed,
    queued_chunks:queue.filter(x=>x.pcm).length,
    in_flight:busy?1:0,
    queued_pcm_bytes:bytes,
    queued_base64_bytes:Math.ceil(bytes/3)*4,
    oldest_age_ms:queue.some(x=>x.pcm)?Math.round(now()-queue.find(x=>x.pcm).at):0,
    catchup,
    catchup_seed_pcm_bytes:catchupSeedBytes,
    durable_pending_items:durableItems,
    durable_pending_pcm_bytes:durableBytes
  });
  const report=(event,extra={})=>onTiming(event,{...stats(),...extra});
  const rebaseQueuedAge=()=>{
    const at=now();
    for(const item of queue)if(item.pcm)item.at=at;
    for(const item of preRoll)if(item.pcm)item.at=at;
  };
  const maybeFinishCatchup=()=>{
    // Leave room for PCM captured while the next HTTP batch is in flight.
    if(!catchup||!catchupSealed||bytes>steadyByteLimit/2||queue.length>catchupExitItemLimit)return false;
    catchup=false;catchupSealed=false;rebaseQueuedAge();
    report('catchup_end',{seed_pcm_bytes:catchupSeedBytes});
    return true;
  };
  const stop=()=>{
    closed=true;clearTimeout(timer);timer=null;
    queue=[];preRoll=[];bytes=0;catchup=false;catchupSealed=false;
  };
  const fail=error=>{report('transport_error',{error_code:String(error?.code??'LIVE_AUDIO_TRANSPORT').slice(0,80)});stop();onError(error);};

  async function pump(){
    if(closed||busy||!queue.length)return;
    clearTimeout(timer);timer=null;
    maybeFinishCatchup();
    const first=queue[0],parts=[];let size=0;
    if(first.end)queue.shift();
    else{
      if(first.pcm&&!catchup&&now()-first.at>maxAgeMs){
        fail(Object.assign(new Error('Сеть не успевает передавать речь. Запустите Live снова.'),{code:'LIVE_AUDIO_QUEUE_AGE'}));
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
      await send(first.end?{audio_stream_end:true,captured_at_ms:first.at,age_ms:Math.max(0,now()-first.at)}:{pcm,captured_at_ms:first.at,age_ms:Math.max(0,now()-first.at)});
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
  function durableRelease(size){durableBytes=Math.max(0,durableBytes-size);durableItems=Math.max(0,durableItems-1);}
  function stage(item){
    lastStagedWasEnd=Boolean(item.end);
    if(typeof persist!=='function'){enqueue(item);return true;}
    const size=item.pcm?.byteLength??0,copy=item.pcm?{...item,pcm:new Int16Array(item.pcm)}:{...item};
    durableBytes+=size;durableItems++;
    const byteLimit=catchup?catchupByteLimit:steadyByteLimit;
    const itemLimit=catchup?maxCatchupItems:steadyItemLimit;
    const durableBytesExceeded=bytes+durableBytes>byteLimit,durableItemsExceeded=queue.length+durableItems>itemLimit;
    if(durableBytesExceeded||durableItemsExceeded){
      durableRelease(size);
      fail(Object.assign(new Error('Локальное сохранение речи не успевает за микрофоном.'),{code:durableBytesExceeded?'LIVE_AUDIO_DURABLE_BYTES':'LIVE_AUDIO_DURABLE_FRAGMENTS'}));
      return false;
    }
    durableChain=durableChain.then(async()=>{
      if(durableError){durableRelease(size);return;}
      try{
        await persist(copy.end
          ?{audio_stream_end:true,captured_at_ms:copy.at}
          :{pcm:copy.pcm,sample_rate:16000,captured_at_ms:copy.at});
      }catch(error){
        durableError=error;durableRelease(size);if(!closed)fail(error);return;
      }
      durableRelease(size);
      report('durable_commit',{pcm_bytes:size,stream_end:Boolean(copy.end)});
      if(!closed){enqueue(copy);schedule();}
    });
    return true;
  }
  async function drainDurable(){await durableChain;if(durableError)throw durableError;}
  async function finish(){
    if(!lastStagedWasEnd){
      active=false;quietMs=0;preRoll=[];
      stage({end:true,at:now()});
    }
    await drainDurable();
  }
  function schedule(){
    if(closed)return;
    maybeFinishCatchup();
    const byteLimit=catchup?catchupByteLimit:steadyByteLimit;
    const itemLimit=catchup?maxCatchupItems:steadyItemLimit;
    const queueBytesExceeded=bytes>byteLimit,queueItemsExceeded=queue.length>itemLimit;
    if(queueBytesExceeded||queueItemsExceeded){
      fail(Object.assign(new Error('Сеть не успевает передавать речь. Запустите Live снова.'),{code:queueBytesExceeded?'LIVE_AUDIO_QUEUE_BYTES':'LIVE_AUDIO_QUEUE_FRAGMENTS'}));
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
    // Keep onset conservative, but do not cut quiet words after a turn began.
    if(rms>=(active?0.003:0.008)){
      if(!active){
        active=true;
        for(const previous of preRoll)stage(previous);
        preRoll=[];
        report('speech_start',{capture_at_ms:at});
      }
      quietMs=0;stage(item);
    }else if(active){
      stage(item);quietMs+=duration;
      if(quietMs>=2000){
        active=false;stage({end:true,at});report('speech_end',{capture_at_ms:at});
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
      fail(Object.assign(new Error('Стартовый буфер речи превышает допустимый предел.'),{code:'LIVE_AUDIO_BOOTSTRAP_BYTES'}));
      return false;
    }
    catchup=true;catchupSealed=false;catchupSeedBytes=seedBytes;
    report('catchup_start',{seed_pcm_bytes:seedBytes,max_bootstrap_ms:maxBootstrapMs,max_catchup_items:maxCatchupItems});
    for(const frame of frames){
      if(closed)return false;
      if(!(frame?.pcm instanceof Int16Array)||!Number.isFinite(frame?.rms)){
        fail(Object.assign(new Error('Стартовый буфер речи повреждён.'),{code:'LIVE_AUDIO_BOOTSTRAP_INVALID'}));
        return false;
      }
      push(frame.pcm,frame.rms);
    }
    catchupSealed=true;
    maybeFinishCatchup();
    schedule();
    return !closed;
  }

  return {push,seed,finish,drainDurable,stop,stats};
}

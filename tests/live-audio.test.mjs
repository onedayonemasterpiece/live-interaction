import test from 'node:test';
import assert from 'node:assert/strict';
import {createLiveAudioSender} from '../browser/live-audio.js';
const tick=()=>new Promise(r=>setImmediate(r));
test('batches ordered PCM below server limit; silence gate keeps pre-roll and tail',async()=>{
 const sent=[],timings=[];let at=0;
 const sender=createLiveAudioSender({send:async m=>sent.push(m),now:()=>at,onTiming:(e,s)=>timings.push({e,...s})});
 const push=(value,rms)=>{at+=85;sender.push(new Int16Array(1365).fill(value),rms);};
 for(let i=0;i<30;i++)push(0,0);
 assert.equal(sent.length,0);
 for(let i=1;i<=12;i++){push(i,.05);await tick();}
 for(let i=0;i<30;i++){push(0,0);await tick();}
 assert.equal(sent.filter(m=>m.audio_stream_end).length,1);
 assert.ok(sent.filter(m=>m.pcm).every(m=>Math.ceil(m.pcm.byteLength/3)*4<=16000));
 const speech=sent.filter(m=>m.pcm).flatMap(m=>[...m.pcm]).filter(v=>v>0);
 assert.deepEqual(speech,Array.from({length:12},(_,i)=>Array(1365).fill(i+1)).flat());
 assert.ok(timings.some(t=>t.e==='speech_end'));sender.stop();
});
test('bounded handoff catch-up drains a startup backlog then restores the steady-state queue guard',async()=>{
 let releaseFirst,releaseSteady,error,holdSteady=false;const sent=[];
 const sender=createLiveAudioSender({
  send:message=>{
   sent.push(message);
   if(sent.length===1)return new Promise(resolve=>releaseFirst=resolve);
   if(holdSteady)return new Promise(resolve=>releaseSteady=resolve);
   return Promise.resolve();
  },
  onError:e=>error=e
 });
 const frames=Array.from({length:64},()=>({pcm:new Int16Array(1486).fill(1000),rms:.05}));
 assert.equal(sender.seed(frames),true);
 assert.equal(error,undefined);
 assert.equal(sender.stats().catchup,true);
 assert.equal(sent[0]?.startup_catchup,true);
 assert.ok(sender.stats().queued_pcm_bytes>48000);
 releaseFirst();
 for(let i=0;i<100&&sender.stats().catchup;i++)await tick();
 assert.equal(error,undefined);
 assert.equal(sender.stats().catchup,false);
 assert.ok(sender.stats().queued_pcm_bytes<=40000);
 holdSteady=true;
 for(let i=0;i<30&&!error;i++)sender.push(new Int16Array(1486).fill(1000),.05);
 assert.match(error?.message??'',/Сеть/);
 releaseSteady?.();await tick();
});
test('handoff catch-up accepts thousands of tiny AudioWorklet frames within the byte bound',async()=>{
 let releaseFirst,error;const sent=[];
 const sender=createLiveAudioSender({
  send:message=>{
   sent.push(message);
   if(sent.length===1)return new Promise(resolve=>releaseFirst=resolve);
   return Promise.resolve();
  },
  onError:value=>{error=value;}
 });
 // 4096 x 43 PCM16 samples is ~11 seconds at 16 kHz / 352 KiB, but represents
 // far more worklet callbacks than the old fixed 512-item catch-up fuse.
 const frames=Array.from({length:4096},()=>({pcm:new Int16Array(43).fill(1000),rms:.05}));
 assert.equal(sender.seed(frames),true);
 assert.equal(error,undefined);
 assert.equal(sender.stats().catchup,true);
 assert.ok(sender.stats().queued_chunks>512);
 releaseFirst();
 for(let i=0;i<200&&sender.stats().catchup;i++)await tick();
 assert.equal(error,undefined);
 assert.equal(sender.stats().catchup,false);
 sender.stop();
});
test('handoff catch-up still rejects startup PCM beyond the 20 second byte budget',()=>{
 let error;
 const sender=createLiveAudioSender({send:async()=>{},onError:value=>{error=value;}});
 const frames=Array.from({length:161},()=>({pcm:new Int16Array(2000).fill(1000),rms:.05}));
 assert.equal(sender.seed(frames),false);
 assert.match(error?.message??'',/Стартовый буфер речи превышает/);
 assert.equal(sender.stats().queued_pcm_bytes,0);
});
test('handoff catch-up retains headroom while a queued HTTP batch is in flight',async()=>{
 let error;const releases=[];
 const sender=createLiveAudioSender({
  send:()=>new Promise(resolve=>releases.push(resolve)),
  onError:e=>error=e
 });
 const frame=()=>({pcm:new Int16Array(1486).fill(1000),rms:.05});
 assert.equal(sender.seed(Array.from({length:64},frame)),true);
 for(let i=0;i<40&&sender.stats().queued_pcm_bytes>70000;i++){
  assert.ok(releases.length);
  releases.shift()();await tick();
 }
 assert.ok(sender.stats().queued_pcm_bytes<=70000);
 assert.ok(sender.stats().queued_pcm_bytes>40000);
 assert.equal(sender.stats().catchup,true);
 for(let i=0;i<5;i++)sender.push(frame().pcm,.05);
 assert.equal(error,undefined);
 for(let i=0;i<40&&sender.stats().catchup;i++){
  assert.ok(releases.length);
  releases.shift()();await tick();
 }
 assert.equal(sender.stats().catchup,false);
 assert.ok(sender.stats().queued_pcm_bytes<=40000);
 sender.stop();
 for(const release of releases)release();
});

test('tiny steady-state AudioWorklet fragments stay bounded by bytes/age rather than object count',async()=>{
 let release,error,count=0;
 const sender=createLiveAudioSender({send:()=>{count++;return count===1?new Promise(resolve=>release=resolve):Promise.resolve();},onError:value=>{error=value;}});
 for(let i=0;i<300;i++)sender.push(new Int16Array(43).fill(1000),.05);
 assert.equal(error,undefined);
 assert.ok(sender.stats().queued_chunks>80);
 assert.ok(sender.stats().queued_pcm_bytes<48000);
 release();
 for(let i=0;i<100&&sender.stats().queued_pcm_bytes;i++)await tick();
 assert.equal(error,undefined);
 sender.stop();
});

test('the former 48 kB / 1.5 s byte watermark does not stop a healthy in-flight turn',()=>{
 let error,count=0;
 const sender=createLiveAudioSender({send:()=>{count++;return new Promise(()=>{});},onError:value=>{error=value;}});
 // First push enters the in-flight batch. The remaining 17 x 1486-sample PCM16
 // frames are 50,524 bytes (~1.58 s at 32,000 bytes/s): above the old 48 kB
 // guard but safely below the existing 2.5 s age/ACK budget.
 for(let i=0;i<18;i++)sender.push(new Int16Array(1486).fill(1000),.05);
 assert.equal(count,1);
 assert.ok(sender.stats().queued_pcm_bytes>48000);
 assert.ok(sender.stats().queued_pcm_bytes<80000);
 assert.equal(error,undefined);
 sender.stop();
});

test('steady-state byte overflow reports a stable diagnostic code',()=>{
 let error,timing;
 const sender=createLiveAudioSender({send:()=>new Promise(()=>{}),onError:value=>{error=value;},onTiming:(event,metrics)=>{if(event==='transport_error')timing=metrics;}});
 for(let i=0;i<30&&!error;i++)sender.push(new Int16Array(1486).fill(1000),.05);
 assert.equal(error?.code,'LIVE_AUDIO_QUEUE_BYTES');
 assert.equal(timing?.error_code,'LIVE_AUDIO_QUEUE_BYTES');
 sender.stop();
});

test('slow transport is bounded; stop discards pending audio and never waits for network',async()=>{
 let release,count=0,error;const sender=createLiveAudioSender({send:()=>{count++;return new Promise(r=>release=r);},onError:e=>error=e});
 for(let i=0;i<45;i++)sender.push(new Int16Array(1365),.1);
 assert.equal(count,1);assert.match(error.message,/Сеть/);assert.equal(sender.stats().queued_pcm_bytes,0);
 sender.stop();release();await tick();assert.equal(count,1);
});
test('a short in-flight stall stays bounded below the 2.5 second steady-state queue',async()=>{
 let release,error,count=0;
 const sender=createLiveAudioSender({send:()=>{count++;return count===1?new Promise(resolve=>{release=resolve;}):Promise.resolve();},onError:value=>{error=value;}});
 for(let i=0;i<14;i++)sender.push(new Int16Array(1365).fill(1000),.1);
 assert.ok(sender.stats().queued_pcm_bytes>=30000);
 assert.ok(sender.stats().queued_pcm_bytes<=80000);
 assert.equal(error,undefined);
 const finishing=sender.finish();
 release();
 await finishing;
 for(let i=0;i<30&&sender.stats().queued_pcm_bytes;i++)await tick();
 assert.equal(error,undefined);
 assert.equal(sender.stats().queued_pcm_bytes,0);
 sender.stop();
});
test('stop during an in-flight request prevents all further sends',async()=>{
 let release,count=0;const sender=createLiveAudioSender({send:()=>{count++;return new Promise(r=>release=r);}});
 for(let i=0;i<12;i++)sender.push(new Int16Array(1365),.1);
 sender.stop();release();await tick();assert.equal(count,1);assert.equal(sender.stats().queued_chunks,0);
});

test('does not flush provider audio before its measured silence window',async()=>{
 const sent=[];let at=0;
 const sender=createLiveAudioSender({send:async m=>sent.push(m),now:()=>at});
 sender.push(new Int16Array(1600).fill(1000),.05);await tick();
 for(let i=0;i<19;i++){at+=100;sender.push(new Int16Array(1600),0);await tick();}
 assert.equal(sent.filter(m=>m.audio_stream_end).length,0);
 at+=100;sender.push(new Int16Array(1600),0);await tick();
 assert.equal(sent.filter(m=>m.audio_stream_end).length,1);sender.stop();
});

test('quiet speech above the reduced gate stays in the same audio turn',async()=>{
 const sent=[];let at=0;
 const sender=createLiveAudioSender({send:async message=>sent.push(message),now:()=>at});
 sender.push(new Int16Array(1600).fill(1000),.05);await tick();
 for(let i=0;i<30;i++){at+=100;sender.push(new Int16Array(1600).fill(100),.004);await tick();}
 assert.equal(sent.filter(message=>message.audio_stream_end).length,0);
 assert.ok(sent.some(message=>message.pcm?.some(sample=>sample===100)));
 sender.stop();
});

test('a delayed end-of-stream marker is flushed even after the audio age bound',async()=>{
 let at=0,release,error;const sent=[];
 const sender=createLiveAudioSender({now:()=>at,send:m=>{sent.push(m);if(m.pcm&&at===2000)return new Promise(r=>release=r);return Promise.resolve();},onError:e=>error=e});
 sender.push(new Int16Array(1600),.05);await tick();
 for(let i=0;i<20;i++){at=(i+1)*100;sender.push(new Int16Array(1600),0);await tick();}
 assert.ok(release);assert.equal(sender.stats().queued_pcm_bytes,0);at+=3000;release();await tick();
 assert.equal(error,undefined);assert.equal(sent.at(-1).audio_stream_end,true);assert.equal(sender.stats().oldest_age_ms,0);sender.stop();
});


test('durable receipt is completed before provider transport sees accepted PCM',async()=>{
  let releasePersist;const order=[],sent=[];
  const sender=createLiveAudioSender({
    batchMs:0,
    persist:message=>{order.push(message.pcm?'persist_pcm':'persist_end');return new Promise(resolve=>{releasePersist=resolve;});},
    send:async message=>{order.push(message.pcm?'send_pcm':'send_end');sent.push(message);}
  });
  sender.push(new Int16Array(1600).fill(1000),.05);
  await tick();
  assert.deepEqual(order,['persist_pcm']);
  assert.equal(sent.length,0);
  releasePersist();await tick();await tick();
  assert.equal(order[1],'send_pcm');
  await sender.drainDurable();
  sender.stop();
});

test('durable failure prevents the corresponding PCM from reaching provider transport',async()=>{
  const sent=[];let failed;
  const sender=createLiveAudioSender({
    batchMs:0,
    persist:async()=>{throw new Error('disk failed');},
    send:async message=>sent.push(message),
    onError:error=>{failed=error;}
  });
  sender.push(new Int16Array(1600).fill(1000),.05);
  await tick();await tick();
  assert.match(failed.message,/disk failed/);
  assert.equal(sent.length,0);
  await assert.rejects(sender.drainDurable(),/disk failed/);
});

test('finish seals a durable source without replaying provider transport requirements',async()=>{
  const durable=[];const sender=createLiveAudioSender({
    batchMs:0,
    persist:async message=>durable.push(message),
    send:async()=>{}
  });
  sender.push(new Int16Array(1600).fill(1000),.05);
  await sender.finish();
  assert.ok(durable.some(item=>item.pcm instanceof Int16Array));
  assert.equal(durable.at(-1).audio_stream_end,true);
  sender.stop();
});
test('honors a longer configured speech-end silence window',async()=>{
 const sent=[];let at=0;
 const sender=createLiveAudioSender({send:async message=>sent.push(message),now:()=>at,speechEndMs:4000});
 sender.push(new Int16Array(1600).fill(1000),.05);await tick();
 for(let i=0;i<39;i++){at+=100;sender.push(new Int16Array(1600),0);await tick();}
 assert.equal(sent.filter(message=>message.audio_stream_end).length,0);
 at+=100;sender.push(new Int16Array(1600),0);await tick();
 assert.equal(sent.filter(message=>message.audio_stream_end).length,1);
 sender.stop();
});

test('steady-state PCM is not marked as startup catchup',async()=>{
  const sent=[];let at=0;
  const sender=createLiveAudioSender({send:async message=>sent.push(message),now:()=>at});
  for(let i=0;i<8;i++){at+=90;sender.push(new Int16Array(1440).fill(1000),.05);await tick();}
  assert.ok(sent.some(message=>message.pcm));
  assert.ok(sent.filter(message=>message.pcm).every(message=>message.startup_catchup===false));
  sender.stop();
});

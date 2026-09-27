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
 assert.ok(sender.stats().queued_pcm_bytes>48000);
 releaseFirst();
 for(let i=0;i<100&&sender.stats().catchup;i++)await tick();
 assert.equal(error,undefined);
 assert.equal(sender.stats().catchup,false);
 assert.ok(sender.stats().queued_pcm_bytes<=24000);
 holdSteady=true;
 for(let i=0;i<30&&!error;i++)sender.push(new Int16Array(1486).fill(1000),.05);
 assert.match(error?.message??'',/Сеть/);
 releaseSteady?.();await tick();
});
test('handoff catch-up retains headroom while a queued HTTP batch is in flight',async()=>{
 let error;const releases=[];
 const sender=createLiveAudioSender({
  send:()=>new Promise(resolve=>releases.push(resolve)),
  onError:e=>error=e
 });
 const frame=()=>({pcm:new Int16Array(1486).fill(1000),rms:.05});
 assert.equal(sender.seed(Array.from({length:64},frame)),true);
 for(let i=0;i<40&&sender.stats().queued_pcm_bytes>43000;i++){
  assert.ok(releases.length);
  releases.shift()();await tick();
 }
 assert.ok(sender.stats().queued_pcm_bytes<=48000);
 assert.ok(sender.stats().queued_pcm_bytes>24000);
 assert.equal(sender.stats().catchup,true);
 for(let i=0;i<5;i++)sender.push(frame().pcm,.05);
 assert.equal(error,undefined);
 for(let i=0;i<40&&sender.stats().catchup;i++){
  assert.ok(releases.length);
  releases.shift()();await tick();
 }
 assert.equal(sender.stats().catchup,false);
 assert.ok(sender.stats().queued_pcm_bytes<=24000);
 sender.stop();
 for(const release of releases)release();
});

test('slow transport is bounded; stop discards pending audio and never waits for network',async()=>{
 let release,count=0,error;const sender=createLiveAudioSender({send:()=>{count++;return new Promise(r=>release=r);},onError:e=>error=e});
 for(let i=0;i<30;i++)sender.push(new Int16Array(1365),.1);
 assert.equal(count,1);assert.match(error.message,/Сеть/);assert.equal(sender.stats().queued_pcm_bytes,0);
 sender.stop();release();await tick();assert.equal(count,1);
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

test('a delayed end-of-stream marker is flushed even after the audio age bound',async()=>{
 let at=0,release,error;const sent=[];
 const sender=createLiveAudioSender({now:()=>at,send:m=>{sent.push(m);if(m.pcm&&at===2000)return new Promise(r=>release=r);return Promise.resolve();},onError:e=>error=e});
 sender.push(new Int16Array(1600),.05);await tick();
 for(let i=0;i<20;i++){at=(i+1)*100;sender.push(new Int16Array(1600),0);await tick();}
 assert.ok(release);assert.equal(sender.stats().queued_pcm_bytes,0);at+=3000;release();await tick();
 assert.equal(error,undefined);assert.equal(sent.at(-1).audio_stream_end,true);assert.equal(sender.stats().oldest_age_ms,0);sender.stop();
});

import test from 'node:test';
import assert from 'node:assert/strict';
import {createMicrophoneCapture,createDurableMicrophoneCapture,createPcm16Resampler} from '../browser/capture.js';
const originalAudioWorkletNode=globalThis.AudioWorkletNode;
globalThis.AudioWorkletNode=class TestAudioWorkletNode {
  constructor(){this.port={onmessage:null};globalThis.__liveTestWorklets?.push?.(this);}
  connect(){return this;}disconnect(){}
};

const tick=()=>new Promise(resolve=>setImmediate(resolve));

test('stateful resampling keeps exact long-run duration across AudioWorklet chunk boundaries',()=>{
  const source=new Float32Array(44100);
  for(let i=0;i<source.length;i++)source[i]=Math.sin(i/37)*.5;
  const whole=createPcm16Resampler(44100).push(source);
  const split=createPcm16Resampler(44100),parts=[];
  for(let offset=0;offset<source.length;offset+=127)parts.push(split.push(source.subarray(offset,Math.min(source.length,offset+127))));
  const joined=new Int16Array(parts.reduce((n,p)=>n+p.length,0));let cursor=0;
  for(const part of parts){joined.set(part,cursor);cursor+=part.length;}
  assert.equal(joined.length,16000);assert.equal(whole.length,16000);
  assert.deepEqual(joined,whole);
});

test('Stop closes a microphone granted after the permission request was cancelled',async()=>{
  const originalNavigator=globalThis.navigator,originalAudio=globalThis.AudioContext;
  let grant,stops=0,contexts=0;
  const track={readyState:'live',stop(){stops++;this.readyState='ended';}};
  Object.defineProperty(globalThis,'navigator',{value:{mediaDevices:{getUserMedia:()=>new Promise(resolve=>{grant=resolve;})}},configurable:true});
  globalThis.AudioContext=class{constructor(){contexts++;}};
  const capture=createMicrophoneCapture();
  try{
    const pending=capture.start();
    capture.stop();
    grant({getTracks:()=>[track]});
    assert.equal(await pending,false);
    assert.equal(track.readyState,'ended');
    assert.equal(stops,1);
    assert.equal(contexts,0);
    assert.equal(capture.stream,null);
  }finally{
    if(originalNavigator===undefined)delete globalThis.navigator;else Object.defineProperty(globalThis,'navigator',{value:originalNavigator,configurable:true});
    if(originalAudio===undefined)delete globalThis.AudioContext;else globalThis.AudioContext=originalAudio;
  }
});

test('shared microphone capture owns getUserMedia, resampling and ordered frame delivery',async()=>{
  const originalNavigator=globalThis.navigator,originalAudio=globalThis.AudioContext;
  const processors=[];globalThis.__liveTestWorklets=processors;let getUserMediaCalls=0,stops=0;
  const track={stop(){stops++;}};
  const stream={getTracks:()=>[track]};
  class Node{connect(){return this;}disconnect(){}}
  class Processor extends Node{onaudioprocess=null;}
  class Context{
    sampleRate=48000;destination={};state='running';audioWorklet={addModule:async()=>{}};
    createMediaStreamSource(value){assert.equal(value,stream);return new Node();}
    createScriptProcessor(){const p=new Processor();processors.push(p);return p;}
    async resume(){}
    async close(){this.state='closed';}
  }
  Object.defineProperty(globalThis,'navigator',{value:{mediaDevices:{getUserMedia:async()=>{getUserMediaCalls++;return stream;}}},configurable:true});
  globalThis.AudioContext=Context;
  const frames=[];
  const capture=createMicrophoneCapture({onFrame:(pcm,rms,meta)=>frames.push({pcm,rms,meta})});
  try{
    assert.equal(await capture.start(),true);
    const samples=new Float32Array(4096).fill(.25);
    processors[0].port.onmessage({data:samples});
    await capture.drain();
    assert.equal(getUserMediaCalls,1);
    assert.equal(frames.length,1);
    assert.ok(frames[0].pcm instanceof Int16Array);
    assert.equal(frames[0].meta.sample_rate,16000);
    assert.ok(frames[0].rms>.2);
    capture.stop();assert.equal(stops,1);
  }finally{
    if(originalNavigator===undefined)delete globalThis.navigator;else Object.defineProperty(globalThis,'navigator',{value:originalNavigator,configurable:true});
    if(originalAudio===undefined)delete globalThis.AudioContext;else globalThis.AudioContext=originalAudio;
  }
});

test('offline durable capture uses shared VAD/sender and seals after microphone drain',async()=>{
  const originalNavigator=globalThis.navigator,originalAudio=globalThis.AudioContext;
  const processors=[];globalThis.__liveTestWorklets=processors;let stops=0;
  const track={stop(){stops++;}},stream={getTracks:()=>[track]};
  class Node{connect(){return this;}disconnect(){}}
  class Processor extends Node{onaudioprocess=null;}
  class Context{
    sampleRate=16000;destination={};state='running';audioWorklet={addModule:async()=>{}};
    createMediaStreamSource(){return new Node();}
    createScriptProcessor(){const p=new Processor();processors.push(p);return p;}
    async resume(){}
    async close(){this.state='closed';}
  }
  Object.defineProperty(globalThis,'navigator',{value:{mediaDevices:{getUserMedia:async()=>stream}},configurable:true});
  globalThis.AudioContext=Context;
  const durable=[];
  const capture=createDurableMicrophoneCapture({persist:async message=>durable.push(message),batchMs:0});
  try{
    await capture.start();
    const speech=new Float32Array(1600).fill(.2);
    processors[0].port.onmessage({data:speech});
    await tick();
    await capture.stop();
    assert.equal(stops,1);
    assert.ok(durable.some(item=>item.pcm instanceof Int16Array));
    assert.equal(durable.at(-1).audio_stream_end,true);
  }finally{
    if(originalNavigator===undefined)delete globalThis.navigator;else Object.defineProperty(globalThis,'navigator',{value:originalNavigator,configurable:true});
    if(originalAudio===undefined)delete globalThis.AudioContext;else globalThis.AudioContext=originalAudio;
  }
});

test('offline durable capture stops microphone immediately when persistence fails',async()=>{
  const originalNavigator=globalThis.navigator,originalAudio=globalThis.AudioContext;
  const processors=[];globalThis.__liveTestWorklets=processors;let stops=0,seenError=null;
  const track={stop(){stops++;}},stream={getTracks:()=>[track]};
  class Node{connect(){return this;}disconnect(){}}
  class Processor extends Node{onaudioprocess=null;}
  class Context{
    sampleRate=16000;destination={};state='running';audioWorklet={addModule:async()=>{}};
    createMediaStreamSource(){return new Node();}
    createScriptProcessor(){const p=new Processor();processors.push(p);return p;}
    async resume(){}
    async close(){this.state='closed';}
  }
  Object.defineProperty(globalThis,'navigator',{value:{mediaDevices:{getUserMedia:async()=>stream}},configurable:true});
  globalThis.AudioContext=Context;
  const capture=createDurableMicrophoneCapture({
    persist:async()=>{throw new Error('idb unavailable');},
    onError:error=>{seenError=error;},
    batchMs:0
  });
  try{
    await capture.start();
    const speech=new Float32Array(1600).fill(.2);
    processors[0].port.onmessage({data:speech});
    for(let i=0;i<20&&!seenError;i++)await tick();
    assert.match(seenError.message,/idb unavailable/);
    assert.equal(capture.running,false);
    assert.equal(stops,1);
  }finally{
    if(originalNavigator===undefined)delete globalThis.navigator;else Object.defineProperty(globalThis,'navigator',{value:originalNavigator,configurable:true});
    if(originalAudio===undefined)delete globalThis.AudioContext;else globalThis.AudioContext=originalAudio;
  }
});

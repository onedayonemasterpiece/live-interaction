import test from 'node:test';
import assert from 'node:assert/strict';
import {createAdaptiveDuplexGate} from '../browser/duplex-gate.js';

const frame=(rms=.01)=>({pcm:new Int16Array(3200).fill(Math.round(rms*32768)),rms});

test('adaptive duplex gate learns playback echo and suppresses it',()=>{
  const gate=createAdaptiveDuplexGate();
  gate.observe(frame(.002).pcm,.002,{playback:false});
  for(let i=0;i<8;i++){
    const value=frame(.012);
    const decision=gate.observe(value.pcm,value.rms,{playback:true});
    assert.equal(decision.action,'suppress');
  }
  assert.equal(gate.stats().admitted_barge_ins,0);
});

test('adaptive duplex gate rejects a spike and admits sustained independent speech',()=>{
  const gate=createAdaptiveDuplexGate();
  for(let i=0;i<4;i++){
    const value=frame(.012);
    gate.observe(value.pcm,value.rms,{playback:true});
  }
  let value=frame(.05);
  assert.equal(gate.observe(value.pcm,value.rms,{playback:true}).action,'suppress');
  value=frame(.012);
  assert.equal(gate.observe(value.pcm,value.rms,{playback:true}).action,'suppress');
  value=frame(.05);
  assert.equal(gate.observe(value.pcm,value.rms,{playback:true}).action,'suppress');
  value=frame(.05);
  const decision=gate.observe(value.pcm,value.rms,{playback:true});
  assert.equal(decision.action,'barge');
  assert.equal(decision.frames.length,2);
  assert.ok(decision.buffered_ms>=350);
});

test('adaptive duplex gate passes ordinary non-playback capture unchanged',()=>{
  const gate=createAdaptiveDuplexGate();
  const value=frame(.02);
  const decision=gate.observe(value.pcm,value.rms,{playback:false});
  assert.equal(decision.action,'pass');
  assert.equal(decision.frames[0].pcm,value.pcm);
});

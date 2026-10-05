export function createAdaptiveDuplexGate({
  sampleRate=16000,
  calibrationMs=300,
  attackMs=350,
  minBargeRms=0.018,
  relativeFactor=2,
  initialEchoFloorRms=0.004,
  maxEchoFloorRms=0.018,
  quietLearningCeiling=0.008,
}={}){
  let echoFloorRms=initialEchoFloorRms;
  let playbackActive=false;
  let playbackMs=0;
  let candidateFrames=[];
  let candidateMs=0;
  let suppressedFrames=0;
  let admitted=0;

  const durationMs=pcm=>pcm.length*1000/sampleRate;
  const clampFloor=value=>Math.max(initialEchoFloorRms,Math.min(maxEchoFloorRms,value));

  function resetPlayback(){
    playbackActive=false;
    playbackMs=0;
    candidateFrames=[];
    candidateMs=0;
  }

  function reset(){
    echoFloorRms=initialEchoFloorRms;
    suppressedFrames=0;
    admitted=0;
    resetPlayback();
  }

  function observe(pcm,rms,{playback=false}={}){
    if(!(pcm instanceof Int16Array)||!Number.isFinite(rms))throw new TypeError('Adaptive duplex frame is invalid');
    const frameMs=durationMs(pcm);
    if(!playback){
      if(rms>=0&&rms<=quietLearningCeiling){
        echoFloorRms=clampFloor(.98*echoFloorRms+.02*rms);
      }
      resetPlayback();
      return {action:'pass',frames:[{pcm,rms}],frame_ms:frameMs,echo_floor_rms:echoFloorRms};
    }
    if(!playbackActive){
      playbackActive=true;
      playbackMs=0;
      candidateFrames=[];
      candidateMs=0;
    }
    playbackMs+=frameMs;
    const threshold=Math.max(minBargeRms,clampFloor(echoFloorRms)*relativeFactor);
    if(playbackMs<=calibrationMs){
      echoFloorRms=clampFloor(.75*echoFloorRms+.25*Math.max(0,rms));
      candidateFrames=[];
      candidateMs=0;
      suppressedFrames++;
      return {action:'suppress',frame_ms:frameMs,playback_ms:playbackMs,threshold_rms:threshold,echo_floor_rms:echoFloorRms};
    }
    if(rms>=threshold){
      candidateFrames.push({pcm:new Int16Array(pcm),rms});
      candidateMs+=frameMs;
      if(candidateMs>=attackMs){
        const frames=candidateFrames;
        const bufferedMs=candidateMs;
        candidateFrames=[];
        candidateMs=0;
        admitted++;
        return {action:'barge',frames,buffered_ms:bufferedMs,threshold_rms:threshold,echo_floor_rms:echoFloorRms};
      }
    }else{
      candidateFrames=[];
      candidateMs=0;
      echoFloorRms=clampFloor(.9*echoFloorRms+.1*Math.max(0,rms));
    }
    suppressedFrames++;
    return {action:'suppress',frame_ms:frameMs,playback_ms:playbackMs,threshold_rms:threshold,echo_floor_rms:echoFloorRms,candidate_ms:candidateMs};
  }

  function stats(){
    return {echo_floor_rms:echoFloorRms,suppressed_frames:suppressedFrames,admitted_barge_ins:admitted,playback_active:playbackActive,candidate_ms:candidateMs};
  }
  return {observe,reset,resetPlayback,stats};
}

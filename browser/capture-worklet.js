class LiveMicrophoneCaptureProcessor extends AudioWorkletProcessor {
  process(inputs,outputs){
    const input=inputs?.[0]?.[0];
    if(input?.length){
      const copy=new Float32Array(input);
      this.port.postMessage(copy,[copy.buffer]);
    }
    const output=outputs?.[0]?.[0];
    if(output)output.fill(0);
    return true;
  }
}
registerProcessor('live-microphone-capture-v1',LiveMicrophoneCaptureProcessor);

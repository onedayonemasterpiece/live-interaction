export const LIVE_SOCKET_PROTOCOL='wl-live-v1';
const INPUT_MAGIC=0x574c4131;
const OUTPUT_MAGIC=0x574c4f31;

function socketUrl(value){
  const base=globalThis.location?.href??'http://localhost/';
  const url=new URL(value,base);
  if(url.protocol==='https:')url.protocol='wss:';
  else if(url.protocol==='http:')url.protocol='ws:';
  if(!['ws:','wss:'].includes(url.protocol))throw new TypeError('Live socket URL must use ws/wss');
  return url.href;
}
function u32(view,offset,value){view.setUint32(offset,Math.max(0,Math.min(0xffffffff,Math.round(value)||0)),false);}
export function encodeLiveAudioFrame(pcm,{seq=0,age_ms=0}={}){
  if(!(pcm instanceof Int16Array))throw new TypeError('PCM frame must be Int16Array');
  const bytes=new Uint8Array(12+pcm.byteLength),view=new DataView(bytes.buffer);
  view.setUint32(0,INPUT_MAGIC,false);u32(view,4,seq);u32(view,8,age_ms);
  bytes.set(new Uint8Array(pcm.buffer,pcm.byteOffset,pcm.byteLength),12);
  return bytes;
}
export function decodeLiveOutputFrame(data){
  const bytes=data instanceof Uint8Array?data:new Uint8Array(data);
  if(bytes.byteLength<12)throw new Error('Live output frame is too short');
  const view=new DataView(bytes.buffer,bytes.byteOffset,bytes.byteLength);
  if(view.getUint32(0,false)!==OUTPUT_MAGIC)throw new Error('Live output frame magic mismatch');
  const seq=view.getUint32(4,false),rate=view.getUint32(8,false);
  if(rate<8000||rate>96000||(bytes.byteLength-12)%2)throw new Error('Live output frame metadata is invalid');
  return {seq,rate,pcm:bytes.slice(12)};
}
export function createLiveSocketTransport({
  WebSocketImpl=globalThis.WebSocket,
  onEvent=()=>{},
  onTiming=()=>{},
  onOpen=()=>{},
  onClose=()=>{},
  onError=()=>{},
  maxBufferedBytes=96*1024,
  maxUnackedAgeMs=2500,
  helloTimeoutMs=2000,
  now=()=>globalThis.performance?.now?.()??Date.now(),
}={}){
  if(typeof WebSocketImpl!=='function')throw new TypeError('WebSocket is unavailable');
  let socket=null,audioSeq=0,closed=true,generation=0;
  const sent=new Map();
  const oldestAge=()=>{
    const first=sent.values().next();
    return first.done?0:Math.max(0,now()-first.value);
  };
  const metrics=()=>({socket_buffered_bytes:socket?.bufferedAmount??0,unacked_audio_frames:sent.size,oldest_unacked_age_ms:Math.round(oldestAge()),connection_generation:generation});
  const requireOpen=()=>{
    if(!socket||socket.readyState!==WebSocketImpl.OPEN)throw Object.assign(new Error('Live WebSocket is not open'),{code:'LIVE_SOCKET_CLOSED'});
    const m=metrics();
    if(m.socket_buffered_bytes>maxBufferedBytes||m.oldest_unacked_age_ms>maxUnackedAgeMs)throw Object.assign(new Error('Live WebSocket queue is stale'),{code:'LIVE_SOCKET_BACKPRESSURE',metrics:m});
  };
  function handleText(text,onHelloAck){
    let message;
    try{message=JSON.parse(text);}catch{return;}
    if(message.type==='audio_ack'){
      const ack=Number(message.seq);
      if(!Number.isSafeInteger(ack)||ack<0||ack>audioSeq)throw Object.assign(new Error('Invalid Live audio acknowledgement'),{code:'LIVE_SOCKET_PROTOCOL'});
      for(const seq of [...sent.keys()])if(seq<=ack)sent.delete(seq);
      onTiming('socket_server_received',{seq:ack,server_received_at:message.server_received_at,...metrics()});
      return;
    }
    if(message.type==='event'&&message.event){onEvent(message.event);return;}
    if(message.type==='hello_ack'){
      if(message.protocol!==LIVE_SOCKET_PROTOCOL||Number(message.connection_generation)!==generation)throw Object.assign(new Error('Live WebSocket protocol mismatch'),{code:'LIVE_SOCKET_PROTOCOL'});
      onTiming('socket_hello_ack',{protocol:message.protocol,connection_generation:message.connection_generation});
      onHelloAck?.(message);return;
    }
    if(message.type==='transport_notice'){onTiming('socket_transport_notice',message);return;}
  }
  function connect({url,ticket,attempt_id,cursor=0,connection_generation}={}){
    if(typeof ticket!=='string'||!/^[A-Za-z0-9_-]{20,512}$/.test(ticket))return Promise.reject(Object.assign(new Error('Live socket ticket is invalid'),{code:'LIVE_SOCKET_TICKET'}));
    if(socket)close({sendStop:false,code:1000,reason:'replace'});
    closed=false;generation=Number.isInteger(connection_generation)?connection_generation:generation+1;
    const epoch=generation;
    return new Promise((resolve,reject)=>{
      let settled=false,helloTimer=null;
      const ws=new WebSocketImpl(socketUrl(url),[LIVE_SOCKET_PROTOCOL,`wl-ticket.${ticket}`]);socket=ws;ws.binaryType='arraybuffer';
      const fail=error=>{clearTimeout(helloTimer);if(!settled){settled=true;reject(error);}onError(error);};
      const protocolFail=error=>{fail(error);try{ws.close(1002,String(error.code??'protocol').slice(0,80));}catch{}};
      ws.onopen=()=>{
        if(epoch!==generation){try{ws.close(1000,'stale');}catch{}return;}
        ws.send(JSON.stringify({type:'hello',protocol:LIVE_SOCKET_PROTOCOL,attempt_id:String(attempt_id??'').slice(0,96),cursor:Number.isSafeInteger(cursor)?cursor:0,connection_generation:generation}));
        onTiming('socket_open',{connection_generation:generation});
        helloTimer=setTimeout(()=>protocolFail(Object.assign(new Error('Live WebSocket hello acknowledgement timeout'),{code:'LIVE_SOCKET_HELLO_TIMEOUT'})),helloTimeoutMs);
      };
      ws.onmessage=event=>{
        if(epoch!==generation)return;
        try{
          if(typeof event.data==='string'){
            handleText(event.data,()=>{
              if(settled)return;
              clearTimeout(helloTimer);settled=true;onOpen({connection_generation:generation});resolve({connection_generation:generation});
            });
            return;
          }
          if(!settled)throw Object.assign(new Error('Binary Live output arrived before hello acknowledgement'),{code:'LIVE_SOCKET_PROTOCOL'});
          const frame=decodeLiveOutputFrame(event.data);
          onTiming('socket_audio_received',{seq:frame.seq,pcm_bytes:frame.pcm.byteLength,rate:frame.rate,...metrics()});
          onEvent({type:'audio',seq:frame.seq,pcm:frame.pcm,mime_type:`audio/pcm;rate=${frame.rate}`,transport:'wss'});
        }catch(error){protocolFail(error);}
      };
      ws.onerror=()=>fail(Object.assign(new Error('Live WebSocket transport failed'),{code:'LIVE_SOCKET_ERROR'}));
      ws.onclose=event=>{
        clearTimeout(helloTimer);
        if(socket===ws)socket=null;
        const wasClosed=closed;sent.clear();
        if(!settled)fail(Object.assign(new Error(`Live WebSocket closed during setup (${event.code})`),{code:'LIVE_SOCKET_CLOSED'}));
        if(epoch===generation)onClose({code:event.code,reason:String(event.reason??'').slice(0,120),expected:wasClosed,connection_generation:generation});
      };
    });
  }
  function send(message){
    requireOpen();
    if(message?.pcm instanceof Int16Array){
      const seq=++audioSeq,frame=encodeLiveAudioFrame(message.pcm,{seq,age_ms:message.age_ms});
      sent.set(seq,now());socket.send(frame);
      onTiming('socket_audio_sent',{seq,pcm_bytes:message.pcm.byteLength,capture_age_ms:Math.round(message.age_ms??0),...metrics()});
      return Promise.resolve({seq});
    }
    const safe=message?.audio_stream_end?{type:'input',message:{audio_stream_end:true,captured_at_ms:message.captured_at_ms,age_ms:message.age_ms}}
      :message?.text!==undefined?{type:'input',message:{text:message.text}}
      :{type:'input',message};
    socket.send(JSON.stringify(safe));
    onTiming('socket_control_sent',{input_type:message?.audio_stream_end?'audio_stream_end':message?.text!==undefined?'text':'context',...metrics()});
    return Promise.resolve({});
  }
  function close({sendStop=false,code=1000,reason='client_stop'}={}){
    closed=true;const ws=socket;socket=null;sent.clear();
    if(!ws)return;
    try{if(sendStop&&ws.readyState===WebSocketImpl.OPEN)ws.send(JSON.stringify({type:'stop',reason:String(reason).slice(0,80)}));}catch{}
    try{ws.close(code,String(reason).slice(0,80));}catch{}
  }
  return {connect,send,close,metrics,get open(){return Boolean(socket&&socket.readyState===WebSocketImpl.OPEN);},get generation(){return generation;}};
}

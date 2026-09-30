import {createHash,timingSafeEqual} from 'node:crypto';

export const LIVE_SOCKET_PROTOCOL='wl-live-v1';
const GUID='258EAFA5-E914-47DA-95CA-C5AB0DC85B11';
const INPUT_MAGIC=0x574c4131;
const OUTPUT_MAGIC=0x574c4f31;
const MAX_CLIENT_PAYLOAD=64*1024;

function protocols(header=''){
  return String(header).split(',').map(value=>value.trim()).filter(Boolean);
}
function reject(socket,status='400 Bad Request'){
  try{socket.write(`HTTP/1.1 ${status}\r\nConnection: close\r\nContent-Length: 0\r\n\r\n`);}catch{}
  try{socket.destroy();}catch{}
}
function frame(opcode,payload=Buffer.alloc(0)){
  const body=Buffer.isBuffer(payload)?payload:Buffer.from(payload);
  let header;
  if(body.length<126){header=Buffer.alloc(2);header[1]=body.length;}
  else if(body.length<=0xffff){header=Buffer.alloc(4);header[1]=126;header.writeUInt16BE(body.length,2);}
  else{header=Buffer.alloc(10);header[1]=127;header.writeBigUInt64BE(BigInt(body.length),2);}
  header[0]=0x80|opcode;
  return Buffer.concat([header,body]);
}
function sendText(socket,value){socket.write(frame(1,Buffer.from(JSON.stringify(value),'utf8')));}
function sendBinary(socket,value){socket.write(frame(2,value));}
function sendClose(socket,code=1000,reason=''){
  const text=Buffer.from(String(reason).slice(0,120),'utf8'),body=Buffer.alloc(2+text.length);body.writeUInt16BE(code,0);text.copy(body,2);
  try{socket.write(frame(8,body));}catch{}
}
function outputAudio(event){
  const raw=Buffer.from(String(event.data??''),'base64'),rate=Number(/rate=(\d+)/.exec(event.mime_type??'')?.[1]??24000);
  const header=Buffer.alloc(12);header.writeUInt32BE(OUTPUT_MAGIC,0);header.writeUInt32BE(Number(event.seq)>>>0,4);header.writeUInt32BE(rate>>>0,8);
  return Buffer.concat([header,raw]);
}
function inputAudio(payload){
  if(payload.length<12||payload.length>11012)throw Object.assign(new Error('Invalid Live audio frame'),{code:'LIVE_SOCKET_FRAME'});
  if(payload.readUInt32BE(0)!==INPUT_MAGIC)throw Object.assign(new Error('Live audio frame magic mismatch'),{code:'LIVE_SOCKET_FRAME'});
  const seq=payload.readUInt32BE(4),ageMs=payload.readUInt32BE(8),pcm=payload.subarray(12);
  if(!pcm.length||pcm.length%2)throw Object.assign(new Error('Live PCM payload is invalid'),{code:'LIVE_SOCKET_FRAME'});
  return {seq,age_ms:ageMs,pcm};
}
function parser(onFrame,onProtocolError){
  let buffer=Buffer.alloc(0);
  return chunk=>{
    buffer=Buffer.concat([buffer,chunk]);
    for(;;){
      if(buffer.length<2)return;
      const b0=buffer[0],b1=buffer[1],fin=Boolean(b0&0x80),opcode=b0&0x0f,masked=Boolean(b1&0x80);
      if(!fin||!masked){onProtocolError(1002,'fragmented or unmasked client frame');return;}
      let length=b1&0x7f,offset=2;
      if(length===126){if(buffer.length<4)return;length=buffer.readUInt16BE(2);offset=4;}
      else if(length===127){
        if(buffer.length<10)return;const big=buffer.readBigUInt64BE(2);
        if(big>BigInt(MAX_CLIENT_PAYLOAD)){onProtocolError(1009,'frame too large');return;}
        length=Number(big);offset=10;
      }
      if(length>MAX_CLIENT_PAYLOAD){onProtocolError(1009,'frame too large');return;}
      if(buffer.length<offset+4+length)return;
      const mask=buffer.subarray(offset,offset+4);offset+=4;
      const payload=Buffer.from(buffer.subarray(offset,offset+length));buffer=buffer.subarray(offset+length);
      for(let i=0;i<payload.length;i++)payload[i]^=mask[i&3];
      onFrame(opcode,payload);
    }
  };
}

export function createLiveWebSocketUpgrade({host,matchPath,protocol=LIVE_SOCKET_PROTOCOL,onDiagnostic=()=>{}}={}){
  if(!host||typeof host.openSocket!=='function')throw new TypeError('Live session host with openSocket is required');
  if(typeof matchPath!=='function')throw new TypeError('matchPath is required');
  return async function handleUpgrade(request,socket,head=Buffer.alloc(0)){
    let matched;
    try{matched=matchPath(new URL(request.url,'http://live.local'));}catch{}
    if(!matched?.sessionId)return false;
    const offered=protocols(request.headers['sec-websocket-protocol']),ticketEntry=offered.find(value=>value.startsWith('wl-ticket.'));
    if(!offered.includes(protocol)||!ticketEntry){reject(socket,'401 Unauthorized');return true;}
    const key=String(request.headers['sec-websocket-key']??'');
    if(Buffer.from(key,'base64').length!==16){reject(socket);return true;}
    let binding;
    try{binding=host.openSocket({sessionId:matched.sessionId,ticket:ticketEntry.slice('wl-ticket.'.length)});}
    catch(error){onDiagnostic({type:'socket_rejected',code:error.code??'LIVE_SOCKET_AUTH'});reject(socket,error.code==='LIVE_SOCKET_TICKET'?'401 Unauthorized':'404 Not Found');return true;}
    const accept=createHash('sha1').update(key+GUID).digest('base64');
    socket.write(`HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: ${accept}\r\nSec-WebSocket-Protocol: ${protocol}\r\n\r\n`);
    socket.setNoDelay?.(true);
    let hello=false,stopped=false,connectionGeneration=0,queued=[];
    const emitEvent=event=>{
      if(!hello){queued.push(event);if(queued.length>96)queued.shift();return;}
      try{event.type==='audio'?sendBinary(socket,outputAudio(event)):sendText(socket,{type:'event',event});}
      catch{try{socket.destroy();}catch{}}
    };
    const unsubscribe=binding.subscribe(emitEvent);
    const flushBacklog=after=>{
      const page=binding.events(after);
      for(const event of page.events)emitEvent(event);
      if(page.has_more){let cursor=page.cursor;for(let guard=0;guard<16&&page.has_more;guard++){const next=binding.events(cursor);for(const event of next.events)emitEvent(event);cursor=next.cursor;if(!next.has_more)break;}}
      for(const event of queued.splice(0))emitEvent(event);
    };
    const fail=(code,reason)=>{
      onDiagnostic({type:'socket_protocol_error',session_id:matched.sessionId,code,connection_generation:connectionGeneration});
      sendClose(socket,code,reason);try{socket.end();}catch{}
    };
    const onFrame=async(opcode,payload)=>{
      try{
        if(opcode===8){stopped=true;try{await binding.stop();}catch{}sendClose(socket,1000,'closed');socket.end();return;}
        if(opcode===9){socket.write(frame(10,payload));return;}
        if(opcode===10)return;
        if(opcode===1){
          const message=JSON.parse(payload.toString('utf8'));
          if(!hello){
            if(message.type!=='hello'||message.protocol!==protocol)throw Object.assign(new Error('Live socket hello required'),{code:'LIVE_SOCKET_HELLO'});
            if(binding.attemptId&&message.attempt_id!==binding.attemptId)throw Object.assign(new Error('Live attempt mismatch'),{code:'LIVE_SOCKET_ATTEMPT'});
            connectionGeneration=Number.isInteger(message.connection_generation)&&message.connection_generation>0?message.connection_generation:1;
            hello=true;binding.transportConnected({connection_generation:connectionGeneration});
            sendText(socket,{type:'hello_ack',protocol,connection_generation:connectionGeneration});
            flushBacklog(Number.isSafeInteger(message.cursor)&&message.cursor>=0?message.cursor:0);
            return;
          }
          if(message.type==='stop'){stopped=true;await binding.stop();sendClose(socket,1000,'stopped');socket.end();return;}
          if(message.type!=='input'||!message.message||typeof message.message!=='object')throw Object.assign(new Error('Invalid Live socket input'),{code:'LIVE_SOCKET_MESSAGE'});
          binding.input(message.message,{connection_generation:connectionGeneration});
          return;
        }
        if(opcode===2){
          if(!hello)throw Object.assign(new Error('Live socket hello required'),{code:'LIVE_SOCKET_HELLO'});
          const receivedAt=Date.now(),audio=inputAudio(payload);
          binding.input({audio_base64:audio.pcm.toString('base64')},{receivedAt,frame_seq:audio.seq,capture_age_ms:audio.age_ms,connection_generation:connectionGeneration});
          sendText(socket,{type:'audio_ack',seq:audio.seq,server_received_at:receivedAt});
          return;
        }
        throw Object.assign(new Error('Unsupported WebSocket opcode'),{code:'LIVE_SOCKET_OPCODE'});
      }catch(error){
        onDiagnostic({type:'socket_input_error',session_id:matched.sessionId,code:error.code??'LIVE_SOCKET_MESSAGE',connection_generation:connectionGeneration});
        fail(1002,error.code??'invalid message');
      }
    };
    const parse=parser((opcode,payload)=>{void onFrame(opcode,payload);},(code,reason)=>fail(code,reason));
    socket.on('data',parse);
    socket.on('error',()=>{});
    socket.on('close',()=>{
      unsubscribe();if(!stopped&&hello)binding.transportGap({connection_generation:connectionGeneration,reason:'socket_closed'});
      binding.close();
    });
    if(head?.length)parse(head);
    return true;
  };
}
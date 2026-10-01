import test from 'node:test';
import assert from 'node:assert/strict';
import {createLiveSocketTransport} from '../browser/socket-transport.js';
const delay=ms=>new Promise(resolve=>setTimeout(resolve,ms));
class Peer {
 static OPEN=1;
 static latest;
 constructor(){Peer.latest=this;this.readyState=0;this.bufferedAmount=0;this.sent=[];queueMicrotask(()=>{this.readyState=1;this.onopen();});}
 send(value){this.sent.push(value);const message=typeof value==='string'?JSON.parse(value):{};if(message.type==='hello')queueMicrotask(()=>this.onmessage({data:JSON.stringify({type:'hello_ack',protocol:'wl-live-v1',connection_generation:1})}));}
 close(code=1000){this.readyState=3;this.onclose?.({code,reason:''});}
}
const start=transport=>transport.connect({url:'https://example.test/socket',ticket:'a'.repeat(32),attempt_id:'attempt_test',connection_generation:1});
test('idle socket heartbeat refreshes liveness without audio or events polling',async()=>{
 const errors=[];let at=0;
 const transport=createLiveSocketTransport({WebSocketImpl:Peer,heartbeatIntervalMs:5,heartbeatTimeoutMs:100,now:()=>at,onError:e=>errors.push(e.code)});
 await start(transport);const peer=Peer.latest;
 await delay(12);
 assert.ok(peer.sent.some(x=>JSON.parse(x).type==='ping'));
 at=90;peer.onmessage({data:'{"type":"pong"}'});at=120;
 await delay(8);assert.deepEqual(errors,[]);
 transport.close();const count=peer.sent.length;await delay(12);assert.equal(peer.sent.length,count);
});
test('missing heartbeat reply closes visibly and does not retry old input',async()=>{
 const errors=[];let at=0;
 const transport=createLiveSocketTransport({WebSocketImpl:Peer,heartbeatIntervalMs:5,heartbeatTimeoutMs:10,now:()=>at,onError:e=>errors.push(e.code)});
 await start(transport);at=11;await delay(15);
 assert.ok(errors.includes('LIVE_SOCKET_HEARTBEAT_TIMEOUT'));
 assert.equal(transport.open,false);transport.close();
});

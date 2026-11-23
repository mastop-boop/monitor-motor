'use strict';
const topic='uni-lab/e7b914ca52/adxl345';
const oledTopic=topic+'/oled';
let ultimoFotograma=0;
const el=id=>document.getElementById(id);
const canvas=el('g'), ctx=canvas.getContext('2d');
const rows=[], trace=[];
let ws,lastAt=0,lastSeq=-1,session='',missing=0,subscribed=false;
const enc=new TextEncoder(),dec=new TextDecoder();
function field(s){const b=enc.encode(s);return [b.length>>8,b.length&255,...b];}
function packet(type,body){let n=body.length,head=[type];do{let k=n%128;n=Math.floor(n/128);head.push(k|(n?128:0));}while(n);return Uint8Array.from([...head,...body]);}
function connect(){
 subscribed=false;el('state').textContent='Conectando al servidor MQTT…';
 ws=new WebSocket('wss://test.mosquitto.org:8081/mqtt','mqtt');ws.binaryType='arraybuffer';
 let pending=new Uint8Array(0);
 ws.onopen=()=>ws.send(packet(16,[...field('MQTT'),4,2,0,30,...field('web-'+crypto.randomUUID().slice(0,14))]));
 ws.onmessage=e=>{
  const b=new Uint8Array(e.data),a=new Uint8Array(pending.length+b.length);a.set(pending);a.set(b,pending.length);
  let p=0;
  while(p<a.length){let start=p,h=a[p++],size=0,m=1,v;
   do{if(p>=a.length){pending=a.slice(start);return;}v=a[p++];size+=(v&127)*m;m*=128;}while(v&128);
   if(p+size>a.length){pending=a.slice(start);return;}
   let data=a.subarray(p,p+size);p+=size;
   if(h>>4===2){if(data[1]!==0){ws.close();return;}ws.send(packet(130,[0,1,...field(topic),0,...field(oledTopic),0]));}
   if(h>>4===9){if(data[2]===128){ws.close();return;}subscribed=true;status();}
   if(h>>4===3 && !(h&1)){let n=(data[0]<<8)+data[1];const canal=dec.decode(data.subarray(2,2+n)), payload=data.subarray(2+n);if(canal===topic)receive(dec.decode(payload));else if(canal===oledTopic)mostrarOLED(payload);}
  }pending=new Uint8Array(0);
 };
 ws.onerror=()=>ws.close();ws.onclose=()=>{subscribed=false;el('state').textContent='Servidor desconectado. Reintentando…';setTimeout(connect,3000);};
}
function mostrarOLED(bytes){
 if(bytes.length!==1024)return;
 const ctx=el('oled').getContext('2d'), imagen=ctx.createImageData(128,64);
 for(let y=0;y<64;y++)for(let x=0;x<128;x++){
   const encendido=bytes[(y>>3)*128+x] & (1<<(y&7));
   const i=4*(128*y+x);
   imagen.data[i]=encendido?202:7;imagen.data[i+1]=encendido?251:19;
   imagen.data[i+2]=encendido?247:24;imagen.data[i+3]=255;
 }
 ctx.putImageData(imagen,0,0);ultimoFotograma=Date.now();
 el('oledEstado').textContent='En vivo · pulsador GPIO13';
}
const heat=[], current=[];
let binMs=100, origin=0,unit='',temp=null,actual=null,peak=null;
const num=s=>s==='nan'||s==='NaN'?null:(s!==''&&Number.isFinite(Number(s))?Number(s):null);
const valid=Number.isFinite;
function limiteConsumo(){return unit==='A'?5:30;}
function amperes(v){return valid(v)?v.toFixed(2)+' '+unit:'—';}
// Keep the measured consumption, including gaps. The CSV retains every sample.
function currentBin(t,a,seq){
 current.push({t,v:[a],seq});
}
// Preserve first, minimum, maximum and last measured value per screen column.
// Start a new segment on missing readings or transport gaps.
function currentSeries(origin,end){
 const width=Math.max(1,el('currentGraph').clientWidth-47), out=[];
 let group=null, prev=null;
 function flush(){
   if(!group)return;
   const selected=[...new Set([group.first,group.min,group.max,group.last])].sort((a,b)=>a.t-b.t);
   out.push(...selected);group=null;
 }
 for(const p of current){
   const gap=prev && (p.seq!==prev.seq+1 || p.t-prev.t>350);
   if(gap || !valid(p.v[0])){
     flush();
     if(out.length && valid(out[out.length-1].v[0]))out.push({t:p.t,v:[null]});
   }
   prev=p;
   if(!valid(p.v[0]))continue;
   const col=Math.floor((p.t-origin)*width/Math.max(1,end-origin));
   if(!group || group.col!==col){flush();group={col,first:p,min:p,max:p,last:p};}
   group.last=p;
   if(p.v[0]<group.min.v[0])group.min=p;
   if(p.v[0]>group.max.v[0])group.max=p;
 }
 flush();return out.map(p=>({t:p.t,v:p.v}));
}
function receive(s){
 const f=s.split(',');if(![6,11,17].includes(f.length)||!/^[a-f0-9]{8}$/.test(f[0]))return;
 const [id]=f,[seq,t,x,y,z]=f.slice(1,6).map(num);
 if(!Number.isSafeInteger(seq)||!valid(t)||seq<0||t<0)return;
 if(f.length>=11&&!['A','P'].includes(f[10]))return;
 if(id!==session){trace.length=heat.length=current.length=0;origin=t;binMs=100;lastSeq=-1;session=id;}
 if(seq<=lastSeq)return;
 if(lastSeq>=0)missing+=seq-lastSeq-1;
 lastSeq=seq;lastAt=Date.now();
 const multi=f.length>=11;
 const high=multi?num(f[7]):null;
 actual=multi?num(f[6]):null;peak=multi?num(f[8]):null;temp=multi?num(f[9]):null;
 unit=multi?(f[10]==='A'?'A':'%'):'';
 const estados=['Calibrando','En reposo','Sin referencia','Fase incierta','Motor normal estimado','Posible desalineación','Tomando referencia','Revisar corriente','Sin ADXL345'];
 const extra=f.length===17?[num(f[11]),num(f[11])===null?null:Number((num(f[11])*1000/119).toFixed(3)),...f.slice(12,15).map(num),['Indeterminada','Estrella','Delta'][f[15]],estados[f[16]]]:Array(7).fill(null);
 rows.push([new Date(lastAt).toISOString(),id,seq,t,x,y,z,actual,high,peak,unit,temp,...extra].join(';')+'\r\n');
 trace.push({t,v:[x,y,z],seq});while(trace.length && t-trace[0].t>10000)trace.shift();
 if(multi){heat.push({t,v:[temp]});while(heat.length&&t-heat[0].t>60000)heat.shift();currentBin(t,actual,seq);}
 el('amps').textContent=amperes(actual);
 el('rangeNote').textContent=valid(actual)&&actual>limiteConsumo()?'Lectura por encima de la escala visible':'';
 el('temp').textContent=valid(temp)?temp.toFixed(1):'—';
 el('currentNote').textContent=!multi?'Carga el sketch de tres sensores':!valid(actual)?'Revisar señal 4–20 mA en GPIO4':unit==='%'?'Consumo en % del rango · configura la entrada del JXK para ver A':'Consumo medido en amperios';
 el('thermal').textContent=!multi?'Carga el sketch de tres sensores':valid(temp)?'Temperatura medida · GPIO5':'Revisar la NTC en GPIO5';
 el('count').textContent=rows.length+' muestras recibidas';el('csv').disabled=false;
 if(missing)el('loss').textContent='Muestras omitidas: '+missing;
 status();draw();
}
function status(){
 if(ultimoFotograma && Date.now()-ultimoFotograma>3500){
   el('oledEstado').textContent='Sin imagen reciente';el('oled').getContext('2d').clearRect(0,0,128,64);ultimoFotograma=0;
 }
 if(subscribed)el('state').textContent=!lastAt?'Esperando al ESP32':Date.now()-lastAt<3000?'● Recibiendo en vivo':'Sin muestras recientes';}
function chart(id,data,start,end,lo,hi,colors,labels,gap){
 const c=el(id),g=c.getContext('2d'),w=c.clientWidth,h=c.clientHeight,d=devicePixelRatio||1;
 if(c.width!==Math.round(w*d)||c.height!==Math.round(h*d)){c.width=Math.round(w*d);c.height=Math.round(h*d);}
 g.setTransform(d,0,0,d,0,0);g.clearRect(0,0,w,h);const l=39,r=w-8,top=14,b=h-24;
 const y=v=>b-(Math.min(hi,Math.max(lo,v))-lo)*(b-top)/(hi-lo);
 g.font='12px system-ui';g.textAlign='right';
 const ticks=id==='currentGraph'&&unit==='%'?6:(id==='g'||id==='currentGraph'?5:2);
 for(let i=0;i<=ticks;i++){const val=lo+(hi-lo)*i/ticks,yy=y(val);
 g.fillStyle='#bacadf';g.fillText(String(Number(val.toFixed(1)))+(id==='currentGraph'&&unit?' '+unit:''),l-6,yy+4);g.strokeStyle='#324157';g.beginPath();g.moveTo(l,yy);g.lineTo(r,yy);g.stroke();}
 g.strokeStyle='#70889e';g.beginPath();g.moveTo(l,top);g.lineTo(l,b);g.lineTo(r,b);g.stroke();
 g.textAlign='left';g.fillText(labels[0],l,h-4);g.textAlign='right';g.fillText(labels[1],r,h-4);
 colors.forEach((color,i)=>{g.strokeStyle=color;g.lineWidth=1.9;g.beginPath();let prev;
 for(const p of data){if(!valid(p.v[i])){prev=null;continue;}const x=l+(p.t-start)*(r-l)/(end-start),yy=y(p.v[i]);
 if(!prev||p.t-prev.t>gap||(p.seq!=null&&prev.seq+1!==p.seq))g.moveTo(x,yy);else g.lineTo(x,yy);prev=p;}g.stroke();g.lineWidth=1;});
}
function draw(){
 const now=trace.at(-1)?.t||0;
 chart('g',trace,now-10000,now,0,50,['#43e5d3','#ffad61','#c393ff'],['Hace 10 s','Ahora'],350);
 const vs=heat.map(p=>p.v[0]).filter(valid);
 const lo=vs.length?Math.floor(Math.min(...vs)/5)*5:0;
 const hi=vs.length?Math.max(lo+5,Math.ceil(Math.max(...vs)/5)*5):50;
 chart('thermalGraph',heat,now-60000,now,lo,hi,['#ffae68'],['−60 s','Ahora'],350);
 const serie=currentSeries(origin,now);
 const max=limiteConsumo();
 chart('currentGraph',serie,origin,Math.max(now,origin+1000),0,max,['#c2f76a'],['Inicio',Math.floor((now-origin)/1000)+' s'],Infinity);
}
el('csv').onclick=()=>{const url=URL.createObjectURL(new Blob(['\uFEFFrecepcion_UTC;sesion;numero;tiempo_ms;X_mg;Y_mg;Z_mg;corriente_actual;max_intervalo;pico_sesion;unidad_corriente;temperatura_C;GPIO4_V;lazo_mA;inclinacion_X_deg;inclinacion_Y_deg;inclinacion_Z_deg;fase_estimada;estado_estimado\r\n',...rows],{type:'text/csv;charset=utf-8'}));const a=document.createElement('a');a.href=url;a.download='ensayo-motor-'+Date.now()+'.csv';a.click();setTimeout(()=>URL.revokeObjectURL(url),2000);};
setInterval(()=>{if(ws?.readyState===1)ws.send(packet(192,[]));},12000);setInterval(status,1000);
addEventListener('resize',draw);draw();connect();

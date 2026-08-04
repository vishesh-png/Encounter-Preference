// encrypt_gate.mjs — wrap a built HTML page in a password gate whose payload is
// AES-GCM encrypted, so the deployed page ships ONLY ciphertext (nothing readable
// in view-source). Entering the password derives the key (PBKDF2) and decrypts the
// original page in-browser. Same-origin sessionStorage means one unlock covers every
// tracker on vishesh-png.github.io for the session.
//
// Usage: node encrypt_gate.mjs <input.html> <output.html> [password]
import fs from 'node:fs';
import { webcrypto as crypto } from 'node:crypto';

const [, , INP, OUTP, PW = process.env.TRK_PW || 'allo'] = process.argv;
if (!INP || !OUTP) { console.error('usage: node encrypt_gate.mjs <in.html> <out.html> [password]'); process.exit(1); }

const htmlBytes = fs.readFileSync(INP);
const enc = new TextEncoder();
const salt = crypto.getRandomValues(new Uint8Array(16));
const iv = crypto.getRandomValues(new Uint8Array(12));
const ITER = 250000;

const baseKey = await crypto.subtle.importKey('raw', enc.encode(PW), 'PBKDF2', false, ['deriveKey']);
const key = await crypto.subtle.deriveKey(
  { name: 'PBKDF2', salt, iterations: ITER, hash: 'SHA-256' },
  baseKey, { name: 'AES-GCM', length: 256 }, false, ['encrypt']);
const ct = new Uint8Array(await crypto.subtle.encrypt({ name: 'AES-GCM', iv }, key, htmlBytes));
const b64 = u => Buffer.from(u).toString('base64');

const gate = `<!doctype html><html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta name="robots" content="noindex, nofollow">
<title>Allo — Protected</title>
<style>
:root{color-scheme:light dark}
*{box-sizing:border-box}
body{margin:0;min-height:100vh;display:grid;place-items:center;background:#0e1116;color:#e6ebf3;
  font:15px/1.5 -apple-system,"Segoe UI",Roboto,Arial,sans-serif}
.box{width:min(92vw,360px);background:#161b23;border:1px solid #28303c;border-radius:16px;
  padding:30px 28px;box-shadow:0 10px 40px rgba(0,0,0,.5);text-align:center}
.lock{font-size:34px}
h1{font-size:17px;margin:12px 0 2px}
p{color:#9aa6b8;font-size:13px;margin:0 0 18px}
input{width:100%;padding:12px 14px;border:1px solid #28303c;border-radius:10px;background:#0e1116;
  color:#e6ebf3;font:inherit;font-size:15px;text-align:center;letter-spacing:.1em}
input:focus{outline:none;border-color:#6ea8ff}
button{width:100%;margin-top:12px;padding:12px;border:0;border-radius:10px;background:#2563eb;color:#fff;
  font:inherit;font-weight:650;font-size:15px;cursor:pointer}
button:hover{background:#1d4ed8}
.err{color:#f08a80;font-size:13px;min-height:18px;margin-top:10px}
.ft{color:#6b7688;font-size:11px;margin-top:16px}
#gate[hidden]{display:none}
#load{font-size:30px;opacity:.5;animation:pulse 1.1s ease-in-out infinite}
@keyframes pulse{0%,100%{opacity:.25}50%{opacity:.7}}
</style></head><body>
<div id="load">🔒</div>
<div class="box" id="gate" hidden>
  <div class="lock">🔒</div>
  <h1>Allo Clinic Trackers</h1>
  <p>Enter password to view</p>
  <form id="gf" autocomplete="off">
    <input type="password" id="pw" placeholder="Password" autocomplete="current-password">
    <button type="submit">Unlock</button>
    <div class="err" id="err"></div>
  </form>
  <div class="ft">Confidential — authorised staff only</div>
</div>
<script>
const SALT="${b64(salt)}",IV="${b64(iv)}",CT="${b64(ct)}",ITER=${ITER},SK="alloTrk";
const b=s=>Uint8Array.from(atob(s),c=>c.charCodeAt(0));
async function keyFrom(pw){
  const bk=await crypto.subtle.importKey('raw',new TextEncoder().encode(pw),'PBKDF2',false,['deriveKey']);
  return crypto.subtle.deriveKey({name:'PBKDF2',salt:b(SALT),iterations:ITER,hash:'SHA-256'},bk,{name:'AES-GCM',length:256},false,['decrypt']);
}
async function unlock(pw){
  try{
    const k=await keyFrom(pw);
    const pt=await crypto.subtle.decrypt({name:'AES-GCM',iv:b(IV)},k,b(CT));
    const html=new TextDecoder().decode(pt);
    try{sessionStorage.setItem(SK,pw);}catch(e){}
    document.open();document.write(html);document.close();
    return true;
  }catch(e){return false;}
}
document.getElementById('gf').addEventListener('submit',async e=>{
  e.preventDefault();
  const btn=e.target.querySelector('button');btn.disabled=true;btn.textContent='Unlocking…';
  const ok=await unlock(document.getElementById('pw').value.trim());
  if(!ok){document.getElementById('err').textContent='Wrong password';btn.disabled=false;btn.textContent='Unlock';
    document.getElementById('pw').select();}
});
function showGate(){document.getElementById('load').remove();const g=document.getElementById('gate');g.hidden=false;document.getElementById('pw').focus();}
// Ask once per tab: while the tab stays open (incl. tab-switches and in-tab reloads)
// sessionStorage keeps the password, so we unlock silently with no flash of the prompt.
// Closing the tab clears sessionStorage, so reopening asks again.
(async()=>{
  let p;try{p=sessionStorage.getItem(SK);}catch(e){}
  if(p && await unlock(p)) return;   // silent unlock — prompt never shown
  showGate();                        // only reached when there is no valid session
})();
</script></body></html>`;

fs.writeFileSync(OUTP, gate);
console.log('gated', OUTP, Math.round(gate.length / 1024) + 'KB');

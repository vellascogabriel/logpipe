#!/usr/bin/env node

/**
 * CSV generator script for LogPipe testing
 *
 * Default: 800 lines + 1 header, semicolon separator
 * Schema (header order):
 * timestamp;level;service;method;path;status;responseTime;userId;sessionId;ip;message;payloadSize;tags;env;host;errorCode
 *
 * Rules:
 * - level distribution: 70% INFO, 20% WARN, 10% ERROR
 * - status distribution (coherent with level): ~75% 2xx, 15% 4xx, 10% 5xx overall
 * - responseTime 0–2000 ms (skewed, mean around ~250ms)
 * - userId: UUID v4 or empty (~15% null)
 * - sessionId: 16 hex chars or empty (~10% null)
 * - tags: 0–3 items joined by pipe, empty when none
 * - errorCode only when level === 'ERROR' (E_AUTH|E_TIMEOUT|E_DB|E_DOWNSTREAM), else empty
 * - message: quote if contains separator or quotes; escape quotes by doubling
 */

const fs = require('fs');
const path = require('path');

function parseArgs(argv) {
  const args = { lines: 800, out: 'data/logs.csv', sep: ';', hours: 4, start: undefined };
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (a === '--lines' || a === '-n') args.lines = parseInt(argv[++i], 10);
    else if (a === '--out' || a === '-o') args.out = argv[++i];
    else if (a === '--sep' || a === '--separator') args.sep = argv[++i];
    else if (a === '--hours') args.hours = parseInt(argv[++i], 10);
    else if (a === '--start') args.start = argv[++i];
  }
  if (!Number.isFinite(args.lines) || args.lines <= 0) throw new Error('Invalid --lines');
  if (!args.sep) throw new Error('Invalid --sep');
  return args;
}

function rand() { return Math.random(); }
function choice(arr) { return arr[Math.floor(rand() * arr.length)]; }
function randomInt(min, max) { return Math.floor(rand() * (max - min + 1)) + min; }
function weightedChoice(pairs) {
  const total = pairs.reduce((s, [, w]) => s + w, 0);
  let r = rand() * total;
  for (const [v, w] of pairs) { if ((r -= w) <= 0) return v; }
  return pairs[pairs.length - 1][0];
}

function uuidv4() {
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, c => {
    const r = Math.random() * 16 | 0;
    const v = c === 'x' ? r : (r & 0x3 | 0x8);
    return v.toString(16);
  });
}
function hex(n) { let s = ''; for (let i = 0; i < n; i++) s += Math.floor(Math.random() * 16).toString(16); return s; }
function ipv4() { const o=()=>Math.floor(rand()*256); return `${o()}.${o()}.${o()}.${o()}`; }
function skewedMs(max = 2000, mean = 250) { const u = Math.max(1e-9, rand()); const val = -Math.log(u) * (mean / Math.log(2)); return Math.max(0, Math.min(Math.round(val), max)); }

function pickLevel() { return weightedChoice([[ 'INFO',70 ],[ 'WARN',20 ],[ 'ERROR',10 ]]); }
function pickService() { return choice(['auth','payments','orders','api-gateway']); }
function pickMethod() { return choice(['GET','POST','PUT','DELETE']); }
function pickPath(method) {
  const base = choice(['/api/login','/api/orders','/api/payments','/api/users/:id','/health']);
  if (base.includes(':id')) return base.replace(':id', String(randomInt(1,5000)));
  if (method==='GET' && base!=='/health' && rand()<0.25) return base + choice(['?page=1','?page=2','?limit=50','?sort=desc','?filter=recent']);
  return base;
}
function pickStatus(level) {
  if (level==='ERROR') return weightedChoice([[randomInt(500,599),80],[randomInt(400,499),15],[randomInt(200,299),5]]);
  if (level==='WARN')  return weightedChoice([[randomInt(200,299),60],[randomInt(400,499),25],[randomInt(500,599),15]]);
  return weightedChoice([[randomInt(200,299),90],[randomInt(400,499),7],[randomInt(500,599),3]]);
}
function pickTags() { const pool=['auth','cache','db','payment','retry','network']; const n=randomInt(0,3); const out=new Set(); while(out.size<n) out.add(choice(pool)); return Array.from(out); }
function pickMessage(level, method, path, status) {
  const base={INFO:['request handled','operation completed','resource fetched','processing successful','heartbeat ok'], WARN:['slow response detected','retry scheduled','partial failure','deprecated endpoint used'], ERROR:['upstream error','database error','authentication failed','timeout while calling dependency']};
  return `${method} ${path} -> ${status}: ${choice(base[level])}`;
}
function pickEnv() { return weightedChoice([[ 'dev',25 ],[ 'staging',25 ],[ 'prod',50 ]]); }
function pickHost(env){ return `${env}-host-${randomInt(1,50)}`; }

function csvEscape(value, sep) {
  if (value == null) return '';
  const s = String(value);
  if (s.includes('"')) {
    const doubled = s.replace(/"/g, '""');
    if (doubled.includes(sep) || /[\r\n]/.test(doubled)) return `"${doubled}` + '"';
    return `"${doubled}` + '"';
  }
  if (s.includes(sep) || /[\r\n]/.test(s)) return `"${s}"`;
  return s;
}

function generate({ lines, out, sep, hours, start }) {
  const outPath = path.resolve(out);
  const dir = path.dirname(outPath);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  const fd = fs.openSync(outPath, 'w');

  const header = ['timestamp','level','service','method','path','status','responseTime','userId','sessionId','ip','message','payloadSize','tags','env','host','errorCode'].join(sep) + '\n';
  fs.writeSync(fd, header);

  const startDate = start ? new Date(start) : new Date();
  const startMs = startDate.getTime() - hours * 3600_000;

  // realistic repetition pools
  const userPool = Array.from({ length: 200 }, () => uuidv4());
  const sessionPool = Array.from({ length: 300 }, () => hex(16));

  for (let i = 0; i < lines; i++) {
    const ts = new Date(startMs + Math.floor(rand() * hours * 3600_000)).toISOString();
    const level = pickLevel();
    const service = pickService();
    const method = pickMethod();
    const pathStr = pickPath(method);
    const status = pickStatus(level);
    const responseTime = skewedMs(2000, 250);
    const userId = rand() < 0.15 ? '' : choice(userPool);
    const sessionId = rand() < 0.10 ? '' : choice(sessionPool);
    const ip = ipv4();
    const message = pickMessage(level, method, pathStr, status);
    const payloadSize = randomInt(0, 200000);
    const tagsArr = pickTags();
    const tags = tagsArr.length ? tagsArr.join('|') : '';
    const env = pickEnv();
    const host = pickHost(env);
    const errorCode = level === 'ERROR' ? choice(['E_AUTH','E_TIMEOUT','E_DB','E_DOWNSTREAM']) : '';

    const row = [
      ts,
      level,
      service,
      method,
      pathStr,
      status,
      responseTime,
      userId,
      sessionId,
      ip,
      message,
      payloadSize,
      tags,
      env,
      host,
      errorCode
    ].map(v => csvEscape(v, sep)).join(sep) + '\n';

    fs.writeSync(fd, row, null, 'utf8');
  }

  fs.closeSync(fd);
  console.log(`Generated CSV with ${lines} rows (+header) into: ${outPath}`);
}

(function main() {
  try {
    const args = parseArgs(process.argv);
    generate(args);
  } catch (err) {
    console.error('Error:', err.message);
    process.exit(1);
  }
})();

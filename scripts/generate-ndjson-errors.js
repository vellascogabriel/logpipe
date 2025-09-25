#!/usr/bin/env node

/**
 * NDJSON generator with blanks, malformed and missing/extra fields
 *
 * Default output: 500 lines total
 * - 10 blank lines (just "\n")
 * - 10 intentionally invalid JSON lines (malformed)
 * - 480 valid lines following the same schema used in generate-ndjson.js
 *   - ~5% of valid lines omit 1–2 non-critical fields (sessionId, tags, payloadSize)
 *   - ~2% of valid lines include an unexpected extra field (e.g., debug: true)
 *
 * Usage (Windows/PowerShell):
 *   node scripts/generate-ndjson-errors.js --out data/logs_with_errors.ndjson
 *   node scripts/generate-ndjson-errors.js -n 500 -o data/logs_with_errors.ndjson \
 *     --invalid 10 --empty 10 --omitPct 0.05 --extraPct 0.02
 */

const fs = require('fs');
const path = require('path');

function parseArgs(argv) {
  const args = {
    lines: 500,
    out: 'data/logs_with_errors.ndjson',
    invalid: 10,
    empty: 10,
    omitPct: 0.05, // of valid lines
    extraPct: 0.02 // of valid lines
  };
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (a === '--lines' || a === '-n') args.lines = parseInt(argv[++i], 10);
    else if (a === '--out' || a === '-o') args.out = argv[++i];
    else if (a === '--invalid') args.invalid = parseInt(argv[++i], 10);
    else if (a === '--empty') args.empty = parseInt(argv[++i], 10);
    else if (a === '--omitPct') args.omitPct = parseFloat(argv[++i]);
    else if (a === '--extraPct') args.extraPct = parseFloat(argv[++i]);
  }
  if (!Number.isFinite(args.lines) || args.lines <= 0) throw new Error('Invalid --lines');
  if (args.invalid < 0 || args.empty < 0) throw new Error('Invalid counts');
  if (args.invalid + args.empty > args.lines) throw new Error('invalid+empty must be <= lines');
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

function skewedMs(max = 2000, mean = 250) {
  const u = Math.max(1e-9, rand());
  const val = -Math.log(u) * (mean / Math.log(2));
  return Math.max(0, Math.min(Math.round(val), max));
}

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
function pickTags() {
  const pool=['auth','cache','db','payment','retry','network'];
  const n=randomInt(0,3); const out=new Set();
  while(out.size<n) out.add(choice(pool));
  return Array.from(out);
}
function pickMessage(level, method, path, status) {
  const base={INFO:['request handled','operation completed','resource fetched','processing successful','heartbeat ok'], WARN:['slow response detected','retry scheduled','partial failure','deprecated endpoint used'], ERROR:['upstream error','database error','authentication failed','timeout while calling dependency']};
  return `${method} ${path} -> ${status}: ${choice(base[level])}`;
}
function pickEnv() { return weightedChoice([[ 'dev',25 ],[ 'staging',25 ],[ 'prod',50 ]]); }
function pickHost(env){ return `${env}-host-${randomInt(1,50)}`; }

function selectPositions(n, total, excluded = new Set()) {
  const pos = new Set();
  while (pos.size < n) {
    const i = randomInt(0, total - 1);
    if (!excluded.has(i)) pos.add(i);
  }
  return pos;
}

function main() {
  const args = parseArgs(process.argv);
  const outPath = path.resolve(args.out);
  const dir = path.dirname(outPath);
  if (!fs.existsSync(dir)) fs.mkdirSync(dir, { recursive: true });
  const fd = fs.openSync(outPath, 'w');

  const total = args.lines;
  const blankIdx = selectPositions(args.empty, total);
  const invalidIdx = selectPositions(args.invalid, total, blankIdx);

  // Determine which valid line indexes will get omissions/extras
  const validCount = total - args.empty - args.invalid;
  const omitCount = Math.max(0, Math.round(args.omitPct * validCount));
  const extraCount = Math.max(0, Math.round(args.extraPct * validCount));

  // Map from sequence of valid line to actual global index
  const validGlobalIdx = [];
  for (let i = 0; i < total; i++) if (!blankIdx.has(i) && !invalidIdx.has(i)) validGlobalIdx.push(i);
  const omitSet = new Set();
  const extraSet = new Set();
  // choose random positions among valid indexes
  for (const i of selectPositions(omitCount, validGlobalIdx.length)) omitSet.add(validGlobalIdx[i]);
  for (const i of selectPositions(extraCount, validGlobalIdx.length, omitSet)) extraSet.add(validGlobalIdx[i]);

  // Pools for realistic repetition
  const start = Date.now() - 4 * 3600_000; // 4h window
  const userPool = Array.from({ length: 200 }, () => uuidv4());
  const sessionPool = Array.from({ length: 300 }, () => hex(16));

  let wroteBytes = 0;

  for (let i = 0; i < total; i++) {
    // blank lines
    if (blankIdx.has(i)) { wroteBytes += fs.writeSync(fd, '\n'); continue; }

    // malformed lines
    if (invalidIdx.has(i)) {
      const kind = i % 5;
      const malformed = [
        '{ "timestamp": "2025-01-01T00:00:00.000Z", "level": "INFO", ', // missing closing
        '{ timestamp: "2025-01-01T00:00:00.000Z", "level": "INFO" }\n',   // key without quotes
        '{ "level": "INFO", "message": "trailing comma", }',              // trailing comma
        'not a json line at all',                                               // plain text
        '{ "level": "ERROR"  "status": 500 }'                              // missing comma between fields
      ][kind];
      wroteBytes += fs.writeSync(fd, malformed + '\n');
      continue;
    }

    // valid line
    const ts = new Date(start + Math.floor(rand() * 4 * 3600_000)).toISOString();
    const level = pickLevel();
    const service = pickService();
    const method = pickMethod();
    const p = pickPath(method);
    const status = pickStatus(level);
    const responseTime = skewedMs(2000, 250);
    let userId = rand() < 0.15 ? null : choice(userPool);
    let sessionId = rand() < 0.10 ? null : choice(sessionPool);
    const ip = ipv4();
    const message = pickMessage(level, method, p, status);
    let payloadSize = randomInt(0, 200000);
    let tags = (() => { const t = pickTags(); return t; })();
    const env = pickEnv();
    const host = pickHost(env);
    const errorCode = level === 'ERROR' ? choice(['E_AUTH','E_TIMEOUT','E_DB','E_DOWNSTREAM']) : undefined;

    const obj = {
      timestamp: ts,
      level,
      service,
      method,
      path: p,
      status,
      responseTime,
      userId,
      sessionId,
      ip,
      message,
      payloadSize,
      tags,
      meta: { env, host }
    };
    if (errorCode) obj.errorCode = errorCode;

    // omissions on ~5% of valid lines
    if (omitSet.has(i)) {
      const candidates = ['sessionId', 'tags', 'payloadSize'];
      const toOmit = randomInt(1, 2);
      for (let k = 0; k < toOmit; k++) {
        const f = choice(candidates);
        delete obj[f];
      }
    }

    // extra unexpected field on ~2% of valid lines
    if (extraSet.has(i)) {
      if (rand() < 0.5) obj.debug = true; else obj.traceId = hex(16);
    }

    wroteBytes += fs.writeSync(fd, JSON.stringify(obj) + '\n', null, 'utf8');
  }

  fs.closeSync(fd);
  console.log(`Generated ${total} NDJSON lines into: ${outPath} ( ${(wroteBytes/1024/1024).toFixed(2)} MB )`);
}

try { main(); } catch (e) { console.error('Error:', e.message); process.exit(1); }

#!/usr/bin/env node

/**
 * NDJSON generator script for LogPipe testing
 *
 * Usage examples (Windows/PowerShell):
 *   node scripts/generate-ndjson.js --lines 1000 --out sample.ndjson
 *   node scripts/generate-ndjson.js -n 500 -o sample.ndjson
 *
 * This script adheres to the schema and distributions described in the project docs:
 * - level: 70% INFO, 20% WARN, 10% ERROR
 * - status: ~75% 2xx, 15% 4xx, 10% 5xx (coherent with level)
 * - responseTime: 0–2000ms (skewed with mean around ~250ms)
 * - userId: UUID v4 or null (~15%)
 * - sessionId: 16 hex chars or null (~10%)
 * - ip: valid IPv4
 * - tags: array with 0–3 items
 * - meta: { env: dev|staging|prod, host }
 * - errorCode: only present when level === 'ERROR'
 */

const fs = require('fs');
const path = require('path');

// --- simple argv parsing (no external deps) ---
function parseArgs(argv) {
  const args = { lines: 1000, out: 'sample.ndjson' };
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    if (a === '--lines' || a === '-n') {
      args.lines = parseInt(argv[++i], 10);
    } else if (a === '--out' || a === '-o') {
      args.out = argv[++i];
    } else if (a === '--hours') {
      args.hours = parseInt(argv[++i], 10);
    } else if (a === '--start') {
      args.start = argv[++i]; // ISO date
    }
  }
  if (!Number.isFinite(args.lines) || args.lines <= 0) {
    throw new Error('Invalid --lines value');
  }
  return args;
}

function rand() { return Math.random(); }
function choice(arr) { return arr[Math.floor(rand() * arr.length)]; }
function weightedChoice(pairs) {
  // pairs: [[value, weight], ...]
  const total = pairs.reduce((s, [, w]) => s + w, 0);
  let r = rand() * total;
  for (const [v, w] of pairs) {
    if ((r -= w) <= 0) return v;
  }
  return pairs[pairs.length - 1][0];
}

function uuidv4() {
  // RFC 4122 v4 (simple)
  return 'xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx'.replace(/[xy]/g, c => {
    const r = Math.random() * 16 | 0;
    const v = c === 'x' ? r : (r & 0x3 | 0x8);
    return v.toString(16);
  });
}

function hex(n) {
  let s = '';
  for (let i = 0; i < n; i++) s += Math.floor(Math.random() * 16).toString(16);
  return s;
}

function ipv4() {
  const octet = () => Math.floor(rand() * 256);
  // Avoid special ranges occasionally
  return `${octet()}.${octet()}.${octet()}.${octet()}`;
}

function randomInt(min, max) { return Math.floor(rand() * (max - min + 1)) + min; }
function randomFloat(min, max) { return rand() * (max - min) + min; }

function skewedMs(max = 2000, mean = 250) {
  // Use a simple exponential-like distribution: -ln(U) scaled
  const u = Math.max(1e-9, rand());
  const val = -Math.log(u) * (mean / Math.log(2));
  return Math.max(0, Math.min(Math.round(val), max));
}

function pickLevel() {
  // 70% INFO, 20% WARN, 10% ERROR
  return weightedChoice([
    ['INFO', 70],
    ['WARN', 20],
    ['ERROR', 10]
  ]);
}

function pickService() { return choice(['auth', 'payments', 'orders', 'api-gateway']); }
function pickMethod() { return choice(['GET', 'POST', 'PUT', 'DELETE']); }
function pickPath(method) {
  const base = choice(['/api/login', '/api/orders', '/api/payments', '/api/users/:id', '/health']);
  if (base.includes(':id')) {
    const id = randomInt(1, 5000);
    return base.replace(':id', String(id));
  }
  // Simulate REST-ish: sometimes add query for GET
  if (method === 'GET' && base !== '/health' && rand() < 0.25) {
    const qp = choice(['?page=1', '?page=2', '?limit=50', '?sort=desc', '?filter=recent']);
    return base + qp;
  }
  return base;
}

function pickStatus(level) {
  // Target: ~75% 2xx, 15% 4xx, 10% 5xx overall.
  // Coherence: ERROR -> mostly 5xx, WARN -> mix 2xx/4xx/5xx, INFO -> mostly 2xx
  if (level === 'ERROR') {
    return weightedChoice([
      [randomInt(500, 599), 80],
      [randomInt(400, 499), 15],
      [randomInt(200, 299), 5]
    ]);
  }
  if (level === 'WARN') {
    return weightedChoice([
      [randomInt(200, 299), 60],
      [randomInt(400, 499), 25],
      [randomInt(500, 599), 15]
    ]);
  }
  // INFO
  return weightedChoice([
    [randomInt(200, 299), 90],
    [randomInt(400, 499), 7],
    [randomInt(500, 599), 3]
  ]);
}

function pickTags() {
  const pool = ['auth', 'cache', 'db', 'payment', 'retry', 'network'];
  const n = randomInt(0, 3);
  const out = new Set();
  while (out.size < n) out.add(choice(pool));
  return Array.from(out);
}

function pickMessage(level, method, path, status) {
  const base = {
    INFO: [
      'request handled', 'operation completed', 'resource fetched', 'processing successful', 'heartbeat ok'
    ],
    WARN: [
      'slow response detected', 'retry scheduled', 'partial failure', 'deprecated endpoint used'
    ],
    ERROR: [
      'upstream error', 'database error', 'authentication failed', 'timeout while calling dependency'
    ]
  };
  const msg = choice(base[level]);
  return `${method} ${path} -> ${status}: ${msg}`;
}

function pickEnv() { return weightedChoice([['dev', 25], ['staging', 25], ['prod', 50]]); }
function pickHost(env) {
  const suffix = randomInt(1, 50);
  return `${env}-host-${suffix}`;
}

function maybeUUID(probNull = 0.15) {
  return rand() < probNull ? null : uuidv4();
}

function maybeHex16(probNull = 0.10) {
  return rand() < probNull ? null : hex(16);
}

function pickErrorCode(level) {
  if (level !== 'ERROR') return undefined;
  return choice(['E_AUTH', 'E_TIMEOUT', 'E_DB', 'E_DOWNSTREAM']);
}

function generate({ lines, out, hours = 4, start }) {
  const startDate = start ? new Date(start) : new Date();
  const startMs = startDate.getTime() - hours * 3600_000; // go back in time by window
  const fd = fs.openSync(path.resolve(out), 'w');
  let wrote = 0;

  // Reuse small pools for userId/sessionId to create realistic aggregation
  const userPool = Array.from({ length: 200 }, () => uuidv4());
  const sessionPool = Array.from({ length: 300 }, () => hex(16));

  try {
    for (let i = 0; i < lines; i++) {
      const ts = new Date(startMs + Math.floor(rand() * hours * 3600_000)).toISOString();
      const level = pickLevel();
      const service = pickService();
      const method = pickMethod();
      const pathStr = pickPath(method);
      const status = pickStatus(level);
      const responseTime = skewedMs(2000, 250);
      const userId = rand() < 0.15 ? null : choice(userPool);
      const sessionId = rand() < 0.10 ? null : choice(sessionPool);
      const ip = ipv4();
      const message = pickMessage(level, method, pathStr, status);
      const payloadSize = randomInt(0, 200000);
      const tags = pickTags();
      const env = pickEnv();
      const host = pickHost(env);
      const errorCode = pickErrorCode(level);

      const obj = {
        timestamp: ts,
        level,
        service,
        method,
        path: pathStr,
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

      const line = JSON.stringify(obj) + '\n';
      wrote += fs.writeSync(fd, line, null, 'utf8');
    }
  } finally {
    fs.closeSync(fd);
  }

  console.log(`Generated ${lines} NDJSON lines into: ${path.resolve(out)} (${(wrote/1024/1024).toFixed(2)} MB)`);
}

(function main() {
  try {
    const args = parseArgs(process.argv);
    // ensure dir exists
    const outDir = path.dirname(path.resolve(args.out));
    if (!fs.existsSync(outDir)) fs.mkdirSync(outDir, { recursive: true });
    generate(args);
  } catch (err) {
    console.error('Error:', err.message);
    process.exit(1);
  }
})();

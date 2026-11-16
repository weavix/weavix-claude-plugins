'use strict';

/**
 * Tests for the parquet sink: config, blob naming, encoding round-trip, upload semantics,
 * retry retention, and an end-to-end hook run against a local fake blob endpoint.
 * Run: node plugins/claude-telemetry/test/parquet.test.cjs
 */

const assert = require('node:assert');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const http = require('node:http');
const { spawn } = require('node:child_process');
const { ParquetReader } = require('@dsnp/parquetjs');
const {
    resolveBlobConfig, spoolPath, appendSpool, claimBatches, batchId, blobUrlFor,
    uploadBlob, flushParquetBatches, flushAll, logFailure, toRow, buildEvent, COLUMNS, PARQUET_COLUMN_TYPES,
    PARQUET_ENTITY, MAX_ROWS_PER_PARQUET, ORPHAN_RETRY_AFTER_MS, SPOOL_HOLD_MAX_BYTES, legacyHookActive,
} = require('../src/telemetry.cjs');
const { encodeParquet } = require('../dist/parquet-writer.cjs');

const HOOK = path.join(__dirname, '..', 'src', 'telemetry.cjs');
const SAS = 'https://acct.blob.core.windows.net/parquet/claude?sv=2021-08-06&sp=c&sig=abc%2Bdef';
const tmp = () => fs.mkdtempSync(path.join(os.tmpdir(), 'cct-'));
const NO_FILE = { CLAUDE_TELEMETRY_CONFIG: path.join(os.tmpdir(), 'cct-does-not-exist.json') };

let pass = 0;
const tests = [];
const test = (name, fn) => tests.push({ name, fn });

/** Read every row of a parquet buffer back as plain objects. */
async function readParquet(buf) {
    const reader = await ParquetReader.openBuffer(buf);
    const cursor = reader.getCursor();
    const rows = [];
    let r;
    while ((r = await cursor.next())) rows.push(r);
    await reader.close();
    return rows;
}

/** A row for one event, in COLUMNS order. */
function sampleRow(overrides = {}) {
    const row = toRow(buildEvent({ hook_event_name: 'Stop', session_id: 's1', cwd: '/repo' }, {
        host: 'h', user: 'ada', environment: 'local', attended: true,
    }));
    for (const [k, v] of Object.entries(overrides)) row[COLUMNS.indexOf(k)] = v;
    return row;
}

/** Local HTTP server that records PUTs and answers with `status(req)`. */
async function fakeBlobServer(status = () => 201) {
    const puts = [];
    const server = http.createServer((req, res) => {
        const chunks = [];
        req.on('data', c => chunks.push(c));
        req.on('end', () => {
            puts.push({ method: req.method, url: req.url, headers: req.headers, body: Buffer.concat(chunks) });
            const r = status(req);
            res.statusCode = typeof r === 'number' ? r : r.status;
            if (typeof r === 'object' && r.code) res.setHeader('x-ms-error-code', r.code);
            res.end();
        });
    });
    await new Promise(r => server.listen(0, '127.0.0.1', r));
    const { port } = server.address();
    return { puts, base: `http://127.0.0.1:${port}/parquet/claude?sp=c&sig=test`, close: () => new Promise(r => server.close(r)) };
}

test('resolveBlobConfig: env wins over the config file', () => {
    const dir = tmp();
    const file = path.join(dir, 'telemetry.json');
    fs.writeFileSync(file, JSON.stringify({ sas_url: 'https://file.example/p?sig=f' }));
    assert.equal(resolveBlobConfig({ CLAUDE_TELEMETRY_CONFIG: file }).sasUrl, 'https://file.example/p?sig=f');
    assert.equal(resolveBlobConfig({ CLAUDE_TELEMETRY_CONFIG: file, CLAUDE_TELEMETRY_SAS_URL: SAS }).sasUrl, new URL(SAS).toString());
});

test('resolveBlobConfig: null when absent, unsigned, malformed, or plain http off loopback', () => {
    assert.equal(resolveBlobConfig({ ...NO_FILE }), null);
    assert.equal(resolveBlobConfig({ ...NO_FILE, CLAUDE_TELEMETRY_SAS_URL: 'https://a.example/p' }), null);
    assert.equal(resolveBlobConfig({ ...NO_FILE, CLAUDE_TELEMETRY_SAS_URL: 'not a url' }), null);
    assert.equal(resolveBlobConfig({ ...NO_FILE, CLAUDE_TELEMETRY_SAS_URL: 'http://a.example/p?sig=x' }), null);
    assert.ok(resolveBlobConfig({ ...NO_FILE, CLAUDE_TELEMETRY_SAS_URL: 'http://127.0.0.1:1/p?sig=x' }));
});

test('resolveBlobConfig: an unreadable or invalid config file is treated as absent', () => {
    const dir = tmp();
    const file = path.join(dir, 'telemetry.json');
    fs.writeFileSync(file, '{not json');
    assert.equal(resolveBlobConfig({ CLAUDE_TELEMETRY_CONFIG: file }), null);
});

test('resolveBlobConfig reports expiry from the SAS se parameter', () => {
    const at = Date.parse('2027-01-01T00:00:00Z');
    const url = 'https://a.example/p?se=2027-01-01T00%3A00%3A00Z&sp=c&sig=x';
    assert.deepEqual(
        [resolveBlobConfig({ ...NO_FILE, CLAUDE_TELEMETRY_SAS_URL: url }, at - 1).expired,
            resolveBlobConfig({ ...NO_FILE, CLAUDE_TELEMETRY_SAS_URL: url }, at).expired],
        [false, true],
    );
    assert.equal(resolveBlobConfig({ ...NO_FILE, CLAUDE_TELEMETRY_SAS_URL: SAS }).expired, false);
    assert.equal(resolveBlobConfig({ ...NO_FILE, CLAUDE_TELEMETRY_SAS_URL: SAS }).expiresAt, null);
});

test('flushAll keeps batches and logs when the SAS has expired', async () => {
    const dir = tmp();
    const prev = process.env.CLAUDE_SF_SPOOL;
    process.env.CLAUDE_SF_SPOOL = path.join(dir, 'spool.ada.parquet.ndjson');
    try {
        appendSpool(process.env.CLAUDE_SF_SPOOL, sampleRow());
        const files = claimBatches(process.env.CLAUDE_SF_SPOOL, Date.now(), '77777777-0000-4000-8000-000000000000');
        await new Promise(r => flushAll({ sasUrl: SAS, expired: true }, files, { timeoutMs: 100, budgetMs: 100 }, r));
        assert.ok(fs.existsSync(files[0]));
        assert.match(fs.readFileSync(path.join(dir, 'errors.log'), 'utf8'), /parquet sas-expired/);
    } finally {
        if (prev === undefined) delete process.env.CLAUDE_SF_SPOOL; else process.env.CLAUDE_SF_SPOOL = prev;
    }
});

test('spoolPath is named apart from the legacy Snowflake hook spool', () => {
    assert.equal(spoolPath({ CLAUDE_SF_SPOOL: '/t/x.ndjson' }), '/t/x.ndjson');
    assert.match(spoolPath({ USER: 'ada' }), /claude-telemetry\/spool\.ada\.parquet\.ndjson$/);
});

test('claimBatches on the parquet spool never adopts the legacy hook\'s batches', () => {
    const dir = tmp();
    const legacy = path.join(dir, 'spool.ada.ndjson');
    const pq = path.join(dir, 'spool.ada.parquet.ndjson');
    appendSpool(legacy, sampleRow());
    appendSpool(pq, sampleRow());
    const old = Date.now() - ORPHAN_RETRY_AFTER_MS * 2;
    claimBatches(legacy, old, 'aaaa');
    const claimed = claimBatches(pq, Date.now(), 'bbbb');
    assert.equal(claimed.length, 1);
    assert.ok(claimed.every(f => path.basename(f).startsWith('spool.ada.parquet.ndjson.batch.')));
});

test('batchId is stable when an orphaned batch is re-adopted', () => {
    const first = '/t/spool.ada.parquet.ndjson.batch.1700000000000.1b2c3d4e-0000-4000-8000-000000000001';
    const adopted = `${first}.r9f8e7d6c-0000-4000-8000-000000000002`;
    assert.equal(batchId(first), '1700000000000-1b2c3d4e-0000-4000-8000-000000000001');
    assert.equal(batchId(adopted), batchId(first));
});

test('blobUrlFor appends entity folder and part, and keeps the SAS query', () => {
    const u = blobUrlFor(SAS, 'b1', 2);
    assert.equal(u.pathname, `/parquet/claude/${PARQUET_ENTITY}/b1-2.parquet`);
    assert.equal(u.searchParams.get('sig'), 'abc+def');
    assert.equal(blobUrlFor('https://a.example/parquet/claude/?sig=x', 'b', 0).pathname, `/parquet/claude/${PARQUET_ENTITY}/b-0.parquet`);
});

test('encodeParquet round-trips every column with the declared types and keeps nulls', async () => {
    const row = sampleRow({ INPUT_TOKENS: 12, OUTPUT_TOKENS: 0, MODEL: 'claude-opus-5-5', SESSION_ATTENDED: false });
    const back = await readParquet(await encodeParquet([row, sampleRow()], COLUMNS, PARQUET_COLUMN_TYPES));
    assert.equal(back.length, 2);
    assert.equal(back[0].SESSION_ID, 's1');
    assert.equal(back[0].INPUT_TOKENS, 12n);
    assert.equal(back[0].OUTPUT_TOKENS, 0n);
    assert.equal(back[0].SESSION_ATTENDED, false);
    assert.equal(back[0].MODEL, 'claude-opus-5-5');
    assert.equal(back[1].SESSION_ATTENDED, true);
    assert.equal(back[1].SKILL, undefined);
    assert.deepEqual(Object.keys(back[0]).filter(k => !COLUMNS.includes(k)), []);
});

test('encodeParquet nulls a token value that is not a number instead of failing the file', async () => {
    const back = await readParquet(await encodeParquet([sampleRow({ INPUT_TOKENS: 'abc' })], COLUMNS, PARQUET_COLUMN_TYPES));
    assert.equal(back[0].INPUT_TOKENS, undefined);
});

test('uploadBlob sends a create-only block blob PUT and treats 201, 409 and a create-only overwrite 403 as success', async () => {
    let next = 201;
    const srv = await fakeBlobServer(() => next);
    try {
        const url = blobUrlFor(srv.base, 'b', 0);
        const r1 = await new Promise(r => uploadBlob(url, Buffer.from('PAR1'), (ok, s) => r({ ok, s }), 2000));
        next = 409;
        const r2 = await new Promise(r => uploadBlob(url, Buffer.from('PAR1'), (ok, s) => r({ ok, s }), 2000));
        next = { status: 403, code: 'UnauthorizedBlobOverwrite' };
        const r3 = await new Promise(r => uploadBlob(url, Buffer.from('PAR1'), (ok, s) => r({ ok, s }), 2000));
        next = { status: 403, code: 'AuthorizationPermissionMismatch' };
        const r4 = await new Promise(r => uploadBlob(url, Buffer.from('PAR1'), (ok, s) => r({ ok, s }), 2000));
        assert.deepEqual([r1, r2, r3, r4], [{ ok: true, s: 201 }, { ok: true, s: 409 }, { ok: true, s: 403 }, { ok: false, s: 403 }]);
        const h = srv.puts[0].headers;
        assert.equal(srv.puts[0].method, 'PUT');
        assert.equal(h['x-ms-blob-type'], 'BlockBlob');
        assert.equal(h['if-none-match'], '*');
        assert.ok(srv.puts[0].url.includes('sig=test'));
    } finally { await srv.close(); }
});

test('uploadBlob reports failure when the endpoint is unreachable', async () => {
    const ok = await new Promise(r => uploadBlob(new URL('http://127.0.0.1:1/x?sig=y'), Buffer.from('x'), r, 1000));
    assert.equal(ok, false);
});

test('flushParquetBatches uploads each batch and deletes it on success', async () => {
    const dir = tmp();
    const sp = path.join(dir, 'spool.ada.parquet.ndjson');
    appendSpool(sp, sampleRow());
    appendSpool(sp, sampleRow());
    const files = claimBatches(sp, Date.now(), '11111111-0000-4000-8000-000000000000');
    const srv = await fakeBlobServer();
    try {
        const ok = await new Promise(r => flushParquetBatches({ sasUrl: srv.base }, files, 2000, r));
        assert.equal(ok, true);
        assert.equal(srv.puts.length, 1);
        assert.match(srv.puts[0].url, new RegExp(`/parquet/claude/${PARQUET_ENTITY}/\\d+-11111111-0000-4000-8000-000000000000-0\\.parquet\\?`));
        assert.equal((await readParquet(srv.puts[0].body)).length, 2);
        assert.equal(fs.existsSync(files[0]), false);
    } finally { await srv.close(); }
});

test('flushParquetBatches keeps a failed batch with a fresh mtime and reports why', async () => {
    const dir = tmp();
    const sp = path.join(dir, 'spool.ada.parquet.ndjson');
    appendSpool(sp, sampleRow());
    const files = claimBatches(sp, Date.now(), '22222222-0000-4000-8000-000000000000');
    const old = (Date.now() - 3600 * 1000) / 1000;
    fs.utimesSync(files[0], old, old);
    const reasons = [];
    const ok = await new Promise(r => flushParquetBatches({ sasUrl: 'https://x/p?sig=s' }, files, 1000, r, {
        upload: (_u, _b, done) => done(false, 500),
        onError: reason => reasons.push(reason),
    }));
    assert.equal(ok, false);
    assert.ok(fs.existsSync(files[0]));
    assert.ok(fs.statSync(files[0]).mtimeMs > Date.now() - 60 * 1000);
    assert.deepEqual(reasons, ['upload-failed status=500']);
});

test('flushParquetBatches retries a batch under the same blob names', async () => {
    const dir = tmp();
    const sp = path.join(dir, 'spool.ada.parquet.ndjson');
    appendSpool(sp, sampleRow());
    const files = claimBatches(sp, Date.now(), '33333333-0000-4000-8000-000000000000');
    const urls = [];
    let attempt = 0;
    const upload = (u, _b, done) => { urls.push(u.pathname); attempt += 1; done(attempt > 1, attempt > 1 ? 409 : 500); };
    assert.equal(await new Promise(r => flushParquetBatches({ sasUrl: SAS }, files, 1000, r, { upload })), false);
    const old = (Date.now() - ORPHAN_RETRY_AFTER_MS * 2) / 1000;
    fs.utimesSync(files[0], old, old);
    const adopted = claimBatches(sp, Date.now(), '44444444-0000-4000-8000-000000000000');
    assert.equal(adopted.length, 1);
    assert.equal(await new Promise(r => flushParquetBatches({ sasUrl: SAS }, adopted, 1000, r, { upload })), true);
    assert.equal(urls[0], urls[1]);
});

test('flushParquetBatches splits a large batch into numbered parts', async () => {
    const dir = tmp();
    const sp = path.join(dir, 'spool.ada.parquet.ndjson');
    const lines = Array.from({ length: MAX_ROWS_PER_PARQUET + 1 }, () => JSON.stringify(sampleRow())).join('\n') + '\n';
    fs.writeFileSync(sp, lines);
    const files = claimBatches(sp, Date.now(), '55555555-0000-4000-8000-000000000000');
    const sizes = [];
    const urls = [];
    const encode = async rows => { sizes.push(rows.length); return Buffer.from('x'); };
    const ok = await new Promise(r => flushParquetBatches({ sasUrl: SAS }, files, 1000, r, {
        encode, upload: (u, _b, done) => { urls.push(u.pathname); done(true, 201); },
    }));
    assert.equal(ok, true);
    assert.deepEqual(sizes, [MAX_ROWS_PER_PARQUET, 1]);
    assert.ok(urls[0].endsWith('-0.parquet') && urls[1].endsWith('-1.parquet'));
});

test('flushParquetBatches stops at the budget and keeps the batch', async () => {
    const dir = tmp();
    const sp = path.join(dir, 'spool.ada.parquet.ndjson');
    appendSpool(sp, sampleRow());
    const files = claimBatches(sp, Date.now(), '66666666-0000-4000-8000-000000000000');
    const reasons = [];
    let hung;
    const ok = await new Promise(r => flushParquetBatches({ sasUrl: SAS }, files, 1000, r, {
        upload: () => { hung = setTimeout(() => {}, 5000); }, totalBudgetMs: 50, onError: x => reasons.push(x),
    }));
    clearTimeout(hung);
    assert.equal(ok, false);
    assert.ok(fs.existsSync(files[0]));
    assert.deepEqual(reasons, ['budget-exceeded']);
});

test('logFailure writes sink and reason only, and caps the log size', () => {
    const dir = tmp();
    const env = { CLAUDE_SF_SPOOL: path.join(dir, 'spool.ada.ndjson') };
    const log = path.join(dir, 'errors.log');
    fs.writeFileSync(log, 'x'.repeat(70 * 1024));
    logFailure(env, 'parquet', 'upload-failed status=403');
    const text = fs.readFileSync(log, 'utf8');
    assert.ok(text.length < 200);
    assert.match(text, /parquet upload-failed status=403\n$/);
});

/** Run the hook once as Claude Code would, with `payload` on stdin. */
function runHook(env, payload) {
    return new Promise(resolve => {
        const child = spawn(process.execPath, [HOOK], { env, stdio: ['pipe', 'ignore', 'pipe'] });
        let err = '';
        child.stderr.on('data', d => { err += d; });
        child.on('exit', code => resolve({ code, err }));
        child.stdin.end(JSON.stringify(payload));
    });
}

test('end to end: a session flushes one parquet file with every event and no prompt text', async () => {
    // A legacy Snowflake connector in the env must not matter: the plugin is parquet only.
    const dir = tmp();
    const srv = await fakeBlobServer();
    try {
        const env = {
            PATH: process.env.PATH, HOME: dir, USER: 'ada',
            CLAUDE_SF_SPOOL: path.join(dir, 'spool.ada.parquet.ndjson'),
            CLAUDE_TELEMETRY_SAS_URL: srv.base,
            CLAUDE_SF_SYNC: '1',
            SNOWFLAKE_ACCOUNT: 'acct', SNOWFLAKE_USER: 'svc', SNOWFLAKE_PRIVATE_KEY: 'not-a-key',
        };
        const base = { session_id: 'e2e-1', cwd: dir };
        for (const p of [
            { ...base, hook_event_name: 'SessionStart', source: 'startup' },
            { ...base, hook_event_name: 'UserPromptSubmit', prompt: 'TOP SECRET PROMPT' },
            { ...base, hook_event_name: 'SessionEnd' },
        ]) {
            const r = await runHook(env, p);
            assert.equal(r.code, 0);
            assert.equal(r.err, '');
        }
        assert.equal(srv.puts.length, 1);
        assert.ok(!srv.puts[0].body.includes('TOP SECRET'));
        const rows = await readParquet(srv.puts[0].body);
        assert.deepEqual(rows.map(r => r.HOOK_EVENT), ['SessionStart', 'UserPromptSubmit', 'SessionEnd']);
        assert.ok(rows.every(r => r.SESSION_ID === 'e2e-1' && r.CLIENT_USER === 'ada'));
        assert.deepEqual(fs.readdirSync(dir).filter(f => f.includes('.batch.')), []);
    } finally { await srv.close(); }
});

test('end to end: an unreachable endpoint keeps the batch and logs the failure', async () => {
    const dir = tmp();
    const env = {
        PATH: process.env.PATH, HOME: dir, USER: 'ada',
        CLAUDE_SF_SPOOL: path.join(dir, 'spool.ada.parquet.ndjson'),
        CLAUDE_TELEMETRY_SAS_URL: 'http://127.0.0.1:1/parquet/claude?sig=SECRETSIG',
        CLAUDE_SF_SYNC: '1',
    };
    const r = await runHook(env, { session_id: 'e2e-2', cwd: dir, hook_event_name: 'SessionEnd' });
    assert.equal(r.code, 0);
    assert.equal(fs.readdirSync(dir).filter(f => f.includes('.batch.')).length, 1);
    const log = fs.readFileSync(path.join(dir, 'errors.log'), 'utf8');
    assert.match(log, /parquet upload-failed/);
    assert.ok(!log.includes('SECRETSIG'));
});

test('end to end: an expired SAS holds events, and the first flush after renewal sends them all', async () => {
    const dir = tmp();
    const srv = await fakeBlobServer();
    try {
        const env = {
            PATH: process.env.PATH, HOME: dir, USER: 'ada',
            CLAUDE_SF_SPOOL: path.join(dir, 'spool.ada.parquet.ndjson'),
            CLAUDE_SF_SYNC: '1',
        };
        const expired = `${srv.base}&se=2020-01-01T00%3A00%3A00Z`;
        for (const id of ['old-1', 'old-2']) {
            const r = await runHook({ ...env, CLAUDE_TELEMETRY_SAS_URL: expired }, { session_id: id, cwd: dir, hook_event_name: 'SessionEnd' });
            assert.equal(r.code, 0);
        }
        assert.equal(srv.puts.length, 0);
        assert.deepEqual(fs.readdirSync(dir).filter(f => f.includes('.batch.')), [], 'nothing claimed while expired');
        assert.match(fs.readFileSync(path.join(dir, 'errors.log'), 'utf8'), /parquet sas-expired/);

        await runHook({ ...env, CLAUDE_TELEMETRY_SAS_URL: srv.base }, { session_id: 'new-1', cwd: dir, hook_event_name: 'SessionEnd' });
        assert.equal(srv.puts.length, 1);
        assert.deepEqual((await readParquet(srv.puts[0].body)).map(r => r.SESSION_ID), ['old-1', 'old-2', 'new-1']);
    } finally { await srv.close(); }
});

test('end to end: an expired SAS stops adding to a spool that has reached the hold cap', async () => {
    const dir = tmp();
    const sp = path.join(dir, 'spool.ada.parquet.ndjson');
    fs.writeFileSync(sp, '');
    fs.truncateSync(sp, SPOOL_HOLD_MAX_BYTES);
    const r = await runHook({
        PATH: process.env.PATH, HOME: dir, USER: 'ada', CLAUDE_SF_SPOOL: sp, CLAUDE_SF_SYNC: '1',
        CLAUDE_TELEMETRY_SAS_URL: 'https://a.example/claude-telemetry?se=2020-01-01T00%3A00%3A00Z&sig=x',
    }, { session_id: 'x', cwd: dir, hook_event_name: 'Stop' });
    assert.equal(r.code, 0);
    assert.equal(fs.statSync(sp).size, SPOOL_HOLD_MAX_BYTES);
});

/** Hook env that spools without uploading: in-process flush to an unreachable loopback URL. */
function spoolOnlyEnv(dir, extra = {}) {
    return {
        PATH: process.env.PATH, HOME: dir, USER: 'ada',
        CLAUDE_SF_SPOOL: path.join(dir, 'spool.ada.parquet.ndjson'),
        CLAUDE_TELEMETRY_SAS_URL: 'http://127.0.0.1:1/claude-telemetry?sig=x',
        ...extra,
    };
}

/** Rows currently in the live spool, as { hook, source, input } objects. */
function spooled(dir) {
    const f = path.join(dir, 'spool.ada.parquet.ndjson');
    if (!fs.existsSync(f)) return [];
    return fs.readFileSync(f, 'utf8').trim().split('\n').filter(Boolean).map(l => {
        const r = JSON.parse(l);
        return { hook: r[COLUMNS.indexOf('HOOK_EVENT')], source: r[COLUMNS.indexOf('SOURCE')], input: r[COLUMNS.indexOf('INPUT_TOKENS')] };
    });
}

test('end to end: a first prompt with no SessionStart records an inferred start and counts the first turn', async () => {
    const dir = tmp();
    const transcript = path.join(dir, 't.jsonl');
    const sid = `inferred-${Date.now()}`;
    fs.writeFileSync(transcript, JSON.stringify({ type: 'user', message: { content: 'hi' } }) + '\n');
    const env = spoolOnlyEnv(dir);
    const base = { session_id: sid, cwd: dir, transcript_path: transcript };
    await runHook(env, { ...base, hook_event_name: 'UserPromptSubmit' });
    fs.appendFileSync(transcript, JSON.stringify({ timestamp: '2026-11-16T00:00:01Z', message: { id: 'm1', model: 'claude-opus-5-5', usage: { input_tokens: 42, output_tokens: 7 } } }) + '\n');
    await runHook(env, { ...base, hook_event_name: 'Stop' });
    await runHook(env, { ...base, hook_event_name: 'UserPromptSubmit' });
    assert.deepEqual(spooled(dir), [
        { hook: 'SessionStart', source: 'inferred', input: null },
        { hook: 'UserPromptSubmit', source: null, input: null },
        { hook: 'Stop', source: null, input: 42 },
        { hook: 'UserPromptSubmit', source: null, input: null },
    ]);
});

test('end to end: a session that did fire SessionStart gets no inferred start', async () => {
    const dir = tmp();
    const transcript = path.join(dir, 't.jsonl');
    fs.writeFileSync(transcript, '');
    const env = spoolOnlyEnv(dir);
    const base = { session_id: `real-${Date.now()}`, cwd: dir, transcript_path: transcript };
    await runHook(env, { ...base, hook_event_name: 'SessionStart', source: 'startup' });
    await runHook(env, { ...base, hook_event_name: 'UserPromptSubmit' });
    assert.deepEqual(spooled(dir).map(r => `${r.hook}:${r.source}`), ['SessionStart:startup', 'UserPromptSubmit:null']);
});

test('end to end: stays silent in a checkout whose legacy hook would write', async () => {
    const dir = tmp();
    const repo = path.join(dir, 'repo');
    fs.mkdirSync(path.join(repo, '.claude', 'hooks'), { recursive: true });
    fs.writeFileSync(path.join(repo, '.claude', 'hooks', 'snowflake-telemetry.cjs'), '');
    fs.mkdirSync(path.join(repo, 'sub'));
    const cfg = path.join(dir, 'sf.json');
    fs.writeFileSync(cfg, '{}');
    const payload = { session_id: 'legacy', cwd: path.join(repo, 'sub'), hook_event_name: 'UserPromptSubmit' };
    await runHook(spoolOnlyEnv(dir, { CLAUDE_SNOWFLAKE_CONFIG: cfg }), payload);
    assert.deepEqual(spooled(dir), [], 'legacy hook configured: plugin silent');
    await runHook(spoolOnlyEnv(dir, { CLAUDE_SNOWFLAKE_CONFIG: path.join(dir, 'none.json') }), payload);
    assert.ok(spooled(dir).length > 0, 'legacy hook present but unconfigured: plugin reports');
});

test('legacyHookActive honours CLAUDE_PROJECT_DIR and needs a Snowflake connector', () => {
    const dir = tmp();
    fs.mkdirSync(path.join(dir, '.claude', 'hooks'), { recursive: true });
    fs.writeFileSync(path.join(dir, '.claude', 'hooks', 'snowflake-telemetry.cjs'), '');
    const none = { CLAUDE_SNOWFLAKE_CONFIG: path.join(dir, 'none.json') };
    assert.equal(legacyHookActive({ ...none, CLAUDE_PROJECT_DIR: dir }, '/'), false);
    assert.equal(legacyHookActive({ ...none, CLAUDE_PROJECT_DIR: dir, SNOWFLAKE_ACCOUNT: 'a', SNOWFLAKE_USER: 'u' }, '/'), true);
    assert.equal(legacyHookActive({ ...none, SNOWFLAKE_ACCOUNT: 'a', SNOWFLAKE_USER: 'u' }, tmp()), false);
});

test('end to end: no sink configured is a silent no-op', async () => {
    const dir = tmp();
    const r = await runHook({
        PATH: process.env.PATH, HOME: dir, USER: 'ada',
        CLAUDE_SF_SPOOL: path.join(dir, 'spool.ada.parquet.ndjson'),
        CLAUDE_TELEMETRY_CONFIG: path.join(dir, 'none.json'),
    }, { session_id: 'e2e-3', hook_event_name: 'SessionEnd' });
    assert.equal(r.code, 0);
    assert.deepEqual(fs.readdirSync(dir), []);
});

let finished = false;
process.on('exit', () => {
    if (!finished) { console.error('\nFAIL - the run exited before every test settled'); process.exitCode = 1; }
});

(async () => {
    for (const { name, fn } of tests) {
        try {
            await fn();
            pass += 1;
            console.log(`  ok   - ${name}`);
        } catch (e) {
            console.error(`  FAIL - ${name}\n         ${e.stack}`);
            process.exitCode = 1;
        }
    }
    finished = true;
    console.log(`\nPassed: ${pass}/${tests.length}`);
    if (pass !== tests.length) process.exitCode = 1;
})();

'use strict';

/**
 * Unit tests for telemetry.cjs event building, token counting, and the spool / batch-claim
 * machinery — no network, no keys. Parquet encoding and upload are in parquet.test.cjs.
 * Run: node plugins/claude-telemetry/test/telemetry.test.cjs
 */

const assert = require('node:assert');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const {
    buildEvent, toRow, COLUMNS, isNonInteractive,
    spoolPath, appendSpool, parseSpool, shouldFlush, chunk, claimBatches,
    SPOOL_MAX_BYTES, ORPHAN_RETRY_AFTER_MS, ORPHAN_DROP_AFTER_MS,
    repoFromRemote, resolveOrigin, turnTokens, initTokenOffset, commitTokenOffset,
    originCacheFile, cleanupSessionState, usageInt, MIN_ROW_WIDTH,
} = require('../src/telemetry.cjs');


const CTX = { host: 'dev-host', user: 'ada', environment: 'local' };
const build = payload => buildEvent(payload, CTX);

let pass = 0;
const pending = [];
function test(name, fn) {
    try {
        const r = fn();
        if (r && typeof r.then === 'function') {
            // Async test: record its outcome when it settles.
            pending.push(r.then(
                () => { pass += 1; console.log(`  ok   - ${name}`); },
                e => { console.error(`  FAIL - ${name}\n         ${e.message}`); process.exitCode = 1; },
            ));
        } else {
            pass += 1;
            console.log(`  ok   - ${name}`);
        }
    } catch (e) {
        console.error(`  FAIL - ${name}\n         ${e.message}`);
        process.exitCode = 1;
    }
}

test('SessionStart -> session/start with source', () => {
    const e = build({ hook_event_name: 'SessionStart', session_id: 's1', cwd: '/repo', source: 'startup' });
    assert.equal(e.category, 'session');
    assert.equal(e.action, 'start');
    assert.equal(e.source, 'startup');
    assert.equal(e.sessionId, 's1');
});

test('SessionEnd -> session/end', () => {
    const e = build({ hook_event_name: 'SessionEnd', session_id: 's1' });
    assert.equal(e.category, 'session');
    assert.equal(e.action, 'end');
});

test('SubagentStart -> subagent/start with agentType', () => {
    const e = build({ hook_event_name: 'SubagentStart', agent_type: 'code-review', agent_id: 'a9' });
    assert.equal(e.category, 'subagent');
    assert.equal(e.action, 'start');
    assert.equal(e.agentType, 'code-review');
    assert.equal(e.agentId, 'a9');
});

test('SubagentStop -> subagent/end (Stop maps to end)', () => {
    const e = build({ hook_event_name: 'SubagentStop', subagent_type: 'Explore' });
    assert.equal(e.category, 'subagent');
    assert.equal(e.action, 'end');
    assert.equal(e.agentType, 'Explore'); // falls back to subagent_type
});

test('PreToolUse Skill -> skill/use with skill name', () => {
    const e = build({ hook_event_name: 'PreToolUse', tool_name: 'Skill', tool_input: { skill: 'build-and-test' } });
    assert.equal(e.category, 'skill');
    assert.equal(e.action, 'use');
    assert.equal(e.skill, 'build-and-test');
    assert.equal(e.toolName, 'Skill');
});

test('absent attributes are dropped from the event', () => {
    const e = build({ hook_event_name: 'SessionStart' });
    assert.ok(!('skill' in e));
    assert.ok(!('agentType' in e));
    assert.ok(!('source' in e));
});

test('unknown/empty payload still produces a valid event', () => {
    const e = build({});
    assert.equal(e.hookEvent, 'Unknown');
    assert.equal(e.host, 'dev-host');
    assert.equal(e.user, 'ada');
});

test('toRow places values in COLUMNS order and nulls the rest', () => {
    const row = toRow(build({ hook_event_name: 'PreToolUse', tool_name: 'Skill', session_id: 's1', tool_input: { skill: 'x' } }));
    assert.equal(row.length, COLUMNS.length);
    assert.equal(row[COLUMNS.indexOf('SESSION_ID')], 's1');
    assert.equal(row[COLUMNS.indexOf('SKILL')], 'x');
    assert.equal(row[COLUMNS.indexOf('CLIENT_USER')], 'ada');
    assert.equal(row[COLUMNS.indexOf('AGENT_ID')], null); // absent -> null
});

// --- Non-interactive / service-identity path (WV-3200) ---

test('isNonInteractive: explicit override wins, CI markers auto-enable', () => {
    assert.equal(isNonInteractive({ CLAUDE_SF_SYNC: '1' }), true);
    assert.equal(isNonInteractive({ CLAUDE_SF_SYNC: '0', CI: 'true' }), false); // explicit off beats CI
    assert.equal(isNonInteractive({ CI: 'true' }), true);
    assert.equal(isNonInteractive({ GITHUB_ACTIONS: 'true' }), true);
    assert.equal(isNonInteractive({}), false); // interactive by default
});

test('UserPromptSubmit -> prompt/submit, and NEVER captures prompt text', () => {
    const e = build({ hook_event_name: 'UserPromptSubmit', session_id: 's1', user_prompt: 'SECRET PROMPT', prompt: 'SECRET' });
    assert.equal(e.category, 'prompt');
    assert.equal(e.action, 'submit');
    assert.ok(!JSON.stringify(e).includes('SECRET')); // no prompt text, ever
});

test('Stop -> turn/stop, and NEVER captures the assistant message', () => {
    const e = build({ hook_event_name: 'Stop', session_id: 's1', last_assistant_message: 'SECRET RESPONSE' });
    assert.equal(e.category, 'turn');
    assert.equal(e.action, 'stop');
    assert.ok(!JSON.stringify(e).includes('SECRET')); // no response text, ever
});

test('the schema has no column that could hold prompt/response content', () => {
    assert.ok(!COLUMNS.some(c => /PROMPT|MESSAGE|TEXT|BODY|CONTENT/.test(c)));
});

// --- Batching / spool (WV: reduce Snowflake compute) ---

test('parseSpool round-trips rows and skips malformed / wrong-width lines', () => {
    const row = toRow(build({ hook_event_name: 'Stop', session_id: 's1' }));
    const text = [JSON.stringify(row), 'not json', JSON.stringify([1, 2, 3]), '', JSON.stringify(row)].join('\n');
    const rows = parseSpool(text);
    assert.equal(rows.length, 2); // two valid, the short array + junk dropped
    assert.equal(rows[0][COLUMNS.indexOf('SESSION_ID')], 's1');
});

test('shouldFlush fires on SessionEnd or at the size cap, not otherwise', () => {
    assert.equal(shouldFlush(0, 'SessionEnd'), true);
    assert.equal(shouldFlush(SPOOL_MAX_BYTES, 'Stop'), true);
    assert.equal(shouldFlush(SPOOL_MAX_BYTES - 1, 'Stop'), false);
    assert.equal(shouldFlush(10, 'UserPromptSubmit'), false);
});

test('chunk splits into bounded groups, empty in / empty out', () => {
    assert.deepEqual(chunk([1, 2, 3, 4, 5], 2), [[1, 2], [3, 4], [5]]);
    assert.deepEqual(chunk([], 500), []);
});

test('spoolPath honours the override and is per-user under tmp otherwise', () => {
    assert.equal(spoolPath({ CLAUDE_SF_SPOOL: '/x/y.ndjson' }), '/x/y.ndjson');
    const p = spoolPath({ USER: 'ada/../root' });
    assert.ok(p.endsWith('.ndjson'));
    assert.ok(!p.includes('/../')); // unsafe chars sanitised out of the filename
});

test('appendSpool + parseSpool: events accumulate as NDJSON', () => {
    const sp = path.join(os.tmpdir(), `sf-spool-${process.pid}-${Date.now()}.ndjson`);
    try {
        appendSpool(sp, toRow(build({ hook_event_name: 'SessionStart', session_id: 's1' })));
        appendSpool(sp, toRow(build({ hook_event_name: 'Stop', session_id: 's1' })));
        const rows = parseSpool(fs.readFileSync(sp, 'utf8'));
        assert.equal(rows.length, 2);
    } finally {
        try { fs.unlinkSync(sp); } catch { /* ignore */ }
    }
});

test('claimBatches renames the live spool and adopts/drops orphans by age', () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'sf-claim-'));
    const sp = path.join(dir, 'spool.ndjson');
    const now = 1_000_000_000_000;
    try {
        // A live spool, an in-flight batch (too young to steal), an adoptable
        // orphan, and a batch old enough to abandon.
        fs.writeFileSync(sp, 'live\n');
        const young = `${sp}.batch.young`;
        const adoptable = `${sp}.batch.adoptable`;
        const ancient = `${sp}.batch.ancient`;
        fs.writeFileSync(young, 'y\n');
        fs.writeFileSync(adoptable, 'a\n');
        fs.writeFileSync(ancient, 'z\n');
        const sec = 1000;
        fs.utimesSync(young, new Date(now), new Date(now - (ORPHAN_RETRY_AFTER_MS - 5 * sec)));
        fs.utimesSync(adoptable, new Date(now), new Date(now - (ORPHAN_RETRY_AFTER_MS + 60 * sec)));
        fs.utimesSync(ancient, new Date(now), new Date(now - (ORPHAN_DROP_AFTER_MS + 60 * sec)));

        const files = claimBatches(sp, now, 'uuid');
        assert.ok(!fs.existsSync(sp)); // live spool claimed away
        assert.ok(files.includes(`${sp}.batch.${now}.uuid`)); // our claim
        assert.ok(!fs.existsSync(adoptable)); // orphan claimed (renamed) away — a peer would ENOENT
        assert.ok(files.some(f => f.startsWith(adoptable) && f.endsWith('.ruuid'))); // adopted under a new owned name
        assert.ok(!files.includes(young)); // in-flight left alone
        assert.ok(fs.existsSync(young));
        assert.ok(!files.includes(ancient)); // too old
        assert.ok(!fs.existsSync(ancient)); // and deleted
    } finally {
        fs.rmSync(dir, { recursive: true, force: true });
    }
});

test('claimBatches with nothing live still adopts a standalone orphan', () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'sf-claim2-'));
    const sp = path.join(dir, 'spool.ndjson');
    const now = 1_000_000_000_000;
    try {
        const orphan = `${sp}.batch.orphan`;
        fs.writeFileSync(orphan, 'o\n');
        fs.utimesSync(orphan, new Date(now), new Date(now - (ORPHAN_RETRY_AFTER_MS + 1000)));
        const files = claimBatches(sp, now, 'uuid'); // no live spool exists
        assert.equal(files.length, 1);
        assert.ok(!fs.existsSync(orphan)); // atomically claimed (renamed) on adoption
        assert.ok(files[0].startsWith(orphan) && files[0].endsWith('.ruuid'));
    } finally {
        fs.rmSync(dir, { recursive: true, force: true });
    }
});

test('claimBatches adopts an orphan atomically (renames it) and does not stack retry suffixes', () => {
    const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'sf-adopt-'));
    const sp = path.join(dir, 'spool.ndjson');
    const now = 1_000_000_000_000;
    const uuidA = '11111111-1111-1111-1111-111111111111';
    const uuidB = '22222222-2222-2222-2222-222222222222';
    try {
        const orphan = `${sp}.batch.orphan`;
        fs.writeFileSync(orphan, 'x\n');
        fs.utimesSync(orphan, new Date(now), new Date(now - (ORPHAN_RETRY_AFTER_MS + 1000)));

        const first = claimBatches(sp, now, uuidA);
        assert.ok(!fs.existsSync(orphan)); // original claimed away — a racing peer would ENOENT
        assert.equal(first.length, 1);
        assert.ok(first[0].endsWith(`.r${uuidA}`));

        // A second adoption normalises the suffix rather than stacking `.r….r…`.
        const second = claimBatches(sp, now, uuidB);
        assert.equal(second.length, 1);
        assert.ok(second[0].endsWith(`.r${uuidB}`));
        assert.ok(!second[0].includes(uuidA));
    } finally {
        fs.rmSync(dir, { recursive: true, force: true });
    }
});

// A batch file on disk holding the given rows as NDJSON.
function writeBatch(dir, name, rows) {
    const p = path.join(dir, name);
    fs.writeFileSync(p, rows.map(r => JSON.stringify(r)).join('\n') + '\n');
    return p;
}
const TARGET_CFG = { target: { database: 'WEAVIX_AI', schema: 'TELEMETRY', table: 'CLAUDE_CODE_EVENTS' } };

// --- Origin: repository / workflow / branch (data-repo V1.2 / V1.3) ---

test('repoFromRemote parses real remotes and rejects local paths', () => {
    assert.equal(repoFromRemote('https://github.com/weavix/weavix.git'), 'weavix/weavix');
    assert.equal(repoFromRemote('https://github.com/weavix/weavix'), 'weavix/weavix');
    assert.equal(repoFromRemote('git@github.com:weavix/weavix.git'), 'weavix/weavix'); // scp-style
    assert.equal(repoFromRemote('ssh://git@github.com/weavix/weavix.git'), 'weavix/weavix');
    assert.equal(repoFromRemote('/Users/ada/dev/weavix'), undefined); // a local path is not a remote
    assert.equal(repoFromRemote(''), undefined);
    assert.equal(repoFromRemote(undefined), undefined);
});

test('resolveOrigin: CI env vars win, HEAD_REF beats REF_NAME on PR runs', () => {
    const o = resolveOrigin({
        GITHUB_REPOSITORY: 'weavix/weavix', GITHUB_WORKFLOW: 'deploy-backend',
        GITHUB_HEAD_REF: 'wv-1-feature', GITHUB_REF_NAME: 'main',
    }, '/nonexistent'); // cwd unused — env short-circuits every git call
    assert.equal(o.repository, 'weavix/weavix');
    assert.equal(o.workflow, 'deploy-backend');
    assert.equal(o.branch, 'wv-1-feature');
});

test('resolveOrigin: REF_NAME on push runs, and no workflow locally', () => {
    const o = resolveOrigin({ GITHUB_REPOSITORY: 'weavix/weavix', GITHUB_REF_NAME: 'main' }, '/nonexistent');
    assert.equal(o.branch, 'main');
    assert.equal(o.workflow, undefined); // WORKFLOW is the CI agent identity; unset off-CI
});

test('resolveOrigin: local git checkout supplies repository when CI env is absent', () => {
    // Running inside this git repo, repository resolves to owner/name. Be tolerant of a
    // checkout without an origin remote (undefined) rather than flaky.
    const o = resolveOrigin({}, path.join(__dirname, '..', '..'));
    if (o.repository !== undefined) assert.match(o.repository, /^[^/\s]+\/[^/\s]+$/);
});

// --- Per-turn token usage + model, from the transcript (data-repo V1.4 / V1.5) ---

function tmpTranscript() {
    return path.join(os.tmpdir(), `sf-transcript-${process.pid}-${Date.now()}-${Math.random().toString(36).slice(2)}.jsonl`);
}
// One assistant JSONL line carrying usage/model (and, deliberately, message text we must not read).
function asstLine(usage, model, text) {
    const message = { role: 'assistant' };
    if (model !== undefined) message.model = model;
    if (usage) message.usage = usage;
    if (text) message.content = [{ type: 'text', text }];
    return JSON.stringify({ type: 'assistant', message }) + '\n';
}
function rmTokenState(sid) {
    try { fs.unlinkSync(path.join(os.tmpdir(), `cc-telemetry-tokens-${sid}.json`)); } catch { /* ignore */ }
}

test('turnTokens sums a turn, keeps the last real model, advances the offset', () => {
    const tp = tmpTranscript();
    const sid = `tok-${process.pid}-${Date.now()}`;
    try {
        fs.writeFileSync(tp, '');
        initTokenOffset(tp, sid); // baseline at 0 (fresh session)
        fs.appendFileSync(tp, asstLine({ input_tokens: 10, output_tokens: 5, cache_creation_input_tokens: 2, cache_read_input_tokens: 3 }, 'claude-opus-4-8', 'SECRET RESPONSE'));
        fs.appendFileSync(tp, asstLine({ input_tokens: 1, output_tokens: 2 }, 'claude-sonnet-5'));
        const t = turnTokens(tp, sid);
        assert.equal(t.input, 11);
        assert.equal(t.output, 7);
        assert.equal(t.cacheCreation, 2);
        assert.equal(t.cacheRead, 3);
        assert.equal(t.model, 'claude-sonnet-5'); // last real model wins

        commitTokenOffset(sid, t.offset);
        assert.equal(turnTokens(tp, sid), null); // nothing new since the commit

        fs.appendFileSync(tp, asstLine({ input_tokens: 100, output_tokens: 0 }, 'claude-opus-4-8'));
        const t2 = turnTokens(tp, sid);
        assert.equal(t2.input, 100); // only the new line, not the earlier turn
        assert.equal(t2.output, 0);
    } finally {
        try { fs.unlinkSync(tp); } catch { /* ignore */ }
        rmTokenState(sid);
    }
});

test('turnTokens without a baseline returns null and does not count pre-existing history', () => {
    const tp = tmpTranscript();
    const sid = `tok2-${process.pid}-${Date.now()}`;
    try {
        fs.writeFileSync(tp, asstLine({ input_tokens: 999, output_tokens: 999 }, 'claude-opus-4-8'));
        assert.equal(turnTokens(tp, sid), null); // no baseline -> baseline to end, count nothing
        fs.appendFileSync(tp, asstLine({ input_tokens: 4, output_tokens: 1 }, 'claude-opus-4-8'));
        const t = turnTokens(tp, sid);
        assert.equal(t.input, 4); // only the turn after the baseline
    } finally {
        try { fs.unlinkSync(tp); } catch { /* ignore */ }
        rmTokenState(sid);
    }
});

test('turnTokens skips <synthetic> messages, so a synthetic-only turn has null model', () => {
    const tp = tmpTranscript();
    const sid = `tok3-${process.pid}-${Date.now()}`;
    try {
        fs.writeFileSync(tp, '');
        initTokenOffset(tp, sid);
        fs.appendFileSync(tp, asstLine({ input_tokens: 3, output_tokens: 0 }, '<synthetic>'));
        const t = turnTokens(tp, sid);
        assert.equal(t.input, 3); // usage still summed
        assert.equal(t.model, null); // but the synthetic model id is not recorded
    } finally {
        try { fs.unlinkSync(tp); } catch { /* ignore */ }
        rmTokenState(sid);
    }
});

test('buildEvent on SessionStart baselines the offset; first Stop counts only new turns', () => {
    const tp = tmpTranscript();
    const sid = `ss-${process.pid}-${Date.now()}`;
    try {
        fs.writeFileSync(tp, asstLine({ input_tokens: 999, output_tokens: 999 }, 'claude-opus-4-8')); // pre-existing
        buildEvent({ hook_event_name: 'SessionStart', session_id: sid, transcript_path: tp }, CTX);
        const e = buildEvent({ hook_event_name: 'Stop', session_id: sid, transcript_path: tp }, CTX);
        assert.ok(!('inputTokens' in e)); // nothing new since the baseline
    } finally {
        try { fs.unlinkSync(tp); } catch { /* ignore */ }
        rmTokenState(sid);
    }
});

test('buildEvent on Stop captures tokens + model + a commit marker, never message text', () => {
    const tp = tmpTranscript();
    const sid = `evt-${process.pid}-${Date.now()}`;
    try {
        fs.writeFileSync(tp, '');
        initTokenOffset(tp, sid);
        fs.appendFileSync(tp, asstLine({ input_tokens: 12, output_tokens: 6 }, 'claude-opus-4-8', 'SECRET RESPONSE'));
        const e = buildEvent({ hook_event_name: 'Stop', session_id: sid, transcript_path: tp }, CTX);
        assert.equal(e.inputTokens, 12);
        assert.equal(e.outputTokens, 6);
        assert.equal(e.model, 'claude-opus-4-8');
        assert.equal(e._tokenCommit.sessionId, sid);
        assert.ok(!JSON.stringify(e).includes('SECRET')); // numbers only, never the message

        // _tokenCommit is bookkeeping, not a column -> toRow ignores it, row width unchanged.
        const row = toRow(e);
        assert.equal(row.length, COLUMNS.length);
        assert.equal(row[COLUMNS.indexOf('INPUT_TOKENS')], 12);
        assert.equal(row[COLUMNS.indexOf('MODEL')], 'claude-opus-4-8');
    } finally {
        try { fs.unlinkSync(tp); } catch { /* ignore */ }
        rmTokenState(sid);
    }
});

test('buildEvent carries repository/workflow/branch/entrypoint/attended from ctx (false kept)', () => {
    const e = buildEvent({ hook_event_name: 'SessionStart', session_id: 's1' }, {
        ...CTX, repository: 'weavix/weavix', workflow: 'ci', branch: 'main', entrypoint: 'cli', attended: false,
    });
    assert.equal(e.repository, 'weavix/weavix');
    assert.equal(e.workflow, 'ci');
    assert.equal(e.branch, 'main');
    assert.equal(e.entrypoint, 'cli');
    assert.equal(e.attended, false); // false is a meaningful value, not dropped
});

// --- Copilot review hardening (PR 5717) ---

test('parseSpool pads an older, narrower row with nulls and skips an over-wide row', () => {
    const full = toRow(build({ hook_event_name: 'Stop', session_id: 's1' })); // current width
    const oldRow = new Array(MIN_ROW_WIDTH).fill('x'); // a pre-upgrade base-width row
    oldRow[COLUMNS.indexOf('SESSION_ID')] = 'old'; // SESSION_ID is within the base columns
    const tooWide = new Array(COLUMNS.length + 1).fill('y'); // a newer/unknown schema
    const text = [JSON.stringify(full), JSON.stringify(oldRow), JSON.stringify(tooWide)].join('\n');
    const rows = parseSpool(text);
    assert.equal(rows.length, 2); // full + padded old row; the over-wide row skipped
    const padded = rows.find(r => r[COLUMNS.indexOf('SESSION_ID')] === 'old');
    assert.equal(padded.length, COLUMNS.length); // padded up to full width
    assert.equal(padded[COLUMNS.indexOf('MODEL')], null); // a newly-appended column is null
});

test('resolveOrigin caches repository per session and reuses it without a git call', () => {
    const sid = `origin-${process.pid}-${Date.now()}`;
    try {
        // Pre-seed the cache; cwd points nowhere so a git fallback would yield undefined.
        fs.writeFileSync(originCacheFile(sid), JSON.stringify({ repository: 'weavix/cached' }));
        const o = resolveOrigin({}, '/nonexistent', sid);
        assert.equal(o.repository, 'weavix/cached'); // served from the cache, not git
    } finally {
        try { fs.unlinkSync(originCacheFile(sid)); } catch { /* ignore */ }
    }
});

test('usageInt coerces strings and rejects junk, so sums stay numeric', () => {
    assert.equal(usageInt(10), 10);
    assert.equal(usageInt('10'), 10); // numeric string -> number, not concatenation
    assert.equal(usageInt(2.9), 2); // truncated to an integer
    assert.equal(usageInt(-5), 0); // negatives rejected
    assert.equal(usageInt('nope'), 0);
    assert.equal(usageInt(undefined), 0);
});

test('turnTokens keeps sums numeric even when the transcript uses string token values', () => {
    const tp = tmpTranscript();
    const sid = `numstr-${process.pid}-${Date.now()}`;
    try {
        fs.writeFileSync(tp, '');
        initTokenOffset(tp, sid);
        // A malformed transcript with string counts must not corrupt the sum or the FIXED bind.
        fs.appendFileSync(tp, asstLine({ input_tokens: '10', output_tokens: '5' }, 'claude-opus-4-8'));
        fs.appendFileSync(tp, asstLine({ input_tokens: 3, output_tokens: 2 }, 'claude-opus-4-8'));
        const t = turnTokens(tp, sid);
        assert.strictEqual(t.input, 13); // 10 + 3 as numbers, not "103" or "0103"
        assert.strictEqual(t.output, 7);
    } finally {
        try { fs.unlinkSync(tp); } catch { /* ignore */ }
        rmTokenState(sid);
    }
});

test('cleanupSessionState removes a session\'s files and sweeps stale ones', () => {
    const sid = `clean-${process.pid}-${Date.now()}`;
    const tokenF = path.join(os.tmpdir(), `cc-telemetry-tokens-${sid}.json`);
    const originF = originCacheFile(sid);
    const staleF = path.join(os.tmpdir(), `cc-telemetry-tokens-stale-${process.pid}-${Date.now()}.json`);
    try {
        fs.writeFileSync(tokenF, '{}');
        fs.writeFileSync(originF, '{}');
        fs.writeFileSync(staleF, '{}');
        const old = (Date.now() - 30 * 24 * 60 * 60 * 1000) / 1000; // 30 days old
        fs.utimesSync(staleF, old, old);
        cleanupSessionState(sid);
        assert.ok(!fs.existsSync(tokenF)); // this session's files removed
        assert.ok(!fs.existsSync(originF));
        assert.ok(!fs.existsSync(staleF)); // and a long-abandoned file swept
    } finally {
        for (const f of [tokenF, originF, staleF]) { try { fs.unlinkSync(f); } catch { /* ignore */ } }
    }
});

test('buildEvent on SessionEnd clears the session state files', () => {
    const sid = `end-${process.pid}-${Date.now()}`;
    const tokenF = path.join(os.tmpdir(), `cc-telemetry-tokens-${sid}.json`);
    try {
        fs.writeFileSync(tokenF, JSON.stringify({ offset: 5 }));
        fs.writeFileSync(originCacheFile(sid), JSON.stringify({ repository: 'weavix/x' }));
        buildEvent({ hook_event_name: 'SessionEnd', session_id: sid }, CTX);
        assert.ok(!fs.existsSync(tokenF));
        assert.ok(!fs.existsSync(originCacheFile(sid)));
    } finally {
        try { fs.unlinkSync(tokenF); } catch { /* ignore */ }
        try { fs.unlinkSync(originCacheFile(sid)); } catch { /* ignore */ }
    }
});

// --- WV-5022: one API message is several transcript lines; subagents have their own file ---

// One line of an API message: Claude Code writes one per content block, each with the usage.
function msgLine(id, i, o, cc, cr, extra = {}) {
    return JSON.stringify({ type: 'assistant', requestId: `req_${id}`, ...(extra.timestamp ? { timestamp: extra.timestamp } : {}),
        message: { id: `msg_${id}`, model: 'claude-opus-5-5', ...(extra.stop ? { stop_reason: extra.stop } : {}), usage: {
            input_tokens: i, output_tokens: o, cache_creation_input_tokens: cc, cache_read_input_tokens: cr } } }) + '\n';
}
const sums = t => [t.input, t.output, t.cacheCreation, t.cacheRead];
// A fresh transcript baselined at 0, cleaned up after the body.
function withTranscript(body) {
    const tp = tmpTranscript();
    const sid = `dedup-${process.pid}-${Date.now()}-${Math.random().toString(36).slice(2)}`;
    try {
        fs.writeFileSync(tp, '');
        initTokenOffset(tp, sid);
        body(tp, sid);
    } finally {
        try { fs.unlinkSync(tp); } catch { /* ignore */ }
        rmTokenState(sid);
    }
}

test('turnTokens counts a message split across lines once, not once per line', () => {
    withTranscript((tp, sid) => {
        // msg_a as three lines (thinking, text, tool_use) with identical usage, then msg_b.
        fs.appendFileSync(tp, msgLine('a', 10, 50, 200, 900).repeat(3) + msgLine('b', 2, 7, 0, 1000));
        assert.deepEqual(sums(turnTokens(tp, sid)), [12, 57, 200, 1900]);
    });
});

test('turnTokens takes the largest usage when a message is written as growing snapshots', () => {
    withTranscript((tp, sid) => {
        // A subagent transcript: output_tokens grows line by line; the final line is the total.
        fs.appendFileSync(tp, msgLine('a', 10, 6, 24914, 0).repeat(2) + msgLine('a', 10, 521, 24914, 0, { stop: 'tool_use' }));
        assert.deepEqual(sums(turnTokens(tp, sid)), [10, 521, 24914, 0]);
    });
});

test('a message straddling two Stops is counted once, its growth on the second', () => {
    withTranscript((tp, sid) => {
        fs.appendFileSync(tp, msgLine('a', 10, 6, 100, 0));
        const t1 = turnTokens(tp, sid);
        assert.deepEqual(sums(t1), [10, 6, 100, 0]);
        commitTokenOffset(sid, t1.offset, t1.carry);
        fs.appendFileSync(tp, msgLine('a', 10, 6, 100, 0) + msgLine('a', 10, 40, 100, 0) + msgLine('b', 1, 2, 0, 3));
        const t2 = turnTokens(tp, sid);
        assert.deepEqual(sums(t2), [1, 36, 0, 3]);
        commitTokenOffset(sid, t2.offset, t2.carry);
        fs.appendFileSync(tp, msgLine('b', 1, 2, 0, 3)); // a later read that only repeats msg_b
        assert.deepEqual(sums(turnTokens(tp, sid)), [0, 0, 0, 0]);
    });
});

test('a rewritten copy of an earlier message, with its original timestamp, is not recounted', () => {
    withTranscript((tp, sid) => {
        fs.appendFileSync(tp, msgLine('a', 1, 10, 0, 0, { timestamp: '2026-08-05T18:57:52.818Z' })
            + msgLine('b', 1, 20, 0, 0, { timestamp: '2026-08-07T15:00:00.000Z' }));
        const t1 = turnTokens(tp, sid);
        assert.deepEqual(sums(t1), [2, 30, 0, 0]);
        commitTokenOffset(sid, t1.offset, t1.carry);
        // After compaction msg_a is written again under its 08-05 timestamp, then a new msg_c.
        fs.appendFileSync(tp, msgLine('a', 1, 10, 0, 0, { timestamp: '2026-08-05T18:57:52.818Z' })
            + msgLine('c', 1, 5, 0, 0, { timestamp: '2026-08-07T15:01:00.000Z' }));
        assert.deepEqual(sums(turnTokens(tp, sid)), [1, 5, 0, 0]);
    });
});

test('SubagentStop counts the subagent transcript from its start, under its own offset', () => {
    withTranscript((tp, sid) => {
        const agentTp = tp.replace(/\.jsonl$/, '-agent-x1.jsonl');
        try {
            fs.writeFileSync(agentTp, msgLine('s1', 3, 9, 50, 0).repeat(2) + msgLine('s2', 1, 4, 0, 50));
            const payload = { hook_event_name: 'SubagentStop', session_id: sid, transcript_path: tp,
                agent_id: 'x1', agent_type: 'Explore', agent_transcript_path: agentTp };
            const e = build(payload);
            assert.deepEqual([e.inputTokens, e.outputTokens, e.cacheCreation, e.cacheRead], [4, 13, 50, 50]);
            assert.equal(e.model, 'claude-opus-5-5');
            assert.equal(e._tokenCommit.sessionId, `${sid}-agent-x1`);
            commitTokenOffset(e._tokenCommit.sessionId, e._tokenCommit.offset, e._tokenCommit.carry);
            // Resumed and stopped again: only the new call counts.
            fs.appendFileSync(agentTp, msgLine('s3', 2, 2, 0, 0));
            const again = build(payload);
            assert.deepEqual([again.inputTokens, again.outputTokens], [2, 2]);
        } finally {
            try { fs.unlinkSync(agentTp); } catch { /* ignore */ }
            rmTokenState(`${sid}-agent-x1`);
        }
    });
});

test('SubagentStop with no subagent transcript carries no token columns', () => {
    const e = build({ hook_event_name: 'SubagentStop', session_id: 's1', agent_id: 'x2' });
    assert.equal(e.inputTokens, undefined);
    assert.equal(e._tokenCommit, undefined);
});

test('cleanupSessionState also removes the session\'s per-agent token files, and only those', () => {
    const sid = `cleanagent-${process.pid}-${Date.now()}`;
    const mine = path.join(os.tmpdir(), `cc-telemetry-tokens-${sid}-agent-x1.json`);
    const other = path.join(os.tmpdir(), `cc-telemetry-tokens-${sid}x-agent-x1.json`);
    try {
        fs.writeFileSync(mine, '{}');
        fs.writeFileSync(other, '{}');
        cleanupSessionState(sid);
        assert.ok(!fs.existsSync(mine));
        assert.ok(fs.existsSync(other)); // a different session that shares the prefix is kept
    } finally {
        for (const f of [mine, other]) { try { fs.unlinkSync(f); } catch { /* ignore */ } }
    }
});

Promise.all(pending).then(() => console.log(`\nPassed: ${pass}`));

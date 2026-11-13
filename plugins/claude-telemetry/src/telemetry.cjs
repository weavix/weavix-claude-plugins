#!/usr/bin/env node
'use strict';

/**
 * telemetry.cjs — emit Claude Code lifecycle telemetry as parquet to blob storage.
 *
 * Wired from the claude-telemetry plugin's hooks.json to the SessionStart, SessionEnd,
 * UserPromptSubmit, Stop, SubagentStart, SubagentStop, and PreToolUse(Skill)
 * hooks. Claude Code passes the hook payload as JSON on stdin; this reshapes it
 * into a flat row of metadata destined for WEAVIX_AI.TELEMETRY.CLAUDE_CODE_EVENTS.
 *
 * Each flushed batch is encoded as parquet (dist/parquet-writer.cjs, loaded only at flush)
 * and PUT to <sas_url>/claudecodeevents/<batch>-<n>.parquet with a create-only container
 * SAS; Snowpipe loads it. The blob name is fixed per batch and the PUT is If-None-Match: *,
 * so a retried upload is a no-op (the blob already exists) rather than a duplicate.
 *
 * Every event also carries where it ran — REPOSITORY / WORKFLOW / BRANCH (CI env
 * vars in Actions, the local git checkout otherwise) — plus the client ENTRYPOINT
 * and whether a human is attending. On Stop, it sums the turn's token usage and
 * the model that produced it from the session transcript (numbers only, never
 * message text), at per-turn grain via a byte offset baselined at SessionStart;
 * on SubagentStop, the same from the subagent's own transcript. Each API message
 * is counted once, however many transcript lines it is written as.
 *
 * Events are NOT uploaded one per event. Each is appended to a machine-local spool, and
 * the whole spool is flushed as one parquet file on SessionEnd (or when it passes a size
 * cap). Interactive sessions flush via a detached child (no session latency); CI / cloud
 * runs flush in-process, since a short-lived job would reap a detached child.
 *
 * Configuration:
 *   CLAUDE_TELEMETRY_SAS_URL       upload URL with SAS (else sas_url in ~/.claude/telemetry.json)
 *   CLAUDE_TELEMETRY_CONFIG        alternate path for that config file
 *   CLAUDE_AI_TELEMETRY_ENV        environment tag for this session (default $TARGET or 'local')
 *   CLAUDE_AI_TELEMETRY_ACTOR      who to attribute the run to in CLIENT_USER
 *                                  (default $GITHUB_ACTOR or $USER) — e.g. the service identity
 *   CLAUDE_SF_SYNC                 '1' forces an in-process flush, '0' a detached one; unset
 *                                  auto-detects CI and flushes in-process there
 *
 * Node built-ins only on the hot path. It is a silent no-op when no upload URL is
 * configured, never exits non-zero, and never emits prompt text or message bodies —
 * only metadata. Failed flushes are noted (status only, never the SAS) in a local
 * errors log next to the spool.
 */

const https = require('https');
const http = require('http');
const fs = require('fs');
const os = require('os');
const path = require('path');
const crypto = require('crypto');
const { spawn, execFileSync } = require('child_process');

// Fixed, ordered column list. Matches WEAVIX_AI.TELEMETRY.CLAUDE_CODE_EVENTS, whose schema the
// data repo's migrations/ai owns and keeps at or ahead of this list. RECORDED_AT is stamped
// by the pipe at load, so it is deliberately absent here.
const COLUMNS = [
    'SESSION_ID', 'HOOK_EVENT', 'CATEGORY', 'ACTION', 'SOURCE',
    'AGENT_TYPE', 'AGENT_ID', 'TOOL_NAME', 'SKILL', 'PERMISSION_MODE',
    'CWD', 'HOST', 'CLIENT_USER', 'ENVIRONMENT',
    'REPOSITORY', 'WORKFLOW', 'BRANCH',
    'INPUT_TOKENS', 'OUTPUT_TOKENS', 'CACHE_CREATION_INPUT_TOKENS',
    'CACHE_READ_INPUT_TOKENS', 'ENTRYPOINT', 'SESSION_ATTENDED',
    'MODEL',
];

// The base column count (through ENVIRONMENT). Newer columns are only ever appended, so a
// spooled row is valid if it has between MIN_ROW_WIDTH and COLUMNS.length values: a row
// written by an older hook (before newer columns existed) is padded with nulls on read
// rather than dropped, so an in-flight spool survives an upgrade. See parseSpool.
const MIN_ROW_WIDTH = 14;

// Per-session state (token offset, origin cache) lives in os.tmpdir(); a session that never
// fires SessionEnd (a crash) would leak its files. cleanupSessionState sweeps any older than
// this as a backstop, in addition to deleting a session's own files on SessionEnd.
const STATE_RETENTION_MS = 7 * 24 * 60 * 60 * 1000;

/**
 * owner/name from a git remote, else undefined. Matches only real remote forms —
 * scheme URLs (https/ssh/git) and scp-style git@host:owner/repo — so a local
 * filesystem path (no scheme, no user@host:) returns undefined rather than a
 * misleading "owner/name" that would mis-join telemetry to GitHub.
 */
function repoFromRemote(url) {
    if (!url) return undefined;
    const m = String(url).trim().match(
        /^(?:[a-z][a-z0-9+.-]*:\/\/(?:[^@/]+@)?[^/]+\/|[^@/\s]+@[^:/\s]+:)([^/\s]+\/[^/\s]+?)(?:\.git)?\/?$/i,
    );
    return m ? m[1] : undefined;
}

/**
 * Best-effort git value from a checkout, or undefined on any failure. Short timeout
 * and swallowed errors keep this in the hook's fire-and-forget contract; used only
 * for local sessions, where CI env vars (GITHUB_*) are absent.
 */
function gitValue(cwd, args) {
    try {
        const out = execFileSync('git', ['-C', cwd || '.', ...args], {
            encoding: 'utf8', timeout: 500, stdio: ['ignore', 'pipe', 'ignore'],
        }).trim();
        return out || undefined;
    } catch {
        return undefined;
    }
}

/** Per-session cache of the resolved repository (immutable for a checkout). */
function originCacheFile(sessionId) {
    const safeId = String(sessionId).replace(/[^A-Za-z0-9_-]/g, '_');
    return path.join(os.tmpdir(), `cc-telemetry-origin-${safeId}.json`);
}

/**
 * Resolve repository / workflow / branch. CI env vars win (present and correct in
 * GitHub Actions, no git call); locally, fall back to the checkout at cwd.
 *
 * The hook fires many times per session, so the git remote lookup — the one that would
 * otherwise run on every event — is resolved once and cached per session (REPOSITORY never
 * changes for a checkout). BRANCH is deliberately NOT cached: a session can switch branches
 * mid-way, and `branch --show-current` is a single cheap call, so it is re-read each event to
 * stay accurate. WORKFLOW is env-only.
 * @param {NodeJS.ProcessEnv} env
 * @param {string} cwd working directory of the session (payload.cwd)
 * @param {string} [sessionId] enables the per-session repository cache
 */
function resolveOrigin(env, cwd, sessionId) {
    let repository = env.GITHUB_REPOSITORY;
    if (!repository && sessionId) {
        try { repository = JSON.parse(fs.readFileSync(originCacheFile(sessionId), 'utf8')).repository; } catch { repository = undefined; }
    }
    if (!repository) {
        repository = repoFromRemote(gitValue(cwd, ['config', '--get', 'remote.origin.url']));
        if (repository && sessionId) {
            try { fs.writeFileSync(originCacheFile(sessionId), JSON.stringify({ repository })); } catch { /* best effort */ }
        }
    }
    return {
        repository,
        // WORKFLOW is the CI agent identity; null for a local session by design.
        workflow: env.GITHUB_WORKFLOW || undefined,
        // GITHUB_HEAD_REF is the source branch on pull_request runs; REF_NAME on push.
        branch: env.GITHUB_HEAD_REF || env.GITHUB_REF_NAME
            || gitValue(cwd, ['branch', '--show-current']),
    };
}

/** Per-stream state file (a session, or `<session>-agent-<id>`): the byte offset already counted. */
function tokenStateFile(sessionId) {
    const safeId = String(sessionId).replace(/[^A-Za-z0-9_-]/g, '_');
    return path.join(os.tmpdir(), `cc-telemetry-tokens-${safeId}.json`);
}

/**
 * Baseline the token offset to the transcript's current end. Called at SessionStart so a
 * resumed session (or one already running when the hook is deployed) counts only new turns
 * on its first Stop, instead of summing the whole cumulative transcript as one bogus turn.
 * A fresh session's transcript is empty here, so the baseline is 0. Best-effort.
 */
function initTokenOffset(transcriptPath, sessionId) {
    if (!sessionId) return;
    let size = 0;
    try { size = fs.statSync(transcriptPath).size; } catch { size = 0; }
    try { fs.writeFileSync(tokenStateFile(sessionId), JSON.stringify({ offset: size })); } catch { /* best effort */ }
}

/**
 * Persist the offset a turn consumed, and the carry: the last API message's key and the usage
 * already counted for it, so a message whose lines straddle two reads is not counted twice.
 * Called only AFTER the row is spooled (see main).
 */
function commitTokenOffset(sessionId, offset, carry) {
    if (!sessionId || !Number.isFinite(offset)) return;
    const state = carry ? { offset, carry } : { offset };
    try { fs.writeFileSync(tokenStateFile(sessionId), JSON.stringify(state)); } catch { /* best effort */ }
}

/**
 * Remove a session's per-session state files, called on SessionEnd so they do not accumulate.
 * Also sweeps any cc-telemetry-{tokens,origin}-* older than STATE_RETENTION_MS as a backstop
 * for sessions that crashed without a SessionEnd. Best-effort; never throws.
 */
function cleanupSessionState(sessionId, now = Date.now()) {
    const dir = os.tmpdir();
    let entries = [];
    try { entries = fs.readdirSync(dir); } catch { entries = []; }
    if (sessionId) {
        for (const f of [tokenStateFile(sessionId), originCacheFile(sessionId)]) {
            try { fs.unlinkSync(f); } catch { /* already gone */ }
        }
        // The session's subagents each have their own token state file (see buildEvent).
        const agentPrefix = path.basename(tokenStateFile(`${sessionId}-agent-`)).replace(/\.json$/, '');
        for (const name of entries) {
            if (name.startsWith(agentPrefix)) {
                try { fs.unlinkSync(path.join(dir, name)); } catch { /* already gone */ }
            }
        }
    }
    for (const name of entries) {
        if (!/^cc-telemetry-(tokens|origin)-/.test(name)) continue;
        const full = path.join(dir, name);
        try {
            if (now - fs.statSync(full).mtimeMs > STATE_RETENTION_MS) fs.unlinkSync(full);
        } catch { /* already gone or unreadable */ }
    }
}

/**
 * Coerce a transcript usage value to a non-negative integer; anything else counts as 0. A
 * numeric string would otherwise turn the running sum into a string ('0' + '10' -> '010'),
 * and a malformed value would produce an invalid FIXED bind that fails the whole batched
 * INSERT — so the transcript is not trusted to be well-typed here.
 */
function usageInt(v) {
    const n = Number(v);
    return Number.isFinite(n) && n >= 0 ? Math.trunc(n) : 0;
}

const USAGE_FIELDS = [
    ['input', 'input_tokens'], ['output', 'output_tokens'],
    ['cacheCreation', 'cache_creation_input_tokens'], ['cacheRead', 'cache_read_input_tokens'],
];

/**
 * Sum token usage produced since this stream's last committed offset, at per-turn grain.
 * A stream is the main session transcript, or one subagent's own transcript. Only new,
 * complete JSONL lines are read. Numbers only -- never message content -- so the
 * metadata-only guarantee holds.
 *
 * One API response is written as several lines (one per content block), each repeating its
 * usage: identical copies in the main transcript, growing snapshots in a subagent's. So usage
 * is taken once per (message.id, requestId), as the largest value of each field.
 *
 * Returns the sums plus `offset` and `carry` to persist, or null when there is nothing to
 * count. It does NOT advance the offset itself: the caller commits via commitTokenOffset only
 * once the row is spooled, so a turn that never reaches the spool replays. With no baseline
 * it baselines to the current end and returns null, rather than counting history -- unless
 * `fromStart`, for a subagent transcript, which is new when it first stops.
 * @returns {{input:number,output:number,cacheCreation:number,cacheRead:number,model:(string|null),offset:number,carry:(object|null)}|null}
 */
function turnTokens(transcriptPath, sessionId, { fromStart = false } = {}) {
    if (!transcriptPath || !sessionId) return null;
    const stateFile = tokenStateFile(sessionId);

    let size;
    try { size = fs.statSync(transcriptPath).size; } catch { return null; }

    let start, prior = null;
    try {
        const s = JSON.parse(fs.readFileSync(stateFile, 'utf8'));
        start = s.offset;
        prior = s.carry || null;
    } catch { start = undefined; }
    if (!Number.isFinite(start)) {
        if (fromStart) {
            start = 0;
        } else {
            // No baseline (session predates the hook, or the SessionStart write failed). Do not
            // sum the whole transcript as one turn -- baseline to the end and count from next.
            try { fs.writeFileSync(stateFile, JSON.stringify({ offset: size })); } catch { /* best effort */ }
            return null;
        }
    }
    if (start > size) { start = 0; prior = null; }  // transcript rotated/truncated -> re-read from the top
    if (start >= size) return null;   // nothing new since the last turn

    let text;
    try {
        const fd = fs.openSync(transcriptPath, 'r');
        const buf = Buffer.alloc(size - start);
        fs.readSync(fd, buf, 0, buf.length, start);
        fs.closeSync(fd);
        text = buf.toString('utf8');
    } catch { return null; }

    // Consume only up to the last complete line; a half-written final line is left for the
    // next turn by not advancing the offset past it.
    const lastNl = text.lastIndexOf('\n');
    if (lastNl < 0) return null;
    const consumed = text.slice(0, lastNl);

    // Largest usage per API message in this window. A line with no message id has nothing to
    // dedupe on and is counted as it stands. A usage line older than the newest already counted
    // is a copy -- after compaction Claude Code rewrites earlier messages, original timestamps
    // and all -- and is skipped. ISO-8601 UTC strings compare correctly as text.
    const t = { input: 0, output: 0, cacheCreation: 0, cacheRead: 0, model: null };
    const perMessage = new Map();
    let lastKey = null;
    let highWater = (prior && typeof prior.ts === 'string' && prior.ts) || '';
    for (const line of consumed.split('\n')) {
        if (!line) continue;
        let obj;
        try { obj = JSON.parse(line); } catch { continue; }
        const u = (obj && obj.message && obj.message.usage) || (obj && obj.usage);
        const ts = obj && typeof obj.timestamp === 'string' ? obj.timestamp : '';
        if (u && typeof u === 'object' && ts && ts < highWater) continue;
        if (u && typeof u === 'object' && ts) highWater = ts;
        if (u && typeof u === 'object') {
            const id = obj.message && obj.message.id;
            if (!id) {
                for (const [k, f] of USAGE_FIELDS) t[k] += usageInt(u[f]);
            } else {
                const key = `${id}|${obj.requestId || ''}`;
                const max = perMessage.get(key) || { input: 0, output: 0, cacheCreation: 0, cacheRead: 0 };
                for (const [k, f] of USAGE_FIELDS) max[k] = Math.max(max[k], usageInt(u[f]));
                perMessage.set(key, max);
                lastKey = key;
            }
        }
        // Model of the turn, alongside usage on each assistant message. Keep the last real one
        // seen; '<synthetic>' marks Claude Code's local, non-API messages (no model), so it is
        // skipped -- a turn with only those stays null.
        const m = obj && obj.message && obj.message.model;
        if (m && m !== '<synthetic>') t.model = m;
    }
    for (const [key, max] of perMessage) {
        // The message the last read ended inside was already counted up to `prior.usage`.
        const counted = prior && prior.key === key ? prior.usage : null;
        for (const [k] of USAGE_FIELDS) t[k] += Math.max(0, max[k] - (counted ? usageInt(counted[k]) : 0));
    }
    // Carried forward: the last message here, at the usage counted for it so far. A message
    // last seen in an earlier read stays carried until another one appears.
    let carry = prior;
    if (lastKey) {
        const max = { ...perMessage.get(lastKey) };
        if (prior && prior.key === lastKey) {
            for (const [k] of USAGE_FIELDS) max[k] = Math.max(max[k], usageInt(prior.usage[k]));
        }
        carry = { key: lastKey, usage: max };
    }
    if (carry && highWater) carry = { ...carry, ts: highWater };
    // The offset to persist once the row is spooled -- not written here.
    t.offset = start + Buffer.byteLength(consumed, 'utf8') + 1;
    t.carry = carry;
    return t;
}

/**
 * Reshape a Claude Code hook payload into a flat metadata event.
 * Absent fields are omitted so every hook type yields a clean event.
 * @param {Record<string, any>} payload parsed hook JSON from stdin
 * @param {{host: string, user: string, environment: string, repository?: string,
 *   workflow?: string, branch?: string, entrypoint?: string, attended?: boolean}} ctx machine context
 * @returns {Record<string, string>}
 */
function buildEvent(payload, ctx) {
    const h = payload.hook_event_name || 'Unknown';
    const toolInput = payload.tool_input || {};

    // One (category, action) pair per hook event. Kept explicit so prompts and
    // turns are first-class adoption signals, not lumped in with tool use.
    let category, action;
    if (h.startsWith('Session')) { category = 'session'; action = h.endsWith('Start') ? 'start' : 'end'; }
    else if (h.startsWith('Subagent')) { category = 'subagent'; action = h.endsWith('Start') ? 'start' : 'end'; }
    else if (h === 'UserPromptSubmit') { category = 'prompt'; action = 'submit'; }
    else if (h === 'Stop') { category = 'turn'; action = 'stop'; }
    else { category = 'skill'; action = 'use'; }

    const raw = {
        hookEvent: h,
        category,
        action,
        sessionId: payload.session_id,
        cwd: payload.cwd,
        permissionMode: payload.permission_mode,
        source: payload.source,
        agentType: payload.agent_type ?? payload.subagent_type ?? payload.agentType,
        agentId: payload.agent_id ?? payload.agentId,
        toolName: payload.tool_name,
        skill: toolInput.skill ?? toolInput.skillName ?? toolInput.command,
        host: ctx.host,
        user: ctx.user,
        environment: ctx.environment,
        repository: ctx.repository,
        workflow: ctx.workflow,
        branch: ctx.branch,
        entrypoint: ctx.entrypoint,
        attended: ctx.attended,
    };

    // Baseline the per-turn token offset when the session starts, so a resumed or
    // already-running session's first Stop counts only new turns (see initTokenOffset).
    if (h === 'SessionStart') {
        initTokenOffset(payload.transcript_path, payload.session_id);
    }

    // The session is over: drop its per-session state files (and sweep any leaked by crashes).
    if (h === 'SessionEnd') {
        cleanupSessionState(payload.session_id);
    }

    // Token usage + model on Stop, from the session transcript, and on SubagentStop, from the
    // subagent's own transcript -- the session transcript never holds a subagent's calls, so
    // each is counted once. A subagent has its own offset, keyed by agent id, so one resumed
    // and stopped again counts only its new calls. The offset is not advanced here --
    // _tokenCommit carries it to main(), which commits it only after the row is spooled, so a
    // turn that never reaches the spool replays next time.
    const agentId = raw.agentId;
    const stream = h === 'Stop' ? { path: payload.transcript_path, id: payload.session_id }
        : h === 'SubagentStop' && agentId && payload.session_id
            ? { path: payload.agent_transcript_path, id: `${payload.session_id}-agent-${agentId}`, fromStart: true }
            : null;
    if (stream) {
        const tok = turnTokens(stream.path, stream.id, { fromStart: !!stream.fromStart });
        if (tok) {
            raw.inputTokens = tok.input;
            raw.outputTokens = tok.output;
            raw.cacheCreation = tok.cacheCreation;
            raw.cacheRead = tok.cacheRead;
            raw.model = tok.model;  // null when the turn had no real model -> dropped below, column stays null
            raw._tokenCommit = { sessionId: stream.id, offset: tok.offset, carry: tok.carry };
        }
    }

    // Drop null/undefined so the event carries only meaningful attributes (0 and false kept).
    return Object.fromEntries(
        Object.entries(raw).filter(([, v]) => v !== null && v !== undefined),
    );
}

/** Map a built event onto the fixed COLUMNS order (missing → null). */
function toRow(event) {
    const byColumn = {
        SESSION_ID: event.sessionId,
        HOOK_EVENT: event.hookEvent,
        CATEGORY: event.category,
        ACTION: event.action,
        SOURCE: event.source,
        AGENT_TYPE: event.agentType,
        AGENT_ID: event.agentId,
        TOOL_NAME: event.toolName,
        SKILL: event.skill,
        PERMISSION_MODE: event.permissionMode,
        CWD: event.cwd,
        HOST: event.host,
        CLIENT_USER: event.user,
        ENVIRONMENT: event.environment,
        REPOSITORY: event.repository,
        WORKFLOW: event.workflow,
        BRANCH: event.branch,
        INPUT_TOKENS: event.inputTokens,
        OUTPUT_TOKENS: event.outputTokens,
        CACHE_CREATION_INPUT_TOKENS: event.cacheCreation,
        CACHE_READ_INPUT_TOKENS: event.cacheRead,
        ENTRYPOINT: event.entrypoint,
        SESSION_ATTENDED: event.attended,
        MODEL: event.model,
    };
    return COLUMNS.map(c => (byColumn[c] === undefined ? null : byColumn[c]));
}


/**
 * Whether to write synchronously rather than via a detached child. A detached
 * writer is right for a long-lived interactive session (no added latency), but a
 * short-lived non-interactive process — a CI/CD step, a scheduled job — exits the
 * instant the hook returns and would reap the child before the upload completes. So
 * write inline there. CLAUDE_SF_SYNC forces it either way ('1' on, '0' off);
 * unset, common CI markers auto-enable it.
 * @param {NodeJS.ProcessEnv} env
 */
function isNonInteractive(env) {
    if (env.CLAUDE_SF_SYNC === '1') return true;
    if (env.CLAUDE_SF_SYNC === '0') return false;
    return Boolean(env.CI || env.GITHUB_ACTIONS || env.CONTINUOUS_INTEGRATION || env.BUILD_ID || env.RUNNER_OS);
}


// --- Spool / batching (all sessions) ---------------------------------------
//
// A session fires this hook many times (once per prompt, turn, subagent, and skill use).
// Each event is appended to a machine-local spool and the whole spool is flushed as one
// parquet upload on SessionEnd (or when it passes a size cap). Interactive sessions flush
// via a detached child, CI / cloud in-process (see main()).

// Flush the spool once it passes this size, so a long or busy session bounds both
// ingest latency and how much telemetry a hard crash could lose.
const SPOOL_MAX_BYTES = 128 * 1024;
// A batch file younger than this is assumed to be in flight in another process;
// only reclaim it as orphaned once it is older, so a live flush is not stolen.
const ORPHAN_RETRY_AFTER_MS = 60 * 1000;
// Give up on (delete) a batch that has failed to send for this long, bounding
// disk use and stopping a poison batch from retrying forever.
// Seven days, so batches held through an expired-SAS gap survive until the SAS is renewed.
const ORPHAN_DROP_AFTER_MS = 7 * 24 * 60 * 60 * 1000;
// While the SAS is expired nothing is claimed or uploaded and the spool keeps growing; past this
// size new events are dropped, bounding disk use until the SAS is renewed.
const SPOOL_HOLD_MAX_BYTES = 16 * 1024 * 1024;

/**
 * Absolute path to the append spool. Per OS user and under the system temp dir,
 * so it always exists/writable and never collides between users on a shared box.
 * Override with CLAUDE_SF_SPOOL (tests). The spool is shared across concurrent
 * sessions on the machine — that only batches more events together; every row
 * still carries its own SESSION_ID.
 */
function spoolPath(env) {
    if (env.CLAUDE_SF_SPOOL) return env.CLAUDE_SF_SPOOL;
    let user = env.USER || env.USERNAME;
    if (!user) { try { user = os.userInfo().username; } catch { user = 'default'; } }
    const safe = String(user).replace(/[^A-Za-z0-9_.-]/g, '_');
    // Own subdirectory (created 0700) so the spool and its batch files are not world-writable
    // siblings in the shared temp root. Named apart from the legacy Snowflake hook's
    // spool.<user>.ndjson, so a machine still running that hook never mixes the two.
    return path.join(os.tmpdir(), 'claude-telemetry', `spool.${safe}.parquet.ndjson`);
}


/**
 * Append one row (COLUMNS order) as an NDJSON line. Creates the dir if needed and
 * opens with O_NOFOLLOW + 0600 so a symlink planted at the (predictable,
 * shared-temp) spool path can never redirect the append to a file the caller did
 * not intend — the open fails instead. Never follows into another user's target.
 */
function appendSpool(sp, row) {
    fs.mkdirSync(path.dirname(sp), { recursive: true, mode: 0o700 });
    const flags = fs.constants.O_WRONLY | fs.constants.O_CREAT | fs.constants.O_APPEND | fs.constants.O_NOFOLLOW;
    const fd = fs.openSync(sp, flags, 0o600);
    try { fs.writeSync(fd, JSON.stringify(row) + '\n'); }
    finally { fs.closeSync(fd); }
}

/**
 * Parse spooled NDJSON into rows. A row written before newer columns were appended is
 * shorter than the current COLUMNS and is padded with trailing nulls rather than dropped,
 * so an in-flight spool from a pre-upgrade hook is not lost on the next flush (the new
 * columns land null). Malformed lines, a row too short to be valid (< MIN_ROW_WIDTH), and a
 * row wider than COLUMNS (a newer/unknown schema) are skipped so one bad line never fails a
 * whole batch.
 * @returns {Array<Array<string|null>>}
 */
function parseSpool(text) {
    const rows = [];
    for (const line of text.split('\n')) {
        const t = line.trim();
        if (!t) continue;
        try {
            const v = JSON.parse(t);
            if (!Array.isArray(v) || v.length < MIN_ROW_WIDTH || v.length > COLUMNS.length) continue;
            while (v.length < COLUMNS.length) v.push(null);
            rows.push(v);
        } catch { /* skip a malformed line */ }
    }
    return rows;
}

/** Flush when a session ends, or when the spool passes the size cap. */
function shouldFlush(sizeBytes, hookEvent) {
    return hookEvent === 'SessionEnd' || sizeBytes >= SPOOL_MAX_BYTES;
}

/** Split an array into chunks of at most `size`. */
function chunk(arr, size) {
    const out = [];
    for (let i = 0; i < arr.length; i += size) out.push(arr.slice(i, i + size));
    return out;
}

/**
 * Take ownership of the spooled events to flush. Atomically renames the live
 * spool out of the append path (so new events start a fresh spool), then adopts
 * orphaned batch files left by a prior crashed/failed flush and deletes ones too
 * old to be worth retrying. Adoption is itself atomic (a rename), so two flushes
 * racing on the same machine never both claim — and therefore never both send —
 * the same orphan. Returns the batch file paths to send.
 * @param {string} sp spool path
 * @param {number} now epoch millis (Date.now(); injected for testability)
 * @param {string} uuid unique suffix for the claimed file
 * @returns {string[]}
 */
function claimBatches(sp, now, uuid) {
    const files = [];
    const claimed = `${sp}.batch.${now}.${uuid}`;
    // Atomic: if a peer process already renamed the spool, this throws and we
    // simply have nothing of our own to claim — we may still adopt orphans below.
    try { fs.renameSync(sp, claimed); files.push(claimed); } catch { /* nothing live to claim */ }

    const dir = path.dirname(sp);
    const prefix = `${path.basename(sp)}.batch.`;
    let entries = [];
    try { entries = fs.readdirSync(dir); } catch { entries = []; }
    for (const name of entries) {
        if (!name.startsWith(prefix)) continue;
        const full = path.join(dir, name);
        if (full === claimed) continue;
        let age;
        try { age = now - fs.statSync(full).mtimeMs; } catch { continue; }
        if (age > ORPHAN_DROP_AFTER_MS) { try { fs.unlinkSync(full); } catch { /* already gone */ } continue; }
        if (age <= ORPHAN_RETRY_AFTER_MS) continue; // too young — a peer flush likely owns it
        // Claim the orphan atomically: only the process whose rename succeeds owns
        // (and sends) it. A normalized `.r<uuid>` suffix keeps ownership current
        // without stacking across repeated retries; the name still starts with the
        // batch prefix so a later flush can re-adopt it if this one also fails.
        const owned = `${full.replace(/\.r[0-9a-f-]+$/, '')}.r${uuid}`;
        try { fs.renameSync(full, owned); files.push(owned); } catch { /* a peer claimed it first */ }
    }
    return files;
}


// Request timeouts. A detached background flush can wait out a slow upload. An in-process
// (CI/cloud) flush runs in the hook's own process, so each PUT is capped short AND the whole
// flush is bounded by a total wall-clock budget, returning before the 10s hook timeout.
const DETACHED_WRITE_TIMEOUT_MS = 30000;
const SYNC_WRITE_TIMEOUT_MS = 8000;
// Hard wall-clock ceiling for one in-process flush, across all its parts, with headroom under
// the 10s hook timeout for process start, loading the encoder, and TLS setup.
const SYNC_FLUSH_BUDGET_MS = 7000;


// --- Parquet sink ------------------------------------------------------------

// Parquet types for the non-text columns. RECORDED_AT is not in the file: the pipe stamps it
// with the load time, as the legacy sink's column default stamped the INSERT time.
const PARQUET_COLUMN_TYPES = {
    INPUT_TOKENS: 'INT64', OUTPUT_TOKENS: 'INT64',
    CACHE_CREATION_INPUT_TOKENS: 'INT64', CACHE_READ_INPUT_TOKENS: 'INT64',
    SESSION_ATTENDED: 'BOOLEAN',
};
// Folder under the SAS path that Snowpipe reads; one folder per entity, as parquet-compactor expects.
const PARQUET_ENTITY = 'claudecodeevents';
// Rows per parquet file. Sessions rarely spool more; a backlog is split rather than held in memory.
const MAX_ROWS_PER_PARQUET = 5000;
const BLOB_API_VERSION = '2021-08-06';
const ERROR_LOG_MAX_BYTES = 64 * 1024;

/** Path of the blob sink config JSON; override with CLAUDE_TELEMETRY_CONFIG. */
function blobConfigFilePath(env) {
    return env.CLAUDE_TELEMETRY_CONFIG || path.join(os.homedir(), '.claude', 'telemetry.json');
}

/**
 * Resolve the parquet sink's SAS URL, or null when it is absent or malformed.
 * CLAUDE_TELEMETRY_SAS_URL wins over sas_url in the config file. Must be signed, and https
 * unless it targets loopback (a local test server). `expired` is true once its `se` has passed.
 * @param {NodeJS.ProcessEnv} env
 * @param {number} [now] epoch millis, injectable for tests
 * @returns {{sasUrl: string, expiresAt: (number|null), expired: boolean}|null}
 */
function resolveBlobConfig(env, now = Date.now()) {
    let sasUrl = env.CLAUDE_TELEMETRY_SAS_URL;
    if (!sasUrl) {
        try { sasUrl = JSON.parse(fs.readFileSync(blobConfigFilePath(env), 'utf8')).sas_url; } catch { sasUrl = undefined; }
    }
    if (!sasUrl) return null;
    try {
        const u = new URL(String(sasUrl).trim());
        const loopback = u.protocol === 'http:' && ['127.0.0.1', 'localhost', '[::1]'].includes(u.hostname);
        if ((u.protocol !== 'https:' && !loopback) || !u.searchParams.get('sig')) return null;
        const se = Date.parse(u.searchParams.get('se') || '');
        return { sasUrl: u.toString(), expiresAt: Number.isNaN(se) ? null : se, expired: !Number.isNaN(se) && se <= now };
    } catch {
        return null;
    }
}


/**
 * Stable id of a claimed batch file: its `<epochMs>.<uuid>` claim suffix, minus any adoption
 * suffix. A retried batch therefore maps to the same blob names as its first attempt.
 * @param {string} file claimed batch file path
 */
function batchId(file) {
    const name = path.basename(file).replace(/\.r[0-9a-f-]+$/, '');
    const i = name.lastIndexOf('.batch.');
    const id = i === -1 ? name : name.slice(i + '.batch.'.length);
    return id.replace(/[^A-Za-z0-9_-]/g, '-');
}

/**
 * Full blob URL for one parquet part of a batch, keeping the SAS query string.
 * @param {string} sasUrl SAS URL whose path is the folder to write under (e.g. .../parquet/claude)
 * @param {string} id batch id from batchId
 * @param {number} part chunk index within the batch
 */
function blobUrlFor(sasUrl, id, part) {
    const u = new URL(sasUrl);
    u.pathname = `${u.pathname.replace(/\/+$/, '')}/${PARQUET_ENTITY}/${id}-${part}.parquet`;
    return u;
}

/**
 * Whether a failed PUT means the blob is already there: 409 BlobAlreadyExists, or the 403
 * UnauthorizedBlobOverwrite a create-only SAS returns for an existing blob.
 * @param {number} status HTTP status
 * @param {import('http').IncomingHttpHeaders} headers response headers
 */
function alreadyExists(status, headers) {
    const code = String(headers['x-ms-error-code'] || '');
    return status === 409 || (status === 403 && code === 'UnauthorizedBlobOverwrite');
}

/**
 * PUT one parquet file as a block blob. done(true) on 201, and when the blob already exists
 * because an earlier attempt landed (see alreadyExists), so a retried batch is idempotent. Never throws.
 * @param {URL} url blob URL with SAS
 * @param {Buffer} body parquet bytes
 * @param {(ok: boolean, status?: number) => void} done
 * @param {number} timeoutMs socket timeout
 */
function uploadBlob(url, body, done, timeoutMs) {
    let finished = false;
    const finish = (ok, status) => { if (!finished) { finished = true; done(ok, status); } };
    let req;
    try {
        const transport = url.protocol === 'http:' ? http : https;
        req = transport.request(url, {
            method: 'PUT',
            headers: {
                'x-ms-blob-type': 'BlockBlob',
                'x-ms-version': BLOB_API_VERSION,
                'If-None-Match': '*',
                'Content-Type': 'application/vnd.apache.parquet',
                'Content-Length': body.length,
                'User-Agent': 'weavix-claude-telemetry/2.0',
            },
        }, res => {
            const status = res.statusCode;
            res.resume();
            res.on('end', () => finish((status >= 200 && status < 300) || alreadyExists(status, res.headers), status));
            res.on('error', () => finish(false, status));
        });
    } catch {
        return finish(false);
    }
    req.setTimeout(timeoutMs, () => req.destroy());
    req.on('error', () => finish(false));
    req.end(body);
}

/** Load the bundled parquet encoder; kept off the per-event path because it is ~1 MB. */
function loadEncoder() {
    process.noDeprecation = true;
    return require(path.join(__dirname, '..', 'dist', 'parquet-writer.cjs')).encodeParquet;
}

/**
 * Append one line to the local errors log beside the spool, truncating it past ERROR_LOG_MAX_BYTES.
 * Carries the sink and a short reason only; never a URL, SAS or row data. Never throws.
 */
function logFailure(env, sink, reason) {
    try {
        const file = path.join(path.dirname(spoolPath(env)), 'errors.log');
        fs.mkdirSync(path.dirname(file), { recursive: true, mode: 0o700 });
        let size = 0;
        try { size = fs.statSync(file).size; } catch { size = 0; }
        const line = `${new Date().toISOString()} ${sink} ${reason}\n`;
        if (size > ERROR_LOG_MAX_BYTES) fs.writeFileSync(file, line, { mode: 0o600 });
        else fs.appendFileSync(file, line, { mode: 0o600 });
    } catch { /* best effort */ }
}

/**
 * Upload each claimed parquet-spool batch file as one or more parquet blobs, deleting a file
 * once all its parts land. A failed or timed-out file is kept with its mtime bumped, so it is
 * re-adopted after ORPHAN_RETRY_AFTER_MS (claimBatches) and re-sent under the same blob names.
 * Never throws; always calls done once. `encode` and `upload` are injectable for tests.
 * @param {{sasUrl: string}} blob parquet sink config
 * @param {string[]} files claimed batch files
 * @param {number} timeoutMs per-upload socket timeout
 * @param {(ok: boolean) => void} done
 * @param {{encode?: Function, upload?: Function, totalBudgetMs?: number, onError?: (reason: string) => void}} [opts]
 */
function flushParquetBatches(blob, files, timeoutMs, done, opts = {}) {
    const upload = opts.upload || uploadBlob;
    const totalBudgetMs = opts.totalBudgetMs ?? Infinity;
    const onError = opts.onError || (() => {});
    let settled = false;
    let allOk = true;
    let hardTimer = null;
    const pendingFiles = [...files];

    const keep = f => {
        const t = Date.now() / 1000;
        try { fs.utimesSync(f, t, t); } catch { /* already gone */ }
    };
    const finalize = () => {
        if (settled) return;
        settled = true;
        if (hardTimer) { clearTimeout(hardTimer); hardTimer = null; }
        for (const f of pendingFiles) keep(f);
        done(allOk);
    };
    if (totalBudgetMs !== Infinity) {
        hardTimer = setTimeout(() => { allOk = false; onError('budget-exceeded'); finalize(); }, totalBudgetMs);
        if (hardTimer.unref) hardTimer.unref();
    }

    const nextFile = async () => {
        if (settled) return;
        if (pendingFiles.length === 0) return finalize();
        const file = pendingFiles[0];
        let rows;
        try { rows = parseSpool(fs.readFileSync(file, 'utf8')); } catch { rows = []; }
        const parts = chunk(rows, MAX_ROWS_PER_PARQUET);
        const id = batchId(file);
        for (let i = 0; i < parts.length; i += 1) {
            let body;
            try {
                const encode = opts.encode || loadEncoder();
                body = await encode(parts[i], COLUMNS, PARQUET_COLUMN_TYPES);
            } catch (e) {
                allOk = false;
                onError(`encode-failed ${e && e.name ? e.name : 'Error'}`);
                return finalize();
            }
            if (settled) return;
            const result = await new Promise(resolve => upload(blobUrlFor(blob.sasUrl, id, i), body, (ok, status) => resolve({ ok, status }), timeoutMs));
            if (settled) return;
            if (!result.ok) {
                allOk = false;
                onError(`upload-failed status=${result.status ?? 'none'}`);
                return finalize();
            }
        }
        pendingFiles.shift();
        try { fs.unlinkSync(file); } catch { /* already gone */ }
        return nextFile();
    };
    nextFile().catch(() => { allOk = false; finalize(); });
}

/**
 * Hand the flush to a detached background process and return, so the session sees no latency.
 * Only the batch file paths are passed; the child re-resolves the SAS from the same env / file.
 */
function emitDetached(files) {
    const job = Buffer.from(JSON.stringify({ parquetFiles: files })).toString('base64');
    const child = spawn(process.execPath, [__filename], {
        detached: true,
        stdio: 'ignore',
        env: { ...process.env, CLAUDE_SF_EMIT: job },
    });
    child.unref();
}

/**
 * Upload claimed batches and call done once they settle. Missing or expired config keeps the
 * batches for a later flush and records why in the local errors log.
 * @param {{sasUrl: string, expired: boolean}|null} blob upload config
 * @param {string[]} files claimed batch files
 * @param {{timeoutMs: number, budgetMs: number}} limits per-request timeout and whole-flush budget
 * @param {() => void} done
 */
function flushAll(blob, files, limits, done) {
    const env = process.env;
    if (!files.length) return done();
    if (!blob || blob.expired) {
        logFailure(env, 'parquet', blob ? 'sas-expired' : 'sas-missing');
        return done();
    }
    flushParquetBatches(blob, files, limits.timeoutMs, () => done(), {
        totalBudgetMs: limits.budgetMs,
        onError: reason => logFailure(env, 'parquet', reason),
    });
}

function main() {
    // Child mode: the detached flusher spawned below. Upload the claimed batches and exit; it
    // owns deleting them on success or leaving them to retry.
    if (process.env.CLAUDE_SF_EMIT) {
        let job;
        try {
            job = JSON.parse(Buffer.from(process.env.CLAUDE_SF_EMIT, 'base64').toString('utf8'));
        } catch {
            process.exit(0);
        }
        return flushAll(resolveBlobConfig(process.env), job.parquetFiles || [], {
            timeoutMs: DETACHED_WRITE_TIMEOUT_MS, budgetMs: Infinity,
        }, () => process.exit(0));
    }

    // Hook mode: build the event and record it, returning fast.
    const blob = resolveBlobConfig(process.env);
    // No upload URL configured — nothing to do; succeed quietly.
    if (!blob) process.exit(0);

    let input = '';
    process.stdin.setEncoding('utf8');
    process.stdin.on('data', chunk => { input += chunk; });
    process.stdin.on('end', () => {
        let payload;
        try {
            payload = JSON.parse(input || '{}');
        } catch {
            process.exit(0);
        }
        const event = buildEvent(payload, {
            host: os.hostname(),
            // In CI/cloud there is no login user; attribute to an explicit actor
            // (the service identity or the human who triggered the run) when given.
            user: process.env.CLAUDE_AI_TELEMETRY_ACTOR || process.env.GITHUB_ACTOR || process.env.USER || 'unknown',
            environment: process.env.CLAUDE_AI_TELEMETRY_ENV || process.env.TARGET || 'local',
            // Which Claude Code client this is, and whether a human is attending it.
            entrypoint: process.env.CLAUDE_CODE_ENTRYPOINT || undefined,
            attended: ('CLAUDE_CODE_SESSION_ATTENDED' in process.env)
                ? /^(1|true)$/i.test(process.env.CLAUDE_CODE_SESSION_ATTENDED || '')
                : undefined,
            // repository / workflow / branch: env in CI, git checkout locally (repository
            // cached per session; see resolveOrigin).
            ...resolveOrigin(process.env, payload.cwd, payload.session_id),
        });

        try {
            // Append the event to the machine-local spool; only when it is time, flush the whole
            // spool as one parquet upload. Most events just append and return in microseconds.
            const sp = spoolPath(process.env);
            const sizeOf = () => { try { return fs.statSync(sp).size; } catch { return 0; } };
            if (!blob.expired || sizeOf() < SPOOL_HOLD_MAX_BYTES) appendSpool(sp, toRow(event));

            // The turn's tokens are now durably in the spool (its own retry owns delivery), so
            // advance the offset once here -- committing only after a deferred flush would let
            // the next Stop re-read and double-count the same turn. A row later dropped as a
            // poison batch loses those tokens; acceptable for best-effort telemetry.
            if (event._tokenCommit) {
                commitTokenOffset(event._tokenCommit.sessionId, event._tokenCommit.offset, event._tokenCommit.carry);
            }

            if (!shouldFlush(sizeOf(), event.hookEvent)) process.exit(0);

            // Expired SAS: hold everything in the spool, unclaimed, so no batch ages toward the
            // drop window before the SAS is renewed; the first flush after renewal sends it all.
            if (blob.expired) {
                logFailure(process.env, 'parquet', 'sas-expired');
                process.exit(0);
            }

            const files = claimBatches(sp, Date.now(), crypto.randomUUID());
            if (!files.length) process.exit(0);

            if (isNonInteractive(process.env)) {
                // CI / cloud: flush in-process, since a short-lived job would reap a detached
                // child before its upload landed. Bounded by SYNC_FLUSH_BUDGET_MS so it returns
                // within the hook timeout. A hard kill mid-flush drops that bounded batch.
                return flushAll(blob, files, {
                    timeoutMs: SYNC_WRITE_TIMEOUT_MS, budgetMs: SYNC_FLUSH_BUDGET_MS,
                }, () => process.exit(0));
            }

            emitDetached(files);
        } catch {
            /* never let telemetry surface an error to the session */
        }
        process.exit(0);
    });
}

if (require.main === module) main();

module.exports = {
    buildEvent, toRow, isNonInteractive, COLUMNS,
    spoolPath, appendSpool, parseSpool, shouldFlush, chunk, claimBatches,
    SPOOL_MAX_BYTES, SPOOL_HOLD_MAX_BYTES, ORPHAN_RETRY_AFTER_MS, ORPHAN_DROP_AFTER_MS,
    repoFromRemote, resolveOrigin, turnTokens, initTokenOffset, commitTokenOffset,
    originCacheFile, cleanupSessionState, usageInt, MIN_ROW_WIDTH, STATE_RETENTION_MS,
    resolveBlobConfig, blobConfigFilePath, batchId, blobUrlFor, uploadBlob, alreadyExists,
    flushParquetBatches, flushAll, logFailure, PARQUET_COLUMN_TYPES, PARQUET_ENTITY, MAX_ROWS_PER_PARQUET,
};

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
 * Two sinks, each with its own spool and retry so one failing never re-sends the other:
 *   - parquet: each flushed batch is encoded as parquet (dist/parquet-writer.cjs, loaded only
 *     at flush) and PUT to <sas_url>/claudecodeevents/<batch>-<n>.parquet with a create-only SAS;
 *     Snowpipe loads it. The blob name is fixed per batch, so a retried upload is a no-op (409).
 *     Config: CLAUDE_TELEMETRY_SAS_URL, else sas_url in ~/.claude/telemetry.json
 *     (override the file with CLAUDE_TELEMETRY_CONFIG).
 *   - snowflake: the legacy multi-row INSERT via the Snowflake SQL REST API, kept while the pipe
 *     is proven. Runs only when its connector (below) is configured; CLAUDE_TELEMETRY_SNOWFLAKE=0
 *     turns it off even then.
 *
 * Every event also carries where it ran — REPOSITORY / WORKFLOW / BRANCH (CI env
 * vars in Actions, the local git checkout otherwise) — plus the client ENTRYPOINT
 * and whether a human is attending. On Stop, it sums the turn's token usage and
 * the model that produced it from the session transcript (numbers only, never
 * message text), at per-turn grain via a byte offset baselined at SessionStart;
 * on SubagentStop, the same from the subagent's own transcript. Each API message
 * is counted once, however many transcript lines it is written as.
 *
 * Events are NOT written one statement per event — that keeps the warehouse
 * resumed continuously for near-zero data. Each event is appended to a
 * machine-local spool, and the whole spool is flushed as ONE multi-row INSERT on
 * SessionEnd (or when it passes a size cap), collapsing ~100 statements/session
 * into ~1. Interactive sessions flush via a detached child (no session latency);
 * CI / cloud / service-identity runs flush in-process (a short-lived job would
 * reap a detached child, and an inline key must never enter a child's env).
 *
 * The snowflake sink's configuration comes from a hidden per-user JSON file written at setup —
 * ~/.snowflake/claude-telemetry.json (override with CLAUDE_SNOWFLAKE_CONFIG) —
 * with keys: account, user, private_key_file, and optionally private_key_passphrase,
 * role, warehouse, database, schema, table, host. The file is read on every run,
 * so no relaunch is needed after it is written. Environment variables below fill
 * any gaps and keep older env-only setups working:
 *   SNOWFLAKE_ACCOUNT              account identifier, e.g. fpthfcq-lx42606 (required)
 *   SNOWFLAKE_USER                 your Snowflake login (required)
 *   SNOWFLAKE_PRIVATE_KEY_FILE     absolute path to the PKCS#8 private key (required*)
 *   SNOWFLAKE_PRIVATE_KEY          the PKCS#8 private key inline as PEM (required*)
 *   SNOWFLAKE_PRIVATE_KEY_B64      the PKCS#8 private key inline, base64 of the PEM (required*)
 *     (* one key source is required: a file path, or an inline PEM / base64 value.
 *      Inline keys let a non-interactive service identity — CI/CD, cloud, a
 *      scheduled job — authenticate from a secret with no on-disk connector file.)
 *   SNOWFLAKE_PRIVATE_KEY_PASSPHRASE  passphrase, if the key is encrypted (optional)
 *   SNOWFLAKE_ROLE                 role to write as (optional; unset = the user's default role)
 *   SNOWFLAKE_WAREHOUSE            warehouse (default WEAVIX_AI_WH)
 *   SNOWFLAKE_AI_DATABASE          target database (default WEAVIX_AI)
 *   SNOWFLAKE_AI_SCHEMA            target schema (default TELEMETRY)
 *   SNOWFLAKE_AI_TABLE             target table (default CLAUDE_CODE_EVENTS)
 *   SNOWFLAKE_HOST                 override the API host (default <account>.snowflakecomputing.com)
 *   CLAUDE_AI_TELEMETRY_ENV        environment tag for this session (default $TARGET or 'local')
 *   CLAUDE_AI_TELEMETRY_ACTOR      who to attribute the run to in CLIENT_USER
 *                                  (default $GITHUB_ACTOR or $USER) — e.g. the service identity
 *   CLAUDE_SF_SYNC                 '1' forces an in-process flush, '0' forces a detached flush — but an
 *                                  inline key always flushes in-process regardless (so the key
 *                                  never enters a detached child's env); unset auto-detects CI,
 *                                  flushing in-process so a short-lived process does not exit first
 *
 * Node built-ins only on the hot path. It is a silent no-op when neither sink is
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

// Fixed, ordered column list. Matches WEAVIX_AI.TELEMETRY.CLAUDE_CODE_EVENTS.
// RECORDED_AT is populated server-side (DEFAULT CURRENT_TIMESTAMP), so it is
// deliberately absent here. The table's schema is owned by the data repo's
// migrations/ai singleton and is always at or ahead of this list, so a widened
// COLUMNS lands cleanly (no per-column schema-lag fallback needed here).
const COLUMNS = [
    'SESSION_ID', 'HOOK_EVENT', 'CATEGORY', 'ACTION', 'SOURCE',
    'AGENT_TYPE', 'AGENT_ID', 'TOOL_NAME', 'SKILL', 'PERMISSION_MODE',
    'CWD', 'HOST', 'CLIENT_USER', 'ENVIRONMENT',
    'REPOSITORY', 'WORKFLOW', 'BRANCH',
    'INPUT_TOKENS', 'OUTPUT_TOKENS', 'CACHE_CREATION_INPUT_TOKENS',
    'CACHE_READ_INPUT_TOKENS', 'ENTRYPOINT', 'SESSION_ATTENDED',
    'MODEL',
];

// Bind types for the columns that are not VARCHAR; the rest bind as TEXT.
const COLUMN_TYPES = {
    INPUT_TOKENS: 'FIXED', OUTPUT_TOKENS: 'FIXED',
    CACHE_CREATION_INPUT_TOKENS: 'FIXED', CACHE_READ_INPUT_TOKENS: 'FIXED',
    SESSION_ATTENDED: 'BOOLEAN',
};

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

const IDENT = /^[A-Za-z0-9_]+$/;

/**
 * Build a parameterised multi-row INSERT and its bindings for the SQL API.
 * Values are bound (never interpolated); identifiers are validated against a
 * strict allow-list so a stray env var can never inject SQL. One statement
 * carries every row, so a whole session's telemetry lands in a single
 * warehouse statement instead of one per event. Each value binds by its column's
 * type (COLUMN_TYPES) so the numeric token columns and the boolean attended flag
 * land as numbers/booleans, not strings; every other column binds as TEXT.
 * @param {Array<Array<string|number|boolean|null>>} rows rows already in COLUMNS order (see toRow)
 * @returns {{statement: string, bindings: Record<string, {type: string, value: string|null}>}}
 */
function buildBatchInsert(rows, target) {
    for (const id of [target.database, target.schema, target.table]) {
        if (!IDENT.test(id)) throw new Error(`invalid identifier: ${id}`);
    }
    const fqtn = `${target.database}.${target.schema}.${target.table}`;
    const tuple = `(${COLUMNS.map(() => '?').join(', ')})`;
    const statement = `INSERT INTO ${fqtn} (${COLUMNS.join(', ')}) VALUES ${rows.map(() => tuple).join(', ')}`;

    const bindings = {};
    let i = 1;
    for (const row of rows) {
        row.forEach((v, j) => {
            const type = COLUMN_TYPES[COLUMNS[j]] || 'TEXT';
            const value = v == null ? null
                : type === 'BOOLEAN' ? (v ? 'true' : 'false')
                : String(v);
            bindings[String(i++)] = { type, value };
        });
    }
    return { statement, bindings };
}

/**
 * Build the INSERT for a single event — a thin wrapper over buildBatchInsert for
 * the degenerate one-row batch. Both write paths flush via buildBatchInsert; this
 * remains as a tested helper that pins the single-row statement shape.
 * @returns {{statement: string, bindings: Record<string, {type: string, value: string|null}>}}
 */
function buildInsert(event, target) {
    return buildBatchInsert([toRow(event)], target);
}

/** base64url, no padding. */
function b64url(buf) {
    return Buffer.from(buf).toString('base64')
        .replace(/\+/g, '-').replace(/\//g, '_').replace(/=+$/, '');
}

/**
 * Normalise an account identifier for the key-pair JWT: uppercase, and drop any
 * region/cloud segment after the first dot (mirrors the Snowflake connectors).
 * e.g. "fpthfcq-lx42606" -> "FPTHFCQ-LX42606"; "abc.us-east-1" -> "ABC".
 */
function normalizeAccount(account) {
    const upper = account.toUpperCase();
    const dot = upper.indexOf('.');
    return dot === -1 ? upper : upper.slice(0, dot);
}

/**
 * Build a Snowflake key-pair (KEYPAIR_JWT) bearer token from the private key.
 * @returns {string} signed JWT
 */
function buildJwt(privateKeyPem, passphrase, account, user, nowSec) {
    const privateKey = crypto.createPrivateKey(
        passphrase ? { key: privateKeyPem, passphrase } : privateKeyPem,
    );
    const publicDer = crypto.createPublicKey(privateKey).export({ type: 'spki', format: 'der' });
    const fp = 'SHA256:' + crypto.createHash('sha256').update(publicDer).digest('base64');

    const acct = normalizeAccount(account);
    const qualifiedUser = `${acct}.${user.toUpperCase()}`;
    const header = { alg: 'RS256', typ: 'JWT' };
    const claims = {
        iss: `${qualifiedUser}.${fp}`,
        sub: qualifiedUser,
        iat: nowSec,
        exp: nowSec + 3600,
    };
    const signingInput = `${b64url(JSON.stringify(header))}.${b64url(JSON.stringify(claims))}`;
    const signature = crypto.sign('RSA-SHA256', Buffer.from(signingInput), privateKey);
    return `${signingInput}.${b64url(signature)}`;
}

/**
 * Absolute path to the connector config JSON. Defaults to a hidden per-user
 * file; override with CLAUDE_SNOWFLAKE_CONFIG (used by tests and CI).
 */
function configFilePath(env) {
    return env.CLAUDE_SNOWFLAKE_CONFIG || path.join(os.homedir(), '.snowflake', 'claude-telemetry.json');
}

/** Read and parse the connector config JSON, or {} if absent/unreadable. */
function readConfigFile(env) {
    try {
        return JSON.parse(fs.readFileSync(configFilePath(env), 'utf8'));
    } catch {
        return {};
    }
}

/**
 * Normalise an inline PEM. CI/CD secret stores often flatten a multi-line key to
 * one line with literal "\n" sequences; restore them if there are no real
 * newlines. A key delivered with genuine newlines (or via base64) is untouched.
 */
function normalizePem(pem) {
    return pem.includes('\n') ? pem : pem.replace(/\\n/g, '\n');
}

/**
 * An inline private key from config/env, or undefined. Prefers base64 (safe for
 * multi-line secrets); falls back to a raw PEM value. This is what lets a
 * non-interactive service identity authenticate without an on-disk key file.
 * @returns {string|undefined} PEM text
 */
function resolveInlineKey(f, env) {
    const b64 = f.private_key_b64 || env.SNOWFLAKE_PRIVATE_KEY_B64;
    // base64 decode never throws; a malformed value yields a bad PEM that fails
    // harmlessly later in buildJwt rather than here.
    if (b64) return Buffer.from(b64, 'base64').toString('utf8');
    const raw = f.private_key || env.SNOWFLAKE_PRIVATE_KEY;
    return raw ? normalizePem(raw) : undefined;
}

/**
 * Resolve connector config, or null if it is not set up. The hidden JSON file
 * (configFilePath) is the source of truth; environment variables fill any gaps
 * and keep older env-only setups working. Requires an account, a user, and a
 * private key from either a file (interactive per-user connector) or an inline
 * value (non-interactive service identity — CI/CD, cloud, scheduled jobs).
 * @param {NodeJS.ProcessEnv} env
 */
function resolveConfig(env) {
    const f = readConfigFile(env);
    const account = f.account || env.SNOWFLAKE_ACCOUNT;
    const user = f.user || env.SNOWFLAKE_USER;
    const keyFile = f.private_key_file || env.SNOWFLAKE_PRIVATE_KEY_FILE;
    const keyData = resolveInlineKey(f, env);
    if (!account || !user || (!keyFile && !keyData)) return null;
    return {
        account,
        user,
        // An inline key wins over a file path when both are present.
        keyFile: keyData ? undefined : keyFile,
        keyData,
        passphrase: f.private_key_passphrase || env.SNOWFLAKE_PRIVATE_KEY_PASSPHRASE || undefined,
        // Role is optional: unset means write as the user's default role, which
        // inherits AI_USAGE_ROLE (granted to the human functional roles) — so
        // telemetry works for every user.
        role: f.role || env.SNOWFLAKE_ROLE || undefined,
        warehouse: f.warehouse || env.SNOWFLAKE_WAREHOUSE || 'WEAVIX_AI_WH',
        host: f.host || env.SNOWFLAKE_HOST || `${account.toLowerCase()}.snowflakecomputing.com`,
        target: {
            database: f.database || env.SNOWFLAKE_AI_DATABASE || 'WEAVIX_AI',
            schema: f.schema || env.SNOWFLAKE_AI_SCHEMA || 'TELEMETRY',
            table: f.table || env.SNOWFLAKE_AI_TABLE || 'CLAUDE_CODE_EVENTS',
        },
    };
}

/** Read the private key from the inline value if present, else from the file. */
function readPrivateKey(cfg) {
    return cfg.keyData != null ? cfg.keyData : fs.readFileSync(cfg.keyFile, 'utf8');
}

/**
 * Whether to write synchronously rather than via a detached child. A detached
 * writer is right for a long-lived interactive session (no added latency), but a
 * short-lived non-interactive process — a CI/CD step, a scheduled job — exits the
 * instant the hook returns and would reap the child before the POST completes. So
 * write inline there. CLAUDE_SF_SYNC forces it either way ('1' on, '0' off);
 * unset, common CI markers auto-enable it.
 * @param {NodeJS.ProcessEnv} env
 */
function isNonInteractive(env) {
    if (env.CLAUDE_SF_SYNC === '1') return true;
    if (env.CLAUDE_SF_SYNC === '0') return false;
    return Boolean(env.CI || env.GITHUB_ACTIONS || env.CONTINUOUS_INTEGRATION || env.BUILD_ID || env.RUNNER_OS);
}

/**
 * Whether to write in-process rather than via a detached child. Two reasons force
 * it: a non-interactive run is short-lived and would reap a detached child before
 * the POST lands; and an inline key must never be serialized into a detached
 * child's environment — the in-process path keeps the key inside this process.
 */
function shouldWriteSync(cfg, env) {
    return (cfg != null && cfg.keyData != null) || isNonInteractive(env);
}

// --- Spool / batching (all sessions) ---------------------------------------
//
// A session fires this hook many times (once per prompt, turn, subagent, and
// skill use). Writing each as its own INSERT keeps the warehouse resumed
// continuously for near-zero data. Instead, each event is appended to a
// machine-local spool and the whole spool is flushed as ONE multi-row INSERT on
// SessionEnd (or when it passes a size cap), collapsing ~100 statements/session
// into ~1. Both write paths spool; they differ only in how the flush is sent —
// interactive via a detached child, CI / cloud in-process (see main()).

// Flush the spool once it passes this size, so a long or busy session bounds both
// warehouse latency and how much telemetry a hard crash could lose.
const SPOOL_MAX_BYTES = 128 * 1024;
// Cap rows per INSERT so a large backlog is chunked rather than sent as one
// oversized statement.
const MAX_ROWS_PER_INSERT = 500;
// A batch file younger than this is assumed to be in flight in another process;
// only reclaim it as orphaned once it is older, so a live flush is not stolen.
const ORPHAN_RETRY_AFTER_MS = 60 * 1000;
// Give up on (delete) a batch that has failed to send for this long, bounding
// disk use and stopping a poison batch from retrying forever.
const ORPHAN_DROP_AFTER_MS = 24 * 60 * 60 * 1000;

/**
 * Absolute path to the append spool. Per OS user and under the system temp dir,
 * so it always exists/writable and never collides between users on a shared box.
 * Override with CLAUDE_SF_SPOOL (tests). The spool is shared across concurrent
 * sessions on the machine — that only batches more events together; every row
 * still carries its own SESSION_ID.
 */
function spoolPath(env, sink = 'snowflake') {
    const base = env.CLAUDE_SF_SPOOL || defaultSpoolPath(env);
    // The snowflake spool keeps its original name so a spool written before the upgrade still flushes.
    return sink === 'snowflake' ? base : base.replace(/\.ndjson$/, '') + `.${sink}.ndjson`;
}

/** Machine-local spool path for the snowflake sink, per OS user, under the system temp dir. */
function defaultSpoolPath(env) {
    let user = env.USER || env.USERNAME;
    if (!user) { try { user = os.userInfo().username; } catch { user = 'default'; } }
    const safe = String(user).replace(/[^A-Za-z0-9_.-]/g, '_');
    // Own subdirectory (created 0700) so the spool and its batch files are not
    // world-writable siblings in the shared temp root.
    return path.join(os.tmpdir(), 'claude-telemetry', `spool.${safe}.ndjson`);
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

/**
 * Read the claimed batch files, send their rows as chunked multi-row INSERTs,
 * and on full success delete the files. The first chunk failure aborts the rest
 * (retention is per-file, so a later flush re-sends the whole batch — sending more
 * now would only duplicate what lands); the files are left in place to be
 * re-adopted (at-least-once), with their mtime bumped to now so they are not
 * re-sent until another ORPHAN_RETRY_AFTER_MS has passed — a backoff, so an
 * intermittent-Snowflake window does not re-resume the warehouse on every flush. A crash between a successful POST and the delete can
 * re-send a batch — a rare duplicate that the adoption grace makes unlikely; rows
 * are otherwise not de-dupable, so consumers should treat counts as approximate.
 * `timeoutMs` bounds each POST; `totalBudgetMs` bounds the whole (possibly
 * multi-chunk) flush in wall-clock time — the in-process path passes a budget
 * under the hook timeout so it stops gracefully rather than being killed mid-POST,
 * while the detached child passes Infinity (it outlives the hook). Never throws;
 * always calls done. `post` is injectable for tests.
 */
function flushBatches(cfg, files, timeoutMs, done, post = postStatement, totalBudgetMs = Infinity) {
    let rows = [];
    for (const f of files) {
        try { rows = rows.concat(parseSpool(fs.readFileSync(f, 'utf8'))); } catch { /* skip unreadable */ }
    }
    if (rows.length === 0) {
        for (const f of files) { try { fs.unlinkSync(f); } catch { /* already gone */ } }
        return done(true);
    }

    let settled = false;
    let allOk = true;
    let hardTimer = null;
    // Finalize exactly once: delete the files if every chunk landed, otherwise
    // retain them (bumping mtime so the retry grace applies) and report failure.
    const finalize = () => {
        if (settled) return;
        settled = true;
        if (hardTimer) { clearTimeout(hardTimer); hardTimer = null; }
        if (allOk) {
            for (const f of files) { try { fs.unlinkSync(f); } catch { /* already gone */ } }
        } else {
            const t = Date.now() / 1000;
            for (const f of files) { try { fs.utimesSync(f, t, t); } catch { /* already gone */ } }
        }
        done(allOk);
    };

    // Hard wall-clock ceiling for the WHOLE (possibly multi-chunk) flush. A per-POST
    // timeout can't bound the total — a socket to a slow/unreachable host may abort
    // early (letting the next chunk start) or late, so total time drifts. This timer
    // guarantees the in-process flush returns within the hook timeout regardless. The
    // detached child passes Infinity (it outlives the hook) and installs no ceiling.
    if (totalBudgetMs !== Infinity) {
        hardTimer = setTimeout(() => { allOk = false; finalize(); }, totalBudgetMs);
        if (hardTimer.unref) hardTimer.unref();
    }

    const chunks = chunk(rows, MAX_ROWS_PER_INSERT);
    let idx = 0;
    const next = () => {
        if (settled) return; // the hard ceiling already finalized this flush
        if (idx >= chunks.length) return finalize();
        let insert;
        try {
            insert = buildBatchInsert(chunks[idx], cfg.target);
        } catch {
            allOk = false; idx += 1; return next();
        }
        post(cfg, insert.statement, insert.bindings, ok => {
            if (settled) return; // a POST that lands after the ceiling fired changes nothing
            if (!ok) {
                // Stop at the first failure. Retention is per-file, so a later flush
                // re-sends the whole batch anyway — sending more chunks now would just
                // duplicate the ones that land and keep the warehouse resumed longer.
                allOk = false;
                return finalize();
            }
            idx += 1; next();
        }, timeoutMs);
    };
    next();
}

// Request timeouts. A detached background writer can wait out a cold-warehouse
// resume (WEAVIX_AI_WH auto-suspends at 60s). A synchronous (CI/cloud) flush runs
// in the hook's own process, so each POST is capped short AND the whole flush is
// bounded by a total wall-clock budget (SYNC_FLUSH_BUDGET_MS) — so even a
// multi-chunk flush against a slow Snowflake returns before the 10s hook timeout
// rather than being killed mid-write.
const DETACHED_WRITE_TIMEOUT_MS = 30000;
const SYNC_WRITE_TIMEOUT_MS = 8000;
// Hard wall-clock ceiling for one in-process flush, across all its chunks —
// comfortably under the 10s hook timeout with headroom for process start, JWT
// signing, and TLS setup, so the hook is never killed mid-flush.
const SYNC_FLUSH_BUDGET_MS = 7000;

/**
 * POST one INSERT to the Snowflake SQL API. Any failure is swallowed so
 * telemetry never surfaces to the user or blocks the session. Calls
 * done(ok) — ok is true only on a 2xx — so the batch flusher knows whether it
 * may delete the spooled rows or must leave them to retry.
 */
function postStatement(cfg, statement, bindings, done, timeoutMs = DETACHED_WRITE_TIMEOUT_MS) {
    let jwt;
    try {
        const pem = readPrivateKey(cfg);
        jwt = buildJwt(pem, cfg.passphrase, cfg.account, cfg.user, Math.floor(Date.now() / 1000));
    } catch {
        return done(false);
    }

    const body = JSON.stringify({
        statement,
        timeout: 60,
        // Omit role entirely when unset → Snowflake uses the user's default role.
        ...(cfg.role ? { role: cfg.role } : {}),
        warehouse: cfg.warehouse,
        database: cfg.target.database,
        schema: cfg.target.schema,
        bindings,
    });

    let url;
    try {
        url = new URL(`https://${cfg.host}/api/v2/statements?requestId=${crypto.randomUUID()}`);
    } catch {
        return done(false);
    }

    const transport = url.protocol === 'http:' ? http : https;
    const req = transport.request(
        url,
        {
            method: 'POST',
            headers: {
                'Content-Type': 'application/json',
                'Accept': 'application/json',
                // The Snowflake SQL API rejects requests without a User-Agent (HTTP 400).
                'User-Agent': 'weavix-claude-telemetry/1.0',
                'Authorization': `Bearer ${jwt}`,
                'X-Snowflake-Authorization-Token-Type': 'KEYPAIR_JWT',
                'Content-Length': Buffer.byteLength(body),
            },
        },
        res => {
            const ok = res.statusCode >= 200 && res.statusCode < 300;
            res.resume();
            res.on('end', () => done(ok));
        },
    );
    // Bounded by the caller: the detached writer allows a full cold-start resume;
    // a synchronous CI write is capped short so it never outlives the hook timeout.
    req.setTimeout(timeoutMs, () => req.destroy());
    req.on('error', () => done(false));
    req.write(body);
    req.end();
}

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
 * unless it targets loopback (a local test server).
 * @param {NodeJS.ProcessEnv} env
 * @returns {{sasUrl: string}|null}
 */
function resolveBlobConfig(env) {
    let sasUrl = env.CLAUDE_TELEMETRY_SAS_URL;
    if (!sasUrl) {
        try { sasUrl = JSON.parse(fs.readFileSync(blobConfigFilePath(env), 'utf8')).sas_url; } catch { sasUrl = undefined; }
    }
    if (!sasUrl) return null;
    try {
        const u = new URL(String(sasUrl).trim());
        const loopback = u.protocol === 'http:' && ['127.0.0.1', 'localhost', '[::1]'].includes(u.hostname);
        if ((u.protocol !== 'https:' && !loopback) || !u.searchParams.get('sig')) return null;
        return { sasUrl: u.toString() };
    } catch {
        return null;
    }
}

/** Whether the legacy Snowflake sink should run for this config (off with CLAUDE_TELEMETRY_SNOWFLAKE=0). */
function snowflakeEnabled(cfg, env) {
    return cfg != null && env.CLAUDE_TELEMETRY_SNOWFLAKE !== '0';
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
 * PUT one parquet file as a block blob. done(true) on 201, and on 409 (the blob already exists
 * because an earlier attempt landed), so a retried batch is idempotent. Never throws.
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
            res.on('end', () => finish((status >= 200 && status < 300) || status === 409, status));
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
 * Hand the batch flush to a detached background process and return. The hook
 * (parent) exits immediately, so telemetry adds no latency to the session, and a
 * slow cold-warehouse resume can't be cut off by the hook timeout — the child
 * owns it. Only ever reached on the interactive (file-keyed) path, so no inline
 * key is ever serialized into the child's environment.
 */
function emitDetached(cfg, files, parquetFiles = []) {
    // The SAS is not serialized: the child re-resolves it from the same env / config file.
    const blob = Buffer.from(JSON.stringify({ cfg, files, parquetFiles })).toString('base64');
    const child = spawn(process.execPath, [__filename], {
        detached: true,
        stdio: 'ignore',
        env: { ...process.env, CLAUDE_SF_EMIT: blob },
    });
    child.unref();
}

/**
 * Flush both sinks' claimed batches in parallel and call done once both settle. A sink with no
 * config or no files is skipped. Failures are recorded in the local errors log.
 * @param {object|null} cfg snowflake config, or null when that sink is off
 * @param {string[]} files claimed snowflake batch files
 * @param {{sasUrl: string}|null} blob parquet sink config, or null when absent
 * @param {string[]} parquetFiles claimed parquet batch files
 * @param {{timeoutMs: number, budgetMs: number}} limits per-request timeout and whole-flush budget
 * @param {() => void} done
 */
function flushAll(cfg, files, blob, parquetFiles, limits, done) {
    const env = process.env;
    let pending = 0;
    const settle = () => { pending -= 1; if (pending === 0) done(); };
    if (cfg && files.length) {
        pending += 1;
        flushBatches(cfg, files, limits.timeoutMs, ok => {
            if (!ok) logFailure(env, 'snowflake', 'insert-failed');
            settle();
        }, postStatement, limits.budgetMs);
    }
    if (blob && parquetFiles.length) {
        pending += 1;
        flushParquetBatches(blob, parquetFiles, limits.timeoutMs, settle, {
            totalBudgetMs: limits.budgetMs,
            onError: reason => logFailure(env, 'parquet', reason),
        });
    }
    if (pending === 0) done();
}

function main() {
    // Child mode: the detached flusher spawned below. Send the claimed batches
    // and exit; it owns deleting them on success or leaving them to retry.
    if (process.env.CLAUDE_SF_EMIT) {
        let job;
        try {
            job = JSON.parse(Buffer.from(process.env.CLAUDE_SF_EMIT, 'base64').toString('utf8'));
        } catch {
            process.exit(0);
        }
        return flushAll(job.cfg, job.files || [], resolveBlobConfig(process.env), job.parquetFiles || [], {
            timeoutMs: DETACHED_WRITE_TIMEOUT_MS, budgetMs: Infinity,
        }, () => process.exit(0));
    }

    // Hook mode: build the event and record it, returning fast.
    const sfCfg = resolveConfig(process.env);
    const cfg = snowflakeEnabled(sfCfg, process.env) ? sfCfg : null;
    const blob = resolveBlobConfig(process.env);
    // No sink configured — nothing to do; succeed quietly.
    if (!cfg && !blob) process.exit(0);

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
            // Both paths batch: append the event to the machine-local spool, and
            // only when it is time flush the whole spool as one multi-row INSERT.
            // Most events just append and return in microseconds.
            const row = toRow(event);
            const sp = spoolPath(process.env, 'snowflake');
            const pp = spoolPath(process.env, 'parquet');
            if (cfg) appendSpool(sp, row);
            if (blob) appendSpool(pp, row);

            // The turn's tokens are now durably in the spool (its own retry owns delivery), so
            // advance the offset once here -- committing only after a deferred flush would let
            // the next Stop re-read and double-count the same turn. A row later dropped as a
            // poison batch loses those tokens; acceptable for best-effort telemetry.
            if (event._tokenCommit) {
                commitTokenOffset(event._tokenCommit.sessionId, event._tokenCommit.offset, event._tokenCommit.carry);
            }

            const sizeOf = p => { try { return fs.statSync(p).size; } catch { return 0; } };
            const flushSf = cfg && shouldFlush(sizeOf(sp), event.hookEvent);
            const flushPq = blob && shouldFlush(sizeOf(pp), event.hookEvent);
            if (!flushSf && !flushPq) process.exit(0);

            const now = Date.now();
            const files = flushSf ? claimBatches(sp, now, crypto.randomUUID()) : [];
            const parquetFiles = flushPq ? claimBatches(pp, now, crypto.randomUUID()) : [];
            if (!files.length && !parquetFiles.length) process.exit(0);

            if (shouldWriteSync(cfg, process.env)) {
                // CI / cloud / inline-key: flush in-process. A short-lived job would
                // reap a detached child before its POST landed, and an inline key
                // must never be serialized into a child's env — so the flush runs
                // here, bounded by SYNC_FLUSH_BUDGET_MS so it returns within the hook
                // timeout even across multiple chunks. A hard kill before/mid-flush
                // drops that (bounded) batch; acceptable for best-effort telemetry,
                // and still far fewer statements per job.
                return flushAll(cfg, files, blob, parquetFiles, {
                    timeoutMs: SYNC_WRITE_TIMEOUT_MS, budgetMs: SYNC_FLUSH_BUDGET_MS,
                }, () => process.exit(0));
            }

            // Interactive: hand the flush to a detached child so the session sees no
            // latency and a cold-warehouse resume can't be cut off by the hook timeout.
            emitDetached(cfg, files, parquetFiles);
        } catch {
            /* never let telemetry surface an error to the session */
        }
        process.exit(0);
    });
}

if (require.main === module) main();

module.exports = {
    buildEvent, toRow, buildInsert, buildBatchInsert, normalizeAccount, resolveConfig, resolveInlineKey,
    normalizePem, readPrivateKey, isNonInteractive, shouldWriteSync, configFilePath, COLUMNS, COLUMN_TYPES,
    spoolPath, appendSpool, parseSpool, shouldFlush, chunk, claimBatches, flushBatches,
    SPOOL_MAX_BYTES, MAX_ROWS_PER_INSERT, ORPHAN_RETRY_AFTER_MS, ORPHAN_DROP_AFTER_MS,
    repoFromRemote, resolveOrigin, turnTokens, initTokenOffset, commitTokenOffset,
    originCacheFile, cleanupSessionState, usageInt, MIN_ROW_WIDTH, STATE_RETENTION_MS,
    resolveBlobConfig, blobConfigFilePath, snowflakeEnabled, batchId, blobUrlFor, uploadBlob,
    flushParquetBatches, flushAll, logFailure, PARQUET_COLUMN_TYPES, PARQUET_ENTITY, MAX_ROWS_PER_PARQUET,
};

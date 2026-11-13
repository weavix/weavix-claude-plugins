'use strict';

/**
 * Tests for guard.cjs: opt-in enforcement, configured detection, bypass, and hook responses.
 * Run: node plugins/claude-telemetry/test/guard.test.cjs
 */

const assert = require('node:assert');
const os = require('node:os');
const path = require('node:path');
const { isConfigured, response } = require('../src/guard.cjs');

const NO_FILE = { CLAUDE_TELEMETRY_CONFIG: path.join(os.tmpdir(), 'cct-guard-none.json') };
const SAS = 'https://acct.blob.core.windows.net/parquet/claude?sp=c&sig=x';
let pass = 0;
const test = (name, fn) => {
    try { fn(); pass += 1; console.log(`  ok   - ${name}`); } catch (e) { console.error(`  FAIL - ${name}\n         ${e.message}`); process.exitCode = 1; }
};

test('guard is off unless the repo opts in', () => {
    assert.equal(isConfigured({ ...NO_FILE }), true);
});

test('opted in with no SAS -> not configured', () => {
    assert.equal(isConfigured({ ...NO_FILE, CLAUDE_TELEMETRY_REQUIRED: '1' }), false);
});

test('opted in with a SAS -> configured', () => {
    assert.equal(isConfigured({ ...NO_FILE, CLAUDE_TELEMETRY_REQUIRED: '1', CLAUDE_TELEMETRY_SAS_URL: SAS }), true);
});

test('a Snowflake connector alone no longer satisfies the guard', () => {
    assert.equal(isConfigured({
        ...NO_FILE, CLAUDE_TELEMETRY_REQUIRED: '1',
        SNOWFLAKE_ACCOUNT: 'a', SNOWFLAKE_USER: 'u', SNOWFLAKE_PRIVATE_KEY: 'k',
    }), false);
});

test('an expired upload URL does not satisfy the guard', () => {
    const expired = 'https://acct.blob.core.windows.net/claude-telemetry?se=2020-01-01T00%3A00%3A00Z&sp=c&sig=x';
    assert.equal(isConfigured({ ...NO_FILE, CLAUDE_TELEMETRY_REQUIRED: '1', CLAUDE_TELEMETRY_SAS_URL: expired }), false);
});

test('bypass vars let edits through', () => {
    assert.equal(isConfigured({ ...NO_FILE, CLAUDE_TELEMETRY_REQUIRED: '1', CLAUDE_TELEMETRY_GUARD_BYPASS: '1' }), true);
    assert.equal(isConfigured({ ...NO_FILE, CLAUDE_TELEMETRY_REQUIRED: '1', CLAUDE_SNOWFLAKE_GUARD_BYPASS: '1' }), true);
});

test('UserPromptSubmit gets guidance with the setup command, not a deny', () => {
    const r = response('UserPromptSubmit');
    assert.equal(r.hookSpecificOutput.hookEventName, 'UserPromptSubmit');
    assert.match(r.hookSpecificOutput.additionalContext, /scripts\/setup\.sh/);
    assert.equal(r.hookSpecificOutput.permissionDecision, undefined);
});

test('PreToolUse (or anything else) is denied with the setup command', () => {
    const r = response('PreToolUse');
    assert.equal(r.hookSpecificOutput.permissionDecision, 'deny');
    assert.match(r.hookSpecificOutput.permissionDecisionReason, /scripts\/setup\.sh/);
});

console.log(`\nPassed: ${pass}`);

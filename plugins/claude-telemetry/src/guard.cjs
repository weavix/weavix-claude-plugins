#!/usr/bin/env node
'use strict';

/**
 * guard.cjs — keep a repo in "setup-only" mode until Claude telemetry is configured.
 * Active only where CLAUDE_TELEMETRY_REQUIRED=1 (set in a repo's .claude/settings.json env block).
 *
 * Wired from hooks.json to two hooks:
 *   - UserPromptSubmit: when telemetry is missing, inject guidance (does NOT block the prompt)
 *     telling Claude to help run the setup script and to refuse codebase work.
 *   - PreToolUse (matcher Edit|Write|NotebookEdit): hard-deny file edits as a backstop.
 *
 * "Configured" means the upload SAS URL resolves (CLAUDE_TELEMETRY_SAS_URL or
 * ~/.claude/telemetry.json) and has not expired, read fresh each run so no relaunch is needed.
 * Escape hatch: CLAUDE_TELEMETRY_GUARD_BYPASS=1 (or the legacy CLAUDE_SNOWFLAKE_GUARD_BYPASS=1).
 *
 * Node built-ins only. Always exits 0.
 */

const path = require('path');
const { resolveBlobConfig } = require('./telemetry.cjs');

const SETUP = `bash "${path.join(__dirname, '..', 'scripts', 'setup.sh')}"`;

const PROMPT_GUIDANCE = [
    '[claude-telemetry] Claude usage telemetry is NOT configured, so this repo is in SETUP-ONLY mode.',
    'Do not work on the codebase: do not edit files, run builds or tests, commit, or make any change.',
    'Your only task is to configure telemetry. Run:',
    `  ${SETUP}`,
    'It reads the telemetry upload URL from Azure Key Vault (needs `az login`) and writes',
    '~/.claude/telemetry.json. No relaunch is needed once that file exists. If a URL is already',
    'configured it has expired; re-running the script picks up the rotated one.',
    'If it reports no vault access, tell the user to ask DevOps for read access to the telemetry secret.',
    'If the user asks for codebase work, decline and offer to run the setup instead.',
].join('\n');

const DENY_REASON = [
    'Repo edits are blocked until Claude usage telemetry is configured (setup-only mode).',
    'A configured upload URL that has expired counts as not configured.',
    '',
    'To configure it (you can still chat while you do this):',
    `  ${SETUP}`,
    'It writes ~/.claude/telemetry.json; no relaunch needed.',
    'CI / outage bypass: set CLAUDE_TELEMETRY_GUARD_BYPASS=1.',
].join('\n');

/**
 * Whether edits may proceed: the guard is off, bypassed, or an unexpired upload URL is configured.
 * @param {NodeJS.ProcessEnv} env
 * @returns {boolean}
 */
function isConfigured(env) {
    if (env.CLAUDE_TELEMETRY_REQUIRED !== '1') return true;
    if (env.CLAUDE_TELEMETRY_GUARD_BYPASS === '1' || env.CLAUDE_SNOWFLAKE_GUARD_BYPASS === '1') return true;
    const blob = resolveBlobConfig(env);
    return blob != null && !blob.expired;
}

/**
 * The hook response for an unconfigured repo: guidance on UserPromptSubmit, a deny otherwise.
 * @param {string} event hook_event_name from the payload
 */
function response(event) {
    if (event === 'UserPromptSubmit') {
        return { hookSpecificOutput: { hookEventName: 'UserPromptSubmit', additionalContext: PROMPT_GUIDANCE } };
    }
    return {
        hookSpecificOutput: {
            hookEventName: 'PreToolUse',
            permissionDecision: 'deny',
            permissionDecisionReason: DENY_REASON,
        },
    };
}

function main() {
    if (isConfigured(process.env)) process.exit(0);
    let input = '';
    process.stdin.setEncoding('utf8');
    process.stdin.on('data', c => { input += c; });
    process.stdin.on('end', () => {
        let event = 'PreToolUse';
        try { event = JSON.parse(input || '{}').hook_event_name || 'PreToolUse'; } catch { /* default deny */ }
        process.stdout.write(JSON.stringify(response(event)));
        process.exit(0);
    });
}

if (require.main === module) main();

module.exports = { isConfigured, response };

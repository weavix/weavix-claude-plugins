# weavix Claude Code plugins

Claude Code plugin marketplace for weavix. Add it once and every Claude Code session — any repo, laptop, CI job or cloud agent — reports usage the same way.

## Plugins

| Plugin | What it does |
| --- | --- |
| `claude-telemetry` | Reports Claude Code usage metadata (sessions, prompts, turns, tokens, model, subagents, skills) as parquet to blob storage. Snowpipe loads it into `WEAVIX_AI.TELEMETRY.CLAUDE_CODE_EVENTS`. Never captures prompt or response text. |

## Enabling it

In a repo, add to `.claude/settings.json`:

```json
{
  "extraKnownMarketplaces": {
    "weavix": { "source": { "source": "github", "repo": "weavix/weavix-claude-plugins" } }
  },
  "enabledPlugins": { "claude-telemetry@weavix": true },
  "env": { "CLAUDE_TELEMETRY_REQUIRED": "1" }
}
```

`CLAUDE_TELEMETRY_REQUIRED=1` turns on the guard: file edits are blocked until telemetry is configured. Leave it out where you only want reporting.

For every session on a machine regardless of repo, put the same `extraKnownMarketplaces` and `enabledPlugins` in `~/.claude/settings.json`.

## Configuring a machine

```sh
bash "<plugin root>/scripts/setup.sh"
```

When the guard is on, Claude prints the exact command. It reads the upload URL from Key Vault (`wv-prod-secrets` / `CLAUDE-TELEMETRY-SAS-URL`, needs `az login` and read access) and writes `~/.claude/telemetry.json` (mode 0600).

CI and cloud agents set the URL as a secret env var instead:

```yaml
env:
  CLAUDE_TELEMETRY_SAS_URL: ${{ secrets.CLAUDE_TELEMETRY_SAS_URL }}
  CLAUDE_AI_TELEMETRY_ENV: ci
```

CI is detected automatically and flushes in-process before the job exits.

## How claude-telemetry works

- **Capture:** each hook event becomes one metadata row, appended to a spool file under the system temp dir.
- **Flush:** on `SessionEnd`, or when the spool passes 128 KB, the batch is encoded as parquet and uploaded to `<sas path>/claudecodeevents/<batch id>-<part>.parquet`.
- **Credentials:** the SAS is create-only, scoped to the `claude/` folder of the `parquet` container. It cannot read, list, overwrite or delete.
- **Retries:** a failed upload keeps its batch, which is retried after 60 s and dropped after 24 h. Blob names are fixed per batch and uploads use `If-None-Match: *`, so a retry never duplicates rows.
- **Errors:** failures are written to `errors.log` beside the spool. That log holds the sink and HTTP status only, never the URL.
- **Ingest:** `parquet-compactor` merges `claude/` into `snowflake-infeed/claude/`, and the Snowpipe in the data repo (`migrations/ai/telemetry`) loads it.

### Settings

| Variable | Purpose |
| --- | --- |
| `CLAUDE_TELEMETRY_SAS_URL` | Upload URL; overrides `sas_url` in `~/.claude/telemetry.json` |
| `CLAUDE_TELEMETRY_CONFIG` | Alternate path for the config file |
| `CLAUDE_TELEMETRY_REQUIRED` | `1` turns on the edit guard |
| `CLAUDE_TELEMETRY_GUARD_BYPASS` | `1` lets edits through without telemetry (outages) |
| `CLAUDE_AI_TELEMETRY_ENV` | `ENVIRONMENT` column value (default `$TARGET` or `local`) |
| `CLAUDE_AI_TELEMETRY_ACTOR` | `CLIENT_USER` override (default `$GITHUB_ACTOR` or `$USER`) |
| `CLAUDE_SF_SYNC` | `1` flush in-process, `0` flush in a detached child (default: in-process under CI) |
| `CLAUDE_TELEMETRY_SNOWFLAKE` | `0` turns off the legacy direct-to-Snowflake writer |

The legacy writer runs alongside parquet only on machines that still have a Snowflake connector (`~/.snowflake/claude-telemetry.json`), until the pipe is proven.

## Developing

```sh
npm ci
npm test
npm run build   # after changing src/parquet-writer.js; commit dist/
```

`dist/parquet-writer.cjs` is a committed esbuild bundle of `@dsnp/parquetjs`, so the plugin runs without an install. CI fails if it is stale.

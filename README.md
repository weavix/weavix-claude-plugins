# weavix Claude Code plugins

Claude Code plugin marketplace for weavix. Add it once and every Claude Code session — any repo, laptop, CI job or cloud agent — reports usage the same way.

## Plugins

| Plugin | What it does |
| --- | --- |
| `claude-telemetry` | Reports Claude Code usage metadata (sessions, prompts, turns, tokens, model, subagents, skills) as parquet to the `claude-telemetry` blob container. Snowpipe loads it into `WEAVIX_AI.TELEMETRY.CLAUDE_CODE_EVENTS`. Never captures prompt or response text. |

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

CI and cloud agents set the URL as a secret env var instead. This repo is internal, so a job's own `GITHUB_TOKEN` cannot clone it; point git at a read-only token first:

```yaml
- run: git config --global url."https://x-access-token:${{ secrets.CLAUDE_PLUGINS_READ_TOKEN }}@github.com/weavix/weavix-claude-plugins".insteadOf "https://github.com/weavix/weavix-claude-plugins"
- uses: anthropics/claude-code-action@v1
  env:
    CLAUDE_TELEMETRY_SAS_URL: ${{ secrets.CLAUDE_TELEMETRY_SAS_URL }}
    CLAUDE_AI_TELEMETRY_ENV: ci
  with:
    plugin_marketplaces: https://github.com/weavix/weavix-claude-plugins.git
    plugins: claude-telemetry@weavix
```

CI is detected automatically and flushes in-process before the job exits.

## How claude-telemetry works

- **Capture:** each hook event becomes one metadata row, appended to a spool file under the system temp dir.
- **Flush:** on `SessionEnd`, or when the spool passes 128 KB, the batch is encoded as parquet and uploaded to `<sas path>/claudecodeevents/<batch id>-<part>.parquet`.
- **Credentials:** the SAS is container-scoped, create-only (`sp=c`) on the dedicated `claude-telemetry` container in `weavixdataplatformprodsa`. It cannot read, list, overwrite or delete, and cannot reach the `parquet` container other pipes ingest. A SAS past its `se` expiry counts as unconfigured: the guard blocks and uploads pause until setup is re-run.
- **Retries:** a failed upload keeps its batch, which is retried after 60 s and dropped after 24 h. Blob names are fixed per batch and uploads use `If-None-Match: *`, so a retry never duplicates rows.
- **Errors:** failures are written to `errors.log` beside the spool. That log holds the HTTP status or reason only, never the URL.
- **Ingest:** Snowpipe `TELEMETRY.CLAUDE_CODE_EVENTS_PIPE` loads straight from the container, defined in the data repo under `migrations/ai/telemetry`. There is no compactor in between.

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

## Rotating the upload SAS (DevOps)

The SAS is signed with the storage account key, so rotating that key revokes it. Issue one for a year:

```sh
az storage container generate-sas --account-name weavixdataplatformprodsa --name claude-telemetry \
  --permissions c --expiry "$(date -u -v+1y +%Y-%m-%dT%H:%MZ)" --https-only --auth-mode key -o tsv
```

Store `https://weavixdataplatformprodsa.blob.core.windows.net/claude-telemetry?<sas>` as `CLAUDE-TELEMETRY-SAS-URL` in `wv-prod-secrets`, and as the `CLAUDE_TELEMETRY_SAS_URL` GitHub secret. Machines pick it up the next time they run `setup.sh`.

## Developing

```sh
npm ci
npm test
npm run build   # after changing src/parquet-writer.js; commit dist/
```

`dist/parquet-writer.cjs` is a committed esbuild bundle of `@dsnp/parquetjs`, so the plugin runs without an install. CI fails if it is stale.

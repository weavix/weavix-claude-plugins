#!/usr/bin/env bash
# Configures Claude usage telemetry on this machine: reads the create-only upload SAS URL from
# Azure Key Vault and writes it to ~/.claude/telemetry.json (mode 0600). Never prints the secret.
# Override the source with CLAUDE_TELEMETRY_VAULT / CLAUDE_TELEMETRY_SECRET.
set -euo pipefail

VAULT="${CLAUDE_TELEMETRY_VAULT:-wv-prod-secrets}"
SECRET="${CLAUDE_TELEMETRY_SECRET:-CLAUDE-TELEMETRY-SAS-URL}"
TARGET="${CLAUDE_TELEMETRY_CONFIG:-$HOME/.claude/telemetry.json}"

if ! command -v az >/dev/null 2>&1; then
  echo "Azure CLI (az) is required: https://learn.microsoft.com/cli/azure/install-azure-cli" >&2
  exit 1
fi
if ! az account show >/dev/null 2>&1; then
  echo "Not signed in to Azure. Run: az login" >&2
  exit 1
fi

if ! SAS_URL="$(az keyvault secret show --vault-name "$VAULT" --name "$SECRET" --query value -o tsv 2>/dev/null)"; then
  echo "Could not read secret $SECRET from vault $VAULT." >&2
  echo "Ask DevOps for Key Vault Secrets User on that secret, then re-run this script." >&2
  exit 1
fi

case "$SAS_URL" in
  https://*sig=*) ;;
  *) echo "Secret $SECRET in $VAULT is not a signed https URL; ask DevOps to check it." >&2; exit 1 ;;
esac

mkdir -p "$(dirname "$TARGET")"
umask 077
SAS_URL="$SAS_URL" node -e 'require("fs").writeFileSync(process.argv[1], JSON.stringify({ sas_url: process.env.SAS_URL }, null, 2) + "\n", { mode: 0o600 })' "$TARGET"
chmod 600 "$TARGET"
echo "Claude telemetry configured: wrote $TARGET. No relaunch needed."

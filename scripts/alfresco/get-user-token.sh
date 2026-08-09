#!/usr/bin/env bash
# Mint an Alfresco *user* OAuth2 token via Keycloak (identity-service) password grant.
# Runs in git bash (Windows), Linux, or macOS. Needs curl.
#
# Yields a real user's token (display name + ACLs), unlike client_credentials which the
# Alfresco source self-fetches and which returns the service account.
#
# Prereq: the 'flexible-graphrag' client must have Direct Access Grants (password) enabled.
#
# Usage:  KC_CLIENT_SECRET=your-secret ./scripts/alfresco/get-user-token.sh
set -euo pipefail

KC_BASE="${KC_BASE:-http://localhost:8091}"
REALM="${KC_REALM:-alfresco}"
CLIENT_ID="${KC_CLIENT_ID:-flexible-graphrag}"
CLIENT_SECRET="${KC_CLIENT_SECRET:-flexible-graphrag-secret}"
USERNAME="${KC_USERNAME:-admin}"
PASSWORD="${KC_PASSWORD:-admin}"
ENDPOINT="$KC_BASE/realms/$REALM/protocol/openid-connect/token"

[ -z "$CLIENT_SECRET" ] && echo "[!] KC_CLIENT_SECRET is empty; the confidential client will likely reject this (invalid_client)." >&2

echo "POST $ENDPOINT (user=$USERNAME, client=$CLIENT_ID)"
args=(-s -X POST "$ENDPOINT"
  --data-urlencode "grant_type=password"
  --data-urlencode "client_id=$CLIENT_ID"
  --data-urlencode "username=$USERNAME"
  --data-urlencode "password=$PASSWORD")
[ -n "$CLIENT_SECRET" ] && args+=(--data-urlencode "client_secret=$CLIENT_SECRET")

# Raw JSON: copy access_token/refresh_token from the output.
# To extract just the access_token, append:  | python -c "import sys,json;print(json.load(sys.stdin)['access_token'])"
curl "${args[@]}"
echo

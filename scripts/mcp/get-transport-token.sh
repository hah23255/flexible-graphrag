#!/usr/bin/env bash
# Mint a bearer token for the MCP *transport* auth layer (FastMCP JWTVerifier), client_credentials.
# Runs in git bash (Windows), Linux, or macOS. Needs curl.
#
# Outer OAuth2 layer: gates WHO may call the MCP server (distinct from data-source auth).
# Start the server with MCP_TRANSPORT_AUTH=true; send the token as Authorization: Bearer <token>.
#
# Usage:  ./scripts/mcp/get-transport-token.sh
set -euo pipefail

KC_BASE="${KC_BASE:-http://localhost:8091}"
REALM="${KC_REALM:-alfresco}"
CLIENT_ID="${KC_CLIENT_ID:-flexible-graphrag}"
CLIENT_SECRET="${KC_CLIENT_SECRET:-flexible-graphrag-secret}"
ENDPOINT="$KC_BASE/realms/$REALM/protocol/openid-connect/token"

echo "POST $ENDPOINT (grant=client_credentials, client=$CLIENT_ID)"
# Raw JSON: copy access_token from the output.
# To extract just the token, append:  | python -c "import sys,json;print(json.load(sys.stdin)['access_token'])"
curl -s -X POST "$ENDPOINT" \
  --data-urlencode "grant_type=client_credentials" \
  --data-urlencode "client_id=$CLIENT_ID" \
  --data-urlencode "client_secret=$CLIENT_SECRET"
echo

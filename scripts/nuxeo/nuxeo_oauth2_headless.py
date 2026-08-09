"""Headless Nuxeo OAuth2 (auth-code + PKCE) token minter.

The `flexible-graphrag` client is registered with autoGrant=true, so with Basic auth
on /oauth2/authorize Nuxeo should 302 straight to the callback with ?code=... — no
browser/consent needed. Prints access_token/refresh_token to paste into the UI.
"""
import os, sys
# Allow importing the flexible-graphrag backend package (sources.*) from scripts/nuxeo/
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "..", "flexible-graphrag"))

import httpx
from nuxeo.client import Nuxeo
from nuxeo.auth import OAuth2

HOST = "http://localhost:8081/nuxeo/"
CID = "flexible-graphrag"
SECRET = None
REDIR = "http://localhost:8888/callback"
USER, PW = "Administrator", "Administrator"

nx = Nuxeo(host=HOST)
auth = OAuth2(nx.client.host, client_id=CID, client_secret=SECRET, redirect_uri=REDIR)
uri, state, verifier = auth.create_authorization_url()
print("AUTHORIZE URL:\n", uri, "\n")

with httpx.Client(follow_redirects=False, verify=False, timeout=30) as c:
    r = c.get(uri, auth=(USER, PW))
    print("authorize status:", r.status_code)
    loc = r.headers.get("location", "")
    print("redirect location:", loc[:200])
    # Follow one hop if it bounced somewhere other than the callback
    hops = 0
    while loc and REDIR not in loc and r.status_code in (301, 302, 303, 307, 308) and hops < 3:
        r = c.get(loc, auth=(USER, PW))
        loc = r.headers.get("location", "")
        print(f"  hop {hops}: {r.status_code} -> {loc[:160]}")
        hops += 1

if REDIR not in loc:
    print("\n[!] Did not land on the callback. Response body head:")
    print(r.text[:600])
    raise SystemExit("Could not obtain an authorization code headlessly.")

callback_url = loc if loc.startswith("http") else (REDIR + loc[loc.find("?"):])
print("\nCALLBACK:", callback_url)

token = auth.request_token(code_verifier=verifier, authorization_response=callback_url, state=state)
print("\n=== TOKEN ===")
print("access_token :", token.get("access_token"))
print("refresh_token:", token.get("refresh_token"))
print("expires_in   :", token.get("expires_in"))

# Smoke test through the actual source
from sources.nuxeo import NuxeoSource  # noqa: E402
src = NuxeoSource({
    "url": "http://localhost:8081/nuxeo",
    "auth_method": "oauth2",
    "oauth2": {
        "client_id": CID, "client_secret": SECRET,
        "access_token": token.get("access_token"),
        "refresh_token": token.get("refresh_token"),
        "expires_in": token.get("expires_in"),
    },
    "path": "/default-domain",
})
print("validate_config:", src.validate_config())
doc = src.nuxeo.documents.get(path="/")
print("ROOT DOC via OAuth2 Bearer:", doc.uid, doc.type)

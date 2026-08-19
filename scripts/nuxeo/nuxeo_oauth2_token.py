"""
Helper: obtain a Nuxeo OAuth2 token via the authorization-code + PKCE flow,
so you can test flexible-graphrag's Nuxeo source with auth_method="oauth2".

PREREQUISITE — register an OAuth2 client in Nuxeo first:
  1. Open the Nuxeo Admin Center: http://localhost:8081/nuxeo/ui/#!/admin
     -> Cloud Services -> Consumers (OAuth2 clients)  [Add]
  2. Set:
       Name:          flexible-graphrag
       Client ID:     flexible-graphrag
       Client Secret: (optional; leave blank for a public PKCE client)
       Redirect URIs: http://localhost:8888/callback
       Enabled:       yes  ("Auto-grant" optional, skips the consent screen)
  3. Fill CLIENT_ID / CLIENT_SECRET below to match.

Run (from repo root):  venv-3.14/Scripts/python scripts/nuxeo/nuxeo_oauth2_token.py
It prints an authorization URL -> Ctrl+Click it to open (don't right-click — in a terminal
that pastes into the prompt). The browser redirects to the callback URL and shows a 404 /
'page not found' — that's EXPECTED (nothing serves that port); copy that FULL redirected URL
from the address bar (it has ?code=...) and paste it back at the prompt. The script prints
access_token / refresh_token. (Log in only if Nuxeo prompts; with an active session / autoGrant
it redirects immediately.)

Then in the UI (Nuxeo source, Authentication = OAuth2):
  Client ID     = CLIENT_ID
  Client Secret = CLIENT_SECRET (if you set one)
  Access Token  = <printed access_token>
  Refresh Token = <printed refresh_token>
"""

import os, sys
# Allow importing the flexible-graphrag backend package (sources.*) from scripts/nuxeo/
_BACKEND_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "flexible-graphrag")
sys.path.insert(0, _BACKEND_DIR)

# Read the backend's .env so NUXEO_PATH (and anything else) matches what the app
# uses.  pydantic's Settings resolves env_file relative to the working directory,
# and this script does not run from the backend directory.
try:
    from dotenv import load_dotenv

    load_dotenv(os.path.join(_BACKEND_DIR, ".env"), override=False)
except ImportError:
    pass

# MUST come before `import nuxeo...` — see the note in nuxeo_oauth2_headless.py.
# nuxeo/auth/oauth2.py imports the GehirnInc `jwt` API at module load; we ship
# PyJWT, and sources.nuxeo installs the compatibility layer on import.
import sources.nuxeo as _fg_nuxeo  # noqa: F401,E402  (import for its side effect)

from nuxeo.client import Nuxeo  # noqa: E402
from nuxeo.auth import OAuth2  # noqa: E402

HOST = "http://localhost:8081/nuxeo/"
CLIENT_ID = "flexible-graphrag"
CLIENT_SECRET = None  # or "your-secret"
REDIRECT_URI = "http://localhost:8888/callback"

nuxeo = Nuxeo(host=HOST)
auth = OAuth2(
    nuxeo.client.host,
    client_id=CLIENT_ID,
    client_secret=CLIENT_SECRET,
    redirect_uri=REDIRECT_URI,
)
nuxeo.client.auth = auth

uri, state, code_verifier = auth.create_authorization_url()
print("\n1) Ctrl+Click the URL below to open it (do NOT right-click — in a terminal that")
print("   pastes your clipboard into the prompt):\n")
print(uri)
print("\n2) The browser redirects to", REDIRECT_URI, "and shows a 404 / 'page not found' —")
print("   that's EXPECTED (nothing serves that port). Log in only if Nuxeo prompts you.")
print("\n3) Copy the FULL redirected URL from the browser address bar (it has ?code=...),")
print("   then paste it below (right-click or Ctrl+V paste is fine HERE, at the prompt):")
authorization_response = input("> ").strip()

token = auth.request_token(
    code_verifier=code_verifier,
    authorization_response=authorization_response,
    state=state,
)
print("\n=== TOKEN ===")
print("access_token :", token.get("access_token"))
print("refresh_token:", token.get("refresh_token"))
print("expires_in   :", token.get("expires_in"))

# Smoke test: list the sample folder with the freshly obtained token
from sources.nuxeo import NuxeoSource  # noqa: E402
src = NuxeoSource({
    "url": "http://localhost:8081/nuxeo",
    "auth_method": "oauth2",
    "oauth2": {
        "client_id": CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "access_token": token.get("access_token"),
        "refresh_token": token.get("refresh_token"),
        "expires_in": token.get("expires_in"),
    },
    # Whatever workspace you actually have.  Reads NUXEO_PATH from the
    # environment so the smoke test follows the backend's own configuration
    # instead of a hard-coded folder that only existed on one machine.
    "path": os.getenv("NUXEO_PATH") or "/default-domain/workspaces",
    "recursive": True,
})
_probe_path = os.getenv("NUXEO_PATH") or "/default-domain/workspaces"
files = src.list_files()
print(f"\noauth2 list_files on {_probe_path} FOUND:", len(files), [f["name"] for f in files])
if not files:
    print("  (no documents there — set NUXEO_PATH to a workspace that has files, "
          "e.g. NUXEO_PATH=/default-domain/workspaces/GraphRAG)")

# Family Tree

A web application for creating and browsing family trees, built with **FastAPI** (backend) and **Next.js** (frontend).

## Architecture

- **Backend**: FastAPI (Python) — REST API, graph management, OAuth authentication
- **Frontend**: Next.js (React/TypeScript) — interactive graph explorer with Cytoscape.js
- **Storage**: Local GML files or Azure Blob Storage
- **Auth**: Microsoft Entra External ID (optional)

## Quick Start (Local Development)

### Backend

```powershell
cd backend
pip install -r requirements.txt

# Required
$env:TREE_BACKEND = "local"              # or "azstorage"
$env:TREE_LOCAL_FILE = "familytree.gml"  # path to local GML file

# If using Azure Storage backend
$env:AZURE_STORAGE_ACCOUNT = "<account-name>"
$env:AZURE_STORAGE_KEY = "<account-key>"
$env:AZURE_STORAGE_CONTAINER = "familytreejson"
$env:AZURE_STORAGE_BLOB = "familytree.gml"
$env:AZURE_STORAGE_PICS_CONTAINER = "familytreepics"

$env:CORS_ORIGINS = "http://localhost:3000"

python -m uvicorn backend.app.main:app --host 0.0.0.0 --port 8000 --reload
```

### Frontend

```powershell
cd frontend
npm install

$env:NEXT_PUBLIC_API_URL = "http://localhost:8000"

npm run dev
```

Access at `http://localhost:3000`. API docs at `http://localhost:8000/docs`.

## Environment Variables Reference

### Backend

| Variable | Required | Description | Default |
|----------|----------|-------------|---------|
| `TREE_BACKEND` | No | Storage backend: `local` or `azstorage` | `local` |
| `TREE_LOCAL_FILE` | For local | Path to GML file | `familytree.gml` |
| `CORS_ORIGINS` | No | Allowed CORS origins (comma-separated) | `http://localhost:3000` |
| **Azure Storage** | | | |
| `AZURE_STORAGE_ACCOUNT` | For azstorage | Storage account name | — |
| `AZURE_STORAGE_KEY` | For azstorage | Storage account key | — |
| `AZURE_STORAGE_CONTAINER` | For azstorage | Container for GML file | `familytreejson` |
| `AZURE_STORAGE_BLOB` | For azstorage | Blob name for GML file | `familytree.gml` |
| `AZURE_STORAGE_PICS_CONTAINER` | For images | Container for profile pictures | `familytreepics` |
| **Authentication** | | | |
| `AZURE_AD_TENANT_ID` | For auth | Entra External ID tenant ID | — (dev mode if unset) |
| `AZURE_AD_CLIENT_ID` | For auth | App registration client ID | — |
| `AZURE_AD_CLIENT_SECRET` | For auth | App registration client secret | — |
| `AZURE_AD_TENANT_NAME` | For auth | CIAM tenant subdomain (e.g. `erjosito`) | — |
| `OAUTH_REDIRECT_URI` | For auth | OAuth callback URL | `http://localhost:8000/api/auth/callback` |
| `GOOGLE_CLIENT_ID` | For Google auth | Google OAuth client ID | — |
| `GOOGLE_CLIENT_SECRET` | For Google auth | Google OAuth client secret | — |
| `SESSION_SECRET` | For auth | Secret key for signing session cookies | Auto-generated |

> **Dev mode**: When `AZURE_AD_TENANT_ID` is not set, the backend skips authentication and uses a default dev user.

### Frontend

| Variable | Required | Description | Default |
|----------|----------|-------------|---------|
| `NEXT_PUBLIC_API_URL` | No | Backend API URL | `http://localhost:8000` |
| `NEXT_PUBLIC_AZURE_AD_CLIENT_ID` | For auth | App registration client ID | — (dev mode if unset) |
| `NEXT_PUBLIC_AZURE_AD_TENANT_ID` | For auth | Entra External ID tenant ID | — |
| `NEXT_PUBLIC_AZURE_AD_AUTHORITY` | For auth | OIDC authority URL | Derived from tenant ID |

> **Dev mode**: When `NEXT_PUBLIC_AZURE_AD_CLIENT_ID` is not set, the frontend skips the login gate and shows the app directly with a "Dev Mode" badge.

## Authentication Setup

Authentication uses **backend-proxied OAuth** — the backend handles the full authorization-code flow with Entra External ID and issues a signed session cookie.

### Flow

1. Frontend checks `GET /api/auth/session` — no cookie → shows login page
2. User clicks "Sign in with Microsoft" → navigates to `/api/auth/login`
3. Backend redirects to Entra External ID (confidential client flow)
4. Entra redirects back to `/api/auth/callback`
5. Backend exchanges code for tokens, sets session cookie, redirects to frontend
6. Frontend reads cookie via `/api/auth/session` → user is authenticated

### Entra Configuration

1. Create an app registration in your Entra External ID tenant
2. Set **Supported account types** to "Accounts in any organizational directory and personal Microsoft accounts"
3. Under **Authentication** → add a **Web** redirect URI: `http://localhost:8000/api/auth/callback`
4. Under **Certificates & secrets** → create a client secret
5. Configure the environment variables listed above

See `docs/auth-setup.md` for detailed Entra External ID setup instructions.

## Shareable graph views

The graph page stores its view in optional URL query parameters, so copying the
browser URL preserves the current graph:

| Parameter | Meaning | Default |
| --- | --- | --- |
| `center` | Person ID at the center of the graph | Entire tree |
| `radius` | Number of relationship steps to show (`1`-`10`) | `2` |
| `layout` | Graph layout (`family`, `breadthfirst`, `concentric`, `cose`, `grid`, or `circle`) | `family` |
| `person` | Person ID whose details are open | None |

Default and invalid values are omitted when the URL is normalized. Browser
Back and Forward restore graph navigation without affecting the separate story
and person profile routes.

## CLI Tool

A command-line interface for managing the tree directly:

```powershell
python cli.py --help
python cli.py list                                    # List all persons
python cli.py show "Alba Farell Torres"               # Show person details
python cli.py add --firstname Ana --lastname Garcia   # Add a person
python cli.py tree "Alba Farell Torres" --degree 3    # Show tree levels
python cli.py info                                    # Tree statistics
```

Use `--backend azstorage` to work with Azure Storage, or `--file path.gml` for local files.

## Docker

```powershell
# Development (separate containers)
docker-compose up --build

# Production (single container with static frontend)
docker build -f Dockerfile.prod -t familytree .
docker run -p 8000:8000 -e CORS_ORIGINS=http://localhost:8000 familytree
```

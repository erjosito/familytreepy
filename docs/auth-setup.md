# Authentication Setup

## Overview

The Family Tree app uses **Microsoft Entra External ID** (formerly Azure AD B2C / CIAM) 
for authentication. Users can sign in with their Microsoft accounts (@outlook.com, 
@hotmail.com, etc.) and optionally with Google accounts.

## Environment Variables

### Frontend (Next.js)

| Variable | Required | Description |
|---|---|---|
| `NEXT_PUBLIC_AZURE_AD_CLIENT_ID` | Yes | App registration client ID |
| `NEXT_PUBLIC_AZURE_AD_TENANT_ID` | Yes | Entra External ID tenant ID |
| `NEXT_PUBLIC_AZURE_AD_AUTHORITY` | Yes | Authority URL (see below) |

**Authority URL format for Entra External ID:**
```
https://<tenant-subdomain>.ciamlogin.com/<tenant-id>
```

Example:
```
NEXT_PUBLIC_AZURE_AD_CLIENT_ID=12345678-abcd-1234-abcd-123456789abc
NEXT_PUBLIC_AZURE_AD_TENANT_ID=87654321-dcba-4321-dcba-987654321fed
NEXT_PUBLIC_AZURE_AD_AUTHORITY=https://familytree.ciamlogin.com/87654321-dcba-4321-dcba-987654321fed
```

**Dev mode:** Omit `NEXT_PUBLIC_AZURE_AD_CLIENT_ID` to bypass auth entirely.

### Backend (FastAPI) — optional

The backend auth (JWT validation) is available but not required when the API 
is served behind the frontend in a single container. Set these only if you 
expose the API separately:

| Variable | Description |
|---|---|
| `AZURE_AD_TENANT_ID` | Entra External ID tenant ID |
| `AZURE_AD_CLIENT_ID` | App registration client ID |

---

## Entra External ID Setup

### 1. Create the External ID Tenant

1. Go to [Azure Portal](https://portal.azure.com) → **Microsoft Entra External ID**
2. Click **Create a new tenant** → choose **Customer** tenant type
3. Pick a tenant subdomain (e.g., `familytree`) → this gives you `familytree.ciamlogin.com`

### 2. Register the App

1. In the External ID tenant → **App registrations** → **New registration**
2. Name: `Family Tree`
3. Supported account types: **Accounts in this organizational directory only**
4. Redirect URI: **Single-page application (SPA)**
   - Dev: `http://localhost:3000`
   - Prod: `https://your-app.azurewebsites.net`
5. After creation, note the **Application (client) ID** and **Directory (tenant) ID**

### 3. Configure Authentication

1. Go to the app → **Authentication**
2. Under **Single-page application**, ensure redirect URIs include both dev and prod URLs
3. Enable **ID tokens** and **Access tokens** under Implicit grant
4. Set **Supported account types** to the desired scope

### 4. Enable Microsoft Account Sign-in

Microsoft accounts (@outlook.com, @hotmail.com, @live.com) are supported by default 
in External ID tenants. Verify under:

**External ID tenant** → **External Identities** → **All identity providers**

You should see **Microsoft Account** listed. If not:
1. Click **+ Add identity provider**
2. Select **Microsoft Account**
3. Follow the prompts (uses the built-in Microsoft configuration)

---

## Adding Google as an Identity Provider

### Step 1: Create Google OAuth Credentials

1. Go to [Google Cloud Console](https://console.cloud.google.com/)
2. Create a project (or select an existing one)
3. Navigate to **APIs & Services** → **Credentials**
4. Click **+ Create Credentials** → **OAuth client ID**
5. Application type: **Web application**
6. Name: `Family Tree - Entra External ID`
7. **Authorized redirect URIs** — add:
   ```
   https://<tenant-subdomain>.ciamlogin.com/<tenant-id>/federation/oauth2
   ```
   Example:
   ```
   https://familytree.ciamlogin.com/87654321-dcba-4321-dcba-987654321fed/federation/oauth2
   ```
8. Click **Create** and note the **Client ID** and **Client Secret**

### Step 2: Configure Google in Entra External ID

1. Go to [Azure Portal](https://portal.azure.com) → your **External ID tenant**
2. Navigate to **External Identities** → **All identity providers**
3. Click **+ Add identity provider**
4. Select **Google**
5. Enter the **Client ID** and **Client Secret** from Step 1
6. Click **Save**

### Step 3: Add Google to User Flows

1. In the External ID tenant → **External Identities** → **User flows**
2. Select your sign-up/sign-in flow (or create one)
3. Under **Identity providers**, check **Google**
4. Save the flow

### Step 4: Verify

1. Open your app and click **Sign in**
2. The login page should now show both **Microsoft** and **Google** options
3. Users can sign in with their Google accounts (@gmail.com, Google Workspace)

---

## Troubleshooting

| Issue | Solution |
|---|---|
| Login page shows only Microsoft | Ensure Google IdP is added to the user flow |
| Redirect error after login | Check redirect URIs match exactly (including trailing slashes) |
| "AADSTS50011" error | Add the correct redirect URI in App registration → Authentication |
| CORS errors in dev | Set `CORS_ORIGINS=http://localhost:3000` in backend env |
| Auth bypassed unexpectedly | Ensure `NEXT_PUBLIC_AZURE_AD_CLIENT_ID` is set |

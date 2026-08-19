# GitHub Actions deployment setup

The workflow in `.github/workflows/deploy-container-app.yml` deploys the
production container to the existing `familytreeapp-app` Azure Container App.
The setup script and workflow pass the subscription explicitly and do not
change the Azure CLI default configured on a developer machine. Each workflow
run authenticates directly to subscription
`3e78e84b-6750-44b9-9d57-d9bba935237a` through GitHub OpenID Connect (OIDC).

## One-time Azure setup

1. Open [Azure Cloud Shell](https://shell.azure.com) in tenant
   `ecd38d6d-544b-494c-9b29-ff3d6a31c040`.
2. Upload `.azure/setup-azure-auth-for-pipeline.sh`.
3. Run:

   ```bash
   chmod +x setup-azure-auth-for-pipeline.sh
   ./setup-azure-auth-for-pipeline.sh
   ```

The script creates a user-assigned managed identity in the separate
`familytreeapp-cicd-rg` resource group. It grants that identity:

- `Contributor` on `familytreeapp-rg`, so it can update the existing Container
  App.
- `AcrPush` on the Azure Container Registry currently used by the app.

The federated credential only accepts tokens from the
`erjosito/familytreepy` repository's `production` GitHub environment. No Azure
client secret is created or stored.

## One-time GitHub setup

The Cloud Shell script prints the exact `gh` commands needed to create the
`production` environment and its deployment variables. The last command sets
the repository variable `DEPLOYMENT_ENABLED=true`; until then, pushes validate
the application but skip Azure deployment. Run the commands in an authenticated
GitHub CLI session. The final printed command starts a manual deployment from
`main`.

When the existing Container App exposes its Entra IDs as regular environment
values, the script includes these public build-time identifiers automatically.
If it cannot discover them because they are secret references, configure them
manually:

```bash
gh variable set NEXT_PUBLIC_AZURE_AD_CLIENT_ID \
  --repo erjosito/familytreepy \
  --env production \
  --body "<application-client-id>"
gh variable set NEXT_PUBLIC_AZURE_AD_TENANT_ID \
  --repo erjosito/familytreepy \
  --env production \
  --body "<application-tenant-id>"
```

Add required reviewers or deployment-branch protection to the `production`
environment in **Repository settings > Environments > production**. Pull
requests only run validation; deployment runs only after a push to `main` or a
manual dispatch from `main`.

## Deployment behavior

The workflow tests the FastAPI backend, builds the Next.js frontend, and builds
the production container before deployment. The deploy job then:

1. Exchanges GitHub's short-lived OIDC token for an Azure token.
2. Discovers the existing app's Azure Container Registry.
3. Pushes an immutable image tagged with the Git commit SHA.
4. Updates the Container App to a new revision.
5. Calls `/api/health` on the public Container App URL and verifies that its
   `revision` exactly matches the commit SHA being deployed. A healthy response
   from an older revision does not count as a successful deployment.

The health response also exposes the semantic application `version`, immutable
Git `revision`, and UTC `built_at` timestamp:

```json
{
  "status": "ok",
  "version": "0.1.0",
  "revision": "<full-git-commit-sha>",
  "built_at": "2026-08-19T13:30:00Z"
}
```

Application runtime settings and secrets remain on the existing Container App;
the workflow changes only its container image.

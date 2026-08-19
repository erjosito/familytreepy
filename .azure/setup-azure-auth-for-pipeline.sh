#!/usr/bin/env bash
set -euo pipefail

SUBSCRIPTION_ID="3e78e84b-6750-44b9-9d57-d9bba935237a"
TENANT_ID="ecd38d6d-544b-494c-9b29-ff3d6a31c040"
APP_RESOURCE_GROUP="familytreeapp-rg"
CONTAINER_APP_NAME="familytreeapp-app"
IDENTITY_RESOURCE_GROUP="familytreeapp-cicd-rg"
IDENTITY_NAME="familytreeapp-github"
GITHUB_REPOSITORY="erjosito/familytreepy"
GITHUB_ENVIRONMENT="production"
FEDERATED_CREDENTIAL_NAME="github-production"

az account show --subscription "$SUBSCRIPTION_ID" --output none

location="$(az group show \
  --name "$APP_RESOURCE_GROUP" \
  --subscription "$SUBSCRIPTION_ID" \
  --query location \
  --output tsv)"

az group create \
  --name "$IDENTITY_RESOURCE_GROUP" \
  --location "$location" \
  --subscription "$SUBSCRIPTION_ID" \
  --output none

if ! az identity show \
  --name "$IDENTITY_NAME" \
  --resource-group "$IDENTITY_RESOURCE_GROUP" \
  --subscription "$SUBSCRIPTION_ID" \
  --output none 2>/dev/null; then
  az identity create \
    --name "$IDENTITY_NAME" \
    --resource-group "$IDENTITY_RESOURCE_GROUP" \
    --location "$location" \
    --subscription "$SUBSCRIPTION_ID" \
    --output none
fi

client_id="$(az identity show \
  --name "$IDENTITY_NAME" \
  --resource-group "$IDENTITY_RESOURCE_GROUP" \
  --subscription "$SUBSCRIPTION_ID" \
  --query clientId \
  --output tsv)"
principal_id="$(az identity show \
  --name "$IDENTITY_NAME" \
  --resource-group "$IDENTITY_RESOURCE_GROUP" \
  --subscription "$SUBSCRIPTION_ID" \
  --query principalId \
  --output tsv)"

if ! az identity federated-credential show \
  --name "$FEDERATED_CREDENTIAL_NAME" \
  --identity-name "$IDENTITY_NAME" \
  --resource-group "$IDENTITY_RESOURCE_GROUP" \
  --subscription "$SUBSCRIPTION_ID" \
  --output none 2>/dev/null; then
  az identity federated-credential create \
    --name "$FEDERATED_CREDENTIAL_NAME" \
    --identity-name "$IDENTITY_NAME" \
    --resource-group "$IDENTITY_RESOURCE_GROUP" \
    --subscription "$SUBSCRIPTION_ID" \
    --issuer "https://token.actions.githubusercontent.com" \
    --subject "repo:${GITHUB_REPOSITORY}:environment:${GITHUB_ENVIRONMENT}" \
    --audiences "api://AzureADTokenExchange" \
    --output none
fi

app_scope="/subscriptions/${SUBSCRIPTION_ID}/resourceGroups/${APP_RESOURCE_GROUP}"
az role assignment create \
  --assignee-object-id "$principal_id" \
  --assignee-principal-type ServicePrincipal \
  --role Contributor \
  --scope "$app_scope" \
  --output none

current_image="$(az containerapp show \
  --name "$CONTAINER_APP_NAME" \
  --resource-group "$APP_RESOURCE_GROUP" \
  --subscription "$SUBSCRIPTION_ID" \
  --query 'properties.template.containers[0].image' \
  --output tsv)"
registry_server="${current_image%%/*}"
frontend_client_id="$(az containerapp show \
  --name "$CONTAINER_APP_NAME" \
  --resource-group "$APP_RESOURCE_GROUP" \
  --subscription "$SUBSCRIPTION_ID" \
  --query "properties.template.containers[0].env[?name=='AZURE_AD_CLIENT_ID'].value | [0]" \
  --output tsv)"
frontend_tenant_id="$(az containerapp show \
  --name "$CONTAINER_APP_NAME" \
  --resource-group "$APP_RESOURCE_GROUP" \
  --subscription "$SUBSCRIPTION_ID" \
  --query "properties.template.containers[0].env[?name=='AZURE_AD_TENANT_ID'].value | [0]" \
  --output tsv)"

if [[ -z "$registry_server" || "$registry_server" == "$current_image" || "$registry_server" != *.azurecr.io ]]; then
  echo "The Container App must already reference an Azure Container Registry image." >&2
  exit 1
fi

acr_name="${registry_server%%.*}"
acr_id="$(az acr show \
  --name "$acr_name" \
  --subscription "$SUBSCRIPTION_ID" \
  --query id \
  --output tsv)"
az role assignment create \
  --assignee-object-id "$principal_id" \
  --assignee-principal-type ServicePrincipal \
  --role AcrPush \
  --scope "$acr_id" \
  --output none

printf '\nAzure OIDC identity created successfully.\n\n'
printf 'Run these commands from a GitHub CLI session authenticated as an administrator\n'
printf 'of %s:\n\n' "$GITHUB_REPOSITORY"
printf 'gh api --method PUT repos/%s/environments/%s\n' "$GITHUB_REPOSITORY" "$GITHUB_ENVIRONMENT"
printf 'gh variable set AZURE_CLIENT_ID --repo %s --env %s --body "%s"\n' "$GITHUB_REPOSITORY" "$GITHUB_ENVIRONMENT" "$client_id"
printf 'gh variable set AZURE_TENANT_ID --repo %s --env %s --body "%s"\n' "$GITHUB_REPOSITORY" "$GITHUB_ENVIRONMENT" "$TENANT_ID"
printf 'gh variable set AZURE_SUBSCRIPTION_ID --repo %s --env %s --body "%s"\n' "$GITHUB_REPOSITORY" "$GITHUB_ENVIRONMENT" "$SUBSCRIPTION_ID"
printf 'gh variable set AZURE_RESOURCE_GROUP --repo %s --env %s --body "%s"\n' "$GITHUB_REPOSITORY" "$GITHUB_ENVIRONMENT" "$APP_RESOURCE_GROUP"
printf 'gh variable set AZURE_CONTAINER_APP_NAME --repo %s --env %s --body "%s"\n\n' "$GITHUB_REPOSITORY" "$GITHUB_ENVIRONMENT" "$CONTAINER_APP_NAME"
if [[ -n "$frontend_client_id" && -n "$frontend_tenant_id" ]]; then
  printf 'gh variable set NEXT_PUBLIC_AZURE_AD_CLIENT_ID --repo %s --env %s --body "%s"\n' "$GITHUB_REPOSITORY" "$GITHUB_ENVIRONMENT" "$frontend_client_id"
  printf 'gh variable set NEXT_PUBLIC_AZURE_AD_TENANT_ID --repo %s --env %s --body "%s"\n\n' "$GITHUB_REPOSITORY" "$GITHUB_ENVIRONMENT" "$frontend_tenant_id"
else
  printf '# Add NEXT_PUBLIC_AZURE_AD_CLIENT_ID and NEXT_PUBLIC_AZURE_AD_TENANT_ID manually if frontend authentication is enabled.\n\n'
fi
printf 'gh variable set DEPLOYMENT_ENABLED --repo %s --body "true"\n\n' "$GITHUB_REPOSITORY"
printf 'gh workflow run "Build and deploy" --repo %s --ref main\n\n' "$GITHUB_REPOSITORY"

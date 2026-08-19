import {
  PublicClientApplication,
  type Configuration,
  BrowserCacheLocation,
} from "@azure/msal-browser";

export function isAuthEnabled(): boolean {
  return (
    !!process.env.NEXT_PUBLIC_AZURE_AD_CLIENT_ID &&
    !!process.env.NEXT_PUBLIC_AZURE_AD_TENANT_ID
  );
}

const authority =
  process.env.NEXT_PUBLIC_AZURE_AD_AUTHORITY ||
  (process.env.NEXT_PUBLIC_AZURE_AD_TENANT_ID
    ? `https://login.microsoftonline.com/${process.env.NEXT_PUBLIC_AZURE_AD_TENANT_ID}`
    : "");

const msalConfig: Configuration = {
  auth: {
    clientId: process.env.NEXT_PUBLIC_AZURE_AD_CLIENT_ID || "",
    authority,
    knownAuthorities: authority ? [new URL(authority).hostname] : [],
    redirectUri: typeof window !== "undefined" ? window.location.origin : "",
  },
  cache: {
    cacheLocation: BrowserCacheLocation.LocalStorage,
  },
};

export const msalInstance = new PublicClientApplication(msalConfig);

export const loginRequest = {
  scopes: [process.env.NEXT_PUBLIC_API_SCOPE || "openid"],
};

"use client";

import { ReactNode, useEffect, useState } from "react";
import { MsalProvider, useMsal, useIsAuthenticated } from "@azure/msal-react";
import { msalInstance, loginRequest, isAuthEnabled } from "@/lib/auth";
import NavBar from "./NavBar";

function AuthGate({ children }: { children: ReactNode }) {
  const { instance } = useMsal();
  const isAuthenticated = useIsAuthenticated();
  const [ready, setReady] = useState(false);

  useEffect(() => {
    instance.handleRedirectPromise().then(() => setReady(true)).catch(() => setReady(true));
  }, [instance]);

  if (!ready) {
    return (
      <div className="flex items-center justify-center h-screen text-gray-500">
        Loading...
      </div>
    );
  }

  if (!isAuthenticated) {
    return (
      <div className="flex flex-col items-center justify-center h-screen gap-4">
        <h1 className="text-2xl font-bold text-gray-900">🌳 Family Tree</h1>
        <p className="text-gray-500">Sign in to continue</p>
        <button
          className="px-6 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors"
          onClick={() => instance.loginRedirect(loginRequest)}
        >
          Sign in with Microsoft
        </button>
      </div>
    );
  }

  return (
    <>
      <NavBar />
      {children}
    </>
  );
}

export default function AuthProvider({ children }: { children: ReactNode }) {
  const [msalReady, setMsalReady] = useState(false);

  useEffect(() => {
    // MSAL v4 requires explicit initialization
    msalInstance.initialize().then(() => setMsalReady(true)).catch(() => setMsalReady(true));
  }, []);

  if (!isAuthEnabled()) {
    return <>{children}</>;
  }

  if (!msalReady) {
    return (
      <div className="flex items-center justify-center h-screen text-gray-500">
        Initializing...
      </div>
    );
  }

  return (
    <MsalProvider instance={msalInstance}>
      <AuthGate>{children}</AuthGate>
    </MsalProvider>
  );
}

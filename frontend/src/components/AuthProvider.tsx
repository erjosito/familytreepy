"use client";

import { ReactNode, useEffect, useState } from "react";
import { isAuthEnabled } from "@/lib/auth";
import { AdminViewProvider } from "@/lib/adminView";
import { useI18n } from "@/lib/i18n";
import NavBar from "./NavBar";

const API_BASE = process.env.NEXT_PUBLIC_API_URL ?? "";

interface SessionUser {
  email: string;
  name: string;
  roles: string[];
  role?: string;
}

function AuthGate({ children }: { children: ReactNode }) {
  const { t } = useI18n();
  const [user, setUser] = useState<SessionUser | null>(null);
  const [loading, setLoading] = useState(true);
  const [authError, setAuthError] = useState<string | null>(null);

  useEffect(() => {
    // Check for auth error in URL params (e.g., ?auth_error=not_authorized)
    if (typeof window !== "undefined") {
      const params = new URLSearchParams(window.location.search);
      const error = params.get("auth_error");
      if (error) {
        setAuthError(error);
        // Clean URL
        window.history.replaceState({}, "", window.location.pathname);
      }
    }

    fetch(`${API_BASE}/api/auth/session`, { credentials: "include" })
      .then((r) => (r.ok ? r.json() : null))
      .then((u) => setUser(u))
      .catch(() => setUser(null))
      .finally(() => setLoading(false));
  }, []);

  if (loading) {
    return (
      <div className="flex items-center justify-center h-screen bg-white dark:bg-gray-900 text-gray-500 dark:text-gray-400">
        {t("auth.loading")}
      </div>
    );
  }

  if (authError === "not_authorized") {
    return (
      <div className="flex flex-col items-center justify-center h-screen gap-4 bg-white dark:bg-gray-900 px-6 text-center">
        <h1 className="text-2xl font-bold text-gray-900 dark:text-gray-100">{t("auth.title")}</h1>
        <div className="bg-red-50 dark:bg-red-900/30 border border-red-200 dark:border-red-800 rounded-lg p-4 max-w-md">
          <p className="text-red-700 dark:text-red-300 font-medium">{t("auth.notAuthorized")}</p>
          <p className="text-red-600 dark:text-red-400 text-sm mt-2">{t("auth.contactAdmin")}</p>
        </div>
        <a
          href={`${API_BASE}/api/auth/login`}
          className="text-sm text-blue-600 hover:underline"
        >
          {t("auth.tryDifferentAccount")}
        </a>
      </div>
    );
  }

  if (authError) {
    return (
      <div className="flex flex-col items-center justify-center h-screen gap-4 bg-white dark:bg-gray-900 px-6 text-center">
        <h1 className="text-2xl font-bold text-gray-900 dark:text-gray-100">{t("auth.title")}</h1>
        <div className="bg-red-50 dark:bg-red-900/30 border border-red-200 dark:border-red-800 rounded-lg p-4 max-w-md">
          <p className="text-red-700 dark:text-red-300">{t("auth.signInError")}: {authError}</p>
        </div>
        <a
          href={`${API_BASE}/api/auth/login`}
          className="px-6 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors"
        >
          {t("auth.tryAgain")}
        </a>
      </div>
    );
  }

  if (!user) {
    return (
      <div className="flex flex-col items-center justify-center h-screen gap-4 bg-white dark:bg-gray-900">
        <h1 className="text-2xl font-bold text-gray-900 dark:text-gray-100">{t("auth.title")}</h1>
        <p className="text-gray-500 dark:text-gray-400">{t("auth.signInPrompt")}</p>
        <div className="flex flex-col gap-2 w-64">
          <a
            href={`${API_BASE}/api/auth/login?provider=microsoft`}
            className="flex items-center justify-center gap-2 px-6 py-2 bg-[#2f2f2f] text-white rounded-lg hover:bg-[#1a1a1a] transition-colors text-sm"
          >
            <svg width="16" height="16" viewBox="0 0 21 21"><rect x="1" y="1" width="9" height="9" fill="#f25022"/><rect x="11" y="1" width="9" height="9" fill="#7fba00"/><rect x="1" y="11" width="9" height="9" fill="#00a4ef"/><rect x="11" y="11" width="9" height="9" fill="#ffb900"/></svg>
            {t("auth.signInMicrosoft")}
          </a>
          <a
            href={`${API_BASE}/api/auth/login?provider=google`}
            className="flex items-center justify-center gap-2 px-6 py-2 bg-white text-gray-700 border border-gray-300 rounded-lg hover:bg-gray-50 transition-colors text-sm dark:bg-gray-800 dark:text-gray-300 dark:border-gray-600"
          >
            <svg width="16" height="16" viewBox="0 0 48 48"><path fill="#EA4335" d="M24 9.5c3.54 0 6.71 1.22 9.21 3.6l6.85-6.85C35.9 2.38 30.47 0 24 0 14.62 0 6.51 5.38 2.56 13.22l7.98 6.19C12.43 13.72 17.74 9.5 24 9.5z"/><path fill="#4285F4" d="M46.98 24.55c0-1.57-.15-3.09-.38-4.55H24v9.02h12.94c-.58 2.96-2.26 5.48-4.78 7.18l7.73 6c4.51-4.18 7.09-10.36 7.09-17.65z"/><path fill="#FBBC05" d="M10.53 28.59c-.48-1.45-.76-2.99-.76-4.59s.27-3.14.76-4.59l-7.98-6.19C.92 16.46 0 20.12 0 24c0 3.88.92 7.54 2.56 10.78l7.97-6.19z"/><path fill="#34A853" d="M24 48c6.48 0 11.93-2.13 15.89-5.81l-7.73-6c-2.15 1.45-4.92 2.3-8.16 2.3-6.26 0-11.57-4.22-13.47-9.91l-7.98 6.19C6.51 42.62 14.62 48 24 48z"/></svg>
            {t("auth.signInGoogle")}
          </a>
        </div>
      </div>
    );
  }

  const isAdmin = user.roles?.includes("admin") || user.role === "admin";

  return (
    <AdminViewProvider isAdmin={isAdmin} userEmail={user.email} userName={user.name}>
      <NavBar userName={user.name} onLogout={async () => {
        await fetch(`${API_BASE}/api/auth/logout`, { method: "POST", credentials: "include" });
        window.location.reload();
      }} />
      {children}
    </AdminViewProvider>
  );
}

export default function AuthProvider({ children }: { children: ReactNode }) {
  if (!isAuthEnabled()) {
    return (
      <AdminViewProvider isAdmin={true} userEmail="dev@localhost">
        <NavBar />
        {children}
      </AdminViewProvider>
    );
  }

  return <AuthGate>{children}</AuthGate>;
}

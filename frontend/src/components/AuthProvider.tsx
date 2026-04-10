"use client";

import { ReactNode, useEffect, useState } from "react";
import { isAuthEnabled } from "@/lib/auth";
import { AdminViewProvider } from "@/lib/adminView";
import { useI18n } from "@/lib/i18n";
import NavBar from "./NavBar";

const API_BASE = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";

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

  useEffect(() => {
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

  if (!user) {
    return (
      <div className="flex flex-col items-center justify-center h-screen gap-4 bg-white dark:bg-gray-900">
        <h1 className="text-2xl font-bold text-gray-900 dark:text-gray-100">{t("auth.title")}</h1>
        <p className="text-gray-500 dark:text-gray-400">{t("auth.signInPrompt")}</p>
        <a
          href={`${API_BASE}/api/auth/login`}
          className="px-6 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors"
        >
          {t("auth.signIn")}
        </a>
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

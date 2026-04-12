"use client";

import Link from "next/link";
import { isAuthEnabled } from "@/lib/auth";
import { useI18n, type Locale } from "@/lib/i18n";
import { useAdminView } from "@/lib/adminView";

const LOCALE_LABELS: Record<Locale, string> = { en: "EN", es: "ES" };

interface NavBarProps {
  userName?: string;
  onLogout?: () => void;
}

export default function NavBar({ userName, onLogout }: NavBarProps) {
  const { t, locale, setLocale } = useI18n();
  const { isAdmin, adminView, setAdminView } = useAdminView();

  return (
    <nav className="flex items-center gap-4 px-4 py-2 bg-white border-b shadow-sm">
      <Link href="/" className="text-lg font-bold text-blue-600">
        {t("nav.title")}
      </Link>

      <div className="flex items-center gap-3 text-sm">
        <Link href="/" className="text-gray-700 hover:text-blue-600">
          {t("nav.explore")}
        </Link>
        <Link href="/image/" className="text-gray-700 hover:text-blue-600">
          {t("nav.image")}
        </Link>
        {adminView && (
          <Link href="/grid/" className="text-gray-700 hover:text-blue-600">
            {t("nav.grid")}
          </Link>
        )}
        {adminView && (
          <Link href="/admin/" className="text-gray-700 hover:text-blue-600">
            {t("nav.admin")}
          </Link>
        )}
      </div>

      <div className="ml-auto flex items-center gap-3">
        {/* Admin/User view toggle */}
        {isAdmin && (
          <button
            onClick={() => setAdminView(!adminView)}
            className={`text-xs px-2 py-1 rounded border transition-colors ${
              adminView
                ? "bg-purple-100 border-purple-400 text-purple-700"
                : "bg-gray-100 border-gray-300 text-gray-500"
            }`}
            title={adminView ? t("nav.switchToUser") : t("nav.switchToAdmin")}
          >
            {adminView ? t("nav.adminBadge") : t("nav.userBadge")}
          </button>
        )}

        {/* Language selector */}
        <div className="flex items-center gap-1 text-xs">
          {(Object.keys(LOCALE_LABELS) as Locale[]).map((l) => (
            <button
              key={l}
              onClick={() => setLocale(l)}
              className={`px-1.5 py-0.5 rounded ${
                locale === l
                  ? "bg-blue-600 text-white"
                  : "text-gray-500 hover:text-gray-800"
              }`}
            >
              {LOCALE_LABELS[l]}
            </button>
          ))}
        </div>

        {userName ? (
          <div className="flex items-center gap-3">
            <span className="text-sm text-gray-700">{userName}</span>
            {onLogout && (
              <button
                className="text-sm text-red-500 hover:underline"
                onClick={onLogout}
              >
                {t("nav.signOut")}
              </button>
            )}
          </div>
        ) : !isAuthEnabled() ? (
          <span className="text-xs text-gray-400 bg-gray-100 px-2 py-1 rounded">
            {t("nav.devMode")}
          </span>
        ) : null}
      </div>
    </nav>
  );
}

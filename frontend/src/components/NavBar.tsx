"use client";

import Link from "next/link";
import { useState } from "react";
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
  const [menuOpen, setMenuOpen] = useState(false);

  const navLinks = (
    <>
      <Link href="/" onClick={() => setMenuOpen(false)} className="text-gray-700 hover:text-blue-600">
        {t("nav.explore")}
      </Link>
      <Link href="/image/" onClick={() => setMenuOpen(false)} className="text-gray-700 hover:text-blue-600">
        {t("nav.image")}
      </Link>
      {adminView && (
        <Link href="/grid/" onClick={() => setMenuOpen(false)} className="text-gray-700 hover:text-blue-600">
          {t("nav.grid")}
        </Link>
      )}
      {adminView && (
        <Link href="/admin/" onClick={() => setMenuOpen(false)} className="text-gray-700 hover:text-blue-600">
          {t("nav.admin")}
        </Link>
      )}
    </>
  );

  const languageSelector = (
    <div className="flex items-center gap-1 text-xs">
      {(Object.keys(LOCALE_LABELS) as Locale[]).map((l) => (
        <button
          key={l}
          type="button"
          onClick={() => setLocale(l)}
          className={`min-h-8 min-w-8 rounded ${
            locale === l
              ? "bg-blue-600 text-white"
              : "text-gray-500 hover:text-gray-800"
          }`}
        >
          {LOCALE_LABELS[l]}
        </button>
      ))}
    </div>
  );

  return (
    <nav className="relative z-40 flex h-14 md:h-12 items-center gap-4 px-4 bg-white border-b shadow-sm">
      <Link href="/" className="text-lg font-bold text-blue-600 whitespace-nowrap">
        {t("nav.title")}
      </Link>

      <div className="hidden md:flex items-center gap-3 text-sm">
        {navLinks}
      </div>

      <div className="ml-auto hidden md:flex items-center gap-3">
        {/* Admin/User view toggle */}
        {isAdmin && (
          <button
            type="button"
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
        {languageSelector}

        {userName ? (
          <div className="flex items-center gap-3">
            <span className="text-sm text-gray-700">{userName}</span>
            {onLogout && (
              <button
                type="button"
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

      <button
        type="button"
        className="ml-auto md:hidden inline-flex h-10 w-10 items-center justify-center rounded-lg border border-gray-200 text-xl text-gray-700 hover:bg-gray-50"
        aria-label={t("nav.title")}
        aria-expanded={menuOpen}
        onClick={() => setMenuOpen((open) => !open)}
      >
        {menuOpen ? "✕" : "☰"}
      </button>

      {menuOpen && (
        <>
          <button
            type="button"
            className="fixed inset-0 top-14 z-[-1] bg-black/20 md:hidden"
            aria-label={t("form.cancel")}
            onClick={() => setMenuOpen(false)}
          />
          <div className="absolute inset-x-0 top-full flex flex-col gap-1 border-b bg-white p-3 shadow-lg md:hidden">
            <div className="flex flex-col text-sm [&>a]:rounded-lg [&>a]:px-3 [&>a]:py-3">
              {navLinks}
            </div>
            <div className="flex items-center justify-between gap-3 border-t px-3 pt-3">
              {isAdmin ? (
                <button
                  type="button"
                  onClick={() => setAdminView(!adminView)}
                  className={`min-h-10 text-xs px-3 py-1 rounded border transition-colors ${
                    adminView
                      ? "bg-purple-100 border-purple-400 text-purple-700"
                      : "bg-gray-100 border-gray-300 text-gray-500"
                  }`}
                >
                  {adminView ? t("nav.adminBadge") : t("nav.userBadge")}
                </button>
              ) : <span />}
              {languageSelector}
            </div>
            <div className="flex items-center justify-between gap-3 px-3 py-2 text-sm">
              {userName ? (
                <>
                  <span className="min-w-0 truncate text-gray-700">{userName}</span>
                  {onLogout && (
                    <button type="button" className="min-h-10 shrink-0 text-red-500" onClick={onLogout}>
                      {t("nav.signOut")}
                    </button>
                  )}
                </>
              ) : !isAuthEnabled() ? (
                <span className="text-xs text-gray-400">{t("nav.devMode")}</span>
              ) : null}
            </div>
          </div>
        </>
      )}
    </nav>
  );
}

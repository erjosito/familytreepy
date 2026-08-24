"use client";

import { useI18n } from "@/lib/i18n";

export default function SkipLink() {
  const { t } = useI18n();

  return (
    <a
      href="#main-content"
      className="fixed left-3 top-3 z-[100] -translate-y-20 rounded bg-blue-700 px-4 py-2 font-medium text-white shadow focus:translate-y-0"
    >
      {t("nav.skipToContent")}
    </a>
  );
}

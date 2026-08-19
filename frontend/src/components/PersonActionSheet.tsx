"use client";

import { useEffect, useId, useRef } from "react";
import type { PersonActionDefinition } from "@/lib/personActions";
import { useI18n } from "@/lib/i18n";

interface Props {
  personName: string;
  actions: PersonActionDefinition[];
  onSelect: (action: string) => void;
  onClose: () => void;
}

export default function PersonActionSheet({ personName, actions, onSelect, onClose }: Props) {
  const { t } = useI18n();
  const titleId = useId();
  const dialogRef = useRef<HTMLDivElement>(null);
  const closeButtonRef = useRef<HTMLButtonElement>(null);

  useEffect(() => {
    const previousFocus = document.activeElement instanceof HTMLElement ? document.activeElement : null;
    closeButtonRef.current?.focus();
    const dismissOnEscape = (event: KeyboardEvent) => {
      if (event.key === "Escape") onClose();
    };
    window.addEventListener("keydown", dismissOnEscape);
    return () => {
      window.removeEventListener("keydown", dismissOnEscape);
      previousFocus?.focus();
    };
  }, [onClose]);

  const trapFocus = (event: React.KeyboardEvent) => {
    if (event.key !== "Tab" || !dialogRef.current) return;
    const controls = [...dialogRef.current.querySelectorAll<HTMLElement>("button:not(:disabled)")];
    const first = controls[0];
    const last = controls.at(-1);
    if (event.shiftKey && document.activeElement === first) {
      event.preventDefault();
      last?.focus();
    } else if (!event.shiftKey && document.activeElement === last) {
      event.preventDefault();
      first?.focus();
    }
  };

  return (
    <>
      <button
        type="button"
        className="fixed inset-0 z-40 bg-black/40"
        aria-label={t("actions.close")}
        onClick={onClose}
      />
      <div
        ref={dialogRef}
        role="dialog"
        aria-modal="true"
        aria-labelledby={titleId}
        onKeyDown={trapFocus}
        className="fixed inset-x-0 bottom-0 z-50 max-h-[78dvh] overflow-y-auto rounded-t-2xl border-t bg-white px-4 pt-3 shadow-2xl pb-[max(1rem,env(safe-area-inset-bottom))]"
      >
        <div className="mx-auto mb-3 h-1 w-10 rounded-full bg-gray-300" aria-hidden="true" />
        <div className="mb-3 flex items-start justify-between gap-4">
          <div className="min-w-0">
            <h2 id={titleId} className="text-lg font-semibold text-gray-900">
              {t("actions.title")}
            </h2>
            <p className="truncate text-sm text-gray-500">{personName}</p>
          </div>
          <button
            ref={closeButtonRef}
            type="button"
            onClick={onClose}
            aria-label={t("actions.close")}
            className="flex min-h-11 min-w-11 items-center justify-center rounded-full text-xl text-gray-500 hover:bg-gray-100 focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-blue-600"
          >
            ×
          </button>
        </div>
        <div className="grid grid-cols-1 gap-2 pb-1 min-[380px]:grid-cols-2">
          {actions.map((item) => (
            <button
              key={item.action}
              type="button"
              onClick={() => onSelect(item.action)}
              className={`flex min-h-11 items-center gap-3 rounded-lg border px-4 py-2.5 text-left text-sm font-medium focus-visible:outline-2 focus-visible:outline-offset-2 focus-visible:outline-blue-600 ${
                item.destructive
                  ? "border-red-200 text-red-700 hover:bg-red-50"
                  : "border-gray-200 text-gray-800 hover:bg-gray-50"
              }`}
            >
              <span className="text-lg" aria-hidden="true">{item.icon}</span>
              <span>{t(item.labelKey)}</span>
            </button>
          ))}
        </div>
      </div>
    </>
  );
}

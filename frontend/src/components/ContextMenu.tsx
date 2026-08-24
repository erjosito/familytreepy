"use client";

import { useEffect, useRef } from "react";
import { useI18n } from "@/lib/i18n";

interface MenuItem {
  label: string;
  action: string;
}

interface Props {
  x: number;
  y: number;
  items: MenuItem[];
  onSelect: (action: string) => void;
  onClose: () => void;
}

export default function ContextMenu({ x, y, items, onSelect, onClose }: Props) {
  const { t } = useI18n();
  const menuRef = useRef<HTMLDivElement>(null);
  const previousFocusRef = useRef<HTMLElement | null>(null);

  useEffect(() => {
    previousFocusRef.current = document.activeElement as HTMLElement | null;
    menuRef.current?.querySelector<HTMLButtonElement>('[role="menuitem"]')?.focus();
    return () => previousFocusRef.current?.focus();
  }, []);

  const moveFocus = (direction: 1 | -1) => {
    const buttons = [...(menuRef.current?.querySelectorAll<HTMLButtonElement>('[role="menuitem"]') || [])];
    const current = buttons.indexOf(document.activeElement as HTMLButtonElement);
    buttons[(current + direction + buttons.length) % buttons.length]?.focus();
  };

  return (
    <>
      <button
        type="button"
        tabIndex={-1}
        aria-label={t("nav.closeMenu")}
        className="fixed inset-0 z-40 cursor-default"
        onClick={onClose}
        onContextMenu={(event) => {
          event.preventDefault();
          onClose();
        }}
      />
      <div
        ref={menuRef}
        role="menu"
        aria-label={t("menu.personActions")}
        className="fixed z-50 bg-white rounded-lg shadow-lg border py-1 min-w-[160px]"
        style={{ left: x, top: y }}
        onKeyDown={(event) => {
          if (event.key === "Escape") {
            event.preventDefault();
            onClose();
          } else if (event.key === "ArrowDown") {
            event.preventDefault();
            moveFocus(1);
          } else if (event.key === "ArrowUp") {
            event.preventDefault();
            moveFocus(-1);
          } else if (event.key === "Home") {
            event.preventDefault();
            menuRef.current?.querySelector<HTMLButtonElement>('[role="menuitem"]')?.focus();
          } else if (event.key === "End") {
            event.preventDefault();
            const buttons = menuRef.current?.querySelectorAll<HTMLButtonElement>('[role="menuitem"]');
            buttons?.[buttons.length - 1]?.focus();
          } else if (event.key === "Tab") {
            onClose();
          }
        }}
      >
        {items.map((item) => (
          <button
            key={item.action}
            type="button"
            role="menuitem"
            className="w-full text-left px-4 py-2 text-sm text-gray-900 hover:bg-blue-50 transition-colors"
            onClick={() => {
              onSelect(item.action);
              onClose();
            }}
          >
            {item.label}
          </button>
        ))}
      </div>
    </>
  );
}

"use client";

import {
  createContext,
  useCallback,
  useContext,
  useMemo,
  useRef,
  useState,
  type ReactNode,
} from "react";
import { useI18n } from "@/lib/i18n";

type ToastVariant = "success" | "error" | "info";

interface ToastAction {
  label: string;
  onClick: () => void | Promise<void>;
}

interface ToastOptions {
  message?: string;
  action?: ToastAction;
  duration?: number;
}

interface ToastItem extends ToastOptions {
  id: number;
  title: string;
  variant: ToastVariant;
}

interface ToastContextValue {
  success: (title: string, options?: ToastOptions) => void;
  error: (title: string, options?: ToastOptions) => void;
  info: (title: string, options?: ToastOptions) => void;
}

const ToastContext = createContext<ToastContextValue | null>(null);

const variantStyles: Record<ToastVariant, string> = {
  success: "border-green-300 bg-green-50 text-green-900",
  error: "border-red-300 bg-red-50 text-red-900",
  info: "border-blue-300 bg-blue-50 text-blue-900",
};

const variantIcons: Record<ToastVariant, string> = {
  success: "✓",
  error: "!",
  info: "i",
};

export function ToastProvider({ children }: { children: ReactNode }) {
  const { t } = useI18n();
  const [toasts, setToasts] = useState<ToastItem[]>([]);
  const nextId = useRef(1);

  const dismiss = useCallback((id: number) => {
    setToasts((current) => current.filter((toast) => toast.id !== id));
  }, []);

  const show = useCallback(
    (variant: ToastVariant, title: string, options: ToastOptions = {}) => {
      const id = nextId.current++;
      const duration = options.duration ?? (variant === "error" ? 9000 : 4500);
      setToasts((current) => [...current.slice(-3), { id, title, variant, ...options }]);
      if (duration > 0) {
        window.setTimeout(() => dismiss(id), duration);
      }
    },
    [dismiss]
  );

  const value = useMemo<ToastContextValue>(
    () => ({
      success: (title, options) => show("success", title, options),
      error: (title, options) => show("error", title, options),
      info: (title, options) => show("info", title, options),
    }),
    [show]
  );

  return (
    <ToastContext.Provider value={value}>
      {children}
      <div
        className="pointer-events-none fixed inset-x-0 top-16 z-[200] flex flex-col items-end gap-2 px-3 sm:left-auto sm:right-4 sm:top-16 sm:w-[min(24rem,calc(100vw-2rem))] sm:px-0"
        aria-label={t("toast.notifications")}
      >
        {toasts.map((toast) => (
          <div
            key={toast.id}
            role={toast.variant === "error" ? "alert" : "status"}
            aria-live={toast.variant === "error" ? "assertive" : "polite"}
            className={`pointer-events-auto flex w-full items-start gap-3 rounded-lg border p-3 shadow-lg ${variantStyles[toast.variant]}`}
          >
            <span
              aria-hidden="true"
              className="mt-0.5 flex h-5 w-5 shrink-0 items-center justify-center rounded-full border border-current text-xs font-bold"
            >
              {variantIcons[toast.variant]}
            </span>
            <div className="min-w-0 flex-1">
              <p className="text-sm font-semibold">{toast.title}</p>
              {toast.message && <p className="mt-0.5 break-words text-xs opacity-80">{toast.message}</p>}
              {toast.action && (
                <button
                  type="button"
                  className="mt-2 min-h-8 rounded border border-current px-2 text-xs font-medium hover:bg-white/50"
                  onClick={() => {
                    dismiss(toast.id);
                    void toast.action?.onClick();
                  }}
                >
                  {toast.action.label}
                </button>
              )}
            </div>
            <button
              type="button"
              className="-mr-1 -mt-1 flex h-8 w-8 shrink-0 items-center justify-center rounded text-current/70 hover:bg-white/50 hover:text-current"
              aria-label={t("toast.dismiss")}
              onClick={() => dismiss(toast.id)}
            >
              ✕
            </button>
          </div>
        ))}
      </div>
    </ToastContext.Provider>
  );
}

export function useToast() {
  const context = useContext(ToastContext);
  if (!context) throw new Error("useToast must be used within ToastProvider");
  return context;
}

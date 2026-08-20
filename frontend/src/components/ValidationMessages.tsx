"use client";

import type { ValidationIssue } from "@/lib/api";
import { useI18n } from "@/lib/i18n";

interface Props {
  issues: ValidationIssue[];
  submitting?: boolean;
  onOverride?: () => void | Promise<void>;
}

export default function ValidationMessages({
  issues,
  submitting = false,
  onOverride,
}: Props) {
  const { t } = useI18n();
  if (issues.length === 0) return null;

  const canOverride = issues.every((issue) => issue.severity === "warning");
  return (
    <div
      role="alert"
      className={`rounded border p-3 text-sm ${
        canOverride
          ? "border-amber-300 bg-amber-50 text-amber-900"
          : "border-red-300 bg-red-50 text-red-900"
      }`}
    >
      <p className="font-semibold">
        {t(canOverride ? "validation.warningTitle" : "validation.errorTitle")}
      </p>
      <ul className="mt-1 list-disc space-y-1 pl-5">
        {issues.map((issue, index) => (
          <li key={`${issue.code}-${issue.field ?? "general"}-${index}`}>
            {issue.message}
          </li>
        ))}
      </ul>
      {canOverride && onOverride && (
        <button
          type="button"
          disabled={submitting}
          onClick={() => void onOverride()}
          className="mt-3 rounded border border-amber-500 bg-amber-100 px-3 py-1.5 text-sm font-medium text-amber-900 hover:bg-amber-200 disabled:cursor-not-allowed disabled:opacity-60"
        >
          {t("validation.saveAnyway")}
        </button>
      )}
    </div>
  );
}

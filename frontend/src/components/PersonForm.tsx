"use client";

import { useState, useEffect, useRef } from "react";
import { getPersonSchema, suggestDuplicates } from "@/lib/api";
import type { DuplicateSuggestion, ValidationIssue } from "@/lib/api";
import type { FieldConfig } from "@/lib/types";
import { useI18n } from "@/lib/i18n";
import ValidationMessages from "@/components/ValidationMessages";

function duplicateReasonLabel(
  code: string,
  t: ReturnType<typeof useI18n>["t"],
): string {
  switch (code) {
    case "normalized_name":
      return t("duplicates.reason.normalizedName");
    case "name_alias_overlap":
      return t("duplicates.reason.alias");
    case "matching_birthdate":
      return t("duplicates.reason.birthdate");
    case "matching_deathdate":
      return t("duplicates.reason.deathdate");
    case "shared_close_relatives":
      return t("duplicates.reason.relatives");
    default:
      return code;
  }
}

interface Props {
  mode: "add" | "edit";
  initialData?: Record<string, unknown>;
  title: string;
  submitting?: boolean;
  validationIssues?: ValidationIssue[];
  personId?: string;
  relativeIds?: string[];
  onSubmit: (data: Record<string, unknown>, overrideWarnings?: boolean) => void | Promise<void>;
  onValidationClear?: () => void;
  onCancel: () => void;
}

export default function PersonForm({
  mode,
  initialData = {},
  title,
  submitting = false,
  validationIssues = [],
  personId,
  relativeIds = [],
  onSubmit,
  onValidationClear,
  onCancel,
}: Props) {
  const { t } = useI18n();
  const [schema, setSchema] = useState<Record<string, FieldConfig>>({});
  const [formData, setFormData] = useState<Record<string, unknown>>(initialData);
  const [duplicateSuggestions, setDuplicateSuggestions] = useState<DuplicateSuggestion[]>([]);
  const duplicateRequest = useRef(0);
  const validationSummaryRef = useRef<HTMLDivElement>(null);
  const relativeIdsKey = relativeIds.join("\0");
  const hasIdentity = ["firstname", "lastname", "alias", "birthdate"].some(
    (field) => typeof formData[field] === "string" && formData[field].trim(),
  );

  useEffect(() => {
    getPersonSchema().then((s) => setSchema(s as Record<string, FieldConfig>)).catch(console.error);
  }, []);

  useEffect(() => {
    const request = ++duplicateRequest.current;
    if (!hasIdentity) return;
    const timeout = window.setTimeout(() => {
      void suggestDuplicates(
        formData,
        personId,
        relativeIdsKey ? relativeIdsKey.split("\0") : [],
      )
        .then((suggestions) => {
          if (duplicateRequest.current === request) {
            setDuplicateSuggestions(suggestions);
          }
        })
        .catch((error) => console.error("Duplicate suggestion failed:", error));
    }, 350);
    return () => window.clearTimeout(timeout);
  }, [formData, hasIdentity, personId, relativeIdsKey]);

  useEffect(() => {
    if (validationIssues.length === 0) return;
    const firstField = validationIssues.find((issue) => issue.field)?.field;
    const field = firstField
      ? document.getElementById(`person-form-${firstField}`)
      : null;
    if (field instanceof HTMLElement) {
      field.focus();
    } else {
      validationSummaryRef.current?.focus();
    }
  }, [validationIssues]);

  const handleChange = (field: string, value: unknown) => {
    setFormData((prev) => ({ ...prev, [field]: value }));
    onValidationClear?.();
  };

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    onSubmit(formData);
  };

  const isVisible = (field: string, config: FieldConfig) => {
    if (!config.visible_when) return true;
    return formData[config.visible_when.field] === config.visible_when.equals;
  };
  return (
    <form onSubmit={handleSubmit} className="space-y-3">
      <h3 className="font-semibold text-lg">{title}</h3>

      <div ref={validationSummaryRef} tabIndex={-1}>
        <ValidationMessages
          issues={validationIssues}
          submitting={submitting}
          onOverride={() => onSubmit(formData, true)}
        />
      </div>

      {hasIdentity && duplicateSuggestions.length > 0 && (
        <aside aria-live="polite" aria-atomic="true" className="rounded-lg border border-amber-200 bg-amber-50 p-3">
          <p className="text-sm font-semibold text-amber-900">{t("duplicates.possible")}</p>
          <p className="mt-1 text-xs text-amber-800">{t("duplicates.nonBlocking")}</p>
          <ul className="mt-2 space-y-1">
            {duplicateSuggestions.slice(0, 3).map((suggestion) => (
              <li key={suggestion.person_id} className="text-sm text-amber-900">
                <a
                  href={`/?person=${encodeURIComponent(suggestion.person_id)}`}
                  className="font-medium underline"
                  target="_blank"
                  rel="noreferrer"
                >
                  {suggestion.fullname || suggestion.person_id}
                </a>
                {" · "}
                {suggestion.score}% · {suggestion.reasons.map((reason) => duplicateReasonLabel(reason.code, t)).join(", ")}
              </li>
            ))}
          </ul>
        </aside>
      )}

      {Object.entries(schema).map(([field, config]) => {
        if (!isVisible(field, config)) return null;
        if (config.type === "image_url" || config.type === "image_url_array") return null;
        const fieldId = `person-form-${field}`;
        const fieldIssues = validationIssues.filter((issue) => issue.field === field);
        const describedBy = fieldIssues.length > 0 ? `${fieldId}-issues` : undefined;

        return (
          <div key={field}>
            <label htmlFor={fieldId} className="block text-sm font-medium text-gray-600 mb-1">{config.label}</label>
            {config.type === "boolean" ? (
              <input
                id={fieldId}
                type="checkbox"
                checked={formData[field] as boolean ?? config.default ?? false}
                onChange={(e) => handleChange(field, e.target.checked)}
                aria-invalid={fieldIssues.some((issue) => issue.severity === "error")}
                aria-describedby={describedBy}
                className="rounded"
              />
            ) : config.type === "date" ? (
              <input
                id={fieldId}
                type="text"
                value={(formData[field] as string) || ""}
                onChange={(e) => handleChange(field, e.target.value)}
                placeholder="dd/mm/yyyy"
                aria-invalid={fieldIssues.some((issue) => issue.severity === "error")}
                aria-describedby={describedBy}
                className="w-full border rounded px-3 py-1.5 text-sm text-gray-900"
              />
            ) : (
              <input
                id={fieldId}
                type="text"
                value={(formData[field] as string) || ""}
                onChange={(e) => handleChange(field, e.target.value)}
                aria-invalid={fieldIssues.some((issue) => issue.severity === "error")}
                aria-describedby={describedBy}
                className="w-full border rounded px-3 py-1.5 text-sm text-gray-900"
              />
            )}
            {fieldIssues.length > 0 && (
              <div id={`${fieldId}-issues`}>
                {fieldIssues.map((issue, index) => (
                <p
                  key={`${issue.code}-${index}`}
                  className={`mt-1 text-xs ${
                    issue.severity === "error" ? "text-red-700" : "text-amber-700"
                  }`}
                >
                  {issue.message}
                </p>
                ))}
              </div>
            )}
          </div>
        );
      })}

      <div className="flex gap-2 pt-2">
        <button
          type="submit"
          disabled={submitting}
          className="px-4 py-1.5 bg-blue-600 text-white text-sm rounded hover:bg-blue-700 disabled:cursor-not-allowed disabled:opacity-60"
        >
          {submitting ? t("form.saving") : mode === "add" ? t("form.create") : t("form.save")}
        </button>
        <button
          type="button"
          disabled={submitting}
          onClick={onCancel}
          className="px-4 py-1.5 bg-gray-200 border border-gray-300 text-gray-700 text-sm rounded hover:bg-gray-300 disabled:cursor-not-allowed disabled:opacity-60"
        >
          {t("form.cancel")}
        </button>
      </div>
    </form>
  );
}

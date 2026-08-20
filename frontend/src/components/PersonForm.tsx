"use client";

import { useState, useEffect } from "react";
import { getPersonSchema } from "@/lib/api";
import type { ValidationIssue } from "@/lib/api";
import type { FieldConfig } from "@/lib/types";
import { useI18n } from "@/lib/i18n";
import ValidationMessages from "@/components/ValidationMessages";

interface Props {
  mode: "add" | "edit";
  initialData?: Record<string, unknown>;
  title: string;
  submitting?: boolean;
  validationIssues?: ValidationIssue[];
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
  onSubmit,
  onValidationClear,
  onCancel,
}: Props) {
  const { t } = useI18n();
  const [schema, setSchema] = useState<Record<string, FieldConfig>>({});
  const [formData, setFormData] = useState<Record<string, unknown>>(initialData);

  useEffect(() => {
    getPersonSchema().then((s) => setSchema(s as Record<string, FieldConfig>)).catch(console.error);
  }, []);

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

      <ValidationMessages
        issues={validationIssues}
        submitting={submitting}
        onOverride={() => onSubmit(formData, true)}
      />

      {Object.entries(schema).map(([field, config]) => {
        if (!isVisible(field, config)) return null;
        if (config.type === "image_url" || config.type === "image_url_array") return null;

        return (
          <div key={field}>
            <label className="block text-sm font-medium text-gray-600 mb-1">{config.label}</label>
            {config.type === "boolean" ? (
              <input
                type="checkbox"
                checked={formData[field] as boolean ?? config.default ?? false}
                onChange={(e) => handleChange(field, e.target.checked)}
                className="rounded"
              />
            ) : config.type === "date" ? (
              <input
                type="text"
                value={(formData[field] as string) || ""}
                onChange={(e) => handleChange(field, e.target.value)}
                placeholder="dd/mm/yyyy"
                className="w-full border rounded px-3 py-1.5 text-sm text-gray-900"
              />
            ) : (
              <input
                type="text"
                value={(formData[field] as string) || ""}
                onChange={(e) => handleChange(field, e.target.value)}
                className="w-full border rounded px-3 py-1.5 text-sm text-gray-900"
              />
            )}
            {validationIssues
              .filter((issue) => issue.field === field)
              .map((issue, index) => (
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

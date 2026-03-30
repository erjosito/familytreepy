"use client";

import { useState, useEffect } from "react";
import { getPersonSchema } from "@/lib/api";
import type { FieldConfig } from "@/lib/types";

interface Props {
  mode: "add" | "edit";
  initialData?: Record<string, unknown>;
  title: string;
  onSubmit: (data: Record<string, unknown>) => void;
  onCancel: () => void;
}

export default function PersonForm({ mode, initialData = {}, title, onSubmit, onCancel }: Props) {
  const [schema, setSchema] = useState<Record<string, FieldConfig>>({});
  const [formData, setFormData] = useState<Record<string, unknown>>(initialData);

  useEffect(() => {
    getPersonSchema().then((s) => setSchema(s as Record<string, FieldConfig>)).catch(console.error);
  }, []);

  useEffect(() => {
    setFormData(initialData);
  }, [initialData]);

  const handleChange = (field: string, value: unknown) => {
    setFormData((prev) => ({ ...prev, [field]: value }));
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
                type="date"
                value={(formData[field] as string) || ""}
                onChange={(e) => handleChange(field, e.target.value)}
                className="w-full border rounded px-3 py-1.5 text-sm"
              />
            ) : (
              <input
                type="text"
                value={(formData[field] as string) || ""}
                onChange={(e) => handleChange(field, e.target.value)}
                className="w-full border rounded px-3 py-1.5 text-sm"
              />
            )}
          </div>
        );
      })}

      <div className="flex gap-2 pt-2">
        <button
          type="submit"
          className="px-4 py-1.5 bg-blue-600 text-white text-sm rounded hover:bg-blue-700"
        >
          {mode === "add" ? "Create" : "Save"}
        </button>
        <button
          type="button"
          onClick={onCancel}
          className="px-4 py-1.5 border text-sm rounded hover:bg-gray-50"
        >
          Cancel
        </button>
      </div>
    </form>
  );
}

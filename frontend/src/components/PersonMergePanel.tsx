"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import {
  listPersons,
  mergePersons,
  previewPersonMerge,
  rollbackHistory,
  type MergePreview,
} from "@/lib/api";
import { useI18n } from "@/lib/i18n";
import { useToast } from "@/components/ToastProvider";

interface PersonOption {
  id: string;
  fullname: string;
  alias?: string;
}

export default function PersonMergePanel() {
  const { t } = useI18n();
  const toast = useToast();
  const [people, setPeople] = useState<PersonOption[]>([]);
  const [sourceId, setSourceId] = useState("");
  const [targetId, setTargetId] = useState("");
  const [preview, setPreview] = useState<MergePreview | null>(null);
  const [fieldChoices, setFieldChoices] = useState<Record<string, "source" | "target">>({});
  const [relationshipChoices, setRelationshipChoices] = useState<Record<string, "source" | "target">>({});
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState("");
  const previewRequest = useRef(0);

  const sortedPeople = useMemo(
    () => [...people].sort((left, right) => left.fullname.localeCompare(right.fullname)),
    [people],
  );

  useEffect(() => {
    void listPersons()
      .then(setPeople)
      .catch((loadError) => setError(String(loadError)));
  }, []);

  const resetPreview = () => {
    previewRequest.current += 1;
    setPreview(null);
    setFieldChoices({});
    setRelationshipChoices({});
    setError("");
    setBusy(false);
  };

  const loadPreview = async (
    nextFieldChoices = fieldChoices,
    nextRelationshipChoices = relationshipChoices,
  ) => {
    if (!sourceId || !targetId || sourceId === targetId || busy) return;
    const request = ++previewRequest.current;
    setBusy(true);
    setError("");
    try {
      const nextPreview = await previewPersonMerge({
        source_id: sourceId,
        target_id: targetId,
        field_choices: nextFieldChoices,
        relationship_choices: nextRelationshipChoices,
      });
      if (previewRequest.current === request) {
        setPreview(nextPreview);
      }
    } catch (previewError) {
      if (previewRequest.current === request) {
        setError(String(previewError));
      }
    } finally {
      if (previewRequest.current === request) {
        setBusy(false);
      }
    }
  };

  const chooseField = (field: string, choice: "source" | "target") => {
    const next = { ...fieldChoices, [field]: choice };
    setFieldChoices(next);
    void loadPreview(next, relationshipChoices);
  };

  const chooseRelationship = (key: string, choice: "source" | "target") => {
    const next = { ...relationshipChoices, [key]: choice };
    setRelationshipChoices(next);
    void loadPreview(fieldChoices, next);
  };

  const unresolvedCount = preview
    ? preview.field_conflicts.filter((conflict) => !fieldChoices[conflict.field]).length
      + preview.relationship_conflicts.filter((conflict) => !relationshipChoices[conflict.key]).length
    : 0;

  const execute = async () => {
    if (!preview || unresolvedCount > 0 || busy) return;
    if (!confirm(t("merge.confirm"))) return;
    setBusy(true);
    setError("");
    try {
      const result = await mergePersons({
        source_id: sourceId,
        target_id: targetId,
        field_choices: fieldChoices,
        relationship_choices: relationshipChoices,
        preview_token: preview.preview_token,
      });
      setPeople((current) => current.filter((person) => person.id !== sourceId));
      resetPreview();
      setSourceId("");
      toast.success(t("merge.completed"), {
        duration: 10000,
        action: {
          label: t("toast.undo"),
          onClick: async () => {
            try {
              await rollbackHistory(result.revision_id);
              setPeople(await listPersons());
              toast.success(t("toast.changeUndone"));
            } catch (rollbackError) {
              toast.error(t("toast.undoFailed"), { message: String(rollbackError) });
            }
          },
        },
      });
    } catch (mergeError) {
      setError(String(mergeError));
    } finally {
      setBusy(false);
    }
  };

  return (
    <section className="space-y-4" aria-labelledby="person-merge-heading">
      <div>
        <h2 id="person-merge-heading" className="text-xl font-semibold text-gray-900">
          {t("merge.title")}
        </h2>
        <p className="mt-1 text-sm text-gray-600">{t("merge.description")}</p>
      </div>

      <div className="grid gap-3 rounded-lg border bg-white p-4 md:grid-cols-2">
        <PersonSelect
          label={t("merge.source")}
          value={sourceId}
          people={sortedPeople}
          excludedId={targetId}
          onChange={(value) => {
            setSourceId(value);
            resetPreview();
          }}
        />
        <PersonSelect
          label={t("merge.target")}
          value={targetId}
          people={sortedPeople}
          excludedId={sourceId}
          onChange={(value) => {
            setTargetId(value);
            resetPreview();
          }}
        />
        <div className="md:col-span-2">
          <button
            type="button"
            disabled={!sourceId || !targetId || sourceId === targetId || busy}
            onClick={() => void loadPreview()}
            className="min-h-10 rounded bg-blue-600 px-4 text-sm font-medium text-white hover:bg-blue-700 disabled:opacity-50"
          >
            {busy ? t("merge.loading") : t("merge.preview")}
          </button>
        </div>
      </div>

      {error && (
        <div role="alert" className="break-words rounded-lg border border-red-200 bg-red-50 p-3 text-sm text-red-700">
          {error}
        </div>
      )}

      {preview && (
        <div className="space-y-4 rounded-lg border bg-white p-4 shadow-sm">
          <div>
            <h3 className="font-semibold text-gray-900">{t("merge.conflicts")}</h3>
            {preview.field_conflicts.length === 0 ? (
              <p className="mt-1 text-sm text-gray-600">{t("merge.noFieldConflicts")}</p>
            ) : (
              <div className="mt-2 overflow-x-auto">
                <table className="w-full min-w-[640px] text-sm">
                  <thead>
                    <tr className="border-b text-left text-gray-600">
                      <th className="p-2">{t("merge.field")}</th>
                      <th className="p-2">{t("merge.sourceValue")}</th>
                      <th className="p-2">{t("merge.targetValue")}</th>
                      <th className="p-2">{t("merge.keep")}</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y">
                    {preview.field_conflicts.map((conflict) => (
                      <tr key={conflict.field}>
                        <td className="p-2 font-medium">{conflict.field}</td>
                        <td className="p-2 break-all">{String(conflict.source)}</td>
                        <td className="p-2 break-all">{String(conflict.target)}</td>
                        <td className="p-2">
                          <select
                            value={fieldChoices[conflict.field] || ""}
                            onChange={(event) => chooseField(
                              conflict.field,
                              event.target.value as "source" | "target",
                            )}
                            className="rounded border px-2 py-1 text-gray-900"
                          >
                            <option value="">{t("merge.choose")}</option>
                            <option value="source">{t("merge.keepSource")}</option>
                            <option value="target">{t("merge.keepTarget")}</option>
                          </select>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>

          {preview.relationship_conflicts.length > 0 && (
            <div>
              <h3 className="font-semibold text-gray-900">{t("merge.relationshipConflicts")}</h3>
              <div className="mt-2 space-y-2">
                {preview.relationship_conflicts.map((conflict) => (
                  <label key={conflict.key} className="block rounded border p-3 text-sm">
                    <span className="font-medium">{conflict.key}</span>
                    <select
                      value={relationshipChoices[conflict.key] || ""}
                      onChange={(event) => chooseRelationship(
                        conflict.key,
                        event.target.value as "source" | "target",
                      )}
                      className="ml-3 rounded border px-2 py-1 text-gray-900"
                    >
                      <option value="">{t("merge.choose")}</option>
                      <option value="source">{t("merge.keepSource")}</option>
                      <option value="target">{t("merge.keepTarget")}</option>
                    </select>
                  </label>
                ))}
              </div>
            </div>
          )}

          <div className="grid gap-2 text-sm sm:grid-cols-2 lg:grid-cols-4">
            <Impact label={t("merge.pictures")} value={preview.pictures.length} />
            <Impact label={t("merge.notes")} value={preview.notes.length} />
            <Impact label={t("merge.repointed")} value={preview.relationships.repointed.length} />
            <Impact
              label={t("merge.removed")}
              value={
                preview.relationships.self_links_removed.length
                + preview.relationships.duplicates_removed.length
              }
            />
          </div>

          {unresolvedCount > 0 && (
            <p className="text-sm text-amber-700">
              {t("merge.unresolved").replace("{count}", String(unresolvedCount))}
            </p>
          )}
          <button
            type="button"
            disabled={unresolvedCount > 0 || busy}
            onClick={() => void execute()}
            className="min-h-10 rounded bg-red-600 px-4 text-sm font-medium text-white hover:bg-red-700 disabled:opacity-50"
          >
            {busy ? t("merge.merging") : t("merge.execute")}
          </button>
        </div>
      )}
    </section>
  );
}

function PersonSelect({
  label,
  value,
  people,
  excludedId,
  onChange,
}: {
  label: string;
  value: string;
  people: PersonOption[];
  excludedId: string;
  onChange: (value: string) => void;
}) {
  return (
    <label className="text-sm font-medium text-gray-700">
      {label}
      <select
        value={value}
        onChange={(event) => onChange(event.target.value)}
        className="mt-1 min-h-10 w-full rounded border px-3 text-gray-900"
      >
        <option value="">—</option>
        {people.filter((person) => person.id !== excludedId).map((person) => (
          <option key={person.id} value={person.id}>
            {person.fullname}{person.alias ? ` (${person.alias})` : ""}
          </option>
        ))}
      </select>
    </label>
  );
}

function Impact({ label, value }: { label: string; value: number }) {
  return (
    <div className="rounded bg-gray-50 p-3">
      <span className="block text-xs text-gray-500">{label}</span>
      <strong className="text-lg text-gray-900">{value}</strong>
    </div>
  );
}

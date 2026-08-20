"use client";

import { useState, useEffect, useCallback } from "react";
import { listPersons, getPerson, updatePerson, deletePerson, getValidationIssues, type ValidationIssue } from "@/lib/api";
import { useAdminView } from "@/lib/adminView";
import { useI18n } from "@/lib/i18n";
import { formatDate } from "@/lib/dateUtils";
import Link from "next/link";
import { useToast } from "@/components/ToastProvider";
import ValidationMessages from "@/components/ValidationMessages";

interface PersonRow {
  id: string;
  fullname: string;
  firstname: string;
  lastname: string;
  alias: string;
  birthdate: string;
  birthplace: string;
  isAlive: boolean;
  deathdate: string;
  gender: string;
}

export default function GridPage() {
  const { adminView } = useAdminView();
  const { t } = useI18n();
  const toast = useToast();
  const [persons, setPersons] = useState<PersonRow[]>([]);
  const [loading, setLoading] = useState(true);
  const [editingId, setEditingId] = useState<string | null>(null);
  const [draft, setDraft] = useState<Partial<PersonRow>>({});
  const [saving, setSaving] = useState(false);
  const [validationIssues, setValidationIssues] = useState<ValidationIssue[]>([]);
  const [deletingId, setDeletingId] = useState<string | null>(null);
  const [sortField, setSortField] = useState<keyof PersonRow>("lastname");
  const [sortDir, setSortDir] = useState<"asc" | "desc">("asc");
  const [filter, setFilter] = useState("");

  const fetchAll = useCallback(async () => {
    setLoading(true);
    try {
      const list = await listPersons();
      const details = await Promise.all(
        list.map(async (p) => {
          try {
            const d = await getPerson(p.id);
            const str = (v: unknown) => (typeof v === "string" ? v : "");
            return {
              id: p.id,
              fullname: p.fullname,
              firstname: str(d.firstname),
              lastname: str(d.lastname),
              alias: str(d.alias),
              birthdate: str(d.birthdate),
              birthplace: str(d.birthplace),
              isAlive: d.isAlive !== false && d.isAlive !== 0,
              deathdate: str(d.deathdate),
              gender: str(d.gender),
            };
          } catch {
            return {
              id: p.id, fullname: p.fullname,
              firstname: "", lastname: "", alias: "", birthdate: "",
              birthplace: "", isAlive: true, deathdate: "", gender: "",
            };
          }
        })
      );
      setPersons(details);
    } catch (err) {
      console.error("Failed to load persons:", err);
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => { fetchAll(); }, [fetchAll]);

  const startEdit = (p: PersonRow) => {
    setEditingId(p.id);
    setDraft({ ...p });
    setValidationIssues([]);
  };

  const cancelEdit = () => {
    setEditingId(null);
    setDraft({});
    setValidationIssues([]);
  };

  const updateDraft = (changes: Partial<PersonRow>) => {
    setValidationIssues([]);
    setDraft((current) => ({ ...current, ...changes }));
  };

  const saveEdit = async (overrideWarnings = false) => {
    if (!editingId || !draft) return;
    setSaving(true);
    try {
      await updatePerson(editingId, {
        firstname: draft.firstname,
        lastname: draft.lastname,
        alias: draft.alias,
        birthdate: draft.birthdate,
        birthplace: draft.birthplace,
        isAlive: draft.isAlive,
        deathdate: draft.isAlive ? "" : draft.deathdate,
        gender: draft.gender,
      }, overrideWarnings);
      setEditingId(null);
      setValidationIssues([]);
      await fetchAll();
      toast.success(t("toast.personSaved"));
    } catch (err) {
      console.error("Save failed:", err);
      const issues = getValidationIssues(err);
      if (issues) {
        setValidationIssues(issues);
        return;
      }
      toast.error(t("toast.personSaveFailed"), {
        action: {
          label: t("toast.retry"),
          onClick: () => saveEdit(overrideWarnings),
        },
      });
    } finally {
      setSaving(false);
    }
  };

  const handleSort = (field: keyof PersonRow) => {
    if (sortField === field) {
      setSortDir(sortDir === "asc" ? "desc" : "asc");
    } else {
      setSortField(field);
      setSortDir("asc");
    }
  };

  // Filter and sort
  const filtered = persons.filter((p) => {
    if (!filter) return true;
    const q = filter.toLowerCase();
    return p.fullname.toLowerCase().includes(q)
      || p.birthplace.toLowerCase().includes(q)
      || p.birthdate.includes(q);
  });

  const sorted = [...filtered].sort((a, b) => {
    const av = a[sortField];
    const bv = b[sortField];
    if (typeof av === "boolean") return (av === bv ? 0 : av ? -1 : 1) * (sortDir === "asc" ? 1 : -1);
    const cmp = String(av).localeCompare(String(bv));
    return sortDir === "asc" ? cmp : -cmp;
  });

  if (!adminView) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center">
        <p className="text-gray-500">{t("admin.title")} — admin only</p>
      </div>
    );
  }

  const SortIcon = ({ field }: { field: keyof PersonRow }) => {
    if (sortField !== field) return <span className="text-gray-300 ml-1">↕</span>;
    return <span className="text-blue-600 ml-1">{sortDir === "asc" ? "↑" : "↓"}</span>;
  };

  const columns: { key: keyof PersonRow; label: string; width: string }[] = [
    { key: "firstname", label: t("field.firstName"), width: "w-32" },
    { key: "lastname", label: t("field.lastName"), width: "w-36" },
    { key: "alias", label: t("field.alias"), width: "w-28" },
    { key: "gender", label: t("field.gender"), width: "w-20" },
    { key: "birthdate", label: t("field.birthdate"), width: "w-28" },
    { key: "birthplace", label: t("field.birthplace"), width: "w-36" },
    { key: "isAlive", label: t("field.status"), width: "w-24" },
    { key: "deathdate", label: t("field.deathDate"), width: "w-28" },
  ];

  return (
    <div className="min-h-screen bg-gray-50">
      <div className="max-w-7xl mx-auto p-3 sm:p-6 space-y-4">
        <div className="flex flex-col gap-3 sm:flex-row sm:items-center sm:justify-between">
          <h1 className="text-2xl font-bold text-gray-900">
            {t("grid.title")} <span className="text-gray-400 font-normal text-lg">({sorted.length})</span>
          </h1>
          <input
            type="text"
            value={filter}
            onChange={(e) => setFilter(e.target.value)}
            placeholder={t("grid.search")}
            className="w-full border rounded-lg px-3 py-2 text-sm text-gray-900 sm:w-64 sm:py-1.5"
          />
        </div>

        {loading ? (
          <div className="flex items-center justify-center py-20 text-gray-400">
            {t("toolbar.loading")}
          </div>
        ) : (
          <>
          {editingId && (
            <ValidationMessages
              issues={validationIssues}
              submitting={saving}
              onOverride={() => saveEdit(true)}
            />
          )}
          <div className="bg-white rounded-lg border shadow-sm overflow-x-auto">
            <table className="w-full text-sm">
              <thead className="bg-gray-50 border-b">
                <tr>
                  {columns.map((col) => (
                    <th
                      key={col.key}
                      onClick={() => handleSort(col.key)}
                      className={`text-left px-3 py-2.5 font-medium text-gray-600 cursor-pointer hover:bg-gray-100 select-none ${col.width}`}
                    >
                      {col.label}<SortIcon field={col.key} />
                    </th>
                  ))}
                  <th className="text-right px-3 py-2.5 font-medium text-gray-600 w-28">
                    {t("admin.actions")}
                  </th>
                </tr>
              </thead>
              <tbody className="divide-y">
                {sorted.map((p) => (
                  <tr key={p.id} className="hover:bg-gray-50">
                    {editingId === p.id ? (
                      <>
                        <td className="px-3 py-1.5">
                          <input
                            type="text" value={draft.firstname || ""}
                            onChange={(e) => updateDraft({ firstname: e.target.value })}
                            className="w-full border rounded px-2 py-1 text-sm text-gray-900"
                          />
                        </td>
                        <td className="px-3 py-1.5">
                          <input
                            type="text" value={draft.lastname || ""}
                            onChange={(e) => updateDraft({ lastname: e.target.value })}
                            className="w-full border rounded px-2 py-1 text-sm text-gray-900"
                          />
                        </td>
                        <td className="px-3 py-1.5">
                          <input
                            type="text" value={draft.alias || ""}
                            onChange={(e) => updateDraft({ alias: e.target.value })}
                            className="w-full border rounded px-2 py-1 text-sm text-gray-900"
                          />
                        </td>
                        <td className="px-3 py-1.5">
                          <select
                            value={draft.gender || ""}
                            onChange={(e) => updateDraft({ gender: e.target.value })}
                            className="w-full border rounded px-1 py-1 text-sm text-gray-900"
                          >
                            <option value="">—</option>
                            <option value="male">♂</option>
                            <option value="female">♀</option>
                          </select>
                        </td>
                        <td className="px-3 py-1.5">
                          <input
                            type="text" value={draft.birthdate ?? ""}
                            onChange={(e) => updateDraft({ birthdate: e.target.value })}
                            placeholder="YYYY-MM-DD"
                            className="w-full border rounded px-2 py-1 text-sm text-gray-900"
                          />
                        </td>
                        <td className="px-3 py-1.5">
                          <input
                            type="text" value={draft.birthplace ?? ""}
                            onChange={(e) => updateDraft({ birthplace: e.target.value })}
                            className="w-full border rounded px-2 py-1 text-sm text-gray-900"
                          />
                        </td>
                        <td className="px-3 py-1.5 text-center">
                          <input
                            type="checkbox" checked={draft.isAlive ?? true}
                            onChange={(e) => updateDraft({ isAlive: e.target.checked })}
                            className="rounded"
                          />
                        </td>
                        <td className="px-3 py-1.5">
                          {!draft.isAlive && (
                            <input
                              type="text" value={draft.deathdate ?? ""}
                              onChange={(e) => updateDraft({ deathdate: e.target.value })}
                              placeholder="YYYY-MM-DD"
                              className="w-full border rounded px-2 py-1 text-sm text-gray-900"
                            />
                          )}
                        </td>
                        <td className="px-3 py-1.5 text-right">
                          <button
                            onClick={() => void saveEdit()} disabled={saving}
                            className="text-xs px-2 py-1 bg-blue-600 text-white rounded hover:bg-blue-700 disabled:opacity-50 mr-1"
                          >
                            {saving ? "..." : t("admin.save")}
                          </button>
                          <button
                            onClick={cancelEdit}
                            className="text-xs px-2 py-1 bg-gray-200 border border-gray-300 rounded text-gray-700 hover:bg-gray-300"
                          >
                            {t("admin.cancel")}
                          </button>
                        </td>
                      </>
                    ) : (
                      <>
                        <td className="px-3 py-2.5 text-gray-900">
                          <Link href={`/person/?id=${p.id}`} className="hover:text-blue-600 hover:underline">
                            {p.firstname || "—"}
                          </Link>
                        </td>
                        <td className="px-3 py-2.5 text-gray-900">{p.lastname || "—"}</td>
                        <td className="px-3 py-2.5 text-gray-700">{p.alias || "—"}</td>
                        <td className="px-3 py-2.5 text-center text-gray-700">
                          {p.gender === "male" ? "♂" : p.gender === "female" ? "♀" : "—"}
                        </td>
                        <td className="px-3 py-2.5 text-gray-700">{formatDate(p.birthdate) || "—"}</td>
                        <td className="px-3 py-2.5 text-gray-700">{p.birthplace || "—"}</td>
                        <td className="px-3 py-2.5 text-center">
                          {p.isAlive ? (
                            <span className="text-xs px-1.5 py-0.5 rounded-full bg-green-100 text-green-700">{t("field.living")}</span>
                          ) : (
                            <span className="text-xs px-1.5 py-0.5 rounded-full bg-gray-200 text-gray-600">{t("field.deceased")}</span>
                          )}
                        </td>
                        <td className="px-3 py-2.5 text-gray-700">{formatDate(p.deathdate) || "—"}</td>
                        <td className="px-3 py-2.5 text-right">
                          <button
                            onClick={() => startEdit(p)}
                            className="text-xs px-2 py-1 border rounded text-blue-600 hover:bg-blue-50 mr-1"
                          >
                            {t("admin.edit")}
                          </button>
                          <button
                            onClick={async () => {
                              if (deletingId || !confirm(t("confirm.deletePerson"))) return;
                              setDeletingId(p.id);
                              try {
                                await deletePerson(p.id);
                                await fetchAll();
                                toast.success(t("toast.personDeleted"));
                              } catch (err) {
                                console.error("Delete failed:", err);
                                toast.error(t("toast.personDeleteFailed"));
                              } finally {
                                setDeletingId(null);
                              }
                            }}
                            disabled={deletingId === p.id}
                            className="text-xs px-2 py-1 border rounded text-red-600 hover:bg-red-50 disabled:cursor-not-allowed disabled:opacity-50"
                          >
                            {t("admin.remove")}
                          </button>
                        </td>
                      </>
                    )}
                  </tr>
                ))}
                {sorted.length === 0 && (
                  <tr>
                    <td colSpan={9} className="px-3 py-8 text-center text-gray-400">
                      {filter ? t("tag.noMatches") : t("admin.noUsers")}
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
          </>
        )}
      </div>
    </div>
  );
}

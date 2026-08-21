"use client";

import { useEffect, useState } from "react";
import {
  listHistory,
  rollbackHistory,
  type ChangeHistoryEntry,
  type HistoryFilters,
} from "@/lib/api";
import { useI18n } from "@/lib/i18n";
import { useToast } from "@/components/ToastProvider";

function operationLabel(
  operation: string,
  t: ReturnType<typeof useI18n>["t"],
): string {
  switch (operation) {
    case "create":
      return t("history.operationCreate");
    case "update":
      return t("history.operationUpdate");
    case "delete":
      return t("history.operationDelete");
    case "deactivate":
      return t("history.operationDeactivate");
    case "reactivate":
      return t("history.operationReactivate");
    case "rollback":
      return t("history.operationRollback");
    default:
      return operation;
  }
}

function entityLabel(entry: ChangeHistoryEntry): string {
  if (entry.entity_type === "relationship") {
    return `${entry.metadata.source || "?"} → ${entry.metadata.target || "?"}`;
  }
  const state = entry.after || entry.before;
  const attributes = state?.attributes;
  if (attributes && typeof attributes === "object") {
    const values = attributes as Record<string, unknown>;
    const firstname = typeof values.firstname === "string" ? values.firstname : "";
    const lastname = typeof values.lastname === "string" ? values.lastname : "";
    const name = `${firstname} ${lastname}`.trim();
    if (name) return name;
  }
  return entry.entity_id;
}

export default function ChangeHistoryPanel() {
  const { t } = useI18n();
  const toast = useToast();
  const [entries, setEntries] = useState<ChangeHistoryEntry[]>([]);
  const [filters, setFilters] = useState({
    actor: "",
    operation: "",
    entity_type: "",
    entity_id: "",
    from_date: "",
    to_date: "",
  });
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState("");
  const [rollingBack, setRollingBack] = useState<string | null>(null);

  const loadHistory = async (nextFilters: typeof filters = filters) => {
    setLoading(true);
    setError("");
    const requestFilters: HistoryFilters = {
      actor: nextFilters.actor,
      operation: nextFilters.operation,
      entity_type: nextFilters.entity_type,
      entity_id: nextFilters.entity_id,
      from_date: nextFilters.from_date
        ? `${nextFilters.from_date}T00:00:00Z`
        : undefined,
      to_date: nextFilters.to_date
        ? `${nextFilters.to_date}T23:59:59Z`
        : undefined,
      limit: 200,
    };
    try {
      setEntries(await listHistory(requestFilters));
    } catch (err) {
      setError(String(err));
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    void listHistory({ limit: 200 })
      .then(setEntries)
      .catch((err) => setError(String(err)))
      .finally(() => setLoading(false));
  }, []);

  const handleRollback = async (entry: ChangeHistoryEntry) => {
    if (!entry.can_rollback || rollingBack) return;
    if (!confirm(t("history.confirmRollback"))) return;
    setRollingBack(entry.id);
    try {
      await rollbackHistory(entry.id);
      await loadHistory();
      toast.success(t("toast.changeUndone"));
    } catch (err) {
      console.error("Rollback failed:", err);
      toast.error(t("toast.undoFailed"), { message: String(err) });
    } finally {
      setRollingBack(null);
    }
  };

  const clearFilters = () => {
    const empty = {
      actor: "",
      operation: "",
      entity_type: "",
      entity_id: "",
      from_date: "",
      to_date: "",
    };
    setFilters(empty);
    void loadHistory(empty);
  };

  return (
    <section className="space-y-3" aria-labelledby="change-history-heading">
      <div>
        <h2 id="change-history-heading" className="text-xl font-semibold text-gray-900">
          {t("history.title")}
        </h2>
        <p className="mt-1 text-sm text-gray-600">{t("history.description")}</p>
      </div>

      <form
        className="grid grid-cols-1 gap-3 rounded-lg border bg-white p-4 sm:grid-cols-2 lg:grid-cols-4"
        onSubmit={(event) => {
          event.preventDefault();
          void loadHistory();
        }}
      >
        <label className="text-xs font-medium text-gray-600">
          {t("history.actor")}
          <input
            value={filters.actor}
            onChange={(event) => setFilters({ ...filters, actor: event.target.value })}
            className="mt-1 w-full rounded border px-2 py-2 text-sm text-gray-900"
          />
        </label>
        <label className="text-xs font-medium text-gray-600">
          {t("history.operation")}
          <select
            value={filters.operation}
            onChange={(event) => setFilters({ ...filters, operation: event.target.value })}
            className="mt-1 w-full rounded border px-2 py-2 text-sm text-gray-900"
          >
            <option value="">{t("history.all")}</option>
            <option value="create">{t("history.operationCreate")}</option>
            <option value="update">{t("history.operationUpdate")}</option>
            <option value="delete">{t("history.operationDelete")}</option>
            <option value="deactivate">{t("history.operationDeactivate")}</option>
            <option value="reactivate">{t("history.operationReactivate")}</option>
            <option value="rollback">{t("history.operationRollback")}</option>
          </select>
        </label>
        <label className="text-xs font-medium text-gray-600">
          {t("history.entityType")}
          <select
            value={filters.entity_type}
            onChange={(event) => setFilters({ ...filters, entity_type: event.target.value })}
            className="mt-1 w-full rounded border px-2 py-2 text-sm text-gray-900"
          >
            <option value="">{t("history.all")}</option>
            <option value="person">{t("history.person")}</option>
            <option value="relationship">{t("history.relationship")}</option>
          </select>
        </label>
        <label className="text-xs font-medium text-gray-600">
          {t("history.entity")}
          <input
            value={filters.entity_id}
            onChange={(event) => setFilters({ ...filters, entity_id: event.target.value })}
            className="mt-1 w-full rounded border px-2 py-2 text-sm text-gray-900"
          />
        </label>
        <label className="text-xs font-medium text-gray-600">
          {t("history.from")}
          <input
            type="date"
            value={filters.from_date}
            onChange={(event) => setFilters({ ...filters, from_date: event.target.value })}
            className="mt-1 w-full rounded border px-2 py-2 text-sm text-gray-900"
          />
        </label>
        <label className="text-xs font-medium text-gray-600">
          {t("history.to")}
          <input
            type="date"
            value={filters.to_date}
            onChange={(event) => setFilters({ ...filters, to_date: event.target.value })}
            className="mt-1 w-full rounded border px-2 py-2 text-sm text-gray-900"
          />
        </label>
        <div className="flex items-end gap-2 sm:col-span-2">
          <button
            type="submit"
            className="min-h-10 rounded bg-blue-600 px-4 text-sm font-medium text-white hover:bg-blue-700"
          >
            {t("history.filter")}
          </button>
          <button
            type="button"
            onClick={clearFilters}
            className="min-h-10 rounded border px-4 text-sm text-gray-700 hover:bg-gray-50"
          >
            {t("history.clear")}
          </button>
        </div>
      </form>

      {error && (
        <div role="alert" className="rounded-lg border border-red-200 bg-red-50 p-3 text-sm text-red-700">
          {error}
        </div>
      )}

      <div className="overflow-hidden rounded-lg border bg-white shadow-sm">
        {loading ? (
          <p className="p-6 text-center text-sm text-gray-500">{t("history.loading")}</p>
        ) : entries.length === 0 ? (
          <p className="p-6 text-center text-sm text-gray-500">{t("history.empty")}</p>
        ) : (
          <ul className="divide-y">
            {entries.map((entry) => (
              <li key={entry.id} className="p-4">
                <div className="flex flex-col gap-3 sm:flex-row sm:items-start sm:justify-between">
                  <div className="min-w-0">
                    <div className="flex flex-wrap items-center gap-2">
                      <span className="rounded-full bg-blue-50 px-2 py-0.5 text-xs font-medium text-blue-700">
                        {operationLabel(entry.operation, t)}
                      </span>
                      <span className="text-sm font-medium text-gray-900">
                        {entityLabel(entry)}
                      </span>
                    </div>
                    <p className="mt-1 text-xs text-gray-500">
                      {entry.actor} · {new Date(entry.timestamp).toLocaleString()}
                    </p>
                    <details className="mt-2 text-xs text-gray-600">
                      <summary className="cursor-pointer text-blue-600 hover:underline">
                        {t("history.details")}
                      </summary>
                      <div className="mt-2 grid gap-2 lg:grid-cols-2">
                        <pre className="max-h-64 overflow-auto rounded bg-gray-50 p-2">
                          {t("history.before")}:{"\n"}
                          {JSON.stringify(entry.before, null, 2)}
                        </pre>
                        <pre className="max-h-64 overflow-auto rounded bg-gray-50 p-2">
                          {t("history.after")}:{"\n"}
                          {JSON.stringify(entry.after, null, 2)}
                        </pre>
                      </div>
                    </details>
                  </div>
                  {entry.can_rollback && (
                    <button
                      type="button"
                      onClick={() => void handleRollback(entry)}
                      disabled={rollingBack !== null}
                      className="min-h-10 shrink-0 rounded border border-amber-300 px-3 text-sm font-medium text-amber-800 hover:bg-amber-50 disabled:opacity-50"
                    >
                      {rollingBack === entry.id ? t("history.rollingBack") : t("history.rollback")}
                    </button>
                  )}
                </div>
              </li>
            ))}
          </ul>
        )}
      </div>
    </section>
  );
}

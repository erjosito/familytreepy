"use client";

import { useState, useEffect, useCallback } from "react";
import GraphViewer, { LAYOUT_OPTIONS, type LayoutMode } from "@/components/GraphViewer";
import DetailPanel from "@/components/DetailPanel";
import ContextMenu from "@/components/ContextMenu";
import PersonForm from "@/components/PersonForm";
import { getGraph, listPersons, getPerson, createPerson, updatePerson, deletePerson, createRelationship, deactivateRelationship, getStorageConfig } from "@/lib/api";
import type { GraphData, PersonNode, GraphEdge } from "@/lib/types";
import { useI18n } from "@/lib/i18n";
import { useAdminView } from "@/lib/adminView";
import { useToast } from "@/components/ToastProvider";

/** Strip non-primitive values (e.g. GML graphics objects) so React won't
 *  choke when rendering person fields. */
function sanitizePerson(raw: Record<string, unknown>): PersonNode {
  const clean: Record<string, unknown> = {};
  for (const [k, v] of Object.entries(raw)) {
    if (v === null || v === undefined) continue;
    if (Array.isArray(v) || typeof v !== "object") {
      clean[k] = v;
    }
  }
  return clean as PersonNode;
}

const EDGE_COLORS: Record<string, string> = {
  isChildOf: "#ef4444",
  isSpouseOf: "#3b82f6",
};

export default function ExplorePage() {
  const { t } = useI18n();
  const toast = useToast();
  const { adminView } = useAdminView();
  const [graphData, setGraphData] = useState<GraphData>({ nodes: [], edges: [] });
  const [personList, setPersonList] = useState<{ id: string; fullname: string }[]>([]);
  const [rootId, setRootId] = useState<string>("");
  const [degree, setDegree] = useState(2);
  const [selectedPerson, setSelectedPerson] = useState<PersonNode | null>(null);
  const [selectedRelationships, setSelectedRelationships] = useState<GraphEdge[]>([]);
  const [selectedSiblings, setSelectedSiblings] = useState<string[]>([]);
  const [contextMenu, setContextMenu] = useState<{ nodeId: string; x: number; y: number } | null>(null);
  const [loading, setLoading] = useState(true);
  const [formMode, setFormMode] = useState<{ type: "add_child" | "add_spouse" | "add_parent" | "edit"; nodeId: string; spouses?: { id: string; name: string }[] } | null>(null);
  const [otherParentId, setOtherParentId] = useState<string>("");
  const [linkMode, setLinkMode] = useState<{ type: "link_child" | "link_spouse" | "link_parent"; nodeId: string } | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const [layoutMode, setLayoutMode] = useState<LayoutMode>("breadthfirst");
  const [sasToken, setSasToken] = useState("");
  const [devMode, setDevMode] = useState(false);
  const showMobilePanel = Boolean(formMode || linkMode || selectedPerson);

  const closeSidePanel = () => {
    setFormMode(null);
    setLinkMode(null);
    setSelectedPerson(null);
  };

  const fetchGraph = useCallback(async () => {
    setLoading(true);
    try {
      const data = await getGraph(rootId || undefined, degree, true);
      setGraphData(data);
    } catch (err) {
      console.error("Failed to load graph:", err);
    } finally {
      setLoading(false);
    }
  }, [rootId, degree]);

  useEffect(() => {
    listPersons().then(setPersonList).catch(console.error);
    getStorageConfig().then((c) => setSasToken(c.sas_token)).catch(console.error);
  }, []);

  useEffect(() => {
    fetchGraph();
  }, [fetchGraph]);

  const handleNodeClick = async (nodeId: string) => {
    setContextMenu(null);
    try {
      const detail = await getPerson(nodeId);
      setSelectedPerson(sanitizePerson(detail));
      setSelectedRelationships((detail.relationships || []) as GraphEdge[]);
      setSelectedSiblings(Array.isArray(detail.siblings) ? detail.siblings as string[] : []);
    } catch (err) {
      console.error("Failed to load person:", err);
    }
  };

  const handleContextMenu = (nodeId: string, x: number, y: number) => {
    setContextMenu({ nodeId, x, y });
  };

  const handleContextAction = async (action: string) => {
    const nodeId = contextMenu?.nodeId;
    setContextMenu(null);
    if (!nodeId) return;
    await handleAction(action, nodeId);
  };

  const handleAction = async (action: string, nodeId: string) => {
    if (action === "center") {
      setRootId(nodeId);
    } else if (action === "story") {
      window.location.href = `/story/?id=${nodeId}&degree=3`;
    } else if (action === "delete") {
      if (!confirm(t("confirm.deletePerson")) || submitting) return;
      setSubmitting(true);
      try {
        await deletePerson(nodeId);
        setSelectedPerson(null);
        await fetchGraph();
        listPersons().then(setPersonList).catch(console.error);
        toast.success(t("toast.personDeleted"));
      } catch (err) {
        console.error("Delete person failed:", err);
        toast.error(t("toast.personDeleteFailed"), {
          action: { label: t("toast.retry"), onClick: () => handleAction("delete", nodeId) },
        });
      } finally {
        setSubmitting(false);
      }
    } else if (action === "edit" || action === "add_child" || action === "add_spouse" || action === "add_parent") {
      setLinkMode(null);
      setOtherParentId("");
      if (action === "add_child") {
        const detail = await getPerson(nodeId);
        const rels = (detail.relationships || []) as GraphEdge[];
        const seenSpouses = new Set<string>();
        const spouseList: { id: string; name: string }[] = [];
        for (const rel of rels) {
          if (rel.type === "isSpouseOf") {
            const otherId = rel.source === nodeId ? rel.target : rel.source;
            if (!seenSpouses.has(otherId)) {
              seenSpouses.add(otherId);
              const spouseName = personList.find((p) => p.id === otherId)?.fullname || otherId;
              spouseList.push({ id: otherId, name: spouseName });
            }
          }
        }
        setFormMode({ type: "add_child", nodeId, spouses: spouseList });
      } else {
        setFormMode({ type: action as "edit" | "add_spouse" | "add_parent", nodeId });
        if (action === "edit") {
          const detail = await getPerson(nodeId);
          setSelectedPerson(sanitizePerson(detail));
        }
      }
    } else if (action === "link_child" || action === "link_spouse" || action === "link_parent") {
      setLinkMode({ type: action, nodeId });
      setFormMode(null);
    }
  };

  const handleFormSubmit = async (data: Record<string, unknown>) => {
    if (!formMode || submitting) return;
    setSubmitting(true);
    try {
      if (formMode.type === "edit") {
        await updatePerson(formMode.nodeId, data);
        toast.success(t("toast.personSaved"));
      } else {
        const result = await createPerson(data);
        const relType = formMode.type === "add_spouse" ? "isSpouseOf" : "isChildOf";
        if (formMode.type === "add_child") {
          await createRelationship({ source: result.id, target: formMode.nodeId, type: "isChildOf" });
          if (otherParentId) {
            await createRelationship({ source: result.id, target: otherParentId, type: "isChildOf" });
          }
        } else if (formMode.type === "add_parent") {
          await createRelationship({ source: formMode.nodeId, target: result.id, type: "isChildOf" });
        } else if (formMode.type === "add_spouse") {
          await createRelationship({ source: formMode.nodeId, target: result.id, type: relType });
          await createRelationship({ source: result.id, target: formMode.nodeId, type: relType });
        }
        toast.success(t("toast.personCreated"));
      }
      setFormMode(null);
      await fetchGraph();
      listPersons().then(setPersonList).catch(console.error);
    } catch (err) {
      console.error("Form submit error:", err);
      const isEdit = formMode.type === "edit";
      toast.error(t(isEdit ? "toast.personSaveFailed" : "toast.personCreateFailed"), isEdit ? {
        action: { label: t("toast.retry"), onClick: () => handleFormSubmit(data) },
      } : undefined);
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div className="flex h-[calc(100dvh-3.5rem)] flex-col md:h-[calc(100dvh-3rem)]">
      {/* Toolbar */}
      <header className="flex flex-col gap-2 bg-white px-3 py-2 shadow-sm border-b md:flex-row md:items-center md:gap-4 md:px-4 md:py-3">
        <div className="flex min-w-0 items-center gap-2">
          <label className="shrink-0 text-sm text-gray-700">{t("toolbar.center")}</label>
          <select
            className="min-w-0 flex-1 border rounded px-2 py-2 text-sm text-gray-900 md:max-w-[250px] md:py-1"
            value={rootId}
            onChange={(e) => setRootId(e.target.value)}
          >
            <option value="">{t("toolbar.all")}</option>
            {[...personList].sort((a, b) => a.fullname.localeCompare(b.fullname)).map((p) => (
              <option key={p.id} value={p.id}>
                {p.fullname}
              </option>
            ))}
          </select>
        </div>

        <div className="flex min-w-0 items-center gap-2">
          <label className="shrink-0 text-sm text-gray-700">{t("toolbar.radius")}</label>
          <input
            type="range"
            min={1}
            max={10}
            value={degree}
            onChange={(e) => setDegree(Number(e.target.value))}
            className="min-w-16 flex-1 md:w-24 md:flex-none"
          />
          <span className="w-4 shrink-0 text-sm font-mono">{degree}</span>

          <label className="ml-1 shrink-0 text-sm text-gray-700">{t("toolbar.layout")}</label>
          <select
            className="min-w-0 flex-1 border rounded px-2 py-2 text-sm text-gray-900 md:flex-none md:py-1"
            value={layoutMode}
            onChange={(e) => setLayoutMode(e.target.value as LayoutMode)}
          >
            {LAYOUT_OPTIONS.map((opt) => (
              <option key={opt.value} value={opt.value}>
                {t(opt.labelKey as Parameters<typeof t>[0])}
              </option>
            ))}
          </select>
        </div>

        {adminView && (
          <div className="ml-auto flex items-center gap-2">
            <button
              onClick={() => setDevMode((v) => !v)}
              className={`text-xs px-2 py-1 rounded border ${devMode ? "bg-yellow-100 border-yellow-400 text-yellow-700" : "border-gray-300 text-gray-400 hover:text-gray-600"}`}
            >
              {devMode ? "🐛 Dev" : "🐛"}
            </button>
          </div>
        )}
      </header>

      {/* Main content */}
      <div className="relative flex min-h-0 flex-1 overflow-hidden">
        {/* Graph panel */}
        <div className="relative min-w-0 flex-1 md:flex-[65]">
          {loading && (
            <div className="absolute inset-0 flex items-center justify-center bg-white/60 z-10">
              <span className="text-gray-400">{t("toolbar.loading")}</span>
            </div>
          )}
          <GraphViewer
            data={graphData}
            layout={layoutMode}
            sasToken={sasToken}
            onNodeClick={handleNodeClick}
            onNodeDblClick={(nodeId) => setRootId(nodeId)}
            onContextMenu={handleContextMenu}
            relationshipColors={EDGE_COLORS}
          />
          {contextMenu && (
            <ContextMenu
              x={contextMenu.x}
              y={contextMenu.y}
              items={[
                { label: t("menu.centerOn"), action: "center" },
                { label: `📖 ${t("story.viewStory")}`, action: "story" },
                { label: t("menu.addChild"), action: "add_child" },
                { label: t("menu.addSpouse"), action: "add_spouse" },
                { label: t("menu.addParent"), action: "add_parent" },
                { label: t("menu.linkChild"), action: "link_child" },
                { label: t("menu.linkSpouse"), action: "link_spouse" },
                { label: t("menu.linkParent"), action: "link_parent" },
                { label: t("menu.editPerson"), action: "edit" },
                { label: t("menu.deletePerson"), action: "delete" },
              ]}
              onSelect={handleContextAction}
              onClose={() => setContextMenu(null)}
            />
          )}
        </div>

        {/* Detail panel */}
        {showMobilePanel && (
          <button
            type="button"
            className="fixed inset-0 z-20 bg-black/30 md:hidden"
            aria-label={t("form.cancel")}
            onClick={closeSidePanel}
          />
        )}
        <div className={`${showMobilePanel ? "fixed inset-x-0 bottom-0 z-30 max-h-[72dvh] rounded-t-2xl border-t shadow-2xl" : "hidden"} w-full overflow-y-auto bg-white md:static md:block md:max-h-none md:flex-[35] md:rounded-none md:border-l md:border-t-0 md:shadow-none`}>
          {formMode ? (
            <div className="p-4">
              <PersonForm
                mode={formMode.type === "edit" ? "edit" : "add"}
                initialData={formMode.type === "edit" && selectedPerson ? Object.fromEntries(
                  Object.entries(selectedPerson).filter(([k]) => !["id", "fullname", "relationships", "siblings"].includes(k))
                ) : {}}
                title={
                  formMode.type === "edit" ? t("form.editPerson") :
                  formMode.type === "add_child" ? t("form.addChild") :
                  formMode.type === "add_spouse" ? t("form.addSpouse") : t("form.addParent")
                }
                submitting={submitting}
                onSubmit={handleFormSubmit}
                onCancel={() => setFormMode(null)}
              />
              {/* Other parent selector for add_child */}
              {formMode.type === "add_child" && formMode.spouses && formMode.spouses.length > 0 && (
                <div className="mt-3 pt-3 border-t">
                  <label className="block text-sm font-medium text-gray-600 mb-1">{t("form.otherParent")}</label>
                  <select
                    value={otherParentId}
                    onChange={(e) => setOtherParentId(e.target.value)}
                    className="w-full border rounded px-3 py-1.5 text-sm text-gray-900"
                  >
                    <option value="">{t("form.noOtherParent")}</option>
                    {formMode.spouses.map((s) => (
                      <option key={s.id} value={s.id}>{s.name}</option>
                    ))}
                  </select>
                </div>
              )}
            </div>
          ) : linkMode ? (
            <LinkPersonPanel
              linkMode={linkMode}
              personList={personList}
              onLink={async (targetId) => {
                if (submitting) return;
                setSubmitting(true);
                try {
                  if (linkMode.type === "link_child") {
                    await createRelationship({ source: targetId, target: linkMode.nodeId, type: "isChildOf" });
                  } else if (linkMode.type === "link_parent") {
                    await createRelationship({ source: linkMode.nodeId, target: targetId, type: "isChildOf" });
                  } else if (linkMode.type === "link_spouse") {
                    await createRelationship({ source: linkMode.nodeId, target: targetId, type: "isSpouseOf" });
                    await createRelationship({ source: targetId, target: linkMode.nodeId, type: "isSpouseOf" });
                  }
                  setLinkMode(null);
                  await fetchGraph();
                  listPersons().then(setPersonList).catch(console.error);
                  toast.success(t("toast.relationshipCreated"));
                } catch (err) {
                  console.error("Link failed:", err);
                  toast.error(t("toast.relationshipCreateFailed"));
                } finally {
                  setSubmitting(false);
                }
              }}
              submitting={submitting}
              onCancel={() => setLinkMode(null)}
              t={t}
            />
          ) : (
            <DetailPanel
              person={selectedPerson}
              relationships={selectedRelationships}
              siblings={selectedSiblings}
              personList={personList}
              devMode={devMode}
              sasToken={sasToken}
              onClose={() => setSelectedPerson(null)}
              onPersonUpdated={async () => {
                await fetchGraph();
                listPersons().then(setPersonList);
                if (selectedPerson) {
                  const detail = await getPerson(selectedPerson.id);
                  setSelectedPerson(sanitizePerson(detail));
                  setSelectedRelationships((detail.relationships || []) as GraphEdge[]);
                  setSelectedSiblings(Array.isArray(detail.siblings) ? detail.siblings as string[] : []);
                }
              }}
              onAction={handleAction}
            />
          )}
        </div>
      </div>
    </div>
  );
}

/* ------------------------------------------------------------------ */
/* Link existing person panel                                          */
/* ------------------------------------------------------------------ */
function LinkPersonPanel({
  linkMode,
  personList,
  onLink,
  onCancel,
  submitting,
  t,
}: {
  linkMode: { type: string; nodeId: string };
  personList: { id: string; fullname: string }[];
  onLink: (targetId: string) => void | Promise<void>;
  onCancel: () => void;
  submitting: boolean;
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  t: (key: any) => string;
}) {
  const [query, setQuery] = useState("");

  const title =
    linkMode.type === "link_child" ? t("menu.linkChild") :
    linkMode.type === "link_spouse" ? t("menu.linkSpouse") : t("menu.linkParent");

  // Exclude the current node from results
  const filtered = query.trim()
    ? personList
        .filter((p) => p.id !== linkMode.nodeId && p.fullname.toLowerCase().includes(query.toLowerCase()))
        .slice(0, 20)
    : [];

  return (
    <div className="p-4 space-y-3">
      <h3 className="font-semibold text-lg text-gray-900">{title}</h3>
      <p className="text-sm text-gray-500">{t("link.hint")}</p>

      <input
        type="text"
        value={query}
        onChange={(e) => setQuery(e.target.value)}
        placeholder={t("tag.searchPlaceholder")}
        className="w-full border rounded px-3 py-2 text-sm text-gray-900"
        autoFocus
      />

      {filtered.length > 0 && (
        <div className="max-h-64 overflow-y-auto border rounded bg-white divide-y">
          {filtered.map((p) => (
            <button
              key={p.id}
              disabled={submitting}
              onClick={() => onLink(p.id)}
              className="w-full text-left px-3 py-2 text-sm text-gray-900 hover:bg-blue-50 transition-colors disabled:cursor-not-allowed disabled:opacity-60"
            >
              {p.fullname}
            </button>
          ))}
        </div>
      )}
      {query.trim() && filtered.length === 0 && (
        <p className="text-sm text-gray-400">{t("tag.noMatches")}</p>
      )}

      <button
        disabled={submitting}
        onClick={onCancel}
        className="px-4 py-1.5 bg-gray-200 border border-gray-300 text-gray-700 text-sm rounded hover:bg-gray-300 disabled:cursor-not-allowed disabled:opacity-60"
      >
        {t("form.cancel")}
      </button>
    </div>
  );
}

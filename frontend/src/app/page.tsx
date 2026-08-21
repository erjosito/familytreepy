"use client";

import { useState, useEffect, useCallback, useRef } from "react";
import GraphViewer, { LAYOUT_OPTIONS, type LayoutMode } from "@/components/GraphViewer";
import DetailPanel from "@/components/DetailPanel";
import ContextMenu from "@/components/ContextMenu";
import PersonForm from "@/components/PersonForm";
import PersonSearch, { type SearchablePerson } from "@/components/PersonSearch";
import PersonActionSheet from "@/components/PersonActionSheet";
import {
  createPerson,
  createRelationship,
  deletePerson,
  getGraph,
  getPerson,
  getStorageConfig,
  getValidationIssues,
  listPersons,
  updatePerson,
  type PendingPersonRelationship,
  type ValidationIssue,
} from "@/lib/api";
import type { GraphData, PersonNode, GraphEdge } from "@/lib/types";
import { useI18n } from "@/lib/i18n";
import { useAdminView } from "@/lib/adminView";
import { useToast } from "@/components/ToastProvider";
import { getPersonActions } from "@/lib/personActions";
import {
  DEFAULT_GRAPH_VIEW_STATE,
  parseGraphViewState,
  serializeGraphViewState,
  type GraphViewState,
} from "@/lib/graphViewState";

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

type HistoryMode = "none" | "push" | "replace";

function isNotFoundError(error: unknown): boolean {
  return error instanceof Error && error.message.startsWith("404:");
}

export default function ExplorePage() {
  const { t } = useI18n();
  const toast = useToast();
  const { adminView } = useAdminView();
  const [graphData, setGraphData] = useState<GraphData>({ nodes: [], edges: [] });
  const [personList, setPersonList] = useState<SearchablePerson[]>([]);
  const [rootId, setRootId] = useState<string>("");
  const [degree, setDegree] = useState(2);
  const [selectedId, setSelectedId] = useState("");
  const [selectedPerson, setSelectedPerson] = useState<PersonNode | null>(null);
  const [selectedRelationships, setSelectedRelationships] = useState<GraphEdge[]>([]);
  const [selectedSiblings, setSelectedSiblings] = useState<string[]>([]);
  const [contextMenu, setContextMenu] = useState<{ nodeId: string; x: number; y: number } | null>(null);
  const [actionSheetNodeId, setActionSheetNodeId] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [formMode, setFormMode] = useState<{ type: "add_child" | "add_spouse" | "add_parent" | "edit"; nodeId: string; spouses?: { id: string; name: string }[] } | null>(null);
  const [otherParentId, setOtherParentId] = useState<string>("");
  const [linkMode, setLinkMode] = useState<{ type: "link_child" | "link_spouse" | "link_parent"; nodeId: string } | null>(null);
  const [submitting, setSubmitting] = useState(false);
  const [formValidationIssues, setFormValidationIssues] = useState<ValidationIssue[]>([]);
  const [layoutMode, setLayoutMode] = useState<LayoutMode>("family");
  const [sasToken, setSasToken] = useState("");
  const [devMode, setDevMode] = useState(false);
  const [nodeFocus, setNodeFocus] = useState({ id: "", request: 0 });
  const [urlReady, setUrlReady] = useState(false);
  const historyModeRef = useRef<HistoryMode>("none");
  const detailRequestRef = useRef(0);
  const graphViewRef = useRef<GraphViewState>(DEFAULT_GRAPH_VIEW_STATE);
  graphViewRef.current = {
    center: rootId,
    radius: degree,
    layout: layoutMode,
    person: selectedId,
  };
  const showMobilePanel = Boolean(formMode || linkMode || selectedPerson);
  const personActions = getPersonActions(adminView);

  const queueHistory = useCallback((mode: Exclude<HistoryMode, "none">) => {
    if (mode === "push" || historyModeRef.current === "none") {
      historyModeRef.current = mode;
    }
  }, []);

  const updateRoot = useCallback((nextRootId: string, mode: "push" | "replace") => {
    if (graphViewRef.current.center === nextRootId) return;
    queueHistory(mode);
    graphViewRef.current = { ...graphViewRef.current, center: nextRootId };
    setRootId(nextRootId);
  }, [queueHistory]);

  const updateRadius = useCallback((nextRadius: number) => {
    if (graphViewRef.current.radius === nextRadius) return;
    queueHistory("replace");
    graphViewRef.current = { ...graphViewRef.current, radius: nextRadius };
    setDegree(nextRadius);
  }, [queueHistory]);

  const updateLayout = useCallback((nextLayout: LayoutMode) => {
    if (graphViewRef.current.layout === nextLayout) return;
    queueHistory("replace");
    graphViewRef.current = { ...graphViewRef.current, layout: nextLayout };
    setLayoutMode(nextLayout);
  }, [queueHistory]);

  const updateSelectedId = useCallback((nextSelectedId: string, mode: "push" | "replace") => {
    if (graphViewRef.current.person === nextSelectedId) return;
    queueHistory(mode);
    graphViewRef.current = { ...graphViewRef.current, person: nextSelectedId };
    setSelectedId(nextSelectedId);
  }, [queueHistory]);

  const clearPersonDetails = useCallback(() => {
    detailRequestRef.current += 1;
    setSelectedPerson(null);
    setSelectedRelationships([]);
    setSelectedSiblings([]);
  }, []);

  const closeSidePanel = () => {
    setFormMode(null);
    setLinkMode(null);
    updateSelectedId("", "push");
    clearPersonDetails();
  };

  const fetchGraph = useCallback(async () => {
    setLoading(true);
    try {
      const data = await getGraph(rootId || undefined, degree, true);
      setGraphData(data);
    } catch (err) {
      console.error("Failed to load graph:", err);
      if (rootId && isNotFoundError(err)) {
        updateRoot("", "replace");
      }
    } finally {
      setLoading(false);
    }
  }, [rootId, degree, updateRoot]);

  const loadPersonDetails = useCallback(async (personId: string) => {
    const request = detailRequestRef.current + 1;
    detailRequestRef.current = request;

    try {
      const detail = await getPerson(personId);
      if (detailRequestRef.current !== request) return;
      setSelectedPerson(sanitizePerson(detail));
      setSelectedRelationships((detail.relationships || []) as GraphEdge[]);
      setSelectedSiblings(Array.isArray(detail.siblings) ? detail.siblings as string[] : []);
    } catch (err) {
      if (detailRequestRef.current !== request) return;
      console.error("Failed to load person:", err);
      clearPersonDetails();
      if (isNotFoundError(err) && graphViewRef.current.person === personId) {
        updateSelectedId("", "replace");
      }
    }
  }, [clearPersonDetails, updateSelectedId]);

  useEffect(() => {
    listPersons().then(setPersonList).catch(console.error);
    getStorageConfig().then((c) => setSasToken(c.sas_token)).catch(console.error);
  }, []);

  useEffect(() => {
    const applyUrlState = () => {
      const nextState = parseGraphViewState(window.location.search);
      historyModeRef.current = "none";
      graphViewRef.current = nextState;
      setRootId(nextState.center);
      setDegree(nextState.radius);
      setLayoutMode(nextState.layout);
      setSelectedId(nextState.person);
      setFormMode(null);
      setLinkMode(null);
      setContextMenu(null);
      setActionSheetNodeId(null);
      clearPersonDetails();
      if (nextState.person) {
        setNodeFocus((current) => ({
          id: nextState.person,
          request: current.request + 1,
        }));
      }

      const normalizedSearch = serializeGraphViewState(window.location.search, nextState);
      if (normalizedSearch !== window.location.search) {
        window.history.replaceState(
          window.history.state,
          "",
          `${window.location.pathname}${normalizedSearch}${window.location.hash}`,
        );
      }
    };

    applyUrlState();
    setUrlReady(true);
    window.addEventListener("popstate", applyUrlState);
    return () => window.removeEventListener("popstate", applyUrlState);
  }, [clearPersonDetails]);

  useEffect(() => {
    if (!urlReady) return;
    fetchGraph();
  }, [fetchGraph, urlReady]);

  useEffect(() => {
    if (!urlReady) return;
    if (selectedId) {
      void loadPersonDetails(selectedId);
    } else {
      clearPersonDetails();
    }
  }, [clearPersonDetails, loadPersonDetails, selectedId, urlReady]);

  useEffect(() => {
    if (!urlReady) return;

    const nextSearch = serializeGraphViewState(window.location.search, {
      center: rootId,
      radius: degree,
      layout: layoutMode,
      person: selectedId,
    });
    const mode = historyModeRef.current;
    historyModeRef.current = "none";
    if (mode === "none" || nextSearch === window.location.search) return;

    const nextUrl = `${window.location.pathname}${nextSearch}${window.location.hash}`;
    if (mode === "push") {
      window.history.pushState(window.history.state, "", nextUrl);
    } else {
      window.history.replaceState(window.history.state, "", nextUrl);
    }
  }, [degree, layoutMode, rootId, selectedId, urlReady]);

  const handleNodeClick = useCallback((nodeId: string) => {
    setContextMenu(null);
    if (graphViewRef.current.person !== nodeId) {
      clearPersonDetails();
    }
    updateSelectedId(nodeId, "push");
  }, [clearPersonDetails, updateSelectedId]);

  const handleContextMenu = useCallback((nodeId: string, x: number, y: number) => {
    setContextMenu({ nodeId, x, y });
  }, []);

  const handleNodeDblClick = useCallback((nodeId: string) => {
    updateRoot(nodeId, "push");
  }, [updateRoot]);

  const handleContextAction = async (action: string) => {
    const nodeId = contextMenu?.nodeId;
    setContextMenu(null);
    if (!nodeId) return;
    await handleAction(action, nodeId);
  };

  const handleSheetAction = async (action: string) => {
    const nodeId = actionSheetNodeId;
    setActionSheetNodeId(null);
    if (nodeId) await handleAction(action, nodeId);
  };

  const handleAction = async (action: string, nodeId: string) => {
    if (action === "center") {
      updateRoot(nodeId, "push");
    } else if (action === "story") {
      window.location.href = `/story/?id=${nodeId}&degree=3`;
    } else if (action === "delete") {
      if (!confirm(t("confirm.deletePerson")) || submitting) return;
      setSubmitting(true);
      try {
        await deletePerson(nodeId);
        if (graphViewRef.current.person === nodeId) {
          updateSelectedId("", "replace");
          clearPersonDetails();
        }
        if (graphViewRef.current.center === nodeId) {
          updateRoot("", "replace");
        }
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
      setFormValidationIssues([]);
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
        if (action === "edit") {
          updateSelectedId(nodeId, "push");
          const detail = await getPerson(nodeId);
          setSelectedPerson(sanitizePerson(detail));
          setFormMode({ type: "edit", nodeId });
        } else {
          setFormMode({ type: action as "add_spouse" | "add_parent", nodeId });
        }
      }
    } else if (action === "link_child" || action === "link_spouse" || action === "link_parent") {
      setLinkMode({ type: action, nodeId });
      setFormMode(null);
    }
  };

  const handleFormSubmit = async (
    data: Record<string, unknown>,
    overrideWarnings = false,
  ) => {
    if (!formMode || submitting) return;
    setSubmitting(true);
    try {
      if (formMode.type === "edit") {
        await updatePerson(formMode.nodeId, data, overrideWarnings);
        toast.success(t("toast.personSaved"));
      } else {
        const relationships: PendingPersonRelationship[] = [];
        if (formMode.type === "add_child") {
          relationships.push({
            related_person_id: formMode.nodeId,
            type: "isChildOf",
            new_person_role: "source",
          });
          if (otherParentId) {
            relationships.push({
              related_person_id: otherParentId,
              type: "isChildOf",
              new_person_role: "source",
            });
          }
        } else if (formMode.type === "add_parent") {
          relationships.push({
            related_person_id: formMode.nodeId,
            type: "isChildOf",
            new_person_role: "target",
          });
        } else if (formMode.type === "add_spouse") {
          relationships.push(
            {
              related_person_id: formMode.nodeId,
              type: "isSpouseOf",
              new_person_role: "target",
            },
            {
              related_person_id: formMode.nodeId,
              type: "isSpouseOf",
              new_person_role: "source",
            },
          );
        }
        await createPerson(data, relationships, overrideWarnings);
        toast.success(t("toast.personCreated"));
      }
      setFormValidationIssues([]);
      setFormMode(null);
      await fetchGraph();
      listPersons().then(setPersonList).catch(console.error);
    } catch (err) {
      console.error("Form submit error:", err);
      const validationIssues = getValidationIssues(err);
      if (validationIssues) {
        setFormValidationIssues(validationIssues);
        return;
      }
      const isEdit = formMode.type === "edit";
      toast.error(t(isEdit ? "toast.personSaveFailed" : "toast.personCreateFailed"), isEdit ? {
        action: {
          label: t("toast.retry"),
          onClick: () => handleFormSubmit(data, overrideWarnings),
        },
      } : undefined);
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div className="flex h-[calc(100dvh-3.5rem)] flex-col md:h-[calc(100dvh-3rem)]">
      {/* Toolbar */}
      <header className="flex flex-col gap-2 bg-white px-3 py-2 shadow-sm border-b md:flex-row md:items-center md:gap-4 md:px-4 md:py-3">
        <PersonSearch
          persons={personList}
          onSelect={(person) => {
            setNodeFocus((current) => ({ id: person.id, request: current.request + 1 }));
            updateRoot(person.id, "push");
            handleNodeClick(person.id);
          }}
        />
        <div className="flex min-w-0 items-center gap-2">
          <label className="shrink-0 text-sm text-gray-700">{t("toolbar.center")}</label>
          <select
            className="min-w-0 flex-1 border rounded px-2 py-2 text-sm text-gray-900 md:max-w-[250px] md:py-1"
            value={rootId}
            onChange={(e) => updateRoot(e.target.value, "push")}
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
            onChange={(e) => updateRadius(Number(e.target.value))}
            className="min-w-16 flex-1 md:w-24 md:flex-none"
          />
          <span className="w-4 shrink-0 text-sm font-mono">{degree}</span>

          <label className="ml-1 shrink-0 text-sm text-gray-700">{t("toolbar.layout")}</label>
          <select
            className="min-w-0 flex-1 border rounded px-2 py-2 text-sm text-gray-900 md:flex-none md:py-1"
            value={layoutMode}
            onChange={(e) => updateLayout(e.target.value as LayoutMode)}
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
            onNodeClick={handleNodeClick}
            onNodeDblClick={handleNodeDblClick}
            onContextMenu={handleContextMenu}
            onNodeLongPress={setActionSheetNodeId}
            relationshipColors={EDGE_COLORS}
            focusNodeId={nodeFocus.id}
            focusRequest={nodeFocus.request}
          />
          {contextMenu && (
            <ContextMenu
              x={contextMenu.x}
              y={contextMenu.y}
              items={personActions.map((item) => ({
                label: t(item.labelKey),
                action: item.action,
              }))}
              onSelect={handleContextAction}
              onClose={() => setContextMenu(null)}
            />
          )}
          {actionSheetNodeId && (
            <PersonActionSheet
              personName={personList.find((person) => person.id === actionSheetNodeId)?.fullname || t("search.unknown")}
              actions={personActions}
              onSelect={handleSheetAction}
              onClose={() => setActionSheetNodeId(null)}
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
                key={`${formMode.type}-${formMode.nodeId}`}
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
                validationIssues={formValidationIssues}
                onSubmit={handleFormSubmit}
                onValidationClear={() => setFormValidationIssues([])}
                onCancel={() => {
                  setFormValidationIssues([]);
                  setFormMode(null);
                }}
              />
              {/* Other parent selector for add_child */}
              {formMode.type === "add_child" && formMode.spouses && formMode.spouses.length > 0 && (
                <div className="mt-3 pt-3 border-t">
                  <label className="block text-sm font-medium text-gray-600 mb-1">{t("form.otherParent")}</label>
                  <select
                    value={otherParentId}
                    onChange={(e) => {
                      setOtherParentId(e.target.value);
                      setFormValidationIssues([]);
                    }}
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
                  const relationships =
                    linkMode.type === "link_child"
                      ? [{ source: targetId, target: linkMode.nodeId, type: "isChildOf" }]
                      : linkMode.type === "link_parent"
                        ? [{ source: linkMode.nodeId, target: targetId, type: "isChildOf" }]
                        : [
                            { source: linkMode.nodeId, target: targetId, type: "isSpouseOf" },
                            { source: targetId, target: linkMode.nodeId, type: "isSpouseOf" },
                          ];
                  const saveRelationships = async (overrideWarnings: boolean) => {
                    for (const relationship of relationships) {
                      await createRelationship(relationship, overrideWarnings);
                    }
                  };

                  try {
                    await saveRelationships(false);
                  } catch (err) {
                    const issues = getValidationIssues(err);
                    if (
                      !issues ||
                      issues.length === 0 ||
                      issues.some((issue) => issue.severity === "error")
                    ) {
                      throw err;
                    }
                    const confirmed = window.confirm(
                      `${t("validation.warningTitle")}\n\n` +
                      `${issues.map((issue) => `• ${issue.message}`).join("\n")}\n\n` +
                      t("validation.confirmOverride"),
                    );
                    if (!confirmed) return;
                    await saveRelationships(true);
                  }
                  setLinkMode(null);
                  await fetchGraph();
                  listPersons().then(setPersonList).catch(console.error);
                  toast.success(t("toast.relationshipCreated"));
                } catch (err) {
                  console.error("Link failed:", err);
                  const issues = getValidationIssues(err);
                  toast.error(t("toast.relationshipCreateFailed"), {
                    message: issues?.map((issue) => issue.message).join(" "),
                  });
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
              onClose={() => {
                updateSelectedId("", "push");
                clearPersonDetails();
              }}
              onPersonUpdated={async () => {
                await fetchGraph();
                listPersons().then(setPersonList);
                if (selectedId) {
                  const detail = await getPerson(selectedId);
                  setSelectedPerson(sanitizePerson(detail));
                  setSelectedRelationships((detail.relationships || []) as GraphEdge[]);
                  setSelectedSiblings(Array.isArray(detail.siblings) ? detail.siblings as string[] : []);
                }
              }}
              onAction={handleAction}
              actions={personActions}
              onOpenActions={setActionSheetNodeId}
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

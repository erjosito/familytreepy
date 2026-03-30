"use client";

import { useState, useEffect, useCallback } from "react";
import GraphViewer, { LAYOUT_OPTIONS, type LayoutMode } from "@/components/GraphViewer";
import DetailPanel from "@/components/DetailPanel";
import ContextMenu from "@/components/ContextMenu";
import PersonForm from "@/components/PersonForm";
import { getGraph, listPersons, getPerson, createPerson, updatePerson, deletePerson, createRelationship, deactivateRelationship, getStorageConfig } from "@/lib/api";
import type { GraphData, PersonNode, GraphEdge } from "@/lib/types";

const CONTEXT_MENU_ITEMS = [
  { label: "Add child", action: "add_child" },
  { label: "Add spouse", action: "add_spouse" },
  { label: "Add parent", action: "add_parent" },
  { label: "Edit person", action: "edit" },
  { label: "Delete person", action: "delete" },
];

const EDGE_COLORS: Record<string, string> = {
  isChildOf: "#ef4444",
  isSpouseOf: "#3b82f6",
};

export default function ExplorePage() {
  const [graphData, setGraphData] = useState<GraphData>({ nodes: [], edges: [] });
  const [personList, setPersonList] = useState<{ id: string; fullname: string }[]>([]);
  const [rootId, setRootId] = useState<string>("");
  const [degree, setDegree] = useState(2);
  const [selectedPerson, setSelectedPerson] = useState<PersonNode | null>(null);
  const [selectedRelationships, setSelectedRelationships] = useState<GraphEdge[]>([]);
  const [selectedSiblings, setSelectedSiblings] = useState<string[]>([]);
  const [contextMenu, setContextMenu] = useState<{ nodeId: string; x: number; y: number } | null>(null);
  const [loading, setLoading] = useState(true);
  const [formMode, setFormMode] = useState<{ type: "add_child" | "add_spouse" | "add_parent" | "edit"; nodeId: string } | null>(null);
  const [layoutMode, setLayoutMode] = useState<LayoutMode>("breadthfirst");
  const [sasToken, setSasToken] = useState("");
  const [devMode, setDevMode] = useState(false);

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
      setSelectedPerson(detail);
      setSelectedRelationships((detail.relationships || []) as GraphEdge[]);
      setSelectedSiblings(detail.siblings || []);
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

    if (action === "delete") {
      if (confirm("Delete this person?")) {
        await deletePerson(nodeId);
        setSelectedPerson(null);
        await fetchGraph();
        listPersons().then(setPersonList);
      }
    } else if (action === "edit" || action === "add_child" || action === "add_spouse" || action === "add_parent") {
      setFormMode({ type: action as "edit" | "add_child" | "add_spouse" | "add_parent", nodeId });
      if (action === "edit") {
        const detail = await getPerson(nodeId);
        setSelectedPerson(detail);
      }
    }
  };

  const handleFormSubmit = async (data: Record<string, unknown>) => {
    if (!formMode) return;
    try {
      if (formMode.type === "edit") {
        await updatePerson(formMode.nodeId, data);
      } else {
        const result = await createPerson(data);
        const relType = formMode.type === "add_spouse" ? "isSpouseOf" : "isChildOf";
        if (formMode.type === "add_child") {
          await createRelationship({ source: result.id, target: formMode.nodeId, type: "isChildOf" });
        } else if (formMode.type === "add_parent") {
          await createRelationship({ source: formMode.nodeId, target: result.id, type: "isChildOf" });
        } else if (formMode.type === "add_spouse") {
          await createRelationship({ source: formMode.nodeId, target: result.id, type: relType });
          await createRelationship({ source: result.id, target: formMode.nodeId, type: relType });
        }
      }
      setFormMode(null);
      await fetchGraph();
      listPersons().then(setPersonList);
    } catch (err) {
      console.error("Form submit error:", err);
    }
  };

  return (
    <div className="flex flex-col h-screen">
      {/* Toolbar */}
      <header className="flex items-center gap-4 px-4 py-3 bg-white border-b shadow-sm flex-wrap">
        <label className="text-sm text-gray-700">Center:</label>
        <select
          className="border rounded px-2 py-1 text-sm text-gray-900 max-w-[250px]"
          value={rootId}
          onChange={(e) => setRootId(e.target.value)}
        >
          <option value="">All</option>
          {personList.map((p) => (
            <option key={p.id} value={p.id}>
              {p.fullname}
            </option>
          ))}
        </select>

        <label className="text-sm text-gray-700">Radius:</label>
        <input
          type="range"
          min={1}
          max={10}
          value={degree}
          onChange={(e) => setDegree(Number(e.target.value))}
          className="w-24"
        />
        <span className="text-sm font-mono w-4">{degree}</span>

        <label className="text-sm text-gray-700">Layout:</label>
        <select
          className="border rounded px-2 py-1 text-sm text-gray-900"
          value={layoutMode}
          onChange={(e) => setLayoutMode(e.target.value as LayoutMode)}
        >
          {LAYOUT_OPTIONS.map((opt) => (
            <option key={opt.value} value={opt.value}>
              {opt.label}
            </option>
          ))}
        </select>

        <div className="ml-auto flex items-center gap-2">
          <button
            onClick={() => setDevMode((v) => !v)}
            className={`text-xs px-2 py-1 rounded border ${devMode ? "bg-yellow-100 border-yellow-400 text-yellow-700" : "border-gray-300 text-gray-400 hover:text-gray-600"}`}
          >
            {devMode ? "🐛 Dev" : "🐛"}
          </button>
        </div>
      </header>

      {/* Main content */}
      <div className="flex flex-1 overflow-hidden">
        {/* Graph panel */}
        <div className="flex-[65] relative">
          {loading && (
            <div className="absolute inset-0 flex items-center justify-center bg-white/60 z-10">
              <span className="text-gray-400">Loading...</span>
            </div>
          )}
          <GraphViewer
            data={graphData}
            layout={layoutMode}
            sasToken={sasToken}
            onNodeClick={handleNodeClick}
            onContextMenu={handleContextMenu}
            relationshipColors={EDGE_COLORS}
          />
          {contextMenu && (
            <ContextMenu
              x={contextMenu.x}
              y={contextMenu.y}
              items={CONTEXT_MENU_ITEMS}
              onSelect={handleContextAction}
              onClose={() => setContextMenu(null)}
            />
          )}
        </div>

        {/* Detail panel */}
        <div className="flex-[35] border-l bg-white overflow-y-auto">
          {formMode ? (
            <div className="p-4">
              <PersonForm
                mode={formMode.type === "edit" ? "edit" : "add"}
                initialData={formMode.type === "edit" && selectedPerson ? Object.fromEntries(
                  Object.entries(selectedPerson).filter(([k]) => !["id", "fullname", "relationships", "siblings"].includes(k))
                ) : {}}
                title={
                  formMode.type === "edit" ? "Edit Person" :
                  formMode.type === "add_child" ? "Add Child" :
                  formMode.type === "add_spouse" ? "Add Spouse" : "Add Parent"
                }
                onSubmit={handleFormSubmit}
                onCancel={() => setFormMode(null)}
              />
            </div>
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
                  setSelectedPerson(detail);
                  setSelectedRelationships((detail.relationships || []) as GraphEdge[]);
                  setSelectedSiblings(detail.siblings || []);
                }
              }}
            />
          )}
        </div>
      </div>
    </div>
  );
}

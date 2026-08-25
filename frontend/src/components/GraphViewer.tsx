"use client";

import { useEffect, useRef, useCallback } from "react";
import cytoscape, { type Core, type EventObject, type LayoutOptions, type Position } from "cytoscape";
import fcose from "cytoscape-fcose";
import type { LayoutMode } from "@/lib/graphViewState";
import type { GraphData } from "@/lib/types";
import { useI18n } from "@/lib/i18n";

cytoscape.use(fcose);

const DEFAULT_RELATIONSHIP_COLORS: Record<string, string> = {};

interface SavedGraphState {
  signature: string;
  positions: Record<string, Position>;
  pan: Position;
  zoom: number;
}

export type { LayoutMode } from "@/lib/graphViewState";

export const LAYOUT_OPTIONS: { value: LayoutMode; labelKey: string }[] = [
  { value: "family", labelKey: "layout.family" },
  { value: "breadthfirst", labelKey: "layout.legacyHierarchical" },
  { value: "concentric", labelKey: "layout.radial" },
  { value: "cose", labelKey: "layout.forceDirected" },
  { value: "grid", labelKey: "layout.grid" },
  { value: "circle", labelKey: "layout.circle" },
];

interface Props {
  data: GraphData;
  layout?: LayoutMode;
  onNodeClick?: (nodeId: string) => void;
  onNodeDblClick?: (nodeId: string) => void;
  onContextMenu?: (nodeId: string, x: number, y: number) => void;
  onNodeLongPress?: (nodeId: string) => void;
  relationshipColors?: Record<string, string>;
  focusNodeId?: string;
  focusRequest?: number;
}

/**
 * Sugiyama-style hierarchical layout for family trees.
 * - Groups nodes by generation level (from backend)
 * - Keeps spouses adjacent as a family unit
 * - Orders nodes within each row via barycenter heuristic to minimise edge crossings
 * - Centers parents above their children
 */
export function buildHierarchicalLayout(
  elements: { data: Record<string, unknown> }[],
  groupSiblings = false,
): LayoutOptions {
  const nodeEls = elements.filter((el) => !el.data.source);
  const edgeEls = elements.filter((el) => el.data.source);

  // --- 1. Level map & node→level -----------------------------------------
  const levelMap = new Map<number, string[]>(); // level → ids
  const nodeLevel = new Map<string, number>();
  for (const el of nodeEls) {
    const id = el.data.id as string;
    const lv = typeof el.data.level === "number" ? el.data.level : 0;
    nodeLevel.set(id, lv);
    if (!levelMap.has(lv)) levelMap.set(lv, []);
    levelMap.get(lv)!.push(id);
  }
  const sortedLevels = [...levelMap.keys()].sort((a, b) => a - b);
  if (sortedLevels.length === 0) {
    return { name: "preset", positions: () => ({ x: 0, y: 0 }), fit: true, padding: 40 } as unknown as LayoutOptions;
  }

  // --- 2. Relationship maps ----------------------------------------------
  // isChildOf: source=child → target=parent
  const parentsOf = new Map<string, string[]>(); // child → parents
  const childrenOf = new Map<string, string[]>(); // parent → children
  const spousesOf = new Map<string, string[]>();

  for (const e of edgeEls) {
    const src = e.data.source as string;
    const tgt = e.data.target as string;
    const type = e.data.type as string;
    if (type === "isChildOf") {
      if (!parentsOf.has(src)) parentsOf.set(src, []);
      parentsOf.get(src)!.push(tgt);
      if (!childrenOf.has(tgt)) childrenOf.set(tgt, []);
      childrenOf.get(tgt)!.push(src);
    } else if (type === "isSpouseOf") {
      if (!spousesOf.has(src)) spousesOf.set(src, []);
      if (!spousesOf.has(tgt)) spousesOf.set(tgt, []);
      spousesOf.get(src)!.push(tgt);
      spousesOf.get(tgt)!.push(src);
    }
  }

  // --- 3. Family units (spouse groups) ------------------------------------
  const nodeToUnit = new Map<string, number>(); // nodeId → unitIndex
  const units: string[][] = [];
  const assigned = new Set<string>();
  for (const id of nodeLevel.keys()) {
    if (assigned.has(id)) continue;
    const group = [id];
    assigned.add(id);
    // flood-fill through spouse edges at the same level
    const queue = [id];
    while (queue.length) {
      const cur = queue.pop()!;
      for (const sp of spousesOf.get(cur) || []) {
        if (!assigned.has(sp) && nodeLevel.get(sp) === nodeLevel.get(id)) {
          assigned.add(sp);
          group.push(sp);
          queue.push(sp);
        }
      }
    }
    const idx = units.length;
    units.push(group);
    for (const m of group) nodeToUnit.set(m, idx);
  }

  // Group sibling family units into blocks. A spouse may have a different
  // birth family, so choose the parent signature shared by the most people
  // on this level as the unit's anchor.
  const siblingBlockByUnit = new Map<number, string>();
  if (groupSiblings) {
    for (const lv of sortedLevels) {
      const ids = levelMap.get(lv)!;
      const signatureByNode = new Map<string, string>();
      const signatureFrequency = new Map<string, number>();
      for (const id of ids) {
        const signature = [...(parentsOf.get(id) || [])].sort().join("\u0000");
        if (!signature) continue;
        signatureByNode.set(id, signature);
        signatureFrequency.set(signature, (signatureFrequency.get(signature) || 0) + 1);
      }

      const levelUnits = new Set(ids.map((id) => nodeToUnit.get(id)!));
      for (const unitIndex of levelUnits) {
        const candidate = units[unitIndex]
          .map((member) => signatureByNode.get(member))
          .filter((signature): signature is string => Boolean(signature))
          .sort((a, b) =>
            (signatureFrequency.get(b) || 0) - (signatureFrequency.get(a) || 0)
            || a.localeCompare(b)
          )[0];
        siblingBlockByUnit.set(
          unitIndex,
          candidate && (signatureFrequency.get(candidate) || 0) > 1
            ? `siblings:${candidate}`
            : `unit:${unitIndex}`,
        );
      }
    }
  }

  // --- 4. Barycenter ordering (multiple passes) --------------------------
  // pos tracks the ordering index of each node within its level row.
  const pos = new Map<string, number>();

  // Initial ordering: arbitrary
  for (const lv of sortedLevels) {
    levelMap.get(lv)!.forEach((id, i) => pos.set(id, i));
  }

  const reorderLevel = (level: number, refGetter: (id: string) => string[]) => {
    const ids = levelMap.get(level)!;
    // Compute barycenter per node
    const bary = new Map<string, number>();
    for (const id of ids) {
      const refs = refGetter(id).filter((r) => pos.has(r));
      if (refs.length > 0) {
        bary.set(id, refs.reduce((s, r) => s + pos.get(r)!, 0) / refs.length);
      } else {
        bary.set(id, pos.get(id) ?? 0);
      }
    }

    // Group by family unit and compute unit barycenter
    type UnitBary = { unit: number; b: number; members: string[] };
    const unitBary = new Map<number, UnitBary>();
    for (const id of ids) {
      const u = nodeToUnit.get(id) ?? -1;
      if (!unitBary.has(u)) unitBary.set(u, { unit: u, b: 0, members: [] });
      unitBary.get(u)!.members.push(id);
    }
    for (const [, ub] of unitBary) {
      ub.b = ub.members.reduce((s, m) => s + (bary.get(m) ?? 0), 0) / ub.members.length;
    }

    // Sort sibling blocks by barycenter, then family units within each block.
    // This keeps siblings and each sibling's spouse contiguous.
    const blocks = new Map<string, { b: number; units: UnitBary[] }>();
    for (const unit of unitBary.values()) {
      const blockKey = groupSiblings
        ? siblingBlockByUnit.get(unit.unit) || `unit:${unit.unit}`
        : `unit:${unit.unit}`;
      if (!blocks.has(blockKey)) blocks.set(blockKey, { b: 0, units: [] });
      blocks.get(blockKey)!.units.push(unit);
    }
    for (const block of blocks.values()) {
      block.b = block.units.reduce((sum, unit) => sum + unit.b, 0) / block.units.length;
      block.units.sort((a, b) => a.b - b.b || a.unit - b.unit);
    }
    const sortedBlocks = [...blocks.values()].sort((a, b) => a.b - b.b);
    let p = 0;
    for (const block of sortedBlocks) {
      for (const unit of block.units) {
        unit.members.sort((a, b) => (pos.get(a) ?? 0) - (pos.get(b) ?? 0));
        for (const member of unit.members) {
          pos.set(member, p++);
        }
      }
    }
  };

  // Additional sweeps improve convergence on larger radius-4 family graphs.
  for (let iter = 0; iter < 8; iter++) {
    // Top-down: order each level based on parents in the level above
    for (let li = 1; li < sortedLevels.length; li++) {
      reorderLevel(sortedLevels[li], (id) => [
        ...(parentsOf.get(id) || []),
        ...(spousesOf.get(id) || []),
      ]);
    }
    // Bottom-up: order each level based on children in the level below
    for (let li = sortedLevels.length - 2; li >= 0; li--) {
      reorderLevel(sortedLevels[li], (id) => [
        ...(childrenOf.get(id) || []),
        ...(spousesOf.get(id) || []),
      ]);
    }
  }

  // --- 5. Assign x,y coordinates ----------------------------------------
  const familyGap = 210;
  const siblingGap = 145;
  const spouseGap = 85;
  const ySpacing = 150;
  const positions: Record<string, { x: number; y: number }> = {};

  for (let row = 0; row < sortedLevels.length; row++) {
    const lv = sortedLevels[row];
    const ids = levelMap.get(lv)!;
    ids.sort((a, b) => (pos.get(a) ?? 0) - (pos.get(b) ?? 0));
    // Variable spacing: closer for spouses, wider between separate units
    let x = 0;
    for (let i = 0; i < ids.length; i++) {
      if (i > 0) {
        const leftUnit = nodeToUnit.get(ids[i - 1])!;
        const rightUnit = nodeToUnit.get(ids[i])!;
        const sameUnit = leftUnit === rightUnit;
        const sameSiblingBlock = groupSiblings
          && siblingBlockByUnit.get(leftUnit) === siblingBlockByUnit.get(rightUnit);
        x += sameUnit ? spouseGap : sameSiblingBlock ? siblingGap : familyGap;
      }
      positions[ids[i]] = { x, y: row * ySpacing };
    }
    // Center around x = 0
    if (ids.length > 0) {
      const center = (positions[ids[0]].x + positions[ids[ids.length - 1]].x) / 2;
      for (const id of ids) positions[id].x -= center;
    }
  }

  // --- 6. Center family units above children + resolve overlaps ----------
  // Helper: minimum gap between two adjacent nodes on the same level
  const minGap = (a: string, b: string) =>
    nodeToUnit.get(a) === nodeToUnit.get(b)
      ? spouseGap
      : groupSiblings
        && siblingBlockByUnit.get(nodeToUnit.get(a)!) === siblingBlockByUnit.get(nodeToUnit.get(b)!)
        ? siblingGap
        : familyGap;

  // Push apart any overlapping nodes on a level (symmetric push)
  const resolveOverlaps = (lv: number) => {
    const ids = levelMap.get(lv)!;
    if (ids.length < 2) return;
    ids.sort((a, b) => positions[a].x - positions[b].x);
    for (let pass = 0; pass < ids.length; pass++) {
      let moved = false;
      for (let i = 1; i < ids.length; i++) {
        const gap = positions[ids[i]].x - positions[ids[i - 1]].x;
        const req = minGap(ids[i - 1], ids[i]);
        if (gap < req) {
          const fix = (req - gap) / 2 + 0.5;
          positions[ids[i - 1]].x -= fix;
          positions[ids[i]].x += fix;
          moved = true;
        }
      }
      if (!moved) break;
    }
  };

  // Iterate: center parents above children, then fix overlaps.
  // Multiple passes let the layout converge when centering on one level
  // shifts things on another.
  for (let pass = 0; pass < 4; pass++) {
    for (let row = sortedLevels.length - 2; row >= 0; row--) {
      const lv = sortedLevels[row];
      const ids = levelMap.get(lv)!;

      // Process each family unit on this level
      const processed = new Set<number>();
      for (const id of ids) {
        const uIdx = nodeToUnit.get(id)!;
        if (processed.has(uIdx)) continue;
        processed.add(uIdx);

        const members = units[uIdx].filter((m) => nodeLevel.get(m) === lv);
        // Collect all children of this family unit
        const allChildren = new Set<string>();
        for (const m of members) {
          for (const c of childrenOf.get(m) || []) {
            if (positions[c]) allChildren.add(c);
          }
        }
        if (allChildren.size === 0) continue;

        const childXs = [...allChildren].map((c) => positions[c].x);
        const centerX =
          childXs.reduce((s, x) => s + x, 0) / childXs.length;

        // Shift the family unit so it is centred above its children
        members.sort((a, b) => positions[a].x - positions[b].x);
        const memberCenter =
          (positions[members[0]].x +
            positions[members[members.length - 1]].x) /
          2;
        const shift = centerX - memberCenter;
        for (const m of members) {
          positions[m].x += shift;
        }
      }

      // Fix any overlaps introduced by centering
      resolveOverlaps(lv);
    }
  }

  return {
    name: "preset",
    positions: (node: { id: () => string }) =>
      positions[node.id()] || { x: 0, y: 0 },
    fit: true,
    padding: 40,
  } as unknown as LayoutOptions;
}

function getLayoutConfig(mode: Exclude<LayoutMode, "family">, elements: { data: Record<string, unknown> }[]): LayoutOptions {
  if (mode === "breadthfirst") {
    return buildHierarchicalLayout(elements);
  }
  switch (mode) {
    case "concentric":
      return { name: "concentric", spacingFactor: 1.5, minNodeSpacing: 50 } as LayoutOptions;
    case "cose":
      return { name: "cose", animate: false, nodeRepulsion: () => 8000, idealEdgeLength: () => 100 } as LayoutOptions;
    case "grid":
      return { name: "grid", spacingFactor: 1.2 } as LayoutOptions;
    case "circle":
      return { name: "circle", spacingFactor: 1.5 } as LayoutOptions;
  }
}

function getGraphSignature(data: GraphData, layout: LayoutMode): string {
  const nodes = data.nodes.map((node) => node.id).sort().join("\u0000");
  const edges = data.edges
    .map((edge) => `${edge.source}\u0000${edge.target}\u0000${edge.type}`)
    .sort()
    .join("\u0001");
  return `${layout}\u0002${nodes}\u0002${edges}`;
}

export default function GraphViewer({ data, layout = "family", onNodeClick, onNodeDblClick, onContextMenu, onNodeLongPress, relationshipColors = DEFAULT_RELATIONSHIP_COLORS, focusNodeId, focusRequest }: Props) {
  const { t } = useI18n();
  const containerRef = useRef<HTMLDivElement>(null);
  const cyRef = useRef<Core | null>(null);
  const suppressTapUntilRef = useRef(0);
  const savedGraphStateRef = useRef<SavedGraphState | null>(null);
  const focusRef = useRef({ nodeId: focusNodeId, request: focusRequest });
  const appliedFocusRef = useRef<{ nodeId: string; request: number | undefined } | null>(null);
  const keyboardNodeIdRef = useRef("");
  const statusRef = useRef<HTMLParagraphElement>(null);

  const prefersReducedMotion = useCallback(
    () => window.matchMedia("(prefers-reduced-motion: reduce)").matches,
    [],
  );

  const announceNode = useCallback((nodeId: string) => {
    const person = data.nodes.find((node) => node.id === nodeId);
    const connectedNames = [...new Set(
      data.edges.flatMap((edge) => {
        if (edge.source === nodeId) return [edge.target];
        if (edge.target === nodeId) return [edge.source];
        return [];
      }),
    )]
      .map((id) => data.nodes.find((node) => node.id === id)?.fullname)
      .filter((name): name is string => Boolean(name))
      .sort((a, b) => a.localeCompare(b));
    if (statusRef.current) {
      statusRef.current.textContent = connectedNames.length > 0
        ? t("graph.selectedWithConnections")
            .replace("{name}", person?.fullname || "?")
            .replace("{connections}", connectedNames.join(", "))
        : t("graph.selected").replace("{name}", person?.fullname || "?");
    }
  }, [data.edges, data.nodes, t]);

  const selectKeyboardNode = useCallback((index: number, center = true) => {
    const cy = cyRef.current;
    if (!cy || data.nodes.length === 0) return;
    const nextIndex = Math.max(0, Math.min(index, data.nodes.length - 1));
    const nodeId = data.nodes[nextIndex].id;
    const node = cy.getElementById(nodeId);
    if (node.empty()) return;
    keyboardNodeIdRef.current = nodeId;
    cy.elements().unselect();
    node.select();
    if (center) {
      if (prefersReducedMotion()) {
        cy.center(node);
      } else {
        cy.animate({ center: { eles: node } }, { duration: 180 });
      }
    }
    announceNode(nodeId);
  }, [announceNode, data.nodes, prefersReducedMotion]);

  const applyPendingFocus = useCallback(() => {
    const cy = cyRef.current;
    const { nodeId, request } = focusRef.current;
    if (!cy || !nodeId) return;
    if (
      appliedFocusRef.current?.nodeId === nodeId &&
      appliedFocusRef.current.request === request
    ) {
      return;
    }
    const node = cy.getElementById(nodeId);
    if (node.empty()) return;
    cy.elements().unselect();
    node.select();
    if (prefersReducedMotion()) {
      cy.center(node);
      cy.zoom(Math.max(cy.zoom(), 1.4));
    } else {
      cy.animate(
        { center: { eles: node }, zoom: Math.max(cy.zoom(), 1.4) },
        { duration: 300 },
      );
    }
    keyboardNodeIdRef.current = nodeId;
    announceNode(nodeId);
    appliedFocusRef.current = { nodeId, request };
  }, [announceNode, prefersReducedMotion]);

  const handleTap = useCallback(
    (e: EventObject) => {
      if (cyRef.current && e.target !== cyRef.current && e.target.isNode()) {
        if (Date.now() < suppressTapUntilRef.current) return;
        cyRef.current.elements().unselect();
        e.target.select();
        onNodeClick?.(e.target.id());
      }
    },
    [onNodeClick]
  );

  const handleDblTap = useCallback(
    (e: EventObject) => {
      if (cyRef.current && e.target !== cyRef.current && e.target.isNode()) {
        onNodeDblClick?.(e.target.id());
      }
    },
    [onNodeDblClick]
  );

  const handleTapHold = useCallback(
    (e: EventObject) => {
      if (!cyRef.current || e.target === cyRef.current || !e.target.isNode()) return;
      const originalEvent = e.originalEvent as MouseEvent & {
        pointerType?: string;
        touches?: TouchList;
      };
      const isTouch = originalEvent.pointerType === "touch" || "touches" in originalEvent;
      if (!isTouch) return;
      suppressTapUntilRef.current = Date.now() + 750;
      onNodeLongPress?.(e.target.id());
    },
    [onNodeLongPress]
  );

  useEffect(() => {
    if (!containerRef.current) return;

    const API_BASE = process.env.NEXT_PUBLIC_API_URL ?? "";
    const graphSignature = getGraphSignature(data, layout);
    const savedGraphState = savedGraphStateRef.current;
    const canRestoreGraphState = Boolean(
      savedGraphState &&
      savedGraphState.signature === graphSignature &&
      data.nodes.every((node) => savedGraphState.positions[node.id]),
    );
    // Proxy profile pics through the backend to avoid browser CORS on canvas
    const proxyUrl = (url: string | undefined) => {
      if (!url) return undefined;
      return `${API_BASE}/api/proxy/image?url=${encodeURIComponent(url)}`;
    };

    const spouseEdges = new Map<string, GraphData["edges"][number]>();
    const otherEdges: GraphData["edges"] = [];
    for (const edge of data.edges) {
      if (edge.type !== "isSpouseOf") {
        otherEdges.push(edge);
        continue;
      }
      const pairKey = [edge.source, edge.target].sort().join("\u0000");
      const existing = spouseEdges.get(pairKey);
      if (!existing || (existing.is_active === false && edge.is_active !== false)) {
        spouseEdges.set(pairKey, edge);
      }
    }
    const visibleEdges = [...otherEdges, ...spouseEdges.values()];

    const elements = [
      ...data.nodes.map((n) => {
        const picUrl = proxyUrl(n.profilepic);
        return {
          data: {
            id: n.id,
            label: (n.fullname || "?") + (!n.isAlive && n.isAlive !== undefined ? " ✝" : ""),
            profilepicUrl: picUrl || "",
            hasPic: n.profilepic ? "yes" : "no",
            isDeceased: !n.isAlive && n.isAlive !== undefined ? "yes" : "no",
            level: typeof n.level === "number" ? n.level : 0,
          },
        };
      }),
      ...visibleEdges.map((e) => ({
        data: {
          id: e.id,
          source: e.source,
          target: e.target,
          type: e.type,
          is_active: e.is_active ?? true,
          familyRouting: layout === "family" ? "yes" : "no",
        },
      })),
    ];

    if (cyRef.current) {
      try { cyRef.current.destroy(); } catch { /* already destroyed */ }
    }

    const defaultEdgeColor = "#888";

    cyRef.current = cytoscape({
      container: containerRef.current,
      elements,
      selectionType: "single",
      style: [
        // Nodes without profile picture
        {
          selector: 'node[hasPic = "no"]',
          style: {
            label: "data(label)",
            "text-wrap": "wrap",
            "text-valign": "bottom",
            "text-halign": "center",
            "font-size": "11px",
            "text-max-width": "110px",
            "background-color": "#FF7F3E",
            width: 40,
            height: 40,
          },
        },
        // Nodes with profile picture
        {
          selector: 'node[hasPic = "yes"]',
          style: {
            label: "data(label)",
            "text-wrap": "wrap",
            "text-valign": "bottom",
            "text-halign": "center",
            "font-size": "11px",
            "text-max-width": "110px",
            "background-image": "data(profilepicUrl)",
            "background-fit": "cover",
            "background-clip": "node",
            "border-width": 2,
            "border-color": "#FF7F3E",
            width: 50,
            height: 50,
            shape: "ellipse",
          },
        },
        {
          selector: "edge",
          style: {
            width: 2,
            "line-color": defaultEdgeColor,
            "target-arrow-color": defaultEdgeColor,
            "target-arrow-shape": "triangle",
            "curve-style": "bezier",
          },
        },
        {
          selector: "edge[?is_active]",
          style: {},
        },
        {
          selector: "edge[is_active = false]",
          style: {
            "line-style": "dashed",
            opacity: 0.4,
          },
        },
        // Dynamic edge colors by type
        ...Object.entries(relationshipColors).map(([type, color]) => ({
          selector: `edge[type = "${type}"]`,
          style: {
            "line-color": color,
            "target-arrow-color": color,
          } as cytoscape.Css.Edge,
        })),
        {
          selector: 'edge[familyRouting = "yes"][type = "isChildOf"]',
          style: {
            "curve-style": "taxi",
            "taxi-direction": "vertical",
            "taxi-turn": "50%",
            "taxi-turn-min-distance": 20,
          } as cytoscape.Css.Edge,
        },
        {
          selector: 'edge[type = "isSpouseOf"]',
          style: {
            "target-arrow-shape": "none",
          } as cytoscape.Css.Edge,
        },
        {
          selector: 'edge[familyRouting = "yes"][type = "isSpouseOf"]',
          style: {
            "curve-style": "straight",
            width: 3,
          } as cytoscape.Css.Edge,
        },
        // Deceased indicator: subtle dark cross badge
        {
          selector: 'node[isDeceased = "yes"]',
          style: {
            "border-width": 2,
            "border-color": "#555",
            "border-style": "solid",
            opacity: 0.8,
          } as cytoscape.Css.Node,
        },
        {
          selector: ".trace-muted",
          style: {
            opacity: 0.16,
          } as cytoscape.Css.Node,
        },
        {
          selector: "node.trace-neighbor",
          style: {
            opacity: 1,
            "border-width": 4,
            "border-color": "#f59e0b",
            "z-index": 10,
          } as cytoscape.Css.Node,
        },
        {
          selector: "edge.trace-connected",
          style: {
            opacity: 1,
            width: 5,
            "z-index": 9,
          } as cytoscape.Css.Edge,
        },
        {
          selector: "node:selected",
          style: {
            opacity: 1,
            "border-width": 5,
            "border-color": "#2563eb",
            "z-index": 11,
          },
        },
      ],
      layout: canRestoreGraphState && savedGraphState
        ? {
            name: "preset",
            positions: (nodeId) => savedGraphState.positions[nodeId],
            fit: false,
          }
        : layout === "family"
          ? buildHierarchicalLayout(elements, true)
          : getLayoutConfig(layout, elements),
    });

    cyRef.current.on("tap", "node", handleTap);
    cyRef.current.on("dbltap", "node", handleDblTap);
    cyRef.current.on("taphold", "node", handleTapHold);
    const syncRelationshipHighlight = () => {
      const cy = cyRef.current;
      if (!cy) return;
      cy.elements().removeClass("trace-muted trace-neighbor trace-connected");
      const selected = cy.$("node:selected").first().nodes();
      if (selected.empty()) return;
      const connectedEdges = selected.connectedEdges();
      const connectedNodes = connectedEdges.connectedNodes().difference(selected);
      connectedEdges.addClass("trace-connected");
      connectedNodes.addClass("trace-neighbor");
      const highlighted = selected.union(connectedEdges).union(connectedNodes);
      cy.elements().difference(highlighted).addClass("trace-muted");
    };
    cyRef.current.on("select unselect", "node", syncRelationshipHighlight);

    if (canRestoreGraphState && savedGraphState) {
      cyRef.current.zoom(savedGraphState.zoom);
      cyRef.current.pan(savedGraphState.pan);
    }
    applyPendingFocus();
    syncRelationshipHighlight();

    return () => {
      const cy = cyRef.current;
      if (cy) {
        const positions: Record<string, Position> = {};
        cy.nodes().forEach((node) => {
          positions[node.id()] = node.position();
        });
        savedGraphStateRef.current = {
          signature: graphSignature,
          positions,
          pan: cy.pan(),
          zoom: cy.zoom(),
        };
      }
      try { cy?.destroy(); } catch { /* ignore */ }
      if (cyRef.current === cy) {
        cyRef.current = null;
      }
    };
  }, [data, layout, relationshipColors, handleTap, handleDblTap, handleTapHold, applyPendingFocus]);

  useEffect(() => {
    focusRef.current = { nodeId: focusNodeId, request: focusRequest };
    applyPendingFocus();
  }, [focusNodeId, focusRequest, applyPendingFocus]);

  useEffect(() => {
    const selectedIndex = data.nodes.findIndex(
      (node) => node.id === keyboardNodeIdRef.current,
    );
    if (selectedIndex >= 0) {
      selectKeyboardNode(selectedIndex, false);
      return;
    }
    keyboardNodeIdRef.current = "";
    if (document.activeElement === containerRef.current && data.nodes.length > 0) {
      selectKeyboardNode(0, false);
    }
  }, [data.nodes, selectKeyboardNode]);

  // Suppress browser context menu and handle right-click on nodes.
  useEffect(() => {
    const el = containerRef.current;
    if (!el) return;
    const handleNativeContextMenu = (e: MouseEvent) => {
      e.preventDefault();
      e.stopPropagation();
      const cy = cyRef.current;
      if (!cy || !onContextMenu) return;
      // Convert page coordinates to Cytoscape model coordinates
      const rect = el.getBoundingClientRect();
      const renderX = e.clientX - rect.left;
      const renderY = e.clientY - rect.top;
      // Use Cytoscape's public API to find nodes near the click
      const pan = cy.pan();
      const zoom = cy.zoom();
      const modelX = (renderX - pan.x) / zoom;
      const modelY = (renderY - pan.y) / zoom;
      // Find closest node within a reasonable radius
      let closestId: string | null = null;
      let closestDist = Infinity;
      const hitRadius = 30 / zoom;
      cy.nodes().forEach((node) => {
        const np = node.position();
        const dist = Math.sqrt((np.x - modelX) ** 2 + (np.y - modelY) ** 2);
        if (dist < hitRadius && dist < closestDist) {
          closestId = node.id();
          closestDist = dist;
        }
      });
      if (closestId) {
        onContextMenu(closestId, e.pageX, e.pageY);
      }
    };
    el.addEventListener("contextmenu", handleNativeContextMenu, { capture: true });
    return () => el.removeEventListener("contextmenu", handleNativeContextMenu, { capture: true });
  }, [onContextMenu]);

  return (
    <>
      <p id="graph-keyboard-instructions" className="sr-only">
        {t("graph.keyboardInstructions")}
      </p>
      <p id="graph-keyboard-status" ref={statusRef} className="sr-only" aria-live="polite" aria-atomic="true" />
      <div
        ref={containerRef}
        role="application"
        tabIndex={0}
        aria-label={t("graph.label")}
        aria-describedby="graph-keyboard-instructions"
        onFocus={() => {
          if (
            data.nodes.length > 0
            && !data.nodes.some((node) => node.id === keyboardNodeIdRef.current)
          ) {
            const requestedIndex = data.nodes.findIndex((node) => node.id === focusNodeId);
            selectKeyboardNode(requestedIndex >= 0 ? requestedIndex : 0, false);
          }
        }}
        onKeyDown={(event) => {
          if (data.nodes.length === 0) return;
          const selectedIndex = data.nodes.findIndex(
            (node) => node.id === keyboardNodeIdRef.current,
          );
          const current = selectedIndex >= 0 ? selectedIndex : 0;
          const currentNode = data.nodes[current];
          if (event.key === "ArrowRight" || event.key === "ArrowDown") {
            event.preventDefault();
            selectKeyboardNode((current + 1) % data.nodes.length);
          } else if (event.key === "ArrowLeft" || event.key === "ArrowUp") {
            event.preventDefault();
            selectKeyboardNode((current - 1 + data.nodes.length) % data.nodes.length);
          } else if (event.key === "Home") {
            event.preventDefault();
            selectKeyboardNode(0);
          } else if (event.key === "End") {
            event.preventDefault();
            selectKeyboardNode(data.nodes.length - 1);
          } else if (event.key === "Enter" || event.key === " ") {
            event.preventDefault();
            onNodeClick?.(currentNode.id);
          } else if (event.key === "c" || event.key === "C") {
            event.preventDefault();
            onNodeDblClick?.(currentNode.id);
          } else if (event.key === "ContextMenu" || (event.shiftKey && event.key === "F10")) {
            event.preventDefault();
            onNodeLongPress?.(currentNode.id);
          }
        }}
        className="h-full min-h-0 w-full touch-none bg-gray-50 md:rounded-lg md:border"
      />
    </>
  );
}

"use client";

import { useEffect, useRef, useCallback } from "react";
import cytoscape, { type Core, type EventObject, type LayoutOptions } from "cytoscape";
import type { GraphData } from "@/lib/types";

export type LayoutMode = "breadthfirst" | "concentric" | "cose" | "grid" | "circle";

export const LAYOUT_OPTIONS: { value: LayoutMode; labelKey: string }[] = [
  { value: "breadthfirst", labelKey: "layout.hierarchical" },
  { value: "concentric", labelKey: "layout.radial" },
  { value: "cose", labelKey: "layout.forceDirected" },
  { value: "grid", labelKey: "layout.grid" },
  { value: "circle", labelKey: "layout.circle" },
];

interface Props {
  data: GraphData;
  layout?: LayoutMode;
  sasToken?: string;
  onNodeClick?: (nodeId: string) => void;
  onContextMenu?: (nodeId: string, x: number, y: number) => void;
  relationshipColors?: Record<string, string>;
}

/**
 * Sugiyama-style hierarchical layout for family trees.
 * - Groups nodes by generation level (from backend)
 * - Keeps spouses adjacent as a family unit
 * - Orders nodes within each row via barycenter heuristic to minimise edge crossings
 * - Centers parents above their children
 */
function buildHierarchicalLayout(
  elements: { data: Record<string, unknown> }[]
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
    const unitBary = new Map<number, { b: number; members: string[] }>();
    for (const id of ids) {
      const u = nodeToUnit.get(id) ?? -1;
      if (!unitBary.has(u)) unitBary.set(u, { b: 0, members: [] });
      unitBary.get(u)!.members.push(id);
    }
    for (const [, ub] of unitBary) {
      ub.b = ub.members.reduce((s, m) => s + (bary.get(m) ?? 0), 0) / ub.members.length;
    }

    // Sort units by barycenter, flatten, assign positions
    const sorted = [...unitBary.values()].sort((a, b) => a.b - b.b);
    let p = 0;
    for (const ub of sorted) {
      // Within a spouse unit, keep a stable internal order
      ub.members.sort((a, b) => (pos.get(a) ?? 0) - (pos.get(b) ?? 0));
      for (const m of ub.members) {
        pos.set(m, p++);
      }
    }
  };

  // Sweep passes (4 full iterations is usually enough for convergence)
  for (let iter = 0; iter < 4; iter++) {
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
  const xSpacing = 160;
  const spouseGap = 90;  // tighter spacing within a spouse unit
  const ySpacing = 140;
  const positions: Record<string, { x: number; y: number }> = {};

  for (let row = 0; row < sortedLevels.length; row++) {
    const lv = sortedLevels[row];
    const ids = levelMap.get(lv)!;
    ids.sort((a, b) => (pos.get(a) ?? 0) - (pos.get(b) ?? 0));
    // Variable spacing: closer for spouses, wider between separate units
    let x = 0;
    for (let i = 0; i < ids.length; i++) {
      if (i > 0) {
        const sameUnit = nodeToUnit.get(ids[i]) === nodeToUnit.get(ids[i - 1]);
        x += sameUnit ? spouseGap : xSpacing;
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
    nodeToUnit.get(a) === nodeToUnit.get(b) ? spouseGap : xSpacing;

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

function getLayoutConfig(mode: LayoutMode, elements: { data: Record<string, unknown> }[]): LayoutOptions {
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

export default function GraphViewer({ data, layout = "breadthfirst", sasToken = "", onNodeClick, onContextMenu, relationshipColors = {} }: Props) {
  const containerRef = useRef<HTMLDivElement>(null);
  const cyRef = useRef<Core | null>(null);

  const handleTap = useCallback(
    (e: EventObject) => {
      if (e.target !== cyRef.current && e.target.isNode()) {
        onNodeClick?.(e.target.id());
      }
    },
    [onNodeClick]
  );

  useEffect(() => {
    if (!containerRef.current) return;

    const API_BASE = process.env.NEXT_PUBLIC_API_URL || "http://localhost:8000";
    // Proxy profile pics through the backend to avoid browser CORS on canvas
    const proxyUrl = (url: string | undefined) => {
      if (!url) return undefined;
      return `${API_BASE}/api/proxy/image?url=${encodeURIComponent(url)}`;
    };

    const elements = [
      ...data.nodes.map((n) => {
        const picUrl = proxyUrl(n.profilepic);
        return {
          data: {
            id: n.id,
            label: n.fullname || "?",
            profilepicUrl: picUrl || "",
            hasPic: n.profilepic ? "yes" : "no",
            level: typeof n.level === "number" ? n.level : 0,
          },
        };
      }),
      ...data.edges.map((e) => ({
        data: {
          id: e.id,
          source: e.source,
          target: e.target,
          type: e.type,
          is_active: e.is_active ?? true,
        },
      })),
    ];

    if (cyRef.current) {
      cyRef.current.destroy();
    }

    const defaultEdgeColor = "#888";

    cyRef.current = cytoscape({
      container: containerRef.current,
      elements,
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
          selector: "node:selected",
          style: {
            "border-width": 3,
            "border-color": "#2563eb",
          },
        },
      ],
      layout: getLayoutConfig(layout, elements),
    });

    cyRef.current.on("tap", "node", handleTap);

    return () => {
      cyRef.current?.destroy();
      cyRef.current = null;
    };
  }, [data, layout, sasToken, relationshipColors, handleTap]);

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
      let closest: { id: string; dist: number } | null = null;
      const hitRadius = 30 / zoom;
      cy.nodes().forEach((node) => {
        const np = node.position();
        const dist = Math.sqrt((np.x - modelX) ** 2 + (np.y - modelY) ** 2);
        if (dist < hitRadius && (!closest || dist < closest.dist)) {
          closest = { id: node.id(), dist };
        }
      });
      if (closest) {
        onContextMenu(closest.id, e.pageX, e.pageY);
      }
    };
    el.addEventListener("contextmenu", handleNativeContextMenu, { capture: true });
    return () => el.removeEventListener("contextmenu", handleNativeContextMenu, { capture: true });
  }, [onContextMenu]);

  return (
    <div
      ref={containerRef}
      className="w-full h-full min-h-[500px] bg-gray-50 rounded-lg border"
    />
  );
}

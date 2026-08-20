export const GRAPH_LAYOUT_MODES = [
  "family",
  "breadthfirst",
  "concentric",
  "cose",
  "grid",
  "circle",
] as const;

export type LayoutMode = (typeof GRAPH_LAYOUT_MODES)[number];

export interface GraphViewState {
  center: string;
  radius: number;
  layout: LayoutMode;
  person: string;
}

export const DEFAULT_GRAPH_VIEW_STATE: GraphViewState = {
  center: "",
  radius: 2,
  layout: "family",
  person: "",
};

const GRAPH_VIEW_PARAMETERS = ["center", "radius", "layout", "person"] as const;
const MIN_RADIUS = 1;
const MAX_RADIUS = 10;

function isLayoutMode(value: string): value is LayoutMode {
  return GRAPH_LAYOUT_MODES.some((mode) => mode === value);
}

function parseRadius(value: string | null): number {
  if (value === null || !/^\d+$/.test(value)) {
    return DEFAULT_GRAPH_VIEW_STATE.radius;
  }

  const radius = Number(value);
  return radius >= MIN_RADIUS && radius <= MAX_RADIUS
    ? radius
    : DEFAULT_GRAPH_VIEW_STATE.radius;
}

export function parseGraphViewState(search: string): GraphViewState {
  const parameters = new URLSearchParams(search);
  const layout = parameters.get("layout");

  return {
    center: parameters.get("center")?.trim() ?? DEFAULT_GRAPH_VIEW_STATE.center,
    radius: parseRadius(parameters.get("radius")),
    layout:
      layout && isLayoutMode(layout)
        ? layout
        : DEFAULT_GRAPH_VIEW_STATE.layout,
    person: parameters.get("person")?.trim() ?? DEFAULT_GRAPH_VIEW_STATE.person,
  };
}

export function serializeGraphViewState(
  search: string,
  state: GraphViewState,
): string {
  const parameters = new URLSearchParams(search);
  GRAPH_VIEW_PARAMETERS.forEach((parameter) => parameters.delete(parameter));

  if (state.center) {
    parameters.set("center", state.center);
  }
  if (state.radius !== DEFAULT_GRAPH_VIEW_STATE.radius) {
    parameters.set("radius", String(state.radius));
  }
  if (state.layout !== DEFAULT_GRAPH_VIEW_STATE.layout) {
    parameters.set("layout", state.layout);
  }
  if (state.person) {
    parameters.set("person", state.person);
  }

  const serialized = parameters.toString();
  return serialized ? `?${serialized}` : "";
}

export function graphViewStatesEqual(
  first: GraphViewState,
  second: GraphViewState,
): boolean {
  return (
    first.center === second.center &&
    first.radius === second.radius &&
    first.layout === second.layout &&
    first.person === second.person
  );
}

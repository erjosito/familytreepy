export interface PersonNode {
  id: string;
  fullname: string;
  firstname?: string;
  lastname?: string;
  birthdate?: string;
  birthplace?: string;
  isAlive?: boolean;
  deathdate?: string;
  gender?: string;
  alias?: string;
  profilepic?: string;
  pictures?: string[];
  level?: number;
  relationships?: GraphEdge[];
  siblings?: string[];
  [key: string]: unknown;
}

export interface GraphEdge {
  id: string;
  source: string;
  target: string;
  type: string;
  is_active?: boolean;
  start_date?: string;
  end_date?: string;
}

export interface GraphData {
  nodes: PersonNode[];
  edges: GraphEdge[];
}

export interface FieldConfig {
  type: string;
  required?: boolean;
  label: string;
  default?: unknown;
  visible_when?: {
    field: string;
    equals: unknown;
  };
}

export interface RendererInfo {
  name: string;
  description: string;
}

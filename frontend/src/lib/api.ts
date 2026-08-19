import { isAuthEnabled } from "./auth";
import type { GraphData, RendererInfo } from "./types";

const API_BASE = process.env.NEXT_PUBLIC_API_URL ?? "";

async function getAuthHeaders(): Promise<Record<string, string>> {
  if (!isAuthEnabled()) return {};
  try {
    const { msalInstance } = await import("./auth");
    const accounts = msalInstance.getAllAccounts();
    if (accounts.length === 0) return {};
    const response = await msalInstance.acquireTokenSilent({
      scopes: [process.env.NEXT_PUBLIC_API_SCOPE || "openid"],
      account: accounts[0],
    });
    return { Authorization: `Bearer ${response.accessToken}` };
  } catch {
    return {};
  }
}

async function apiFetch(path: string, init?: RequestInit): Promise<Response> {
  const headers = { ...(await getAuthHeaders()), ...init?.headers };
  const res = await fetch(`${API_BASE}${path}`, { ...init, headers, credentials: "include" });
  if (!res.ok) {
    const text = await res.text().catch(() => res.statusText);
    throw new Error(`${res.status}: ${text}`);
  }
  return res;
}

// ── Graph ────────────────────────────────────────────────────────────────

export async function getGraph(
  rootId?: string,
  degree?: number,
  includeInactive?: boolean
): Promise<GraphData> {
  const params = new URLSearchParams();
  if (rootId) params.append("root_id", rootId);
  if (degree !== undefined) params.append("degree", String(degree));
  if (includeInactive) params.append("include_inactive", "true");
  const res = await apiFetch(`/api/graph?${params}`);
  return res.json();
}

// ── Persons ──────────────────────────────────────────────────────────────

export async function listPersons(): Promise<{ id: string; fullname: string; alias?: string }[]> {
  const res = await apiFetch("/api/persons");
  return res.json();
}

export async function getPerson(personId: string): Promise<Record<string, unknown>> {
  const res = await apiFetch(`/api/persons/${personId}`);
  return res.json();
}

export async function createPerson(data: Record<string, unknown>): Promise<{ id: string }> {
  const res = await apiFetch("/api/persons", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(data),
  });
  return res.json();
}

export async function updatePerson(
  personId: string,
  data: Record<string, unknown>
): Promise<{ id: string; updated: boolean }> {
  const res = await apiFetch(`/api/persons/${personId}`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(data),
  });
  return res.json();
}

export async function deletePerson(personId: string): Promise<{ id: string; deleted: boolean }> {
  const res = await apiFetch(`/api/persons/${personId}`, { method: "DELETE" });
  return res.json();
}

// ── Pictures ─────────────────────────────────────────────────────────────

export async function uploadProfilePic(
  personId: string,
  file: Blob,
  filename: string
): Promise<{ url: string }> {
  const formData = new FormData();
  formData.append("file", file, filename);
  const res = await fetch(`${API_BASE}/api/persons/${personId}/profilepic`, {
    method: "POST",
    credentials: "include",
    body: formData,
  });
  if (!res.ok) throw new Error(`Upload failed: ${res.status}`);
  return res.json();
}

export async function uploadPicture(
  personId: string,
  file: File,
  filename: string
): Promise<{ url: string }> {
  const formData = new FormData();
  formData.append("file", file, filename);
  const res = await fetch(`${API_BASE}/api/persons/${personId}/pictures`, {
    method: "POST",
    credentials: "include",
    body: formData,
  });
  if (!res.ok) {
    const detail = await res.text().catch(() => "");
    throw new Error(`Upload failed: ${res.status} ${detail}`);
  }
  return res.json();
}

export async function tagPicture(
  personId: string,
  url: string,
  personIds: string[]
): Promise<{ url: string; tagged: string[] }> {
  const res = await apiFetch(`/api/persons/${personId}/pictures/tag`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ url, person_ids: personIds }),
  });
  return res.json();
}

export async function removePicture(
  personId: string,
  url: string
): Promise<{ removed: boolean }> {
  const res = await apiFetch(`/api/persons/${personId}/pictures`, {
    method: "DELETE",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ url }),
  });
  return res.json();
}

export async function getPeopleInPicture(
  personId: string,
  url: string
): Promise<{ id: string; fullname: string }[]> {
  const res = await apiFetch(`/api/persons/${personId}/pictures/people`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ url }),
  });
  return res.json();
}

export async function untagPicture(
  personId: string,
  url: string,
  targetPersonId: string
): Promise<{ url: string; untagged: string }> {
  const res = await apiFetch(`/api/persons/${personId}/pictures/untag`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ url, person_id: targetPersonId }),
  });
  return res.json();
}

// ── Notes ────────────────────────────────────────────────────────────────

export interface Note {
  text: string;
  author: string;
  timestamp: string;
}

export async function getNotes(personId: string): Promise<Note[]> {
  const res = await apiFetch(`/api/persons/${personId}/notes`);
  return res.json();
}

export async function addNote(
  personId: string,
  text: string,
  author: string
): Promise<Note> {
  const res = await apiFetch(`/api/persons/${personId}/notes`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ text, author }),
  });
  return res.json();
}

export async function deleteNote(personId: string, noteIndex: number): Promise<void> {
  await apiFetch(`/api/persons/${personId}/notes/${noteIndex}`, {
    method: "DELETE",
  });
}

// ── Relationships ────────────────────────────────────────────────────────

export async function createRelationship(data: {
  source: string;
  target: string;
  type: string;
  start_date?: string;
}): Promise<{ source: string; target: string; type: string; created: boolean }> {
  const res = await apiFetch("/api/relationships", {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(data),
  });
  return res.json();
}

export async function deactivateRelationship(
  sourceId: string,
  targetId: string,
  endDate?: string
): Promise<{ source: string; target: string; deactivated: boolean }> {
  const res = await apiFetch(`/api/relationships/${sourceId}/${targetId}/deactivate`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ end_date: endDate }),
  });
  return res.json();
}

export async function reactivateRelationship(
  sourceId: string,
  targetId: string
): Promise<{ source: string; target: string; reactivated: boolean }> {
  const res = await apiFetch(`/api/relationships/${sourceId}/${targetId}/reactivate`, {
    method: "PUT",
  });
  return res.json();
}

export async function deleteRelationship(
  sourceId: string,
  targetId: string
): Promise<{ source: string; target: string; deleted: boolean }> {
  const res = await apiFetch(`/api/relationships/${sourceId}/${targetId}`, {
    method: "DELETE",
  });
  return res.json();
}

// ── Schema ───────────────────────────────────────────────────────────────

export async function getPersonSchema(): Promise<Record<string, unknown>> {
  const res = await apiFetch("/api/schema/person");
  return res.json();
}

// ── Storage Config ───────────────────────────────────────────────────────

export async function getStorageConfig(): Promise<{ sas_token: string }> {
  const res = await apiFetch("/api/config/storage");
  return res.json();
}

// ── User Management (Admin) ──────────────────────────────────────────────

export async function listUsers(): Promise<{ email: string; role: string }[]> {
  const res = await fetch(`${API_BASE}/api/auth/users`, { credentials: "include" });
  if (!res.ok) throw new Error(`${res.status}: ${await res.text()}`);
  return res.json();
}

export async function addUser(email: string, role: string): Promise<{ email: string; role: string }> {
  const res = await fetch(`${API_BASE}/api/auth/users`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    credentials: "include",
    body: JSON.stringify({ email, role }),
  });
  if (!res.ok) throw new Error(`${res.status}: ${await res.text()}`);
  return res.json();
}

export async function updateUser(
  originalEmail: string,
  email: string,
  role: string
): Promise<{ email: string; role: string }> {
  const res = await fetch(`${API_BASE}/api/auth/users/${encodeURIComponent(originalEmail)}`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    credentials: "include",
    body: JSON.stringify({ email, role }),
  });
  if (!res.ok) throw new Error(`${res.status}: ${await res.text()}`);
  return res.json();
}

export async function deleteUser(email: string): Promise<void> {
  const res = await fetch(`${API_BASE}/api/auth/users/${encodeURIComponent(email)}`, {
    method: "DELETE",
    credentials: "include",
  });
  if (!res.ok) throw new Error(`${res.status}: ${await res.text()}`);
}

// ── Renderers & Image Generation ─────────────────────────────────────────

export async function listRenderers(): Promise<RendererInfo[]> {
  const res = await apiFetch("/api/renderers");
  return res.json();
}

export interface ImageOptions {
  canvasWidth?: number;
  canvasHeight?: number;
  fontScale?: number;
  lineWidth?: number;
  colorScheme?: string;
}

export async function generateImage(
  rootId: string,
  degree: number,
  renderer: string,
  options?: ImageOptions
): Promise<Blob> {
  const params = new URLSearchParams({
    root_id: rootId,
    degree: String(degree),
    renderer,
  });
  if (options?.canvasWidth) params.append("canvas_width", String(options.canvasWidth));
  if (options?.canvasHeight) params.append("canvas_height", String(options.canvasHeight));
  if (options?.fontScale) params.append("font_scale", String(options.fontScale));
  if (options?.lineWidth) params.append("line_width", String(options.lineWidth));
  if (options?.colorScheme) params.append("color_scheme", options.colorScheme);
  const res = await fetch(`${API_BASE}/api/graph/image?${params}`, {
    method: "POST",
    credentials: "include",
  });
  if (!res.ok) throw new Error(`Image generation failed: ${res.status}`);
  return res.blob();
}

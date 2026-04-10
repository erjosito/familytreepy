"use client";

import { useState, useEffect, useCallback } from "react";
import { useSearchParams } from "next/navigation";
import Link from "next/link";
import { getPerson, listPersons, getStorageConfig, getNotes, addNote, deleteNote, type Note } from "@/lib/api";
import type { PersonNode, GraphEdge } from "@/lib/types";
import { useAdminView } from "@/lib/adminView";
import { useI18n } from "@/lib/i18n";

export default function PersonPage() {
  const searchParams = useSearchParams();
  const personId = searchParams.get("id") || "";
  const { userEmail, userName, isAdmin, adminView } = useAdminView();
  const { t } = useI18n();

  const [person, setPerson] = useState<PersonNode | null>(null);
  const [relationships, setRelationships] = useState<GraphEdge[]>([]);
  const [siblings, setSiblings] = useState<string[]>([]);
  const [personList, setPersonList] = useState<{ id: string; fullname: string }[]>([]);
  const [sasToken, setSasToken] = useState("");
  const [notes, setNotes] = useState<Note[]>([]);
  const [loading, setLoading] = useState(true);
  const [newNote, setNewNote] = useState("");
  const [addingNote, setAddingNote] = useState(false);
  const [lightboxUrl, setLightboxUrl] = useState<string | null>(null);

  const noteAuthor = userEmail || userName || "anonymous";

  const nameOf = useCallback(
    (id: string) => personList.find((p) => p.id === id)?.fullname || id,
    [personList]
  );

  const withSas = (url: string | undefined) => {
    if (!url || !sasToken) return url;
    return url.includes("?") ? url : `${url}?${sasToken}`;
  };

  const fetchPerson = useCallback(async () => {
    if (!personId) return;
    setLoading(true);
    try {
      const [detail, pList, config, personNotes] = await Promise.all([
        getPerson(personId),
        listPersons(),
        getStorageConfig().catch(() => ({ sas_token: "" })),
        getNotes(personId).catch(() => []),
      ]);
      const clean: Record<string, unknown> = {};
      for (const [k, v] of Object.entries(detail)) {
        if (v !== null && v !== undefined && (Array.isArray(v) || typeof v !== "object")) {
          clean[k] = v;
        }
      }
      setPerson(clean as PersonNode);
      setRelationships((detail.relationships || []) as GraphEdge[]);
      setSiblings(detail.siblings || []);
      setPersonList(pList);
      setSasToken(config.sas_token);
      setNotes(personNotes);
    } catch (err) {
      console.error("Failed to load person:", err);
    } finally {
      setLoading(false);
    }
  }, [personId]);

  useEffect(() => {
    fetchPerson();
  }, [fetchPerson]);

  const refreshNotes = async () => {
    try {
      setNotes(await getNotes(personId));
    } catch {}
  };

  const handleAddNote = async () => {
    if (!newNote.trim()) return;
    setAddingNote(true);
    try {
      await addNote(personId, newNote.trim(), noteAuthor);
      setNewNote("");
      await refreshNotes();
    } catch (err) {
      console.error("Failed to add note:", err);
    } finally {
      setAddingNote(false);
    }
  };

  const handleDeleteNote = async (index: number) => {
    try {
      await deleteNote(personId, index);
      await refreshNotes();
    } catch (err) {
      console.error("Failed to delete note:", err);
    }
  };

  const formatDate = (ts: string) => {
    try {
      return new Date(ts).toLocaleDateString(undefined, {
        year: "numeric", month: "short", day: "numeric",
        hour: "2-digit", minute: "2-digit",
      });
    } catch { return ts; }
  };

  if (!personId) {
    return (
      <div className="min-h-screen bg-gray-50 flex flex-col items-center justify-center gap-4">
        <p className="text-gray-500">No person selected</p>
        <Link href="/" className="text-blue-600 hover:underline text-sm">{t("person.backToExplore")}</Link>
      </div>
    );
  }

  if (loading) {
    return (
      <div className="min-h-screen bg-gray-50 flex items-center justify-center text-gray-400">
        {t("person.loading")}
      </div>
    );
  }

  if (!person) {
    return (
      <div className="min-h-screen bg-gray-50 flex flex-col items-center justify-center gap-4">
        <p className="text-gray-500">{t("person.notFound")}</p>
        <Link href="/" className="text-blue-600 hover:underline text-sm">{t("person.backToExplore")}</Link>
      </div>
    );
  }

  // Group relationships by type from the person's perspective
  const parents: { id: string; name: string; rel: GraphEdge }[] = [];
  const children: { id: string; name: string; rel: GraphEdge }[] = [];
  const spouses: { id: string; name: string; rel: GraphEdge }[] = [];
  const seenSpouses = new Set<string>();

  for (const rel of relationships) {
    const isSource = rel.source === personId;
    const otherId = isSource ? rel.target : rel.source;
    const otherName = nameOf(otherId);

    if (rel.type === "isChildOf") {
      if (isSource) {
        parents.push({ id: otherId, name: otherName, rel });
      } else {
        children.push({ id: otherId, name: otherName, rel });
      }
    } else if (rel.type === "isSpouseOf" && !seenSpouses.has(otherId)) {
      seenSpouses.add(otherId);
      spouses.push({ id: otherId, name: otherName, rel });
    }
  }

  const pics = person.pictures && person.pictures.length > 0 ? person.pictures : [];

  return (
    <div className="min-h-screen bg-gray-50">
      <div className="max-w-4xl mx-auto p-6 space-y-6">
        {/* Back link */}
        <Link href="/" className="text-sm text-blue-600 hover:underline">{t("person.backToExplore")}</Link>

        {/* Profile header */}
        <div className="bg-white rounded-lg border shadow-sm p-6">
          <div className="flex items-start gap-6">
            {person.profilepic ? (
              <img
                src={withSas(person.profilepic)}
                alt={person.fullname}
                className="w-32 h-32 rounded-full object-cover border-2 border-gray-200 flex-shrink-0"
              />
            ) : (
              <div className="w-32 h-32 rounded-full bg-gray-200 flex items-center justify-center text-gray-400 text-3xl font-bold border-2 border-gray-200 flex-shrink-0">
                {(person.firstname?.[0] || "?").toUpperCase()}
              </div>
            )}

            <div className="flex-1 min-w-0">
              <h1 className="text-2xl font-bold text-gray-900 mb-3">{person.fullname || "Unknown"}</h1>
              <div className="grid grid-cols-2 gap-x-8 gap-y-1 text-sm">
                {person.birthdate && (
                  <>
                    <span className="text-gray-500">{t("person.born")}</span>
                    <span className="text-gray-900">{person.birthdate}</span>
                  </>
                )}
                {person.birthplace && (
                  <>
                    <span className="text-gray-500">{t("person.birthplace")}</span>
                    <span className="text-gray-900">{person.birthplace}</span>
                  </>
                )}
                <span className="text-gray-500">{t("person.status")}</span>
                <span className="text-gray-900">
                  {person.isAlive !== false
                    ? t("person.living")
                    : `${t("person.deceased")}${person.deathdate ? ` (${person.deathdate})` : ""}`}
                </span>
              </div>
            </div>
          </div>
        </div>

        {/* Family connections */}
        <div className="bg-white rounded-lg border shadow-sm p-6">
          <h2 className="font-semibold text-gray-900 mb-4">{t("person.family")}</h2>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
            <div>
              <h3 className="text-xs font-medium text-gray-500 uppercase tracking-wide mb-2">{t("person.parents")}</h3>
              {parents.length > 0 ? (
                <ul className="space-y-1">
                  {parents.map((p) => (
                    <li key={p.id}>
                      <Link href={`/person/?id=${p.id}`} className="text-sm text-blue-600 hover:underline">
                        {p.name}
                      </Link>
                    </li>
                  ))}
                </ul>
              ) : (
                <p className="text-sm text-gray-400">—</p>
              )}
            </div>

            <div>
              <h3 className="text-xs font-medium text-gray-500 uppercase tracking-wide mb-2">{t("person.spouses")}</h3>
              {spouses.length > 0 ? (
                <ul className="space-y-1">
                  {spouses.map((s) => (
                    <li key={s.id} className="flex items-center gap-2">
                      <Link href={`/person/?id=${s.id}`} className="text-sm text-blue-600 hover:underline">
                        {s.name}
                      </Link>
                      {!s.rel.is_active && (
                        <span className="text-xs text-gray-400">{t("rel.inactive")}</span>
                      )}
                    </li>
                  ))}
                </ul>
              ) : (
                <p className="text-sm text-gray-400">—</p>
              )}
            </div>

            <div>
              <h3 className="text-xs font-medium text-gray-500 uppercase tracking-wide mb-2">{t("person.children")}</h3>
              {children.length > 0 ? (
                <ul className="space-y-1">
                  {children.map((c) => (
                    <li key={c.id}>
                      <Link href={`/person/?id=${c.id}`} className="text-sm text-blue-600 hover:underline">
                        {c.name}
                      </Link>
                    </li>
                  ))}
                </ul>
              ) : (
                <p className="text-sm text-gray-400">—</p>
              )}
            </div>
          </div>

          {siblings.length > 0 && (
            <div className="mt-4 pt-4 border-t">
              <h3 className="text-xs font-medium text-gray-500 uppercase tracking-wide mb-2">{t("person.siblings")}</h3>
              <div className="flex flex-wrap gap-2">
                {siblings.map((s) => (
                  <Link key={s} href={`/person/?id=${s}`} className="text-sm text-blue-600 hover:underline">
                    {nameOf(s)}
                  </Link>
                ))}
              </div>
            </div>
          )}
        </div>

        {/* Pictures */}
        <div className="bg-white rounded-lg border shadow-sm p-6">
          <h2 className="font-semibold text-gray-900 mb-4">
            {t("person.pictures")} {pics.length > 0 && <span className="text-gray-400 font-normal">({pics.length})</span>}
          </h2>
          {pics.length > 0 ? (
            <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-3">
              {pics.map((url, i) => (
                <img
                  key={i}
                  src={withSas(url) || url}
                  alt=""
                  className="rounded-lg border object-contain h-40 w-full bg-gray-100 cursor-pointer hover:shadow-md transition-shadow"
                  onClick={() => setLightboxUrl(withSas(url) || url)}
                />
              ))}
            </div>
          ) : (
            <p className="text-sm text-gray-400">{t("person.noPictures")}</p>
          )}
        </div>

        {/* Notes */}
        <div className="bg-white rounded-lg border shadow-sm p-6">
          <h2 className="font-semibold text-gray-900 mb-4">
            {t("notes.title")} {notes.length > 0 && <span className="text-gray-400 font-normal">({notes.length})</span>}
          </h2>

          {notes.length > 0 && (
            <div className="space-y-3 mb-4">
              {notes.map((note, i) => (
                <div key={i} className="bg-gray-50 rounded-lg border p-4 group relative">
                  <p className="text-gray-900 whitespace-pre-wrap">{note.text}</p>
                  <div className="flex items-center gap-2 mt-2">
                    <span className="text-xs text-gray-500">— {note.author}</span>
                    <span className="text-xs text-gray-400">{formatDate(note.timestamp)}</span>
                  </div>
                  {adminView && (
                    <button
                      onClick={() => handleDeleteNote(i)}
                      className="absolute top-2 right-2 text-gray-300 hover:text-red-500 opacity-0 group-hover:opacity-100 transition-opacity"
                      title={t("notes.delete")}
                    >
                      ✕
                    </button>
                  )}
                </div>
              ))}
            </div>
          )}

          <div className="space-y-2">
            <textarea
              value={newNote}
              onChange={(e) => setNewNote(e.target.value)}
              placeholder={t("notes.placeholder")}
              rows={3}
              className="w-full border rounded-lg px-3 py-2 text-sm text-gray-900 resize-y"
            />
            <button
              onClick={handleAddNote}
              disabled={addingNote || !newNote.trim()}
              className="px-4 py-1.5 bg-blue-600 text-white text-sm rounded-lg hover:bg-blue-700 disabled:opacity-50"
            >
              {addingNote ? t("notes.adding") : t("notes.add")}
            </button>
          </div>
        </div>
      </div>

      {/* Lightbox */}
      {lightboxUrl && (
        <div
          className="fixed inset-0 z-[100] bg-black/80 flex items-center justify-center cursor-pointer"
          onClick={() => setLightboxUrl(null)}
        >
          <button
            className="absolute top-4 right-4 text-white text-2xl hover:text-gray-300"
            onClick={() => setLightboxUrl(null)}
          >
            ✕
          </button>
          <img
            src={lightboxUrl}
            alt=""
            className="max-w-[90vw] max-h-[90vh] object-contain rounded-lg shadow-2xl"
            onClick={(e) => e.stopPropagation()}
          />
        </div>
      )}
    </div>
  );
}

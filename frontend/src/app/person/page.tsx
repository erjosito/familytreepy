"use client";

import { useState, useEffect, useCallback, useRef } from "react";
import { useSearchParams } from "next/navigation";
import Link from "next/link";
import { getPerson, listPersons, getStorageConfig, getNotes, addNote, deleteNote, uploadPicture, uploadProfilePic, tagPicture, removePicture, getPeopleInPicture, untagPicture, deactivateRelationship, reactivateRelationship, deleteRelationship, updatePerson, deletePerson, rollbackHistory, getValidationIssues, type Note, type ValidationIssue } from "@/lib/api";
import type { PersonNode, GraphEdge } from "@/lib/types";
import { useAdminView } from "@/lib/adminView";
import { useI18n } from "@/lib/i18n";
import { formatDate, formatTimestamp } from "@/lib/dateUtils";
import { useToast } from "@/components/ToastProvider";
import ValidationMessages from "@/components/ValidationMessages";

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type TFunc = (key: any) => string;

import { Suspense } from "react";

export default function PersonPage() {
  return (
    <Suspense fallback={<div className="min-h-screen bg-gray-50 flex items-center justify-center text-gray-400">Loading...</div>}>
      <PersonPageContent />
    </Suspense>
  );
}

function PersonPageContent() {
  const searchParams = useSearchParams();
  const personId = searchParams.get("id") || "";
  const { userEmail, userName, isAdmin, adminView } = useAdminView();
  const { t } = useI18n();
  const toast = useToast();

  const [person, setPerson] = useState<PersonNode | null>(null);
  const [relationships, setRelationships] = useState<GraphEdge[]>([]);
  const [siblings, setSiblings] = useState<string[]>([]);
  const [personList, setPersonList] = useState<{ id: string; fullname: string }[]>([]);
  const [sasToken, setSasToken] = useState("");
  const [notes, setNotes] = useState<Note[]>([]);
  const [loading, setLoading] = useState(true);
  const [newNote, setNewNote] = useState("");
  const [addingNote, setAddingNote] = useState(false);
  const [deletingNoteIndex, setDeletingNoteIndex] = useState<number | null>(null);
  const [lightboxUrl, setLightboxUrl] = useState<string | null>(null);

  const noteAuthor = userName && userEmail
    ? `${userName} (${userEmail})`
    : userName || userEmail || "anonymous";

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
      setSiblings(Array.isArray(detail.siblings) ? detail.siblings as string[] : []);
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
      toast.success(t("toast.noteAdded"));
    } catch (err) {
      console.error("Failed to add note:", err);
      toast.error(t("toast.noteAddFailed"));
    } finally {
      setAddingNote(false);
    }
  };

  const handleDeleteNote = async (index: number) => {
    if (deletingNoteIndex !== null) return;
    setDeletingNoteIndex(index);
    try {
      await deleteNote(personId, index);
      await refreshNotes();
      toast.success(t("toast.noteDeleted"));
    } catch (err) {
      console.error("Failed to delete note:", err);
      toast.error(t("toast.noteDeleteFailed"));
    } finally {
      setDeletingNoteIndex(null);
    }
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
      <div className="max-w-4xl mx-auto p-3 sm:p-6 space-y-6">
        {/* Back link */}
        <Link href="/" className="text-sm text-blue-600 hover:underline">{t("person.backToExplore")}</Link>

        {/* Profile header */}
        <ProfileHeader
          person={person}
          withSas={withSas}
          onUpdated={fetchPerson}
          onDeleted={() => setPerson(null)}
          t={t}
        />

        {/* Family connections */}
        <div className="bg-white rounded-lg border shadow-sm p-6">
          <h2 className="font-semibold text-gray-900 mb-4">{t("person.family")}</h2>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
            <div>
              <h3 className="text-xs font-medium text-gray-500 uppercase tracking-wide mb-2">{t("person.parents")}</h3>
              {parents.length > 0 ? (
                <ul className="space-y-1">
                  {parents.map((p) => (
                    <li key={p.id} className="flex items-center gap-2">
                      <Link href={`/person/?id=${p.id}`} className="text-sm text-blue-600 hover:underline">
                        {p.name}
                      </Link>
                      {adminView && (
                        <RelDeleteBtn source={p.rel.source} target={p.rel.target} onDeleted={fetchPerson} t={t} />
                      )}
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
                <ul className="space-y-1.5">
                  {spouses.map((s) => (
                    <li key={s.id} className={`flex items-center gap-2 ${!s.rel.is_active ? "opacity-60" : ""}`}>
                      <Link href={`/person/?id=${s.id}`} className="text-sm text-blue-600 hover:underline">
                        {s.name}
                      </Link>
                      <SpouseToggle
                        source={s.rel.source}
                        target={s.rel.target}
                        isActive={s.rel.is_active !== false}
                        onToggled={fetchPerson}
                        t={t}
                      />
                      {adminView && (
                        <RelDeleteBtn source={s.rel.source} target={s.rel.target} onDeleted={fetchPerson} t={t} />
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
                    <li key={c.id} className="flex items-center gap-2">
                      <Link href={`/person/?id=${c.id}`} className="text-sm text-blue-600 hover:underline">
                        {c.name}
                      </Link>
                      {adminView && (
                        <RelDeleteBtn source={c.rel.source} target={c.rel.target} onDeleted={fetchPerson} t={t} />
                      )}
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
                {[...new Set(siblings)].map((s) => (
                  <Link key={s} href={`/person/?id=${s}`} className="text-sm text-blue-600 hover:underline">
                    {nameOf(s)}
                  </Link>
                ))}
              </div>
            </div>
          )}
        </div>

        {/* Pictures */}
        <PersonPictures
          personId={personId}
          pics={pics}
          personList={personList}
          withSas={withSas}
          onLightbox={setLightboxUrl}
          onUpdated={fetchPerson}
          t={t}
          adminView={adminView}
        />

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
                    <span className="text-xs text-gray-400">{formatTimestamp(note.timestamp)}</span>
                  </div>
                  {adminView && (
                    <button
                      onClick={() => handleDeleteNote(i)}
                      disabled={deletingNoteIndex !== null}
                      className="absolute top-2 right-2 text-gray-300 hover:text-red-500 opacity-0 group-hover:opacity-100 transition-opacity disabled:cursor-not-allowed disabled:opacity-50"
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

/* ------------------------------------------------------------------ */
/* Editable profile header                                              */
/* ------------------------------------------------------------------ */
function ProfileHeader({
  person,
  withSas,
  onUpdated,
  onDeleted,
  t,
}: {
  person: PersonNode;
  withSas: (url: string | undefined) => string | undefined;
  onUpdated: () => void;
  onDeleted: () => void;
  t: TFunc;
}) {
  const { adminView } = useAdminView();
  const toast = useToast();
  const [editing, setEditing] = useState(false);
  const [saving, setSaving] = useState(false);
  const [validationIssues, setValidationIssues] = useState<ValidationIssue[]>([]);
  const [cropSrc, setCropSrc] = useState<string | null>(null);
  const [removingProfilePic, setRemovingProfilePic] = useState(false);
  const [deletingPerson, setDeletingPerson] = useState(false);
  const [draft, setDraft] = useState({
    firstname: "",
    lastname: "",
    alias: "",
    birthdate: "",
    birthplace: "",
    isAlive: true,
    deathdate: "",
  });

  const startEdit = () => {
    setDraft({
      firstname: person.firstname || "",
      lastname: person.lastname || "",
      alias: person.alias || "",
      birthdate: person.birthdate || "",
      birthplace: person.birthplace || "",
      isAlive: !!person.isAlive,
      deathdate: person.deathdate || "",
    });
    setValidationIssues([]);
    setEditing(true);
  };

  const updateDraft = (changes: Partial<typeof draft>) => {
    setValidationIssues([]);
    setDraft((current) => ({ ...current, ...changes }));
  };

  const handleSave = async (overrideWarnings = false) => {
    setSaving(true);
    try {
      await updatePerson(person.id, draft, overrideWarnings);
      setEditing(false);
      setValidationIssues([]);
      onUpdated();
      toast.success(t("toast.personSaved"));
    } catch (err) {
      console.error("Save failed:", err);
      const issues = getValidationIssues(err);
      if (issues) {
        setValidationIssues(issues);
        return;
      }
      toast.error(t("toast.personSaveFailed"), {
        action: {
          label: t("toast.retry"),
          onClick: () => handleSave(overrideWarnings),
        },
      });
    } finally {
      setSaving(false);
    }
  };

  const handlePicSelected = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;
    e.target.value = "";
    const reader = new FileReader();
    reader.onload = () => setCropSrc(reader.result as string);
    reader.readAsDataURL(file);
  };

  const handleRemoveProfilePic = async () => {
    if (removingProfilePic || !confirm(t("detail.deletePhoto"))) return;
    setRemovingProfilePic(true);
    try {
      await updatePerson(person.id, { profilepic: "" });
      onUpdated();
      toast.success(t("toast.photoRemoved"));
    } catch (err) {
      console.error("Profile photo removal failed:", err);
      toast.error(t("toast.photoRemoveFailed"), {
        action: { label: t("toast.retry"), onClick: handleRemoveProfilePic },
      });
    } finally {
      setRemovingProfilePic(false);
    }
  };

  const handleDeletePerson = async () => {
    if (deletingPerson || !confirm(t("confirm.deletePerson"))) return;
    setDeletingPerson(true);
    try {
      const deleted = await deletePerson(person.id);
      onDeleted();
      toast.success(t("toast.personDeleted"), {
        duration: 10000,
        action: {
          label: t("toast.undo"),
          onClick: async () => {
            try {
              await rollbackHistory(deleted.revision_id);
              await onUpdated();
              toast.success(t("toast.changeUndone"));
            } catch (err) {
              console.error("Undo person deletion failed:", err);
              toast.error(t("toast.undoFailed"));
            }
          },
        },
      });
    } catch (err) {
      console.error("Delete person failed:", err);
      toast.error(t("toast.personDeleteFailed"), {
        action: { label: t("toast.retry"), onClick: handleDeletePerson },
      });
      setDeletingPerson(false);
    }
  };

  return (
    <div className="bg-white rounded-lg border shadow-sm p-6">
      <div className="flex items-start gap-6">
        {/* Profile picture with change overlay */}
        <div className="relative group flex-shrink-0">
          {person.profilepic ? (
            <img
              src={withSas(person.profilepic)}
              alt={person.fullname}
              className="w-32 h-32 rounded-full object-cover border-2 border-gray-200"
            />
          ) : (
            <div className="w-32 h-32 rounded-full bg-gray-200 flex items-center justify-center text-gray-400 text-3xl font-bold border-2 border-gray-200">
              {(person.firstname?.[0] || "?").toUpperCase()}
            </div>
          )}
          <div className="absolute inset-0 rounded-full flex flex-col items-center justify-center bg-black/40 text-white text-xs font-medium opacity-0 group-hover:opacity-100 transition-opacity gap-1">
            <label className="cursor-pointer hover:underline">
              {t("detail.changePhoto")}
              <input type="file" accept=".jpg,.jpeg,.png,.gif,.webp,.bmp,.tif,.tiff" className="hidden" onChange={handlePicSelected} />
            </label>
            {person.profilepic && (
              <button
                onClick={handleRemoveProfilePic}
                disabled={removingProfilePic}
                className="hover:underline text-red-300 disabled:cursor-not-allowed disabled:opacity-50"
              >
                {t("detail.removePhoto")}
              </button>
            )}
          </div>
        </div>

        {/* Crop modal */}
        {cropSrc && (
          <ProfileCropModal
            src={cropSrc}
            personId={person.id}
            onDone={() => { setCropSrc(null); onUpdated(); }}
            onCancel={() => setCropSrc(null)}
            t={t}
          />
        )}

        <div className="flex-1 min-w-0">
          {editing ? (
            <div className="space-y-3">
              <div className="grid grid-cols-2 gap-3">
                <div>
                  <label className="block text-xs font-medium text-gray-500 mb-1">{t("field.firstName")}</label>
                  <input
                    type="text"
                    value={draft.firstname}
                    onChange={(e) => updateDraft({ firstname: e.target.value })}
                    className="w-full border rounded px-3 py-1.5 text-sm text-gray-900"
                  />
                </div>
                <div>
                  <label className="block text-xs font-medium text-gray-500 mb-1">{t("field.lastName")}</label>
                  <input
                    type="text"
                    value={draft.lastname}
                    onChange={(e) => updateDraft({ lastname: e.target.value })}
                    className="w-full border rounded px-3 py-1.5 text-sm text-gray-900"
                  />
                </div>
              </div>
              <div>
                <label className="block text-xs font-medium text-gray-500 mb-1">{t("field.alias")}</label>
                <input
                  type="text"
                  value={draft.alias}
                  onChange={(e) => updateDraft({ alias: e.target.value })}
                  className="w-full border rounded px-3 py-1.5 text-sm text-gray-900"
                />
              </div>
              <div className="grid grid-cols-2 gap-3">
                <div>
                  <label className="block text-xs font-medium text-gray-500 mb-1">{t("field.birthdate")}</label>
                  <input
                    type="text"
                    value={draft.birthdate}
                    onChange={(e) => updateDraft({ birthdate: e.target.value })}
                    placeholder="dd/mm/yyyy"
                    className="w-full border rounded px-3 py-1.5 text-sm text-gray-900"
                  />
                </div>
                <div>
                  <label className="block text-xs font-medium text-gray-500 mb-1">{t("field.birthplace")}</label>
                  <input
                    type="text"
                    value={draft.birthplace}
                    onChange={(e) => updateDraft({ birthplace: e.target.value })}
                    className="w-full border rounded px-3 py-1.5 text-sm text-gray-900"
                  />
                </div>
              </div>
              <div className="flex items-center gap-4">
                <label className="flex items-center gap-2 text-sm text-gray-700">
                  <input
                    type="checkbox"
                    checked={draft.isAlive}
                    onChange={(e) => updateDraft({ isAlive: e.target.checked })}
                    className="rounded"
                  />
                  {t("field.alive")}
                </label>
                {!draft.isAlive && (
                  <div>
                    <label className="block text-xs font-medium text-gray-500 mb-1">{t("field.deathDate")}</label>
                    <input
                      type="text"
                      value={draft.deathdate}
                      onChange={(e) => updateDraft({ deathdate: e.target.value })}
                      placeholder="dd/mm/yyyy"
                      className="border rounded px-3 py-1.5 text-sm text-gray-900"
                    />
                  </div>
                )}
              </div>
              <ValidationMessages
                issues={validationIssues}
                submitting={saving}
                onOverride={() => handleSave(true)}
              />
              <div className="flex gap-2 pt-1">
                <button
                  onClick={() => void handleSave()}
                  disabled={saving}
                  className="px-4 py-1.5 bg-blue-600 text-white text-sm rounded hover:bg-blue-700 disabled:opacity-50"
                >
                  {saving ? t("detail.saving") : t("detail.save")}
                </button>
                <button
                  onClick={() => {
                    setValidationIssues([]);
                    setEditing(false);
                  }}
                  className="px-4 py-1.5 border text-sm rounded hover:bg-gray-50"
                >
                  {t("form.cancel")}
                </button>
              </div>
            </div>
          ) : (
            <>
              <div className="flex items-center gap-2 mb-1">
                <h1 className="text-2xl font-bold text-gray-900">
                  {person.fullname || "Unknown"}
                  {person.alias && <span className="text-lg font-normal text-gray-500 ml-2">({person.alias})</span>}
                </h1>
                <button
                  onClick={startEdit}
                  className="text-xs px-2 py-1 rounded border border-blue-300 text-blue-600 hover:bg-blue-50"
                >
                  {t("detail.edit")}
                </button>
              </div>
              {/* Action buttons */}
              <div className="flex flex-wrap gap-1 mb-3">
                <Link
                  href={`/story/?id=${person.id}&degree=3`}
                  className="text-xs px-2 py-1 rounded border border-gray-200 text-gray-600 hover:bg-gray-50"
                >
                  📖 {t("story.viewStory")}
                </Link>
                <Link
                  href={`/?root=${person.id}`}
                  className="text-xs px-2 py-1 rounded border border-gray-200 text-gray-600 hover:bg-gray-50"
                >
                  🎯 {t("menu.centerOn")}
                </Link>
                <button
                  onClick={() => { window.location.href = `/?action=add_child&nodeId=${person.id}`; }}
                  className="text-xs px-2 py-1 rounded border border-gray-200 text-gray-600 hover:bg-gray-50"
                >
                  ➕ {t("menu.addChild")}
                </button>
                <button
                  onClick={() => { window.location.href = `/?action=add_spouse&nodeId=${person.id}`; }}
                  className="text-xs px-2 py-1 rounded border border-gray-200 text-gray-600 hover:bg-gray-50"
                >
                  💑 {t("menu.addSpouse")}
                </button>
                <button
                  onClick={() => { window.location.href = `/?action=add_parent&nodeId=${person.id}`; }}
                  className="text-xs px-2 py-1 rounded border border-gray-200 text-gray-600 hover:bg-gray-50"
                >
                  👆 {t("menu.addParent")}
                </button>
                <button
                  onClick={() => { window.location.href = `/?action=link_child&nodeId=${person.id}`; }}
                  className="text-xs px-2 py-1 rounded border border-gray-200 text-gray-600 hover:bg-gray-50"
                >
                  🔗 {t("menu.linkChild")}
                </button>
                <button
                  onClick={() => { window.location.href = `/?action=link_spouse&nodeId=${person.id}`; }}
                  className="text-xs px-2 py-1 rounded border border-gray-200 text-gray-600 hover:bg-gray-50"
                >
                  🔗 {t("menu.linkSpouse")}
                </button>
                <button
                  onClick={() => { window.location.href = `/?action=link_parent&nodeId=${person.id}`; }}
                  className="text-xs px-2 py-1 rounded border border-gray-200 text-gray-600 hover:bg-gray-50"
                >
                  🔗 {t("menu.linkParent")}
                </button>
                {adminView && (
                  <button
                    onClick={handleDeletePerson}
                    disabled={deletingPerson}
                    className="text-xs px-2 py-1 rounded border border-red-200 text-red-500 hover:bg-red-50 disabled:cursor-not-allowed disabled:opacity-50"
                  >
                    🗑 {t("menu.deletePerson")}
                  </button>
                )}
              </div>
              <div className="grid grid-cols-2 gap-x-8 gap-y-1 text-sm">
                {person.birthdate && (
                  <>
                    <span className="text-gray-500">{t("person.born")}</span>
                    <span className="text-gray-900">{formatDate(person.birthdate)}</span>
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
                  {!!person.isAlive
                    ? t("person.living")
                    : `${t("person.deceased")}${person.deathdate ? ` (${person.deathdate})` : ""}`}
                </span>
              </div>
            </>
          )}
        </div>
      </div>
    </div>
  );
}

/* ------------------------------------------------------------------ */
/* Profile picture crop & upload modal                                  */
/* ------------------------------------------------------------------ */
function ProfileCropModal({
  src,
  personId,
  onDone,
  onCancel,
  t,
}: {
  src: string;
  personId: string;
  onDone: () => void;
  onCancel: () => void;
  t: TFunc;
}) {
  const toast = useToast();
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const imgRef = useRef<HTMLImageElement | null>(null);
  const [offset, setOffset] = useState({ x: 0, y: 0 });
  const [zoom, setZoom] = useState(1);
  const [uploading, setUploading] = useState(false);
  const [loaded, setLoaded] = useState(false);
  const cropSize = 280;

  const draw = useCallback(() => {
    const ctx = canvasRef.current?.getContext("2d");
    const img = imgRef.current;
    if (!ctx || !img) return;
    ctx.clearRect(0, 0, cropSize, cropSize);
    ctx.save();
    ctx.beginPath();
    ctx.arc(cropSize / 2, cropSize / 2, cropSize / 2, 0, Math.PI * 2);
    ctx.clip();
    const w = img.naturalWidth * zoom;
    const h = img.naturalHeight * zoom;
    ctx.drawImage(img, offset.x + (cropSize - w) / 2, offset.y + (cropSize - h) / 2, w, h);
    ctx.restore();
    ctx.beginPath();
    ctx.arc(cropSize / 2, cropSize / 2, cropSize / 2 - 1, 0, Math.PI * 2);
    ctx.strokeStyle = "#3b82f6";
    ctx.lineWidth = 2;
    ctx.stroke();
  }, [offset, zoom, cropSize]);

  const handleLoad = () => {
    const img = imgRef.current!;
    const fitScale = cropSize / Math.min(img.naturalWidth, img.naturalHeight);
    setZoom(fitScale);
    setLoaded(true);
  };

  if (loaded) {
    requestAnimationFrame(draw);
  }

  const handleMouseDown = (e: React.MouseEvent) => {
    const startX = e.clientX;
    const startY = e.clientY;
    const startOff = { ...offset };
    const onMove = (ev: MouseEvent) => {
      setOffset({ x: startOff.x + (ev.clientX - startX), y: startOff.y + (ev.clientY - startY) });
    };
    const onUp = () => {
      window.removeEventListener("mousemove", onMove);
      window.removeEventListener("mouseup", onUp);
    };
    window.addEventListener("mousemove", onMove);
    window.addEventListener("mouseup", onUp);
  };

  const handleUpload = async () => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const out = document.createElement("canvas");
    out.width = 400;
    out.height = 400;
    out.getContext("2d")!.drawImage(canvas, 0, 0, cropSize, cropSize, 0, 0, 400, 400);
    out.toBlob(async (blob) => {
      if (!blob) return;
      setUploading(true);
      try {
        await uploadProfilePic(personId, blob, "profile.jpg");
        toast.success(t("toast.photoUpdated"));
        onDone();
      } catch (err) {
        console.error("Upload failed:", err);
        toast.error(t("toast.photoUploadFailed"));
      } finally {
        setUploading(false);
      }
    }, "image/jpeg", 0.9);
  };

  return (
    <div className="fixed inset-0 z-[100] bg-black/60 flex items-center justify-center" onClick={onCancel}>
      <div className="bg-white rounded-xl p-6 shadow-2xl space-y-4 max-w-sm" onClick={(e) => e.stopPropagation()}>
        <p className="text-sm text-gray-600 font-medium">{t("pic.dragHint")}</p>

        <img ref={imgRef} src={src} alt="" className="hidden" onLoad={handleLoad} />

        <div className="flex justify-center">
          <canvas
            ref={canvasRef}
            width={cropSize}
            height={cropSize}
            className="rounded-full cursor-move border-2 border-gray-300"
            onMouseDown={handleMouseDown}
            onWheel={(e) => {
              e.preventDefault();
              setZoom((z) => Math.max(0.1, z + (e.deltaY < 0 ? 0.05 : -0.05)));
            }}
          />
        </div>

        <div className="flex items-center gap-2 text-xs text-gray-500">
          <span>−</span>
          <input
            type="range" min={0.1} max={3} step={0.01} value={zoom}
            onChange={(e) => setZoom(Number(e.target.value))}
            className="flex-1"
          />
          <span>+</span>
        </div>

        <div className="flex gap-2">
          <button
            onClick={handleUpload}
            disabled={uploading}
            className="px-4 py-1.5 bg-blue-600 text-white text-sm rounded-lg hover:bg-blue-700 disabled:opacity-50"
          >
            {uploading ? t("pic.uploading") : t("pic.uploadBtn")}
          </button>
          <button
            onClick={onCancel}
            disabled={uploading}
            className="px-4 py-1.5 bg-gray-200 border border-gray-300 text-gray-700 text-sm rounded-lg hover:bg-gray-300 disabled:cursor-not-allowed disabled:opacity-50"
          >
            {t("form.cancel")}
          </button>
        </div>
      </div>
    </div>
  );
}

/* ------------------------------------------------------------------ */
/* Pictures section with upload, tagging, and removal                   */
/* ------------------------------------------------------------------ */
function PersonPictures({
  personId,
  pics,
  personList,
  withSas,
  onLightbox,
  onUpdated,
  t,
  adminView,
}: {
  personId: string;
  pics: string[];
  personList: { id: string; fullname: string }[];
  withSas: (url: string | undefined) => string | undefined;
  onLightbox: (url: string) => void;
  onUpdated: () => void;
  t: TFunc;
  adminView: boolean;
}) {
  const toast = useToast();
  const [uploading, setUploading] = useState(false);
  const [preview, setPreview] = useState<{ file: File; dataUrl: string } | null>(null);
  const [taggedIds, setTaggedIds] = useState<string[]>([]);
  const [removing, setRemoving] = useState<string | null>(null);
  const [tagQuery, setTagQuery] = useState("");

  const taggable = personList.filter((p) => p.id !== personId);

  const handleFileSelected = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;
    const reader = new FileReader();
    reader.onload = () => setPreview({ file, dataUrl: reader.result as string });
    reader.readAsDataURL(file);
    e.target.value = "";
    setTaggedIds([]);
    setTagQuery("");
  };

  const handleUpload = async () => {
    if (!preview) return;
    setUploading(true);
    try {
      const result = await uploadPicture(personId, preview.file, preview.file.name);
      if (taggedIds.length > 0) {
        await tagPicture(personId, result.url, taggedIds);
      }
      setPreview(null);
      setTaggedIds([]);
      onUpdated();
      toast.success(t("toast.photoUploaded"));
    } catch (err) {
      console.error("Upload failed:", err);
      toast.error(t("toast.photoUploadFailed"));
    } finally {
      setUploading(false);
    }
  };

  const handleRemove = async (url: string) => {
    setRemoving(url);
    try {
      await removePicture(personId, url);
      onUpdated();
      toast.success(t("toast.photoRemoved"));
    } catch (err) {
      console.error("Remove failed:", err);
      toast.error(t("toast.photoRemoveFailed"), {
        action: { label: t("toast.retry"), onClick: () => handleRemove(url) },
      });
    } finally {
      setRemoving(null);
    }
  };

  const toggleTag = (id: string) => {
    setTaggedIds((prev) => prev.includes(id) ? prev.filter((x) => x !== id) : [...prev, id]);
  };

  const filteredTaggable = tagQuery.trim()
    ? taggable.filter((p) => p.fullname.toLowerCase().includes(tagQuery.toLowerCase()))
    : [];

  const tagged = taggable.filter((p) => taggedIds.includes(p.id));

  return (
    <div className="bg-white rounded-lg border shadow-sm p-6">
      <div className="flex items-center justify-between mb-4">
        <h2 className="font-semibold text-gray-900">
          {t("person.pictures")} {pics.length > 0 && <span className="text-gray-400 font-normal">({pics.length})</span>}
        </h2>
        {!preview && (
          <label className="text-xs px-3 py-1.5 rounded border border-gray-300 text-gray-700 hover:bg-gray-50 cursor-pointer">
            {t("pic.addPhoto")}
            <input type="file" accept=".jpg,.jpeg,.png,.gif,.webp,.bmp,.tif,.tiff" className="hidden" onChange={handleFileSelected} />
          </label>
        )}
      </div>

      {/* Upload preview + tagging */}
      {preview && (
        <div className="border rounded-lg p-4 bg-gray-50 space-y-3 mb-4">
          <img src={preview.dataUrl} alt="Preview" className="rounded border max-h-48 w-full object-contain" />

          {/* Tag people search */}
          {taggable.length > 0 && (
            <div>
              <p className="text-xs font-medium text-gray-600 mb-1">{t("pic.tagPeople")}</p>

              {tagged.length > 0 && (
                <div className="flex flex-wrap gap-1 mb-2">
                  {tagged.map((p) => (
                    <button
                      key={p.id}
                      type="button"
                      onClick={() => toggleTag(p.id)}
                      className="text-xs px-2 py-0.5 rounded-full bg-blue-100 border border-blue-400 text-blue-700 hover:bg-blue-200"
                    >
                      ✓ {p.fullname} ✕
                    </button>
                  ))}
                </div>
              )}

              <input
                type="text"
                value={tagQuery}
                onChange={(e) => setTagQuery(e.target.value)}
                placeholder={t("tag.searchPlaceholder")}
                className="w-full border rounded px-2 py-1.5 text-sm text-gray-900 mb-1"
              />

              {filteredTaggable.length > 0 && (
                <div className="max-h-32 overflow-y-auto border rounded bg-white">
                  {filteredTaggable.slice(0, 20).map((p) => {
                    const isTagged = taggedIds.includes(p.id);
                    return (
                      <button
                        key={p.id}
                        type="button"
                        onClick={() => { toggleTag(p.id); setTagQuery(""); }}
                        className={`w-full text-left px-2 py-1.5 text-sm hover:bg-blue-50 border-b last:border-b-0 ${
                          isTagged ? "bg-blue-50 text-blue-700" : "text-gray-900"
                        }`}
                      >
                        {isTagged ? "✓ " : ""}{p.fullname}
                      </button>
                    );
                  })}
                </div>
              )}
              {tagQuery.trim() && filteredTaggable.length === 0 && (
                <p className="text-xs text-gray-400 py-1">{t("tag.noMatches")}</p>
              )}
            </div>
          )}

          <div className="flex gap-2">
            <button
              onClick={handleUpload}
              disabled={uploading}
              className="px-4 py-1.5 bg-blue-600 text-white text-sm rounded hover:bg-blue-700 disabled:opacity-50"
            >
              {uploading ? t("pic.uploading") : t("pic.upload")}
            </button>
            <button
              onClick={() => { setPreview(null); setTaggedIds([]); }}
              className="px-4 py-1.5 border text-sm rounded hover:bg-gray-50"
            >
              {t("pic.cancel")}
            </button>
          </div>
        </div>
      )}

      {/* Gallery grid */}
      {pics.length > 0 ? (
        <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-4 gap-3">
          {pics.map((url, i) => (
            <PictureCard
              key={i}
              personId={personId}
              url={url}
              withSas={withSas}
              personList={personList}
              adminView={adminView}
              onLightbox={onLightbox}
              onRemove={() => handleRemove(url)}
              removing={removing === url}
              onUpdated={onUpdated}
              t={t}
            />
          ))}
        </div>
      ) : (
        <p className="text-sm text-gray-400">{t("person.noPictures")}</p>
      )}
    </div>
  );
}

/* ------------------------------------------------------------------ */
/* Individual picture card with tagged people                           */
/* ------------------------------------------------------------------ */
function PictureCard({
  personId,
  url,
  withSas,
  personList,
  adminView,
  onLightbox,
  onRemove,
  removing,
  onUpdated,
  t,
}: {
  personId: string;
  url: string;
  withSas: (url: string | undefined) => string | undefined;
  personList: { id: string; fullname: string }[];
  adminView: boolean;
  onLightbox: (url: string) => void;
  onRemove: () => void;
  removing: boolean;
  onUpdated: () => void;
  t: TFunc;
}) {
  const toast = useToast();
  const [people, setPeople] = useState<{ id: string; fullname: string }[]>([]);
  const [showTagEditor, setShowTagEditor] = useState(false);
  const [tagQuery, setTagQuery] = useState("");
  const [taggingId, setTaggingId] = useState<string | null>(null);

  useEffect(() => {
    getPeopleInPicture(personId, url).then(setPeople).catch(() => setPeople([]));
  }, [personId, url]);

  const handleUntag = async (targetId: string) => {
    if (taggingId) return;
    setTaggingId(targetId);
    try {
      await untagPicture(personId, url, targetId);
      setPeople((prev) => prev.filter((p) => p.id !== targetId));
      onUpdated();
      toast.success(t("toast.photoTagUpdated"));
    } catch (err) {
      console.error("Untag failed:", err);
      toast.error(t("toast.photoTagFailed"), {
        action: { label: t("toast.retry"), onClick: () => handleUntag(targetId) },
      });
    } finally {
      setTaggingId(null);
    }
  };

  const handleTag = async (targetId: string) => {
    if (taggingId) return;
    setTaggingId(targetId);
    try {
      await tagPicture(personId, url, [targetId]);
      const added = personList.find((p) => p.id === targetId);
      if (added) setPeople((prev) => [...prev, added]);
      setTagQuery("");
      toast.success(t("toast.photoTagUpdated"));
    } catch (err) {
      console.error("Tag failed:", err);
      toast.error(t("toast.photoTagFailed"), {
        action: { label: t("toast.retry"), onClick: () => handleTag(targetId) },
      });
    } finally {
      setTaggingId(null);
    }
  };

  const alreadyTagged = new Set(people.map((p) => p.id));
  const filtered = tagQuery.trim()
    ? personList.filter((p) => !alreadyTagged.has(p.id) && p.fullname.toLowerCase().includes(tagQuery.toLowerCase()))
    : [];

  return (
    <div className="space-y-1">
      <div className="relative group">
        <img
          src={withSas(url) || url}
          alt=""
          className="rounded-lg border object-contain h-40 w-full bg-gray-100 cursor-pointer hover:shadow-md transition-shadow"
          onClick={() => onLightbox(withSas(url) || url)}
        />
        {adminView && (
          <>
            <button
              onClick={onRemove}
              disabled={removing}
              className="absolute top-1 right-1 bg-black/50 text-white text-xs rounded-full w-6 h-6 flex items-center justify-center opacity-0 group-hover:opacity-100 transition-opacity hover:bg-red-600"
              title={t("pic.remove")}
            >
              ✕
            </button>
            <button
              onClick={() => setShowTagEditor(!showTagEditor)}
              className="absolute top-1 left-1 bg-black/50 text-white text-xs rounded-full w-6 h-6 flex items-center justify-center opacity-0 group-hover:opacity-100 transition-opacity hover:bg-blue-600"
              title="Edit tags"
            >
              🏷
            </button>
          </>
        )}
      </div>

      {/* Tagged people names */}
      {people.length > 0 && (
        <div className="flex flex-wrap gap-1">
          {people.map((p) => (
            <span key={p.id} className="text-xs text-gray-500">{p.fullname}</span>
          ))}
        </div>
      )}

      {/* Tag editor (admin only) */}
      {showTagEditor && adminView && (
        <div className="border rounded p-2 bg-gray-50 space-y-1">
          {/* Current tags with remove */}
          {people.length > 0 && (
            <div className="flex flex-wrap gap-1 mb-1">
              {people.map((p) => (
                <button
                  key={p.id}
                  onClick={() => handleUntag(p.id)}
                  disabled={taggingId !== null}
                  className="text-xs px-1.5 py-0.5 rounded-full bg-blue-100 border border-blue-300 text-blue-700 hover:bg-red-100 hover:border-red-300 hover:text-red-700 disabled:cursor-not-allowed disabled:opacity-50"
                  title={`Untag ${p.fullname}`}
                >
                  {p.fullname} ✕
                </button>
              ))}
            </div>
          )}
          {/* Search to add */}
          <input
            type="text"
            disabled={taggingId !== null}
            value={tagQuery}
            onChange={(e) => setTagQuery(e.target.value)}
            placeholder={t("tag.searchPlaceholder")}
            className="w-full border rounded px-2 py-1 text-xs text-gray-900"
          />
          {filtered.length > 0 && (
            <div className="max-h-24 overflow-y-auto border rounded bg-white">
              {filtered.slice(0, 10).map((p) => (
                <button
                  key={p.id}
                  onClick={() => handleTag(p.id)}
                  disabled={taggingId !== null}
                  className="w-full text-left px-2 py-1 text-xs hover:bg-blue-50 border-b last:border-b-0 text-gray-900 disabled:cursor-not-allowed disabled:opacity-50"
                >
                  {p.fullname}
                </button>
              ))}
            </div>
          )}
          {tagQuery.trim() && filtered.length === 0 && (
            <p className="text-xs text-gray-400">{t("tag.noMatches")}</p>
          )}
        </div>
      )}
    </div>
  );
}

/* ------------------------------------------------------------------ */
/* Relationship delete button                                          */
/* ------------------------------------------------------------------ */
function RelDeleteBtn({
  source,
  target,
  onDeleted,
  t,
}: {
  source: string;
  target: string;
  onDeleted: () => void;
  t: TFunc;
}) {
  const toast = useToast();
  const [busy, setBusy] = useState(false);

  const handleDelete = async () => {
    if (!confirm(t("rel.confirmDelete"))) return;
    setBusy(true);
    try {
      const deleted = await deleteRelationship(source, target);
      onDeleted();
      toast.success(t("toast.relationshipDeleted"), {
        duration: 10000,
        action: {
          label: t("toast.undo"),
          onClick: async () => {
            try {
              await rollbackHistory(deleted.revision_id);
              onDeleted();
              toast.success(t("toast.changeUndone"));
            } catch (err) {
              console.error("Undo relationship deletion failed:", err);
              toast.error(t("toast.undoFailed"));
            }
          },
        },
      });
    } catch (err) {
      console.error("Delete failed:", err);
      toast.error(t("toast.relationshipDeleteFailed"), {
        action: { label: t("toast.retry"), onClick: handleDelete },
      });
    } finally {
      setBusy(false);
    }
  };

  return (
    <button
      onClick={handleDelete}
      disabled={busy}
      className="flex-shrink-0 text-xs text-gray-300 hover:text-red-500 transition-colors disabled:opacity-50"
      title={t("rel.delete")}
    >
      {busy ? "…" : "✕"}
    </button>
  );
}

/* ------------------------------------------------------------------ */
/* Spouse relationship active/inactive toggle                          */
/* ------------------------------------------------------------------ */
function SpouseToggle({
  source,
  target,
  isActive,
  onToggled,
  t,
}: {
  source: string;
  target: string;
  isActive: boolean;
  onToggled: () => void;
  t: TFunc;
}) {
  const toast = useToast();
  const [busy, setBusy] = useState(false);

  const handleToggle = async () => {
    setBusy(true);
    try {
      if (isActive) {
        await deactivateRelationship(source, target);
      } else {
        await reactivateRelationship(source, target);
      }
      onToggled();
      toast.success(t("toast.relationshipUpdated"));
    } catch (err) {
      console.error("Toggle failed:", err);
      toast.error(t("toast.relationshipUpdateFailed"), {
        action: { label: t("toast.retry"), onClick: handleToggle },
      });
    } finally {
      setBusy(false);
    }
  };

  return (
    <button
      onClick={handleToggle}
      disabled={busy}
      className={`ml-auto flex-shrink-0 text-xs px-1.5 py-0.5 rounded border transition-colors ${
        isActive
          ? "border-green-300 text-green-700 hover:bg-green-50"
          : "border-gray-300 text-gray-500 hover:bg-gray-50"
      } disabled:opacity-50`}
      title={isActive ? t("rel.deactivate") : t("rel.reactivate")}
    >
      {busy ? "…" : isActive ? t("rel.active") : t("rel.relInactive")}
    </button>
  );
}

"use client";

import { useState, useEffect, useRef, useCallback } from "react";
import Link from "next/link";
import type { PersonNode, GraphEdge } from "@/lib/types";
import { updatePerson, tagPicture, removePicture, deactivateRelationship, reactivateRelationship, deleteRelationship, rollbackHistory, getNotes, addNote, deleteNote, getValidationIssues, type Note, type ValidationIssue } from "@/lib/api";
import { useI18n } from "@/lib/i18n";
import { useAdminView } from "@/lib/adminView";
import { formatDate, formatTimestamp } from "@/lib/dateUtils";
import { useToast } from "@/components/ToastProvider";
import type { PersonActionDefinition } from "@/lib/personActions";
import ValidationMessages from "@/components/ValidationMessages";
import { PhotoPicker, ProfilePhotoCrop, UploadProgress } from "@/components/PhotoTools";
import type { PreparedPhoto } from "@/lib/photoProcessing";
import { uploadPhotoWithProgress } from "@/lib/photoUpload";

interface Props {
  person: PersonNode | null;
  relationships?: GraphEdge[];
  siblings?: string[];
  personList?: { id: string; fullname: string }[];
  devMode?: boolean;
  sasToken?: string;
  currentUserEmail?: string;
  onClose?: () => void;
  onPersonUpdated?: () => void;
  onAction?: (action: string, nodeId: string) => void;
  actions?: PersonActionDefinition[];
  onOpenActions?: (nodeId: string) => void;
}

export default function DetailPanel({
  person,
  relationships = [],
  siblings = [],
  personList = [],
  devMode = false,
  sasToken = "",
  currentUserEmail = "",
  onClose,
  onPersonUpdated,
  onAction,
  actions = [],
  onOpenActions,
}: Props) {
  const { t } = useI18n();
  const toast = useToast();
  const { adminView } = useAdminView();
  const [editing, setEditing] = useState(false);
  const [draft, setDraft] = useState<Record<string, unknown>>({});
  const [saving, setSaving] = useState(false);
  const [validationIssues, setValidationIssues] = useState<ValidationIssue[]>([]);
  const [cropPhoto, setCropPhoto] = useState<PreparedPhoto | null>(null);
  const [removingProfilePic, setRemovingProfilePic] = useState(false);

  // Reset edit mode when the selected person changes
  const personId = person?.id;
  useEffect(() => {
    setEditing(false);
    setDraft({});
    setValidationIssues([]);
    setCropPhoto(null);
  }, [personId]);

  if (!person) {
    return (
      <div className="p-6 text-gray-400 text-center">
        <p className="text-lg">{t("detail.selectPerson")}</p>
        <p className="text-sm mt-2">{t("detail.selectHint")}</p>
      </div>
    );
  }

  const withSas = (url: string | undefined) => {
    if (!url) return undefined;
    if (!sasToken) return url;
    return url.includes("?") ? url : `${url}?${sasToken}`;
  };

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

  const cancelEdit = () => {
    setEditing(false);
    setValidationIssues([]);
    setCropPhoto(null);
  };

  const saveEdit = async (overrideWarnings = false) => {
    if (saving) return;
    setSaving(true);
    try {
      await updatePerson(person.id, draft, overrideWarnings);
      setEditing(false);
      setValidationIssues([]);
      onPersonUpdated?.();
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
          onClick: () => saveEdit(overrideWarnings),
        },
      });
    } finally {
      setSaving(false);
    }
  };

  const removeProfilePic = async () => {
    if (removingProfilePic || !confirm(t("detail.deletePhoto"))) return;
    setRemovingProfilePic(true);
    try {
      await updatePerson(person.id, { profilepic: "" });
      onPersonUpdated?.();
      toast.success(t("toast.photoRemoved"));
    } catch (err) {
      console.error("Profile photo removal failed:", err);
      toast.error(t("toast.photoRemoveFailed"), {
        action: { label: t("toast.retry"), onClick: removeProfilePic },
      });
    } finally {
      setRemovingProfilePic(false);
    }
  };

  const clearField = (field: string) => {
    setValidationIssues([]);
    setDraft((d) => ({ ...d, [field]: "" }));
  };

  // --- Profile picture upload / crop ---
  return (
    <div className="p-4 space-y-4 overflow-y-auto h-full">
      {/* Header */}
      <div className="flex justify-between items-start">
        <div>
          <h2 className="text-xl font-bold text-gray-900">{person.fullname || "Unknown"}</h2>
          <Link
            href={`/person/?id=${person.id}`}
            className="text-xs text-blue-600 hover:underline"
          >
            {t("detail.viewProfile")}
          </Link>
        </div>
        <div className="flex gap-1">
          {!editing && (
            <button
              onClick={startEdit}
              className="text-xs px-2 py-1 rounded border border-blue-300 text-blue-600 hover:bg-blue-50"
            >
              {t("detail.edit")}
            </button>
          )}
          {onClose && (
            <button
              type="button"
              onClick={onClose}
              aria-label={t("detail.close")}
              className="ml-1 flex min-h-11 min-w-11 items-center justify-center rounded-full text-xl text-gray-500 hover:bg-gray-100 hover:text-gray-700 md:min-h-0 md:min-w-0 md:text-base"
            >
              ✕
            </button>
          )}
        </div>
      </div>

      {/* Quick actions */}
      {onAction && !editing && (
        <>
          {onOpenActions && (
            <button
              type="button"
              onClick={() => onOpenActions(person.id)}
              className="flex min-h-11 w-full items-center justify-center gap-2 rounded-lg border border-blue-200 bg-blue-50 px-4 py-2 text-sm font-medium text-blue-700 md:hidden"
            >
              <span aria-hidden="true">•••</span>
              {t("actions.open")}
            </button>
          )}
          <PersonActionBar personId={person.id} onAction={onAction} actions={actions} />
        </>
      )}

      {/* Profile picture */}
      <div className="flex items-center gap-3">
        {person.profilepic ? (
          <img
            src={withSas(person.profilepic)}
            alt={person.fullname}
            className="w-24 h-24 rounded-full object-cover border-2 border-gray-200"
          />
        ) : (
          <div className="w-24 h-24 rounded-full bg-gray-200 flex items-center justify-center text-gray-400 text-2xl font-bold border-2 border-gray-200">
            {(person.firstname?.[0] || "?").toUpperCase()}
          </div>
        )}
        {editing && (
          <div className="flex flex-col gap-1">
            <PhotoPicker compact onSelected={setCropPhoto} />
            {person.profilepic && (
              <button
                onClick={removeProfilePic}
                disabled={removingProfilePic}
                className="text-xs px-2 py-1 rounded border border-red-300 text-red-500 hover:bg-red-50 text-center disabled:cursor-not-allowed disabled:opacity-50"
              >
                {removingProfilePic ? "…" : t("detail.removePhoto")}
              </button>
            )}
          </div>
        )}
      </div>

      {/* Crop modal */}
      {cropPhoto && (
        <ProfilePhotoCrop
          photo={cropPhoto}
          personId={person.id}
          onDone={() => {
            setCropPhoto(null);
            onPersonUpdated?.();
          }}
          onCancel={() => setCropPhoto(null)}
        />
      )}

      {/* Fields */}
      {editing ? (
        <EditFields
          draft={draft}
          setDraft={(next) => {
            setValidationIssues([]);
            setDraft(next);
          }}
          onClear={clearField}
        />
      ) : (
        <ViewFields person={person} />
      )}

      {/* Save / Cancel */}
      {editing && (
        <>
          <ValidationMessages
            issues={validationIssues}
            submitting={saving}
            onOverride={() => saveEdit(true)}
          />
          <div className="flex gap-2 pt-1">
            <button
              onClick={() => void saveEdit()}
              disabled={saving}
              className="px-4 py-1.5 bg-blue-600 text-white text-sm rounded hover:bg-blue-700 disabled:opacity-50"
            >
              {saving ? t("detail.saving") : t("detail.save")}
            </button>
            <button
              onClick={cancelEdit}
              className="px-4 py-1.5 border text-sm rounded hover:bg-gray-50"
            >
              {t("detail.cancel")}
            </button>
          </div>
        </>
      )}

      {/* Relationships */}
      {relationships.length > 0 && (
        <div>
          <h3 className="font-semibold text-sm text-gray-600 uppercase tracking-wide mb-2">
            {t("rel.title")}
          </h3>
          <ul className="space-y-1.5 text-sm">
            {(() => {
              const seenSpouses = new Set<string>();
              return relationships.map((rel, i) => {
                const isSource = rel.source === person.id;
                const otherId = isSource ? rel.target : rel.source;
                // Deduplicate bidirectional spouse edges
                if (rel.type === "isSpouseOf") {
                  if (seenSpouses.has(otherId)) return null;
                  seenSpouses.add(otherId);
                }
                const otherName = personList.find((p) => p.id === otherId)?.fullname || otherId;
                let label: string;
                if (rel.type === "isChildOf") {
                  label = isSource ? t("rel.parent") : t("rel.child");
                } else if (rel.type === "isSpouseOf") {
                  label = t("rel.spouse");
                } else {
                  label = rel.type;
                }
                const isActive = rel.is_active !== false;
                const canToggle = rel.type !== "isChildOf";
                return (
                  <li key={i} className={`flex items-center gap-2 ${!isActive ? "opacity-60" : ""}`}>
                    <span
                      className="inline-block w-2 h-2 rounded-full flex-shrink-0"
                      style={{ backgroundColor: rel.type === "isChildOf" ? "#ef4444" : "#3b82f6" }}
                    />
                    <span className="font-medium text-gray-700">{label}:</span>
                    <span className="text-gray-900">{otherName}</span>
                    {rel.start_date && <span className="text-xs text-gray-500">{formatDate(rel.start_date)}</span>}
                    {rel.end_date && <span className="text-xs text-gray-500">– {formatDate(rel.end_date)}</span>}
                    {canToggle && (
                      <RelationshipToggle
                        source={rel.source}
                        target={rel.target}
                        isActive={isActive}
                        onToggled={onPersonUpdated}
                      />
                    )}
                    {adminView && (
                      <RelationshipDeleteBtn
                        source={rel.source}
                        target={rel.target}
                        onDeleted={onPersonUpdated}
                      />
                    )}
                  </li>
                );
              });
            })()}
          </ul>
        </div>
      )}

      {/* Siblings */}
      {siblings.length > 0 && (
        <div>
          <h3 className="font-semibold text-sm text-gray-600 uppercase tracking-wide mb-2">
            {t("rel.siblings")}
          </h3>
          <ul className="text-sm text-gray-800">
            {[...new Set(siblings)].map((s) => (
              <li key={s}>{personList.find((p) => p.id === s)?.fullname || s}</li>
            ))}
          </ul>
        </div>
      )}

      {/* Pictures gallery */}
      <PicturesGallery
        person={person}
        personList={personList}
        sasToken={sasToken}
        withSas={withSas}
        onUpdated={onPersonUpdated}
      />

      {/* Notes */}
      <NotesSection personId={person.id} currentUserEmail={currentUserEmail} />

      {/* Dev mode */}
      {devMode && <RawJsonSection label={t("dev.nodeJson")} data={person} />}
      {devMode && relationships.length > 0 && <RawJsonSection label={t("dev.relJson")} data={relationships} />}
    </div>
  );
}

/* ------------------------------------------------------------------ */
/* View-only fields                                                    */
/* ------------------------------------------------------------------ */
function ViewFields({ person }: { person: PersonNode }) {
  const { t } = useI18n();
  return (
    <div className="space-y-2 text-sm text-gray-900">
      {person.firstname && (
        <div>
          <span className="font-medium text-gray-600">{t("field.firstName")}:</span> {person.firstname}
        </div>
      )}
      {person.lastname && (
        <div>
          <span className="font-medium text-gray-600">{t("field.lastName")}:</span> {person.lastname}
        </div>
      )}
      {person.alias && (
        <div>
          <span className="font-medium text-gray-600">{t("field.alias")}:</span> {person.alias}
        </div>
      )}
      {person.birthdate && (
        <div>
          <span className="font-medium text-gray-600">{t("field.born")}</span> {formatDate(person.birthdate)}
        </div>
      )}
      {person.birthplace && (
        <div>
          <span className="font-medium text-gray-600">{t("field.birthplace")}:</span> {person.birthplace}
        </div>
      )}
      <div>
        <span className="font-medium text-gray-600">{t("field.status")}</span>{" "}
        {!!person.isAlive ? t("field.living") : `${t("field.deceased")}${person.deathdate ? ` (${formatDate(person.deathdate)})` : ""}`}
      </div>
    </div>
  );
}

/* ------------------------------------------------------------------ */
/* Editable fields                                                     */
/* ------------------------------------------------------------------ */
function EditFields({
  draft,
  setDraft,
  onClear,
}: {
  draft: Record<string, unknown>;
  setDraft: React.Dispatch<React.SetStateAction<Record<string, unknown>>>;
  onClear: (field: string) => void;
}) {
  const { t } = useI18n();
  const set = (field: string, value: unknown) => setDraft((d) => ({ ...d, [field]: value }));

  return (
    <div className="space-y-3 text-sm text-gray-900">
      <Field label={t("field.firstName")} value={draft.firstname as string} onChange={(v) => set("firstname", v)} onClear={() => onClear("firstname")} clearLabel={t("detail.clear")} />
      <Field label={t("field.lastName")} value={draft.lastname as string} onChange={(v) => set("lastname", v)} onClear={() => onClear("lastname")} clearLabel={t("detail.clear")} />
      <Field label={t("field.alias")} value={draft.alias as string} onChange={(v) => set("alias", v)} onClear={() => onClear("alias")} clearLabel={t("detail.clear")} />
      <Field label={t("field.birthdate")}value={draft.birthdate as string} onChange={(v) => set("birthdate", v)} onClear={() => onClear("birthdate")} placeholder={t("field.placeholderDate")} clearLabel={t("detail.clear")} />
      <Field label={t("field.birthplace")} value={draft.birthplace as string} onChange={(v) => set("birthplace", v)} onClear={() => onClear("birthplace")} clearLabel={t("detail.clear")} />

      <div className="flex items-center gap-2">
        <label className="font-medium text-gray-600 text-sm">{t("field.alive")}</label>
        <input
          type="checkbox"
          checked={draft.isAlive as boolean}
          onChange={(e) => set("isAlive", e.target.checked)}
          className="rounded"
        />
      </div>

      {!(draft.isAlive as boolean) && (
        <Field label={t("field.deathDate")} value={draft.deathdate as string} onChange={(v) => set("deathdate", v)} onClear={() => onClear("deathdate")} placeholder={t("field.placeholderDeathDate")} clearLabel={t("detail.clear")} />
      )}
    </div>
  );
}

function Field({
  label,
  value,
  onChange,
  onClear,
  type = "text",
  placeholder,
  clearLabel = "Clear",
}: {
  label: string;
  value: string;
  onChange: (v: string) => void;
  onClear: () => void;
  type?: string;
  placeholder?: string;
  clearLabel?: string;
}) {
  return (
    <div>
      <label className="block text-xs font-medium text-gray-600 mb-0.5">{label}</label>
      <div className="flex gap-1">
        <input
          type={type}
          value={value}
          placeholder={placeholder}
          onChange={(e) => onChange(e.target.value)}
          className="flex-1 border rounded px-2 py-1 text-sm text-gray-900"
        />
        {value && (
          <button
            type="button"
            onClick={onClear}
            className="text-gray-400 hover:text-red-500 text-xs px-1"
            title={clearLabel}
          >
            ✕
          </button>
        )}
      </div>
    </div>
  );
}

/* ------------------------------------------------------------------ */
/* Pictures gallery with upload + tagging                              */
/* ------------------------------------------------------------------ */
function PicturesGallery({
  person,
  personList,
  sasToken,
  withSas,
  onUpdated,
}: {
  person: PersonNode;
  personList: { id: string; fullname: string }[];
  sasToken: string;
  withSas: (url: string | undefined) => string | undefined;
  onUpdated?: () => void;
}) {
  const { t } = useI18n();
  const toast = useToast();
  const [preview, setPreview] = useState<PreparedPhoto | null>(null);
  const [uploadProgress, setUploadProgress] = useState<number | null>(null);
  const [uploadFailed, setUploadFailed] = useState(false);
  const [uploadedUrl, setUploadedUrl] = useState<string | null>(null);
  const uploadAbort = useRef<AbortController | null>(null);
  const [taggedIds, setTaggedIds] = useState<string[]>([]);
  const [removing, setRemoving] = useState<string | null>(null);
  const [lightboxUrl, setLightboxUrl] = useState<string | null>(null);

  const pics = person.pictures && person.pictures.length > 0 ? person.pictures : [];

  const handleSelected = (photo: PreparedPhoto) => {
    if (preview) URL.revokeObjectURL(preview.previewUrl);
    setPreview(photo);
    setTaggedIds([]);
    setUploadFailed(false);
    setUploadedUrl(null);
  };

  const handleUpload = async () => {
    if (!preview) return;
    const controller = new AbortController();
    uploadAbort.current = controller;
    setUploadFailed(false);
    setUploadProgress(uploadedUrl ? 100 : 0);
    try {
      let pictureUrl = uploadedUrl;
      if (!pictureUrl) {
        const result = await uploadPhotoWithProgress(
          person.id,
          "pictures",
          preview.file,
          preview.file.name,
          preview.uploadId,
          setUploadProgress,
          controller.signal,
        );
        pictureUrl = result.url;
        setUploadedUrl(pictureUrl);
      }
      // Tag other people
      if (taggedIds.length > 0) {
        await tagPicture(person.id, pictureUrl, taggedIds);
      }
      URL.revokeObjectURL(preview.previewUrl);
      setPreview(null);
      setUploadedUrl(null);
      setTaggedIds([]);
      onUpdated?.();
      toast.success(t("toast.photoUploaded"));
    } catch (err) {
      if (!(err instanceof DOMException && err.name === "AbortError")) {
        console.error("Upload failed:", err);
        setUploadFailed(true);
        toast.error(t("toast.photoUploadFailed"));
      }
    } finally {
      setUploadProgress(null);
    }
  };

  useEffect(() => () => {
    uploadAbort.current?.abort();
    if (preview) URL.revokeObjectURL(preview.previewUrl);
  }, [preview]);

  const handleRemove = async (url: string) => {
    setRemoving(url);
    try {
      await removePicture(person.id, url);
      onUpdated?.();
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
    setTaggedIds((prev) => (prev.includes(id) ? prev.filter((x) => x !== id) : [...prev, id]));
  };

  // Persons available for tagging (exclude current person)
  const taggable = personList.filter((p) => p.id !== person.id);

  return (
    <div>
      <div className="flex items-center justify-between mb-2">
        <h3 className="font-semibold text-sm text-gray-600 uppercase tracking-wide">
          {t("pic.title")} {pics.length > 0 && <span className="text-gray-400 normal-case">({pics.length})</span>}
        </h3>
        {!preview && (
          <PhotoPicker compact onSelected={handleSelected} />
        )}
      </div>

      {/* Upload preview + tagging */}
      {preview && (
        <div className="border rounded-lg p-3 bg-gray-50 space-y-3 mb-3">
          <img src={preview.previewUrl} alt={t("pic.previewAlt")} className="rounded border max-h-40 w-full object-contain" />

          {/* Tag people */}
          {taggable.length > 0 && (
            <PersonTagSearch
              persons={taggable}
              taggedIds={taggedIds}
              onToggle={toggleTag}
            />
          )}

          <div className="flex gap-2">
            <button
              onClick={handleUpload}
              disabled={uploadProgress !== null}
              className="px-3 py-1 bg-blue-600 text-white text-xs rounded hover:bg-blue-700 disabled:opacity-50"
            >
              {uploadProgress !== null ? t("pic.uploading") : t("pic.upload")}
            </button>
            <button
              onClick={() => {
                URL.revokeObjectURL(preview.previewUrl);
                setPreview(null);
                setTaggedIds([]);
              }}
              disabled={uploadProgress !== null}
              className="px-3 py-1 border text-xs rounded hover:bg-gray-50 disabled:cursor-not-allowed disabled:opacity-50"
            >
              {t("pic.cancel")}
            </button>
          </div>
          <UploadProgress
            progress={uploadProgress}
            error={uploadFailed}
            onCancel={() => uploadAbort.current?.abort()}
            onRetry={handleUpload}
          />
        </div>
      )}

      {/* Gallery grid */}
      {pics.length > 0 && (
        <div className="grid grid-cols-2 gap-2">
          {pics.map((url, i) => (
            <div key={`pic-${i}-${url.slice(-12)}`} className="relative group">
              <img
                src={withSas(url) || url}
                alt=""
                className="rounded border object-contain h-24 w-full cursor-pointer bg-gray-100"
                onClick={() => setLightboxUrl(withSas(url) || url)}
              />
              <button
                onClick={() => handleRemove(url)}
                disabled={removing === url}
                className="absolute top-1 right-1 bg-black/50 text-white text-xs rounded-full w-5 h-5 flex items-center justify-center opacity-0 group-hover:opacity-100 transition-opacity hover:bg-red-600"
                title={t("pic.remove")}
              >
                ✕
              </button>
            </div>
          ))}
        </div>
      )}

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
/* Notes section                                                       */
/* ------------------------------------------------------------------ */
function NotesSection({
  personId,
  currentUserEmail,
}: {
  personId: string;
  currentUserEmail: string;
}) {
  const { t } = useI18n();
  const toast = useToast();
  const { adminView, userEmail, userName } = useAdminView();
  const [notes, setNotes] = useState<Note[]>([]);
  const [loading, setLoading] = useState(true);
  const [newText, setNewText] = useState("");
  const [adding, setAdding] = useState(false);
  const [deletingIndex, setDeletingIndex] = useState<number | null>(null);

  const effectiveEmail = currentUserEmail || userEmail;
  const noteAuthor = userName && effectiveEmail
    ? `${userName} (${effectiveEmail})`
    : userName || effectiveEmail || "anonymous";

  const refresh = useCallback(() => {
    getNotes(personId)
      .then(setNotes)
      .catch(() => setNotes([]))
      .finally(() => setLoading(false));
  }, [personId]);

  useEffect(() => {
    setLoading(true);
    setNewText("");
    refresh();
  }, [refresh]);

  const handleAdd = async () => {
    if (!newText.trim()) return;
    setAdding(true);
    try {
      await addNote(personId, newText.trim(), noteAuthor);
      setNewText("");
      refresh();
      toast.success(t("toast.noteAdded"));
    } catch (err) {
      console.error("Failed to add note:", err);
      toast.error(t("toast.noteAddFailed"));
    } finally {
      setAdding(false);
    }
  };

  const handleDelete = async (index: number) => {
    if (deletingIndex !== null) return;
    setDeletingIndex(index);
    try {
      await deleteNote(personId, index);
      refresh();
      toast.success(t("toast.noteDeleted"));
    } catch (err) {
      console.error("Failed to delete note:", err);
      toast.error(t("toast.noteDeleteFailed"));
    } finally {
      setDeletingIndex(null);
    }
  };

  return (
    <div>
      <h3 className="font-semibold text-sm text-gray-600 uppercase tracking-wide mb-2">
        {t("notes.title")} {notes.length > 0 && <span className="text-gray-400 normal-case">({notes.length})</span>}
      </h3>

      {loading ? (
        <p className="text-xs text-gray-400">{t("notes.loading")}</p>
      ) : (
        <>
          {/* Existing notes */}
          {notes.length > 0 && (
            <div className="space-y-2 mb-3">
              {notes.map((note, i) => (
                <div key={i} className="bg-gray-50 rounded border p-2 text-sm group relative">
                  <p className="text-gray-900 whitespace-pre-wrap">{note.text}</p>
                  <div className="flex items-center gap-2 mt-1">
                    <span className="text-xs text-gray-500">— {note.author}</span>
                    <span className="text-xs text-gray-400">{formatTimestamp(note.timestamp)}</span>
                  </div>
                  {adminView && (
                    <button
                      onClick={() => handleDelete(i)}
                      disabled={deletingIndex !== null}
                      className="absolute top-1 right-1 text-xs text-gray-300 hover:text-red-500 opacity-0 group-hover:opacity-100 transition-opacity disabled:cursor-not-allowed disabled:opacity-50"
                      title={t("notes.delete")}
                    >
                      ✕
                    </button>
                  )}
                </div>
              ))}
            </div>
          )}

          {/* Add note */}
          <div className="space-y-1">
            <textarea
              value={newText}
              onChange={(e) => setNewText(e.target.value)}
              placeholder={t("notes.placeholder")}
              rows={2}
              className="w-full border rounded px-2 py-1.5 text-sm text-gray-900 resize-y"
            />
            <button
              onClick={handleAdd}
              disabled={adding || !newText.trim()}
              className="px-3 py-1 bg-blue-600 text-white text-xs rounded hover:bg-blue-700 disabled:opacity-50"
            >
              {adding ? t("notes.adding") : t("notes.add")}
            </button>
          </div>
        </>
      )}
    </div>
  );
}

/* ------------------------------------------------------------------ */
/* Relationship delete button (admin only)                             */
/* ------------------------------------------------------------------ */
function RelationshipDeleteBtn({
  source,
  target,
  onDeleted,
}: {
  source: string;
  target: string;
  onDeleted?: () => void;
}) {
  const { t } = useI18n();
  const toast = useToast();
  const [busy, setBusy] = useState(false);

  const handleDelete = async () => {
    if (!confirm(t("rel.confirmDelete"))) return;
    setBusy(true);
    try {
      const deleted = await deleteRelationship(source, target);
      onDeleted?.();
      toast.success(t("toast.relationshipDeleted"), {
        duration: 10000,
        action: {
          label: t("toast.undo"),
          onClick: async () => {
            try {
              await rollbackHistory(deleted.revision_id);
              onDeleted?.();
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
/* Relationship active/inactive toggle                                 */
/* ------------------------------------------------------------------ */
function RelationshipToggle({
  source,
  target,
  isActive,
  onToggled,
}: {
  source: string;
  target: string;
  isActive: boolean;
  onToggled?: () => void;
}) {
  const { t } = useI18n();
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
      onToggled?.();
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

/* ------------------------------------------------------------------ */
/* Person tag search (type-ahead for tagging people in photos)          */
/* ------------------------------------------------------------------ */
function PersonTagSearch({
  persons,
  taggedIds,
  onToggle,
}: {
  persons: { id: string; fullname: string }[];
  taggedIds: string[];
  onToggle: (id: string) => void;
}) {
  const { t } = useI18n();
  const [query, setQuery] = useState("");

  const filtered = query.trim()
    ? persons.filter((p) => p.fullname.toLowerCase().includes(query.toLowerCase()))
    : [];

  const tagged = persons.filter((p) => taggedIds.includes(p.id));

  return (
    <div>
      <p className="text-xs font-medium text-gray-600 mb-1">{t("pic.tagPeople")}</p>

      {/* Selected tags */}
      {tagged.length > 0 && (
        <div className="flex flex-wrap gap-1 mb-2">
          {tagged.map((p) => (
            <button
              key={p.id}
              type="button"
              onClick={() => onToggle(p.id)}
              className="text-xs px-2 py-0.5 rounded-full bg-blue-100 border border-blue-400 text-blue-700 transition-colors hover:bg-blue-200"
            >
              ✓ {p.fullname} ✕
            </button>
          ))}
        </div>
      )}

      {/* Search input */}
      <input
        type="text"
        value={query}
        onChange={(e) => setQuery(e.target.value)}
        placeholder={t("tag.searchPlaceholder")}
        className="w-full border rounded px-2 py-1 text-sm text-gray-900 mb-1"
      />

      {/* Search results */}
      {filtered.length > 0 && (
        <div className="max-h-32 overflow-y-auto border rounded bg-white">
          {filtered.slice(0, 20).map((p) => {
            const isTagged = taggedIds.includes(p.id);
            return (
              <button
                key={p.id}
                type="button"
                onClick={() => { onToggle(p.id); setQuery(""); }}
                className={`w-full text-left px-2 py-1.5 text-sm hover:bg-blue-50 border-b last:border-b-0 transition-colors ${
                  isTagged ? "bg-blue-50 text-blue-700" : "text-gray-900"
                }`}
              >
                {isTagged ? "✓ " : ""}{p.fullname}
              </button>
            );
          })}
        </div>
      )}
      {query.trim() && filtered.length === 0 && (
        <p className="text-xs text-gray-400 py-1">{t("tag.noMatches")}</p>
      )}
    </div>
  );
}

/* ------------------------------------------------------------------ */
/* Person action bar (add/link/delete/story)                           */
/* ------------------------------------------------------------------ */
function PersonActionBar({
  personId,
  onAction,
  actions,
}: {
  personId: string;
  onAction: (action: string, nodeId: string) => void;
  actions: PersonActionDefinition[];
}) {
  const { t } = useI18n();
  return (
    <div className="hidden flex-wrap gap-1 md:flex">
      {actions.filter((item) => item.action !== "edit").map((item) => (
        <button
          key={item.action}
          onClick={() => onAction(item.action, personId)}
          className={`rounded border px-2 py-1 text-xs transition-colors ${
            item.destructive
              ? "border-red-200 text-red-500 hover:border-red-300 hover:bg-red-50"
              : "border-gray-200 text-gray-600 hover:border-gray-300 hover:bg-gray-50"
          }`}
          title={t(item.labelKey)}
          aria-label={t(item.labelKey)}
        >
          {item.icon}
        </button>
      ))}
    </div>
  );
}

/* ------------------------------------------------------------------ */
/* Dev mode raw JSON viewer                                            */
/* ------------------------------------------------------------------ */
function RawJsonSection({ label, data }: { label: string; data: unknown }) {
  const [expanded, setExpanded] = useState(false);
  return (
    <div className="border-t pt-2">
      <button
        onClick={() => setExpanded((v) => !v)}
        className="flex items-center gap-1 text-xs font-mono text-yellow-600 hover:text-yellow-800"
      >
        <span>{expanded ? "▼" : "▶"}</span>
        <span>🐛 {label}</span>
      </button>
      {expanded && (
        <pre className="mt-1 p-2 bg-gray-900 text-green-400 text-xs rounded overflow-x-auto max-h-64 overflow-y-auto">
          {JSON.stringify(data, null, 2)}
        </pre>
      )}
    </div>
  );
}

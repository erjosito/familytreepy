"use client";

import { useState, useEffect, useRef, useCallback } from "react";
import type { PersonNode, GraphEdge } from "@/lib/types";
import { updatePerson, uploadProfilePic, uploadPicture, tagPicture, removePicture } from "@/lib/api";

interface Props {
  person: PersonNode | null;
  relationships?: GraphEdge[];
  siblings?: string[];
  personList?: { id: string; fullname: string }[];
  devMode?: boolean;
  sasToken?: string;
  onClose?: () => void;
  onPersonUpdated?: () => void;
}

export default function DetailPanel({
  person,
  relationships = [],
  siblings = [],
  personList = [],
  devMode = false,
  sasToken = "",
  onClose,
  onPersonUpdated,
}: Props) {
  const [editing, setEditing] = useState(false);
  const [draft, setDraft] = useState<Record<string, unknown>>({});
  const [saving, setSaving] = useState(false);
  const [cropSrc, setCropSrc] = useState<string | null>(null);
  const [uploadingPic, setUploadingPic] = useState(false);

  // Reset edit mode when the selected person changes
  const personId = person?.id;
  useEffect(() => {
    setEditing(false);
    setDraft({});
    setCropSrc(null);
  }, [personId]);

  if (!person) {
    return (
      <div className="p-6 text-gray-400 text-center">
        <p className="text-lg">Select a person</p>
        <p className="text-sm mt-2">Click on a node in the graph to see details</p>
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
      birthdate: person.birthdate || "",
      birthplace: person.birthplace || "",
      isAlive: person.isAlive !== false,
      deathdate: person.deathdate || "",
    });
    setEditing(true);
  };

  const cancelEdit = () => {
    setEditing(false);
    setCropSrc(null);
  };

  const saveEdit = async () => {
    setSaving(true);
    try {
      await updatePerson(person.id, draft);
      setEditing(false);
      onPersonUpdated?.();
    } catch (err) {
      console.error("Save failed:", err);
    } finally {
      setSaving(false);
    }
  };

  const clearField = (field: string) => {
    setDraft((d) => ({ ...d, [field]: "" }));
  };

  // --- Profile picture upload / crop ---
  const handlePicSelected = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;
    const reader = new FileReader();
    reader.onload = () => setCropSrc(reader.result as string);
    reader.readAsDataURL(file);
    e.target.value = "";
  };

  return (
    <div className="p-4 space-y-4 overflow-y-auto h-full">
      {/* Header */}
      <div className="flex justify-between items-start">
        <h2 className="text-xl font-bold text-gray-900">{person.fullname || "Unknown"}</h2>
        <div className="flex gap-1">
          {!editing && (
            <button
              onClick={startEdit}
              className="text-xs px-2 py-1 rounded border border-blue-300 text-blue-600 hover:bg-blue-50"
            >
              ✏️ Edit
            </button>
          )}
          {onClose && (
            <button onClick={onClose} className="text-gray-400 hover:text-gray-600 ml-1">
              ✕
            </button>
          )}
        </div>
      </div>

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
            <label className="text-xs px-2 py-1 rounded border border-gray-300 text-gray-600 hover:bg-gray-50 cursor-pointer text-center">
              📷 Change
              <input type="file" accept="image/*" className="hidden" onChange={handlePicSelected} />
            </label>
          </div>
        )}
      </div>

      {/* Crop modal */}
      {cropSrc && (
        <CropUploader
          src={cropSrc}
          personId={person.id}
          onDone={() => {
            setCropSrc(null);
            onPersonUpdated?.();
          }}
          onCancel={() => setCropSrc(null)}
        />
      )}

      {/* Fields */}
      {editing ? (
        <EditFields draft={draft} setDraft={setDraft} onClear={clearField} />
      ) : (
        <ViewFields person={person} />
      )}

      {/* Save / Cancel */}
      {editing && (
        <div className="flex gap-2 pt-1">
          <button
            onClick={saveEdit}
            disabled={saving}
            className="px-4 py-1.5 bg-blue-600 text-white text-sm rounded hover:bg-blue-700 disabled:opacity-50"
          >
            {saving ? "Saving..." : "💾 Save"}
          </button>
          <button
            onClick={cancelEdit}
            className="px-4 py-1.5 border text-sm rounded hover:bg-gray-50"
          >
            Cancel
          </button>
        </div>
      )}

      {/* Relationships */}
      {relationships.length > 0 && (
        <div>
          <h3 className="font-semibold text-sm text-gray-600 uppercase tracking-wide mb-2">
            Relationships
          </h3>
          <ul className="space-y-1 text-sm">
            {relationships.map((rel, i) => (
              <li key={i} className={`flex items-center gap-2 ${!rel.is_active ? "opacity-50" : ""}`}>
                <span
                  className="inline-block w-2 h-2 rounded-full"
                  style={{ backgroundColor: rel.type === "isChildOf" ? "#ef4444" : "#3b82f6" }}
                />
                <span>{rel.type}</span>
                <span className="text-gray-500">→ {rel.source === person.id ? rel.target : rel.source}</span>
                {!rel.is_active && <span className="text-xs text-red-400">(inactive)</span>}
                {rel.start_date && <span className="text-xs text-gray-500">{rel.start_date}</span>}
                {rel.end_date && <span className="text-xs text-gray-500">– {rel.end_date}</span>}
              </li>
            ))}
          </ul>
        </div>
      )}

      {/* Siblings */}
      {siblings.length > 0 && (
        <div>
          <h3 className="font-semibold text-sm text-gray-600 uppercase tracking-wide mb-2">
            Siblings
          </h3>
          <ul className="text-sm text-gray-800">
            {siblings.map((s) => (
              <li key={s}>{s}</li>
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

      {/* Dev mode */}
      {devMode && <RawJsonSection label="Node JSON" data={person} />}
      {devMode && relationships.length > 0 && <RawJsonSection label="Relationships JSON" data={relationships} />}
    </div>
  );
}

/* ------------------------------------------------------------------ */
/* View-only fields                                                    */
/* ------------------------------------------------------------------ */
function ViewFields({ person }: { person: PersonNode }) {
  return (
    <div className="space-y-2 text-sm text-gray-900">
      {person.firstname && (
        <div>
          <span className="font-medium text-gray-600">First name:</span> {person.firstname}
        </div>
      )}
      {person.lastname && (
        <div>
          <span className="font-medium text-gray-600">Last name:</span> {person.lastname}
        </div>
      )}
      {person.birthdate && (
        <div>
          <span className="font-medium text-gray-600">Born:</span> {person.birthdate}
        </div>
      )}
      {person.birthplace && (
        <div>
          <span className="font-medium text-gray-600">Birthplace:</span> {person.birthplace}
        </div>
      )}
      <div>
        <span className="font-medium text-gray-600">Status:</span>{" "}
        {person.isAlive !== false ? "Living" : `Deceased${person.deathdate ? ` (${person.deathdate})` : ""}`}
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
  const set = (field: string, value: unknown) => setDraft((d) => ({ ...d, [field]: value }));

  return (
    <div className="space-y-3 text-sm text-gray-900">
      <Field label="First name" value={draft.firstname as string} onChange={(v) => set("firstname", v)} onClear={() => onClear("firstname")} />
      <Field label="Last name" value={draft.lastname as string} onChange={(v) => set("lastname", v)} onClear={() => onClear("lastname")} />
      <Field label="Birthdate" value={draft.birthdate as string} onChange={(v) => set("birthdate", v)} onClear={() => onClear("birthdate")} placeholder="e.g. 1990-01-31" />
      <Field label="Birthplace" value={draft.birthplace as string} onChange={(v) => set("birthplace", v)} onClear={() => onClear("birthplace")} />

      <div className="flex items-center gap-2">
        <label className="font-medium text-gray-600 text-sm">Alive:</label>
        <input
          type="checkbox"
          checked={draft.isAlive as boolean}
          onChange={(e) => set("isAlive", e.target.checked)}
          className="rounded"
        />
      </div>

      {!(draft.isAlive as boolean) && (
        <Field label="Death date" value={draft.deathdate as string} onChange={(v) => set("deathdate", v)} onClear={() => onClear("deathdate")} placeholder="e.g. 2020-12-15" />
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
}: {
  label: string;
  value: string;
  onChange: (v: string) => void;
  onClear: () => void;
  type?: string;
  placeholder?: string;
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
            title="Clear"
          >
            ✕
          </button>
        )}
      </div>
    </div>
  );
}

/* ------------------------------------------------------------------ */
/* Profile picture crop & upload                                       */
/* ------------------------------------------------------------------ */
function CropUploader({
  src,
  personId,
  onDone,
  onCancel,
}: {
  src: string;
  personId: string;
  onDone: () => void;
  onCancel: () => void;
}) {
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const imgRef = useRef<HTMLImageElement | null>(null);
  const [offset, setOffset] = useState({ x: 0, y: 0 });
  const [zoom, setZoom] = useState(1);
  const [uploading, setUploading] = useState(false);
  const [loaded, setLoaded] = useState(false);
  const cropSize = 200;

  const draw = useCallback(() => {
    const ctx = canvasRef.current?.getContext("2d");
    const img = imgRef.current;
    if (!ctx || !img) return;
    ctx.clearRect(0, 0, cropSize, cropSize);
    ctx.save();
    ctx.beginPath();
    ctx.arc(cropSize / 2, cropSize / 2, cropSize / 2, 0, Math.PI * 2);
    ctx.clip();
    const scale = zoom;
    const w = img.naturalWidth * scale;
    const h = img.naturalHeight * scale;
    ctx.drawImage(img, offset.x + (cropSize - w) / 2, offset.y + (cropSize - h) / 2, w, h);
    ctx.restore();
    // circle border
    ctx.beginPath();
    ctx.arc(cropSize / 2, cropSize / 2, cropSize / 2 - 1, 0, Math.PI * 2);
    ctx.strokeStyle = "#3b82f6";
    ctx.lineWidth = 2;
    ctx.stroke();
  }, [offset, zoom, cropSize]);

  const handleLoad = () => {
    const img = imgRef.current!;
    // Auto-zoom to fit the crop area
    const fitScale = cropSize / Math.min(img.naturalWidth, img.naturalHeight);
    setZoom(fitScale);
    setLoaded(true);
  };

  // Redraw whenever offset/zoom changes
  if (loaded) {
    // Use requestAnimationFrame to draw after state updates
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
    const ctx = canvasRef.current?.getContext("2d");
    if (!ctx) return;
    // Render final 400×400 version
    const out = document.createElement("canvas");
    out.width = 400;
    out.height = 400;
    const outCtx = out.getContext("2d")!;
    outCtx.drawImage(canvasRef.current!, 0, 0, cropSize, cropSize, 0, 0, 400, 400);
    out.toBlob(async (blob) => {
      if (!blob) return;
      setUploading(true);
      try {
        await uploadProfilePic(personId, blob, "profile.jpg");
        onDone();
      } catch (err) {
        console.error("Upload failed:", err);
      } finally {
        setUploading(false);
      }
    }, "image/jpeg", 0.9);
  };

  return (
    <div className="border rounded-lg p-3 bg-gray-50 space-y-3">
      <p className="text-xs text-gray-500 font-medium">Drag to position · Scroll to zoom</p>
      {/* Hidden img for loading */}
      <img
        ref={imgRef}
        src={src}
        alt=""
        className="hidden"
        onLoad={handleLoad}
      />
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
          type="range"
          min={0.1}
          max={3}
          step={0.01}
          value={zoom}
          onChange={(e) => setZoom(Number(e.target.value))}
          className="flex-1"
        />
        <span>+</span>
      </div>
      <div className="flex gap-2">
        <button
          onClick={handleUpload}
          disabled={uploading}
          className="px-3 py-1 bg-blue-600 text-white text-xs rounded hover:bg-blue-700 disabled:opacity-50"
        >
          {uploading ? "Uploading..." : "✓ Upload"}
        </button>
        <button
          onClick={onCancel}
          className="px-3 py-1 border text-xs rounded hover:bg-gray-50"
        >
          Cancel
        </button>
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
  const [uploading, setUploading] = useState(false);
  const [preview, setPreview] = useState<{ file: File; dataUrl: string } | null>(null);
  const [taggedIds, setTaggedIds] = useState<string[]>([]);
  const [removing, setRemoving] = useState<string | null>(null);

  const pics = person.pictures && person.pictures.length > 0 ? person.pictures : [];

  const handleFileSelected = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;
    const reader = new FileReader();
    reader.onload = () => setPreview({ file, dataUrl: reader.result as string });
    reader.readAsDataURL(file);
    e.target.value = "";
    setTaggedIds([]);
  };

  const handleUpload = async () => {
    if (!preview) return;
    setUploading(true);
    try {
      const result = await uploadPicture(person.id, preview.file, preview.file.name);
      // Tag other people
      if (taggedIds.length > 0) {
        await tagPicture(person.id, result.url, taggedIds);
      }
      setPreview(null);
      setTaggedIds([]);
      onUpdated?.();
    } catch (err) {
      console.error("Upload failed:", err);
    } finally {
      setUploading(false);
    }
  };

  const handleRemove = async (url: string) => {
    setRemoving(url);
    try {
      await removePicture(person.id, url);
      onUpdated?.();
    } catch (err) {
      console.error("Remove failed:", err);
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
          Pictures {pics.length > 0 && <span className="text-gray-400 normal-case">({pics.length})</span>}
        </h3>
        {!preview && (
          <label className="text-xs px-2 py-1 rounded border border-gray-300 text-gray-700 hover:bg-gray-50 cursor-pointer">
            + Add photo
            <input type="file" accept="image/*" className="hidden" onChange={handleFileSelected} />
          </label>
        )}
      </div>

      {/* Upload preview + tagging */}
      {preview && (
        <div className="border rounded-lg p-3 bg-gray-50 space-y-3 mb-3">
          <img src={preview.dataUrl} alt="Preview" className="rounded border max-h-40 w-full object-contain" />

          {/* Tag people */}
          {taggable.length > 0 && (
            <div>
              <p className="text-xs font-medium text-gray-600 mb-1">Tag people in this photo:</p>
              <div className="flex flex-wrap gap-1 max-h-24 overflow-y-auto">
                {taggable.map((p) => (
                  <button
                    key={p.id}
                    type="button"
                    onClick={() => toggleTag(p.id)}
                    className={`text-xs px-2 py-0.5 rounded-full border transition-colors ${
                      taggedIds.includes(p.id)
                        ? "bg-blue-100 border-blue-400 text-blue-700"
                        : "border-gray-300 text-gray-600 hover:bg-gray-100"
                    }`}
                  >
                    {taggedIds.includes(p.id) ? "✓ " : ""}{p.fullname}
                  </button>
                ))}
              </div>
            </div>
          )}

          <div className="flex gap-2">
            <button
              onClick={handleUpload}
              disabled={uploading}
              className="px-3 py-1 bg-blue-600 text-white text-xs rounded hover:bg-blue-700 disabled:opacity-50"
            >
              {uploading ? "Uploading..." : "⬆ Upload"}
            </button>
            <button
              onClick={() => { setPreview(null); setTaggedIds([]); }}
              className="px-3 py-1 border text-xs rounded hover:bg-gray-50"
            >
              Cancel
            </button>
          </div>
        </div>
      )}

      {/* Gallery grid */}
      {pics.length > 0 && (
        <div className="grid grid-cols-2 gap-2">
          {pics.map((url, i) => (
            <div key={i} className="relative group">
              <img
                src={withSas(url) || url}
                alt=""
                className="rounded border object-cover h-24 w-full"
              />
              <button
                onClick={() => handleRemove(url)}
                disabled={removing === url}
                className="absolute top-1 right-1 bg-black/50 text-white text-xs rounded-full w-5 h-5 flex items-center justify-center opacity-0 group-hover:opacity-100 transition-opacity hover:bg-red-600"
                title="Remove from this person"
              >
                ✕
              </button>
            </div>
          ))}
        </div>
      )}
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

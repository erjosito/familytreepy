"use client";

import { useState, useEffect } from "react";
import { listPersons, listRenderers, generateImage } from "@/lib/api";
import type { RendererInfo } from "@/lib/types";

export default function ImagePage() {
  const [personList, setPersonList] = useState<{ id: string; fullname: string }[]>([]);
  const [renderers, setRenderers] = useState<RendererInfo[]>([]);
  const [rootId, setRootId] = useState("");
  const [degree, setDegree] = useState(3);
  const [renderer, setRenderer] = useState("");
  const [loading, setLoading] = useState(false);
  const [imageUrl, setImageUrl] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    listPersons().then(setPersonList).catch(console.error);
    listRenderers().then((r) => {
      setRenderers(r);
      if (r.length > 0) setRenderer(r[0].name);
    }).catch(console.error);
  }, []);

  const handleGenerate = async () => {
    if (!rootId || !renderer) return;
    setLoading(true);
    setError(null);
    setImageUrl(null);
    try {
      const blob = await generateImage(rootId, degree, renderer);
      const url = URL.createObjectURL(blob);
      setImageUrl(url);
    } catch (err) {
      setError(String(err));
    } finally {
      setLoading(false);
    }
  };

  const handleDownload = () => {
    if (!imageUrl) return;
    const a = document.createElement("a");
    a.href = imageUrl;
    a.download = "familytree.png";
    a.click();
  };

  return (
    <div className="min-h-screen bg-gray-50">

      <div className="max-w-4xl mx-auto p-6 space-y-6">
        {/* Controls */}
        <div className="bg-white rounded-lg border p-6 space-y-4">
          <h2 className="text-lg font-semibold text-gray-900">Generate Family Tree Image</h2>

          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">Center Person</label>
              <select
                className="w-full border rounded px-3 py-2 text-sm text-gray-900"
                value={rootId}
                onChange={(e) => setRootId(e.target.value)}
              >
                <option value="">Select a person...</option>
                {personList.map((p) => (
                  <option key={p.id} value={p.id}>
                    {p.fullname}
                  </option>
                ))}
              </select>
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                Degree: {degree}
              </label>
              <input
                type="range"
                min={1}
                max={5}
                value={degree}
                onChange={(e) => setDegree(Number(e.target.value))}
                className="w-full mt-2"
              />
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">Layout</label>
              <select
                className="w-full border rounded px-3 py-2 text-sm text-gray-900"
                value={renderer}
                onChange={(e) => setRenderer(e.target.value)}
              >
                {renderers.map((r) => (
                  <option key={r.name} value={r.name}>
                    {r.description}
                  </option>
                ))}
              </select>
            </div>
          </div>

          <button
            className="px-6 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50 transition-colors"
            onClick={handleGenerate}
            disabled={!rootId || !renderer || loading}
          >
            {loading ? "Generating..." : "Generate Image"}
          </button>
        </div>

        {/* Error */}
        {error && (
          <div className="bg-red-50 text-red-700 rounded-lg border border-red-200 p-4 text-sm">
            {error}
          </div>
        )}

        {/* Preview */}
        {imageUrl && (
          <div className="bg-white rounded-lg border p-6 space-y-4">
            <div className="flex items-center justify-between">
              <h3 className="font-semibold text-gray-900">Preview</h3>
              <button
                className="px-4 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700 text-sm transition-colors"
                onClick={handleDownload}
              >
                ⬇ Download PNG
              </button>
            </div>
            <img src={imageUrl} alt="Family tree" className="w-full rounded border" />
          </div>
        )}
      </div>
    </div>
  );
}

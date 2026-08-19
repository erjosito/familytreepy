"use client";

import { useState, useEffect } from "react";
import { listPersons, listRenderers, generateImage } from "@/lib/api";
import type { RendererInfo } from "@/lib/types";
import { useI18n } from "@/lib/i18n";

const COLOR_SCHEMES = [
  { value: "sepia", label: "Sepia" },
  { value: "blue", label: "Blue" },
  { value: "green", label: "Green" },
  { value: "grayscale", label: "Grayscale" },
];

export default function ImagePage() {
  const { t } = useI18n();
  const [personList, setPersonList] = useState<{ id: string; fullname: string }[]>([]);
  const [renderers, setRenderers] = useState<RendererInfo[]>([]);
  const [rootId, setRootId] = useState("");
  const [degree, setDegree] = useState(3);
  const [renderer, setRenderer] = useState("");
  const [loading, setLoading] = useState(false);
  const [imageUrl, setImageUrl] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  // Customization options
  const [colorScheme, setColorScheme] = useState("sepia");
  const [canvasWidth, setCanvasWidth] = useState(2000);
  const [canvasHeight, setCanvasHeight] = useState(1500);
  const [fontScale, setFontScale] = useState(1.0);
  const [lineWidth, setLineWidth] = useState(2);
  const [showAdvanced, setShowAdvanced] = useState(false);

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
      const blob = await generateImage(rootId, degree, renderer, {
        colorScheme,
        canvasWidth,
        canvasHeight,
        fontScale,
        lineWidth,
      });
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
      <div className="max-w-4xl mx-auto p-3 sm:p-6 space-y-6">
        {/* Controls */}
        <div className="bg-white rounded-lg border p-4 sm:p-6 space-y-4">
          <h2 className="text-lg font-semibold text-gray-900">{t("image.title")}</h2>

          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">{t("image.centerPerson")}</label>
              <select
                className="w-full border rounded px-3 py-2 text-sm text-gray-900"
                value={rootId}
                onChange={(e) => setRootId(e.target.value)}
              >
                <option value="">{t("image.selectPerson")}</option>
                {personList.map((p) => (
                  <option key={p.id} value={p.id}>{p.fullname}</option>
                ))}
              </select>
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">
                {t("image.degree")} {degree}
              </label>
              <input
                type="range" min={1} max={5} value={degree}
                onChange={(e) => setDegree(Number(e.target.value))}
                className="w-full mt-2"
              />
            </div>

            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">{t("image.layout")}</label>
              <select
                className="w-full border rounded px-3 py-2 text-sm text-gray-900"
                value={renderer}
                onChange={(e) => setRenderer(e.target.value)}
              >
                {renderers.map((r) => (
                  <option key={r.name} value={r.name}>{r.description}</option>
                ))}
              </select>
            </div>
          </div>

          {/* Color scheme */}
          <div>
            <label className="block text-sm font-medium text-gray-700 mb-2">{t("image.colorScheme")}</label>
            <div className="flex flex-wrap gap-2">
              {COLOR_SCHEMES.map((cs) => (
                <button
                  key={cs.value}
                  onClick={() => setColorScheme(cs.value)}
                  className={`px-3 py-1.5 text-sm rounded-lg border transition-colors ${
                    colorScheme === cs.value
                      ? "bg-blue-600 text-white border-blue-600"
                      : "bg-white text-gray-700 border-gray-300 hover:bg-gray-50"
                  }`}
                >
                  {cs.label}
                </button>
              ))}
            </div>
          </div>

          {/* Advanced options toggle */}
          <button
            onClick={() => setShowAdvanced(!showAdvanced)}
            className="text-sm text-blue-600 hover:underline"
          >
            {showAdvanced ? "▼" : "▶"} {t("image.advanced")}
          </button>

          {showAdvanced && (
            <div className="grid grid-cols-1 md:grid-cols-2 gap-4 border-t pt-4">
              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  {t("image.canvasSize")} ({canvasWidth} × {canvasHeight})
                </label>
                <div className="flex gap-2">
                  <select
                    className="flex-1 border rounded px-2 py-1.5 text-sm text-gray-900"
                    value={`${canvasWidth}x${canvasHeight}`}
                    onChange={(e) => {
                      const [w, h] = e.target.value.split("x").map(Number);
                      setCanvasWidth(w);
                      setCanvasHeight(h);
                    }}
                  >
                    <option value="1200x900">1200 × 900</option>
                    <option value="1600x1200">1600 × 1200</option>
                    <option value="2000x1500">2000 × 1500</option>
                    <option value="2400x1800">2400 × 1800</option>
                    <option value="3000x2000">3000 × 2000</option>
                    <option value="4000x3000">4000 × 3000</option>
                  </select>
                </div>
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  {t("image.fontScale")} ({fontScale.toFixed(1)}×)
                </label>
                <input
                  type="range" min={0.5} max={2.0} step={0.1} value={fontScale}
                  onChange={(e) => setFontScale(Number(e.target.value))}
                  className="w-full"
                />
              </div>

              <div>
                <label className="block text-sm font-medium text-gray-700 mb-1">
                  {t("image.lineWidth")} ({lineWidth}px)
                </label>
                <input
                  type="range" min={1} max={5} step={1} value={lineWidth}
                  onChange={(e) => setLineWidth(Number(e.target.value))}
                  className="w-full"
                />
              </div>
            </div>
          )}

          <button
            className="px-6 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50 transition-colors"
            onClick={handleGenerate}
            disabled={!rootId || !renderer || loading}
          >
            {loading ? t("image.generating") : t("image.generate")}
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
          <div className="bg-white rounded-lg border p-4 sm:p-6 space-y-4">
            <div className="flex items-center justify-between">
              <h3 className="font-semibold text-gray-900">{t("image.preview")}</h3>
              <button
                className="px-4 py-2 bg-green-600 text-white rounded-lg hover:bg-green-700 text-sm transition-colors"
                onClick={handleDownload}
              >
                {t("image.download")}
              </button>
            </div>
            <img src={imageUrl} alt="Family tree" className="w-full rounded border" />
          </div>
        )}
      </div>
    </div>
  );
}

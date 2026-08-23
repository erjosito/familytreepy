"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import { PHOTO_ACCEPT, photoErrorKey, preparePhoto, type PreparedPhoto } from "@/lib/photoProcessing";
import { uploadPhotoWithProgress } from "@/lib/photoUpload";
import { useI18n } from "@/lib/i18n";
import { useToast } from "@/components/ToastProvider";

export function PhotoPicker({
  onSelected,
  disabled = false,
  compact = false,
}: {
  onSelected: (photo: PreparedPhoto) => void;
  disabled?: boolean;
  compact?: boolean;
}) {
  const { t } = useI18n();
  const cameraRef = useRef<HTMLInputElement>(null);
  const chooserRef = useRef<HTMLInputElement>(null);
  const [processing, setProcessing] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const select = async (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    event.target.value = "";
    if (!file) return;
    setProcessing(true);
    setError(null);
    try {
      onSelected(await preparePhoto(file));
    } catch (cause) {
      setError(t(photoErrorKey(cause)));
    } finally {
      setProcessing(false);
    }
  };

  const buttonClass = compact
    ? "text-xs px-2 py-1 rounded border border-gray-300 text-gray-700 hover:bg-gray-50 disabled:opacity-50"
    : "text-xs px-3 py-1.5 rounded border border-gray-300 text-gray-700 hover:bg-gray-50 disabled:opacity-50";

  return (
    <div className="space-y-1">
      <div className="flex flex-wrap gap-1">
        <button type="button" className={buttonClass} disabled={disabled || processing} onClick={() => cameraRef.current?.click()}>
          {processing ? t("pic.processing") : t("pic.takePhoto")}
        </button>
        <button type="button" className={buttonClass} disabled={disabled || processing} onClick={() => chooserRef.current?.click()}>
          {t("pic.choosePhoto")}
        </button>
      </div>
      <input ref={cameraRef} type="file" accept="image/*" capture="environment" className="hidden" onChange={select} />
      <input ref={chooserRef} type="file" accept={PHOTO_ACCEPT} className="hidden" onChange={select} />
      <p className="max-w-xs text-[11px] text-gray-500">{t("pic.heicHint")}</p>
      {error && <p role="alert" className="max-w-xs text-xs text-red-600">{error}</p>}
    </div>
  );
}

export function UploadProgress({
  progress,
  error,
  onCancel,
  onRetry,
}: {
  progress: number | null;
  error: boolean;
  onCancel: () => void;
  onRetry: () => void;
}) {
  const { t } = useI18n();
  if (progress === null && !error) return null;
  return (
    <div className="space-y-1" aria-live="polite">
      {progress !== null && (
        <>
          <div className="h-2 overflow-hidden rounded bg-gray-200">
            <div className="h-full bg-blue-600 transition-[width]" style={{ width: `${progress}%` }} />
          </div>
          <div className="flex justify-between text-xs text-gray-600">
            <span>
              {progress < 100
                ? t("pic.uploadProgress").replace("{percent}", String(progress))
                : t("pic.serverProcessing")}
            </span>
            {progress < 100 && (
              <button type="button" className="text-red-600 hover:underline" onClick={onCancel}>
                {t("pic.cancelUpload")}
              </button>
            )}
          </div>
        </>
      )}
      {error && (
        <button type="button" className="text-xs font-medium text-blue-700 hover:underline" onClick={onRetry}>
          {t("pic.retryUpload")}
        </button>
      )}
    </div>
  );
}

export function ProfilePhotoCrop({
  photo,
  personId,
  modal = false,
  onDone,
  onCancel,
}: {
  photo: PreparedPhoto;
  personId: string;
  modal?: boolean;
  onDone: () => void;
  onCancel: () => void;
}) {
  const { t } = useI18n();
  const toast = useToast();
  const canvasRef = useRef<HTMLCanvasElement>(null);
  const imgRef = useRef<HTMLImageElement | null>(null);
  const abortRef = useRef<AbortController | null>(null);
  const dragRef = useRef<{ x: number; y: number; ox: number; oy: number } | null>(null);
  const [offset, setOffset] = useState({ x: 0, y: 0 });
  const [zoom, setZoom] = useState(1);
  const [loaded, setLoaded] = useState(false);
  const [progress, setProgress] = useState<number | null>(null);
  const [failed, setFailed] = useState(false);
  const cropSize = modal ? 280 : 200;

  const draw = useCallback(() => {
    const context = canvasRef.current?.getContext("2d");
    const image = imgRef.current;
    if (!context || !image) return;
    context.clearRect(0, 0, cropSize, cropSize);
    context.save();
    context.beginPath();
    context.arc(cropSize / 2, cropSize / 2, cropSize / 2, 0, Math.PI * 2);
    context.clip();
    const width = image.naturalWidth * zoom;
    const height = image.naturalHeight * zoom;
    context.drawImage(image, offset.x + (cropSize - width) / 2, offset.y + (cropSize - height) / 2, width, height);
    context.restore();
    context.beginPath();
    context.arc(cropSize / 2, cropSize / 2, cropSize / 2 - 1, 0, Math.PI * 2);
    context.strokeStyle = "#3b82f6";
    context.lineWidth = 2;
    context.stroke();
  }, [cropSize, offset, zoom]);

  useEffect(() => {
    if (loaded) requestAnimationFrame(draw);
  }, [draw, loaded]);

  useEffect(() => () => {
    abortRef.current?.abort();
    URL.revokeObjectURL(photo.previewUrl);
  }, [photo.previewUrl]);

  const upload = async () => {
    const source = canvasRef.current;
    if (!source) return;
    const output = document.createElement("canvas");
    output.width = 400;
    output.height = 400;
    output.getContext("2d")?.drawImage(source, 0, 0, cropSize, cropSize, 0, 0, 400, 400);
    const blob = await new Promise<Blob | null>((resolve) => output.toBlob(resolve, "image/jpeg", 0.88));
    if (!blob) return;
    const controller = new AbortController();
    abortRef.current = controller;
    setFailed(false);
    setProgress(0);
    try {
      await uploadPhotoWithProgress(
        personId,
        "profilepic",
        blob,
        "profile.jpg",
        photo.uploadId,
        setProgress,
        controller.signal,
      );
      setProgress(null);
      toast.success(t("toast.photoUpdated"));
      onDone();
    } catch (error) {
      setProgress(null);
      if (!(error instanceof DOMException && error.name === "AbortError")) {
        setFailed(true);
        toast.error(t("toast.photoUploadFailed"));
      }
    }
  };

  const content = (
    <div className={`${modal ? "bg-white rounded-xl p-6 shadow-2xl max-w-sm" : "border rounded-lg p-3 bg-gray-50"} space-y-3`} onClick={(event) => event.stopPropagation()}>
      <p className="text-xs text-gray-600 font-medium">{t("pic.dragHint")}</p>
      <img
        ref={imgRef}
        src={photo.previewUrl}
        alt=""
        className="hidden"
        onLoad={() => {
          const image = imgRef.current;
          if (!image) return;
          setZoom(cropSize / Math.min(image.naturalWidth, image.naturalHeight));
          setLoaded(true);
        }}
      />
      <div className="flex justify-center">
        <canvas
          ref={canvasRef}
          width={cropSize}
          height={cropSize}
          className="rounded-full cursor-move border-2 border-gray-300 touch-none"
          onPointerDown={(event) => {
            event.currentTarget.setPointerCapture(event.pointerId);
            dragRef.current = { x: event.clientX, y: event.clientY, ox: offset.x, oy: offset.y };
          }}
          onPointerMove={(event) => {
            if (!dragRef.current) return;
            setOffset({
              x: dragRef.current.ox + event.clientX - dragRef.current.x,
              y: dragRef.current.oy + event.clientY - dragRef.current.y,
            });
          }}
          onPointerUp={() => { dragRef.current = null; }}
          onWheel={(event) => {
            event.preventDefault();
            setZoom((value) => Math.max(0.1, value + (event.deltaY < 0 ? 0.05 : -0.05)));
          }}
        />
      </div>
      <input type="range" min={0.1} max={3} step={0.01} value={zoom} onChange={(event) => setZoom(Number(event.target.value))} className="w-full" aria-label={t("pic.zoom")} />
      <UploadProgress progress={progress} error={failed} onCancel={() => abortRef.current?.abort()} onRetry={upload} />
      <div className="flex gap-2">
        <button type="button" onClick={upload} disabled={progress !== null} className="px-3 py-1 bg-blue-600 text-white text-xs rounded hover:bg-blue-700 disabled:opacity-50">
          {t("pic.uploadBtn")}
        </button>
        <button type="button" onClick={onCancel} disabled={progress !== null} className="px-3 py-1 border text-xs rounded hover:bg-gray-50 disabled:opacity-50">
          {t("pic.cancel")}
        </button>
      </div>
    </div>
  );

  return modal ? <div className="fixed inset-0 z-[100] bg-black/60 flex items-center justify-center" onClick={onCancel}>{content}</div> : content;
}

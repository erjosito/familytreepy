import type { TranslationKey } from "@/lib/i18n";

export const PHOTO_ACCEPT = "image/*,.heic,.heif,image/heic,image/heif";
export const PHOTO_MAX_SOURCE_BYTES = 50 * 1024 * 1024;
export const PHOTO_MAX_OUTPUT_BYTES = 2 * 1024 * 1024;
export const PHOTO_MAX_DIMENSION = 2048;

export interface PreparedPhoto {
  blob: Blob;
  file: File;
  previewUrl: string;
  uploadId: string;
}

function isHeic(file: File): boolean {
  return /image\/hei[cf]/i.test(file.type) || /\.hei[cf]$/i.test(file.name);
}

async function convertHeic(file: File): Promise<Blob> {
  try {
    const { default: heic2any } = await import("heic2any");
    const converted = await heic2any({ blob: file, toType: "image/jpeg", quality: 0.92 });
    return Array.isArray(converted) ? converted[0] : converted;
  } catch {
    throw new Error("heic");
  }
}

async function decodeImage(blob: Blob): Promise<ImageBitmap> {
  try {
    return await createImageBitmap(blob, { imageOrientation: "from-image" });
  } catch {
    throw new Error("type");
  }
}

function canvasBlob(canvas: HTMLCanvasElement, quality: number): Promise<Blob> {
  return new Promise((resolve, reject) => {
    canvas.toBlob(
      (blob) => blob ? resolve(blob) : reject(new Error("processing")),
      "image/jpeg",
      quality,
    );
  });
}

export async function preparePhoto(
  selected: File,
  maxDimension = PHOTO_MAX_DIMENSION,
  maxBytes = PHOTO_MAX_OUTPUT_BYTES,
): Promise<PreparedPhoto> {
  if (selected.size > PHOTO_MAX_SOURCE_BYTES) throw new Error("size");
  if (!selected.type.startsWith("image/") && !isHeic(selected)) throw new Error("type");

  const source = isHeic(selected) ? await convertHeic(selected) : selected;
  const bitmap = await decodeImage(source);
  const scale = Math.min(1, maxDimension / Math.max(bitmap.width, bitmap.height));
  const canvas = document.createElement("canvas");
  canvas.width = Math.max(1, Math.round(bitmap.width * scale));
  canvas.height = Math.max(1, Math.round(bitmap.height * scale));
  const context = canvas.getContext("2d");
  if (!context) {
    bitmap.close();
    throw new Error("processing");
  }
  context.drawImage(bitmap, 0, 0, canvas.width, canvas.height);
  bitmap.close();

  let quality = 0.9;
  let blob = await canvasBlob(canvas, quality);
  while (blob.size > maxBytes && quality > 0.45) {
    quality -= 0.1;
    blob = await canvasBlob(canvas, quality);
  }
  if (blob.size > maxBytes) throw new Error("size");

  const stem = selected.name.replace(/\.[^.]+$/, "") || "photo";
  const file = new File([blob], `${stem}.jpg`, {
    type: "image/jpeg",
    lastModified: Date.now(),
  });
  return {
    blob,
    file,
    previewUrl: URL.createObjectURL(blob),
    uploadId: crypto.randomUUID(),
  };
}

export function photoErrorKey(error: unknown): TranslationKey {
  if (error instanceof Error && error.message === "heic") return "pic.errorHeic";
  if (error instanceof Error && error.message === "size") return "pic.errorSize";
  return "pic.errorType";
}

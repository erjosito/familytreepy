const API_BASE = process.env.NEXT_PUBLIC_API_URL ?? "";

export type PhotoUploadKind = "profilepic" | "pictures";

export function uploadPhotoWithProgress(
  personId: string,
  kind: PhotoUploadKind,
  blob: Blob,
  filename: string,
  uploadId: string,
  onProgress: (percent: number) => void,
  signal?: AbortSignal,
): Promise<{ url: string }> {
  return new Promise((resolve, reject) => {
    const request = new XMLHttpRequest();
    request.open("POST", `${API_BASE}/api/persons/${personId}/${kind}`);
    request.withCredentials = true;
    request.responseType = "json";
    request.setRequestHeader("X-Upload-Id", uploadId);
    request.upload.onprogress = (event) => {
      if (event.lengthComputable) onProgress(Math.round((event.loaded / event.total) * 100));
    };
    request.onload = () => {
      if (request.status >= 200 && request.status < 300) {
        resolve(request.response as { url: string });
      } else {
        reject(new Error(`Upload failed: ${request.status}`));
      }
    };
    request.onerror = () => reject(new Error("Upload failed"));
    request.onabort = () => reject(new DOMException("Upload cancelled", "AbortError"));
    signal?.addEventListener("abort", () => request.abort(), { once: true });

    const form = new FormData();
    form.append("file", blob, filename);
    request.send(form);
  });
}

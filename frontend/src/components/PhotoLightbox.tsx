"use client";

import { useEffect, useRef } from "react";
import Link from "next/link";
import { useI18n } from "@/lib/i18n";

export interface LightboxPhoto {
  url: string;
  people?: { id: string; fullname: string }[];
}

export default function PhotoLightbox({
  photo,
  onClose,
}: {
  photo: LightboxPhoto;
  onClose: () => void;
}) {
  const { t } = useI18n();
  const closeButtonRef = useRef<HTMLButtonElement>(null);
  const onCloseRef = useRef(onClose);

  useEffect(() => {
    onCloseRef.current = onClose;
  }, [onClose]);

  useEffect(() => {
    const previouslyFocused = document.activeElement instanceof HTMLElement
      ? document.activeElement
      : null;
    const previousOverflow = document.body.style.overflow;
    document.body.style.overflow = "hidden";
    closeButtonRef.current?.focus();

    const handleKeyDown = (event: KeyboardEvent) => {
      if (event.key === "Escape") onCloseRef.current();
    };
    document.addEventListener("keydown", handleKeyDown);
    return () => {
      document.removeEventListener("keydown", handleKeyDown);
      document.body.style.overflow = previousOverflow;
      previouslyFocused?.focus();
    };
  }, []);

  return (
    <div
      role="dialog"
      aria-modal="true"
      aria-label={t("person.photoViewer")}
      className="fixed inset-0 z-[300] flex cursor-pointer items-center justify-center bg-black/85 p-4 pt-20"
      onMouseDown={(event) => {
        if (event.target === event.currentTarget) onClose();
      }}
    >
      <button
        ref={closeButtonRef}
        type="button"
        className="fixed right-4 top-16 z-[310] flex h-11 w-11 items-center justify-center rounded-full border border-white/70 bg-black/80 text-2xl text-white shadow-lg hover:bg-black"
        aria-label={t("pic.closeViewer")}
        title={t("pic.closeViewer")}
        onClick={onClose}
      >
        ✕
      </button>
      <div
        className="max-h-full max-w-[94vw] cursor-default overflow-y-auto rounded-lg bg-gray-950 shadow-2xl"
        onMouseDown={(event) => event.stopPropagation()}
      >
        <img
          src={photo.url}
          alt={t("pic.photoAlt")}
          className="max-h-[calc(100vh-14rem)] w-full object-contain"
        />
        <div className="border-t border-white/20 bg-black/80 p-3 text-white">
          <h2 className="text-sm font-semibold">
            {t("pic.peopleInPhoto")}
          </h2>
          {photo.people === undefined ? (
            <p className="mt-2 text-sm text-gray-300">{t("pic.loadingPeople")}</p>
          ) : photo.people.length > 0 ? (
            <div className="mt-2 flex flex-wrap gap-2">
              {photo.people.map((person) => (
                <Link
                  key={person.id}
                  href={`/person/?id=${person.id}`}
                  onClick={onClose}
                  className="rounded-full border border-blue-300 bg-blue-950 px-3 py-1 text-sm text-blue-100 hover:bg-blue-900"
                >
                  {person.fullname}
                </Link>
              ))}
            </div>
          ) : (
            <p className="mt-2 text-sm text-gray-300">{t("pic.noPeopleTagged")}</p>
          )}
        </div>
      </div>
    </div>
  );
}

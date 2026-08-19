"use client";

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useI18n } from "@/lib/i18n";

export interface SearchablePerson {
  id: string;
  fullname: string;
  alias?: string;
}

interface Props {
  persons: SearchablePerson[];
  onSelect: (person: SearchablePerson) => void;
}

const RECENT_STORAGE_KEY = "familytree.recentPeople";
const MAX_RESULTS = 20;
const MAX_RECENT = 5;

function normalized(value: string | undefined) {
  return (value || "").trim().toLocaleLowerCase();
}

function matchScore(person: SearchablePerson, query: string) {
  const name = normalized(person.fullname);
  const alias = normalized(person.alias);
  if (name === query || alias === query) return 0;
  if (name.startsWith(query)) return 1;
  if (alias.startsWith(query)) return 2;
  if (name.split(/\s+/).some((part) => part.startsWith(query))) return 3;
  if (name.includes(query)) return 4;
  if (alias.includes(query)) return 5;
  return Number.POSITIVE_INFINITY;
}

export default function PersonSearch({ persons, onSelect }: Props) {
  const { t } = useI18n();
  const inputRef = useRef<HTMLInputElement>(null);
  const containerRef = useRef<HTMLDivElement>(null);
  const recentLoadedRef = useRef(false);
  const [query, setQuery] = useState("");
  const [open, setOpen] = useState(false);
  const [activeIndex, setActiveIndex] = useState(0);
  const [recentIds, setRecentIds] = useState<string[]>([]);

  const loadRecent = useCallback(() => {
    if (recentLoadedRef.current) return;
    recentLoadedRef.current = true;
    try {
      const saved = JSON.parse(localStorage.getItem(RECENT_STORAGE_KEY) || "[]");
      if (Array.isArray(saved)) {
        setRecentIds(saved.filter((id): id is string => typeof id === "string").slice(0, MAX_RECENT));
      }
    } catch (error) {
      console.warn("Unable to load recent people", error);
    }
  }, []);

  useEffect(() => {
    const focusSearch = (event: KeyboardEvent) => {
      if (event.key !== "/" || event.ctrlKey || event.metaKey || event.altKey) return;
      const target = event.target as HTMLElement | null;
      if (
        target?.matches("input, textarea, select") ||
        target?.isContentEditable
      ) {
        return;
      }
      event.preventDefault();
      loadRecent();
      inputRef.current?.focus();
      setOpen(true);
    };
    window.addEventListener("keydown", focusSearch);
    return () => window.removeEventListener("keydown", focusSearch);
  }, [loadRecent]);

  useEffect(() => {
    const closeOnOutsideClick = (event: PointerEvent) => {
      if (!containerRef.current?.contains(event.target as Node)) setOpen(false);
    };
    window.addEventListener("pointerdown", closeOnOutsideClick);
    return () => window.removeEventListener("pointerdown", closeOnOutsideClick);
  }, []);

  const results = useMemo(() => {
    const search = normalized(query);
    if (!search) {
      const byId = new Map(persons.map((person) => [person.id, person]));
      return recentIds.map((id) => byId.get(id)).filter((person): person is SearchablePerson => Boolean(person));
    }
    return persons
      .map((person) => ({ person, score: matchScore(person, search) }))
      .filter((result) => Number.isFinite(result.score))
      .sort((a, b) => a.score - b.score || a.person.fullname.localeCompare(b.person.fullname))
      .slice(0, MAX_RESULTS)
      .map((result) => result.person);
  }, [persons, query, recentIds]);

  const selectPerson = (person: SearchablePerson) => {
    const updatedRecent = [person.id, ...recentIds.filter((id) => id !== person.id)].slice(0, MAX_RECENT);
    setRecentIds(updatedRecent);
    try {
      localStorage.setItem(RECENT_STORAGE_KEY, JSON.stringify(updatedRecent));
    } catch (error) {
      console.warn("Unable to save recent people", error);
    }
    setQuery(person.fullname);
    setOpen(false);
    onSelect(person);
  };

  const listId = "graph-person-search-results";
  const activeResult = results[activeIndex];

  useEffect(() => {
    if (!open || !activeResult) return;
    document.getElementById(`graph-search-person-${activeResult.id}`)?.scrollIntoView({ block: "nearest" });
  }, [activeResult, open]);

  return (
    <div
      ref={containerRef}
      className="relative w-full md:w-72"
      onBlur={(event) => {
        if (!event.currentTarget.contains(event.relatedTarget)) setOpen(false);
      }}
    >
      <label htmlFor="graph-person-search" className="sr-only">
        {t("search.label")}
      </label>
      <div className="relative">
        <span className="pointer-events-none absolute inset-y-0 left-3 flex items-center text-gray-400" aria-hidden="true">
          <svg viewBox="0 0 20 20" className="h-4 w-4 fill-none stroke-current" strokeWidth="2">
            <circle cx="8.5" cy="8.5" r="5.5" />
            <path d="m13 13 4 4" />
          </svg>
        </span>
        <input
          ref={inputRef}
          id="graph-person-search"
          type="search"
          role="combobox"
          aria-autocomplete="list"
          aria-expanded={open}
          aria-controls={listId}
          aria-activedescendant={open && activeResult ? `graph-search-person-${activeResult.id}` : undefined}
          value={query}
          onFocus={() => {
            loadRecent();
            setOpen(true);
          }}
          onChange={(event) => {
            setQuery(event.target.value);
            setActiveIndex(0);
            setOpen(true);
          }}
          onKeyDown={(event) => {
            if (event.key === "ArrowDown") {
              event.preventDefault();
              setOpen(true);
              setActiveIndex((index) => Math.min(index + 1, Math.max(results.length - 1, 0)));
            } else if (event.key === "ArrowUp") {
              event.preventDefault();
              setActiveIndex((index) => Math.max(index - 1, 0));
            } else if (event.key === "Enter" && open && activeResult) {
              event.preventDefault();
              selectPerson(activeResult);
            } else if (event.key === "Escape") {
              setOpen(false);
            }
          }}
          placeholder={t("search.placeholder")}
          className="w-full rounded border py-2 pl-9 pr-9 text-sm text-gray-900"
        />
        {!query && (
          <kbd className="pointer-events-none absolute inset-y-0 right-2 hidden items-center text-xs text-gray-400 sm:flex">
            /
          </kbd>
        )}
      </div>

      {open && (
        <div
          id={listId}
          role="listbox"
          aria-label={query.trim() ? t("search.results") : t("search.recent")}
          className="absolute left-0 right-0 top-full z-50 mt-1 max-h-72 overflow-y-auto rounded-lg border bg-white py-1 shadow-xl"
        >
          {!query.trim() && results.length > 0 && (
            <p className="px-3 py-1 text-xs font-medium uppercase tracking-wide text-gray-400">
              {t("search.recent")}
            </p>
          )}
          {results.map((person, index) => (
            <button
              key={person.id}
              id={`graph-search-person-${person.id}`}
              type="button"
              role="option"
              aria-selected={index === activeIndex}
              onPointerMove={() => setActiveIndex(index)}
              onClick={() => selectPerson(person)}
              className={`flex w-full items-center justify-between gap-3 px-3 py-2 text-left text-sm ${
                index === activeIndex ? "bg-blue-50 text-blue-900" : "text-gray-900 hover:bg-gray-50"
              }`}
            >
              <span className="min-w-0 truncate">{person.fullname || t("search.unknown")}</span>
              {person.alias && <span className="shrink-0 text-xs text-gray-500">{person.alias}</span>}
            </button>
          ))}
          {results.length === 0 && (
            <p className="px-3 py-3 text-sm text-gray-500" role="status">
              {query.trim() ? t("search.noResults") : t("search.noRecent")}
            </p>
          )}
        </div>
      )}
    </div>
  );
}

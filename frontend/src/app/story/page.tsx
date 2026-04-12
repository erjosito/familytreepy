"use client";

import { useState, useEffect, useCallback, useRef, Suspense } from "react";
import { useSearchParams } from "next/navigation";
import Link from "next/link";
import { getGraph, getPerson, getNotes, type Note } from "@/lib/api";
import type { PersonNode, GraphEdge } from "@/lib/types";
import { useI18n } from "@/lib/i18n";
import { formatDate } from "@/lib/dateUtils";

const API_BASE = process.env.NEXT_PUBLIC_API_URL ?? "";

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type TFunc = (key: any) => string;

/* ------------------------------------------------------------------ */
/* Slide types                                                         */
/* ------------------------------------------------------------------ */
interface TitleSlide {
  kind: "title";
  familyName: string;
}

interface PersonSlide {
  kind: "person";
  person: PersonNode;
  profilePicUrl: string | null;
  parents: string[];
  spouses: string[];
  children: string[];
}

interface PhotosSlide {
  kind: "photos";
  person: PersonNode;
  pictures: string[];
}

interface NotesSlide {
  kind: "notes";
  person: PersonNode;
  notes: Note[];
}

interface TreeContextSlide {
  kind: "context";
  person: PersonNode;
  level: number;
  childCount: number;
  parents: string[];
  spouses: string[];
  children: string[];
  centerId?: string;
  centerName?: string;
  centerRelation?: string; // e.g. "grandchild", "parent", "sibling"
}

interface FullTreeSlide {
  kind: "fulltree";
  persons: PersonNode[];
  edges: { source: string; target: string; type: string }[];
  centerId: string;
}

interface TransitionSlide {
  kind: "transition";
  text: string;
}

type Slide =
  | TitleSlide
  | PersonSlide
  | PhotosSlide
  | NotesSlide
  | TreeContextSlide
  | FullTreeSlide
  | TransitionSlide;

/* ------------------------------------------------------------------ */
/* Proxy helper (same approach as GraphViewer)                         */
/* ------------------------------------------------------------------ */
function proxyUrl(url: string | undefined): string | null {
  if (!url) return null;
  return `${API_BASE}/api/proxy/image?url=${encodeURIComponent(url)}`;
}

/* ------------------------------------------------------------------ */
/* Build the ordered slide sequence                                    */
/* ------------------------------------------------------------------ */
function buildSlides(
  persons: Map<string, PersonNode>,
  notes: Map<string, Note[]>,
  edges: GraphEdge[],
  t: TFunc,
  centerId?: string,
): Slide[] {
  const slides: Slide[] = [];

  // Build relationship maps first (needed for filtering)
  const childOf = new Map<string, string[]>(); // childId -> [parentId]
  const parentOf = new Map<string, string[]>(); // parentId -> [childId]
  const spouseOf = new Map<string, Set<string>>();

  for (const e of edges) {
    if (e.type === "isChildOf") {
      if (!childOf.has(e.source)) childOf.set(e.source, []);
      childOf.get(e.source)!.push(e.target);
      if (!parentOf.has(e.target)) parentOf.set(e.target, []);
      parentOf.get(e.target)!.push(e.source);
    } else if (e.type === "isSpouseOf") {
      if (!spouseOf.has(e.source)) spouseOf.set(e.source, new Set());
      spouseOf.get(e.source)!.add(e.target);
      if (!spouseOf.has(e.target)) spouseOf.set(e.target, new Set());
      spouseOf.get(e.target)!.add(e.source);
    }
  }

  // Filter to direct lineage only
  let includedIds: Set<string>;
  let centerSpouseIds = new Set<string>();
  if (centerId && persons.has(centerId)) {
    includedIds = new Set<string>();

    // Walk up: all direct ancestors
    const walkUp = (id: string) => {
      if (includedIds.has(id)) return;
      includedIds.add(id);
      for (const parentId of childOf.get(id) || []) {
        walkUp(parentId);
      }
    };

    // Walk down: all direct descendants
    const walkDown = (id: string) => {
      if (includedIds.has(id)) return;
      includedIds.add(id);
      for (const childId of parentOf.get(id) || []) {
        walkDown(childId);
      }
    };

    walkUp(centerId);
    // walkDown must re-process centerId even though walkUp already added it
    for (const childId of parentOf.get(centerId) || []) {
      walkDown(childId);
    }

    // Add siblings of the center person (but NOT their spouses)
    const centerParents = childOf.get(centerId) || [];
    for (const parentId of centerParents) {
      for (const siblingId of parentOf.get(parentId) || []) {
        includedIds.add(siblingId);
      }
    }

    // Add ONLY the center person's spouse(s)
    for (const spouseId of spouseOf.get(centerId) || []) {
      includedIds.add(spouseId);
      centerSpouseIds.add(spouseId);
    }

    // Add spouses of ancestors only (to show in mini-tree, but they appear in data)
    // We do NOT add spouses of siblings or descendants
  } else {
    includedIds = new Set(persons.keys());
  }

  // Custom ordering: ancestors → siblings → center → spouse → descendants (grouped by family)
  const centerLevel = centerId ? (persons.get(centerId)?.level ?? 0) : 0;
  const siblingIds = new Set<string>();
  if (centerId) {
    const centerParentIds = childOf.get(centerId) || [];
    for (const parentId of centerParentIds) {
      for (const sibId of parentOf.get(parentId) || []) {
        if (sibId !== centerId) siblingIds.add(sibId);
      }
    }
  }

  // Build ordered list by walking the tree
  const orderedIds: string[] = [];
  const addedIds = new Set<string>();

  const addPerson = (id: string) => {
    if (addedIds.has(id) || !includedIds.has(id)) return;
    addedIds.add(id);
    orderedIds.push(id);
  };

  // Walk descendants depth-first, grouping siblings together
  const walkDescendants = (id: string) => {
    const kids = (parentOf.get(id) || []).filter(cid => includedIds.has(cid));
    // Also include children of spouse
    for (const spouseId of spouseOf.get(id) || []) {
      for (const cid of parentOf.get(spouseId) || []) {
        if (includedIds.has(cid) && !kids.includes(cid)) {
          kids.push(cid);
        }
      }
    }
    // Sort siblings alphabetically within a family
    kids.sort((a, b) => ((persons.get(a)?.fullname || "").localeCompare(persons.get(b)?.fullname || "")));
    for (const kid of kids) {
      addPerson(kid);
      walkDescendants(kid);
    }
  };

  if (centerId) {
    // 1. Ancestors (sorted by level ascending, oldest first)
    const ancestors = [...persons.values()]
      .filter(p => includedIds.has(p.id) && (p.level ?? 0) < centerLevel && !siblingIds.has(p.id) && p.id !== centerId)
      .sort((a, b) => {
        const la = a.level ?? 0;
        const lb = b.level ?? 0;
        if (la !== lb) return la - lb;
        return (a.fullname || "").localeCompare(b.fullname || "");
      });
    for (const a of ancestors) addPerson(a.id);

    // 2. Siblings (same generation, before center)
    const sibs = [...siblingIds]
      .filter(id => persons.has(id))
      .sort((a, b) => ((persons.get(a)?.fullname || "").localeCompare(persons.get(b)?.fullname || "")));
    for (const s of sibs) addPerson(s);

    // 3. Center person
    addPerson(centerId);

    // 4. Center person's spouse(s)
    for (const spouseId of centerSpouseIds) addPerson(spouseId);

    // 5. Descendants (depth-first walk, siblings grouped)
    walkDescendants(centerId);
  } else {
    // No center — just sort by level
    const all = [...persons.values()].sort((a, b) => {
      const la = a.level ?? 0;
      const lb = b.level ?? 0;
      if (la !== lb) return la - lb;
      return (a.fullname || "").localeCompare(b.fullname || "");
    });
    for (const p of all) addPerson(p.id);
  }

  const sorted = orderedIds
    .map(id => persons.get(id))
    .filter((p): p is PersonNode => p !== undefined);

  if (sorted.length === 0) return slides;

  // Title slide — use center person's name
  const centerPerson = centerId ? persons.get(centerId) : null;
  const familyName = centerPerson?.fullname || t("story.familyDefault");

  slides.push({ kind: "title", familyName });

  // Full tree overview slide
  if (centerId) {
    const treeEdges = edges
      .filter(e => e.type === "isChildOf" || e.type === "isSpouseOf")
      .filter(e => includedIds.has(e.source) && includedIds.has(e.target))
      .map(e => ({ source: e.source, target: e.target, type: e.type }));
    slides.push({
      kind: "fulltree",
      persons: sorted,
      edges: treeEdges,
      centerId,
    });
  }

  // Build child count map
  const childCountMap = new Map<string, number>();
  for (const e of edges) {
    if (e.type === "isChildOf") {
      childCountMap.set(e.target, (childCountMap.get(e.target) || 0) + 1);
    }
  }

  // Build a map of picture URL → set of person IDs who have that picture
  // so we can defer shared pictures to the latest person in the story
  const picOwners = new Map<string, Set<string>>();
  for (const p of sorted) {
    if (p.pictures) {
      for (const url of p.pictures) {
        if (!picOwners.has(url)) picOwners.set(url, new Set());
        picOwners.get(url)!.add(p.id);
      }
    }
  }
  // Track which pictures have already been shown
  const shownPics = new Set<string>();

  for (let i = 0; i < sorted.length; i++) {
    const p = sorted[i];

    // Transition slide (between persons, not before the first)
    if (i > 0) {
      const prev = sorted[i - 1];
      const transitionText = getTransitionText(prev, p, childOf, parentOf, spouseOf, persons, t, centerId);
      slides.push({ kind: "transition", text: transitionText });
    }

    // Person slide with family context
    const personParents = (childOf.get(p.id) || []).map(id => persons.get(id)?.fullname || "").filter(Boolean);
    const personSpouses = [...(spouseOf.get(p.id) || [])].map(id => persons.get(id)?.fullname || "").filter(Boolean);
    const personChildren = (parentOf.get(p.id) || []).map(id => persons.get(id)?.fullname || "").filter(Boolean);

    // Tree context slide first — shows where the person fits
    const relativeLevel = (p.level ?? 0) - centerLevel;
    const ctrPerson = centerId ? persons.get(centerId) : undefined;
    const ctrName = ctrPerson?.alias || ctrPerson?.firstname || ctrPerson?.fullname || "";
    // Determine relationship label between this person and center
    let centerRelation = "";
    if (centerId && p.id !== centerId) {
      const lvDiff = relativeLevel;
      if (lvDiff < 0) {
        // Person is an ancestor of center
        const gen = Math.abs(lvDiff);
        centerRelation = gen === 1 ? (isFemale(persons.get(p.id)) ? t("story.motherOf") : t("story.fatherOf"))
          : gen === 2 ? (isFemale(persons.get(p.id)) ? t("story.grandmotherOf") : t("story.grandfatherOf"))
          : t("story.ancestorOf");
      } else if (lvDiff === 0 && siblingIds.has(p.id)) {
        centerRelation = isFemale(persons.get(p.id)) ? t("story.sisterOf") : t("story.brotherOf");
      } else if (lvDiff === 0 && centerSpouseIds.has(p.id)) {
        centerRelation = t("story.spouseOf");
      } else if (lvDiff > 0) {
        const gen = lvDiff;
        centerRelation = gen === 1 ? (isFemale(persons.get(p.id)) ? t("story.daughter") : t("story.son"))
          : gen === 2 ? (isFemale(persons.get(p.id)) ? t("story.granddaughterOf") : t("story.grandsonOf"))
          : t("story.descendantOf");
      }
    }

    slides.push({
      kind: "context",
      person: p,
      level: relativeLevel,
      childCount: childCountMap.get(p.id) || 0,
      parents: personParents,
      spouses: personSpouses,
      children: personChildren,
      centerId,
      centerName: ctrName,
      centerRelation,
    });

    // Then the person intro slide
    slides.push({
      kind: "person",
      person: p,
      profilePicUrl: proxyUrl(p.profilepic),
      parents: personParents,
      spouses: personSpouses,
      children: personChildren,
    });

    // Photos slides — only show pictures not shared with persons appearing later
    if (p.pictures && p.pictures.length > 0) {
      const laterIds = new Set(sorted.slice(i + 1).map(s => s.id));
      const uniquePics = p.pictures.filter(url => {
        if (shownPics.has(url)) return false; // already shown
        const owners = picOwners.get(url);
        // Skip if any later person also has this picture
        if (owners && [...owners].some(oid => laterIds.has(oid))) return false;
        return true;
      });
      for (const url of uniquePics) shownPics.add(url);
      if (uniquePics.length > 0) {
        for (let j = 0; j < uniquePics.length; j += 3) {
          slides.push({
            kind: "photos",
            person: p,
            pictures: uniquePics.slice(j, j + 3),
          });
        }
      }
    }

    // Notes slide
    const personNotes = notes.get(p.id) || [];
    if (personNotes.length > 0) {
      slides.push({
        kind: "notes",
        person: p,
        notes: personNotes,
      });
    }
  }

  return slides;
}

function isFemale(person: PersonNode | undefined): boolean {
  if (!person) return false;
  return person.gender === "female";
}

function getTransitionText(
  prev: PersonNode,
  current: PersonNode,
  childOf: Map<string, string[]>,
  parentOf: Map<string, string[]>,
  spouseOf: Map<string, Set<string>>,
  persons: Map<string, PersonNode>,
  t: TFunc,
  centerId?: string,
): string {
  const currentPerson = persons.get(current.id);
  const prevPerson = persons.get(prev.id);
  const curName = currentPerson?.alias || currentPerson?.firstname || currentPerson?.fullname || "";
  const prevName = prevPerson?.alias || prevPerson?.firstname || prevPerson?.fullname || "";

  // Helper: get both parent names for a person
  const getParentNames = (personId: string): string => {
    const parentIds = childOf.get(personId) || [];
    const names = parentIds
      .map(pid => persons.get(pid)?.alias || persons.get(pid)?.firstname || persons.get(pid)?.fullname || "")
      .filter(Boolean);
    if (names.length === 2) return `${names[0]} ${t("story.and")} ${names[1]}`;
    if (names.length === 1) return names[0];
    return "";
  };

  // Helper: describe relationship to center person
  const centerName = centerId ? (persons.get(centerId)?.alias || persons.get(centerId)?.firstname || persons.get(centerId)?.fullname || "") : "";

  // Center person gets a special introduction
  if (centerId && current.id === centerId) {
    const prot = isFemale(currentPerson) ? t("story.protagonistF") : t("story.protagonistM");
    return `${curName}, ${prot}`;
  }

  // Check if current is a child of prev (or prev's spouse)
  const currentParents = childOf.get(current.id) || [];
  const isPrevParent = currentParents.includes(prev.id);
  const isPrevSpouseParent = !isPrevParent && [...(spouseOf.get(prev.id) || [])].some(sid => currentParents.includes(sid));

  if (isPrevParent || isPrevSpouseParent) {
    const rel = isFemale(currentPerson) ? t("story.daughter") : t("story.son");
    const parentStr = getParentNames(current.id);
    return `${curName}, ${rel} ${parentStr}`;
  }

  // Check if current is a sibling of prev (same parents, grouped together)
  const prevParents = childOf.get(prev.id) || [];
  const sharedParent = currentParents.find(pid => prevParents.includes(pid));
  if (sharedParent && current.id !== prev.id) {
    // They're siblings — show as child of parents + sibling of prev
    const rel = isFemale(currentPerson) ? t("story.daughter") : t("story.son");
    const parentStr = getParentNames(current.id);
    const sibRel = isFemale(currentPerson) ? t("story.sisterOf") : t("story.brotherOf");
    return `${curName}, ${rel} ${parentStr} ${t("story.and")} ${sibRel} ${prevName}`;
  }

  // Check if current has parents that are in the story (grandchild etc.)
  if (currentParents.length > 0) {
    const parentInStory = currentParents.find(pid => persons.has(pid));
    if (parentInStory) {
      const rel = isFemale(currentPerson) ? t("story.daughter") : t("story.son");
      const parentStr = getParentNames(current.id);
      return `${curName}, ${rel} ${parentStr}`;
    }
  }

  // Check if current is spouse of prev
  if (spouseOf.get(prev.id)?.has(current.id)) {
    return `${prevName} ${t("story.marriedTo")} ${curName}`;
  }

  // Check if prev is a child of current (going up the tree)
  if (prevParents.includes(current.id)) {
    const rel = isFemale(currentPerson) ? t("story.motherOf") : t("story.fatherOf");
    return `${curName}, ${rel} ${prevName}`;
  }

  // Check if they are siblings (share a parent)
  const prevParentSet = new Set(prevParents);
  for (const parentId of currentParents) {
    if (prevParentSet.has(parentId)) {
      const childRel = isFemale(currentPerson) ? t("story.daughter") : t("story.son");
      const sibRel = isFemale(currentPerson) ? t("story.sisterOf") : t("story.brotherOf");
      const parentStr = getParentNames(current.id);

      // Find all siblings (children of same parents, excluding current)
      const siblingNames: string[] = [];
      for (const pid of currentParents) {
        for (const sibId of (parentOf.get(pid) || [])) {
          if (sibId !== current.id) {
            const sibPerson = persons.get(sibId);
            const sibName = sibPerson?.alias || sibPerson?.firstname || sibPerson?.fullname || "";
            if (sibName && !siblingNames.includes(sibName)) {
              siblingNames.push(sibName);
            }
          }
        }
      }

      const sibListStr = siblingNames.length > 1
        ? siblingNames.slice(0, -1).join(", ") + " " + t("story.and") + " " + siblingNames[siblingNames.length - 1]
        : siblingNames[0] || prevName;

      return `${curName}, ${childRel} ${parentStr} ${t("story.and")} ${sibRel} ${sibListStr}`;
    }
  }

  // Generic
  return `${curName}...`;
}

function extractYear(dateStr: string | undefined): number | null {
  if (!dateStr) return null;
  const match = dateStr.match(/(\d{4})/);
  return match ? parseInt(match[1], 10) : null;
}

/* ------------------------------------------------------------------ */
/* Individual slide renderers                                          */
/* ------------------------------------------------------------------ */
function TitleSlideView({ slide, t }: { slide: TitleSlide; t: TFunc }) {
  return (
    <div className="flex flex-col items-center justify-center h-full text-center px-8">
      <h1 className="text-5xl md:text-7xl font-bold text-white mb-4 tracking-tight">
        {slide.familyName}
      </h1>
      <p className="text-xl md:text-2xl text-gray-300 mb-6">{t("story.title")}</p>
    </div>
  );
}

function PersonSlideView({ slide, t }: { slide: PersonSlide; t: TFunc }) {
  const p = slide.person;
  return (
    <div className="flex flex-col items-center justify-center h-full px-8">
      {/* Profile picture */}
      {slide.profilePicUrl ? (
        <img
          src={slide.profilePicUrl}
          alt={p.fullname}
          className="w-48 h-48 md:w-64 md:h-64 rounded-full object-cover border-4 border-white/20 mb-6 shadow-2xl"
        />
      ) : (
        <div className="w-48 h-48 md:w-64 md:h-64 rounded-full bg-gray-700 flex items-center justify-center text-gray-300 text-6xl md:text-7xl font-bold border-4 border-white/10 mb-6">
          {(p.firstname?.[0] || "?").toUpperCase()}
        </div>
      )}

      {/* Name */}
      <h2 className="text-4xl md:text-5xl font-bold text-white mb-4">
        {p.fullname || "Unknown"}
      </h2>

      {/* Details */}
      <div className="bg-black/30 rounded-xl px-8 py-4 backdrop-blur-sm space-y-1 text-center mb-4">
        {p.birthdate && (
          <p className="text-gray-200 text-lg">
            🎂 {formatDate(p.birthdate)}
            {p.birthplace ? ` · ${p.birthplace}` : ""}
          </p>
        )}
        <p className="text-gray-300 text-base">
          {!!p.isAlive
            ? `✨ ${t("person.living")}`
            : `🕊️ ${t("person.deceased")}${p.deathdate ? ` · ${formatDate(p.deathdate)}` : ""}`}
        </p>
      </div>

      {/* Mini family tree */}
      {(slide.parents.length > 0 || slide.spouses.length > 0 || slide.children.length > 0) && (
        <div className="flex gap-8 text-sm text-gray-400 mt-2">
          {slide.parents.length > 0 && (
            <div className="text-center">
              <p className="text-xs uppercase tracking-wide text-gray-500 mb-1">↑ {t("person.parents")}</p>
              {slide.parents.map((name, i) => (
                <p key={i} className="text-gray-300">{name}</p>
              ))}
            </div>
          )}
          {slide.spouses.length > 0 && (
            <div className="text-center">
              <p className="text-xs uppercase tracking-wide text-gray-500 mb-1">♥ {t("person.spouses")}</p>
              {slide.spouses.map((name, i) => (
                <p key={i} className="text-gray-300">{name}</p>
              ))}
            </div>
          )}
          {slide.children.length > 0 && (
            <div className="text-center">
              <p className="text-xs uppercase tracking-wide text-gray-500 mb-1">↓ {t("person.children")}</p>
              {slide.children.map((name, i) => (
                <p key={i} className="text-gray-300">{name}</p>
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  );
}

function PhotosSlideView({ slide }: { slide: PhotosSlide }) {
  const pics = slide.pictures;
  const count = pics.length;
  // Ken Burns CSS animation variations
  const kenBurnsVariants = [
    "animate-kenburns-1",
    "animate-kenburns-2",
    "animate-kenburns-3",
  ];

  return (
    <div className="flex flex-col items-center justify-center h-full px-8">
      <p className="text-sm text-gray-400 mb-4 uppercase tracking-widest">
        {slide.person.fullname}
      </p>

      {count === 1 ? (
        <div className="overflow-hidden rounded-xl shadow-2xl max-h-[75vh] max-w-[90vw]">
          <img
            src={proxyUrl(pics[0]) || ""}
            alt={slide.person.fullname}
            className={`max-h-[75vh] max-w-full object-cover ${kenBurnsVariants[0]}`}
          />
        </div>
      ) : count === 2 ? (
        <div className="flex gap-4 max-h-[70vh]">
          {pics.map((pic, i) => (
            <div key={i} className="overflow-hidden rounded-xl shadow-2xl flex-1">
              <img
                src={proxyUrl(pic) || ""}
                alt={`${slide.person.fullname} ${i + 1}`}
                className={`w-full h-[65vh] object-cover ${kenBurnsVariants[i]}`}
              />
            </div>
          ))}
        </div>
      ) : (
        <div className="grid grid-cols-3 gap-3 max-h-[70vh]">
          {pics.map((pic, i) => (
            <div key={i} className="overflow-hidden rounded-xl shadow-2xl">
              <img
                src={proxyUrl(pic) || ""}
                alt={`${slide.person.fullname} ${i + 1}`}
                className={`w-full h-[60vh] object-cover ${kenBurnsVariants[i]}`}
              />
            </div>
          ))}
        </div>
      )}

      {/* Ken Burns CSS */}
      <style jsx>{`
        @keyframes kenburns1 {
          0% { transform: scale(1) translate(0, 0); }
          50% { transform: scale(1.15) translate(-2%, -1%); }
          100% { transform: scale(1) translate(0, 0); }
        }
        @keyframes kenburns2 {
          0% { transform: scale(1.1) translate(1%, -1%); }
          50% { transform: scale(1) translate(0, 0); }
          100% { transform: scale(1.1) translate(1%, -1%); }
        }
        @keyframes kenburns3 {
          0% { transform: scale(1) translate(0, 0); }
          50% { transform: scale(1.12) translate(2%, 1%); }
          100% { transform: scale(1) translate(0, 0); }
        }
        .animate-kenburns-1 { animation: kenburns1 12s ease-in-out infinite; }
        .animate-kenburns-2 { animation: kenburns2 14s ease-in-out infinite; }
        .animate-kenburns-3 { animation: kenburns3 10s ease-in-out infinite; }
      `}</style>
    </div>
  );
}

function NotesSlideView({ slide }: { slide: NotesSlide }) {
  return (
    <div className="flex flex-col items-center justify-center h-full px-8 max-w-3xl mx-auto">
      <p className="text-sm text-gray-400 mb-6 uppercase tracking-widest">
        {slide.person.fullname}
      </p>

      <div className="space-y-6 w-full">
        {slide.notes.map((note, i) => (
          <blockquote
            key={i}
            className="border-l-4 border-amber-400/60 pl-6 py-2"
          >
            <p className="text-xl text-gray-100 italic leading-relaxed">
              &ldquo;{note.text}&rdquo;
            </p>
            {(note.author || note.timestamp) && (
              <footer className="mt-2 text-sm text-gray-400">
                {note.author && <span>— {note.author}</span>}
                {note.timestamp && (
                  <span className="ml-2 text-gray-500">
                    {new Date(note.timestamp).toLocaleDateString()}
                  </span>
                )}
              </footer>
            )}
          </blockquote>
        ))}
      </div>
    </div>
  );
}

function TreeContextSlideView({ slide, t }: { slide: TreeContextSlide; t: TFunc }) {
  const { person, parents, spouses, children, centerId, centerName, centerRelation } = slide;
  const personName = person.fullname || "?";
  const isPersonCenter = centerId === person.id;

  // Estimate text width: ~7px per character at fontSize 13
  const charW = 7;
  const nodePadding = 24;
  const nodeRy = 22;
  const calcRx = (label: string) => Math.max(60, (label.length * charW) / 2 + nodePadding);

  // Layout constants
  const cx = 400;
  const parentY = 60;
  const mainY = 180;
  const childY = 300;
  const hGap = 30; // gap between node edges

  // Build node data with dynamic widths
  // nodeType: "current" = this slide's person, "center" = story protagonist, "normal" = others
  type NodeInfo = { x: number; y: number; label: string; nodeType: "current" | "center" | "normal"; rx: number };
  const allNodes: NodeInfo[] = [];
  const lines: { x1: number; y1: number; x2: number; y2: number }[] = [];

  // Helper: lay out a row of labels centered on cx, returns positions
  function layoutRow(labels: string[], y: number): { x: number; rx: number }[] {
    const rxs = labels.map((l) => calcRx(l));
    // Total width: sum of diameters + gaps
    let totalW = 0;
    for (let i = 0; i < rxs.length; i++) {
      totalW += rxs[i] * 2;
      if (i > 0) totalW += hGap;
    }
    let curX = cx - totalW / 2;
    const positions: { x: number; rx: number }[] = [];
    for (let i = 0; i < rxs.length; i++) {
      const nodeX = curX + rxs[i];
      positions.push({ x: nodeX, rx: rxs[i] });
      curX += rxs[i] * 2 + hGap;
    }
    return positions;
  }

  // Parents row
  const parentPositions = layoutRow(parents, parentY);
  parents.forEach((name, i) => {
    const pos = parentPositions[i];
    allNodes.push({ x: pos.x, y: parentY, label: name, nodeType: "normal", rx: pos.rx });
    lines.push({ x1: pos.x, y1: parentY + nodeRy, x2: cx, y2: mainY - nodeRy });
  });

  // Main person + spouses row
  const mainLabels = [personName, ...spouses];
  const mainPositions = layoutRow(mainLabels, mainY);
  mainLabels.forEach((name, i) => {
    const pos = mainPositions[i];
    const nt = i === 0 ? (isPersonCenter ? "center" : "current") : "normal";
    allNodes.push({ x: pos.x, y: mainY, label: name, nodeType: nt, rx: pos.rx });
    if (i > 0) {
      const prev = mainPositions[i - 1];
      lines.push({ x1: prev.x + prev.rx, y1: mainY, x2: pos.x - pos.rx, y2: mainY });
    }
  });

  // Children row
  const childPositions = layoutRow(children, childY);
  const parentMidX = spouses.length > 0 && mainPositions.length > 1
    ? (mainPositions[0].x + mainPositions[1].x) / 2
    : cx;
  children.forEach((name, i) => {
    const pos = childPositions[i];
    allNodes.push({ x: pos.x, y: childY, label: name, nodeType: "normal", rx: pos.rx });
    lines.push({ x1: parentMidX, y1: mainY + nodeRy, x2: pos.x, y2: childY - nodeRy });
  });

  // Calculate SVG viewBox dynamically
  const allLeft = allNodes.map((n) => n.x - n.rx);
  const allRight = allNodes.map((n) => n.x + n.rx);
  const minX = Math.min(...allLeft, cx - 50) - 20;
  const maxX = Math.max(...allRight, cx + 50) + 20;
  const svgW = maxX - minX;
  const svgH = 360;

  return (
    <div className="flex flex-col items-center justify-center h-full px-8">
      <p className="text-sm text-gray-400 mb-2 uppercase tracking-widest">
        {t("story.generation")} {slide.level > 0 ? `+${slide.level}` : slide.level}
      </p>

      <svg
        viewBox={`${minX} 0 ${svgW} ${svgH}`}
        className="w-full max-w-4xl h-auto max-h-[60vh]"
        xmlns="http://www.w3.org/2000/svg"
      >
        {/* Lines */}
        {lines.map((l, i) => (
          <line
            key={i}
            x1={l.x1} y1={l.y1} x2={l.x2} y2={l.y2}
            stroke="#555" strokeWidth="1.5" opacity="0.6"
          />
        ))}

        {/* Nodes */}
        {allNodes.map((node, i) => {
          const fills = {
            center: { bg: "#d97706", stroke: "#fbbf24", text: "#fff", sw: 3 },
            current: { bg: "#2563eb", stroke: "#60a5fa", text: "#fff", sw: 2 },
            normal: { bg: "#374151", stroke: "#555", text: "#d1d5db", sw: 1 },
          };
          const f = fills[node.nodeType];
          return (
            <g key={i}>
              <ellipse
                cx={node.x} cy={node.y}
                rx={node.rx} ry={nodeRy}
                fill={f.bg} stroke={f.stroke} strokeWidth={f.sw}
                opacity={0.9}
              />
              {node.nodeType === "center" && (
                <text x={node.x} y={node.y - nodeRy - 6} textAnchor="middle" fill="#fbbf24" fontSize="14">★</text>
              )}
              <text
                x={node.x} y={node.y + 1}
                textAnchor="middle" dominantBaseline="middle"
                fill={f.text} fontSize="13" fontFamily="system-ui, sans-serif"
              >
                {node.label}
              </text>
            </g>
          );
        })}
      </svg>

      {/* Relationship to center person */}
      {!isPersonCenter && centerName && centerRelation && (
        <p className="mt-4 text-base text-amber-400 italic text-center">
          ★ {centerRelation} {centerName}
        </p>
      )}
    </div>
  );
}

function FullTreeSlideView({ slide, t }: { slide: FullTreeSlide; t: TFunc }) {
  const { persons, edges, centerId } = slide;

  const charW = 6;
  const nodePadding = 16;
  const nodeRy = 18;
  const ySpacing = 70;
  const hGap = 20;
  const calcRx = (label: string) => Math.max(45, (label.length * charW) / 2 + nodePadding);

  // Group persons by level
  const levels = new Map<number, PersonNode[]>();
  for (const p of persons) {
    const lv = p.level ?? 0;
    if (!levels.has(lv)) levels.set(lv, []);
    levels.get(lv)!.push(p);
  }
  const sortedLevels = [...levels.keys()].sort((a, b) => a - b);

  // Layout each level row
  const cx = 500;
  const nodePositions = new Map<string, { x: number; y: number; rx: number }>();

  sortedLevels.forEach((lv, row) => {
    const rowPersons = levels.get(lv)!;
    const labels = rowPersons.map(p => p.fullname || "?");
    const rxs = labels.map(l => calcRx(l));
    let totalW = 0;
    for (let i = 0; i < rxs.length; i++) {
      totalW += rxs[i] * 2;
      if (i > 0) totalW += hGap;
    }
    let curX = cx - totalW / 2;
    rowPersons.forEach((p, i) => {
      const nodeX = curX + rxs[i];
      nodePositions.set(p.id, { x: nodeX, y: row * ySpacing + 40, rx: rxs[i] });
      curX += rxs[i] * 2 + hGap;
    });
  });

  // Build lines from edges
  const lineData: { x1: number; y1: number; x2: number; y2: number; type: string }[] = [];
  const seenEdges = new Set<string>();
  for (const e of edges) {
    const key = `${e.source}-${e.target}-${e.type}`;
    if (seenEdges.has(key)) continue;
    seenEdges.add(key);
    const sp = nodePositions.get(e.source);
    const tp = nodePositions.get(e.target);
    if (sp && tp) {
      if (e.type === "isSpouseOf") {
        const rKey = `${e.target}-${e.source}-${e.type}`;
        seenEdges.add(rKey);
        lineData.push({ x1: sp.x + sp.rx, y1: sp.y, x2: tp.x - tp.rx, y2: tp.y, type: e.type });
      } else {
        lineData.push({ x1: sp.x, y1: sp.y + nodeRy, x2: tp.x, y2: tp.y - nodeRy, type: e.type });
      }
    }
  }

  // Calculate viewBox
  const allPositions = [...nodePositions.values()];
  const minX = Math.min(...allPositions.map(p => p.x - p.rx)) - 20;
  const maxX = Math.max(...allPositions.map(p => p.x + p.rx)) + 20;
  const maxY = Math.max(...allPositions.map(p => p.y)) + nodeRy + 20;
  const svgW = maxX - minX;
  const svgH = maxY + 10;

  return (
    <div className="flex flex-col items-center justify-center h-full px-4">
      <p className="text-sm text-gray-400 mb-2 uppercase tracking-widest">{t("story.familyOverview")}</p>
      <svg
        viewBox={`${minX} 0 ${svgW} ${svgH}`}
        className="w-full max-w-5xl h-auto max-h-[80vh]"
        xmlns="http://www.w3.org/2000/svg"
      >
        {lineData.map((l, i) => (
          <line key={i} x1={l.x1} y1={l.y1} x2={l.x2} y2={l.y2}
            stroke={l.type === "isSpouseOf" ? "#666" : "#555"} strokeWidth="1" opacity="0.5"
            strokeDasharray={l.type === "isSpouseOf" ? "4 2" : "none"}
          />
        ))}
        {persons.map((p) => {
          const pos = nodePositions.get(p.id);
          if (!pos) return null;
          const isCenter = p.id === centerId;
          return (
            <g key={p.id}>
              <ellipse cx={pos.x} cy={pos.y} rx={pos.rx} ry={nodeRy}
                fill={isCenter ? "#d97706" : "#374151"}
                stroke={isCenter ? "#fbbf24" : "#555"}
                strokeWidth={isCenter ? 2 : 1}
                opacity={0.9}
              />
              {isCenter && (
                <text x={pos.x} y={pos.y - nodeRy - 4} textAnchor="middle" fill="#fbbf24" fontSize="12">★</text>
              )}
              <text x={pos.x} y={pos.y + 1} textAnchor="middle" dominantBaseline="middle"
                fill={isCenter ? "#fff" : "#d1d5db"} fontSize="11" fontFamily="system-ui, sans-serif"
              >
                {p.fullname || "?"}
              </text>
            </g>
          );
        })}
      </svg>
    </div>
  );
}

function TransitionSlideView({ slide }: { slide: TransitionSlide }) {
  return (
    <div className="flex items-center justify-center h-full px-8">
      <p className="text-3xl md:text-4xl text-gray-300 italic font-light text-center">
        {slide.text}
      </p>
    </div>
  );
}

/* ------------------------------------------------------------------ */
/* Main presentation component                                         */
/* ------------------------------------------------------------------ */
function SlideRenderer({ slide, t }: { slide: Slide; t: TFunc }) {
  switch (slide.kind) {
    case "title":
      return <TitleSlideView slide={slide} t={t} />;
    case "person":
      return <PersonSlideView slide={slide} t={t} />;
    case "photos":
      return <PhotosSlideView slide={slide} />;
    case "notes":
      return <NotesSlideView slide={slide} />;
    case "context":
      return <TreeContextSlideView slide={slide} t={t} />;
    case "fulltree":
      return <FullTreeSlideView slide={slide} t={t} />;
    case "transition":
      return <TransitionSlideView slide={slide} />;
  }
}

/* ------------------------------------------------------------------ */
/* Speed options                                                       */
/* ------------------------------------------------------------------ */
const SPEED_OPTIONS = [5, 8, 10, 15] as const;

/* ------------------------------------------------------------------ */
/* Story page content (inside Suspense)                                */
/* ------------------------------------------------------------------ */
function StoryPageContent() {
  const searchParams = useSearchParams();
  const rootId = searchParams.get("id") || "";
  const { t } = useI18n();

  const [slides, setSlides] = useState<Slide[]>([]);
  const [currentIndex, setCurrentIndex] = useState(0);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [playing, setPlaying] = useState(false);
  const [speed, setSpeed] = useState(8);
  const [fadeState, setFadeState] = useState<"in" | "out">("in");

  const timerRef = useRef<ReturnType<typeof setTimeout> | null>(null);
  const containerRef = useRef<HTMLDivElement>(null);

  // Load all data upfront
  useEffect(() => {
    if (!rootId) {
      setError("No person ID provided");
      setLoading(false);
      return;
    }

    let cancelled = false;

    async function loadData() {
      try {
        // 1. Get full graph (no degree limit — filtering handles inclusion)
        const graph = await getGraph(undefined, undefined, true);

        if (cancelled) return;

        // 2. Fetch full person details for each node
        const personPromises = graph.nodes.map((n) =>
          getPerson(n.id).catch(() => null)
        );
        const personResults = await Promise.all(personPromises);

        if (cancelled) return;

        const persons = new Map<string, PersonNode>();
        for (const raw of personResults) {
          if (!raw) continue;
          const clean: Record<string, unknown> = {};
          for (const [k, v] of Object.entries(raw)) {
            if (v !== null && v !== undefined && (Array.isArray(v) || typeof v !== "object")) {
              clean[k] = v;
            }
          }
          const p = clean as PersonNode;
          // Preserve level from graph nodes
          const graphNode = graph.nodes.find((n) => n.id === p.id);
          if (graphNode?.level !== undefined) p.level = graphNode.level;
          persons.set(p.id, p);
        }

        // 3. Fetch notes for each person
        const notePromises = [...persons.keys()].map((id) =>
          getNotes(id)
            .then((notes) => [id, notes] as [string, Note[]])
            .catch(() => [id, []] as [string, Note[]])
        );
        const noteResults = await Promise.all(notePromises);

        if (cancelled) return;

        const notesMap = new Map<string, Note[]>(noteResults);

        // 4. Build slides
        const builtSlides = buildSlides(persons, notesMap, graph.edges, t, rootId);
        setSlides(builtSlides);
        setCurrentIndex(0);
        setFadeState("in");
      } catch (err) {
        console.error("Failed to load story data:", err);
        setError(String(err));
      } finally {
        if (!cancelled) setLoading(false);
      }
    }

    loadData();
    return () => {
      cancelled = true;
    };
  }, [rootId, t]);

  // Navigate with fade transition
  const goToSlide = useCallback(
    (index: number) => {
      if (index < 0 || index >= slides.length) return;
      setFadeState("out");
      setTimeout(() => {
        setCurrentIndex(index);
        setFadeState("in");
      }, 300);
    },
    [slides.length],
  );

  const goNext = useCallback(() => {
    if (currentIndex < slides.length - 1) {
      goToSlide(currentIndex + 1);
    } else {
      setPlaying(false);
    }
  }, [currentIndex, slides.length, goToSlide]);

  const goPrev = useCallback(() => {
    if (currentIndex > 0) {
      goToSlide(currentIndex - 1);
    }
  }, [currentIndex, goToSlide]);

  // Auto-advance timer
  useEffect(() => {
    if (timerRef.current) {
      clearTimeout(timerRef.current);
      timerRef.current = null;
    }

    if (playing && slides.length > 0 && currentIndex < slides.length - 1) {
      timerRef.current = setTimeout(goNext, speed * 1000);
    }

    return () => {
      if (timerRef.current) clearTimeout(timerRef.current);
    };
  }, [playing, currentIndex, speed, slides.length, goNext]);

  // Keyboard controls
  useEffect(() => {
    const handleKey = (e: KeyboardEvent) => {
      if (e.key === "ArrowRight" || e.key === "ArrowDown") {
        e.preventDefault();
        goNext();
      } else if (e.key === "ArrowLeft" || e.key === "ArrowUp") {
        e.preventDefault();
        goPrev();
      } else if (e.key === " ") {
        e.preventDefault();
        setPlaying((p) => !p);
      } else if (e.key === "Escape") {
        window.location.href = "/";
      }
    };

    window.addEventListener("keydown", handleKey);
    return () => window.removeEventListener("keydown", handleKey);
  }, [goNext, goPrev]);

  // Click navigation (left/right halves of screen)
  const handleClick = useCallback(
    (e: React.MouseEvent) => {
      const rect = containerRef.current?.getBoundingClientRect();
      if (!rect) return;
      // Ignore clicks on the control bar area (bottom 80px)
      if (e.clientY > rect.bottom - 80) return;
      const x = e.clientX - rect.left;
      if (x < rect.width / 2) {
        goPrev();
      } else {
        goNext();
      }
    },
    [goNext, goPrev],
  );

  // Loading screen
  if (loading) {
    return (
      <div className="fixed inset-0 bg-gray-900 flex flex-col items-center justify-center z-50">
        <div className="animate-pulse text-white text-xl mb-4">📖</div>
        <p className="text-gray-300 text-lg">{t("story.loading")}</p>
      </div>
    );
  }

  // Error screen
  if (error || slides.length === 0) {
    return (
      <div className="fixed inset-0 bg-gray-900 flex flex-col items-center justify-center z-50 gap-4">
        <p className="text-gray-300">{error || t("story.noSlides")}</p>
        <Link href="/" className="text-blue-400 hover:underline">
          {t("story.exit")}
        </Link>
      </div>
    );
  }

  const progress = ((currentIndex + 1) / slides.length) * 100;

  return (
    <div
      ref={containerRef}
      className="fixed inset-0 bg-gray-900 flex flex-col z-50 select-none"
      onClick={handleClick}
    >
      {/* Slide content with fade */}
      <div
        className="flex-1 overflow-hidden transition-opacity duration-300 ease-in-out"
        style={{ opacity: fadeState === "in" ? 1 : 0 }}
      >
        <SlideRenderer slide={slides[currentIndex]} t={t} />
      </div>

      {/* Bottom control bar */}
      <div
        className="flex-shrink-0 bg-black/60 backdrop-blur-sm border-t border-white/10 px-4 py-3"
        onClick={(e) => e.stopPropagation()}
      >
        {/* Progress bar */}
        <div className="w-full h-1 bg-white/10 rounded-full mb-3 overflow-hidden">
          <div
            className="h-full bg-amber-400 rounded-full transition-all duration-500 ease-out"
            style={{ width: `${progress}%` }}
          />
        </div>

        <div className="flex items-center justify-between">
          {/* Left: play/pause + speed */}
          <div className="flex items-center gap-3">
            <button
              onClick={() => setPlaying((p) => !p)}
              className="text-white hover:text-amber-300 transition-colors text-sm px-3 py-1.5 rounded bg-white/10 hover:bg-white/20"
            >
              {playing ? `⏸ ${t("story.pause")}` : `▶ ${t("story.play")}`}
            </button>

            <div className="flex items-center gap-1.5">
              <span className="text-xs text-gray-400">{t("story.speed")}:</span>
              {SPEED_OPTIONS.map((s) => (
                <button
                  key={s}
                  onClick={() => setSpeed(s)}
                  className={`text-xs px-2 py-1 rounded transition-colors ${
                    speed === s
                      ? "bg-amber-400 text-gray-900 font-medium"
                      : "bg-white/10 text-gray-300 hover:bg-white/20"
                  }`}
                >
                  {s}s
                </button>
              ))}
            </div>
          </div>

          {/* Center: slide counter */}
          <span className="text-xs text-gray-400">
            {currentIndex + 1} / {slides.length}
          </span>

          {/* Right: exit button */}
          <Link
            href="/"
            className="text-sm text-gray-300 hover:text-white px-3 py-1.5 rounded bg-white/10 hover:bg-white/20 transition-colors"
          >
            ✕ {t("story.exit")}
          </Link>
        </div>
      </div>
    </div>
  );
}

/* ------------------------------------------------------------------ */
/* Page wrapper with Suspense (required for useSearchParams)           */
/* ------------------------------------------------------------------ */
export default function StoryPage() {
  return (
    <Suspense
      fallback={
        <div className="fixed inset-0 bg-gray-900 flex items-center justify-center z-50">
          <p className="text-gray-300 text-lg">Loading...</p>
        </div>
      }
    >
      <StoryPageContent />
    </Suspense>
  );
}

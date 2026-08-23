"""Duplicate detection and deterministic person-merge planning."""

from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from copy import deepcopy
from datetime import datetime
from typing import Any

import networkx as nx

from tree_validation import (
    TreeValidationError,
    ValidationIssue,
    enforce_issues,
    validate_person_dates,
    validate_relationship,
)


_COLLECTION_FIELDS = {"pictures", "tags"}
_INTERNAL_FIELDS = {"notes_json"}


def _normalize(value: Any) -> str:
    text = unicodedata.normalize("NFKD", str(value or ""))
    return "".join(char for char in text.casefold() if char.isalnum())


def _normalized_date(value: Any) -> str:
    text = str(value or "").strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y-%m", "%Y"):
        try:
            parsed = datetime.strptime(text, fmt)
            if fmt == "%Y":
                return f"{parsed.year:04d}"
            if fmt == "%Y-%m":
                return f"{parsed.year:04d}-{parsed.month:02d}"
            return parsed.date().isoformat()
        except ValueError:
            continue
    return _normalize(text)


def _names(person: dict[str, Any]) -> set[str]:
    values = {
        _normalize(f"{person.get('firstname', '')} {person.get('lastname', '')}"),
        _normalize(f"{person.get('lastname', '')} {person.get('firstname', '')}"),
    }
    aliases = re.split(r"[,;/|\n]+", str(person.get("alias") or ""))
    values.update(_normalize(alias) for alias in aliases)
    return {value for value in values if value}


def duplicate_suggestions(
    graph,
    candidate: dict[str, Any],
    *,
    person_id: str | None = None,
    relative_ids: list[str] | None = None,
) -> list[dict[str, Any]]:
    candidate_names = _names(candidate)
    candidate_relatives = set(relative_ids or [])
    if person_id in graph:
        candidate_relatives.update(graph.predecessors(person_id))
        candidate_relatives.update(graph.successors(person_id))

    suggestions = []
    for other_id, other in graph.nodes(data=True):
        if other_id == person_id:
            continue
        reasons: list[dict[str, Any]] = []
        score = 0
        overlap = candidate_names & _names(other)
        full_name = _normalize(
            f"{candidate.get('firstname', '')} {candidate.get('lastname', '')}"
        )
        other_full_name = _normalize(
            f"{other.get('firstname', '')} {other.get('lastname', '')}"
        )
        if full_name and full_name == other_full_name:
            score += 60
            reasons.append({"code": "normalized_name", "value": full_name})
        elif overlap:
            score += 45
            reasons.append(
                {"code": "name_alias_overlap", "value": sorted(overlap)[0]}
            )
        for field in ("birthdate", "deathdate"):
            left = _normalized_date(candidate.get(field))
            right = _normalized_date(other.get(field))
            if left and left == right:
                score += 20 if field == "birthdate" else 10
                reasons.append({"code": f"matching_{field}", "value": left})
        other_relatives = set(graph.predecessors(other_id)) | set(
            graph.successors(other_id)
        )
        shared = sorted(candidate_relatives & other_relatives)
        if shared:
            score += min(20, 10 + 2 * (len(shared) - 1))
            reasons.append({"code": "shared_close_relatives", "person_ids": shared})
        has_name_match = bool(overlap or (full_name and full_name == other_full_name))
        has_date_and_relative = (
            any(reason["code"].startswith("matching_") for reason in reasons)
            and bool(shared)
        )
        if reasons and (has_name_match or has_date_and_relative):
            suggestions.append(
                {
                    "person_id": other_id,
                    "fullname": (
                        f"{other.get('firstname', '')} {other.get('lastname', '')}"
                    ).strip(),
                    "score": min(score, 100),
                    "reasons": reasons,
                }
            )
    return sorted(suggestions, key=lambda item: (-item["score"], item["person_id"]))


def _dedupe(values: list[Any]) -> list[Any]:
    result = []
    seen = set()
    for value in values:
        key = json.dumps(value, sort_keys=True, default=str)
        if key not in seen:
            seen.add(key)
            result.append(deepcopy(value))
    return result


def _notes(attributes: dict[str, Any]) -> list[Any]:
    raw = attributes.get("notes_json", "[]")
    if isinstance(raw, str):
        try:
            value = json.loads(raw)
        except (TypeError, ValueError):
            return [raw] if raw else []
    else:
        value = raw
    if isinstance(value, list):
        return value
    return [value] if value not in (None, "") else []


def _merge_token(
    graph,
    source_id: str,
    target_id: str,
    field_choices: dict[str, str],
    relationship_choices: dict[str, str],
) -> str:
    payload = {
        "nodes": [
            [node_id, dict(attributes)]
            for node_id, attributes in sorted(graph.nodes(data=True))
        ],
        "edges": [
            [source, target, dict(attributes)]
            for source, target, attributes in sorted(graph.edges(data=True))
        ],
        "source_id": source_id,
        "target_id": target_id,
        "field_choices": field_choices,
        "relationship_choices": relationship_choices,
    }
    serialized = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str)
    return hashlib.sha256(serialized.encode("utf-8")).hexdigest()


def _edge_key(source: str, target: str) -> str:
    return f"{source}->{target}"


def build_merge_preview(
    tree,
    source_id: str,
    target_id: str,
    *,
    field_choices: dict[str, str] | None = None,
    relationship_choices: dict[str, str] | None = None,
) -> dict[str, Any]:
    if source_id == target_id:
        raise ValueError("Source and target must be different people")
    if source_id not in tree.graph or target_id not in tree.graph:
        raise ValueError("Source and target persons must exist")
    field_choices = field_choices or {}
    relationship_choices = relationship_choices or {}
    source_attrs = dict(tree.graph.nodes[source_id])
    target_attrs = dict(tree.graph.nodes[target_id])
    conflicts = []
    retained: dict[str, Any] = {}
    all_fields = sorted(set(source_attrs) | set(target_attrs))
    for field in all_fields:
        if field in _COLLECTION_FIELDS or field in _INTERNAL_FIELDS:
            continue
        source_value = source_attrs.get(field)
        target_value = target_attrs.get(field)
        if source_value not in (None, "") and target_value not in (None, "") and source_value != target_value:
            choice = field_choices.get(field)
            conflicts.append(
                {
                    "field": field,
                    "source": source_value,
                    "target": target_value,
                    "choice": choice,
                    "resolved": choice in {"source", "target"},
                }
            )
            if choice in {"source", "target"}:
                retained[field] = source_value if choice == "source" else target_value
        else:
            retained[field] = target_value if target_value not in (None, "") else source_value

    for field in _COLLECTION_FIELDS:
        values = []
        for attrs in (target_attrs, source_attrs):
            value = attrs.get(field, [])
            values.extend(value if isinstance(value, list) else ([value] if value else []))
        if values:
            retained[field] = _dedupe(values)
    profile_pictures = [
        value
        for value in (target_attrs.get("profilepic"), source_attrs.get("profilepic"))
        if value
    ]
    if profile_pictures:
        retained["pictures"] = _dedupe(
            [*retained.get("pictures", []), *profile_pictures]
        )
    notes = _dedupe([*_notes(target_attrs), *_notes(source_attrs)])
    if notes:
        retained["notes_json"] = json.dumps(notes)

    repointed = []
    removed_self_links = []
    deduplicated = []
    relationship_conflicts = []
    final_edges: dict[tuple[str, str], dict[str, Any]] = {}
    for old_source, old_target, attributes in tree.graph.edges(data=True):
        new_source = target_id if old_source == source_id else old_source
        new_target = target_id if old_target == source_id else old_target
        edge = {
            "from": {"source": old_source, "target": old_target},
            "to": {"source": new_source, "target": new_target},
            "attributes": deepcopy(dict(attributes)),
        }
        if new_source == new_target:
            removed_self_links.append(edge)
            continue
        pair = (new_source, new_target)
        origin = "source" if source_id in (old_source, old_target) else "target"
        if pair not in final_edges:
            final_edges[pair] = edge["attributes"]
        elif final_edges[pair] == edge["attributes"]:
            deduplicated.append(edge)
        else:
            key = _edge_key(*pair)
            choice = relationship_choices.get(key)
            relationship_conflicts.append(
                {
                    "key": key,
                    "source": edge["attributes"] if origin == "source" else final_edges[pair],
                    "target": final_edges[pair] if origin == "source" else edge["attributes"],
                    "choice": choice,
                    "resolved": choice in {"source", "target"},
                }
            )
            if choice == origin:
                final_edges[pair] = edge["attributes"]
        if old_source != new_source or old_target != new_target:
            repointed.append(edge)

    return {
        "source_id": source_id,
        "target_id": target_id,
        "preview_token": _merge_token(
            tree.graph,
            source_id,
            target_id,
            field_choices,
            relationship_choices,
        ),
        "field_conflicts": conflicts,
        "relationship_conflicts": relationship_conflicts,
        "retained": retained,
        "notes": notes,
        "pictures": retained.get("pictures", []),
        "relationships": {
            "repointed": repointed,
            "self_links_removed": removed_self_links,
            "duplicates_removed": deduplicated,
            "final": [
                {"source": pair[0], "target": pair[1], "attributes": attrs}
                for pair, attrs in final_edges.items()
                if target_id in pair
            ],
        },
    }


def execute_merge(
    tree,
    preview: dict[str, Any],
    *,
    preview_token: str | None,
    override_warnings: bool,
) -> None:
    if preview_token and preview_token != preview["preview_token"]:
        raise RuntimeError("The merge plan changed after preview; generate a new preview")
    unresolved = [
        conflict
        for key in ("field_conflicts", "relationship_conflicts")
        for conflict in preview[key]
        if not conflict["resolved"]
    ]
    if unresolved:
        raise ValueError("Every merge conflict requires an explicit choice")

    source_id = preview["source_id"]
    target_id = preview["target_id"]
    staged = tree.graph.copy()
    staged.remove_node(source_id)
    staged.nodes[target_id].clear()
    staged.nodes[target_id].update(deepcopy(preview["retained"]))
    for source, target in list(staged.edges()):
        if source == target_id or target == target_id:
            staged.remove_edge(source, target)
    for edge in preview["relationships"]["final"]:
        staged.add_edge(edge["source"], edge["target"], **deepcopy(edge["attributes"]))

    issues = validate_person_dates(dict(staged.nodes[target_id]), target_id)
    for source, target, attributes in staged.edges(data=True):
        if source == target:
            raise ValueError("Merge would create a self relationship")
        if target_id not in (source, target):
            continue
        relationship_type = attributes.get("type", "")
        if tree.relationship_schema and not tree.relationship_schema.is_valid_type(
            relationship_type
        ):
            raise ValueError(f"Invalid relationship type '{relationship_type}'")
        issues.extend(
            validate_relationship(
                staged,
                source,
                target,
                relationship_type,
                start_date=attributes.get("start_date"),
                end_date=attributes.get("end_date"),
                check_structure=False,
            )
        )
    ancestry = nx.DiGraph(
        (source, target)
        for source, target, attrs in staged.edges(data=True)
        if attrs.get("type") == "isChildOf" and attrs.get("is_active", True)
    )
    if not nx.is_directed_acyclic_graph(ancestry):
        raise TreeValidationError(
            [
                ValidationIssue(
                    code="parent_child_cycle",
                    severity="error",
                    message="The merge would create an ancestry cycle.",
                    person_ids=(source_id, target_id),
                )
            ]
        )
    enforce_issues(issues, override_warnings=override_warnings)
    tree.graph = staged

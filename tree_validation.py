"""Family-tree integrity and chronology validation."""

from __future__ import annotations

import calendar
import re
from dataclasses import dataclass
from datetime import date
from typing import Any, Iterable, Literal

import networkx as nx


Severity = Literal["error", "warning"]


@dataclass(frozen=True)
class ValidationIssue:
    code: str
    severity: Severity
    message: str
    field: str | None = None
    person_ids: tuple[str, ...] = ()

    def to_dict(self) -> dict[str, Any]:
        result: dict[str, Any] = {
            "code": self.code,
            "severity": self.severity,
            "message": self.message,
            "person_ids": list(self.person_ids),
        }
        if self.field:
            result["field"] = self.field
        return result


class TreeValidationError(ValueError):
    def __init__(self, issues: Iterable[ValidationIssue]) -> None:
        self.issues = tuple(issues)
        super().__init__("; ".join(issue.message for issue in self.issues))

    def to_detail(self) -> dict[str, Any]:
        has_errors = any(issue.severity == "error" for issue in self.issues)
        return {
            "code": "validation_error" if has_errors else "validation_warning",
            "message": (
                "The requested change contains validation errors."
                if has_errors
                else "The requested change has chronology warnings."
            ),
            "issues": [issue.to_dict() for issue in self.issues],
        }


@dataclass(frozen=True)
class DateBounds:
    earliest: date
    latest: date


_YEAR = re.compile(r"^(\d{4})$")
_YEAR_MONTH = re.compile(r"^(\d{4})-(\d{2})$")
_ISO_DATE = re.compile(r"^(\d{4})-(\d{2})-(\d{2})$")
_DAY_FIRST_DATE = re.compile(r"^(\d{2})/(\d{2})/(\d{4})$")


def _person_name(graph, person_id: str, overrides: dict[str, dict[str, Any]] | None = None) -> str:
    data = (overrides or {}).get(person_id)
    if data is None and person_id in graph:
        data = graph.nodes[person_id]
    if data:
        fullname = f"{data.get('firstname', '')} {data.get('lastname', '')}".strip()
        if fullname:
            return fullname
    return person_id


def _parse_date(
    value: Any,
    *,
    field: str,
    person_ids: tuple[str, ...],
) -> tuple[DateBounds | None, list[ValidationIssue]]:
    if value is None or value == "":
        return None, []
    if not isinstance(value, str):
        return None, [
            ValidationIssue(
                code="invalid_date",
                severity="error",
                field=field,
                person_ids=person_ids,
                message=f"{field.replace('_', ' ').title()} must be a date string.",
            )
        ]

    value = value.strip()
    try:
        if match := _YEAR.fullmatch(value):
            year = int(match.group(1))
            return DateBounds(date(year, 1, 1), date(year, 12, 31)), []
        if match := _YEAR_MONTH.fullmatch(value):
            year, month = map(int, match.groups())
            last_day = calendar.monthrange(year, month)[1]
            return DateBounds(date(year, month, 1), date(year, month, last_day)), []
        if match := _ISO_DATE.fullmatch(value):
            parsed = date(*map(int, match.groups()))
            return DateBounds(parsed, parsed), []
        if match := _DAY_FIRST_DATE.fullmatch(value):
            day, month, year = map(int, match.groups())
            parsed = date(year, month, day)
            return DateBounds(parsed, parsed), []
    except ValueError:
        pass

    return None, [
        ValidationIssue(
            code="invalid_date",
            severity="error",
            field=field,
            person_ids=person_ids,
            message=(
                f"{field.replace('_', ' ').title()} must use YYYY, YYYY-MM, "
                "YYYY-MM-DD, or DD/MM/YYYY."
            ),
        )
    ]


def validate_person_dates(person: dict[str, Any], person_id: str = "") -> list[ValidationIssue]:
    person_ids = (person_id,) if person_id else ()
    birth, issues = _parse_date(
        person.get("birthdate"),
        field="birthdate",
        person_ids=person_ids,
    )
    death, death_issues = _parse_date(
        person.get("deathdate"),
        field="deathdate",
        person_ids=person_ids,
    )
    issues.extend(death_issues)

    if birth and death and birth.earliest > death.latest:
        issues.append(
            ValidationIssue(
                code="birth_after_death",
                severity="warning",
                field="deathdate",
                person_ids=person_ids,
                message="The death date is earlier than the birth date.",
            )
        )
    if death and person.get("isAlive") is True:
        issues.append(
            ValidationIssue(
                code="living_with_death_date",
                severity="warning",
                field="deathdate",
                person_ids=person_ids,
                message="This person is marked as living but has a death date.",
            )
        )
    return issues


def _event_lifetime_issues(
    graph,
    person_id: str,
    event: DateBounds,
    event_field: str,
    overrides: dict[str, dict[str, Any]] | None,
) -> list[ValidationIssue]:
    person = (overrides or {}).get(person_id)
    if person is None:
        person = dict(graph.nodes[person_id])
    name = _person_name(graph, person_id, overrides)
    birth, _ = _parse_date(
        person.get("birthdate"),
        field="birthdate",
        person_ids=(person_id,),
    )
    death, _ = _parse_date(
        person.get("deathdate"),
        field="deathdate",
        person_ids=(person_id,),
    )
    # Existing imported data may contain legacy date formats. Only validate
    # fields being written; malformed lifetime data is ignored for event checks.
    issues: list[ValidationIssue] = []
    if birth and event.latest < birth.earliest:
        issues.append(
            ValidationIssue(
                code="event_before_birth",
                severity="warning",
                field=event_field,
                person_ids=(person_id,),
                message=f"The {event_field.replace('_', ' ')} is before {name}'s birth.",
            )
        )
    if death and event.earliest > death.latest:
        issues.append(
            ValidationIssue(
                code="event_after_death",
                severity="warning",
                field=event_field,
                person_ids=(person_id,),
                message=f"The {event_field.replace('_', ' ')} is after {name}'s death.",
            )
        )
    return issues


def validate_relationship(
    graph,
    source: str,
    target: str,
    relationship_type: str,
    *,
    start_date: Any = None,
    end_date: Any = None,
    overrides: dict[str, dict[str, Any]] | None = None,
    check_structure: bool = True,
) -> list[ValidationIssue]:
    issues: list[ValidationIssue] = []
    person_ids = (source, target)

    if check_structure:
        missing = [person_id for person_id in person_ids if person_id not in graph]
        if missing:
            return [
                ValidationIssue(
                    code="person_not_found",
                    severity="error",
                    person_ids=tuple(missing),
                    message="Every person in a relationship must exist.",
                )
            ]
        if source == target:
            issues.append(
                ValidationIssue(
                    code="self_relationship",
                    severity="error",
                    person_ids=(source,),
                    message="A person cannot have a relationship with themselves.",
                )
            )
        if graph.has_edge(source, target):
            existing_type = graph[source][target].get("type")
            issues.append(
                ValidationIssue(
                    code=(
                        "duplicate_relationship"
                        if existing_type == relationship_type
                        else "relationship_conflict"
                    ),
                    severity="error",
                    person_ids=person_ids,
                    message=(
                        "This relationship already exists."
                        if existing_type == relationship_type
                        else f"These people already have a {existing_type} relationship."
                    ),
                )
            )
        if relationship_type == "isChildOf" and source != target:
            ancestry = nx.DiGraph()
            ancestry.add_nodes_from(graph.nodes)
            ancestry.add_edges_from(
                (child, parent)
                for child, parent, data in graph.edges(data=True)
                if data.get("type") == "isChildOf" and data.get("is_active", True)
            )
            if source in ancestry and target in ancestry:
                creates_cycle = nx.has_path(ancestry, target, source)
                if creates_cycle:
                    issues.append(
                        ValidationIssue(
                            code="parent_child_cycle",
                            severity="error",
                            person_ids=person_ids,
                            message="This parent/child relationship would create an ancestry cycle.",
                        )
                    )

    start, start_issues = _parse_date(
        start_date,
        field="start_date",
        person_ids=person_ids,
    )
    end, end_issues = _parse_date(
        end_date,
        field="end_date",
        person_ids=person_ids,
    )
    issues.extend(start_issues)
    issues.extend(end_issues)
    if start and end and start.earliest > end.latest:
        issues.append(
            ValidationIssue(
                code="relationship_end_before_start",
                severity="warning",
                field="end_date",
                person_ids=person_ids,
                message="The relationship end date is earlier than its start date.",
            )
        )
    for event, field in ((start, "start_date"), (end, "end_date")):
        if event:
            for person_id in person_ids:
                if person_id in graph or person_id in (overrides or {}):
                    issues.extend(
                        _event_lifetime_issues(graph, person_id, event, field, overrides)
                    )

    if relationship_type == "isChildOf":
        child = (overrides or {}).get(source)
        parent = (overrides or {}).get(target)
        if child is None and source in graph:
            child = dict(graph.nodes[source])
        if parent is None and target in graph:
            parent = dict(graph.nodes[target])
        if child is not None and parent is not None:
            child_birth, _ = _parse_date(
                child.get("birthdate"),
                field="birthdate",
                person_ids=(source,),
            )
            parent_birth, _ = _parse_date(
                parent.get("birthdate"),
                field="birthdate",
                person_ids=(target,),
            )
            parent_death, _ = _parse_date(
                parent.get("deathdate"),
                field="deathdate",
                person_ids=(target,),
            )
            child_name = _person_name(graph, source, overrides)
            parent_name = _person_name(graph, target, overrides)
            if child_birth and parent_birth:
                oldest_possible = child_birth.latest.year - parent_birth.earliest.year
                youngest_possible = child_birth.earliest.year - parent_birth.latest.year
                if oldest_possible < 12:
                    issues.append(
                        ValidationIssue(
                            code="parent_too_young",
                            severity="warning",
                            field="birthdate",
                            person_ids=person_ids,
                            message=f"{parent_name} appears to be under 12 when {child_name} was born.",
                        )
                    )
                elif youngest_possible > 80:
                    issues.append(
                        ValidationIssue(
                            code="parent_age_implausible",
                            severity="warning",
                            field="birthdate",
                            person_ids=person_ids,
                            message=f"{parent_name} appears to be over 80 when {child_name} was born.",
                        )
                    )
            if child_birth and parent_death and child_birth.earliest > parent_death.latest:
                issues.append(
                    ValidationIssue(
                        code="child_born_after_parent_death",
                        severity="warning",
                        field="birthdate",
                        person_ids=person_ids,
                        message=f"{child_name} was born after {parent_name}'s recorded death.",
                    )
                )
    return _deduplicate_issues(issues)


def validate_person_relationships(
    graph,
    person_id: str,
    prospective_person: dict[str, Any],
) -> list[ValidationIssue]:
    overrides = {person_id: prospective_person}
    issues = validate_person_dates(prospective_person, person_id)
    for source, target, data in graph.in_edges(person_id, data=True):
        issues.extend(
            validate_relationship(
                graph,
                source,
                target,
                data.get("type", ""),
                start_date=data.get("start_date"),
                end_date=data.get("end_date"),
                overrides=overrides,
                check_structure=False,
            )
        )
    for source, target, data in graph.out_edges(person_id, data=True):
        issues.extend(
            validate_relationship(
                graph,
                source,
                target,
                data.get("type", ""),
                start_date=data.get("start_date"),
                end_date=data.get("end_date"),
                overrides=overrides,
                check_structure=False,
            )
        )
    return _deduplicate_issues(issues)


def enforce_issues(
    issues: Iterable[ValidationIssue],
    *,
    override_warnings: bool,
) -> None:
    issues = tuple(issues)
    if any(issue.severity == "error" for issue in issues):
        raise TreeValidationError(issues)
    if issues and not override_warnings:
        raise TreeValidationError(issues)


def _deduplicate_issues(issues: Iterable[ValidationIssue]) -> list[ValidationIssue]:
    unique: list[ValidationIssue] = []
    seen: set[tuple[str, str | None, tuple[str, ...], str]] = set()
    for issue in issues:
        key = (issue.code, issue.field, issue.person_ids, issue.message)
        if key not in seen:
            seen.add(key)
            unique.append(issue)
    return unique

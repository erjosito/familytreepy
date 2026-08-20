#!/usr/bin/env python3
"""CLI tool to interact with the Family Tree directly.

Usage examples:
  python cli.py list
  python cli.py show "Alba Farell Torres"
  python cli.py add --firstname Ana --lastname Garcia --birthdate 1990-05-20
  python cli.py edit "Alba Farell Torres" --birthplace Barcelona
  python cli.py delete "Alba Farell Torres"
  python cli.py add-rel "Alba Farell Torres" "Lucas Moreno Torres" isChildOf
  python cli.py deactivate-rel "Person A" "Person B"
  python cli.py reactivate-rel "Person A" "Person B"
  python cli.py tree "Alba Farell Torres" --degree 3
  python cli.py info
  python cli.py activate-all
"""

import argparse
import json
import os
import sys

from familytree import FamilyTree


def _build_tree(args: argparse.Namespace) -> FamilyTree:
    """Instantiate a FamilyTree from CLI args / env vars."""
    backend = args.backend or os.getenv("TREE_BACKEND", "local")
    if backend == "local":
        localfile = args.file or os.getenv("TREE_LOCAL_FILE", "familytree.gml")
        return FamilyTree(backend="local", localfile=localfile, verbose=args.verbose)
    elif backend == "azstorage":
        return FamilyTree(
            backend="azstorage",
            azstorage_account=args.az_account or os.getenv("AZURE_STORAGE_ACCOUNT"),
            azstorage_key=args.az_key or os.getenv("AZURE_STORAGE_KEY"),
            azstorage_container=args.az_container or os.getenv("AZURE_STORAGE_CONTAINER", "familytreejson"),
            azstorage_blob=args.az_blob or os.getenv("AZURE_STORAGE_BLOB", "familytree.gml"),
            verbose=args.verbose,
        )
    else:
        print(f"Error: unsupported backend '{backend}'", file=sys.stderr)
        sys.exit(1)


def _resolve_person(tree: FamilyTree, name_or_id: str) -> str:
    """Resolve a full name or raw ID to a person ID."""
    # Try direct ID match first
    if tree.get_person(name_or_id) is not None:
        return name_or_id
    # Try full name lookup
    pid = tree.get_person_by_full_name(name_or_id)
    if pid is not None:
        return pid
    print(f"Error: person '{name_or_id}' not found.", file=sys.stderr)
    sys.exit(1)


def _fullname(tree: FamilyTree, pid: str) -> str:
    data = tree.get_person(pid)
    if data is None:
        return pid
    return (data.get("firstname", "") + " " + data.get("lastname", "")).strip() or pid


# ── Commands ──────────────────────────────────────────────────────────────

def cmd_list(tree: FamilyTree, args: argparse.Namespace) -> None:
    """List all persons."""
    persons = []
    for pid in tree.graph.nodes():
        data = tree.graph.nodes[pid]
        fullname = (data.get("firstname", "") + " " + data.get("lastname", "")).strip()
        persons.append((pid, fullname))
    persons.sort(key=lambda x: x[1].lower())
    print(f"{'Name':<40} {'ID'}")
    print("-" * 80)
    for pid, name in persons:
        print(f"{name or '(no name)':<40} {pid}")
    print(f"\nTotal: {len(persons)} persons")


def cmd_show(tree: FamilyTree, args: argparse.Namespace) -> None:
    """Show details for a person."""
    pid = _resolve_person(tree, args.person)
    data = dict(tree.get_person(pid))
    fullname = _fullname(tree, pid)

    print(f"{'─' * 50}")
    print(f"  {fullname}  (ID: {pid})")
    print(f"{'─' * 50}")

    skip = {"firstname", "lastname", "graphics"}
    for key, value in sorted(data.items()):
        if key in skip or (isinstance(value, dict)):
            continue
        print(f"  {key:<20} {value}")

    # Relationships
    rels = tree.get_relationships(pid, include_inactive=True)
    if rels:
        print(f"\n  Relationships:")
        for r in rels:
            other_id = r["target"] if r["source"] == pid else r["source"]
            other_name = _fullname(tree, other_id)
            rtype = r.get("type", "?")
            if rtype == "isChildOf":
                label = "Parent" if r["source"] == pid else "Child"
            elif rtype == "isSpouseOf":
                label = "Spouse"
            else:
                label = rtype
            active = r.get("is_active", True)
            status = "" if active else " (inactive)"
            print(f"    {label:<12} {other_name}{status}")

    # Siblings
    try:
        siblings = tree.get_siblings(pid)
        if siblings:
            print(f"\n  Siblings:")
            for s in siblings:
                print(f"    {_fullname(tree, s)}")
    except ValueError:
        pass
    print()


def cmd_add(tree: FamilyTree, args: argparse.Namespace) -> None:
    """Add a new person."""
    attrs = {}
    if args.firstname:
        attrs["firstname"] = args.firstname
    if args.lastname:
        attrs["lastname"] = args.lastname
    if args.birthdate:
        attrs["birthdate"] = args.birthdate
    if args.birthplace:
        attrs["birthplace"] = args.birthplace
    if args.id:
        attrs["id"] = args.id

    pid = tree.add_person(
        override_warnings=args.override_warnings,
        **attrs,
    )
    name = (attrs.get("firstname", "") + " " + attrs.get("lastname", "")).strip()
    print(f"Created: {name or '(no name)'}  (ID: {pid})")


def cmd_edit(tree: FamilyTree, args: argparse.Namespace) -> None:
    """Edit a person's attributes."""
    pid = _resolve_person(tree, args.person)
    attrs = {}
    if args.firstname is not None:
        attrs["firstname"] = args.firstname
    if args.lastname is not None:
        attrs["lastname"] = args.lastname
    if args.birthdate is not None:
        attrs["birthdate"] = args.birthdate
    if args.birthplace is not None:
        attrs["birthplace"] = args.birthplace
    if not attrs:
        print("No attributes to update. Use --firstname, --lastname, --birthdate, --birthplace.")
        return
    tree.update_person(
        pid,
        override_warnings=args.override_warnings,
        **attrs,
    )
    print(f"Updated: {_fullname(tree, pid)}")


def cmd_delete(tree: FamilyTree, args: argparse.Namespace) -> None:
    """Delete a person."""
    pid = _resolve_person(tree, args.person)
    name = _fullname(tree, pid)
    if not args.yes:
        confirm = input(f"Delete '{name}'? [y/N] ")
        if confirm.lower() != "y":
            print("Cancelled.")
            return
    tree.delete_person(pid)
    print(f"Deleted: {name}")


def cmd_add_rel(tree: FamilyTree, args: argparse.Namespace) -> None:
    """Add a relationship."""
    pid1 = _resolve_person(tree, args.person1)
    pid2 = _resolve_person(tree, args.person2)
    rel_type = args.type
    tree.add_relationship(
        pid1,
        pid2,
        type=rel_type,
        start_date=args.start_date,
        override_warnings=args.override_warnings,
    )
    print(f"Created: {_fullname(tree, pid1)} --[{rel_type}]--> {_fullname(tree, pid2)}")

    # For spouse, also create reverse edge
    if rel_type == "isSpouseOf":
        tree.add_relationship(
            pid2,
            pid1,
            type=rel_type,
            start_date=args.start_date,
            override_warnings=args.override_warnings,
        )
        print(f"Created: {_fullname(tree, pid2)} --[{rel_type}]--> {_fullname(tree, pid1)}")


def cmd_deactivate_rel(tree: FamilyTree, args: argparse.Namespace) -> None:
    """Deactivate a relationship."""
    pid1 = _resolve_person(tree, args.person1)
    pid2 = _resolve_person(tree, args.person2)
    tree.deactivate_relationship(
        pid1,
        pid2,
        end_date=args.end_date,
        override_warnings=args.override_warnings,
    )
    print(f"Deactivated: {_fullname(tree, pid1)} <-> {_fullname(tree, pid2)}")


def cmd_reactivate_rel(tree: FamilyTree, args: argparse.Namespace) -> None:
    """Reactivate a relationship."""
    pid1 = _resolve_person(tree, args.person1)
    pid2 = _resolve_person(tree, args.person2)
    tree.reactivate_relationship(pid1, pid2)
    print(f"Reactivated: {_fullname(tree, pid1)} <-> {_fullname(tree, pid2)}")


def cmd_activate_all(tree: FamilyTree, args: argparse.Namespace) -> None:
    """Activate all relationships."""
    tree.activate_all_relationships()
    print("All relationships set to active.")


def cmd_tree(tree: FamilyTree, args: argparse.Namespace) -> None:
    """Show a text tree centered on a person."""
    pid = _resolve_person(tree, args.person)
    degree = args.degree

    subgraph = tree.get_subgraph_degrees(pid, degree=degree)
    tree.assign_generation_levels()

    # Group by level
    levels: dict[int, list[str]] = {}
    for nid in subgraph.nodes():
        lv = tree.graph.nodes[nid].get("level", 0)
        levels.setdefault(lv, []).append(nid)

    print(f"\nTree centered on: {_fullname(tree, pid)} (degree={degree})")
    print(f"Nodes: {subgraph.number_of_nodes()}, Edges: {subgraph.number_of_edges()}\n")

    for lv in sorted(levels.keys()):
        names = [_fullname(tree, n) for n in levels[lv]]
        marker = ">>>" if pid in levels[lv] else "   "
        print(f"  {marker} Level {lv}: {', '.join(sorted(names))}")
    print()


def cmd_info(tree: FamilyTree, args: argparse.Namespace) -> None:
    """Show general tree statistics."""
    n_persons = tree.graph.number_of_nodes()
    n_edges = tree.graph.number_of_edges()
    n_active = sum(1 for _, _, d in tree.graph.edges(data=True) if d.get("is_active", True))
    n_inactive = n_edges - n_active
    longest = tree.get_longest_ancestor_chain()

    print(f"Persons:            {n_persons}")
    print(f"Relationships:      {n_edges} ({n_active} active, {n_inactive} inactive)")
    print(f"Longest ancestor chain: {longest}")

    # Relationship types breakdown
    types: dict[str, int] = {}
    for _, _, d in tree.graph.edges(data=True):
        t = d.get("type", "unknown")
        types[t] = types.get(t, 0) + 1
    if types:
        print(f"\nBy type:")
        for t, count in sorted(types.items()):
            print(f"  {t:<20} {count}")


def cmd_export(tree: FamilyTree, args: argparse.Namespace) -> None:
    """Export tree data as JSON."""
    root_id = None
    if args.person:
        root_id = _resolve_person(tree, args.person)
    data = tree.format_for_api(
        root_id=root_id,
        degree=args.degree if root_id else None,
        include_inactive=args.include_inactive,
    )
    print(json.dumps(data, indent=2, default=str))


# ── Argument parser ───────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Family Tree CLI — manage your genealogical tree from the terminal.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    # Global options
    parser.add_argument("--backend", choices=["local", "azstorage"], help="Storage backend (default: $TREE_BACKEND or 'local')")
    parser.add_argument("--file", help="Local GML file path (default: $TREE_LOCAL_FILE or 'familytree.gml')")
    parser.add_argument("--az-account", help="Azure Storage account name")
    parser.add_argument("--az-key", help="Azure Storage account key")
    parser.add_argument("--az-container", help="Azure Storage container (default: familytreejson)")
    parser.add_argument("--az-blob", help="Azure Storage blob name (default: familytree.gml)")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")

    sub = parser.add_subparsers(dest="command", required=True)

    # list
    sub.add_parser("list", help="List all persons")

    # show
    p = sub.add_parser("show", help="Show person details")
    p.add_argument("person", help="Person name or ID")

    # add
    p = sub.add_parser("add", help="Add a new person")
    p.add_argument("--id", help="Custom ID (default: auto-generated UUID)")
    p.add_argument("--firstname", help="First name")
    p.add_argument("--lastname", help="Last name")
    p.add_argument("--birthdate", help="Birth date (e.g. 1990-01-31)")
    p.add_argument("--birthplace", help="Birth place")
    p.add_argument("--override-warnings", action="store_true", help="Accept chronology warnings")

    # edit
    p = sub.add_parser("edit", help="Edit a person's attributes")
    p.add_argument("person", help="Person name or ID")
    p.add_argument("--firstname", help="First name")
    p.add_argument("--lastname", help="Last name")
    p.add_argument("--birthdate", help="Birth date")
    p.add_argument("--birthplace", help="Birth place")
    p.add_argument("--override-warnings", action="store_true", help="Accept chronology warnings")

    # delete
    p = sub.add_parser("delete", help="Delete a person")
    p.add_argument("person", help="Person name or ID")
    p.add_argument("--yes", "-y", action="store_true", help="Skip confirmation")

    # add-rel
    p = sub.add_parser("add-rel", help="Add a relationship")
    p.add_argument("person1", help="Source person name or ID")
    p.add_argument("person2", help="Target person name or ID")
    p.add_argument("type", choices=["isChildOf", "isSpouseOf"], help="Relationship type")
    p.add_argument("--start-date", help="Start date")
    p.add_argument("--override-warnings", action="store_true", help="Accept chronology warnings")

    # deactivate-rel
    p = sub.add_parser("deactivate-rel", help="Deactivate a relationship")
    p.add_argument("person1", help="Person 1 name or ID")
    p.add_argument("person2", help="Person 2 name or ID")
    p.add_argument("--end-date", help="End date")
    p.add_argument("--override-warnings", action="store_true", help="Accept chronology warnings")

    # reactivate-rel
    p = sub.add_parser("reactivate-rel", help="Reactivate a relationship")
    p.add_argument("person1", help="Person 1 name or ID")
    p.add_argument("person2", help="Person 2 name or ID")

    # activate-all
    sub.add_parser("activate-all", help="Set all relationships to active")

    # tree
    p = sub.add_parser("tree", help="Show tree centered on a person")
    p.add_argument("person", help="Person name or ID")
    p.add_argument("--degree", "-d", type=int, default=3, help="Degree of separation (default: 3)")

    # info
    sub.add_parser("info", help="Show tree statistics")

    # export
    p = sub.add_parser("export", help="Export tree data as JSON")
    p.add_argument("--person", help="Center on person (optional)")
    p.add_argument("--degree", "-d", type=int, default=3, help="Degree (with --person)")
    p.add_argument("--include-inactive", action="store_true", help="Include inactive relationships")

    args = parser.parse_args()
    tree = _build_tree(args)

    commands = {
        "list": cmd_list,
        "show": cmd_show,
        "add": cmd_add,
        "edit": cmd_edit,
        "delete": cmd_delete,
        "add-rel": cmd_add_rel,
        "deactivate-rel": cmd_deactivate_rel,
        "reactivate-rel": cmd_reactivate_rel,
        "activate-all": cmd_activate_all,
        "tree": cmd_tree,
        "info": cmd_info,
        "export": cmd_export,
    }

    commands[args.command](tree, args)


if __name__ == "__main__":
    main()

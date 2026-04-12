#!/usr/bin/env python3
"""CLI tool to search and import profiles from Geni.com into the Family Tree.

Usage:
  python geni_cli.py auth                          # Authenticate with Geni (opens browser)
  python geni_cli.py search "Torres Luna"          # Search for profiles
  python geni_cli.py profile 12345678              # View profile details
  python geni_cli.py family 12345678               # View immediate family
  python geni_cli.py import 12345678               # Import a profile into the tree
  python geni_cli.py import 12345678 --with-family # Import profile + immediate family
  python geni_cli.py lookup "Alba Farell Torres"   # Search tree person on Geni
"""

import argparse
import http.server
import json
import os
import sys
import threading
import webbrowser
from urllib.parse import urlencode, urlparse, parse_qs

import httpx

from familytree import FamilyTree

GENI_APP_KEY = os.getenv("GENI_APP_KEY", "")
GENI_APP_SECRET = os.getenv("GENI_APP_SECRET", "")
GENI_API_BASE = "https://www.geni.com/api"
GENI_AUTHORIZE_URL = "https://www.geni.com/platform/oauth/authorize"
GENI_TOKEN_URL = "https://www.geni.com/platform/oauth/request_token"
TOKEN_FILE = os.path.join(os.path.expanduser("~"), ".geni_token")
CALLBACK_PORT = 8765


# ── Token management ─────────────────────────────────────────────────────

def _save_token(token: str) -> None:
    with open(TOKEN_FILE, "w") as f:
        f.write(token)
    print(f"Token saved to {TOKEN_FILE}")


def _load_token() -> str:
    if not os.path.exists(TOKEN_FILE):
        print("Not authenticated. Run: python geni_cli.py auth")
        sys.exit(1)
    with open(TOKEN_FILE) as f:
        return f.read().strip()


def _api_get(path: str, params: dict | None = None) -> dict:
    token = _load_token()
    p = {"access_token": token, **(params or {})}
    resp = httpx.get(f"{GENI_API_BASE}{path}", params=p, timeout=15)
    if resp.status_code == 401:
        print("Token expired. Run: python geni_cli.py auth")
        sys.exit(1)
    if resp.status_code != 200:
        print(f"API error {resp.status_code}: {resp.text[:200]}")
        sys.exit(1)
    return resp.json()


# ── Tree helper ──────────────────────────────────────────────────────────

def _build_tree(args: argparse.Namespace) -> FamilyTree:
    backend = args.backend or os.getenv("TREE_BACKEND", "local")
    if backend == "local":
        localfile = args.file or os.getenv("TREE_LOCAL_FILE", "familytree.gml")
        return FamilyTree(backend="local", localfile=localfile)
    elif backend == "azstorage":
        return FamilyTree(
            backend="azstorage",
            azstorage_account=os.getenv("AZURE_STORAGE_ACCOUNT"),
            azstorage_key=os.getenv("AZURE_STORAGE_KEY"),
            azstorage_container=os.getenv("AZURE_STORAGE_CONTAINER", "familytreejson"),
            azstorage_blob=os.getenv("AZURE_STORAGE_BLOB", "familytree.gml"),
        )
    else:
        print(f"Unknown backend: {backend}")
        sys.exit(1)


# ── Data extraction helpers ──────────────────────────────────────────────

def _get_date(data: dict, prefix: str) -> str:
    date_obj = data.get(f"{prefix}_date", {})
    if isinstance(date_obj, dict):
        year = date_obj.get("year", "")
        month = date_obj.get("month", "")
        day = date_obj.get("day", "")
        if year:
            parts = [str(year)]
            if month:
                parts.append(str(month).zfill(2))
                if day:
                    parts.append(str(day).zfill(2))
            return "-".join(parts)
    elif isinstance(date_obj, str):
        return date_obj
    return ""


def _get_location(data: dict, prefix: str) -> str:
    loc = data.get(f"{prefix}_location", {})
    if isinstance(loc, dict):
        return loc.get("place_name", loc.get("city", ""))
    elif isinstance(loc, str):
        return loc
    return ""


def _extract_id(data: dict) -> str:
    geni_id = data.get("id", "")
    if isinstance(geni_id, str) and geni_id.startswith("profile-"):
        return geni_id.replace("profile-", "")
    return str(geni_id)


def _fullname(data: dict) -> str:
    return data.get("name", f"{data.get('first_name', '')} {data.get('last_name', '')}").strip()


# ── Commands ─────────────────────────────────────────────────────────────

def cmd_auth(args: argparse.Namespace) -> None:
    """Authenticate with Geni via OAuth (opens browser)."""
    if not GENI_APP_KEY or not GENI_APP_SECRET:
        print("Error: set GENI_APP_KEY and GENI_APP_SECRET environment variables")
        sys.exit(1)

    auth_code_holder = {"code": None}

    class CallbackHandler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            qs = parse_qs(urlparse(self.path).query)
            auth_code_holder["code"] = qs.get("code", [None])[0]
            self.send_response(200)
            self.send_header("Content-Type", "text/html")
            self.end_headers()
            self.wfile.write(b"<html><body><h2>Authenticated! You can close this tab.</h2></body></html>")

        def log_message(self, format, *a):
            pass  # suppress logs

    server = http.server.HTTPServer(("localhost", CALLBACK_PORT), CallbackHandler)
    redirect_uri = f"http://localhost:{CALLBACK_PORT}/callback"

    params = {
        "client_id": GENI_APP_KEY,
        "redirect_uri": redirect_uri,
    }
    auth_url = f"{GENI_AUTHORIZE_URL}?{urlencode(params)}"
    print(f"Opening browser for Geni authentication...")
    webbrowser.open(auth_url)

    print("Waiting for callback...")
    server.handle_request()
    server.server_close()

    code = auth_code_holder["code"]
    if not code:
        print("Error: no authorization code received")
        sys.exit(1)

    # Exchange code for token
    resp = httpx.get(GENI_TOKEN_URL, params={
        "client_id": GENI_APP_KEY,
        "client_secret": GENI_APP_SECRET,
        "code": code,
        "redirect_uri": redirect_uri,
    }, timeout=15)

    if resp.status_code != 200:
        print(f"Token exchange failed: {resp.status_code} {resp.text[:200]}")
        sys.exit(1)

    token = resp.json().get("access_token", "")
    if not token:
        print("Error: no access token in response")
        sys.exit(1)

    _save_token(token)
    print("✓ Authenticated successfully!")


def cmd_search(args: argparse.Namespace) -> None:
    """Search Geni profiles by name."""
    data = _api_get("/profile/search", {"names": args.names})
    results = data.get("results", [])

    if not results:
        print("No profiles found.")
        return

    print(f"\n{'ID':<15} {'Name':<35} {'Birth':<12} {'Death':<12}")
    print("-" * 75)
    for item in results:
        gid = _extract_id(item)
        name = _fullname(item)
        birth = _get_date(item, "birth")
        death = _get_date(item, "death")
        print(f"{gid:<15} {name:<35} {birth:<12} {death:<12}")
    print(f"\n{len(results)} results. Use 'profile <id>' for details.")


def cmd_profile(args: argparse.Namespace) -> None:
    """Show detailed profile info."""
    data = _api_get(f"/profile-{args.profile_id}")
    gid = _extract_id(data)
    name = _fullname(data)
    first = data.get("first_name", "")
    last = data.get("last_name", "")
    gender = data.get("gender", "")
    birth = _get_date(data, "birth")
    birth_loc = _get_location(data, "birth")
    death = _get_date(data, "death")
    death_loc = _get_location(data, "death")
    about = data.get("about_me", "")

    print(f"\n{'─' * 50}")
    print(f"  {name}  (Geni ID: {gid})")
    print(f"{'─' * 50}")
    if first:
        print(f"  {'First name':<20} {first}")
    if last:
        print(f"  {'Last name':<20} {last}")
    if gender:
        print(f"  {'Gender':<20} {gender}")
    if birth:
        print(f"  {'Born':<20} {birth}")
    if birth_loc:
        print(f"  {'Birthplace':<20} {birth_loc}")
    if death:
        print(f"  {'Died':<20} {death}")
    if death_loc:
        print(f"  {'Death place':<20} {death_loc}")
    if about:
        print(f"\n  About: {about[:200]}")
    print()


def cmd_family(args: argparse.Namespace) -> None:
    """Show immediate family of a profile."""
    data = _api_get(f"/profile-{args.profile_id}/immediate-family")
    nodes = data.get("nodes", {})
    focus_key = f"profile-{args.profile_id}"
    focus_node = nodes.get(focus_key, {})
    focus_name = _fullname(focus_node)

    print(f"\nFamily of: {focus_name} (Geni ID: {args.profile_id})\n")

    # Parse relationships from focus node's edges
    focus_edges = focus_node.get("edges", {})
    parents, spouses, children = [], [], []

    for node_key, rel_data in focus_edges.items():
        rel = rel_data.get("rel", "")
        node = nodes.get(node_key, {})
        nid = _extract_id(node)
        name = _fullname(node)
        entry = f"{name} (ID: {nid})"

        if rel == "child":
            parents.append(entry)
        elif rel == "partner":
            spouses.append(entry)
        elif rel == "parent":
            children.append(entry)

    for label, items in [("Parents", parents), ("Spouses", spouses), ("Children", children)]:
        print(f"  {label}:")
        if items:
            for item in items:
                print(f"    • {item}")
        else:
            print(f"    —")
    print()


def cmd_import(args: argparse.Namespace) -> None:
    """Import a Geni profile into the local tree."""
    tree = _build_tree(args)
    profile_id = args.profile_id

    # Check for duplicates
    for pid, pdata in tree.graph.nodes(data=True):
        if pdata.get("geni_profile_id") == profile_id:
            name = (pdata.get("firstname", "") + " " + pdata.get("lastname", "")).strip()
            print(f"Already imported: {name} (local ID: {pid})")
            return

    # Fetch profile
    data = _api_get(f"/profile-{profile_id}")
    person_id = _import_single(tree, data, profile_id)
    name = _fullname(data)
    print(f"✓ Imported: {name} (local ID: {person_id})")

    if args.with_family:
        fam_data = _api_get(f"/profile-{profile_id}/immediate-family")
        nodes = fam_data.get("nodes", {})
        focus_key = f"profile-{profile_id}"
        focus_edges = nodes.get(focus_key, {}).get("edges", {})

        for node_key, rel_data in focus_edges.items():
            rel = rel_data.get("rel", "")
            node = nodes.get(node_key, {})
            nid = _extract_id(node)

            # Skip if already imported
            existing = None
            for pid, pdata in tree.graph.nodes(data=True):
                if pdata.get("geni_profile_id") == nid:
                    existing = pid
                    break

            if existing:
                fam_pid = existing
                fam_name = _fullname(node)
                print(f"  Already exists: {fam_name} (local ID: {fam_pid})")
            else:
                fam_pid = _import_single(tree, node, nid)
                fam_name = _fullname(node)
                print(f"  ✓ Imported: {fam_name} (local ID: {fam_pid})")

            # Create relationship
            try:
                if rel == "child":
                    tree.add_relationship(person_id, fam_pid, type="isChildOf")
                    print(f"    → Parent: {fam_name}")
                elif rel == "partner":
                    tree.add_relationship(person_id, fam_pid, type="isSpouseOf")
                    tree.add_relationship(fam_pid, person_id, type="isSpouseOf")
                    print(f"    → Spouse: {fam_name}")
                elif rel == "parent":
                    tree.add_relationship(fam_pid, person_id, type="isChildOf")
                    print(f"    → Child: {fam_name}")
            except Exception as e:
                print(f"    ⚠ Relationship error: {e}")

    print(f"\nDone.")


def _import_single(tree: FamilyTree, data: dict, geni_id: str) -> str:
    """Import a single Geni profile into the tree."""
    attrs = {
        "firstname": data.get("first_name", data.get("name", "")),
        "lastname": data.get("last_name", ""),
        "geni_profile_id": geni_id,
    }
    birth = _get_date(data, "birth")
    if birth:
        attrs["birthdate"] = birth
    birth_loc = _get_location(data, "birth")
    if birth_loc:
        attrs["birthplace"] = birth_loc
    death = _get_date(data, "death")
    if death:
        attrs["deathdate"] = death
        attrs["isAlive"] = False
    else:
        attrs["isAlive"] = True
    return tree.add_person(**attrs)


def cmd_lookup(args: argparse.Namespace) -> None:
    """Search for a person from the tree on Geni."""
    tree = _build_tree(args)
    pid = tree.get_person_by_full_name(args.name)
    if pid is None:
        print(f"Person '{args.name}' not found in local tree.")
        sys.exit(1)
    data = tree.get_person(pid)
    name = (data.get("firstname", "") + " " + data.get("lastname", "")).strip()
    print(f"Looking up '{name}' on Geni...")

    results = _api_get("/profile/search", {"names": name})
    items = results.get("results", [])
    if not items:
        print("No matches found on Geni.")
        return

    print(f"\n{'ID':<15} {'Name':<35} {'Birth':<12} {'Death':<12}")
    print("-" * 75)
    for item in items:
        gid = _extract_id(item)
        gname = _fullname(item)
        birth = _get_date(item, "birth")
        death = _get_date(item, "death")
        print(f"{gid:<15} {gname:<35} {birth:<12} {death:<12}")
    print(f"\n{len(items)} results. Use 'import <id>' to import a match.")


# ── Main ─────────────────────────────────────────────────────────────────

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Geni.com CLI — search and import genealogical profiles.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )

    # Global options
    parser.add_argument("--backend", choices=["local", "azstorage"], help="Storage backend")
    parser.add_argument("--file", help="Local GML file path")

    sub = parser.add_subparsers(dest="command", required=True)

    # auth
    sub.add_parser("auth", help="Authenticate with Geni.com (opens browser)")

    # search
    p = sub.add_parser("search", help="Search Geni profiles by name")
    p.add_argument("names", help="Name(s) to search for")

    # profile
    p = sub.add_parser("profile", help="Show profile details")
    p.add_argument("profile_id", help="Geni profile ID")

    # family
    p = sub.add_parser("family", help="Show immediate family")
    p.add_argument("profile_id", help="Geni profile ID")

    # import
    p = sub.add_parser("import", help="Import a Geni profile into the tree")
    p.add_argument("profile_id", help="Geni profile ID")
    p.add_argument("--with-family", action="store_true", help="Also import immediate family")

    # lookup
    p = sub.add_parser("lookup", help="Search for a tree person on Geni")
    p.add_argument("name", help="Person full name in the local tree")

    args = parser.parse_args()

    commands = {
        "auth": cmd_auth,
        "search": cmd_search,
        "profile": cmd_profile,
        "family": cmd_family,
        "import": cmd_import,
        "lookup": cmd_lookup,
    }

    commands[args.command](args)


if __name__ == "__main__":
    main()

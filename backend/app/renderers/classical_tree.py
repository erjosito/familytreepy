"""Classical genealogical tree renderer.

Generates a top-down tree image styled as an antique genealogy chart on
aged parchment paper.  Person nodes are displayed inside ornate round
portrait frames (gold/bronze concentric rings).  Connecting lines use
warm sepia tones.  Names are rendered in dark brown and automatically
sized to avoid overlapping.  Uses only PIL — no external assets required.
"""

from __future__ import annotations

import io
import math
import random
import sys
from typing import Any

import networkx as nx
from PIL import Image, ImageDraw, ImageFilter, ImageFont

from backend.app.renderers.base import ImageRenderer
from backend.app.renderers.registry import RendererRegistry

# ---------------------------------------------------------------------------
# Colour palettes  (sepia / antique tones)
# ---------------------------------------------------------------------------

# Warm sepia palette for connecting lines (one per family)
_LINE_COLORS = [
    (120, 72, 42),
    (90, 65, 50),
    (110, 55, 35),
    (80, 80, 55),
    (100, 50, 60),
    (70, 85, 65),
    (95, 70, 70),
    (75, 60, 80),
    (110, 85, 50),
    (85, 75, 45),
]

# Muted palette for initials circles (when no profile pic)
_NODE_FILLS = [
    (180, 155, 120),
    (160, 140, 115),
    (170, 145, 105),
    (155, 135, 120),
    (175, 150, 110),
    (145, 130, 115),
    (165, 150, 125),
    (150, 140, 100),
]

# Frame ring colours (outer → inner): gold / bronze
_FRAME_RINGS = [
    (140, 115, 65),   # outer dark gold
    (185, 160, 95),   # mid gold
    (210, 185, 115),  # bright gold
    (185, 160, 95),   # mid gold
    (140, 115, 65),   # inner dark gold
]

_TEXT_COLOR = (60, 40, 20)          # dark brown
_PARCHMENT_BASE = (225, 210, 180)   # warm parchment
_PARCHMENT_DARK = (195, 175, 145)   # darker parchment for vignette

# ---------------------------------------------------------------------------
# Color schemes
# ---------------------------------------------------------------------------

_COLOR_SCHEMES = {
    "sepia": {
        "line_colors": _LINE_COLORS,
        "node_fills": _NODE_FILLS,
        "frame_rings": _FRAME_RINGS,
        "text_color": (60, 40, 20),
        "parchment_base": (225, 210, 180),
        "parchment_dark": (195, 175, 145),
    },
    "blue": {
        "line_colors": [
            (42, 72, 120), (50, 65, 110), (35, 55, 100),
            (55, 80, 130), (60, 50, 100), (65, 85, 120),
            (70, 70, 115), (80, 60, 105), (50, 85, 110), (45, 75, 100),
        ],
        "node_fills": [
            (140, 170, 210), (130, 160, 200), (150, 175, 205),
            (125, 155, 195), (145, 165, 200), (135, 150, 190),
            (140, 160, 195), (130, 155, 185),
        ],
        "frame_rings": [
            (65, 105, 160), (90, 135, 190), (110, 155, 210),
            (90, 135, 190), (65, 105, 160),
        ],
        "text_color": (20, 30, 60),
        "parchment_base": (220, 230, 240),
        "parchment_dark": (185, 200, 215),
    },
    "green": {
        "line_colors": [
            (42, 100, 55), (50, 90, 60), (35, 85, 50),
            (55, 105, 65), (60, 80, 55), (45, 95, 70),
            (70, 90, 60), (55, 85, 75), (50, 100, 65), (65, 95, 55),
        ],
        "node_fills": [
            (150, 190, 155), (140, 180, 145), (155, 185, 150),
            (135, 175, 140), (145, 185, 150), (140, 170, 145),
            (150, 180, 155), (135, 175, 135),
        ],
        "frame_rings": [
            (65, 120, 70), (90, 150, 95), (110, 170, 115),
            (90, 150, 95), (65, 120, 70),
        ],
        "text_color": (20, 45, 25),
        "parchment_base": (225, 235, 220),
        "parchment_dark": (190, 205, 185),
    },
    "grayscale": {
        "line_colors": [
            (80, 80, 80), (90, 90, 90), (70, 70, 70),
            (100, 100, 100), (85, 85, 85), (75, 75, 75),
            (95, 95, 95), (65, 65, 65), (105, 105, 105), (60, 60, 60),
        ],
        "node_fills": [
            (180, 180, 180), (170, 170, 170), (175, 175, 175),
            (165, 165, 165), (185, 185, 185), (160, 160, 160),
            (178, 178, 178), (168, 168, 168),
        ],
        "frame_rings": [
            (100, 100, 100), (140, 140, 140), (170, 170, 170),
            (140, 140, 140), (100, 100, 100),
        ],
        "text_color": (30, 30, 30),
        "parchment_base": (240, 240, 240),
        "parchment_dark": (210, 210, 210),
    },
}


# ---------------------------------------------------------------------------
# Procedural parchment background
# ---------------------------------------------------------------------------

def _make_parchment(w: int, h: int, palette: dict | None = None) -> Image.Image:
    """Generate a parchment-textured background."""
    parchment_base = (palette or {}).get("parchment_base", _PARCHMENT_BASE)
    parchment_dark = (palette or {}).get("parchment_dark", _PARCHMENT_DARK)

    img = Image.new("RGB", (w, h), parchment_base)
    draw = ImageDraw.Draw(img)

    # Grain noise
    rng = random.Random(42)
    for _ in range(w * h // 8):
        x = rng.randint(0, w - 1)
        y = rng.randint(0, h - 1)
        offset = rng.randint(-15, 15)
        c = tuple(max(0, min(255, parchment_base[i] + offset)) for i in range(3))
        draw.point((x, y), fill=c)

    # Subtle stain blotches
    for _ in range(6):
        sx = rng.randint(w // 6, w * 5 // 6)
        sy = rng.randint(h // 6, h * 5 // 6)
        sr = rng.randint(min(w, h) // 12, min(w, h) // 6)
        stain_color = tuple(max(0, parchment_base[i] - rng.randint(10, 25)) for i in range(3))
        stain = Image.new("RGB", (sr * 2, sr * 2), parchment_base)
        sd = ImageDraw.Draw(stain)
        sd.ellipse((0, 0, sr * 2, sr * 2), fill=stain_color)
        stain = stain.filter(ImageFilter.GaussianBlur(sr // 2))
        img.paste(stain, (sx - sr, sy - sr), stain.split()[0])

    # Soft blur for smooth texture
    img = img.filter(ImageFilter.GaussianBlur(1.5))

    # Vignette (darken edges)
    vignette = Image.new("L", (w, h), 0)
    vd = ImageDraw.Draw(vignette)
    cx, cy = w // 2, h // 2
    max_r = math.hypot(cx, cy)
    steps = 40
    for i in range(steps):
        r_frac = 1.0 - i / steps
        alpha = int(45 * (r_frac ** 2))
        r = int(max_r * (1.0 - i / steps))
        vd.ellipse((cx - r, cy - r, cx + r, cy + r), fill=255 - alpha)
    # Darken by blending towards dark parchment
    dark = Image.new("RGB", (w, h), parchment_dark)
    mask = vignette.filter(ImageFilter.GaussianBlur(30))
    img = Image.composite(img, dark, mask)

    return img


# ---------------------------------------------------------------------------
# Portrait frame drawing
# ---------------------------------------------------------------------------

def _draw_portrait_frame(
    draw: ImageDraw.ImageDraw,
    img: Image.Image,
    cx: int,
    cy: int,
    r: int,
    portrait: Image.Image | None,
    initials: str,
    font: ImageFont.ImageFont,
    node_fills: list | None = None,
    frame_rings: list | None = None,
) -> None:
    """Draw an ornate circular portrait frame at (cx, cy) with radius r."""
    node_fills = node_fills or _NODE_FILLS
    frame_rings = frame_rings or _FRAME_RINGS
    frame_w = max(3, r // 6)
    outer_r = r + frame_w * len(frame_rings) // 2

    # Draw frame rings (outer → inner)
    for i, color in enumerate(frame_rings):
        ring_r = outer_r - i * frame_w
        draw.ellipse(
            (cx - ring_r, cy - ring_r, cx + ring_r, cy + ring_r),
            outline=color,
            width=frame_w,
        )

    # Inner circle fill or portrait
    if portrait is not None:
        pic = portrait.convert("RGBA").resize((r * 2, r * 2))
        mask = Image.new("L", (r * 2, r * 2), 0)
        ImageDraw.Draw(mask).ellipse((0, 0, r * 2, r * 2), fill=255)
        img.paste(pic, (cx - r, cy - r), mask)
    else:
        color_idx = hash(initials) % len(node_fills)
        draw.ellipse(
            (cx - r, cy - r, cx + r, cy + r),
            fill=node_fills[color_idx],
            outline=frame_rings[0],
            width=2,
        )
        draw.text((cx, cy), initials, fill=(255, 250, 240), font=font, anchor="mm")

    # Inner bright ring
    bevel = frame_rings[2] if len(frame_rings) > 2 else (210, 195, 140)
    draw.ellipse(
        (cx - r - 1, cy - r - 1, cx + r + 1, cy + r + 1),
        outline=bevel,
        width=2,
    )


# ---------------------------------------------------------------------------
# Level assignment helpers (ported from FamilyTree.generate_image)
# ---------------------------------------------------------------------------

def _assign_vlevels(graph: nx.DiGraph) -> tuple[nx.DiGraph, list[int] | None]:
    """Assign vertical (generation) levels via recursive traversal."""

    def _walk(node_id: str, vlevel: int) -> None:
        graph.nodes[node_id]["vlevel"] = vlevel
        for neighbor in graph.successors(node_id):
            edge_type = graph[node_id][neighbor].get("type")
            if "vlevel" in graph.nodes[neighbor]:
                continue
            if edge_type == "isChildOf":
                _walk(neighbor, vlevel - 1)
            elif edge_type == "isSpouseOf":
                _walk(neighbor, vlevel)
        for neighbor in graph.predecessors(node_id):
            edge_type = graph[neighbor][node_id].get("type")
            if "vlevel" in graph.nodes[neighbor]:
                continue
            if edge_type == "isChildOf":
                _walk(neighbor, vlevel + 1)
            elif edge_type == "isSpouseOf":
                _walk(neighbor, vlevel)

    if len(graph) == 0:
        return graph, None

    old_limit = sys.getrecursionlimit()
    sys.setrecursionlimit(max(old_limit, len(graph) * 4 + 1000))
    try:
        _walk(list(graph.nodes)[0], 0)
    finally:
        sys.setrecursionlimit(old_limit)

    vlevels_vals = [d["vlevel"] for _, d in graph.nodes(data=True) if "vlevel" in d]
    min_v = min(vlevels_vals)
    if min_v != 0:
        for n in graph.nodes():
            if "vlevel" in graph.nodes[n]:
                graph.nodes[n]["vlevel"] -= min_v
    min_v = min(d["vlevel"] for _, d in graph.nodes(data=True) if "vlevel" in d)
    max_v = max(d["vlevel"] for _, d in graph.nodes(data=True) if "vlevel" in d)
    return graph, [min_v, max_v]


def _assign_hlevels(
    graph: nx.DiGraph, vlevels: list[int]
) -> tuple[nx.DiGraph, list[int]]:
    """Assign horizontal positions within each generation level."""

    def _walk(node_id: str, hlevels: list[int]) -> list[int]:
        if "hlevel" not in graph.nodes[node_id]:
            vl = graph.nodes[node_id]["vlevel"]
            graph.nodes[node_id]["hlevel"] = hlevels[vl]
            hlevels[vl] += 1
        for neighbor in graph.successors(node_id):
            if graph[node_id][neighbor].get("type") == "isSpouseOf":
                if "hlevel" not in graph.nodes[neighbor]:
                    vl = graph.nodes[neighbor]["vlevel"]
                    graph.nodes[neighbor]["hlevel"] = hlevels[vl]
                    hlevels[vl] += 1
        for neighbor in graph.predecessors(node_id):
            if graph[neighbor][node_id].get("type") == "isChildOf":
                if "hlevel" not in graph.nodes[neighbor]:
                    hlevels = _walk(neighbor, hlevels)
        return hlevels

    num_levels = vlevels[1] - vlevels[0] + 1
    hlevels = [0] * num_levels

    old_limit = sys.getrecursionlimit()
    sys.setrecursionlimit(max(old_limit, len(graph) * 4 + 1000))
    try:
        for vl in range(vlevels[0], vlevels[1] + 1):
            persons = [n for n, d in graph.nodes(data=True) if d.get("vlevel") == vl]
            for p in persons:
                hlevels = _walk(p, hlevels)
    finally:
        sys.setrecursionlimit(old_limit)

    hlevels = [max(h - 1, 0) for h in hlevels]
    return graph, hlevels


# ---------------------------------------------------------------------------
# Renderer
# ---------------------------------------------------------------------------

class ClassicalTreeRenderer(ImageRenderer):
    """Renders a classical top-down genealogical tree as an antique PNG."""

    name = "classical_tree"
    description = "Genealogical tree"

    # ------------------------------------------------------------------
    def render(
        self, subgraph: nx.DiGraph, options: dict[str, Any] | None = None
    ) -> bytes:
        options = options or {}
        canvas_w: int = int(options.get("canvas_width", 2000))
        canvas_h: int = int(options.get("canvas_height", 1500))
        azure_sas: str | None = options.get("azure_storage_sas")
        font_scale: float = float(options.get("font_scale", 1.0))
        line_width: int = int(options.get("line_width", 2))
        color_scheme: str = str(options.get("color_scheme", "sepia"))

        # Select color palette
        palette = _COLOR_SCHEMES.get(color_scheme, _COLOR_SCHEMES["sepia"])

        G = subgraph.copy()

        if len(G) == 0:
            return self._to_png_bytes(
                _make_parchment(canvas_w, canvas_h, palette)
            )

        # --- level assignment ---
        G, vlevels = _assign_vlevels(G)
        if vlevels is None:
            return self._to_png_bytes(
                _make_parchment(canvas_w, canvas_h, palette)
            )
        G, hlevels = _assign_hlevels(G, vlevels)

        # --- dynamic sizing based on density ---
        border = 0.07
        usable_w = int(canvas_w * (1 - 2 * border))
        usable_h = int(canvas_h * (1 - 2 * border))
        num_vlevels = vlevels[1] - vlevels[0] + 1
        max_h_slots = max(max(hlevels) + 1, 1)
        vlevel_h = usable_h // num_vlevels

        # Node radius adapts to the most crowded row
        node_radius = int(min(
            vlevel_h * 0.28,
            usable_w / max(max_h_slots * 2.5, 1),
            60,
        ))
        node_radius = max(node_radius, 18)

        # Font size adapts, scaled by user preference
        base_font_size = max(10, min(node_radius // 2, 18))
        font_size = max(8, int(base_font_size * font_scale))
        try:
            font = ImageFont.truetype("arial.ttf", font_size)
            font_initials = ImageFont.truetype("arial.ttf", max(font_size, int(node_radius // 2 * font_scale)))
        except (OSError, IOError):
            font = ImageFont.load_default()
            font_initials = font

        # Max text width per slot (to truncate long names)
        slot_w = usable_w // max(max_h_slots, 1)

        # --- compute per-node positions ---
        for pid, pdata in G.nodes(data=True):
            vl = pdata["vlevel"]
            hl = pdata["hlevel"]
            slots = hlevels[vl] + 1
            hlevel_w = usable_w // max(slots, 1)
            cx = int(canvas_w * border + (hl + 0.5) * hlevel_w)
            cy = int(canvas_h * border + (vl + 0.5) * vlevel_h)
            G.nodes[pid]["cx"] = cx
            G.nodes[pid]["cy"] = cy
            G.nodes[pid]["slot_w"] = hlevel_w

        self._adjust_spouses(G, node_radius)

        # --- spouse-aware hlevel for line colouring ---
        for vl in range(vlevels[0], vlevels[1] + 1):
            persons = sorted(
                [n for n, d in G.nodes(data=True) if d.get("vlevel") == vl],
                key=lambda n: G.nodes[n].get("hlevel", 0),
            )
            sp_idx = 0
            for p in persons:
                G.nodes[p]["hlevel_spouse"] = sp_idx
                is_left_spouse = any(
                    G[p][nb].get("type") == "isSpouseOf"
                    and G.nodes[nb].get("hlevel", 0) > G.nodes[p].get("hlevel", 0)
                    for nb in G.successors(p)
                )
                if not is_left_spouse:
                    sp_idx += 1

        # --- create parchment canvas ---
        img = _make_parchment(canvas_w, canvas_h, palette)
        draw = ImageDraw.Draw(img)

        # --- draw edges ---
        self._draw_edges(draw, G, node_radius, line_width, palette)

        # --- draw nodes ---
        self._draw_nodes(draw, img, G, node_radius, font, font_initials, azure_sas, slot_w, palette)

        return self._to_png_bytes(img)

    # ------------------------------------------------------------------
    @staticmethod
    def _to_png_bytes(img: Image.Image) -> bytes:
        buf = io.BytesIO()
        img.save(buf, format="PNG", quality=95)
        buf.seek(0)
        return buf.read()

    @staticmethod
    def _adjust_spouses(G: nx.DiGraph, radius: int) -> None:
        """Move spouses closer together so they appear as a couple."""
        seen: set[str] = set()
        for pid in list(G.nodes):
            if pid in seen:
                continue
            for nb in G.successors(pid):
                if G[pid][nb].get("type") != "isSpouseOf" or nb in seen:
                    continue
                cx1 = G.nodes[pid]["cx"]
                cx2 = G.nodes[nb]["cx"]
                dist = abs(cx2 - cx1)
                desired = max(int(radius * 3.0), 70)
                if dist > desired:
                    shift = (dist - desired) // 2
                    if cx1 < cx2:
                        G.nodes[pid]["cx"] = cx1 + shift
                        G.nodes[nb]["cx"] = cx2 - shift
                    else:
                        G.nodes[pid]["cx"] = cx1 - shift
                        G.nodes[nb]["cx"] = cx2 + shift
                mid_x = (G.nodes[pid]["cx"] + G.nodes[nb]["cx"]) // 2
                G.nodes[pid]["spouse_mid_x"] = mid_x
                G.nodes[nb]["spouse_mid_x"] = mid_x
                seen.add(pid)
                seen.add(nb)

    @staticmethod
    def _draw_edges(draw: ImageDraw.ImageDraw, G: nx.DiGraph, radius: int, lw: int = 2, palette: dict | None = None) -> None:
        """Draw connecting lines."""
        line_colors = (palette or {}).get("line_colors", _LINE_COLORS)
        for src, tgt, edata in G.edges(data=True):
            if "cx" not in G.nodes[src] or "cx" not in G.nodes[tgt]:
                continue
            etype = edata.get("type", "")
            color = line_colors[
                G.nodes[tgt].get("hlevel_spouse", 0) % len(line_colors)
            ]

            if etype == "isSpouseOf":
                if G.nodes[src]["cy"] == G.nodes[tgt]["cy"]:
                    y = G.nodes[src]["cy"]
                    x1 = G.nodes[src]["cx"] + radius
                    x2 = G.nodes[tgt]["cx"] - radius
                    if x1 > x2:
                        x1, x2 = x2, x1
                    draw.line([(x1, y), (x2, y)], fill=color, width=lw)

            elif etype == "isChildOf":
                child_cx = G.nodes[src]["cx"]
                child_cy = G.nodes[src]["cy"]
                parent_cx = G.nodes[tgt]["cx"]
                parent_cy = G.nodes[tgt]["cy"]
                start_x = G.nodes[tgt].get("spouse_mid_x", parent_cx)

                mid_y = (parent_cy + child_cy) // 2
                draw.line(
                    [(start_x, parent_cy + radius), (start_x, mid_y)],
                    fill=color, width=lw,
                )
                draw.line(
                    [(start_x, mid_y), (child_cx, mid_y)],
                    fill=color, width=lw,
                )
                draw.line(
                    [(child_cx, mid_y), (child_cx, child_cy - radius)],
                    fill=color, width=lw,
                )

    @staticmethod
    def _draw_nodes(
        draw: ImageDraw.ImageDraw,
        img: Image.Image,
        G: nx.DiGraph,
        radius: int,
        font: ImageFont.ImageFont,
        font_initials: ImageFont.ImageFont,
        azure_sas: str | None,
        slot_w: int,
        palette: dict | None = None,
    ) -> None:
        """Draw ornate portrait frames with pictures or initials, and names."""
        text_color = (palette or {}).get("text_color", _TEXT_COLOR)
        node_fills = (palette or {}).get("node_fills", _NODE_FILLS)
        frame_rings = (palette or {}).get("frame_rings", _FRAME_RINGS)
        # Build a smaller font for tight spaces
        small_font = font
        try:
            base_size = font.size if hasattr(font, "size") else 14
            small_font = ImageFont.truetype("arial.ttf", max(8, base_size - 3))
        except Exception:
            pass

        for pid, pdata in G.nodes(data=True):
            cx = pdata.get("cx")
            cy = pdata.get("cy")
            if cx is None or cy is None:
                continue

            firstname = pdata.get("firstname", "")
            lastname = pdata.get("lastname", "")
            fullname = f"{firstname} {lastname}".strip()
            initials = (firstname[:1] + lastname[:1]).upper() or "?"

            # Try to load profile picture
            portrait = None
            if "profilepic" in pdata and pdata["profilepic"] and azure_sas:
                try:
                    import requests
                    url = pdata["profilepic"] + "?" + azure_sas
                    resp = requests.get(url, timeout=10)
                    if resp.status_code == 200:
                        portrait = Image.open(io.BytesIO(resp.content))
                except Exception:
                    pass

            # Draw the ornate frame
            _draw_portrait_frame(draw, img, cx, cy, radius, portrait, initials, font_initials, node_fills, frame_rings)

            # --- Draw name below the frame, one word per line ---
            frame_bottom = cy + radius + max(3, radius // 6) * len(frame_rings) // 2 + 4
            node_slot_w = int(pdata.get("slot_w", slot_w) * 0.85)

            words = fullname.split()
            if not words:
                continue

            # Pick font: use normal font if widest word fits, else use small font
            use_font = font
            try:
                widest = max(use_font.getbbox(w)[2] - use_font.getbbox(w)[0] for w in words)
                if widest > node_slot_w:
                    use_font = small_font
            except Exception:
                pass

            # Render each word centred on its own line
            line_h = 0
            try:
                line_h = use_font.getbbox("Ag")[3] - use_font.getbbox("Ag")[1] + 2
            except Exception:
                line_h = 12

            y = frame_bottom
            for word in words:
                # Truncate if still too wide
                display = word
                try:
                    tw = use_font.getbbox(display)[2] - use_font.getbbox(display)[0]
                    while tw > node_slot_w and len(display) > 2:
                        display = display[:-2] + "…"
                        tw = use_font.getbbox(display)[2] - use_font.getbbox(display)[0]
                except Exception:
                    pass
                draw.text((cx, y), display, fill=text_color, font=use_font, anchor="mt")
                y += line_h


# Auto-register on import
RendererRegistry.register(ClassicalTreeRenderer())

"""Fan chart renderers – ancestor and descendant modes.

Coloured concentric wedge rings with adaptive font sizing.
Root person (+ spouse) at centre; ancestors or descendants fan outward.
"""

from __future__ import annotations

import colorsys
import math
from io import BytesIO
from typing import Any

import networkx as nx
from PIL import Image, ImageDraw, ImageFont

from backend.app.renderers.base import ImageRenderer
from backend.app.renderers.registry import RendererRegistry

# ── colours ─────────────────────────────────────────────────────────────────
_BRANCH_HUES = [270, 315, 178, 152]

_BG = (245, 240, 230)
_DIVIDER = (255, 255, 255)
_WEDGE_TEXT = (255, 255, 255)
_CENTER_TEXT = (50, 30, 15)
_CENTER_FILL_TOP = (225, 180, 215)
_CENTER_FILL_BOT = (170, 215, 215)
_CENTER_FILL_SINGLE = (215, 200, 180)


def _wedge_color(branch: int, level: int) -> tuple[int, int, int]:
    hue = _BRANCH_HUES[branch % len(_BRANCH_HUES)] / 360.0
    lightness = 0.55 - (level - 1) * 0.05
    saturation = 0.58 + (level - 1) * 0.03
    lightness = max(0.35, min(lightness, 0.65))
    saturation = max(0.35, min(saturation, 0.75))
    r, g, b = colorsys.hls_to_rgb(hue, lightness, saturation)
    return (int(r * 255), int(g * 255), int(b * 255))


def _display_name(nd: dict) -> str:
    first = nd.get("firstname") or ""
    last = nd.get("lastname") or ""
    return f"{first} {last}".strip() or "?"


# ── graph helpers ───────────────────────────────────────────────────────────

def _get_parents(graph: nx.DiGraph, person: Any) -> list[Any]:
    return [s for s in graph.successors(person)
            if graph[person][s].get("type") == "isChildOf"]


def _get_children(graph: nx.DiGraph, person: Any) -> list[Any]:
    return [p for p in graph.predecessors(person)
            if graph[p][person].get("type") == "isChildOf"]


def _get_spouse(graph: nx.DiGraph, person: Any) -> Any | None:
    for u, v, d in graph.edges(data=True):
        if d.get("type") == "isSpouseOf":
            if u == person:
                return v
            if v == person:
                return u
    return None


# ── geometry ────────────────────────────────────────────────────────────────

def _annular_sector(cx, cy, r_in, r_out, a_start, a_end, n=64):
    pts = []
    for i in range(n + 1):
        a = a_start + (a_end - a_start) * i / n
        pts.append((cx + r_out * math.cos(a), cy - r_out * math.sin(a)))
    for i in range(n + 1):
        a = a_end + (a_start - a_end) * i / n
        pts.append((cx + r_in * math.cos(a), cy - r_in * math.sin(a)))
    return pts


# ── font helpers ────────────────────────────────────────────────────────────

def _load_font(size: int) -> ImageFont.ImageFont:
    try:
        return ImageFont.truetype("arial.ttf", max(size, 7))
    except OSError:
        return ImageFont.load_default()


def _compute_level_fonts(
    segments: list[tuple],
    center_r: float,
    ring_w: float,
    max_level: int,
) -> dict[int, ImageFont.ImageFont]:
    """Return a font per level sized to fit the narrowest wedge at that level."""
    fonts: dict[int, ImageFont.ImageFont] = {}
    for lvl in range(1, max_level + 1):
        spans = [abs(a1 - a0) for _, l, a0, a1, _ in segments if l == lvl]
        if not spans:
            fonts[lvl] = _load_font(12)
            continue
        min_span = min(spans)
        r_mid = center_r + (lvl - 0.5) * ring_w
        arc_len = r_mid * min_span
        # Font limited by ring height and narrowest arc width
        fs = max(8, min(int(ring_w * 0.30), int(arc_len * 0.20), 32))
        fonts[lvl] = _load_font(fs)
    return fonts


# ── fan builders ────────────────────────────────────────────────────────────

def _build_ancestor_fan(graph, person, level, a0, a1, branch, out, max_lvl=10):
    if level > max_lvl:
        return
    parents = _get_parents(graph, person)
    if not parents:
        return
    mid = (a0 + a1) / 2
    out.append((parents[0], level, a0, mid, branch))
    _build_ancestor_fan(graph, parents[0], level + 1, a0, mid, branch, out, max_lvl)
    if len(parents) >= 2:
        out.append((parents[1], level, mid, a1, branch))
        _build_ancestor_fan(graph, parents[1], level + 1, mid, a1, branch, out, max_lvl)


def _build_descendant_fan(graph, person, level, a0, a1, branch, out, max_lvl=10):
    if level > max_lvl:
        return
    children = _get_children(graph, person)
    if not children:
        return
    arc_each = (a1 - a0) / len(children)
    for i, child in enumerate(children):
        c0 = a0 + i * arc_each
        c1 = c0 + arc_each
        # Assign branch index from the root-level child
        br = branch if level > 1 else i % len(_BRANCH_HUES)
        out.append((child, level, c0, c1, br))
        _build_descendant_fan(graph, child, level + 1, c0, c1, br, out, max_lvl)


# ── text rendering ──────────────────────────────────────────────────────────

def _render_wedge_text(
    img: Image.Image,
    name: str,
    cx: float, cy: float,
    r_in: float, r_out: float,
    a_start: float, a_end: float,
    font: ImageFont.ImageFont,
    color: tuple,
) -> None:
    mid_a = (a_start + a_end) / 2
    mid_r = (r_in + r_out) / 2
    px = cx + mid_r * math.cos(mid_a)
    py = cy - mid_r * math.sin(mid_a)

    words = name.split()
    if not words:
        return

    line_h = 0
    max_w = 0
    for w in words:
        bb = font.getbbox(w)
        max_w = max(max_w, bb[2] - bb[0])
        line_h = max(line_h, bb[3] - bb[1])
    line_h += 2
    pad = 4
    tw = int(max_w + pad * 2)
    th = int(line_h * len(words) + pad * 2)

    txt_img = Image.new("RGBA", (tw, th), (0, 0, 0, 0))
    td = ImageDraw.Draw(txt_img)
    fill = color + (255,) if len(color) == 3 else color
    for i, w in enumerate(words):
        ww = font.getbbox(w)[2] - font.getbbox(w)[0]
        td.text((int((tw - ww) / 2), int(pad + i * line_h)), w, fill=fill, font=font)

    rot_deg = math.degrees(mid_a) - 90
    if 90 < (rot_deg % 360) < 270:
        rot_deg += 180

    txt_img = txt_img.rotate(rot_deg, resample=Image.BICUBIC, expand=True)
    img.paste(txt_img, (int(px - txt_img.width / 2), int(py - txt_img.height / 2)), txt_img)


def _render_center_text(
    draw: ImageDraw.ImageDraw,
    name: str,
    cx: float, cy: float,
    radius: float,
    base_fs: int,
    color: tuple,
    y_offset: float = 0,
) -> None:
    """Draw *name* in the centre circle, wrapping and shrinking to fit."""
    max_w = radius * 1.6
    words = name.split()

    # Shrink font until widest word fits
    fs = base_fs
    font = _load_font(fs)
    while fs > 8:
        widest = max(font.getbbox(w)[2] - font.getbbox(w)[0] for w in words)
        if widest <= max_w:
            break
        fs -= 2
        font = _load_font(fs)

    # Single-line check
    full_bb = font.getbbox(name)
    full_w = full_bb[2] - full_bb[0]
    if full_w <= max_w:
        draw.text((cx, cy + y_offset), name, fill=color, font=font, anchor="mm")
        return

    # Multi-line (one word per line)
    line_h = full_bb[3] - full_bb[1] + 3
    total_h = line_h * len(words)
    y_start = cy + y_offset - total_h / 2 + line_h / 2
    for i, w in enumerate(words):
        draw.text((cx, y_start + i * line_h), w, fill=color, font=font, anchor="mm")


# ── shared drawing logic ────────────────────────────────────────────────────

def _render_fan_chart(
    subgraph: nx.DiGraph,
    segments: list[tuple],
    root: Any,
    spouse: Any | None,
    width: int,
    height: int,
) -> bytes:
    max_level = max((s[1] for s in segments), default=0)
    cx, cy = width / 2, height / 2
    max_radius = min(cx, cy) * 0.92
    center_r = max_radius * 0.16 if max_level > 0 else max_radius * 0.30
    ring_w = (max_radius - center_r) / max(max_level, 1)

    center_fs = max(14, min(int(center_r * 0.28), 30))
    level_fonts = _compute_level_fonts(segments, center_r, ring_w, max_level)

    img = Image.new("RGBA", (width, height), _BG + (255,))
    draw = ImageDraw.Draw(img)

    # Coloured wedges (outer first)
    for node, level, a0, a1, branch in sorted(segments, key=lambda s: -s[1]):
        r_in = center_r + (level - 1) * ring_w
        r_out = center_r + level * ring_w
        draw.polygon(_annular_sector(cx, cy, r_in, r_out, a0, a1),
                     fill=_wedge_color(branch, level))

    # White divider lines
    for _, level, a0, a1, _ in segments:
        r_in = center_r + (level - 1) * ring_w
        r_out = center_r + level * ring_w
        for a in (a0, a1):
            x1 = cx + r_in * math.cos(a)
            y1 = cy - r_in * math.sin(a)
            x2 = cx + r_out * math.cos(a)
            y2 = cy - r_out * math.sin(a)
            draw.line([(x1, y1), (x2, y2)], fill=_DIVIDER, width=2)

    # Ring borders
    for lvl in range(1, max_level + 1):
        r = center_r + lvl * ring_w
        draw.ellipse([cx - r, cy - r, cx + r, cy + r], outline=_DIVIDER, width=2)

    # Centre circle
    has_spouse = spouse is not None and spouse in subgraph.nodes
    if has_spouse:
        draw.ellipse(
            [cx - center_r, cy - center_r, cx + center_r, cy + center_r],
            fill=_CENTER_FILL_TOP, outline=_DIVIDER, width=2,
        )
        draw.pieslice(
            [cx - center_r, cy - center_r, cx + center_r, cy + center_r],
            start=0, end=180, fill=_CENTER_FILL_BOT,
        )
        draw.line([(cx - center_r, cy), (cx + center_r, cy)], fill=_DIVIDER, width=2)
        draw.ellipse(
            [cx - center_r, cy - center_r, cx + center_r, cy + center_r],
            outline=_DIVIDER, width=2,
        )
        _render_center_text(draw, _display_name(subgraph.nodes[root]),
                            cx, cy, center_r, center_fs, _CENTER_TEXT, y_offset=-center_r * 0.28)
        _render_center_text(draw, _display_name(subgraph.nodes[spouse]),
                            cx, cy, center_r, center_fs, _CENTER_TEXT, y_offset=center_r * 0.28)
    else:
        draw.ellipse(
            [cx - center_r, cy - center_r, cx + center_r, cy + center_r],
            fill=_CENTER_FILL_SINGLE, outline=_DIVIDER, width=2,
        )
        _render_center_text(draw, _display_name(subgraph.nodes[root]),
                            cx, cy, center_r, center_fs, _CENTER_TEXT)

    draw.ellipse(
        [cx - center_r, cy - center_r, cx + center_r, cy + center_r],
        outline=_DIVIDER, width=2,
    )

    # Wedge names (per-level font)
    for node, level, a0, a1, _ in segments:
        nd = subgraph.nodes[node]
        r_in = center_r + (level - 1) * ring_w
        r_out = center_r + level * ring_w
        font = level_fonts.get(level, _load_font(12))
        _render_wedge_text(img, _display_name(nd), cx, cy, r_in, r_out, a0, a1, font, _WEDGE_TEXT)

    out = Image.new("RGB", (width, height), _BG)
    out.paste(img, (0, 0), img)
    buf = BytesIO()
    out.save(buf, format="PNG")
    return buf.getvalue()


# ── ancestor renderer ───────────────────────────────────────────────────────

class RadialAncestorRenderer(ImageRenderer):
    name = "radial_ancestors"
    description = "Ancestor fan chart (concentric rings)"

    def render(self, subgraph: nx.DiGraph, options: dict[str, Any] | None = None) -> bytes:
        opts = options or {}
        size = int(opts.get("canvas_width", 1800))

        root = self._resolve_root(subgraph, opts)
        spouse = _get_spouse(subgraph, root)

        segments: list[tuple] = []
        if spouse and spouse in subgraph.nodes:
            rp = _get_parents(subgraph, root)
            sp = _get_parents(subgraph, spouse)
            if len(rp) >= 1:
                segments.append((rp[0], 1, math.pi / 2, math.pi, 0))
                _build_ancestor_fan(subgraph, rp[0], 2, math.pi / 2, math.pi, 0, segments)
            if len(rp) >= 2:
                segments.append((rp[1], 1, 0, math.pi / 2, 1))
                _build_ancestor_fan(subgraph, rp[1], 2, 0, math.pi / 2, 1, segments)
            if len(sp) >= 1:
                segments.append((sp[0], 1, -math.pi / 2, 0, 2))
                _build_ancestor_fan(subgraph, sp[0], 2, -math.pi / 2, 0, 2, segments)
            if len(sp) >= 2:
                segments.append((sp[1], 1, -math.pi, -math.pi / 2, 3))
                _build_ancestor_fan(subgraph, sp[1], 2, -math.pi, -math.pi / 2, 3, segments)
        else:
            rp = _get_parents(subgraph, root)
            if len(rp) >= 1:
                segments.append((rp[0], 1, 0, math.pi, 0))
                _build_ancestor_fan(subgraph, rp[0], 2, 0, math.pi, 0, segments)
            if len(rp) >= 2:
                segments.append((rp[1], 1, -math.pi, 0, 1))
                _build_ancestor_fan(subgraph, rp[1], 2, -math.pi, 0, 1, segments)

        return _render_fan_chart(subgraph, segments, root, spouse, size, size)

    @staticmethod
    def _resolve_root(subgraph, opts):
        root_id = opts.get("root_id")
        if root_id and root_id in subgraph.nodes:
            return root_id
        for n in subgraph.nodes:
            if not _get_parents(subgraph, n):
                return n
        return next(iter(subgraph.nodes))


# ── descendant renderer ─────────────────────────────────────────────────────

class RadialDescendantRenderer(ImageRenderer):
    name = "radial_descendants"
    description = "Descendant fan chart (concentric rings)"

    def render(self, subgraph: nx.DiGraph, options: dict[str, Any] | None = None) -> bytes:
        opts = options or {}
        size = int(opts.get("canvas_width", 1800))

        root = self._resolve_root(subgraph, opts)
        spouse = _get_spouse(subgraph, root)

        segments: list[tuple] = []
        children = _get_children(subgraph, root)
        if not children and spouse and spouse in subgraph.nodes:
            children = _get_children(subgraph, spouse)

        if children:
            arc_each = 2 * math.pi / len(children)
            for i, child in enumerate(children):
                a0 = -math.pi + i * arc_each
                a1 = a0 + arc_each
                br = i % len(_BRANCH_HUES)
                segments.append((child, 1, a0, a1, br))
                _build_descendant_fan(subgraph, child, 2, a0, a1, br, segments)

        return _render_fan_chart(subgraph, segments, root, spouse, size, size)

    @staticmethod
    def _resolve_root(subgraph, opts):
        root_id = opts.get("root_id")
        if root_id and root_id in subgraph.nodes:
            return root_id
        for n in subgraph.nodes:
            if not _get_parents(subgraph, n):
                return n
        return next(iter(subgraph.nodes))


# Register both renderers
RendererRegistry.register(RadialAncestorRenderer())
RendererRegistry.register(RadialDescendantRenderer())

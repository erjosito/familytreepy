"""Radial family tree renderer – root person at center, descendants in concentric rings.

Names are rendered along the concentric ring arcs (radially oriented text).
The root person's spouse shares the center position.
"""

from __future__ import annotations

import math
from collections import deque
from io import BytesIO
from typing import Any

import networkx as nx
from PIL import Image, ImageDraw, ImageFont

from backend.app.renderers.base import ImageRenderer
from backend.app.renderers.registry import RendererRegistry

# ---------------------------------------------------------------------------
# Colour palette – warm tones for ring bands
# ---------------------------------------------------------------------------
_RING_FILLS = [
    (235, 225, 200, 60),   # parchment (center)
    (200, 175, 140, 45),
    (180, 160, 130, 40),
    (170, 150, 120, 40),
    (160, 145, 115, 35),
    (150, 140, 110, 35),
    (145, 135, 105, 30),
    (140, 130, 100, 30),
]

_RING_STROKES = [
    (160, 130, 80),
    (150, 120, 75),
    (140, 115, 70),
    (135, 110, 65),
    (130, 105, 60),
    (125, 100, 55),
    (120, 95, 50),
    (115, 90, 45),
]

_NODE_DOT_COLOR = (140, 100, 50)
_LINE_COLOR = (170, 145, 110)
_SPOUSE_LINE_COLOR = (160, 130, 90)
_TEXT_COLOR = (55, 35, 15)
_BG_COLOR = (230, 218, 195)   # warm parchment


def _initials(node_data: dict) -> str:
    first = (node_data.get("firstname") or "")[:1]
    last = (node_data.get("lastname") or "")[:1]
    return (first + last).upper() or "?"


def _display_name(node_data: dict) -> str:
    first = node_data.get("firstname") or ""
    last = node_data.get("lastname") or ""
    return f"{first} {last}".strip() or "?"


def _draw_curved_text(
    img: Image.Image,
    text: str,
    cx: float,
    cy: float,
    radius: float,
    center_angle: float,
    font: ImageFont.ImageFont,
    color: tuple = _TEXT_COLOR,
) -> None:
    """Render *text* along an arc at *radius* from (cx, cy), centred at *center_angle*.

    Characters on the bottom half of the circle are flipped so text is always
    readable (never upside-down).
    """
    if not text:
        return

    # Measure character widths
    char_widths = []
    for ch in text:
        try:
            bb = font.getbbox(ch)
            char_widths.append(bb[2] - bb[0])
        except Exception:
            char_widths.append(8)
    total_w = sum(char_widths)

    # Angular span of the text at this radius
    if radius < 1:
        return
    arc_per_px = 1.0 / radius  # radians per pixel along the arc
    total_arc = total_w * arc_per_px

    # Determine if we're on the "bottom" half (text would be upside-down)
    # Normalise center_angle to [0, 2π)
    norm = center_angle % (2 * math.pi)
    flip = math.pi * 0.25 < norm < math.pi * 1.25

    if flip:
        start_angle = center_angle + total_arc / 2
        direction = -1
    else:
        start_angle = center_angle - total_arc / 2
        direction = 1

    angle = start_angle
    for i, ch in enumerate(text):
        ch_arc = char_widths[i] * arc_per_px
        ch_angle = angle + direction * ch_arc / 2  # centre of this character

        # Position on the arc
        px = cx + radius * math.cos(ch_angle)
        py = cy + radius * math.sin(ch_angle)

        # Render the character onto a small transparent image, rotated
        try:
            bb = font.getbbox(ch)
            tw, th = bb[2] - bb[0] + 4, bb[3] - bb[1] + 4
        except Exception:
            tw, th = 12, 14

        char_img = Image.new("RGBA", (tw * 2, th * 2), (0, 0, 0, 0))
        cd = ImageDraw.Draw(char_img)
        cd.text((tw, th), ch, fill=color + (255,) if len(color) == 3 else color, font=font, anchor="mm")

        # Rotation: tangent to the arc + 90° so text reads outward
        rot_deg = -math.degrees(ch_angle) - 90
        if flip:
            rot_deg += 180
        char_img = char_img.rotate(rot_deg, resample=Image.BICUBIC, expand=True)

        # Paste centred on (px, py)
        paste_x = int(px - char_img.width / 2)
        paste_y = int(py - char_img.height / 2)
        img.paste(char_img, (paste_x, paste_y), char_img)

        angle += direction * ch_arc


class RadialTreeRenderer(ImageRenderer):
    """Radial tree with root person at center, descendants in concentric rings."""

    name = "radial_tree"
    description = "Radial tree with root person at center and names along concentric rings"

    # ------------------------------------------------------------------ #
    @staticmethod
    def _find_root(graph: nx.DiGraph) -> Any:
        for node in graph.nodes:
            has_parent = any(
                graph[node][succ].get("type") == "isChildOf"
                for succ in graph.successors(node)
            )
            if not has_parent:
                return node
        return next(iter(graph.nodes))

    @staticmethod
    def _build_levels(graph: nx.DiGraph, root: Any) -> dict[Any, int]:
        levels: dict[Any, int] = {root: 0}
        queue: deque[Any] = deque([root])
        while queue:
            current = queue.popleft()
            cur_level = levels[current]
            for pred in graph.predecessors(current):
                edge_data = graph[pred][current]
                if edge_data.get("type") == "isChildOf" and pred not in levels:
                    levels[pred] = cur_level + 1
                    queue.append(pred)
        return levels

    @staticmethod
    def _get_spouses(graph: nx.DiGraph, node: Any) -> list[Any]:
        spouses = []
        for u, v, data in graph.edges(data=True):
            if data.get("type") == "isSpouseOf":
                if u == node and v not in spouses:
                    spouses.append(v)
                elif v == node and u not in spouses:
                    spouses.append(u)
        return spouses

    @staticmethod
    def _spouse_pairs(graph: nx.DiGraph) -> list[tuple[Any, Any]]:
        seen: set[tuple] = set()
        pairs: list[tuple[Any, Any]] = []
        for u, v, data in graph.edges(data=True):
            if data.get("type") == "isSpouseOf":
                key = (min(u, v, key=str), max(u, v, key=str))
                if key not in seen:
                    seen.add(key)
                    pairs.append((u, v))
        return pairs

    # ------------------------------------------------------------------ #
    def render(self, subgraph: nx.DiGraph, options: dict[str, Any] | None = None) -> bytes:
        opts = options or {}
        width = int(opts.get("canvas_width", 1800))
        height = int(opts.get("canvas_height", 1800))

        root = self._find_root(subgraph)
        root_spouses = self._get_spouses(subgraph, root)
        center_members = {root} | set(root_spouses)

        levels = self._build_levels(subgraph, root)

        # Assign remaining orphan nodes
        for node in subgraph.nodes:
            if node not in levels:
                levels[node] = 0

        # Pull root's spouse(s) to level 0 (center)
        for sp in root_spouses:
            levels[sp] = 0

        # Pull all spouse pairs to the same level (the lower one)
        for a, b in self._spouse_pairs(subgraph):
            if a in levels and b in levels:
                lvl = min(levels[a], levels[b])
                levels[a] = lvl
                levels[b] = lvl

        max_level = max(levels.values()) if levels else 0

        cx, cy = width / 2, height / 2
        ring_gap = min(cx, cy) * 0.85 / max(max_level + 1, 1)

        # Group nodes by level
        by_level: dict[int, list[Any]] = {}
        for node, lvl in levels.items():
            by_level.setdefault(lvl, []).append(node)

        # Compute angular positions
        positions: dict[Any, tuple[float, float]] = {}
        angles: dict[Any, float] = {}

        # Level 0: root + spouse(s) at center
        level0 = by_level.get(0, [root])
        if len(level0) == 1:
            positions[level0[0]] = (cx, cy)
            angles[level0[0]] = 0
        else:
            # Place side by side at center
            spacing = min(ring_gap * 0.3, 50)
            total_w = (len(level0) - 1) * spacing
            for i, n in enumerate(level0):
                positions[n] = (cx - total_w / 2 + i * spacing, cy)
                angles[n] = 0

        # Levels 1+
        for lvl in range(1, max_level + 1):
            members = by_level.get(lvl, [])
            if not members:
                continue
            radius = ring_gap * lvl
            count = len(members)
            for i, node in enumerate(members):
                angle = 2 * math.pi * i / count - math.pi / 2
                positions[node] = (cx + radius * math.cos(angle), cy + radius * math.sin(angle))
                angles[node] = angle

        # ----- draw -----
        img = Image.new("RGBA", (width, height), _BG_COLOR + (255,))
        draw = ImageDraw.Draw(img)

        # Font
        font_size = max(10, min(int(ring_gap * 0.14), 16))
        center_font_size = max(12, min(int(ring_gap * 0.18), 20))
        try:
            font = ImageFont.truetype("arial.ttf", font_size)
            center_font = ImageFont.truetype("arial.ttf", center_font_size)
        except OSError:
            font = ImageFont.load_default()
            center_font = font

        # Draw filled ring bands
        for lvl in range(max_level, 0, -1):
            r_outer = ring_gap * lvl + ring_gap * 0.48
            r_inner = ring_gap * lvl - ring_gap * 0.48
            fill = _RING_FILLS[lvl % len(_RING_FILLS)]
            stroke = _RING_STROKES[lvl % len(_RING_STROKES)]
            # Outer ring
            draw.ellipse(
                [cx - r_outer, cy - r_outer, cx + r_outer, cy + r_outer],
                fill=fill[:3],
                outline=stroke,
                width=1,
            )

        # Inner circle for level 0
        r0 = ring_gap * 0.48
        draw.ellipse(
            [cx - r0, cy - r0, cx + r0, cy + r0],
            fill=(225, 210, 180),
            outline=_RING_STROKES[0],
            width=2,
        )

        # Draw parent→child lines
        for u, v, data in subgraph.edges(data=True):
            if data.get("type") == "isChildOf":
                child, parent = u, v
                if child in positions and parent in positions:
                    draw.line(
                        [positions[parent], positions[child]],
                        fill=_LINE_COLOR,
                        width=2,
                    )

        # Draw spouse lines
        for a, b in self._spouse_pairs(subgraph):
            if a in positions and b in positions:
                draw.line(
                    [positions[a], positions[b]],
                    fill=_SPOUSE_LINE_COLOR,
                    width=1,
                )

        # Draw node dots (small)
        dot_r = max(4, int(ring_gap * 0.04))
        for node, (x, y) in positions.items():
            draw.ellipse(
                [x - dot_r, y - dot_r, x + dot_r, y + dot_r],
                fill=_NODE_DOT_COLOR,
                outline=(100, 75, 40),
                width=1,
            )

        # Draw names
        for node, (x, y) in positions.items():
            nd = subgraph.nodes[node]
            name = _display_name(nd)
            lvl = levels.get(node, 0)

            if lvl == 0:
                # Center names: draw as straight text below the dot
                draw.text(
                    (x, y + dot_r + 3), name,
                    fill=_TEXT_COLOR, font=center_font, anchor="mt",
                )
            else:
                # Curved text along the ring arc
                radius = ring_gap * lvl
                angle = angles.get(node, 0)
                _draw_curved_text(img, name, cx, cy, radius + dot_r + 6, angle, font, _TEXT_COLOR)

        # Convert to RGB for PNG
        out = Image.new("RGB", (width, height), _BG_COLOR)
        out.paste(img, (0, 0), img)

        buf = BytesIO()
        out.save(buf, format="PNG")
        return buf.getvalue()


# Register with the global registry on import
RendererRegistry.register(RadialTreeRenderer())

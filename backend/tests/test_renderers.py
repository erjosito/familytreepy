"""Tests that both renderers produce valid PNG bytes."""

import networkx as nx
import pytest

from backend.app.renderers.classical_tree import ClassicalTreeRenderer
from backend.app.renderers.radial_tree import RadialAncestorRenderer, RadialDescendantRenderer

PNG_SIGNATURE = b"\x89PNG\r\n\x1a\n"


@pytest.fixture()
def small_graph():
    """Create a small test graph: grandparent -> parent -> child."""
    G = nx.DiGraph()
    G.add_node("gp", firstname="Grand", lastname="Parent")
    G.add_node("p", firstname="Parent", lastname="One")
    G.add_node("c", firstname="Child", lastname="One")
    G.add_edge("p", "gp", type="isChildOf")
    G.add_edge("c", "p", type="isChildOf")
    return G


class TestClassicalTreeRenderer:
    def test_render_returns_png(self, small_graph):
        renderer = ClassicalTreeRenderer()
        result = renderer.render(small_graph)
        assert isinstance(result, bytes)
        assert result[:8] == PNG_SIGNATURE

    def test_render_empty_graph(self):
        renderer = ClassicalTreeRenderer()
        result = renderer.render(nx.DiGraph())
        assert isinstance(result, bytes)
        assert result[:8] == PNG_SIGNATURE


class TestRadialAncestorRenderer:
    def test_render_returns_png(self, small_graph):
        renderer = RadialAncestorRenderer()
        result = renderer.render(small_graph)
        assert isinstance(result, bytes)
        assert result[:8] == PNG_SIGNATURE

    def test_render_empty_graph(self):
        renderer = RadialAncestorRenderer()
        G = nx.DiGraph()
        G.add_node("solo", firstname="Solo", lastname="Node")
        result = renderer.render(G)
        assert isinstance(result, bytes)
        assert result[:8] == PNG_SIGNATURE


class TestRadialDescendantRenderer:
    def test_render_returns_png(self, small_graph):
        renderer = RadialDescendantRenderer()
        result = renderer.render(small_graph)
        assert isinstance(result, bytes)
        assert result[:8] == PNG_SIGNATURE

    def test_render_empty_graph(self):
        renderer = RadialDescendantRenderer()
        G = nx.DiGraph()
        G.add_node("solo", firstname="Solo", lastname="Node")
        result = renderer.render(G)
        assert isinstance(result, bytes)
        assert result[:8] == PNG_SIGNATURE

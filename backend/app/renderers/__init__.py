"""Pluggable image renderer framework."""

from backend.app.renderers.base import ImageRenderer
from backend.app.renderers.registry import RendererRegistry

# Import renderer modules so they auto-register on package load
import backend.app.renderers.classical_tree  # noqa: F401
import backend.app.renderers.radial_tree  # noqa: F401

__all__ = ["ImageRenderer", "RendererRegistry"]

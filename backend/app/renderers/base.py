"""Abstract base class for family tree image renderers."""

from abc import ABC, abstractmethod
from typing import Any

import networkx as nx


class ImageRenderer(ABC):
    """Base class for all family tree image renderers.

    Subclasses must set ``name`` and ``description`` class attributes and
    implement the :meth:`render` method.
    """

    name: str  # e.g. "classical_tree"
    description: str  # e.g. "Classical genealogical tree with generations as rows"

    @abstractmethod
    def render(self, subgraph: nx.DiGraph, options: dict[str, Any] | None = None) -> bytes:
        """Render the subgraph as an image.

        Args:
            subgraph: The family tree subgraph to render.
            options: Optional renderer-specific settings.

        Returns:
            PNG image bytes.
        """
        ...

    def get_info(self) -> dict[str, str]:
        """Return a summary dict with the renderer's name and description."""
        return {"name": self.name, "description": self.description}

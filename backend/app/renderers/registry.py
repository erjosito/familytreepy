"""Registry for discovering and retrieving image renderers."""

from __future__ import annotations

from backend.app.renderers.base import ImageRenderer


class RendererRegistry:
    """Class-level registry that maps renderer names to instances."""

    _renderers: dict[str, ImageRenderer] = {}

    @classmethod
    def register(cls, renderer: ImageRenderer) -> None:
        """Register a renderer instance.

        Args:
            renderer: An :class:`ImageRenderer` subclass instance.
        """
        cls._renderers[renderer.name] = renderer

    @classmethod
    def get(cls, name: str) -> ImageRenderer | None:
        """Look up a renderer by name.

        Args:
            name: The unique renderer name.

        Returns:
            The renderer instance, or ``None`` if not found.
        """
        return cls._renderers.get(name)

    @classmethod
    def list_all(cls) -> list[dict[str, str]]:
        """Return summary info for every registered renderer."""
        return [r.get_info() for r in cls._renderers.values()]

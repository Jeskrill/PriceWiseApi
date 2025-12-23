"""
Shared helpers for provider modules.
Exports underscore helpers/constants from app.search_service to avoid circular imports.
"""

from app import search_service as _svc

_EXTRA_EXPORTS = {"logger", "settings"}


def _should_export(name: str) -> bool:
    if name in _EXTRA_EXPORTS:
        return True
    if name.startswith("__"):
        return False
    if name.startswith("_"):
        return True
    if name.isupper():
        return True
    return False


__all__ = [name for name in dir(_svc) if _should_export(name)]
globals().update({name: getattr(_svc, name) for name in __all__})

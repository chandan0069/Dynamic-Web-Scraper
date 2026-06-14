from __future__ import annotations


import importlib

from src.engine.base_adapter import BaseAdapter
from src.registry.models import AdapterType
from src.config import get_logger

logger = get_logger(__name__, component="adapter_registry")

_ADAPTER_MAP: dict[AdapterType, type[BaseAdapter]] = {}


def register_adapter(adapter_type: AdapterType):
    def decorator(cls: type[BaseAdapter]) -> type[BaseAdapter]:
        _ADAPTER_MAP[adapter_type] = cls
        logger.debug(
            "Registered adapter",
            adapter_type=str(adapter_type),
            cls=cls.__name__,
        )
        return cls

    return decorator


def resolve_adapter(adapter_type: AdapterType) -> BaseAdapter:
    if adapter_type not in _ADAPTER_MAP:
        raise KeyError(
            f"No adapter registered for {adapter_type!r}. "
            f"Registered: {list(_ADAPTER_MAP.keys())}"
        )
    cls = _ADAPTER_MAP[adapter_type]
    logger.info(
        "Resolving adapter",
        adapter_type=str(adapter_type),
        cls=cls.__name__,
    )
    return cls()


_ADAPTER_MODULES: list[str] = [
    "src.engine.adapters.rate_limited_http",
    "src.engine.adapters.playwright_browser",
    "src.engine.adapters.live_update",
]


def load_adapters() -> None:
    for module_path in _ADAPTER_MODULES:
        try:
            importlib.import_module(module_path)
            logger.debug("Loaded adapter module", module=module_path)
        except Exception as exc:
            logger.warning(
                "Could not load adapter module",
                module=module_path,
                error=str(exc),
            )

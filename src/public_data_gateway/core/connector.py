"""Base connector abstraction and plugin registry for data extractors."""

import inspect
from abc import ABC, abstractmethod
from typing import Any

import pyarrow as pa

from public_data_gateway.core.config import GatewaySettings


class BaseConnector(ABC):
    """Abstract interface for all Brazilian government data source connectors."""

    def __init__(self, source_id: str, settings: GatewaySettings | None = None) -> None:
        self.source_id = source_id
        self.settings = settings or GatewaySettings()

    @abstractmethod
    def extract(self) -> pa.Table:
        """Execute ingestion and return standardized PyArrow table."""
        raise NotImplementedError

    @abstractmethod
    def test_connection(self) -> bool:
        """Run connectivity check against upstream source endpoint."""
        raise NotImplementedError


# Global plugin registry for dynamic connector discovery
_CONNECTOR_REGISTRY: dict[str, type[BaseConnector]] = {}


def register_connector(source_id: str, connector_cls: type[Any]) -> None:
    """Register a connector class for a given source_id."""
    _CONNECTOR_REGISTRY[source_id] = connector_cls


def get_connector(source_id: str, settings: GatewaySettings | None = None) -> Any:
    """Instantiate a registered connector by source_id."""
    if source_id not in _CONNECTOR_REGISTRY:
        try:
            import public_data_gateway.connectors  # noqa: F401
        except ImportError:
            pass
    cls = _CONNECTOR_REGISTRY.get(source_id)
    if cls is None:
        raise KeyError(
            f"No connector registered for source_id '{source_id}'. Registered: {list(_CONNECTOR_REGISTRY.keys())}"
        )
    params = inspect.signature(cls.__init__).parameters
    if "source_id" in params:
        return cls(source_id=source_id, settings=settings)
    if "settings" in params:
        return cls(settings=settings)
    return cls()


def list_registered_connectors() -> list[str]:
    """Return all currently registered connector identifiers."""
    try:
        import public_data_gateway.connectors  # noqa: F401
    except ImportError:
        pass
    return sorted(list(_CONNECTOR_REGISTRY.keys()))

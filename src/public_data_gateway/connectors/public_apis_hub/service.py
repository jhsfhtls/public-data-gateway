"""Unified PublicApisHubService composing Blocks 1..4 for SDK, FastAPI, CLI & MCP."""

from __future__ import annotations

from pathlib import Path

from public_data_gateway.connectors.public_apis_hub.global_research import GlobalResearchClient
from public_data_gateway.connectors.public_apis_hub.kyc_geo import KycGeoClient
from public_data_gateway.connectors.public_apis_hub.market_regulatory import MarketRegulatoryClient
from public_data_gateway.connectors.public_apis_hub.spatial_ml import SpatialMlClient
from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.core.http_client import ResilientHttpClient


class PublicApisHubService(
    KycGeoClient,
    MarketRegulatoryClient,
    SpatialMlClient,
    GlobalResearchClient,
):
    """Single facade exposing all Phase 10 Public & Global APIs with shared resilience."""

    def __init__(
        self,
        settings: GatewaySettings | None = None,
        http_client: ResilientHttpClient | None = None,
        registry_path: Path | None = None,
    ) -> None:
        super().__init__(
            settings=settings,
            http_client=http_client,
            registry_path=registry_path,
        )

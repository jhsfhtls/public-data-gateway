"""Offline Medallion & 4-interface parity tests for Wave 10.5D (Block 4 Global Research)."""

from __future__ import annotations

from pathlib import Path

import pytest

from public_data_gateway.api.app import create_app
from public_data_gateway.connectors.agregadores.gdelt_doc_api import GdeltDocApiConnector
from public_data_gateway.connectors.demografico.rest_countries import RestCountriesConnector
from public_data_gateway.connectors.educacao.openalex_api import OpenAlexApiConnector
from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.core.connector import get_connector, list_registered_connectors
from public_data_gateway.mcp.server import MCPDispatcher


@pytest.mark.parametrize(
    ("source_id", "cls"),
    [
        ("rest_countries", RestCountriesConnector),
        ("openalex_api", OpenAlexApiConnector),
        ("gdelt_doc_api", GdeltDocApiConnector),
    ],
)
def test_wave10_5d_connectors_medallion_extract(
    tmp_path: Path, source_id: str, cls: type
) -> None:
    """Each Wave 10.5D connector persists Bronze/Silver/Gold via extract_table(raw_records=...)."""
    settings = GatewaySettings(data_dir=tmp_path)
    assert source_id in list_registered_connectors()
    assert isinstance(get_connector(source_id), cls)
    conn = cls(settings=settings)
    table = conn.extract_table(raw_records=[])
    assert table.num_rows >= 1
    silver_file = tmp_path / source_id / "silver" / f"{conn.default_table}.parquet"
    assert silver_file.exists()


def test_wave10_5d_interfaces_parity(tmp_path: Path) -> None:
    """FastAPI and MCP expose Wave 10.5D Block 4 tools and endpoints."""
    app = create_app(GatewaySettings(data_dir=tmp_path))
    routes = {getattr(r, "path", "") for r in app.routes}
    assert "/api/v1/public-apis/global/country/{code}" in routes
    assert "/api/v1/public-apis/global/openalex" in routes
    assert "/api/v1/public-apis/global/gdelt" in routes

    dispatcher = MCPDispatcher(settings=GatewaySettings(data_dir=tmp_path))
    tools_res = dispatcher.handle_request({"jsonrpc": "2.0", "id": 1, "method": "tools/list"})
    tool_names = {t["name"] for t in tools_res["result"]["tools"]}
    for expected in ("bdg_lookup_country", "bdg_search_openalex_works", "bdg_query_gdelt_doc"):
        assert expected in tool_names

"""Offline Medallion & 4-interface parity tests for Wave 10.5C (Block 1 KYC/Geo/Compliance)."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from public_data_gateway.api.app import create_app
from public_data_gateway.connectors.compliance.bgpview_ip_intel import BgpviewIpIntelConnector
from public_data_gateway.connectors.compliance.rdap_registro_br import RdapRegistroBrConnector
from public_data_gateway.connectors.empresarial.cnpja_open import CnpjaOpenConnector
from public_data_gateway.connectors.empresarial.company_enrich_apollo_bdc import (
    CompanyEnrichConnector,
)
from public_data_gateway.connectors.empresarial.gleif_lei import GleifLeiConnector
from public_data_gateway.connectors.empresarial.minha_receita import MinhaReceitaConnector
from public_data_gateway.connectors.empresarial.receitaws import ReceitaWsConnector
from public_data_gateway.connectors.geografico.nominatim_osm import NominatimOsmConnector
from public_data_gateway.connectors.geografico.opencep import OpenCepConnector
from public_data_gateway.connectors.geografico.viacep import ViaCepConnector
from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.core.connector import get_connector, list_registered_connectors
from public_data_gateway.mcp.server import MCPDispatcher


@pytest.mark.parametrize(
    ("source_id", "cls"),
    [
        ("minha_receita", MinhaReceitaConnector),
        ("cnpja_open", CnpjaOpenConnector),
        ("receitaws", ReceitaWsConnector),
        ("gleif_lei", GleifLeiConnector),
        ("company_enrich_apollo_bdc", CompanyEnrichConnector),
        ("viacep", ViaCepConnector),
        ("opencep", OpenCepConnector),
        ("nominatim_osm", NominatimOsmConnector),
        ("rdap_registro_br", RdapRegistroBrConnector),
        ("bgpview_ip_intel", BgpviewIpIntelConnector),
    ],
)
def test_wave10_5c_connectors_medallion_extract(
    tmp_path: Path, source_id: str, cls: type
) -> None:
    """Each Wave 10.5C connector persists Bronze/Silver/Gold via extract_table(raw_records=...)."""
    settings = GatewaySettings(data_dir=tmp_path)
    assert source_id in list_registered_connectors()
    assert isinstance(get_connector(source_id), cls)
    conn = cls(settings=settings)
    table = conn.extract_table(raw_records=[])
    assert table.num_rows >= 1
    silver_file = tmp_path / source_id / "silver" / f"{conn.default_table}.parquet"
    assert silver_file.exists()


def test_wave10_5c_interfaces_parity(tmp_path: Path) -> None:
    """FastAPI and MCP expose Wave 10.5C Block 1 tools and endpoints."""
    app = create_app(GatewaySettings(data_dir=tmp_path))
    routes = {getattr(r, "path", "") for r in app.routes}
    assert "/api/v1/public-apis/geo/nominatim" in routes
    assert "/api/v1/public-apis/compliance/rdap/{domain}" in routes
    assert "/api/v1/public-apis/kyc/gleif" in routes
    assert "/api/v1/public-apis/compliance/ip/{ip}" in routes
    assert "/api/v1/public-apis/kyc/b2b-domain/{domain}" in routes

    dispatcher = MCPDispatcher(settings=GatewaySettings(data_dir=tmp_path))
    tools_res = dispatcher.handle_request({"jsonrpc": "2.0", "id": 1, "method": "tools/list"})
    tool_names = {t["name"] for t in tools_res["result"]["tools"]}
    for expected in (
        "bdg_geocode_nominatim",
        "bdg_lookup_rdap_registro_br",
        "bdg_lookup_gleif_lei",
        "bdg_lookup_ip_intel",
        "bdg_lookup_b2b_domain_profile",
    ):
        assert expected in tool_names

    b2b_res = dispatcher.handle_request(
        {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {
                "name": "bdg_lookup_b2b_domain_profile",
                "arguments": {"domain": "petrobras.com.br"},
            },
        }
    )
    text = b2b_res["result"]["content"][0]["text"]
    assert "company_enrich_apollo_bdc" in text
    assert json.loads(json.dumps(text))

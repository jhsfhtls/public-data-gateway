"""Deterministic offline tests for Phase 10 Public APIs Hub & Bipartite Catalog."""

from __future__ import annotations

import json
from typing import Any

import httpx
import pytest
from fastapi.testclient import TestClient

from public_data_gateway.api.app import create_app
from public_data_gateway.cli.main import main as cli_main
from public_data_gateway.connectors.public_apis_hub import PublicApisHubService
from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.core.http_client import ResilientHttpClient
from public_data_gateway.mcp.server import MCPDispatcher
from public_data_gateway.sdk import BrazilData


def _make_hub(handler: Any, tmp_path: Any) -> PublicApisHubService:
    settings = GatewaySettings(data_dir=tmp_path / "data")
    transport = httpx.MockTransport(handler)
    client = httpx.Client(transport=transport, timeout=5.0)
    http_client = ResilientHttpClient(settings=settings)
    http_client._client = client
    return PublicApisHubService(settings=settings, http_client=http_client)


def test_bipartite_public_apis_catalog_sync_and_filter(tmp_path: Any) -> None:
    hub = _make_hub(lambda req: httpx.Response(200, json={}), tmp_path)
    full = hub.load_catalog()
    assert full["total"] == 29
    assert "brasilapi_core" in full["already_homologated_gov_br"]

    bloco1 = hub.load_catalog(block="BLOCO_1_KYC_GEO")
    assert bloco1["total"] == 10
    p03 = hub.load_catalog(pack_id="P03")
    assert any(a["api_id"] == "overpass_osm" for a in p03["apis"])


def test_cnpj_cascade_fallback_minha_receita_to_cnpja(tmp_path: Any) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        host = request.url.host
        if host == "minhareceita.org":
            return httpx.Response(503, json={"error": "upstream busy"})
        if host == "open.cnpja.com":
            return httpx.Response(
                200,
                json={
                    "company": {"name": "BAZAR EXEMPLO LTDA", "members": [{"name": "SOCIO A"}]},
                    "status": {"text": "Ativa"},
                    "address": {"state": "SP", "city": "Sao Paulo"},
                    "mainActivity": {"id": 4761003},
                },
            )
        return httpx.Response(404, json={})

    hub = _make_hub(handler, tmp_path)
    res = hub.enrich_cnpj_cascade("11.222.333/0001-81")
    assert res["provider_used"] == "cnpja_open"
    assert res["razao_social"] == "BAZAR EXEMPLO LTDA"
    assert res["qsa_count"] == 1
    assert res["uf"] == "SP"

    with pytest.raises(ValueError, match="Invalid CNPJ"):
        hub.enrich_cnpj_cascade("123")


def test_cep_cascade_fallback_viacep_to_opencep(tmp_path: Any) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.host == "viacep.com.br":
            return httpx.Response(200, json={"erro": True})
        if request.url.host == "opencep.com":
            return httpx.Response(
                200,
                json={
                    "cep": "01310-100",
                    "logradouro": "Avenida Paulista",
                    "bairro": "Bela Vista",
                    "localidade": "Sao Paulo",
                    "uf": "SP",
                    "ibge": "3550308",
                },
            )
        return httpx.Response(404, json={})

    hub = _make_hub(handler, tmp_path)
    res = hub.lookup_cep_cascade("01310-100")
    assert res["provider_used"] == "opencep"
    assert res["ibge"] == "3550308"
    assert res["uf"] == "SP"


def test_kyc_geo_endpoints(tmp_path: Any) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        host = request.url.host
        if host == "nominatim.openstreetmap.org":
            return httpx.Response(200, json=[{"display_name": "Praca da Se", "lat": "-23.55", "lon": "-46.63", "type": "square"}])
        if host == "rdap.registro.br":
            return httpx.Response(
                200,
                json={
                    "ldhName": "exemplo.com.br",
                    "status": ["active"],
                    "events": [{"eventAction": "registration", "eventDate": "2010-01-01T00:00:00Z"}],
                    "nameservers": [{"ldhName": "ns1.exemplo.com.br"}],
                },
            )
        if host == "api.gleif.org":
            return httpx.Response(
                200,
                json={
                    "data": [
                        {
                            "id": "5493001KJTIIGC8Y1R12",
                            "attributes": {
                                "entity": {
                                    "legalName": {"name": "GLOBAL CORP"},
                                    "legalAddress": {"country": "BR"},
                                    "status": "ACTIVE",
                                }
                            },
                        }
                    ]
                },
            )
        if host == "api.bgpview.io":
            return httpx.Response(
                200,
                json={"data": {"ip": "8.8.8.8", "ptr_record": "dns.google", "prefixes": [{"asn": {"asn": 15169, "name": "GOOGLE", "country_code": "US"}}]}},
            )
        return httpx.Response(404, json={})

    hub = _make_hub(handler, tmp_path)
    geo = hub.geocode_nominatim("Praca da Se, Sao Paulo")
    assert geo[0]["lat"] == pytest.approx(-23.55)

    rdap = hub.lookup_rdap_registro_br("exemplo.com.br")
    assert rdap["registered_at"] == "2010-01-01T00:00:00Z"

    lei = hub.lookup_gleif_lei("GLOBAL CORP")
    assert lei[0]["lei"] == "5493001KJTIIGC8Y1R12"

    ip_info = hub.lookup_ip_intel("8.8.8.8")
    assert ip_info["asn"] == 15169


def test_market_regulatory_endpoints(tmp_path: Any) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        host = request.url.host
        if host == "brapi.dev":
            return httpx.Response(200, json={"results": [{"symbol": "PETR4", "shortName": "PETROBRAS PN", "regularMarketPrice": 38.5, "regularMarketChangePercent": 1.2, "marketCap": 500000000000}]})
        if host == "economia.awesomeapi.com.br":
            return httpx.Response(200, json={"USDBRL": {"bid": "5.75"}})
        if host == "www.ipeadata.gov.br":
            return httpx.Response(200, json={"value": [{"VALDATA": "2026-01-01", "VALVALOR": 100.5}]})
        if host == "dadosabertos.aneel.gov.br":
            return httpx.Response(200, json={"result": {"records": [{"SigAgente": "ENEL SP"}]}})
        if host == "dadosabertos.camara.leg.br":
            return httpx.Response(200, json={"dados": [{"id": 1, "nome": "Dep Exemplo"}]})
        if host == "api.worldbank.org":
            return httpx.Response(200, json=[{"page": 1}, [{"country": {"id": "BR"}, "indicator": {"id": "NY.GDP.MKTP.KD.ZG"}, "date": "2025", "value": 2.9}]])
        if host == "ghoapi.azureedge.net":
            return httpx.Response(200, json={"value": [{"SpatialDim": "BRA", "NumericValue": 76.2}]})
        return httpx.Response(404, json={})

    hub = _make_hub(handler, tmp_path)
    assert hub.fetch_brapi_quote("PETR4")[0]["price"] == 38.5
    assert hub.fetch_awesomeapi_rates("USD-BRL")["USDBRL"]["bid"] == "5.75"
    assert hub.fetch_ipeadata_series("PAN12_IPCAG12")[0]["VALVALOR"] == 100.5
    assert hub.query_ckan_datastore("res-123", portal="aneel")[0]["SigAgente"] == "ENEL SP"
    assert hub.fetch_camara_deputados()[0]["nome"] == "Dep Exemplo"
    assert hub.fetch_world_bank_indicator()[0]["value"] == 2.9
    assert hub.fetch_who_gho_indicator()[0]["SpatialDim"] == "BRA"


def test_spatial_ml_and_global_research(tmp_path: Any, monkeypatch: pytest.MonkeyPatch) -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        host = request.url.host
        if host == "overpass-api.de":
            return httpx.Response(200, json={"elements": [{"type": "count", "tags": {"total": "14", "nodes": "10", "ways": "4"}}]})
        if host == "router.project-osrm.org":
            return httpx.Response(200, json={"routes": [{"distance": 4250.0, "duration": 660.0}]})
        if host == "api.open-meteo.com":
            return httpx.Response(200, json={"daily": {"temperature_2m_max": [28.4]}})
        if host == "power.larc.nasa.gov":
            return httpx.Response(200, json={"properties": {"parameter": {"ALLSKY_SFC_SW_DWN": {"20260101": 5.4}}}})
        if host == "api.open-elevation.com":
            return httpx.Response(200, json={"results": [{"elevation": 760.0}]})
        if host == "router.huggingface.co":
            return httpx.Response(200, json=[[{"label": "POSITIVE", "score": 0.99}]])
        if host == "api.nasa.gov":
            return httpx.Response(200, json={"title": "Nebula", "media_type": "image"})
        if host == "restcountries.com":
            return httpx.Response(200, json=[{"cca2": "BR", "cca3": "BRA", "name": {"common": "Brazil"}, "region": "Americas", "subregion": "South America", "population": 212000000, "currencies": {"BRL": {}}, "borders": ["ARG"]}])
        if host == "api.openalex.org":
            return httpx.Response(200, json={"results": [{"id": "W1", "title": "Urban Retail", "publication_year": 2025, "cited_by_count": 12, "doi": "10.1/x"}]})
        if host == "api.gdeltproject.org":
            return httpx.Response(200, json={"articles": [{"title": "Brazil Economy", "url": "https://example.org"}]})
        return httpx.Response(404, json={})

    hub = _make_hub(handler, tmp_path)
    pois = hub.count_osm_pois_radius(-23.55, -46.63, amenity="school", radius_m=1000)
    assert pois["total_pois"] == 14

    route = hub.compute_osrm_route(-23.55, -46.63, -23.56, -46.65)
    assert route["distance_km"] == 4.25
    assert route["duration_min"] == 11.0

    assert hub.fetch_open_meteo_forecast(-23.55, -46.63)["daily"]["temperature_2m_max"] == [28.4]
    assert "properties" in hub.fetch_nasa_power_daily(-23.55, -46.63, start="20260101", end="20260102")
    assert hub.fetch_elevation(-23.55, -46.63)["elevation_m"] == 760.0
    assert hub.fetch_nasa_apod()["title"] == "Nebula"
    assert hub.describe_upgini_features()["package"] == "upgini"

    monkeypatch.delenv("HF_TOKEN", raising=False)
    with pytest.raises(ValueError, match="HF_TOKEN"):
        hub.run_hf_inference("distilbert", "otimo servico")
    monkeypatch.setenv("HF_TOKEN", "test-token")
    assert hub.run_hf_inference("distilbert", "otimo servico")[0][0]["label"] == "POSITIVE"

    assert hub.lookup_country("BRA")["cca2"] == "BR"
    assert hub.search_openalex_works("retail")[0]["title"] == "Urban Retail"
    assert hub.query_gdelt_doc("Brazil")[0]["title"] == "Brazil Economy"


def test_sdk_fastapi_cli_mcp_integration(capsys: Any) -> None:
    sdk = BrazilData()
    cat = sdk.public_apis.list_catalog(block="BLOCO_3_ESPACIAL_CLIMA_ML")
    assert cat["total"] == 9

    app = create_app()
    client = TestClient(app)
    resp = client.get("/api/v1/public-apis/catalog?pack_id=P04")
    assert resp.status_code == 200
    assert resp.json()["total"] >= 5

    rc = cli_main(["public-apis", "catalog", "--pack", "P03", "--json"])
    assert rc == 0
    captured = json.loads(capsys.readouterr().out)
    assert captured["total"] >= 4

    dispatcher = MCPDispatcher()
    tools_res = dispatcher.handle_request({"jsonrpc": "2.0", "id": 1, "method": "tools/list"})
    tool_names = {t["name"] for t in tools_res["result"]["tools"]}
    assert {
        "bdg_list_public_apis_catalog",
        "bdg_enrich_cnpj_cascade",
        "bdg_count_osm_pois_radius",
    }.issubset(tool_names)

    call_res = dispatcher.handle_request(
        {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {"name": "bdg_list_public_apis_catalog", "arguments": {"pack_id": "P05"}},
        }
    )
    assert call_res["result"]["isError"] is False

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

from public_data_gateway.api.app import create_app
from public_data_gateway.cli.main import main as cli_main
from public_data_gateway.connectors.agregadores.dadosgov_ckan import DadosGovCkanConnector
from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.mcp.server import MCPDispatcher
from public_data_gateway.sdk import BrazilData


def _isolated_settings(tmp_path: Path) -> GatewaySettings:
    settings = GatewaySettings(data_dir=tmp_path / "data")
    DadosGovCkanConnector(settings=settings).materialize_all(force=True)
    return settings


def test_dadosgov_ckan_extract_and_materialize_tables(tmp_path: Path) -> None:
    conn = DadosGovCkanConnector(settings=_isolated_settings(tmp_path))
    outputs = conn.materialize_all(force=True)
    assert len(outputs) == 5

    orgs = conn.extract("ckan_organizacoes_publicas_municipio")
    formats = conn.extract("ckan_formatos_recursos_tema")
    gov = conn.extract("ckan_licencas_governanca_lai")
    eng = conn.extract("ckan_engajamento_reuso_api")
    feats = conn.extract("features_dadosgov_ckan_transparencia")

    assert len(orgs) == 12
    assert len(formats) == 48
    assert len(gov) == 12
    assert len(eng) == 12
    assert len(feats) == 12


def test_dadosgov_ckan_curitiba_and_tier_calibration(tmp_path: Path) -> None:
    conn = DadosGovCkanConnector(settings=_isolated_settings(tmp_path))
    curitiba = conn.get_ckan_features("4106902")
    assert curitiba["nome_municipio"] == "Curitiba"
    assert curitiba["score_transparencia_ckan"] == 89.5
    assert curitiba["tier_transparencia_ckan"] == "CKAN_TRANSPARENCIA_OURO_LAI"

    ranking = conn.list_top_ckan_municipalities(limit=12)
    tiers = {row["tier_transparencia_ckan"] for row in ranking}
    assert tiers == {
        "CKAN_TRANSPARENCIA_OURO_LAI",
        "CKAN_CATALOGO_ABERTO_CONSOLIDADO",
        "CKAN_ABERTURA_PARCIAL_EXPANSAO",
        "CKAN_DEFICIT_DADOS_ABERTOS",
    }


def test_dadosgov_ckan_cep_and_ibge6_normalization(tmp_path: Path) -> None:
    conn = DadosGovCkanConnector(settings=_isolated_settings(tmp_path))
    by_cep = conn.get_ckan_features("80010-010")
    by_ibge6 = conn.get_ckan_features("410690")
    assert by_cep["id_municipio"] == "4106902"
    assert by_ibge6["id_municipio"] == "4106902"

    with pytest.raises(ValueError):
        conn.get_ckan_features("")

    with pytest.raises(KeyError):
        conn.get_ckan_features("9999999")


def test_dadosgov_ckan_fetch_and_format_filter(tmp_path: Path) -> None:
    conn = DadosGovCkanConnector(settings=_isolated_settings(tmp_path))
    payload = conn.fetch("4106902")
    assert payload["source_id"] == "dadosgov_ckan"
    assert set(payload.keys()) >= {
        "organizacoes_publicas",
        "formatos_recursos",
        "governanca_lai",
        "engajamento_reuso",
        "features_ckan",
    }

    csv_formats = conn.list_ckan_formats("4106902", formato_recurso_ckan="CSV_Estruturado")
    assert len(csv_formats) == 1
    assert csv_formats[0]["formato_recurso_ckan"] == "CSV_Estruturado"


def test_dadosgov_ckan_test_connection_mocked(tmp_path: Path) -> None:
    conn = DadosGovCkanConnector(settings=_isolated_settings(tmp_path))
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    with patch(
        "public_data_gateway.connectors.agregadores.dadosgov_ckan.connector.requests.get",
        return_value=mock_resp,
    ):
        probe = conn.test_connection()
    assert probe["ok"] is True
    assert probe["portal_status"] == 200
    assert probe["catalog_status"] == 200


def test_dadosgov_ckan_sdk_facade_parity(tmp_path: Path) -> None:
    sdk = BrazilData(settings=_isolated_settings(tmp_path))
    assert sdk.metaportals.get_ckan_organizations("4106902")["id_municipio"] == "4106902"
    assert len(sdk.list_ckan_formats("4106902")) == 4
    assert sdk.get_ckan_governance("4106902")["id_municipio"] == "4106902"
    assert sdk.get_ckan_engagement("4106902")["id_municipio"] == "4106902"
    assert sdk.get_ckan_features("4106902")["tier_transparencia_ckan"] == "CKAN_TRANSPARENCIA_OURO_LAI"
    assert len(sdk.list_top_ckan_municipalities(limit=3, sigla_uf="PR")) == 1


def test_dadosgov_ckan_api_routes(tmp_path: Path) -> None:
    client = TestClient(create_app(settings=_isolated_settings(tmp_path)))

    r_org = client.get("/api/v1/metaportals/dadosgov/organizations/4106902")
    assert r_org.status_code == 200
    assert r_org.json()["id_municipio"] == "4106902"

    r_alias = client.get("/api/v1/dadosgov/4106902")
    assert r_alias.status_code == 200

    r_fmt = client.get("/api/v1/metaportals/dadosgov/formats/4106902?formato=CSV_Estruturado")
    assert r_fmt.status_code == 200
    assert len(r_fmt.json()) == 1

    r_gov = client.get("/api/v1/metaportals/dadosgov/governance/4106902")
    assert r_gov.status_code == 200

    r_eng = client.get("/api/v1/metaportals/dadosgov/engagement/4106902")
    assert r_eng.status_code == 200

    r_feat = client.get("/api/v1/metaportals/dadosgov/features/4106902")
    assert r_feat.status_code == 200
    assert r_feat.json()["score_transparencia_ckan"] == 89.5

    r_top = client.get("/api/v1/metaportals/dadosgov/top-municipalities?limit=5")
    assert r_top.status_code == 200
    assert len(r_top.json()) == 5

    r_err = client.get("/api/v1/metaportals/dadosgov/features/9999999")
    assert r_err.status_code == 400


def test_dadosgov_ckan_cli_and_mcp_parity(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    with patch.object(sys, "argv", ["bdg", "dadosgov", "features", "4106902", "--json"]):
        rc = cli_main()
    assert rc == 0
    out = json.loads(capsys.readouterr().out)
    assert out["score_transparencia_ckan"] == 89.5

    with patch.object(sys, "argv", ["bdg", "dadosgov", "ranking", "--limit", "3", "--json"]):
        rc_rk = cli_main()
    assert rc_rk == 0
    rk_out = json.loads(capsys.readouterr().out)
    assert len(rk_out) == 3

    server = MCPDispatcher(settings=_isolated_settings(tmp_path))
    tools_resp = server.handle_request({"jsonrpc": "2.0", "id": 1, "method": "tools/list"})
    tool_names = {t["name"] for t in tools_resp["result"]["tools"]}
    assert {
        "bdg_get_ckan_organizations",
        "bdg_get_ckan_governance",
        "bdg_get_ckan_features",
        "bdg_list_top_ckan_municipalities",
    }.issubset(tool_names)

    call_resp = server.handle_request(
        {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {
                "name": "bdg_get_ckan_features",
                "arguments": {"id_municipio": "4106902"},
            },
        }
    )
    assert call_resp["result"]["isError"] is False
    text = call_resp["result"]["content"][0]["text"]
    assert "dadosgov_ckan" in text
    assert "CKAN_TRANSPARENCIA_OURO_LAI" in text

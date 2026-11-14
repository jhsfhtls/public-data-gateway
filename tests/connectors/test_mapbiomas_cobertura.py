"""Testes offline determinísticos do conector MapBiomas (Cobertura e Uso da Terra)."""

from __future__ import annotations

import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from public_data_gateway.api.app import create_app
from public_data_gateway.cli.main import main as cli_main
from public_data_gateway.connectors.ambiental.mapbiomas_cobertura import (
    MapBiomasCoberturaConnector,
)
from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.core.connector import list_registered_connectors
from public_data_gateway.mcp.server import MCPDispatcher
from public_data_gateway.sdk import BrazilData


def _settings_with_data(tmp_path: Path) -> GatewaySettings:
    settings = GatewaySettings(data_dir=tmp_path / "data")
    MapBiomasCoberturaConnector(settings=settings).materialize_all(force=True)
    return settings


def test_connector_is_registered() -> None:
    connectors = list_registered_connectors()
    assert "mapbiomas_cobertura" in connectors


def test_materialize_all_and_tier_balance(tmp_path: Path) -> None:
    settings = GatewaySettings(data_dir=tmp_path / "data")
    connector = MapBiomasCoberturaConnector(settings=settings)
    outputs = connector.materialize_all(force=True)

    assert len(outputs) == 5
    for path in outputs.values():
        assert path.exists()

    cob = connector.extract("mapbiomas_cobertura_anual_municipio")
    trans = connector.extract("mapbiomas_transicoes_uso_terra_classe")
    agro = connector.extract("mapbiomas_agropecuaria_pastagem_irrigacao")
    alertas = connector.extract("mapbiomas_alerta_desmatamento_validado")
    feats = connector.extract("features_mapbiomas_cobertura_solo")

    assert len(cob) == 12
    assert len(trans) == 48
    assert len(agro) == 12
    assert len(alertas) == 12
    assert len(feats) == 12

    tiers = Counter(r["tier_sustentabilidade_mapbiomas"] for r in feats)
    assert dict(tiers) == {
        "PATRIMONIO_NATIVO_ESTAVEL": 3,
        "TRANSICAO_PRODUTIVA_EQUILIBRADA": 3,
        "EXPANSAO_AGRO_EM_ATENCAO": 3,
        "PRESSAO_CONVERSAO_CRITICA": 3,
    }


def test_fetch_and_cep_ibge6_resolution(tmp_path: Path) -> None:
    settings = _settings_with_data(tmp_path)
    connector = MapBiomasCoberturaConnector(settings=settings)

    by_ibge = connector.fetch("1302603")
    by_cep = connector.fetch("69005-010")
    by_ibge6 = connector.fetch("130260")

    assert by_ibge["nome_municipio"] == "Manaus"
    assert by_cep["id_municipio"] == "1302603"
    assert by_ibge6["id_municipio"] == "1302603"
    assert len(by_ibge["transicoes"]) == 4
    assert by_ibge["features_cobertura"]["score_sustentabilidade_mapbiomas"] == 89.0


def test_sdk_environment_facade(tmp_path: Path) -> None:
    settings = _settings_with_data(tmp_path)
    sdk = BrazilData(settings=settings)

    lc = sdk.get_mapbiomas_land_cover("4106902")
    tr = sdk.list_mapbiomas_transitions("4106902", classe_transicao_mapbiomas="Floresta_Para_Pastagem")
    ag = sdk.get_mapbiomas_agriculture("4106902")
    al = sdk.get_mapbiomas_alerts("4106902")
    ft = sdk.get_mapbiomas_features("4106902")
    top = sdk.list_top_mapbiomas_municipalities(limit=3)

    assert lc["nome_municipio"] == "Curitiba"
    assert len(tr) == 1
    assert tr[0]["classe_transicao_mapbiomas"] == "Floresta_Para_Pastagem"
    assert ag["area_irrigada_ha"] == 840.0
    assert al["qtd_alertas_mapbiomas_ano"] == 5
    assert ft["tier_sustentabilidade_mapbiomas"] == "PATRIMONIO_NATIVO_ESTAVEL"
    assert len(top) == 3
    assert top[0]["id_municipio"] == "1302603"


def test_fastapi_routes(tmp_path: Path) -> None:
    app = create_app(settings=_settings_with_data(tmp_path))
    client = TestClient(app)

    r_top = client.get("/api/v1/environment/mapbiomas/top-municipalities?limit=2")
    assert r_top.status_code == 200
    assert len(r_top.json()) == 2

    r_feat = client.get("/api/v1/environment/mapbiomas/features/1302603")
    assert r_feat.status_code == 200
    assert r_feat.json()["score_sustentabilidade_mapbiomas"] == 89.0

    r_tr = client.get("/api/v1/environment/mapbiomas/transitions/1302603?classe=Floresta_Para_Pastagem")
    assert r_tr.status_code == 200
    assert len(r_tr.json()) == 1

    r_ag = client.get("/api/v1/environment/mapbiomas/agriculture/1302603")
    assert r_ag.status_code == 200
    assert r_ag.json()["id_municipio"] == "1302603"

    r_al = client.get("/api/v1/environment/mapbiomas/alerts/1302603")
    assert r_al.status_code == 200
    assert r_al.json()["qtd_alertas_mapbiomas_ano"] == 14

    r_alias = client.get("/api/v1/mapbiomas/1302603")
    assert r_alias.status_code == 200
    assert r_alias.json()["cobertura_nativa_pct"] == 88.6


def test_cli_mapbiomas_commands(tmp_path: Path, monkeypatch: Any, capsys: Any) -> None:
    data_dir = tmp_path / "data"
    MapBiomasCoberturaConnector(settings=GatewaySettings(data_dir=data_dir)).materialize_all(force=True)
    monkeypatch.setenv("PDG_DATA_DIR", str(data_dir))
    monkeypatch.setenv("BDG_DATA_DIR", str(data_dir))

    for argv in [
        ["bdg", "mapbiomas", "land-cover", "1302603", "--json"],
        ["bdg", "mapbiomas", "transitions", "1302603", "--classe", "Floresta_Para_Pastagem", "--json"],
        ["bdg", "mapbiomas", "agriculture", "1302603", "--json"],
        ["bdg", "mapbiomas", "alerts", "1302603", "--json"],
        ["bdg", "mapbiomas", "features", "1302603", "--json"],
        ["bdg", "mapbiomas", "ranking", "--limit", "3", "--json"],
    ]:
        monkeypatch.setattr(sys, "argv", argv)
        rc = cli_main()
        assert rc == 0
        captured = capsys.readouterr()
        payload = json.loads(captured.out)
        assert payload


def test_mcp_mapbiomas_tools(tmp_path: Path) -> None:
    dispatcher = MCPDispatcher(settings=_settings_with_data(tmp_path))

    tools_resp = dispatcher.handle_request({"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}})
    tool_names = {t["name"] for t in tools_resp["result"]["tools"]}
    expected = {
        "bdg_get_mapbiomas_land_cover",
        "bdg_get_mapbiomas_alerts",
        "bdg_get_mapbiomas_features",
        "bdg_list_top_mapbiomas_municipalities",
    }
    assert expected.issubset(tool_names)

    for idx, (tool_name, args) in enumerate(
        [
            ("bdg_get_mapbiomas_land_cover", {"id_municipio": "1302603"}),
            ("bdg_get_mapbiomas_alerts", {"id_municipio": "1302603"}),
            ("bdg_get_mapbiomas_features", {"id_municipio": "1302603"}),
            ("bdg_list_top_mapbiomas_municipalities", {"limit": 2}),
        ],
        start=10,
    ):
        resp = dispatcher.handle_request(
            {
                "jsonrpc": "2.0",
                "id": idx,
                "method": "tools/call",
                "params": {"name": tool_name, "arguments": args},
            }
        )
        assert resp["result"]["isError"] is False
        text = resp["result"]["content"][0]["text"]
        assert "mapbiomas_cobertura" in text


def test_invalid_municipality_raises_key_error(tmp_path: Path) -> None:
    connector = MapBiomasCoberturaConnector(settings=_settings_with_data(tmp_path))
    with pytest.raises(KeyError):
        connector.fetch("9999999")

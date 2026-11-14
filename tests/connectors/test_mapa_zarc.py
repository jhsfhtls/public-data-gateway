"""Testes offline determinísticos do conector MAPA ZARC (Zoneamento Agrícola de Risco Climático)."""

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
from public_data_gateway.connectors.ambiental.mapa_zarc import MapaZarcConnector
from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.core.connector import list_registered_connectors
from public_data_gateway.mcp.server import MCPDispatcher
from public_data_gateway.sdk import BrazilData


def _settings_with_data(tmp_path: Path) -> GatewaySettings:
    settings = GatewaySettings(data_dir=tmp_path / "data")
    MapaZarcConnector(settings=settings).materialize_all(force=True)
    return settings


def test_connector_is_registered() -> None:
    connectors = list_registered_connectors()
    assert "mapa_zarc" in connectors


def test_materialize_all_and_tier_balance(tmp_path: Path) -> None:
    settings = GatewaySettings(data_dir=tmp_path / "data")
    connector = MapaZarcConnector(settings=settings)
    outputs = connector.materialize_all(force=True)

    assert len(outputs) == 5
    for path in outputs.values():
        assert path.exists()

    apt = connector.extract("zarc_aptidao_climatica_municipio")
    jan = connector.extract("zarc_janelas_plantio_cultura")
    pro = connector.extract("zarc_proagro_seguro_rural_municipio")
    solo = connector.extract("zarc_risco_solo_ad_biomassa")
    feats = connector.extract("features_mapa_zarc_agroclimatico")

    assert len(apt) == 12
    assert len(jan) == 48
    assert len(pro) == 12
    assert len(solo) == 12
    assert len(feats) == 12

    tiers = Counter(r["tier_risco_zarc"] for r in feats)
    assert dict(tiers) == {
        "ZARC_BAIXO_RISCO_PROAGRO_OURO": 3,
        "ZARC_APTIDAO_MODERADA_REGULAR": 3,
        "ZARC_RISCO_HIDRICO_EM_MONITORAMENTO": 3,
        "ZARC_VULNERABILIDADE_CLIMATICA_ALTA": 3,
    }


def test_fetch_and_cep_ibge6_resolution(tmp_path: Path) -> None:
    settings = _settings_with_data(tmp_path)
    connector = MapaZarcConnector(settings=settings)

    by_ibge = connector.fetch("4106902")
    by_cep = connector.fetch("80010-010")
    by_ibge6 = connector.fetch("410690")

    assert by_ibge["nome_municipio"] == "Curitiba"
    assert by_cep["id_municipio"] == "4106902"
    assert by_ibge6["id_municipio"] == "4106902"
    assert len(by_ibge["janelas_plantio"]) == 4
    assert by_ibge["features_zarc"]["score_aptidao_zarc"] == 88.4


def test_sdk_environment_facade(tmp_path: Path) -> None:
    settings = _settings_with_data(tmp_path)
    sdk = BrazilData(settings=settings)

    cl = sdk.get_zarc_climate_aptitude("4106902")
    wn = sdk.list_zarc_planting_windows("4106902", cultura_zarc="Soja_Grao")
    pr = sdk.get_zarc_proagro_coverage("4106902")
    sl = sdk.get_zarc_soil_risk("4106902")
    ft = sdk.get_zarc_features("4106902")
    top = sdk.list_top_zarc_municipalities(limit=3)

    assert cl["nome_municipio"] == "Curitiba"
    assert len(wn) == 1
    assert wn[0]["cultura_zarc"] == "Soja_Grao"
    assert pr["adesao_zarc_credito_custeio_pct"] == 95.8
    assert sl["capacidade_retencao_agua_solo_pct"] == 88.4
    assert ft["tier_risco_zarc"] == "ZARC_BAIXO_RISCO_PROAGRO_OURO"
    assert len(top) == 3
    assert top[0]["id_municipio"] == "4106902"


def test_fastapi_routes(tmp_path: Path) -> None:
    app = create_app(settings=_settings_with_data(tmp_path))
    client = TestClient(app)

    r_top = client.get("/api/v1/environment/zarc/top-municipalities?limit=2")
    assert r_top.status_code == 200
    assert len(r_top.json()) == 2

    r_feat = client.get("/api/v1/environment/zarc/features/4106902")
    assert r_feat.status_code == 200
    assert r_feat.json()["score_aptidao_zarc"] == 88.4

    r_wn = client.get(
        "/api/v1/environment/zarc/windows/4106902?cultura=Soja_Grao"
    )
    assert r_wn.status_code == 200
    assert len(r_wn.json()) == 1

    r_pr = client.get("/api/v1/environment/zarc/proagro/4106902")
    assert r_pr.status_code == 200
    assert r_pr.json()["id_municipio"] == "4106902"

    r_sl = client.get("/api/v1/environment/zarc/soil/4106902")
    assert r_sl.status_code == 200
    assert r_sl.json()["capacidade_retencao_agua_solo_pct"] == 88.4

    r_alias = client.get("/api/v1/zarc/4106902")
    assert r_alias.status_code == 200
    assert r_alias.json()["qtd_culturas_zoneadas"] == 28


def test_cli_zarc_commands(tmp_path: Path, monkeypatch: Any, capsys: Any) -> None:
    data_dir = tmp_path / "data"
    MapaZarcConnector(settings=GatewaySettings(data_dir=data_dir)).materialize_all(
        force=True
    )
    monkeypatch.setenv("PDG_DATA_DIR", str(data_dir))
    monkeypatch.setenv("BDG_DATA_DIR", str(data_dir))

    for argv in [
        ["bdg", "zarc", "climate", "4106902", "--json"],
        ["bdg", "zarc", "windows", "4106902", "--cultura", "Soja_Grao", "--json"],
        ["bdg", "zarc", "proagro", "4106902", "--json"],
        ["bdg", "zarc", "soil", "4106902", "--json"],
        ["bdg", "zarc", "features", "4106902", "--json"],
        ["bdg", "zarc", "ranking", "--limit", "3", "--json"],
    ]:
        monkeypatch.setattr(sys, "argv", argv)
        rc = cli_main()
        assert rc == 0
        captured = capsys.readouterr()
        payload = json.loads(captured.out)
        assert payload


def test_mcp_zarc_tools(tmp_path: Path) -> None:
    dispatcher = MCPDispatcher(settings=_settings_with_data(tmp_path))

    tools_resp = dispatcher.handle_request(
        {"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}}
    )
    tool_names = {t["name"] for t in tools_resp["result"]["tools"]}
    expected = {
        "bdg_get_zarc_climate_aptitude",
        "bdg_get_zarc_proagro_coverage",
        "bdg_get_zarc_features",
        "bdg_list_top_zarc_municipalities",
    }
    assert expected.issubset(tool_names)

    for idx, (tool_name, args) in enumerate(
        [
            ("bdg_get_zarc_climate_aptitude", {"id_municipio": "4106902"}),
            ("bdg_get_zarc_proagro_coverage", {"id_municipio": "4106902"}),
            ("bdg_get_zarc_features", {"id_municipio": "4106902"}),
            ("bdg_list_top_zarc_municipalities", {"limit": 2}),
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
        assert "mapa_zarc" in text


def test_invalid_municipality_raises_key_error(tmp_path: Path) -> None:
    connector = MapaZarcConnector(settings=_settings_with_data(tmp_path))
    with pytest.raises(KeyError):
        connector.fetch("9999999")

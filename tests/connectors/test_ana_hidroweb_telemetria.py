"""Testes offline determinísticos do conector ANA HidroWeb (Telemetria & Segurança Hídrica)."""

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
from public_data_gateway.connectors.ambiental.ana_hidroweb_telemetria import (
    AnaHidrowebConnector,
)
from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.core.connector import list_registered_connectors
from public_data_gateway.mcp.server import MCPDispatcher
from public_data_gateway.sdk import BrazilData


def _settings_with_data(tmp_path: Path) -> GatewaySettings:
    settings = GatewaySettings(data_dir=tmp_path / "data")
    AnaHidrowebConnector(settings=settings).materialize_all(force=True)
    return settings


def test_connector_is_registered() -> None:
    connectors = list_registered_connectors()
    assert "ana_hidroweb_telemetria" in connectors


def test_materialize_all_and_tier_balance(tmp_path: Path) -> None:
    settings = GatewaySettings(data_dir=tmp_path / "data")
    connector = AnaHidrowebConnector(settings=settings)
    outputs = connector.materialize_all(force=True)

    assert len(outputs) == 5
    for path in outputs.values():
        assert path.exists()

    estacoes = connector.extract("hidroweb_estacoes_telemetricas_rede")
    cotas = connector.extract("hidroweb_cotas_vazoes_bacia")
    pluv = connector.extract("hidroweb_pluviometria_estiagem_cheias")
    qual = connector.extract("hidroweb_qualidade_seguranca_hidrica")
    feats = connector.extract("features_ana_hidroweb_hidrologia")

    assert len(estacoes) == 12
    assert len(cotas) == 48
    assert len(pluv) == 12
    assert len(qual) == 12
    assert len(feats) == 12

    tiers = Counter(r["tier_seguranca_hidrica_hidroweb"] for r in feats)
    assert dict(tiers) == {
        "SEGURANCA_HIDRICA_RESILIENTE": 3,
        "EQUILIBRIO_HIDROLOGICO_MODERADO": 3,
        "ALERTA_ESTRESSE_HIDRICO": 3,
        "CRITICO_ESCASSEZ_OU_CHEIA": 3,
    }


def test_fetch_and_cep_ibge6_resolution(tmp_path: Path) -> None:
    settings = _settings_with_data(tmp_path)
    connector = AnaHidrowebConnector(settings=settings)

    by_ibge = connector.fetch("4106902")
    by_cep = connector.fetch("80010-010")
    by_ibge6 = connector.fetch("410690")
    by_alias = connector.fetch("4106900")

    assert by_ibge["nome_municipio"] == "Curitiba"
    assert by_cep["id_municipio"] == "4106902"
    assert by_ibge6["id_municipio"] == "4106902"
    assert by_alias["id_municipio"] == "4106902"
    assert len(by_ibge["segmentos_cotas_vazoes"]) == 4
    assert by_ibge["features_hidrologia"]["score_seguranca_hidrica_hidroweb"] == 86.9


def test_sdk_environment_facade(tmp_path: Path) -> None:
    settings = _settings_with_data(tmp_path)
    sdk = BrazilData(settings=settings)

    st = sdk.get_hidroweb_stations("1302603")
    flows = sdk.list_hidroweb_flows("1302603", tipo_monitoramento_hidrologico="Rio_Principal")
    rain = sdk.get_hidroweb_rainfall("1302603")
    sec = sdk.get_hidroweb_water_security("1302603")
    feat = sdk.get_hidroweb_hydrology_features("1302603")
    top = sdk.list_top_hidroweb_municipalities(limit=3)

    assert st["nome_municipio"] == "Manaus"
    assert len(flows) == 1
    assert flows[0]["tipo_monitoramento_hidrologico"] == "Rio_Principal"
    assert rain["precipitacao_acumulada_anual_mm"] == 2340.0
    assert sec["indice_seguranca_hidrica_ish"] == 86.2
    assert feat["tier_seguranca_hidrica_hidroweb"] == "SEGURANCA_HIDRICA_RESILIENTE"
    assert len(top) == 3
    assert top[0]["id_municipio"] == "4106902"


def test_fastapi_routes(tmp_path: Path) -> None:
    app = create_app(settings=_settings_with_data(tmp_path))
    client = TestClient(app)

    r_top = client.get("/api/v1/environment/hidroweb/top-municipalities?limit=2")
    assert r_top.status_code == 200
    assert len(r_top.json()) == 2

    r_feat = client.get("/api/v1/environment/hidroweb/features/4106902")
    assert r_feat.status_code == 200
    assert r_feat.json()["score_seguranca_hidrica_hidroweb"] == 86.9

    r_flows = client.get("/api/v1/environment/hidroweb/flows/4106902?segmento=Rio_Principal")
    assert r_flows.status_code == 200
    assert len(r_flows.json()) == 1

    r_rain = client.get("/api/v1/environment/hidroweb/rainfall/4106902")
    assert r_rain.status_code == 200
    assert r_rain.json()["id_municipio"] == "4106902"

    r_sec = client.get("/api/v1/environment/hidroweb/security/4106902")
    assert r_sec.status_code == 200
    assert r_sec.json()["indice_qualidade_agua_iqa"] == 82.4

    r_alias = client.get("/api/v1/hidroweb/4106902")
    assert r_alias.status_code == 200
    assert r_alias.json()["qtd_estacoes_telemetricas_ativas"] == 32


def test_cli_hidroweb_commands(tmp_path: Path, monkeypatch: Any, capsys: Any) -> None:
    data_dir = tmp_path / "data"
    AnaHidrowebConnector(settings=GatewaySettings(data_dir=data_dir)).materialize_all(force=True)
    monkeypatch.setenv("PDG_DATA_DIR", str(data_dir))
    monkeypatch.setenv("BDG_DATA_DIR", str(data_dir))

    for argv in [
        ["bdg", "hidroweb", "stations", "4106902", "--json"],
        ["bdg", "hidroweb", "flows", "4106902", "--segmento", "Rio_Principal", "--json"],
        ["bdg", "hidroweb", "rainfall", "4106902", "--json"],
        ["bdg", "hidroweb", "security", "4106902", "--json"],
        ["bdg", "hidroweb", "features", "4106902", "--json"],
        ["bdg", "hidroweb", "ranking", "--limit", "3", "--json"],
    ]:
        monkeypatch.setattr(sys, "argv", argv)
        rc = cli_main()
        assert rc == 0
        captured = capsys.readouterr()
        payload = json.loads(captured.out)
        assert payload


def test_mcp_hidroweb_tools(tmp_path: Path) -> None:
    dispatcher = MCPDispatcher(settings=_settings_with_data(tmp_path))

    tools_resp = dispatcher.handle_request({"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}})
    tool_names = {t["name"] for t in tools_resp["result"]["tools"]}
    expected = {
        "bdg_get_hidroweb_stations",
        "bdg_get_hidroweb_water_security",
        "bdg_get_hidroweb_hydrology_features",
        "bdg_list_top_hidroweb_municipalities",
    }
    assert expected.issubset(tool_names)

    for idx, (tool_name, args) in enumerate(
        [
            ("bdg_get_hidroweb_stations", {"id_municipio": "4106902"}),
            ("bdg_get_hidroweb_water_security", {"id_municipio": "4106902"}),
            ("bdg_get_hidroweb_hydrology_features", {"id_municipio": "4106902"}),
            ("bdg_list_top_hidroweb_municipalities", {"limit": 2}),
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
        assert "ana_hidroweb_telemetria" in text


def test_invalid_municipality_raises_key_error(tmp_path: Path) -> None:
    connector = AnaHidrowebConnector(settings=_settings_with_data(tmp_path))
    with pytest.raises(KeyError):
        connector.fetch("9999999")

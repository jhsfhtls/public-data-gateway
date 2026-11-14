"""Testes offline determinísticos do conector SFB/SICAR (Cadastro Ambiental Rural)."""

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
from public_data_gateway.connectors.ambiental.sfb_sicar import SfbSicarConnector
from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.core.connector import list_registered_connectors
from public_data_gateway.mcp.server import MCPDispatcher
from public_data_gateway.sdk import BrazilData


def _settings_with_data(tmp_path: Path) -> GatewaySettings:
    settings = GatewaySettings(data_dir=tmp_path / "data")
    SfbSicarConnector(settings=settings).materialize_all(force=True)
    return settings


def test_connector_is_registered() -> None:
    connectors = list_registered_connectors()
    assert "sfb_sicar" in connectors


def test_materialize_all_and_tier_balance(tmp_path: Path) -> None:
    settings = GatewaySettings(data_dir=tmp_path / "data")
    connector = SfbSicarConnector(settings=settings)
    outputs = connector.materialize_all(force=True)

    assert len(outputs) == 5
    for path in outputs.values():
        assert path.exists()

    imoveis = connector.extract("sicar_imoveis_rurais_municipio")
    rl_app = connector.extract("sicar_reserva_legal_app_situacao")
    pra = connector.extract("sicar_regularizacao_pra_embargos")
    geom = connector.extract("sicar_geometria_validacao_sigef")
    feats = connector.extract("features_sfb_sicar_fundiario")

    assert len(imoveis) == 12
    assert len(rl_app) == 48
    assert len(pra) == 12
    assert len(geom) == 12
    assert len(feats) == 12

    tiers = Counter(r["tier_conformidade_sicar"] for r in feats)
    assert dict(tiers) == {
        "SICAR_CONFORMIDADE_EUDR_OURO": 3,
        "CAR_REGULARIZADO_EM_CONSOLIDACAO": 3,
        "PASSIVO_PRA_EM_MONITORAMENTO": 3,
        "RISCO_SOBREPOSICAO_OU_PASSIVO": 3,
    }


def test_fetch_and_cep_ibge6_resolution(tmp_path: Path) -> None:
    settings = _settings_with_data(tmp_path)
    connector = SfbSicarConnector(settings=settings)

    by_ibge = connector.fetch("4106902")
    by_cep = connector.fetch("80010-010")
    by_ibge6 = connector.fetch("410690")

    assert by_ibge["nome_municipio"] == "Curitiba"
    assert by_cep["id_municipio"] == "4106902"
    assert by_ibge6["id_municipio"] == "4106902"
    assert len(by_ibge["reserva_legal_app"]) == 4
    assert by_ibge["features_sicar"]["score_conformidade_sicar"] == 86.9


def test_sdk_environment_facade(tmp_path: Path) -> None:
    settings = _settings_with_data(tmp_path)
    sdk = BrazilData(settings=settings)

    pr = sdk.get_sicar_properties("4106902")
    rl = sdk.list_sicar_legal_reserve(
        "4106902", categoria_imovel_car="Pequena_Propriedade_Ate_4MF"
    )
    pra = sdk.get_sicar_pra_compliance("4106902")
    geo = sdk.get_sicar_geometry("4106902")
    ft = sdk.get_sicar_features("4106902")
    top = sdk.list_top_sicar_municipalities(limit=3)

    assert pr["nome_municipio"] == "Curitiba"
    assert len(rl) == 1
    assert rl[0]["categoria_imovel_car"] == "Pequena_Propriedade_Ate_4MF"
    assert pra["imoveis_livres_embargo_ibama_pct"] == 98.6
    assert geo["poligonos_sem_sobreposicao_pct"] == 94.6
    assert ft["tier_conformidade_sicar"] == "SICAR_CONFORMIDADE_EUDR_OURO"
    assert len(top) == 3
    assert top[0]["id_municipio"] == "4106902"


def test_fastapi_routes(tmp_path: Path) -> None:
    app = create_app(settings=_settings_with_data(tmp_path))
    client = TestClient(app)

    r_top = client.get("/api/v1/environment/sicar/top-municipalities?limit=2")
    assert r_top.status_code == 200
    assert len(r_top.json()) == 2

    r_feat = client.get("/api/v1/environment/sicar/features/4106902")
    assert r_feat.status_code == 200
    assert r_feat.json()["score_conformidade_sicar"] == 86.9

    r_rl = client.get(
        "/api/v1/environment/sicar/legal-reserve/4106902?categoria=Pequena_Propriedade_Ate_4MF"
    )
    assert r_rl.status_code == 200
    assert len(r_rl.json()) == 1

    r_pra = client.get("/api/v1/environment/sicar/pra/4106902")
    assert r_pra.status_code == 200
    assert r_pra.json()["id_municipio"] == "4106902"

    r_geo = client.get("/api/v1/environment/sicar/geometry/4106902")
    assert r_geo.status_code == 200
    assert r_geo.json()["poligonos_sem_sobreposicao_pct"] == 94.6

    r_alias = client.get("/api/v1/sicar/4106902")
    assert r_alias.status_code == 200
    assert r_alias.json()["qtd_imoveis_car_ativos"] == 1480


def test_cli_sicar_commands(tmp_path: Path, monkeypatch: Any, capsys: Any) -> None:
    data_dir = tmp_path / "data"
    SfbSicarConnector(settings=GatewaySettings(data_dir=data_dir)).materialize_all(
        force=True
    )
    monkeypatch.setenv("PDG_DATA_DIR", str(data_dir))
    monkeypatch.setenv("BDG_DATA_DIR", str(data_dir))

    for argv in [
        ["bdg", "sicar", "properties", "4106902", "--json"],
        [
            "bdg",
            "sicar",
            "legal-reserve",
            "4106902",
            "--categoria",
            "Pequena_Propriedade_Ate_4MF",
            "--json",
        ],
        ["bdg", "sicar", "pra", "4106902", "--json"],
        ["bdg", "sicar", "geometry", "4106902", "--json"],
        ["bdg", "sicar", "features", "4106902", "--json"],
        ["bdg", "sicar", "ranking", "--limit", "3", "--json"],
    ]:
        monkeypatch.setattr(sys, "argv", argv)
        rc = cli_main()
        assert rc == 0
        captured = capsys.readouterr()
        payload = json.loads(captured.out)
        assert payload


def test_mcp_sicar_tools(tmp_path: Path) -> None:
    dispatcher = MCPDispatcher(settings=_settings_with_data(tmp_path))

    tools_resp = dispatcher.handle_request(
        {"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}}
    )
    tool_names = {t["name"] for t in tools_resp["result"]["tools"]}
    expected = {
        "bdg_get_sicar_properties",
        "bdg_get_sicar_pra_compliance",
        "bdg_get_sicar_features",
        "bdg_list_top_sicar_municipalities",
    }
    assert expected.issubset(tool_names)

    for idx, (tool_name, args) in enumerate(
        [
            ("bdg_get_sicar_properties", {"id_municipio": "4106902"}),
            ("bdg_get_sicar_pra_compliance", {"id_municipio": "4106902"}),
            ("bdg_get_sicar_features", {"id_municipio": "4106902"}),
            ("bdg_list_top_sicar_municipalities", {"limit": 2}),
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
        assert "sfb_sicar" in text


def test_invalid_municipality_raises_key_error(tmp_path: Path) -> None:
    connector = SfbSicarConnector(settings=_settings_with_data(tmp_path))
    with pytest.raises(KeyError):
        connector.fetch("9999999")

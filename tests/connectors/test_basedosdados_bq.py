"""Testes offline determinísticos do conector Base dos Dados (BigQuery)."""

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
from public_data_gateway.connectors.agregadores.basedosdados_bq import (
    BaseDosDadosBqConnector,
)
from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.core.connector import list_registered_connectors
from public_data_gateway.mcp.server import MCPDispatcher
from public_data_gateway.sdk import BrazilData


def _settings_with_data(tmp_path: Path) -> GatewaySettings:
    settings = GatewaySettings(data_dir=tmp_path / "data")
    BaseDosDadosBqConnector(settings=settings).materialize_all(force=True)
    return settings


def test_connector_is_registered() -> None:
    connectors = list_registered_connectors()
    assert "basedosdados_bq" in connectors


def test_materialize_all_and_tier_balance(tmp_path: Path) -> None:
    settings = GatewaySettings(data_dir=tmp_path / "data")
    connector = BaseDosDadosBqConnector(settings=settings)
    outputs = connector.materialize_all(force=True)

    assert len(outputs) == 5
    for path in outputs.values():
        assert path.exists()

    cob = connector.extract("bd_datalake_cobertura_municipio")
    ds = connector.extract("bd_datasets_publicos_tema")
    gov = connector.extract("bd_governanca_metadados_odcs")
    con = connector.extract("bd_consumo_federado_sql")
    feats = connector.extract("features_basedosdados_datalake_federado")

    assert len(cob) == 12
    assert len(ds) == 48
    assert len(gov) == 12
    assert len(con) == 12
    assert len(feats) == 12

    tiers = Counter(r["tier_maturidade_bd"] for r in feats)
    assert dict(tiers) == {
        "BD_DATALAKE_OURO_FEDERADO": 3,
        "BD_INTEGRACAO_CROSS_DOMAIN_ALTA": 3,
        "BD_COBERTURA_PARCIAL_EXPANSAO": 3,
        "BD_LACUNA_CATALOGACAO_PRIORITARIA": 3,
    }


def test_fetch_and_cep_ibge6_resolution(tmp_path: Path) -> None:
    settings = _settings_with_data(tmp_path)
    connector = BaseDosDadosBqConnector(settings=settings)

    by_ibge = connector.fetch("3550308")
    by_cep = connector.fetch("01001-000")
    by_ibge6 = connector.fetch("355030")

    assert by_ibge["nome_municipio"] == "São Paulo"
    assert by_cep["id_municipio"] == "3550308"
    assert by_ibge6["id_municipio"] == "3550308"
    assert len(by_ibge["datasets_tema"]) == 4
    assert by_ibge["features_bd"]["score_maturidade_bd"] == 90.1


def test_sdk_metaportals_facade(tmp_path: Path) -> None:
    settings = _settings_with_data(tmp_path)
    sdk = BrazilData(settings=settings)

    cov = sdk.get_bd_coverage("3550308")
    ds = sdk.list_bd_datasets("3550308", dataset_bd_id="br_ibge_pib_populacao")
    gov = sdk.get_bd_governance("3550308")
    con = sdk.get_bd_consumption("3550308")
    ft = sdk.get_bd_features("3550308")
    top = sdk.list_top_bd_municipalities(limit=3)

    assert cov["nome_municipio"] == "São Paulo"
    assert len(ds) == 1
    assert ds[0]["dataset_bd_id"] == "br_ibge_pib_populacao"
    assert gov["conformidade_chaves_canonicas_pct"] == 96.8
    assert con["consultas_bigquery_mensais"] == 48200
    assert ft["tier_maturidade_bd"] == "BD_DATALAKE_OURO_FEDERADO"
    assert len(top) == 3
    assert top[0]["id_municipio"] == "3550308"


def test_fastapi_routes(tmp_path: Path) -> None:
    app = create_app(settings=_settings_with_data(tmp_path))
    client = TestClient(app)

    r_top = client.get("/api/v1/metaportals/basedosdados/top-municipalities?limit=2")
    assert r_top.status_code == 200
    assert len(r_top.json()) == 2

    r_feat = client.get("/api/v1/metaportals/basedosdados/features/3550308")
    assert r_feat.status_code == 200
    assert r_feat.json()["score_maturidade_bd"] == 90.1

    r_ds = client.get(
        "/api/v1/metaportals/basedosdados/datasets/3550308?dataset=br_ibge_pib_populacao"
    )
    assert r_ds.status_code == 200
    assert len(r_ds.json()) == 1

    r_gov = client.get("/api/v1/metaportals/basedosdados/governance/3550308")
    assert r_gov.status_code == 200
    assert r_gov.json()["id_municipio"] == "3550308"

    r_con = client.get("/api/v1/metaportals/basedosdados/consumption/3550308")
    assert r_con.status_code == 200
    assert r_con.json()["consultas_bigquery_mensais"] == 48200

    r_alias = client.get("/api/v1/basedosdados/3550308")
    assert r_alias.status_code == 200
    assert r_alias.json()["qtd_datasets_tratados_bd"] == 142


def test_cli_basedosdados_commands(
    tmp_path: Path, monkeypatch: Any, capsys: Any
) -> None:
    data_dir = tmp_path / "data"
    BaseDosDadosBqConnector(settings=GatewaySettings(data_dir=data_dir)).materialize_all(
        force=True
    )
    monkeypatch.setenv("PDG_DATA_DIR", str(data_dir))
    monkeypatch.setenv("BDG_DATA_DIR", str(data_dir))

    for argv in [
        ["bdg", "basedosdados", "coverage", "3550308", "--json"],
        [
            "bdg",
            "basedosdados",
            "datasets",
            "3550308",
            "--dataset",
            "br_ibge_pib_populacao",
            "--json",
        ],
        ["bdg", "basedosdados", "governance", "3550308", "--json"],
        ["bdg", "basedosdados", "consumption", "3550308", "--json"],
        ["bdg", "basedosdados", "features", "3550308", "--json"],
        ["bdg", "basedosdados", "ranking", "--limit", "3", "--json"],
    ]:
        monkeypatch.setattr(sys, "argv", argv)
        rc = cli_main()
        assert rc == 0
        captured = capsys.readouterr()
        payload = json.loads(captured.out)
        assert payload


def test_mcp_basedosdados_tools(tmp_path: Path) -> None:
    dispatcher = MCPDispatcher(settings=_settings_with_data(tmp_path))

    tools_resp = dispatcher.handle_request(
        {"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}}
    )
    tool_names = {t["name"] for t in tools_resp["result"]["tools"]}
    expected = {
        "bdg_get_bd_coverage",
        "bdg_get_bd_governance",
        "bdg_get_bd_features",
        "bdg_list_top_bd_municipalities",
    }
    assert expected.issubset(tool_names)

    for idx, (tool_name, args) in enumerate(
        [
            ("bdg_get_bd_coverage", {"id_municipio": "3550308"}),
            ("bdg_get_bd_governance", {"id_municipio": "3550308"}),
            ("bdg_get_bd_features", {"id_municipio": "3550308"}),
            ("bdg_list_top_bd_municipalities", {"limit": 2}),
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
        assert "basedosdados_bq" in text


def test_invalid_municipality_raises_key_error(tmp_path: Path) -> None:
    connector = BaseDosDadosBqConnector(settings=_settings_with_data(tmp_path))
    with pytest.raises(KeyError):
        connector.fetch("9999999")

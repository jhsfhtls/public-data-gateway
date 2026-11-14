"""Testes offline determinísticos para o conector inpe_terrabrasilis."""

from __future__ import annotations

import json
import sys
from collections import Counter
from pathlib import Path

import pytest
import yaml
from fastapi.testclient import TestClient

import public_data_gateway.connectors  # noqa: F401
from public_data_gateway.api.app import create_app
from public_data_gateway.cli.main import main as cli_main
from public_data_gateway.connectors.ambiental.inpe_terrabrasilis.connector import (
    InpeTerrabrasilisConnector,
)
from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.core.connector import get_connector
from public_data_gateway.mcp.server import MCPDispatcher
from public_data_gateway.sdk import BrazilData


def _settings_with_data(tmp_path: Path) -> GatewaySettings:
    return GatewaySettings(data_dir=tmp_path)


def test_inpe_terrabrasilis_contract_and_registry() -> None:
    contract_path = (
        Path(__file__).resolve().parents[2]
        / "src"
        / "public_data_gateway"
        / "connectors"
        / "ambiental"
        / "inpe_terrabrasilis"
        / "contract.yaml"
    )
    assert contract_path.exists()
    contract = yaml.safe_load(contract_path.read_text(encoding="utf-8"))
    assert contract["contract_id"] == "ODCS-BR-INPE-TERRABRASILIS-001"
    assert contract["source_id"] == "inpe_terrabrasilis"
    assert len(contract["schema"]["tables"]) == 5

    conn = get_connector("inpe_terrabrasilis")
    assert isinstance(conn, InpeTerrabrasilisConnector)
    assert conn.source_id == "inpe_terrabrasilis"


def test_inpe_terrabrasilis_medallion_materialization_and_tiers(tmp_path: Path) -> None:
    conn = InpeTerrabrasilisConnector(settings=_settings_with_data(tmp_path))
    written = conn.materialize_all(force=True)
    assert len(written) == 5
    for p in written.values():
        assert p.exists()

    gold = conn.load_or_materialize("features_inpe_terrabrasilis_desmatamento")
    assert len(gold) == 12
    tiers = Counter(r["perfil_risco_ambiental_terrabrasilis"] for r in gold)
    assert tiers == {
        "MUNICIPIO_CONFORMIDADE_VERDE_EUDR": 3,
        "ZONA_CONSOLIDADA_BAIXA_PRESSAO_DETER": 3,
        "FRONTEIRA_ATENCAO_CONTROLE_AMBIENTAL": 3,
        "ARCO_CRITICO_DESMATAMENTO_PRIORITARIO": 3,
    }


def test_inpe_terrabrasilis_municipality_resolution_and_errors(tmp_path: Path) -> None:
    conn = InpeTerrabrasilisConnector(settings=_settings_with_data(tmp_path))
    mao_ibge7 = conn.get_terrabrasilis_prodes_deforestation("1302603")
    mao_ibge6 = conn.get_terrabrasilis_prodes_deforestation("130260")
    mao_cep = conn.get_terrabrasilis_prodes_deforestation("69005010")
    cwb_alias = conn.get_terrabrasilis_prodes_deforestation("4106900")
    assert mao_ibge7["id_municipio"] == "1302603"
    assert mao_ibge6["id_municipio"] == "1302603"
    assert mao_cep["id_municipio"] == "1302603"
    assert cwb_alias["id_municipio"] == "4106902"

    with pytest.raises(ValueError):
        conn.get_terrabrasilis_prodes_deforestation("9999999")


def test_inpe_terrabrasilis_deter_alerts_invariant(tmp_path: Path) -> None:
    conn = InpeTerrabrasilisConnector(settings=_settings_with_data(tmp_path))
    alerts = conn.list_terrabrasilis_deter_alerts("1302603")
    assert len(alerts) == 4
    gold = conn.get_terrabrasilis_environmental_features("1302603")
    assert sum(r["num_alertas_deter_ano"] for r in alerts) == gold["total_alertas_deter_ano"]
    assert round(sum(r["area_alertas_deter_km2"] for r in alerts), 2) == gold["area_total_alertas_deter_km2"]

    corte = conn.list_terrabrasilis_deter_alerts(
        "1302603", classe_alerta_deter="Corte_Raso"
    )
    assert len(corte) == 1
    assert corte[0]["classe_alerta_deter"] == "Desmatamento_Corte_Raso"


def test_inpe_terrabrasilis_sdk_parity(tmp_path: Path) -> None:
    sdk = BrazilData(settings=_settings_with_data(tmp_path))
    prodes = sdk.get_terrabrasilis_prodes_deforestation("1302603")
    assert prodes["bioma_predominante"] == "Amazonia"
    assert len(sdk.list_terrabrasilis_deter_alerts("1302603")) == 4
    assert sdk.get_terrabrasilis_land_cover("1302603")["area_vegetacao_nativa_km2"] == 8710.4
    assert sdk.get_terrabrasilis_esg_governance("1302603")["embargos_ibama_ativos_ano"] == 64
    assert (
        sdk.get_terrabrasilis_environmental_features("1302603")[
            "perfil_risco_ambiental_terrabrasilis"
        ]
        == "ARCO_CRITICO_DESMATAMENTO_PRIORITARIO"
    )
    top = sdk.list_top_terrabrasilis_municipalities(limit=3)
    assert [r["id_municipio"] for r in top] == ["4106902", "3550308", "3304557"]


def test_inpe_terrabrasilis_cli_subcommands(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    for argv in (
        ["bdg", "terrabrasilis", "prodes", "1302603", "--json"],
        ["bdg", "terrabrasilis", "deter", "1302603", "--classe", "Corte_Raso", "--json"],
        ["bdg", "terrabrasilis", "land-cover", "1302603", "--json"],
        ["bdg", "terrabrasilis", "governance", "1302603", "--json"],
        ["bdg", "terrabrasilis", "features", "1302603", "--json"],
        ["bdg", "terrabrasilis", "ranking", "--limit", "3", "--json"],
    ):
        monkeypatch.setattr(sys, "argv", argv)
        rc = cli_main()
        assert rc == 0
        out = capsys.readouterr().out
        parsed = json.loads(out)
        assert parsed


def test_inpe_terrabrasilis_fastapi_routes(tmp_path: Path) -> None:
    client = TestClient(create_app(settings=_settings_with_data(tmp_path)))
    for path in (
        "/api/v1/environment/terrabrasilis/prodes/1302603",
        "/api/v1/terrabrasilis/1302603",
        "/api/v1/environment/terrabrasilis/deter/1302603?classe=Corte_Raso",
        "/api/v1/environment/terrabrasilis/land-cover/1302603",
        "/api/v1/environment/terrabrasilis/governance/1302603",
        "/api/v1/environment/terrabrasilis/features/1302603",
        "/api/v1/environment/terrabrasilis/top-municipalities?limit=3",
    ):
        resp = client.get(path)
        assert resp.status_code == 200, f"{path} -> {resp.text}"
        assert resp.json()

    bad = client.get("/api/v1/environment/terrabrasilis/prodes/9999999")
    assert bad.status_code == 400


def test_inpe_terrabrasilis_mcp_tools(tmp_path: Path) -> None:
    server = MCPDispatcher(settings=_settings_with_data(tmp_path))
    tools_resp = server.handle_request(
        {"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}}
    )
    tool_names = {t["name"] for t in tools_resp["result"]["tools"]}
    expected = {
        "bdg_get_terrabrasilis_prodes_deforestation",
        "bdg_get_terrabrasilis_esg_governance",
        "bdg_get_terrabrasilis_environmental_features",
        "bdg_list_top_terrabrasilis_municipalities",
    }
    assert expected.issubset(tool_names)

    for idx, (name, args) in enumerate(
        [
            ("bdg_get_terrabrasilis_prodes_deforestation", {"id_municipio": "1302603"}),
            ("bdg_get_terrabrasilis_esg_governance", {"id_municipio": "1302603"}),
            ("bdg_get_terrabrasilis_environmental_features", {"id_municipio": "1302603"}),
            ("bdg_list_top_terrabrasilis_municipalities", {"limit": 3}),
        ],
        start=2,
    ):
        resp = server.handle_request(
            {
                "jsonrpc": "2.0",
                "id": idx,
                "method": "tools/call",
                "params": {"name": name, "arguments": args},
            }
        )
        assert not resp["result"]["isError"]
        text = resp["result"]["content"][0]["text"]
        assert "inpe_terrabrasilis" in text

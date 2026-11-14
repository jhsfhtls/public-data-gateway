"""Testes offline determinísticos para o conector capes_sucupira."""

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
from public_data_gateway.connectors.educacao.capes_sucupira.connector import (
    CapesSucupiraConnector,
)
from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.core.connector import get_connector
from public_data_gateway.mcp.server import MCPDispatcher
from public_data_gateway.sdk import BrazilData


def _settings_with_data(tmp_path: Path) -> GatewaySettings:
    return GatewaySettings(data_dir=tmp_path)


def test_capes_sucupira_contract_and_registry() -> None:
    contract_path = (
        Path(__file__).resolve().parents[2]
        / "src"
        / "public_data_gateway"
        / "connectors"
        / "educacao"
        / "capes_sucupira"
        / "contract.yaml"
    )
    assert contract_path.exists()
    contract = yaml.safe_load(contract_path.read_text(encoding="utf-8"))
    assert contract["contract_id"] == "ODCS-BR-CAPES-SUCUPIRA-001"
    assert contract["source_id"] == "capes_sucupira"
    assert len(contract["schema"]["tables"]) == 5

    conn = get_connector("capes_sucupira")
    assert isinstance(conn, CapesSucupiraConnector)
    assert conn.source_id == "capes_sucupira"


def test_capes_sucupira_medallion_materialization_and_tiers(tmp_path: Path) -> None:
    conn = CapesSucupiraConnector(settings=_settings_with_data(tmp_path))
    written = conn.materialize_all(force=True)
    assert len(written) == 5
    for p in written.values():
        assert p.exists()

    gold = conn.load_or_materialize("features_capes_sucupira_pos_graduacao")
    assert len(gold) == 12
    tiers = Counter(r["perfil_pos_graduacao_sucupira"] for r in gold)
    assert tiers == {
        "HUB_EXCELENCIA_CIENTIFICA_INTERNACIONAL": 3,
        "POLO_POS_GRADUACAO_CONSOLIDADO_NACIONAL": 3,
        "CENTRO_REGIONAL_EXPANSAO_MESTRADO_DOUTORADO": 3,
        "ECOSISTEMA_EMERGENTE_FOMENTO_CAPES": 3,
    }


def test_capes_sucupira_municipality_resolution_and_errors(tmp_path: Path) -> None:
    conn = CapesSucupiraConnector(settings=_settings_with_data(tmp_path))
    sp_ibge7 = conn.get_sucupira_program_evaluation("3550308")
    sp_ibge6 = conn.get_sucupira_program_evaluation("355030")
    sp_cep = conn.get_sucupira_program_evaluation("01001000")
    cwb_alias = conn.get_sucupira_program_evaluation("4106900")
    assert sp_ibge7["id_municipio"] == "3550308"
    assert sp_ibge6["id_municipio"] == "3550308"
    assert sp_cep["id_municipio"] == "3550308"
    assert cwb_alias["id_municipio"] == "4106902"

    with pytest.raises(ValueError):
        conn.get_sucupira_program_evaluation("9999999")


def test_capes_sucupira_students_by_area_invariant(tmp_path: Path) -> None:
    conn = CapesSucupiraConnector(settings=_settings_with_data(tmp_path))
    areas = conn.list_sucupira_students_by_area("3550308")
    assert len(areas) == 4
    gold = conn.get_sucupira_postgrad_features("3550308")
    total_area_students = sum(
        r["matriculados_mestrado_area"] + r["matriculados_doutorado_area"]
        for r in areas
    )
    assert total_area_students == gold["total_matriculados_pos_graduacao_ano"]

    eng = conn.list_sucupira_students_by_area(
        "3550308", grande_area_conhecimento="Engenharias"
    )
    assert len(eng) == 1
    assert eng[0]["grande_area_conhecimento"] == "Engenharias_e_Computacao"


def test_capes_sucupira_sdk_parity(tmp_path: Path) -> None:
    sdk = BrazilData(settings=_settings_with_data(tmp_path))
    prog = sdk.get_sucupira_program_evaluation("3550308")
    assert prog["total_programas_stricto_sensu_ano"] == 348
    assert len(sdk.list_sucupira_students_by_area("3550308")) == 4
    assert sdk.get_sucupira_faculty_research("3550308")["total_docentes_permanentes_ano"] == 8940
    assert sdk.get_sucupira_scientific_production("3550308")["artigos_qualis_a1_a2_ano"] == 18640
    assert (
        sdk.get_sucupira_postgrad_features("3550308")["perfil_pos_graduacao_sucupira"]
        == "HUB_EXCELENCIA_CIENTIFICA_INTERNACIONAL"
    )
    top = sdk.list_top_sucupira_municipalities(limit=3)
    assert [r["id_municipio"] for r in top] == ["3550308", "3304557", "3106200"]


def test_capes_sucupira_cli_subcommands(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    for argv in (
        ["bdg", "sucupira", "programs", "3550308", "--json"],
        ["bdg", "sucupira", "students", "3550308", "--area", "Engenharias", "--json"],
        ["bdg", "sucupira", "faculty", "3550308", "--json"],
        ["bdg", "sucupira", "production", "3550308", "--json"],
        ["bdg", "sucupira", "features", "3550308", "--json"],
        ["bdg", "sucupira", "ranking", "--limit", "3", "--json"],
    ):
        monkeypatch.setattr(sys, "argv", argv)
        rc = cli_main()
        assert rc == 0
        out = capsys.readouterr().out
        parsed = json.loads(out)
        assert parsed


def test_capes_sucupira_fastapi_routes(tmp_path: Path) -> None:
    client = TestClient(create_app(settings=_settings_with_data(tmp_path)))
    for path in (
        "/api/v1/education/sucupira/programs/3550308",
        "/api/v1/sucupira/3550308",
        "/api/v1/education/sucupira/students/3550308?area=Engenharias",
        "/api/v1/education/sucupira/faculty/3550308",
        "/api/v1/education/sucupira/production/3550308",
        "/api/v1/education/sucupira/features/3550308",
        "/api/v1/education/sucupira/top-municipalities?limit=3",
    ):
        resp = client.get(path)
        assert resp.status_code == 200, f"{path} -> {resp.text}"
        assert resp.json()

    bad = client.get("/api/v1/education/sucupira/programs/9999999")
    assert bad.status_code == 400


def test_capes_sucupira_mcp_tools(tmp_path: Path) -> None:
    server = MCPDispatcher(settings=_settings_with_data(tmp_path))
    tools_resp = server.handle_request(
        {"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}}
    )
    tool_names = {t["name"] for t in tools_resp["result"]["tools"]}
    expected = {
        "bdg_get_sucupira_program_evaluation",
        "bdg_get_sucupira_scientific_production",
        "bdg_get_sucupira_postgrad_features",
        "bdg_list_top_sucupira_municipalities",
    }
    assert expected.issubset(tool_names)

    for idx, (name, args) in enumerate(
        [
            ("bdg_get_sucupira_program_evaluation", {"id_municipio": "3550308"}),
            ("bdg_get_sucupira_scientific_production", {"id_municipio": "3550308"}),
            ("bdg_get_sucupira_postgrad_features", {"id_municipio": "3550308"}),
            ("bdg_list_top_sucupira_municipalities", {"limit": 3}),
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
        assert "capes_sucupira" in text

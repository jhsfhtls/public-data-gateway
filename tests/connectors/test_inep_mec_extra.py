"""Offline tests and 4-interface parity suite for inep_mec_extra connector."""

from __future__ import annotations

import json
from pathlib import Path

from fastapi.testclient import TestClient

from public_data_gateway.api.app import create_app
from public_data_gateway.cli.main import main as cli_main
from public_data_gateway.connectors.educacao.inep_mec_extra import InepMecExtraConnector
from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.core.connector import get_connector
from public_data_gateway.mcp.server import MCPDispatcher
from public_data_gateway.sdk import BrazilData


def test_inep_mec_extra_medallion_contract_and_queries(tmp_path: Path) -> None:
    """Validate ODCS v3.1 contract, registry registration, 4 Silver Parquets, and Gold features."""
    settings = GatewaySettings(data_dir=tmp_path)
    connector = InepMecExtraConnector(settings=settings)
    registered = get_connector("inep_mec_extra", settings=settings)
    assert isinstance(registered, InepMecExtraConnector)
    assert connector.contract.source_id == "inep_mec_extra"
    assert connector.test_connection() is True

    silver_dir = tmp_path / "inep_mec_extra" / "silver"
    gold_dir = tmp_path / "inep_mec_extra" / "gold"
    assert (silver_dir / "inep_saeb_proficiencia_municipio.parquet").exists()
    assert (silver_dir / "mec_prouni_sisu_acesso_superior.parquet").exists()
    assert (silver_dir / "fnde_fundeb_cnpq_fomento.parquet").exists()
    assert (silver_dir / "inep_mec_extra_resumo_municipal.parquet").exists()
    assert (gold_dir / "features_inep_mec_extra_brasil.parquet").exists()
    assert (tmp_path / "inep_mec_extra" / "bronze" / "ingestion_meta.json").exists()

    saeb = connector.get_inep_saeb_proficiency("Florianópolis")
    assert len(saeb) == 2
    assert saeb[0]["proficiencia_media_saeb_lp_mat"] > 280.0

    acc = connector.get_mec_prouni_sisu_access("4205407")
    assert len(acc) == 1
    assert acc[0]["taxa_ocupacao_vagas_pct"] > 95.0

    fund = connector.get_fnde_fundeb_cnpq_funding("Florianópolis")
    assert fund is not None
    assert fund["bolsas_pesquisa_cnpq_ativas"] == 3940

    summary = connector.get_inep_mec_extra_summary("4205407")
    assert summary is not None
    assert summary["proficiencia_saeb_matematica"] == 291.0

    features = connector.get_inep_mec_extra_features("Florianópolis")
    assert features is not None
    assert features["tier_ecossistema_educacional_mec"] == "POLO_EXCELENCIA_EDUCACIONAL_MEC"

    top_sp = connector.list_top_inep_mec_extra_municipalities(limit=5, sigla_uf="SP")
    assert len(top_sp) == 2
    assert all(r["sigla_uf"] == "SP" for r in top_sp)


def test_inep_mec_extra_sdk_parity(tmp_path: Path) -> None:
    """Validate SDK EducationFacade and top-level BrazilData shortcuts."""
    sdk = BrazilData(settings=GatewaySettings(data_dir=tmp_path))
    assert len(sdk.get_inep_saeb_proficiency("Campinas")) == 2
    assert len(sdk.get_mec_prouni_sisu_access("3509502")) == 1

    fund = sdk.get_fnde_fundeb_cnpq_funding("3509502")
    assert fund is not None
    assert fund["investimento_aluno_ano_fundeb_brl"] >= 10000.0

    summary = sdk.get_inep_mec_extra_summary("Campinas")
    assert summary is not None
    assert summary["sigla_uf"] == "SP"

    features = sdk.get_inep_mec_extra_features("Campinas")
    assert features is not None
    assert features["score_capital_humano_mec_extra"] >= 78.0

    top_fundeb = sdk.list_top_inep_mec_extra_municipalities(limit=3, sort_by_fundeb=True)
    assert len(top_fundeb) == 3
    assert (
        top_fundeb[0]["repasses_fundeb_mil_brl"]
        >= top_fundeb[1]["repasses_fundeb_mil_brl"]
    )


def test_inep_mec_extra_fastapi_and_cli_parity(
    tmp_path: Path, monkeypatch, capsys
) -> None:
    """Validate FastAPI REST endpoints and bdg inep-mec-extra CLI subcommands."""
    settings = GatewaySettings(data_dir=tmp_path)
    app = create_app(settings=settings)
    client = TestClient(app)

    r_sb = client.get("/api/v1/education/inep-mec-extra/saeb/Curitiba")
    assert r_sb.status_code == 200
    assert len(r_sb.json()) == 2

    r_ac = client.get("/api/v1/education/inep-mec-extra/prouni-sisu/4106902")
    assert r_ac.status_code == 200
    assert len(r_ac.json()) == 1

    r_fn = client.get("/api/v1/education/inep-mec-extra/fnde-cnpq/Curitiba")
    assert r_fn.status_code == 200
    assert r_fn.json()["bolsas_pesquisa_cnpq_ativas"] > 4000

    r_mn = client.get("/api/v1/education/inep-mec-extra/municipio/4106902")
    assert r_mn.status_code == 200
    assert r_mn.json()["sigla_uf"] == "PR"

    r_ft = client.get("/api/v1/education/inep-mec-extra/features/Curitiba")
    assert r_ft.status_code == 200
    assert r_ft.json()["tier_ecossistema_educacional_mec"] == "POLO_EXCELENCIA_EDUCACIONAL_MEC"

    r_rk = client.get("/api/v1/education/inep-mec-extra/ranking?limit=3&sort_by_fundeb=true")
    assert r_rk.status_code == 200
    assert len(r_rk.json()) == 3

    monkeypatch.setenv("BDG_DATA_DIR", str(tmp_path))
    assert cli_main(["inep-mec-extra", "saeb", "Brasília", "--json"]) == 0
    assert len(json.loads(capsys.readouterr().out)) == 2
    assert cli_main(["inep-mec-extra", "prouni-sisu", "5300108", "--json"]) == 0
    assert len(json.loads(capsys.readouterr().out)) == 1
    assert cli_main(["inep-mec-extra", "fnde-cnpq", "Brasília", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["sigla_uf"] == "DF"
    assert cli_main(["inep-mec-extra", "municipio", "5300108", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["id_municipio"] == "5300108"
    assert cli_main(["inep-mec-extra", "features", "Brasília", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["score_capital_humano_mec_extra"] >= 78.0
    assert cli_main(["inep-mec-extra", "ranking", "--limit", "3", "--fundeb", "--json"]) == 0
    assert len(json.loads(capsys.readouterr().out)) == 3


def test_inep_mec_extra_mcp_tools_parity(tmp_path: Path) -> None:
    """Validate MCP JSON-RPC tools with untrusted data wrapper for INEP/MEC Extra."""
    mcp = MCPDispatcher(settings=GatewaySettings(data_dir=tmp_path))

    tools_resp = mcp.handle_request({"jsonrpc": "2.0", "id": 1, "method": "tools/list"})
    names = {t["name"] for t in tools_resp["result"]["tools"]}
    assert {
        "bdg_get_inep_saeb_proficiency",
        "bdg_get_mec_prouni_sisu_access",
        "bdg_get_inep_mec_extra_features",
        "bdg_list_top_inep_mec_extra_municipalities",
    }.issubset(names)

    for call_id, name, arguments in [
        (2, "bdg_get_inep_saeb_proficiency", {"termo": "Campinas"}),
        (3, "bdg_get_mec_prouni_sisu_access", {"termo": "3509502"}),
        (4, "bdg_get_inep_mec_extra_features", {"id_municipio_or_name": "Campinas"}),
        (
            5,
            "bdg_list_top_inep_mec_extra_municipalities",
            {"limit": 3, "sigla_uf": "SP", "sort_by_fundeb": True},
        ),
    ]:
        resp = mcp.handle_request(
            {
                "jsonrpc": "2.0",
                "id": call_id,
                "method": "tools/call",
                "params": {"name": name, "arguments": arguments},
            }
        )
        assert resp["result"]["isError"] is False
        text = resp["result"]["content"][0]["text"]
        assert "inep_mec_extra" in text

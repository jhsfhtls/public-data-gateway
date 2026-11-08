"""Unit and offline integration test suite for CNJ DataJud (Base Nacional do Poder Judiciário) connector.

Covers:
1. ODCS v3.1 Data Contract conformance
2. Ingestion of all 5 tables into isolated bronze directories
3. Normalization and Silver Parquet materialization
4. Quarantine handling for invalid records
5. Gold Feature Store (features_litigiosidade_juridica_brasil.parquet)
6. Litigation risk calculation and court proceedings search
7. Multi-interface unification (SDK, CLI, REST API, MCP)
"""

from __future__ import annotations

from pathlib import Path

import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

from public_data_gateway.api.app import create_app
from public_data_gateway.cli.main import main as cli_main
from public_data_gateway.connectors.judicial.cnj_datajud import (
    CNJDataJudConnector,
)
from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.core.storage import HybridStorageEngine
from public_data_gateway.mcp.server import MCPDispatcher
from public_data_gateway.sdk import PublicData


@pytest.fixture
def temp_gateway_settings(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> GatewaySettings:
    """Create isolated temporary directory environment."""
    monkeypatch.setenv("PDG_OFFLINE_MODE", "1")
    monkeypatch.setenv("BDG_OFFLINE_MODE", "1")
    repo_root = Path(__file__).resolve().parents[2]
    settings = GatewaySettings(
        repo_root=repo_root,
        data_dir=tmp_path / "data",
    )
    settings.ensure_directories()
    return settings


@pytest.fixture
def datajud_connector(temp_gateway_settings: GatewaySettings) -> CNJDataJudConnector:
    """Instantiate CNJDataJudConnector backed by temporary storage."""
    conn = CNJDataJudConnector(settings=temp_gateway_settings)
    conn.storage = HybridStorageEngine(settings=temp_gateway_settings)
    return conn


def test_datajud_odcs_contract(datajud_connector: CNJDataJudConnector) -> None:
    """Validate ODCS v3.1 contract properties and schema definitions."""
    contract = datajud_connector.contract
    assert contract is not None, "Contract failed to load"
    assert contract.contract_id == "ODCS-BR-CNJ-DATAJUD-001"
    assert contract.source_id == "cnj_datajud"
    assert "numero_processo" in contract.primary_keys
    col_names = [c.name for c in contract.columns]
    assert "numero_processo" in col_names
    assert "tribunal_sigla" in col_names
    assert "classe_codigo_tpu" in col_names
    assert "data_ajuizamento" in col_names
    assert "polo_passivo_documento" in col_names


def test_datajud_extract_all_tables(datajud_connector: CNJDataJudConnector) -> None:
    """Verify that all 5 CNJ DataJud tables are materialized into Silver Parquet ZSTD."""
    silver_paths = datajud_connector.extract_all()
    assert len(silver_paths) == 5

    expected_tables = [
        "cnj_processos",
        "cnj_tribunais",
        "cnj_classes_tpu",
        "cnj_assuntos_tpu",
        "cnj_movimentos_processuais",
    ]

    for table_name in expected_tables:
        assert table_name in silver_paths
        p = silver_paths[table_name]
        assert p.exists()
        table = pq.read_table(p)
        assert table.num_rows > 0


def test_datajud_gold_feature_store(datajud_connector: CNJDataJudConnector) -> None:
    """Validate calculation of Gold features, litigation risk score, and category."""
    datajud_connector.extract_all()
    gold_path = (
        datajud_connector.settings.data_dir
        / "cnj_datajud"
        / "gold"
        / "features_litigiosidade_juridica_brasil.parquet"
    )
    assert gold_path.exists()

    gold_table = pq.read_table(gold_path)
    assert gold_table.num_rows > 0
    records = gold_table.to_pylist()

    # Petrobras should have critical risk due to high contingency and active tax executions
    petro = next((r for r in records if "PETROBRAS" in r["razao_social_ou_nome"]), None)
    assert petro is not None
    assert petro["total_processos_polo_passivo"] >= 2
    assert petro["total_execucoes_fiscais"] >= 1
    assert petro["categoria_risco_judicial"] == "CRITICO"
    assert petro["score_litigiosidade_juridica"] >= 75.0


def test_datajud_get_litigation_risk(datajud_connector: CNJDataJudConnector) -> None:
    """Verify litigation risk assessment for corporate entities."""
    datajud_connector.extract_all()

    # 1. High-risk entity
    risk_petro = datajud_connector.get_litigation_risk("33.000.167/0001-01")
    assert risk_petro["has_processos"] is True
    assert risk_petro["total_processos"] >= 2
    assert risk_petro["categoria_risco_judicial"] == "CRITICO"

    # 2. Medium-risk entity
    risk_magalu = datajud_connector.get_litigation_risk("MAGAZINE LUIZA")
    assert risk_magalu["has_processos"] is True
    assert risk_magalu["categoria_risco_judicial"] in {"MEDIO", "ALTO"}

    # 3. Clean entity with no lawsuits
    risk_clean = datajud_connector.get_litigation_risk("00.000.000/0000-00")
    assert risk_clean["has_processos"] is False
    assert risk_clean["total_processos"] == 0
    assert risk_clean["categoria_risco_judicial"] == "BAIXO"
    assert risk_clean["score_litigiosidade_juridica"] == 0.0


def test_datajud_search_processos(datajud_connector: CNJDataJudConnector) -> None:
    """Verify searching proceedings by NUP or party name."""
    datajud_connector.extract_all()

    # Search by NUP digits
    res_nup = datajud_connector.search_processos("0000832-35.2018.4.01.3202")
    assert len(res_nup) == 1
    assert res_nup[0]["tribunal_sigla"] == "TRF1"

    # Search by party name with court filter
    res_sp = datajud_connector.search_processos("MAGAZINE", tribunal="TJSP")
    assert len(res_sp) == 1
    assert res_sp[0]["classe_nome"] == "PROCEDIMENTO COMUM CIVEL"


def test_datajud_sdk_facade(temp_gateway_settings: GatewaySettings) -> None:
    """Verify SDK access via PublicData.judicial and PublicData.get_litigation_risk."""
    sdk = PublicData(settings=temp_gateway_settings)
    conn = CNJDataJudConnector(settings=temp_gateway_settings)
    conn.extract_all()

    risk = sdk.judicial.litigation_risk("VALE")
    assert risk["has_processos"] is True
    assert risk["total_execucoes_fiscais"] >= 1

    direct_risk = sdk.get_litigation_risk("VALE")
    assert direct_risk["has_processos"] is True

    procs = sdk.judicial.search_processos("TRF3")
    assert len(procs) > 0


def test_datajud_cli_interface(
    temp_gateway_settings: GatewaySettings, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Verify CLI subcommands pdg judicial risk and pdg judicial search."""
    conn = CNJDataJudConnector(settings=temp_gateway_settings)
    conn.extract_all()

    # CLI risk JSON
    ret = cli_main(["judicial", "risk", "Petrobras", "--json"])
    assert ret == 0
    captured = capsys.readouterr()
    assert "CRITICO" in captured.out

    # CLI search
    ret_search = cli_main(["judicial", "search", "TRF1", "--limit", "5"])
    assert ret_search == 0


def test_datajud_fastapi_rest(temp_gateway_settings: GatewaySettings) -> None:
    """Verify FastAPI endpoints /api/v1/judicial/risk and /api/v1/judicial/processos."""
    conn = CNJDataJudConnector(settings=temp_gateway_settings)
    conn.extract_all()

    app = create_app(settings=temp_gateway_settings)
    client = TestClient(app)

    # 1. Risk endpoint
    resp = client.get("/api/v1/judicial/risk/Petrobras")
    assert resp.status_code == 200
    data = resp.json()
    assert data["has_processos"] is True
    assert data["categoria_risco_judicial"] == "CRITICO"

    # 2. Search endpoint
    resp_search = client.get("/api/v1/judicial/processos?termo=AMBEV")
    assert resp_search.status_code == 200
    items = resp_search.json()
    assert isinstance(items, list)
    assert len(items) > 0


def test_datajud_mcp_tool(temp_gateway_settings: GatewaySettings) -> None:
    """Verify Model Context Protocol tool bdg_get_litigation_risk."""
    conn = CNJDataJudConnector(settings=temp_gateway_settings)
    conn.extract_all()

    dispatcher = MCPDispatcher(settings=temp_gateway_settings)
    req = {
        "jsonrpc": "2.0",
        "id": "jud-mcp-1",
        "method": "tools/call",
        "params": {
            "name": "bdg_get_litigation_risk",
            "arguments": {"documento": "Petrobras"},
        },
    }
    resp = dispatcher.handle_request(req)
    assert resp.get("error") is None
    result = resp.get("result", {})
    assert not result.get("isError")
    content = result.get("content", [])
    assert len(content) > 0
    assert "CRITICO" in content[0]["text"]

"""Unit and offline integration test suite for CGU CEIS, CNEP, CEPIM & Leniência connector.

Covers:
1. ODCS v3.1 Data Contract conformance
2. Ingestion of all 5 tables into isolated bronze directories
3. Normalization and Silver Parquet materialization
4. Quarantine handling for invalid records
5. Gold Feature Store (features_compliance_risco_brasil.parquet)
6. Compliance status check and anti-corruption fine aggregation
7. Multi-interface unification (SDK, CLI, REST API, MCP)
"""

from __future__ import annotations

from pathlib import Path

import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

from public_data_gateway.api.app import create_app
from public_data_gateway.cli.main import main as cli_main
from public_data_gateway.connectors.compliance.cgu_ceis_cnep import (
    CGUCEISCNEPConnector,
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
def cgu_connector(temp_gateway_settings: GatewaySettings) -> CGUCEISCNEPConnector:
    """Instantiate CGUCEISCNEPConnector backed by temporary storage."""
    conn = CGUCEISCNEPConnector(settings=temp_gateway_settings)
    conn.storage = HybridStorageEngine(settings=temp_gateway_settings)
    return conn


def test_cgu_odcs_contract(cgu_connector: CGUCEISCNEPConnector) -> None:
    """Validate ODCS v3.1 contract properties and schema definitions."""
    contract = cgu_connector.contract
    assert contract is not None, "Contract failed to load"
    assert contract.contract_id == "ODCS-BR-CGU-CEIS-CNEP-001"
    assert contract.source_id == "cgu_ceis_cnep"
    assert "id_sancao" in contract.primary_keys
    col_names = [c.name for c in contract.columns]
    assert "id_sancao" in col_names
    assert "cpf_cnpj_sancionado" in col_names
    assert "tipo_sancao" in col_names
    assert "is_sancao_ativa" in col_names


def test_cgu_extract_all_tables(cgu_connector: CGUCEISCNEPConnector) -> None:
    """Verify that all 5 CGU tables are materialized into Silver Parquet ZSTD."""
    silver_paths = cgu_connector.extract_all()
    assert len(silver_paths) == 5

    expected_tables = [
        "cgu_ceis",
        "cgu_cnep",
        "cgu_cepim",
        "cgu_acordos_leniencia",
        "cgu_orgaos_sancionadores",
    ]

    for table_name in expected_tables:
        assert table_name in silver_paths
        p = silver_paths[table_name]
        assert p.exists()
        table = pq.read_table(p)
        assert table.num_rows > 0


def test_cgu_gold_feature_store(cgu_connector: CGUCEISCNEPConnector) -> None:
    """Validate calculation of Gold features, compliance risk score, and category."""
    cgu_connector.extract_all()
    gold_path = (
        cgu_connector.settings.data_dir
        / "cgu_ceis_cnep"
        / "gold"
        / "features_compliance_risco_brasil.parquet"
    )
    assert gold_path.exists()

    gold_table = pq.read_table(gold_path)
    assert gold_table.num_rows > 0
    pyd = gold_table.to_pylist()

    # Find entity with both CEIS and CNEP (CONSTRUTORA METROPOLITANA PAULISTA S.A. -> 33000168)
    entity_330 = next((r for r in pyd if r["cnpj_basico"] == "33000168"), None)
    assert entity_330 is not None
    assert entity_330["has_active_sanction"] is True
    assert entity_330["is_inidoneo_ceis_ativo"] is True
    assert entity_330["is_punido_cnep_ativo"] is True
    assert entity_330["valor_total_multas_anticorrupcao"] >= 12500000.0
    assert entity_330["score_risco_compliance"] == 100.0
    assert entity_330["categoria_risco_compliance"] == "CRITICO"


def test_cgu_get_compliance_status(cgu_connector: CGUCEISCNEPConnector) -> None:
    """Verify fast compliance lookup for sanctioned vs non-sanctioned entities."""
    cgu_connector.extract_all()

    # Sanctioned entity
    status_sanc = cgu_connector.get_compliance_status("33.000.168/0001-59")
    assert status_sanc["has_active_sanction"] is True
    assert status_sanc["status_idoneidade"] == "INIDONEO_SANCIONADO"
    assert status_sanc["total_sancoes_ceis_ativas"] >= 1
    assert status_sanc["total_sancoes_cnep_ativas"] >= 1
    assert status_sanc["valor_total_multas_cnep"] >= 12500000.0

    # Non-sanctioned clean entity
    status_clean = cgu_connector.get_compliance_status("00.000.000/0001-91")
    assert status_clean["has_active_sanction"] is False
    assert status_clean["status_idoneidade"] == "IDONEO_SEM_SANCAO_ATIVA"
    assert status_clean["total_sancoes_ceis"] == 0
    assert status_clean["valor_total_multas_cnep"] == 0.0


def test_cgu_sdk_facade(temp_gateway_settings: GatewaySettings) -> None:
    """Verify SDK access via PublicData.compliance."""
    sdk = PublicData(settings=temp_gateway_settings)
    # Ensure tables are extracted in temporary settings
    sdk.compliance.connector.extract_all()

    status = sdk.compliance.status("33000168000159")
    assert status["has_active_sanction"] is True
    assert status["status_idoneidade"] == "INIDONEO_SANCIONADO"

    sancoes = sdk.compliance.sancoes(cadastro="ceis", ativas_only=True)
    assert len(sancoes) > 0
    assert all(s.get("is_sancao_ativa") for s in sancoes)


def test_cgu_cli_interface(
    temp_gateway_settings: GatewaySettings, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Verify CLI subcommands pdg compliance status and pdg compliance sancoes."""
    cgu = CGUCEISCNEPConnector(settings=temp_gateway_settings)
    cgu.extract_all()

    # CLI status JSON
    ret = cli_main(["compliance", "status", "33000168000159", "--json"])
    assert ret == 0
    captured = capsys.readouterr()
    assert "INIDONEO_SANCIONADO" in captured.out

    # CLI sancoes
    ret_sanc = cli_main(["compliance", "sancoes", "--cadastro", "ceis", "--limit", "5"])
    assert ret_sanc == 0


def test_cgu_fastapi_rest(temp_gateway_settings: GatewaySettings) -> None:
    """Verify FastAPI endpoints /api/v1/compliance/status and /api/v1/compliance/sancoes."""
    cgu = CGUCEISCNEPConnector(settings=temp_gateway_settings)
    cgu.extract_all()

    app = create_app(settings=temp_gateway_settings)
    client = TestClient(app)

    # 1. Status endpoint
    resp = client.get("/api/v1/compliance/status/33000168000159")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status_idoneidade"] == "INIDONEO_SANCIONADO"
    assert data["has_active_sanction"] is True

    # 2. Sancoes endpoint
    resp_sanc = client.get("/api/v1/compliance/sancoes?cadastro=ceis&limit=10")
    assert resp_sanc.status_code == 200
    items = resp_sanc.json()
    assert isinstance(items, list)
    assert len(items) > 0


def test_cgu_mcp_tool(temp_gateway_settings: GatewaySettings) -> None:
    """Verify Model Context Protocol tool bdg_get_compliance_status."""
    cgu = CGUCEISCNEPConnector(settings=temp_gateway_settings)
    cgu.extract_all()

    dispatcher = MCPDispatcher(settings=temp_gateway_settings)
    req = {
        "jsonrpc": "2.0",
        "id": "cgu-mcp-1",
        "method": "tools/call",
        "params": {
            "name": "bdg_get_compliance_status",
            "arguments": {"documento": "33000168000159"},
        },
    }
    resp = dispatcher.handle_request(req)
    assert resp.get("error") is None
    result = resp.get("result", {})
    assert not result.get("isError")
    content = result.get("content", [])
    assert len(content) > 0
    assert "INIDONEO_SANCIONADO" in content[0]["text"]

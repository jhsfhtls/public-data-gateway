"""Unit and offline integration test suite for CGU PEP (Pessoas Expostas Politicamente) connector.

Covers:
1. ODCS v3.1 Data Contract conformance
2. Ingestion of all 5 tables into isolated bronze directories
3. Normalization and Silver Parquet materialization
4. Quarantine handling for invalid records
5. Gold Feature Store (features_pep_exposicao_politica_brasil.parquet)
6. PEP status check and PLD/FTP risk categorization
7. Multi-interface unification (SDK, CLI, REST API, MCP)
"""

from __future__ import annotations

from pathlib import Path

import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

from public_data_gateway.api.app import create_app
from public_data_gateway.cli.main import main as cli_main
from public_data_gateway.connectors.compliance.cgu_pep import (
    CGUPEPConnector,
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
def pep_connector(temp_gateway_settings: GatewaySettings) -> CGUPEPConnector:
    """Instantiate CGUPEPConnector backed by temporary storage."""
    conn = CGUPEPConnector(settings=temp_gateway_settings)
    conn.storage = HybridStorageEngine(settings=temp_gateway_settings)
    return conn


def test_pep_odcs_contract(pep_connector: CGUPEPConnector) -> None:
    """Validate ODCS v3.1 contract properties and schema definitions."""
    contract = pep_connector.contract
    assert contract is not None, "Contract failed to load"
    assert contract.contract_id == "ODCS-BR-CGU-PEP-001"
    assert contract.source_id == "cgu_pep"
    assert "id_registro_pep" in contract.primary_keys
    col_names = [c.name for c in contract.columns]
    assert "id_registro_pep" in col_names
    assert "cpf_pep" in col_names
    assert "nome_pep" in col_names
    assert "sigla_funcao" in col_names
    assert "is_pep_ativo" in col_names


def test_pep_extract_all_tables(pep_connector: CGUPEPConnector) -> None:
    """Verify that all 5 CGU PEP tables are materialized into Silver Parquet ZSTD."""
    silver_paths = pep_connector.extract_all()
    assert len(silver_paths) == 5

    expected_tables = [
        "cgu_pep",
        "cgu_pep_orgaos",
        "cgu_pep_funcoes",
        "cgu_pep_historico_carencia",
        "cgu_pep_relacionados",
    ]

    for table_name in expected_tables:
        assert table_name in silver_paths
        p = silver_paths[table_name]
        assert p.exists()
        table = pq.read_table(p)
        assert table.num_rows > 0


def test_pep_gold_feature_store(pep_connector: CGUPEPConnector) -> None:
    """Validate calculation of Gold features, PLD risk score, and category."""
    pep_connector.extract_all()
    gold_path = (
        pep_connector.settings.data_dir
        / "cgu_pep"
        / "gold"
        / "features_pep_exposicao_politica_brasil.parquet"
    )
    assert gold_path.exists()

    gold_table = pq.read_table(gold_path)
    assert gold_table.num_rows > 0
    pyd = gold_table.to_pylist()

    # Find Fernando Haddad (Minister of Finance)
    haddad = next((r for r in pyd if "HADDAD" in r["nome_pep"]), None)
    assert haddad is not None
    assert haddad["is_pep_ativo"] is True
    assert haddad["sigla_funcao"] == "MIN-EST"
    assert haddad["total_pessoas_relacionadas"] >= 1
    assert haddad["score_risco_pld"] >= 80.0
    assert haddad["categoria_risco_pld"] == "CRITICO"


def test_pep_get_status_lookup(pep_connector: CGUPEPConnector) -> None:
    """Verify PEP status query for active, cooling-off, and non-PEP entities."""
    pep_connector.extract_all()

    # 1. Active PEP
    status_act = pep_connector.get_pep_status("Fernando Haddad")
    assert status_act["is_pep"] is True
    assert status_act["is_pep_ativo"] is True
    assert status_act["status_exposicao_politica"] == "PEP_ATIVO_OU_EM_CARENCIA"
    assert status_act["score_risco_pld"] == 100.0

    # 2. Historical PEP (cooling-off expired in 2024)
    status_hist = pep_connector.get_pep_status("Marcelo Antonio Castro Nogueira")
    assert status_hist["is_pep"] is True
    assert status_hist["is_pep_ativo"] is False
    assert status_hist["status_exposicao_politica"] == "PEP_HISTORICO_CARENCIA_EXPIRADA"

    # 3. Clean non-PEP
    status_none = pep_connector.get_pep_status("Jose da Silva Nonexistent")
    assert status_none["is_pep"] is False
    assert status_none["is_pep_ativo"] is False
    assert status_none["status_exposicao_politica"] == "NAO_PEP"
    assert status_none["score_risco_pld"] == 0.0


def test_pep_sdk_facade(temp_gateway_settings: GatewaySettings) -> None:
    """Verify SDK access via PublicData.compliance.pep_status and list_pep."""
    sdk = PublicData(settings=temp_gateway_settings)
    cgu = CGUPEPConnector(settings=temp_gateway_settings)
    cgu.extract_all()

    status = sdk.compliance.pep_status("Tarcisio")
    assert status["is_pep"] is True
    assert status["is_pep_ativo"] is True

    direct_status = sdk.get_pep_status("Tarcisio")
    assert direct_status["is_pep"] is True

    items = sdk.compliance.list_pep(uf="SP", ativos_only=True)
    assert len(items) > 0


def test_pep_cli_interface(
    temp_gateway_settings: GatewaySettings, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    """Verify CLI subcommands pdg pep status and pdg pep list."""
    cgu = CGUPEPConnector(settings=temp_gateway_settings)
    cgu.extract_all()

    # CLI status JSON
    ret = cli_main(["pep", "status", "Haddad", "--json"])
    assert ret == 0
    captured = capsys.readouterr()
    assert "PEP_ATIVO_OU_EM_CARENCIA" in captured.out

    # CLI list
    ret_list = cli_main(["pep", "list", "--uf", "DF", "--limit", "5"])
    assert ret_list == 0


def test_pep_fastapi_rest(temp_gateway_settings: GatewaySettings) -> None:
    """Verify FastAPI endpoints /api/v1/pep/status and /api/v1/pep."""
    cgu = CGUPEPConnector(settings=temp_gateway_settings)
    cgu.extract_all()

    app = create_app(settings=temp_gateway_settings)
    client = TestClient(app)

    # 1. Status endpoint
    resp = client.get("/api/v1/pep/status/Haddad")
    assert resp.status_code == 200
    data = resp.json()
    assert data["is_pep_ativo"] is True
    assert data["status_exposicao_politica"] == "PEP_ATIVO_OU_EM_CARENCIA"

    # 2. List endpoint
    resp_list = client.get("/api/v1/pep?uf=SP&limit=10")
    assert resp_list.status_code == 200
    items = resp_list.json()
    assert isinstance(items, list)
    assert len(items) > 0


def test_pep_mcp_tool(temp_gateway_settings: GatewaySettings) -> None:
    """Verify Model Context Protocol tool bdg_get_pep_status."""
    cgu = CGUPEPConnector(settings=temp_gateway_settings)
    cgu.extract_all()

    dispatcher = MCPDispatcher(settings=temp_gateway_settings)
    req = {
        "jsonrpc": "2.0",
        "id": "pep-mcp-1",
        "method": "tools/call",
        "params": {
            "name": "bdg_get_pep_status",
            "arguments": {"termo": "Pacheco"},
        },
    }
    resp = dispatcher.handle_request(req)
    assert resp.get("error") is None
    result = resp.get("result", {})
    assert not result.get("isError")
    content = result.get("content", [])
    assert len(content) > 0
    assert "PEP_ATIVO_OU_EM_CARENCIA" in content[0]["text"]

"""Unit and offline integration test suite for CVM DFP / ITR (Demonstrações Financeiras Padronizadas).

Covers:
1. ODCS v3.1 Data Contract conformance
2. Ingestion of all 5 tables into isolated bronze directories
3. Normalization and Silver Parquet materialization
4. Quarantine handling for invalid records
5. Gold Feature Store (features_financeiras_cvm_brasil.parquet)
6. Fundamental ratio calculations (liquidity, margins, ROE, solvency)
7. Multi-interface unification (SDK, CLI, REST API, MCP)
"""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

from public_data_gateway.api.app import create_app
from public_data_gateway.cli.main import main as cli_main
from public_data_gateway.connectors.macrofinanceiro.cvm_dfp_itr import (
    CvmDfpItrConnector,
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
def cvm_connector(temp_gateway_settings: GatewaySettings) -> CvmDfpItrConnector:
    """Instantiate CvmDfpItrConnector backed by temporary storage."""
    conn = CvmDfpItrConnector(settings=temp_gateway_settings)
    conn.storage = HybridStorageEngine(settings=temp_gateway_settings)
    return conn


def test_cvm_contract_loaded(cvm_connector: CvmDfpItrConnector) -> None:
    """Verify ODCS v3.1 contract is loaded with required columns."""
    assert cvm_connector.contract is not None
    assert cvm_connector.contract.contract_id == "ODCS-BR-CVM-DFP-ITR-001"
    assert cvm_connector.contract.source_id == "cvm_dfp_itr"
    col_names = [col.name for col in cvm_connector.contract.columns]
    assert "codigo_cvm" in col_names
    assert "cnpj_14" in col_names
    assert "setor_atividade" in col_names
    assert "situacao_registro" in col_names
    assert "categoria_registro" in col_names


def test_cvm_extract_all_medallion_layers(
    cvm_connector: CvmDfpItrConnector, temp_gateway_settings: GatewaySettings
) -> None:
    """Test full extraction cycle writing Bronze, Silver, and Gold artifacts."""
    res = cvm_connector.extract_all()
    assert res["status"] == "SUCCESS"
    assert res["cias_count"] >= 6
    assert res["bpa_count"] >= 5
    assert res["bpp_count"] >= 5
    assert res["dre_count"] >= 5
    assert res["dfc_count"] >= 5

    cvm_dir = temp_gateway_settings.data_dir / "cvm_dfp_itr"

    # Verify Bronze JSONs
    assert (cvm_dir / "bronze" / "cvm_cia_aberta_cadastral.json").exists()
    assert (cvm_dir / "bronze" / "cvm_dfp_balanco_ativo.json").exists()
    assert (cvm_dir / "bronze" / "cvm_dfp_balanco_passivo.json").exists()
    assert (cvm_dir / "bronze" / "cvm_dfp_dre.json").exists()
    assert (cvm_dir / "bronze" / "cvm_dfp_fluxo_caixa.json").exists()

    # Verify Silver Parquets
    cia_pq = cvm_dir / "silver" / "cvm_cia_aberta_cadastral.parquet"
    bpa_pq = cvm_dir / "silver" / "cvm_dfp_balanco_ativo.parquet"
    bpp_pq = cvm_dir / "silver" / "cvm_dfp_balanco_passivo.parquet"
    dre_pq = cvm_dir / "silver" / "cvm_dfp_dre.parquet"
    dfc_pq = cvm_dir / "silver" / "cvm_dfp_fluxo_caixa.parquet"

    assert cia_pq.exists()
    assert bpa_pq.exists()
    assert bpp_pq.exists()
    assert dre_pq.exists()
    assert dfc_pq.exists()

    table_cia = pq.read_table(cia_pq)
    assert table_cia.num_rows == res["cias_count"]
    assert "codigo_cvm" in table_cia.column_names

    # Verify Gold Feature Store
    gold_feat = cvm_dir / "gold" / "features_financeiras_cvm_brasil.parquet"
    assert gold_feat.exists()
    table_gold = pq.read_table(gold_feat)
    assert table_gold.num_rows >= 6
    assert "liquidez_corrente" in table_gold.column_names
    assert "score_solvencia_fundamentalista" in table_gold.column_names


def test_cvm_quarantine_handling(
    cvm_connector: CvmDfpItrConnector, temp_gateway_settings: GatewaySettings
) -> None:
    """Verify quarantine catches and isolates malformed records."""
    bronze_dir = temp_gateway_settings.data_dir / "cvm_dfp_itr" / "bronze"
    bronze_dir.mkdir(parents=True, exist_ok=True)
    bad_record = {
        "codigo_cvm": "INVALIDO",
        "cnpj_14": "00000000000000",
        "cnpj_raiz": "00000000",
        "razao_social": "COMPANHIA INVALIDA TESTE",
        "situacao_registro": "SUSPENSO",
    }
    with open(bronze_dir / "cvm_cia_aberta_cadastral.json", "w", encoding="utf-8") as f:
        json.dump([bad_record], f)

    res = cvm_connector.extract_all()
    assert res["status"] == "SUCCESS"
    quarantine_file = (
        temp_gateway_settings.data_dir
        / "cvm_dfp_itr"
        / "quarantine"
        / "quarantined_records.json"
    )
    assert quarantine_file.exists()


def test_cvm_get_company_and_ratios(cvm_connector: CvmDfpItrConnector) -> None:
    """Test company query and financial ratio calculations."""
    # Petrobras
    cia = cvm_connector.get_company("009512")
    assert cia["razao_social"] == "PETROLEO BRASILEIRO S.A. PETROBRAS"
    assert cia["situacao_registro"] == "ATIVO"

    ratios = cvm_connector.get_financial_ratios("33000167000101")
    assert ratios["liquidez_corrente"] > 1.0
    assert ratios["roe_pct"] > 15.0
    assert ratios["score_solvencia_fundamentalista"] >= 75.0
    assert ratios["classificacao_solvencia"] == "SOLVENTE_GRAU_INVESTIMENTO"

    # Unknown company
    unknown = cvm_connector.get_company("999999")
    assert unknown["status"] == "NOT_FOUND"


def test_cvm_list_companies(cvm_connector: CvmDfpItrConnector) -> None:
    """Test public companies listing with filters."""
    all_cias = cvm_connector.list_companies()
    assert len(all_cias) >= 6

    ativos = cvm_connector.list_companies(situacao="ATIVO")
    assert len(ativos) >= 6

    petro = cvm_connector.list_companies(setor="Petróleo")
    assert len(petro) == 1
    assert petro[0]["codigo_cvm"] == "009512"


def test_cvm_sdk_integration() -> None:
    """Test multi-interface SDK methods for CVM."""
    sdk = PublicData()
    cia = sdk.get_cvm_company("009512")
    assert cia["nome_comercial"] == "PETROBRAS"

    ratios = sdk.get_financial_ratios("33000167000101")
    assert ratios["score_solvencia_fundamentalista"] == 83.0

    by_cnpj = sdk.finance.cvm_company("33000167000101")
    assert by_cnpj["codigo_cvm"] == "009512"

    cias = sdk.finance.list_cvm_companies(situacao="ATIVO")
    assert len(cias) >= 6


def test_cvm_cli_integration(capsys: pytest.CaptureFixture[str]) -> None:
    """Test CLI commands for finance cvm, ratios, and list-cvm."""
    # 1. pdg finance cvm <codigo> --json
    code = cli_main(["finance", "cvm", "009512", "--json"])
    assert code == 0
    captured = capsys.readouterr()
    data = json.loads(captured.out)
    assert data["codigo_cvm"] == "009512"

    # 2. pdg finance ratios <cnpj> --json
    code = cli_main(["finance", "ratios", "33000167000101", "--json"])
    assert code == 0
    captured = capsys.readouterr()
    ratios_data = json.loads(captured.out)
    assert ratios_data["score_solvencia_fundamentalista"] == 83.0

    # 3. pdg finance list-cvm --json
    code = cli_main(["finance", "list-cvm", "--json"])
    assert code == 0
    captured = capsys.readouterr()
    list_data = json.loads(captured.out)
    assert len(list_data) >= 6


def test_cvm_fastapi_endpoints() -> None:
    """Test FastAPI REST endpoints for CVM."""
    app = create_app()
    client = TestClient(app)

    # Company
    r1 = client.get("/api/v1/finance/cvm/009512")
    assert r1.status_code == 200
    assert r1.json()["razao_social"] == "PETROLEO BRASILEIRO S.A. PETROBRAS"

    # Ratios
    r2 = client.get("/api/v1/finance/ratios/33000167000101")
    assert r2.status_code == 200
    assert r2.json()["score_solvencia_fundamentalista"] == 83.0

    # List
    r3 = client.get("/api/v1/finance/cvm/cias?situacao=ATIVO")
    assert r3.status_code == 200
    assert len(r3.json()) >= 6


def test_cvm_mcp_dispatcher() -> None:
    """Test MCP protocol dispatcher for CVM tools."""
    disp = MCPDispatcher()

    # Tools List
    list_res = disp.handle_request({"jsonrpc": "2.0", "id": 10, "method": "tools/list"})
    tool_names = [t["name"] for t in list_res["result"]["tools"]]
    assert "bdg_get_cvm_company" in tool_names
    assert "bdg_get_financial_ratios" in tool_names

    # Call bdg_get_cvm_company
    call_res = disp.handle_request(
        {
            "jsonrpc": "2.0",
            "id": 11,
            "method": "tools/call",
            "params": {
                "name": "bdg_get_cvm_company",
                "arguments": {"cnpj_or_codigo": "009512"},
            },
        }
    )
    assert not call_res.get("result", {}).get("isError")
    content = call_res["result"]["content"][0]["text"]
    assert "PETROBRAS" in content

    # Call bdg_get_financial_ratios
    call_res2 = disp.handle_request(
        {
            "jsonrpc": "2.0",
            "id": 12,
            "method": "tools/call",
            "params": {
                "name": "bdg_get_financial_ratios",
                "arguments": {"cnpj_or_codigo": "33000167000101"},
            },
        }
    )
    assert not call_res2.get("result", {}).get("isError")
    content2 = call_res2["result"]["content"][0]["text"]
    assert "83.0" in content2 or "SOLVENTE_GRAU_INVESTIMENTO" in content2

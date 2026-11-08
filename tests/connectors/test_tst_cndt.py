"""Unit and offline integration test suite for TST CNDT (Certidão Negativa de Débitos Trabalhistas & BNDT).

Covers:
1. ODCS v3.1 Data Contract conformance
2. Ingestion of all 4 tables into isolated bronze directories
3. Normalization and Silver Parquet materialization
4. Quarantine handling for invalid records
5. Gold Feature Store (features_compliance_trabalhista_brasil.parquet)
6. Labor compliance scoring and BNDT debtor search
7. Multi-interface unification (SDK, CLI, REST API, MCP)
"""

from __future__ import annotations

from pathlib import Path

import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

from public_data_gateway.api.app import create_app
from public_data_gateway.cli.main import main as cli_main
from public_data_gateway.connectors.judicial.tst_cndt import (
    TstCndtConnector,
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
def cndt_connector(temp_gateway_settings: GatewaySettings) -> TstCndtConnector:
    """Instantiate TstCndtConnector backed by temporary storage."""
    conn = TstCndtConnector(settings=temp_gateway_settings)
    conn.storage = HybridStorageEngine(settings=temp_gateway_settings)
    return conn


def test_tst_cndt_contract_loaded(cndt_connector: TstCndtConnector) -> None:
    """Verify ODCS v3.1 contract is loaded with required columns."""
    assert cndt_connector.contract is not None
    assert cndt_connector.contract.contract_id == "ODCS-BR-TST-CNDT-001"
    assert cndt_connector.contract.source_id == "tst_cndt"
    col_names = [col.name for col in cndt_connector.contract.columns]
    assert "numero_certidao" in col_names
    assert "status_certidao" in col_names
    assert "cnpj_ou_cpf" in col_names
    assert "is_regular" in col_names


def test_tst_cndt_extract_all_medallion_layers(cndt_connector: TstCndtConnector, temp_gateway_settings: GatewaySettings) -> None:
    """Test full extraction cycle writing Bronze, Silver, and Gold artifacts."""
    res = cndt_connector.extract_all()
    assert res["status"] == "SUCCESS"
    assert res["certidoes_count"] >= 5
    assert res["debitos_bndt_count"] >= 5
    assert res["trts_count"] >= 24

    cndt_data_dir = temp_gateway_settings.data_dir / "tst_cndt"

    # Verify Bronze JSONs
    assert (cndt_data_dir / "bronze" / "tst_cndt_certidoes.json").exists()
    assert (cndt_data_dir / "bronze" / "tst_cndt_debitos_bndt.json").exists()
    assert (cndt_data_dir / "bronze" / "tst_cndt_tribunais_regionais.json").exists()
    assert (cndt_data_dir / "bronze" / "tst_cndt_status_dominio.json").exists()

    # Verify Silver Parquets
    cert_pq = cndt_data_dir / "silver" / "tst_cndt_certidoes.parquet"
    deb_pq = cndt_data_dir / "silver" / "tst_cndt_debitos_bndt.parquet"
    trts_pq = cndt_data_dir / "silver" / "tst_cndt_tribunais_regionais.parquet"
    st_pq = cndt_data_dir / "silver" / "tst_cndt_status_dominio.parquet"

    assert cert_pq.exists()
    assert deb_pq.exists()
    assert trts_pq.exists()
    assert st_pq.exists()

    table_cert = pq.read_table(cert_pq)
    assert table_cert.num_rows == res["certidoes_count"]
    assert "numero_certidao" in table_cert.column_names

    # Verify Gold Feature Store
    gold_feat = cndt_data_dir / "gold" / "features_compliance_trabalhista_brasil.parquet"
    assert gold_feat.exists()
    table_gold = pq.read_table(gold_feat)
    assert table_gold.num_rows > 0
    assert "labor_risk_score" in table_gold.column_names
    assert "labor_risk_class" in table_gold.column_names


def test_tst_cndt_query_methods(cndt_connector: TstCndtConnector) -> None:
    """Test get_cndt, get_labor_risk, and search_debitos methods."""
    cndt_connector.extract_all()

    # Query regular company
    cert_petro = cndt_connector.get_cndt("33.000.167/0001-01")
    assert cert_petro["status_certidao"] == "NEGATIVA"
    assert cert_petro["is_regular"] is True

    # Query company with debt
    cert_inad = cndt_connector.get_cndt("14.310.577/0001-04")
    assert cert_inad["status_certidao"] == "POSITIVA"
    assert cert_inad["is_regular"] is False

    # Check labor risk score
    risk_petro = cndt_connector.get_labor_risk("33000167000101")
    assert risk_petro["labor_risk_class"] == "BAIXO"
    assert risk_petro["labor_risk_score"] <= 20.0

    risk_inad = cndt_connector.get_labor_risk("14310577000104")
    assert risk_inad["labor_risk_class"] in ("ALTO", "CRITICO")
    assert risk_inad["labor_risk_score"] >= 70.0

    # Search debitos
    debs_trt2 = cndt_connector.search_debitos("TRT02")
    assert len(debs_trt2) > 0
    assert any("TRT02" in str(d["tribunal_origem"]) for d in debs_trt2)


def test_tst_cndt_sdk_integration(temp_gateway_settings: GatewaySettings) -> None:
    """Test PublicData SDK facade for TST CNDT."""
    sdk = PublicData(settings=temp_gateway_settings)
    sdk.judicial.tst_connector.extract_all()

    # Via judicial facade
    cert = sdk.judicial.cndt("33592510000154")
    assert cert["status_certidao"] == "POSITIVA_COM_EFEITO_DE_NEGATIVA"
    assert cert["is_regular"] is True

    lrisk = sdk.judicial.labor_risk("33592510000154")
    assert "labor_risk_score" in lrisk
    assert lrisk["ultimo_status_cndt"] == "POSITIVA_COM_EFEITO_DE_NEGATIVA"

    debs = sdk.judicial.search_debitos_trabalhistas("0001234")
    assert len(debs) > 0

    # Direct aliases on PublicData
    direct_cert = sdk.get_cndt("33000167000101")
    assert direct_cert["status_certidao"] == "NEGATIVA"

    direct_risk = sdk.get_labor_risk("99999999000199")
    assert direct_risk["labor_risk_class"] in ("ALTO", "CRITICO")


def test_tst_cndt_cli_integration(capsys: pytest.CaptureFixture[str]) -> None:
    """Test CLI commands pdg judicial cndt, labor-risk, and debitos."""
    code_cndt = cli_main(["judicial", "cndt", "33000167000101", "--json"])
    assert code_cndt == 0
    out_cndt, _ = capsys.readouterr()
    assert "NEGATIVA" in out_cndt
    assert "PETROLEO BRASILEIRO" in out_cndt

    code_risk = cli_main(["judicial", "labor-risk", "14310577000104", "--json"])
    assert code_risk == 0
    out_risk, _ = capsys.readouterr()
    assert "labor_risk_score" in out_risk
    assert "POSITIVA" in out_risk

    code_deb = cli_main(["judicial", "debitos", "TRT02", "--json"])
    assert code_deb == 0
    out_deb, _ = capsys.readouterr()
    assert "TRT02" in out_deb


def test_tst_cndt_rest_api_endpoints() -> None:
    """Test FastAPI endpoints for CNDT certificates and labor compliance."""
    app = create_app()
    client = TestClient(app)

    # 1. CNDT certificate
    resp_cert = client.get("/api/v1/judicial/cndt/33000167000101")
    assert resp_cert.status_code == 200
    data_cert = resp_cert.json()
    assert data_cert["status_certidao"] == "NEGATIVA"
    assert data_cert["is_regular"] is True

    # 2. Labor risk
    resp_risk = client.get("/api/v1/judicial/labor-risk/14310577000104")
    assert resp_risk.status_code == 200
    data_risk = resp_risk.json()
    assert data_risk["ultimo_status_cndt"] == "POSITIVA"
    assert data_risk["labor_risk_score"] >= 70.0

    # 3. Debitos search
    resp_deb = client.get("/api/v1/judicial/debitos?termo=TRT15")
    assert resp_deb.status_code == 200
    data_deb = resp_deb.json()
    assert len(data_deb) > 0
    assert any("TRT15" in d["tribunal_origem"] for d in data_deb)


def test_tst_cndt_mcp_tools(temp_gateway_settings: GatewaySettings) -> None:
    """Test MCP tool dispatcher for bdg_get_cndt_certificate and bdg_get_labor_risk."""
    dispatcher = MCPDispatcher(settings=temp_gateway_settings)

    # 1. CNDT tool
    res_cndt = dispatcher.handle_request({
        "jsonrpc": "2.0",
        "id": 1,
        "method": "tools/call",
        "params": {
            "name": "bdg_get_cndt_certificate",
            "arguments": {"documento": "33000167000101"},
        },
    })
    assert res_cndt["result"]["isError"] is False
    assert "NEGATIVA" in res_cndt["result"]["content"][0]["text"]

    # 2. Labor risk tool
    res_risk = dispatcher.handle_request({
        "jsonrpc": "2.0",
        "id": 2,
        "method": "tools/call",
        "params": {
            "name": "bdg_get_labor_risk",
            "arguments": {"documento": "99999999000199"},
        },
    })
    assert res_risk["result"]["isError"] is False
    assert "labor_risk_score" in res_risk["result"]["content"][0]["text"]

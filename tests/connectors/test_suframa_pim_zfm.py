"""Unit and offline integration test suite for SUFRAMA PIM / ZFM & Áreas de Livre Comércio.

Covers:
1. ODCS v3.1 Data Contract conformance
2. Ingestion of all 5 tables into isolated bronze directories
3. Normalization and Silver Parquet materialization
4. Quarantine handling for invalid records
5. Gold Feature Store (features_incentivos_fiscais_zfm_brasil.parquet)
6. Corporate fiscal incentive scoring and PPB compliance
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
from public_data_gateway.connectors.empresarial.suframa_pim_zfm import (
    SuframaPimZfmConnector,
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
def suframa_connector(temp_gateway_settings: GatewaySettings) -> SuframaPimZfmConnector:
    """Instantiate SuframaPimZfmConnector backed by temporary storage."""
    conn = SuframaPimZfmConnector(settings=temp_gateway_settings)
    conn.storage = HybridStorageEngine(settings=temp_gateway_settings)
    return conn


def test_suframa_contract_loaded(suframa_connector: SuframaPimZfmConnector) -> None:
    """Verify ODCS v3.1 contract is loaded with required columns."""
    assert suframa_connector.contract is not None
    assert suframa_connector.contract.contract_id == "ODCS-BR-SUFRAMA-PIM-001"
    assert suframa_connector.contract.source_id == "suframa_pim_zfm"
    col_names = [col.name for col in suframa_connector.contract.columns]
    assert "numero_inscricao_suframa" in col_names
    assert "cnpj_14" in col_names
    assert "situacao_cadastral" in col_names
    assert "tipo_area_incentivada" in col_names
    assert "subsetor_industrial" in col_names


def test_suframa_extract_all_medallion_layers(
    suframa_connector: SuframaPimZfmConnector, temp_gateway_settings: GatewaySettings
) -> None:
    """Test full extraction cycle writing Bronze, Silver, and Gold artifacts."""
    res = suframa_connector.extract_all()
    assert res["status"] == "SUCCESS"
    assert res["empresas_count"] >= 5
    assert res["projetos_count"] >= 5
    assert res["beneficios_count"] >= 5
    assert res["areas_count"] >= 8
    assert res["subsetores_count"] >= 8

    suf_dir = temp_gateway_settings.data_dir / "suframa_pim_zfm"

    # Verify Bronze JSONs
    assert (suf_dir / "bronze" / "suframa_empresas_cadastradas.json").exists()
    assert (suf_dir / "bronze" / "suframa_projetos_incentivados.json").exists()
    assert (suf_dir / "bronze" / "suframa_beneficios_fiscais.json").exists()
    assert (suf_dir / "bronze" / "suframa_areas_livre_comercio.json").exists()
    assert (suf_dir / "bronze" / "suframa_subsetores_industriais.json").exists()

    # Verify Silver Parquets
    emp_pq = suf_dir / "silver" / "suframa_empresas_cadastradas.parquet"
    proj_pq = suf_dir / "silver" / "suframa_projetos_incentivados.parquet"
    ben_pq = suf_dir / "silver" / "suframa_beneficios_fiscais.parquet"
    area_pq = suf_dir / "silver" / "suframa_areas_livre_comercio.parquet"
    sub_pq = suf_dir / "silver" / "suframa_subsetores_industriais.parquet"

    assert emp_pq.exists()
    assert proj_pq.exists()
    assert ben_pq.exists()
    assert area_pq.exists()
    assert sub_pq.exists()

    table_emp = pq.read_table(emp_pq)
    assert table_emp.num_rows == res["empresas_count"]
    assert "numero_inscricao_suframa" in table_emp.column_names

    # Verify Gold Feature Store
    gold_feat = suf_dir / "gold" / "features_incentivos_fiscais_zfm_brasil.parquet"
    assert gold_feat.exists()
    table_gold = pq.read_table(gold_feat)
    assert table_gold.num_rows >= 5
    assert "tax_incentive_score" in table_gold.column_names
    assert "capacidade_fabril_estimada" in table_gold.column_names


def test_suframa_quarantine_handling(
    suframa_connector: SuframaPimZfmConnector, temp_gateway_settings: GatewaySettings
) -> None:
    """Verify quarantine catches and isolates malformed records."""
    bronze_dir = temp_gateway_settings.data_dir / "suframa_pim_zfm" / "bronze"
    bronze_dir.mkdir(parents=True, exist_ok=True)
    bad_record = {
        "numero_inscricao_suframa": "INVALIDO",
        "cnpj_14": "00000000000000",
        "cnpj_raiz": "00000000",
        "razao_social": "EMPRESA INVALIDA TESTE",
        "situacao_cadastral": "SUSPENSA",
    }
    with open(bronze_dir / "suframa_empresas_cadastradas.json", "w", encoding="utf-8") as f:
        json.dump([bad_record], f)

    res = suframa_connector.extract_all()
    assert res["status"] == "SUCCESS"
    quarantine_file = (
        temp_gateway_settings.data_dir
        / "suframa_pim_zfm"
        / "quarantine"
        / "quarantined_records.json"
    )
    assert quarantine_file.exists()


def test_suframa_get_status_and_scoring(suframa_connector: SuframaPimZfmConnector) -> None:
    """Test SUFRAMA status query and tax incentive profiling."""
    # Known PIM company: Moto Honda
    status = suframa_connector.get_suframa_status("04337168000148")
    assert status["situacao_cadastral"] == "ATIVA"
    assert status["numero_inscricao_suframa"] == "200001018"
    assert status["tem_incentivo_fiscal_aprovado"] is True

    profile = suframa_connector.get_tax_incentives_profile("04337168000148")
    assert profile["tax_incentive_score"] >= 80.0
    assert profile["tax_incentive_class"] == "ALTO_INCENTIVO_PIM"
    assert profile["cumpre_processo_produtivo_basico"] is True
    assert profile["capacidade_fabril_estimada"] == "GRANDE_PORTE"

    # Unknown company
    unknown = suframa_connector.get_suframa_status("99999999000199")
    assert unknown["status"] == "NOT_FOUND"


def test_suframa_list_projetos(suframa_connector: SuframaPimZfmConnector) -> None:
    """Test industrial projects listing with filters."""
    all_projs = suframa_connector.list_projetos()
    assert len(all_projs) >= 5

    op_projs = suframa_connector.list_projetos(status="EM_OPERACAO")
    assert len(op_projs) >= 4
    for p in op_projs:
        assert p["status_projeto"] == "EM_OPERACAO"

    honda_projs = suframa_connector.list_projetos("04337168000148")
    assert len(honda_projs) == 1
    assert "PROJ-CAS-2021-042" == honda_projs[0]["codigo_projeto"]


def test_suframa_sdk_integration() -> None:
    """Test multi-interface SDK methods for SUFRAMA and fiscal incentives."""
    sdk = PublicData()
    status = sdk.get_suframa_status("04337168000148")
    assert status["razao_social"] == "MOTO HONDA DA AMAZONIA LTDA"
    assert status["tem_incentivo_fiscal_aprovado"] is True

    tax = sdk.get_tax_incentives("04337168000148")
    assert tax["tax_incentive_score"] == 90.0

    by_insc = sdk.company.suframa("200001018")
    assert by_insc["razao_social"] == "MOTO HONDA DA AMAZONIA LTDA"

    projs = sdk.company.list_pim_projects(status="EM_OPERACAO")
    assert len(projs) >= 4


def test_suframa_cli_integration(capsys: pytest.CaptureFixture[str]) -> None:
    """Test CLI commands for company suframa, tax-incentives, and pim-projects."""
    # 1. pdg company suframa <cnpj> --json
    code = cli_main(["company", "suframa", "04337168000148", "--json"])
    assert code == 0
    captured = capsys.readouterr()
    data = json.loads(captured.out)
    assert data["numero_inscricao_suframa"] == "200001018"

    # 2. pdg company tax-incentives <cnpj> --json
    code = cli_main(["company", "tax-incentives", "04337168000148", "--json"])
    assert code == 0
    captured = capsys.readouterr()
    tax_data = json.loads(captured.out)
    assert tax_data["tax_incentive_score"] == 90.0

    # 3. pdg company pim-projects --json
    code = cli_main(["company", "pim-projects", "--json"])
    assert code == 0
    captured = capsys.readouterr()
    projs_data = json.loads(captured.out)
    assert len(projs_data) >= 5


def test_suframa_fastapi_endpoints() -> None:
    """Test FastAPI REST endpoints for SUFRAMA and fiscal incentives."""
    app = create_app()
    client = TestClient(app)

    # Status
    r1 = client.get("/api/v1/company/suframa/04337168000148")
    assert r1.status_code == 200
    assert r1.json()["razao_social"] == "MOTO HONDA DA AMAZONIA LTDA"

    # Tax Incentives
    r2 = client.get("/api/v1/company/tax-incentives/04337168000148")
    assert r2.status_code == 200
    assert r2.json()["tax_incentive_score"] == 90.0

    # Projects
    r3 = client.get("/api/v1/company/suframa/projetos?status=EM_OPERACAO")
    assert r3.status_code == 200
    assert len(r3.json()) >= 4


def test_suframa_mcp_dispatcher() -> None:
    """Test MCP protocol dispatcher for SUFRAMA tools."""
    disp = MCPDispatcher()

    # Tools List
    list_res = disp.handle_request({"jsonrpc": "2.0", "id": 10, "method": "tools/list"})
    tool_names = [t["name"] for t in list_res["result"]["tools"]]
    assert "bdg_get_suframa_status" in tool_names
    assert "bdg_get_tax_incentives_profile" in tool_names

    # Call bdg_get_suframa_status
    call_res = disp.handle_request(
        {
            "jsonrpc": "2.0",
            "id": 11,
            "method": "tools/call",
            "params": {
                "name": "bdg_get_suframa_status",
                "arguments": {"cnpj_or_inscricao": "04337168000148"},
            },
        }
    )
    assert not call_res.get("result", {}).get("isError")
    content = call_res["result"]["content"][0]["text"]
    assert "MOTO HONDA" in content

    # Call bdg_get_tax_incentives_profile
    call_res2 = disp.handle_request(
        {
            "jsonrpc": "2.0",
            "id": 12,
            "method": "tools/call",
            "params": {
                "name": "bdg_get_tax_incentives_profile",
                "arguments": {"cnpj": "04337168000148"},
            },
        }
    )
    assert not call_res2.get("result", {}).get("isError")
    content2 = call_res2["result"]["content"][0]["text"]
    assert "90.0" in content2 or "GRANDE_PORTE" in content2

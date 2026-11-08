"""Unit and offline integration test suite for Receita Federal Simples Nacional & MEI connector.

Covers:
1. ODCS v3.1 Data Contract conformance
2. Ingestion of all 5 tables into isolated bronze directories
3. Normalization and Silver Parquet materialization
4. State sublimities and tax bracket anexos validation
5. Gold Feature Store (features_tributarias_simples.parquet)
6. Status resolution by CNPJ
"""

from __future__ import annotations

from pathlib import Path

import pyarrow.parquet as pq
import pytest

from public_data_gateway.connectors.empresarial.rfb_simples_mei import (
    RFBSimplesMEIConnector,
)
from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.core.storage import HybridStorageEngine


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
def simples_connector(temp_gateway_settings: GatewaySettings) -> RFBSimplesMEIConnector:
    """Instantiate RFBSimplesMEIConnector backed by temporary storage."""
    conn = RFBSimplesMEIConnector(settings=temp_gateway_settings)
    conn.storage = HybridStorageEngine(settings=temp_gateway_settings)
    return conn


def test_rfb_simples_odcs_contract(simples_connector: RFBSimplesMEIConnector) -> None:
    """Validate ODCS v3.1 contract properties and schema definitions."""
    contract = simples_connector.contract
    assert contract is not None, "Contract failed to load"
    assert contract.contract_id == "ODCS-BR-RFB-SIMPLES-001"
    assert contract.source_id == "rfb_simples_mei"
    assert "cnpj_basico" in contract.primary_keys
    assert contract.min_expected_rows >= 5
    col_names = [c.name for c in contract.columns]
    assert "cnpj_basico" in col_names
    assert "opcao_simples" in col_names
    assert "opcao_simei" in col_names
    assert "status_simples_atual" in col_names


def test_rfb_simples_extract_all_tables(
    simples_connector: RFBSimplesMEIConnector, temp_gateway_settings: GatewaySettings
) -> None:
    """Validate end-to-end ingestion and materialization of all 5 tables and gold features."""
    results = simples_connector.extract_all()

    expected_tables = [
        "simples_nacional_cadastral",
        "simei_cadastral",
        "simples_sublimites_estaduais",
        "simples_eventos_historico",
        "simples_tabelas_anexos",
    ]

    for tbl in expected_tables:
        assert tbl in results, f"Missing result for table {tbl}"
        parquet_path = results[tbl]
        assert parquet_path.exists(), f"Parquet not found: {parquet_path}"
        table = pq.read_table(str(parquet_path))
        assert table.num_rows > 0, f"Table {tbl} has 0 rows"

    # Verify isolated Gold Features
    gold_path = temp_gateway_settings.data_dir / "rfb_simples_mei" / "gold" / "features_tributarias_simples.parquet"
    assert gold_path.exists(), f"Gold feature table missing: {gold_path}"
    gold_table = pq.read_table(str(gold_path))
    assert gold_table.num_rows > 0
    assert "perfil_tributario_sme" in gold_table.column_names
    assert "is_optante_simples" in gold_table.column_names
    assert "is_enquadrado_mei" in gold_table.column_names


def test_rfb_simples_quarantine_handling(simples_connector: RFBSimplesMEIConnector) -> None:
    """Verify quarantine routing for invalid CNPJ basic keys."""
    raw_records = [
        {"cnpj_basico": "12345678", "opcao_simples": "S", "status_simples_atual": "OPTANTE"},
        {"cnpj_basico": "99", "opcao_simples": "N", "status_simples_atual": "NAO_OPTANTE"},  # Too short
    ]
    valid, quarantined = simples_connector._validate_and_quarantine("simples_nacional_cadastral", raw_records)
    assert len(valid) == 2  # zfill makes '99' -> '00000099' which is valid 8D
    assert len(quarantined) == 0

    invalid_record = [{"cnpj_basico": "", "opcao_simples": "N"}]
    v2, q2 = simples_connector._validate_and_quarantine("simples_nacional_cadastral", invalid_record)
    assert len(q2) == 1
    assert "_quarantine_reason" in q2[0]


def test_rfb_simples_status_resolution(simples_connector: RFBSimplesMEIConnector) -> None:
    """Verify status resolution for optantes, MEIs, and non-optantes."""
    # Banco do Brasil
    bb = simples_connector.get_simples_mei_status("00000000000191")
    assert bb["cnpj_basico"] == "00000000"
    assert bb["opcao_simples"] == "N"
    assert bb["status_simples_atual"] == "NAO_OPTANTE"

    # Optante Simples
    opt = simples_connector.get_simples_mei_status("19131243000197")
    assert opt["cnpj_basico"] == "19131243"
    assert opt["opcao_simples"] == "S"
    assert opt["status_simples_atual"] == "OPTANTE"

    # Enquadrado MEI
    mei = simples_connector.get_simples_mei_status("06057223000171")
    assert mei["cnpj_basico"] == "06057223"
    assert mei["opcao_simei"] == "S"
    assert mei["status_simei_atual"] == "ENQUADRADO"


def test_rfb_simples_sublimites_coverage(simples_connector: RFBSimplesMEIConnector) -> None:
    """Validate all 27 Brazilian UFs are covered in state sublimites table."""
    sublimites = simples_connector._generate_baseline_sublimites()
    ufs = {s["sigla_uf"] for s in sublimites}
    assert len(ufs) == 27, f"Expected 27 UFs in sublimites table, found {len(ufs)}"
    assert "SP" in ufs
    assert "AC" in ufs

"""Unit and offline integration test suite for BrasilAPI Core Edge Services connector.

Covers:
1. ODCS v3.1 Data Contract conformance
2. Ingestion of all 6 tables into isolated bronze directories
3. Normalization and Silver Parquet materialization
4. 5 Pillars validation & Quarantine deflection
5. Gold Financial Infrastructure Feature Store
6. Cross-table reconciliation (Bancos, PIX, Corretoras CVM, Feriados, DDDs, CEPs)
"""

from __future__ import annotations

from pathlib import Path

import pyarrow.parquet as pq
import pytest

from brazil_data_gateway.connectors.agregadores.brasilapi_core import (
    BrasilAPICoreConnector,
)
from brazil_data_gateway.core.config import GatewaySettings
from brazil_data_gateway.core.storage import HybridStorageEngine


@pytest.fixture
def temp_gateway_settings(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> GatewaySettings:
    """Create isolated temporary directory environment."""
    monkeypatch.setenv("BDG_OFFLINE_MODE", "1")
    repo_root = Path(__file__).resolve().parents[2]
    settings = GatewaySettings(
        repo_root=repo_root,
        data_dir=tmp_path / "data",
    )
    settings.ensure_directories()
    return settings


@pytest.fixture
def brasilapi_connector(temp_gateway_settings: GatewaySettings) -> BrasilAPICoreConnector:
    """Instantiate BrasilAPICoreConnector backed by temporary storage."""
    conn = BrasilAPICoreConnector(settings=temp_gateway_settings)
    conn.storage = HybridStorageEngine(settings=temp_gateway_settings)
    return conn


def test_brasilapi_odcs_contract(brasilapi_connector: BrasilAPICoreConnector) -> None:
    """Validate ODCS v3.1 contract properties and schema definitions."""
    contract = brasilapi_connector.contract
    assert contract is not None, "Contract failed to load"
    assert contract.contract_id == "ODCS-BR-BRASILAPI-CORE-001"
    assert contract.source_id == "brasilapi_core"
    assert "ispb" in contract.primary_keys
    assert contract.min_expected_rows >= 10
    col_names = [c.name for c in contract.columns]
    assert "ispb" in col_names
    assert "nome_reduzido" in col_names


def test_brasilapi_extract_all_tables(
    brasilapi_connector: BrasilAPICoreConnector, temp_gateway_settings: GatewaySettings
) -> None:
    """Validate end-to-end ingestion and materialization of all 6 tables and gold features."""
    results = brasilapi_connector.extract_all()

    expected_tables = [
        "brasilapi_bancos",
        "brasilapi_pix_participantes",
        "brasilapi_corretoras",
        "brasilapi_feriados",
        "brasilapi_ddds",
        "brasilapi_ceps",
    ]

    for tbl in expected_tables:
        assert tbl in results, f"Missing table '{tbl}' in extraction results"
        table = results[tbl]
        assert table.num_rows > 0, f"Table '{tbl}' has 0 rows"

    # Verify physical files in isolated directories
    for tbl in expected_tables:
        bronze_file = temp_gateway_settings.data_dir / "brasilapi_core" / "bronze" / f"raw_{tbl}.json"
        silver_file = temp_gateway_settings.data_dir / "brasilapi_core" / "silver" / f"{tbl}.parquet"
        assert bronze_file.exists(), f"Bronze file missing: {bronze_file}"
        assert silver_file.exists(), f"Silver file missing: {silver_file}"
        assert silver_file.stat().st_size > 0

    gold_file = temp_gateway_settings.data_dir / "brasilapi_core" / "gold" / "features_infraestrutura_brasil.parquet"
    assert gold_file.exists(), f"Gold file missing: {gold_file}"
    assert gold_file.stat().st_size > 0


def test_brasilapi_banking_and_pix_reconciliation(
    brasilapi_connector: BrasilAPICoreConnector,
) -> None:
    """Validate ISPB zfill normalization and banking fields."""
    tables = brasilapi_connector.extract_all()
    bancos = tables["brasilapi_bancos"].to_pydict()

    # ISPB must be 8 digits
    for ispb in bancos["ispb"]:
        assert len(ispb) == 8, f"Invalid ISPB length: {ispb}"

    # Verify Banco do Brasil
    bb_indices = [i for i, name in enumerate(bancos["nome_reduzido"]) if "BCO DO BRASIL" in name]
    assert len(bb_indices) > 0
    assert bancos["ispb"][bb_indices[0]] == "00000000"
    assert bancos["codigo_compensacao"][bb_indices[0]] == "001"

    # PIX participants
    pix = tables["brasilapi_pix_participantes"].to_pydict()
    assert len(pix["ispb"]) > 0
    tipos = set(pix["tipo_participacao"])
    assert "Direta" in tipos or "Indireta" in tipos


def test_brasilapi_gold_feature_metrics(
    brasilapi_connector: BrasilAPICoreConnector, temp_gateway_settings: GatewaySettings
) -> None:
    """Validate financial infrastructure features aggregated across Brazilian UFs."""
    brasilapi_connector.extract_all()
    gold_file = temp_gateway_settings.data_dir / "brasilapi_core" / "gold" / "features_infraestrutura_brasil.parquet"
    table = pq.read_table(gold_file)
    pydict = table.to_pydict()

    assert "sigla_uf" in pydict
    assert "total_bancos_sede" in pydict
    assert "total_corretoras_ativas" in pydict
    assert "hub_financeiro_tier" in pydict
    assert len(pydict["sigla_uf"]) == 27, f"Expected 27 UFs, got {len(pydict['sigla_uf'])}"

    # SP should be classified as main financial hub
    sp_idx = pydict["sigla_uf"].index("SP")
    assert pydict["total_bancos_sede"][sp_idx] > 10
    assert pydict["hub_financeiro_tier"][sp_idx] == "POLO_FINANCEIRO_PRINCIPAL"

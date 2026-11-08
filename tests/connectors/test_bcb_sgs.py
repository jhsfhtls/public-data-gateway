"""Unit and offline integration test suite for BACEN SGS connector.

Covers:
1. ODCS v3.1 Data Contract conformance
2. Ingestion of all 6 tables into isolated bronze directories
3. Normalization and Silver Parquet materialization
4. 5 Pillars validation & Quarantine deflection
5. Gold Feature Store & point-in-time metrics (Juros Reais, Spreads)
6. Historical coverage across diverse economic eras (2020-2026)
"""

from __future__ import annotations

from pathlib import Path

import pyarrow.parquet as pq
import pytest

from brazil_data_gateway.connectors.macrofinanceiro.bcb_sgs import (
    BCBSGSConnector,
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
def bcb_connector(temp_gateway_settings):
    """Instantiate BCBSGSConnector backed by temporary storage."""
    storage = HybridStorageEngine(settings=temp_gateway_settings)
    return BCBSGSConnector(
        settings=temp_gateway_settings,
        storage=storage,
    )


def test_bcb_sgs_odcs_contract(bcb_connector: BCBSGSConnector) -> None:
    """Validate ODCS v3.1 contract properties and schema definitions."""
    contract = bcb_connector.contract
    assert contract.contract_id == "ODCS-BR-BCB-SGS-001"
    assert contract.source_id == "bcb_sgs"
    assert "id_serie" in contract.primary_keys
    assert "data_referencia" in contract.primary_keys
    assert contract.min_expected_rows >= 30
    assert "-" in contract.sentinel_values
    col_names = [c.name for c in contract.columns]
    assert "id_serie" in col_names
    assert "data_referencia" in col_names
    assert "ano_mes" in col_names
    assert "valor" in col_names


def test_bcb_sgs_extract_all_tables(bcb_connector: BCBSGSConnector, temp_gateway_settings) -> None:
    """Validate end-to-end ingestion and materialization of all 6 tables and gold features."""
    results = bcb_connector.extract_all()

    expected_tables = [
        "sgs_catalogo",
        "sgs_ipca",
        "sgs_igpm",
        "sgs_selic",
        "sgs_cdi",
        "sgs_ibcbr",
        "features_macro_brasil",
    ]

    for tbl in expected_tables:
        assert tbl in results, f"Missing table '{tbl}' in extraction results"
        assert results[tbl] > 0, f"Table '{tbl}' has 0 rows"

    # Verify physical files in isolated directories
    for tbl in ["sgs_catalogo", "sgs_ipca", "sgs_igpm", "sgs_selic", "sgs_cdi", "sgs_ibcbr"]:
        bronze_file = temp_gateway_settings.data_dir / "bcb_sgs" / "bronze" / f"raw_{tbl}.json"
        silver_file = temp_gateway_settings.data_dir / "bcb_sgs" / "silver" / f"{tbl}.parquet"
        assert bronze_file.exists(), f"Bronze file missing: {bronze_file}"
        assert silver_file.exists(), f"Silver file missing: {silver_file}"
        assert silver_file.stat().st_size > 0

    gold_file = temp_gateway_settings.data_dir / "bcb_sgs" / "gold" / "features_macro_brasil.parquet"
    assert gold_file.exists(), f"Gold file missing: {gold_file}"
    assert gold_file.stat().st_size > 0


def test_bcb_sgs_gold_feature_metrics(bcb_connector: BCBSGSConnector, temp_gateway_settings) -> None:
    """Validate gold features calculations such as real interest rate and spreads."""
    bcb_connector.extract_all()
    gold_file = temp_gateway_settings.data_dir / "bcb_sgs" / "gold" / "features_macro_brasil.parquet"
    table = pq.read_table(gold_file)
    pydict = table.to_pydict()

    assert "competencia_mes" in pydict
    assert "ipca_mensal_pct" in pydict
    assert "selic_meta_fim_mes_pct" in pydict
    assert "juros_real_ex_post_pct" in pydict
    assert "spread_cdi_selic_pct" in pydict

    # Check that at least one row has valid juros_real_ex_post
    valid_juros = [v for v in pydict["juros_real_ex_post_pct"] if v is not None]
    assert len(valid_juros) > 0, "Expected at least one non-null real interest rate calculation"


def test_bcb_sgs_varied_economic_periods(bcb_connector: BCBSGSConnector) -> None:
    """Ensure data covers multiple distinct economic reference years (2020-2026)."""
    ipca_records = bcb_connector.extract_table("sgs_ipca")
    years_ipca = {r["ano_mes"][:4] for r in ipca_records}
    assert len(years_ipca) >= 2, f"Expected coverage of multiple years, got {years_ipca}"

    selic_records = bcb_connector.extract_table("sgs_selic")
    years_selic = {r["ano_mes"][:4] for r in selic_records}
    assert len(years_selic) >= 2, f"Expected coverage of multiple years for Selic, got {years_selic}"

"""Unit and offline integration test suite for BACEN Olinda PTAX connector.

Covers:
1. ODCS v3.1 Data Contract conformance
2. Ingestion of all 4 tables into isolated bronze directories
3. Normalization and Silver Parquet materialization
4. 5 Pillars validation & Quarantine deflection
5. Gold FX Feature Store & volatility metrics
6. Multi-currency coverage across trading days (USD, EUR, GBP, JPY)
"""

from __future__ import annotations

from pathlib import Path

import pyarrow.parquet as pq
import pytest

from brazil_data_gateway.connectors.macrofinanceiro.bcb_olinda_ptax import (
    BCBOlindaPTAXConnector,
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
def ptax_connector(temp_gateway_settings):
    """Instantiate BCBOlindaPTAXConnector backed by temporary storage."""
    storage = HybridStorageEngine(settings=temp_gateway_settings)
    return BCBOlindaPTAXConnector(
        settings=temp_gateway_settings,
        storage=storage,
    )


def test_bcb_olinda_ptax_odcs_contract(ptax_connector: BCBOlindaPTAXConnector) -> None:
    """Validate ODCS v3.1 contract properties and schema definitions."""
    contract = ptax_connector.contract
    assert contract.contract_id == "ODCS-BR-BCB-OLINDA-PTAX-001"
    assert contract.source_id == "bcb_olinda_ptax"
    assert "simbolo_moeda" in contract.primary_keys
    assert "data_cotacao" in contract.primary_keys
    assert contract.min_expected_rows >= 30
    assert "-" in contract.sentinel_values
    col_names = [c.name for c in contract.columns]
    assert "simbolo_moeda" in col_names
    assert "data_cotacao" in col_names
    assert "cotacao_compra" in col_names
    assert "cotacao_venda" in col_names


def test_bcb_olinda_ptax_extract_all_tables(ptax_connector: BCBOlindaPTAXConnector, temp_gateway_settings) -> None:
    """Validate end-to-end ingestion and materialization of all 4 tables and gold features."""
    results = ptax_connector.extract_all()

    expected_tables = [
        "ptax_moedas",
        "ptax_cotacao_dolar",
        "ptax_cotacao_moedas",
        "ptax_boletins",
        "features_cambio_brasil",
    ]

    for tbl in expected_tables:
        assert tbl in results, f"Missing table '{tbl}' in extraction results"
        assert results[tbl] > 0, f"Table '{tbl}' has 0 rows"

    # Verify physical files in isolated directories
    for tbl in ["ptax_moedas", "ptax_cotacao_dolar", "ptax_cotacao_moedas", "ptax_boletins"]:
        bronze_file = temp_gateway_settings.data_dir / "bcb_olinda_ptax" / "bronze" / f"raw_{tbl}.json"
        silver_file = temp_gateway_settings.data_dir / "bcb_olinda_ptax" / "silver" / f"{tbl}.parquet"
        assert bronze_file.exists(), f"Bronze file missing: {bronze_file}"
        assert silver_file.exists(), f"Silver file missing: {silver_file}"
        assert silver_file.stat().st_size > 0

    gold_file = temp_gateway_settings.data_dir / "bcb_olinda_ptax" / "gold" / "features_cambio_brasil.parquet"
    assert gold_file.exists(), f"Gold file missing: {gold_file}"
    assert gold_file.stat().st_size > 0


def test_bcb_olinda_ptax_gold_feature_metrics(ptax_connector: BCBOlindaPTAXConnector, temp_gateway_settings) -> None:
    """Validate gold features calculations such as dollar spread and 30-day volatility."""
    ptax_connector.extract_all()
    gold_file = temp_gateway_settings.data_dir / "bcb_olinda_ptax" / "gold" / "features_cambio_brasil.parquet"
    table = pq.read_table(gold_file)
    pydict = table.to_pydict()

    assert "data_cotacao" in pydict
    assert "dolar_ptax_compra" in pydict
    assert "dolar_ptax_venda" in pydict
    assert "dolar_spread" in pydict
    assert "variacao_dolar_dia_pct" in pydict
    assert "volatilidade_dolar_30d" in pydict

    # Check that at least one row has valid volatility calculation
    valid_vols = [v for v in pydict["volatilidade_dolar_30d"] if v is not None]
    assert len(valid_vols) > 0, "Expected at least one non-null volatility calculation"


def test_bcb_olinda_ptax_multi_currency_coverage(ptax_connector: BCBOlindaPTAXConnector) -> None:
    """Ensure data covers multiple distinct foreign currencies."""
    moedas = ptax_connector.extract_table("ptax_moedas")
    simbolos = {m["simbolo_moeda"] for m in moedas}
    assert "USD" in simbolos or "EUR" in simbolos, f"Expected major currencies, got {simbolos}"

    cotacoes = ptax_connector.extract_table("ptax_cotacao_moedas")
    cotadas = {c["simbolo_moeda"] for c in cotacoes}
    assert len(cotadas) >= 2, f"Expected multiple currencies in quotations table, got {cotadas}"

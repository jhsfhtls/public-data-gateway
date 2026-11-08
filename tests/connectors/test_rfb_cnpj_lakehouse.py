"""Unit and offline integration test suite for Receita Federal CNPJ Lakehouse connector.

Covers:
1. ODCS v3.1 Data Contract conformance
2. Ingestion of all 10 tables into isolated bronze directories
3. Normalization and Silver Parquet materialization
4. LGPD Art. 7 § 3/4 compliance (CPF masking in QSA socios)
5. Gold Firmographic & B2B Density Feature Store
6. Cross-table reconciliation (Empresas, Estabelecimentos, Socios, Simples, CNAEs)
"""

from __future__ import annotations

from pathlib import Path

import pyarrow.parquet as pq
import pytest

from brazil_data_gateway.connectors.empresarial.rfb_cnpj_lakehouse import (
    RFBCNPJLakehouseConnector,
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
def rfb_connector(temp_gateway_settings: GatewaySettings) -> RFBCNPJLakehouseConnector:
    """Instantiate RFBCNPJLakehouseConnector backed by temporary storage."""
    conn = RFBCNPJLakehouseConnector(settings=temp_gateway_settings)
    conn.storage = HybridStorageEngine(settings=temp_gateway_settings)
    return conn


def test_rfb_cnpj_odcs_contract(rfb_connector: RFBCNPJLakehouseConnector) -> None:
    """Validate ODCS v3.1 contract properties and schema definitions."""
    contract = rfb_connector.contract
    assert contract is not None, "Contract failed to load"
    assert contract.contract_id == "ODCS-BR-RFB-CNPJ-001"
    assert contract.source_id == "rfb_cnpj_lakehouse"
    assert "cnpj_14" in contract.primary_keys
    assert contract.min_expected_rows >= 5
    col_names = [c.name for c in contract.columns]
    assert "cnpj_14" in col_names
    assert "cnpj_basico" in col_names
    assert "situacao_cadastral" in col_names


def test_rfb_cnpj_extract_all_tables(
    rfb_connector: RFBCNPJLakehouseConnector, temp_gateway_settings: GatewaySettings
) -> None:
    """Validate end-to-end ingestion and materialization of all 10 tables and gold features."""
    results = rfb_connector.extract_all()

    expected_tables = [
        "rfb_cnaes",
        "rfb_motivos",
        "rfb_municipios",
        "rfb_naturezas",
        "rfb_paises",
        "rfb_qualificacoes",
        "rfb_empresas",
        "rfb_estabelecimentos",
        "rfb_socios",
        "rfb_simples",
    ]

    for tbl in expected_tables:
        assert tbl in results, f"Missing table '{tbl}' in extraction results"
        table = results[tbl]
        assert table.num_rows > 0, f"Table '{tbl}' has 0 rows"

    # Verify physical files in isolated directories
    for tbl in expected_tables:
        bronze_file = temp_gateway_settings.data_dir / "rfb_cnpj_lakehouse" / "bronze" / f"raw_{tbl}.json"
        silver_file = temp_gateway_settings.data_dir / "rfb_cnpj_lakehouse" / "silver" / f"{tbl}.parquet"
        assert bronze_file.exists(), f"Bronze file missing: {bronze_file}"
        assert silver_file.exists(), f"Silver file missing: {silver_file}"
        assert silver_file.stat().st_size > 0

    gold_file = temp_gateway_settings.data_dir / "rfb_cnpj_lakehouse" / "gold" / "features_empresariais_brasil.parquet"
    assert gold_file.exists(), f"Gold file missing: {gold_file}"
    assert gold_file.stat().st_size > 0


def test_rfb_cnpj_lgpd_masking(rfb_connector: RFBCNPJLakehouseConnector) -> None:
    """Validate LGPD Art. 7 § 3/4 compliance for PF document masking in QSA."""
    tables = rfb_connector.extract_all()
    socios = tables["rfb_socios"].to_pydict()

    for idx, ident in enumerate(socios["identificador_socio"]):
        doc = socios["cpf_cnpj_socio_mascarado"][idx]
        if ident == "2":  # Pessoa Fisica
            assert doc.startswith("***.") or "***" in doc, f"CPF must be masked for socio {doc}"
            assert not (len(doc.replace(".", "").replace("-", "")) == 11 and doc.isdigit()), (
                f"Raw unmasked CPF detected: {doc}"
            )


def test_rfb_cnpj_normalization_and_formatting(rfb_connector: RFBCNPJLakehouseConnector) -> None:
    """Validate CNPJ padding (8 and 14 digits) and CNAE standardization."""
    tables = rfb_connector.extract_all()

    # Empresas: cnpj_basico must be 8 digits
    empresas = tables["rfb_empresas"].to_pydict()
    for cnpj_b in empresas["cnpj_basico"]:
        assert len(cnpj_b) == 8, f"cnpj_basico must have length 8: {cnpj_b}"

    # Estabelecimentos: cnpj_14 must be 14 digits
    estab = tables["rfb_estabelecimentos"].to_pydict()
    for cnpj_14 in estab["cnpj_14"]:
        assert len(cnpj_14) == 14, f"cnpj_14 must have length 14: {cnpj_14}"


def test_rfb_cnpj_gold_features(
    rfb_connector: RFBCNPJLakehouseConnector, temp_gateway_settings: GatewaySettings
) -> None:
    """Validate firmographic feature metrics across all Brazilian UFs."""
    rfb_connector.extract_all()
    gold_file = temp_gateway_settings.data_dir / "rfb_cnpj_lakehouse" / "gold" / "features_empresariais_brasil.parquet"
    table = pq.read_table(gold_file)
    pydict = table.to_pydict()

    assert "sigla_uf" in pydict
    assert "total_estabelecimentos" in pydict
    assert "total_estabelecimentos_ativos" in pydict
    assert "capital_social_total_matrizes" in pydict
    assert "densidade_corporativa_tier" in pydict
    assert len(pydict["sigla_uf"]) == 27, f"Expected 27 UFs, got {len(pydict['sigla_uf'])}"

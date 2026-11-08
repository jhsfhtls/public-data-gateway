"""Unit and offline integration test suite for PNCP Contratações e Licitações connector.

Covers:
1. ODCS v3.1 Data Contract conformance
2. Ingestion of all 5 tables into isolated bronze directories
3. Normalization and Silver Parquet materialization
4. Quarantine handling for invalid records
5. Gold Feature Store (features_compras_publicas_brasil.parquet)
6. Listing and filtering methods for procurements and contracts
"""

from __future__ import annotations

from pathlib import Path

import pyarrow.parquet as pq
import pytest

from public_data_gateway.connectors.compras_publicas.pncp_contratacoes import (
    PNCPContratacoesConnector,
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
def pncp_connector(temp_gateway_settings: GatewaySettings) -> PNCPContratacoesConnector:
    """Instantiate PNCPContratacoesConnector backed by temporary storage."""
    conn = PNCPContratacoesConnector(settings=temp_gateway_settings)
    conn.storage = HybridStorageEngine(settings=temp_gateway_settings)
    return conn


def test_pncp_odcs_contract(pncp_connector: PNCPContratacoesConnector) -> None:
    """Validate ODCS v3.1 contract properties and schema definitions."""
    contract = pncp_connector.contract
    assert contract is not None, "Contract failed to load"
    assert contract.contract_id == "ODCS-BR-PNCP-CONTRATACOES-001"
    assert contract.source_id == "pncp_contratacoes"
    assert "numero_controle_pncp" in contract.primary_keys
    assert contract.min_expected_rows >= 5
    col_names = [c.name for c in contract.columns]
    assert "numero_controle_pncp" in col_names
    assert "cnpj_orgao" in col_names
    assert "modalidade_id" in col_names
    assert "modalidade_nome" in col_names


def test_pncp_extract_all_tables(
    pncp_connector: PNCPContratacoesConnector, temp_gateway_settings: GatewaySettings
) -> None:
    """Validate end-to-end ingestion and materialization of all 5 tables and gold features."""
    results = pncp_connector.extract_all()

    expected_tables = [
        "pncp_contratacoes",
        "pncp_contratos",
        "pncp_atas",
        "pncp_orgaos",
        "pncp_modalidades",
    ]
    for table_name in expected_tables:
        assert table_name in results, f"Missing table in extraction results: {table_name}"
        silver_path = results[table_name]
        assert silver_path.exists(), f"Silver Parquet file was not materialized: {silver_path}"

        # Verify Parquet content
        arrow_table = pq.read_table(str(silver_path))
        assert arrow_table.num_rows > 0, f"Table {table_name} is empty"

    # Verify Gold Feature Store
    global_gold = (
        temp_gateway_settings.data_dir / "gold" / "features_compras_publicas_brasil.parquet"
    )
    assert global_gold.exists(), "Global features_compras_publicas_brasil.parquet was not created"
    gold_table = pq.read_table(str(global_gold))
    assert gold_table.num_rows > 0, "Gold feature table is empty"
    assert "cnpj_orgao" in gold_table.column_names
    assert "score_maturidade_compras_publicas" in gold_table.column_names


def test_pncp_quarantine_handling(
    pncp_connector: PNCPContratacoesConnector, temp_gateway_settings: GatewaySettings
) -> None:
    """Verify that records with malformed CNPJ or missing control IDs are quarantined."""
    malformed_records = [
        {
            "numero_controle_pncp": "",  # invalid: empty
            "ano_compra": 2024,
            "cnpj_orgao": "01612612000106",
            "razao_social_orgao": "TESTE INVALIDO",
            "valor_total_estimado": 100.0,
            "valor_total_homologado": 90.0,
        },
        {
            "numero_controle_pncp": "VALID-ID-123",
            "ano_compra": 2024,
            "cnpj_orgao": "123",  # invalid: not 14 digits
            "razao_social_orgao": "TESTE INVALIDO CNPJ",
            "valor_total_estimado": 100.0,
            "valor_total_homologado": 90.0,
        },
    ]

    valid, quarantined = pncp_connector._validate_and_quarantine(
        "pncp_contratacoes", malformed_records
    )
    assert len(valid) == 0, f"Expected 0 valid records, got {len(valid)}"
    assert len(quarantined) == 2, f"Expected 2 quarantined records, got {len(quarantined)}"


def test_pncp_listing_filters(pncp_connector: PNCPContratacoesConnector) -> None:
    """Verify list_contratacoes and list_contratos filtering capabilities."""
    all_proc = pncp_connector.list_contratacoes()
    assert len(all_proc) >= 5

    pi_proc = pncp_connector.list_contratacoes(uf="PI")
    assert len(pi_proc) >= 1
    assert all(r["sigla_uf"] == "PI" for r in pi_proc)

    all_contracts = pncp_connector.list_contratos()
    assert len(all_contracts) >= 4

    mg_contracts = pncp_connector.list_contratos(cnpj_orgao="16888315000157")
    assert len(mg_contracts) >= 1
    assert all(r["cnpj_orgao"] == "16888315000157" for r in mg_contracts)

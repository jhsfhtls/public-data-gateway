"""Pytest configuration, markers, and shared fixtures for offline test suites."""

from collections.abc import Generator
from pathlib import Path

import pytest

from brazil_data_gateway.core.config import GatewaySettings
from brazil_data_gateway.core.models import ColumnContract, DataContract, QualityRule


def pytest_configure(config: pytest.Config) -> None:
    """Register custom markers."""
    config.addinivalue_line(
        "markers",
        "live: executes live smoke requests against official government endpoints",
    )


@pytest.fixture
def tmp_settings(tmp_path: Path) -> Generator[GatewaySettings, None, None]:
    """Isolated GatewaySettings using temporary directories."""
    settings = GatewaySettings(
        repo_root=tmp_path / "repo",
        data_dir=tmp_path / "repo" / "data",
    )
    settings.ensure_directories()
    yield settings


@pytest.fixture
def sample_contract() -> DataContract:
    """Standard test data contract with 5-pillar rules."""
    return DataContract(
        contract_id="contract_test_caged",
        source_id="mte_novo_caged",
        version="1.0.0",
        grain="movimentacao_trabalhador",
        primary_keys=["id_registro", "id_municipio"],
        reference_date_column="mes_competencia",
        min_expected_rows=2,
        max_volume_drop_pct=25.0,
        sentinel_values=[99, "SEM INFORMACAO", "-1"],
        columns=[
            ColumnContract(name="id_registro", data_type="int64", required=True, nullable=False, max_null_pct=0.0),
            ColumnContract(name="id_municipio", data_type="string", required=True, nullable=False, max_null_pct=0.0),
            ColumnContract(name="mes_competencia", data_type="string", required=True, nullable=False, max_null_pct=0.0),
            ColumnContract(name="salario", data_type="float64", required=False, nullable=True, max_null_pct=0.1, sentinel_values=[999999.0]),
        ],
        quality_rules=[
            QualityRule(rule_type="non_null", column="id_registro"),
            QualityRule(rule_type="min", column="salario", params={"min_value": 0.0}),
        ],
    )

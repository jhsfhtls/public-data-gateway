"""Tests for Hybrid Medallion Lakehouse, 5-Pillar Observability, and ASOF Join (POP_T_074, POP_T_128)."""


import pyarrow as pa
import pytest

from brazil_data_gateway.core.config import GatewaySettings
from brazil_data_gateway.core.contracts import apply_sentinel_cleaning
from brazil_data_gateway.core.http_client import CircuitBreakerOpenError, ResilientHttpClient
from brazil_data_gateway.core.models import DataContract
from brazil_data_gateway.core.storage import HybridStorageEngine


def test_sqlite_cache_and_stale_fallback(tmp_settings: GatewaySettings) -> None:
    """Test SQLite TTL cache and Stale-While-Error fallback when circuit breaker trips."""
    storage = HybridStorageEngine(tmp_settings)
    key = "GET:https://api.gov.br/caged:123"

    # Set expired cache entry directly (ttl -10s)
    storage.set_cached(key, b'{"status": "stale_data"}', ttl_seconds=-10)

    # get_cached returns None because it is expired
    assert storage.get_cached(key) is None

    # get_stale returns data even though expired
    stale_entry = storage.get_stale(key)
    assert stale_entry is not None
    assert stale_entry["value"] == b'{"status": "stale_data"}'
    assert stale_entry["stale"] is True

    # Test CircuitBreaker fallback with ResilientHttpClient
    client = ResilientHttpClient(settings=tmp_settings, storage_engine=storage)
    # Trip breaker by recording failures
    for _ in range(3):
        client.circuit_breaker.record_failure()

    assert not client.circuit_breaker.can_execute()

    # When stale_while_error=True, should return stale response instead of raising
    resp = client.get(
        "https://api.gov.br/caged:123",
        stale_while_error=True,
    )
    assert resp.status_code == 200
    assert resp.json() == {"status": "stale_data"}
    assert resp.headers.get("X-BDG-Stale") == "true"

    # When stale_while_error=False, should raise CircuitBreakerOpenError
    with pytest.raises(CircuitBreakerOpenError):
        client.get("https://api.gov.br/caged:123", stale_while_error=False)


def test_sentinel_cleaning(sample_contract: DataContract) -> None:
    """Test replacement of contract sentinel values with None."""
    raw_rows = [
        {"id_registro": 1, "id_municipio": "3550308", "mes_competencia": "2023-01", "salario": 2500.0},
        {"id_registro": 2, "id_municipio": "SEM INFORMACAO", "mes_competencia": "2023-01", "salario": 999999.0},
        {"id_registro": 99, "id_municipio": "1200013", "mes_competencia": "2023-01", "salario": -1},
    ]

    cleaned = apply_sentinel_cleaning(raw_rows, sample_contract)
    assert cleaned[0]["id_municipio"] == "3550308"
    assert cleaned[0]["salario"] == 2500.0

    # Column-level and global sentinel replaced with None
    assert cleaned[1]["id_municipio"] is None
    assert cleaned[1]["salario"] is None

    assert cleaned[2]["id_registro"] is None
    assert cleaned[2]["salario"] is None


def test_five_pillars_silver_atomic_write(
    tmp_settings: GatewaySettings, sample_contract: DataContract
) -> None:
    """Test 5-Pillar validation pass resulting in atomic Silver layer write and SHA-256 manifest."""
    storage = HybridStorageEngine(tmp_settings)

    valid_data = pa.table(
        {
            "id_registro": [101, 102, 103],
            "id_municipio": ["3550308", "1200013", "5101837"],
            "mes_competencia": ["2023-01", "2023-01", "2023-01"],
            "salario": [3000.0, 4500.0, 2800.0],
        }
    )

    target_file, manifest, report = storage.write_medallion_table(
        valid_data, source_id="mte_novo_caged", layer="silver", contract=sample_contract
    )

    assert target_file.is_file()
    assert target_file == tmp_settings.silver_dir / "mte_novo_caged.parquet"
    assert report is not None
    assert report.passed is True
    assert len(report.violations) == 0

    assert manifest.source_id == "mte_novo_caged"
    assert manifest.layer == "silver"
    assert manifest.row_count == 3
    assert len(manifest.sha256) == 64

    # Verify manifest on disk
    manifest_file = tmp_settings.silver_dir / "mte_novo_caged.manifest.json"
    assert manifest_file.is_file()


def test_five_pillars_failure_quarantine_routing(
    tmp_settings: GatewaySettings, sample_contract: DataContract
) -> None:
    """Test that a schema/volume violation automatically routes to quarantine and preserves healthy silver."""
    storage = HybridStorageEngine(tmp_settings)

    # 1. First write healthy silver table (10 rows)
    healthy_data = pa.table(
        {
            "id_registro": list(range(1, 11)),
            "id_municipio": ["3550308"] * 10,
            "mes_competencia": ["2023-01"] * 10,
            "salario": [2500.0] * 10,
        }
    )
    silver_path, _, _ = storage.write_medallion_table(
        healthy_data, source_id="mte_novo_caged", layer="silver", contract=sample_contract
    )
    assert silver_path.is_file()
    assert storage.read_table("mte_novo_caged", layer="silver").num_rows == 10

    # 2. Write bad table: contains NULL in primary key 'id_registro' AND volume drops by 90%
    corrupted_data = pa.table(
        {
            "id_registro": [None],
            "id_municipio": ["3550308"],
            "mes_competencia": ["2023-01"],
            "salario": [2500.0],
        }
    )

    target_file, manifest, report = storage.write_medallion_table(
        corrupted_data, source_id="mte_novo_caged", layer="silver", contract=sample_contract
    )

    # 3. Assertions: Routed to quarantine, Silver table untouched
    assert report is not None
    assert report.passed is False
    assert any("Primary key 'id_registro'" in v for v in report.violations)
    assert any("Volume: Volume dropped by" in v for v in report.violations)

    assert manifest.layer == "quarantine"
    assert target_file.parent == tmp_settings.quarantine_dir

    # Healthy silver table still exists with original 10 rows!
    silver_table = storage.read_table("mte_novo_caged", layer="silver")
    assert silver_table.num_rows == 10


def test_asof_point_in_time_join_zero_data_leakage(tmp_settings: GatewaySettings) -> None:
    """Test DuckDB ASOF join preventing future data leakage in ML features (POP_T_128)."""
    storage = HybridStorageEngine(tmp_settings)

    # Left table: transactions / events
    left_table = pa.table(
        {
            "cnpj": ["00000000000191", "00000000000191", "00000000000191"],
            "event_date": ["2023-01-15", "2023-04-15", "2023-08-01"],
            "amount": [100.0, 200.0, 300.0],
        }
    )

    # Right table: financial risk ratings released over time
    right_table = pa.table(
        {
            "cnpj": ["00000000000191", "00000000000191", "00000000000191"],
            "reference_date": ["2023-01-01", "2023-04-01", "2023-07-01"],
            "risk_score": [0.10, 0.25, 0.80],
        }
    )

    joined = storage.asof_join(
        left_table=left_table,
        right_table=right_table,
        entity_key="cnpj",
        left_time_col="event_date",
        right_time_col="reference_date",
    )

    joined_dict = joined.to_pydict()

    # Event 2023-01-15 must match 2023-01-01 (score 0.10), NOT future 2023-04-01 (score 0.25)
    assert joined_dict["risk_score"][0] == 0.10
    # Event 2023-04-15 must match 2023-04-01 (score 0.25), NOT future 2023-07-01 (score 0.80)
    assert joined_dict["risk_score"][1] == 0.25
    # Event 2023-08-01 must match 2023-07-01 (score 0.80)
    assert joined_dict["risk_score"][2] == 0.80


def test_profile_parquet(tmp_settings: GatewaySettings) -> None:
    """Test compact token-safe Parquet metadata profiling."""
    storage = HybridStorageEngine(tmp_settings)
    table = pa.table(
        {
            "id": [1, 2, 3],
            "val": [10.5, 20.0, 30.5],
            "name": ["A", "B", None],
        }
    )
    path = tmp_settings.silver_dir / "sample_profile.parquet"
    storage.write_medallion_table(table, "sample_profile", layer="silver")

    profile = storage.profile_parquet(path)
    assert profile["row_count"] == 3
    assert "val" in profile["columns"]
    assert profile["null_counts"]["name"] == 1
    assert profile["numeric_summary"]["val"]["min"] == 10.5
    assert profile["numeric_summary"]["val"]["max"] == 30.5
    assert len(profile["sha256"]) == 64
    assert profile["size_bytes"] > 0

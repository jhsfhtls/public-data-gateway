"""ODCS v3.1 Data Contracts and 5-Pillar Data Observability Engine (POP_T_070, POP_T_074)."""

import hashlib
from pathlib import Path
from typing import Any

import pyarrow as pa
import yaml

from brazil_data_gateway.core.models import DataContract, ObservabilityReport


def load_contract(path: Path) -> DataContract:
    """Load an ODCS v3.1 YAML contract specification from disk."""
    if not path.is_file():
        raise FileNotFoundError(f"Contract file not found at: {path}")

    with open(path, encoding="utf-8") as f:
        data = yaml.safe_load(f)

    if not isinstance(data, dict):
        raise ValueError(f"Invalid contract format in {path}: expected YAML mapping.")

    return DataContract.model_validate(data)


def apply_sentinel_cleaning(
    rows: list[dict[str, Any]], contract: DataContract
) -> list[dict[str, Any]]:
    """Replaces global and column-specific sentinel values (e.g. 99, -1, 'SEM INFORMACAO') with None."""
    # Build fast sentinel lookup sets
    global_sentinels: set[Any] = set()
    for s in contract.sentinel_values:
        global_sentinels.add(s)
        global_sentinels.add(str(s).strip())
        if isinstance(s, str):
            global_sentinels.add(s.strip().upper())
            global_sentinels.add(s.strip().lower())
            try:
                global_sentinels.add(int(s))
                global_sentinels.add(float(s))
            except ValueError:
                pass

    col_sentinels: dict[str, set[Any]] = {}
    for col in contract.columns:
        if col.sentinel_values:
            c_set: set[Any] = set()
            for s in col.sentinel_values:
                c_set.add(s)
                c_set.add(str(s).strip())
                if isinstance(s, str):
                    c_set.add(s.strip().upper())
                    c_set.add(s.strip().lower())
                    try:
                        c_set.add(int(s))
                        c_set.add(float(s))
                    except ValueError:
                        pass
            col_sentinels[col.name] = c_set

    cleaned_rows: list[dict[str, Any]] = []
    for row in rows:
        cleaned_row: dict[str, Any] = {}
        for k, v in row.items():
            if v is None:
                cleaned_row[k] = None
                continue

            v_str = str(v).strip()
            # Check global sentinels
            is_sentinel = (
                v in global_sentinels
                or v_str in global_sentinels
                or v_str.upper() in global_sentinels
                or v_str.lower() in global_sentinels
            )

            # Check column-specific sentinels
            if not is_sentinel and k in col_sentinels:
                c_set = col_sentinels[k]
                is_sentinel = (
                    v in c_set
                    or v_str in c_set
                    or v_str.upper() in c_set
                    or v_str.lower() in c_set
                )

            cleaned_row[k] = None if is_sentinel else v
        cleaned_rows.append(cleaned_row)

    return cleaned_rows


def evaluate_five_pillars(
    table: pa.Table,
    contract: DataContract,
    previous_row_count: int | None = None,
) -> ObservabilityReport:
    """Evaluates the 5 Pillars of Data Observability: Freshness, Volume, Schema, Distribution, Lineage."""
    violations: list[str] = []
    freshness_ok = True
    volume_ok = True
    schema_ok = True
    distribution_ok = True

    # 1. Freshness Pillar
    if contract.reference_date_column and contract.reference_date_column in table.column_names:
        date_col = table[contract.reference_date_column]
        if date_col.null_count == table.num_rows:
            freshness_ok = False
            violations.append(
                f"Freshness: Reference date column '{contract.reference_date_column}' contains only nulls."
            )

    # 2. Volume Pillar
    if table.num_rows < contract.min_expected_rows:
        volume_ok = False
        violations.append(
            f"Volume: Row count {table.num_rows} is below contract minimum {contract.min_expected_rows}."
        )

    if previous_row_count is not None and previous_row_count > 0:
        if table.num_rows < previous_row_count:
            drop_pct = ((previous_row_count - table.num_rows) / previous_row_count) * 100.0
            if drop_pct > contract.max_volume_drop_pct:
                volume_ok = False
                violations.append(
                    f"Volume: Volume dropped by {drop_pct:.1f}%, exceeding allowable threshold of {contract.max_volume_drop_pct:.1f}%."
                )

    # 3. Schema Pillar (Required columns & Non-null Primary Keys)
    table_cols = set(table.column_names)
    for col_contract in contract.columns:
        if col_contract.required and col_contract.name not in table_cols:
            schema_ok = False
            violations.append(f"Schema: Missing mandatory column '{col_contract.name}'.")

    for pk in contract.primary_keys:
        if pk in table_cols:
            null_count = table[pk].null_count
            if null_count > 0:
                schema_ok = False
                violations.append(
                    f"Schema: Primary key '{pk}' contains {null_count} null entries."
                )
        else:
            schema_ok = False
            violations.append(f"Schema: Primary key '{pk}' not found in table columns.")

    # 4. Distribution Pillar (Null ceiling checks)
    for col_contract in contract.columns:
        if col_contract.name in table_cols:
            col_nulls = table[col_contract.name].null_count
            null_pct = col_nulls / max(table.num_rows, 1)
            if null_pct > col_contract.max_null_pct:
                distribution_ok = False
                violations.append(
                    f"Distribution: Column '{col_contract.name}' null percentage {null_pct:.1%} exceeds ceiling {col_contract.max_null_pct:.1%}."
                )

    # 5. Lineage & Deterministic Hash Pillar
    # Calculate deterministic hash from sorted schema and row representation
    hasher = hashlib.sha256()
    hasher.update(contract.contract_id.encode("utf-8"))
    hasher.update(str(table.num_rows).encode("utf-8"))
    for name in sorted(table.column_names):
        hasher.update(name.encode("utf-8"))
        col_type = str(table.schema.field(name).type)
        hasher.update(col_type.encode("utf-8"))

    # Include small sample or row count for lineage
    lineage_hash = hasher.hexdigest()

    passed = freshness_ok and volume_ok and schema_ok and distribution_ok

    return ObservabilityReport(
        freshness_ok=freshness_ok,
        volume_ok=volume_ok,
        schema_ok=schema_ok,
        distribution_ok=distribution_ok,
        lineage_hash=lineage_hash,
        passed=passed,
        violations=violations,
    )

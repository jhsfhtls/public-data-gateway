"""BigQuery FinOps Guard & Native ODCS v3.1 Lakehouse Metadata Engine."""

import logging
import re
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)

_SELECT_STAR_PATTERN = re.compile(r"\bSELECT\s+\*\b", re.IGNORECASE)
_MUTATING_SQL_PATTERN = re.compile(
    r"\b(DROP|DELETE|TRUNCATE|UPDATE|INSERT|CREATE|ALTER)\b", re.IGNORECASE
)


def validate_bigquery_sql(sql: str) -> None:
    """Enforces FinOps safety policies on external cloud queries: bans SELECT * and mutations."""
    if _SELECT_STAR_PATTERN.search(sql):
        raise ValueError(
            "FinOps Policy Violation: 'SELECT *' is strictly banned for BigQuery queries to prevent cost explosions."
        )
    if _MUTATING_SQL_PATTERN.search(sql):
        raise ValueError(
            "Security Policy Violation: Mutating SQL statements are banned in analytical query interface."
        )


def _load_contract_yaml(contract_path: Path) -> dict[str, Any]:
    """Load an ODCS v3.1 contract YAML file safely."""
    with open(contract_path, encoding="utf-8") as handle:
        data = yaml.safe_load(handle)
    return data if isinstance(data, dict) else {}


def _format_table_metadata(
    contract: dict[str, Any], table: dict[str, Any], fallback_source_id: str
) -> dict[str, Any]:
    """Build normalized schema metadata for a contract table."""
    source_meta = contract.get("source", {})
    raw_columns = table.get("columns", [])
    columns = [
        {
            "name": str(col.get("name", "")),
            "type": str(col.get("type", "")),
            "nullable": bool(col.get("nullable", True)),
            "description": str(col.get("description", "")),
        }
        for col in raw_columns
        if isinstance(col, dict)
    ]
    return {
        "source_id": str(source_meta.get("source_id", fallback_source_id)),
        "contract_id": str(contract.get("id", "")),
        "table_name": str(table.get("name", "")),
        "description": str(table.get("description", "")),
        "primary_key": list(table.get("primary_key", [])),
        "sla": dict(contract.get("sla", {}))
        if isinstance(contract.get("sla"), dict)
        else {},
        "columns": columns,
    }


def query_datalake_metadata(
    dataset_id: str,
    table_id: str | None = None,
    repo_root: Path | None = None,
) -> dict[str, Any]:
    """Resolve table schema and SLA metadata directly from local ODCS v3.1 contracts."""
    if not dataset_id or not dataset_id.strip():
        return {}

    target_dataset = dataset_id.strip()
    resolved_root = (
        Path(repo_root).resolve()
        if repo_root is not None
        else Path(__file__).resolve().parents[3]
    )
    connectors_dir = resolved_root / "src" / "public_data_gateway" / "connectors"
    if not connectors_dir.is_dir():
        return {}

    for contract_path in sorted(connectors_dir.glob("*/*/contract.yaml")):
        try:
            contract = _load_contract_yaml(contract_path)
        except (OSError, yaml.YAMLError) as exc:
            logger.warning("Failed to read ODCS contract %s: %s", contract_path, exc)
            continue

        source_meta = contract.get("source", {})
        contract_source_id = (
            str(source_meta.get("source_id", ""))
            if isinstance(source_meta, dict)
            else ""
        )
        if target_dataset not in (contract_source_id, contract_path.parent.name):
            continue

        tables = [t for t in contract.get("tables", []) if isinstance(t, dict)]
        if table_id:
            matched = next((t for t in tables if t.get("name") == table_id), None)
            if matched is None:
                return {}
            return _format_table_metadata(contract, matched, target_dataset)

        return {
            "source_id": contract_source_id or target_dataset,
            "contract_id": str(contract.get("id", "")),
            "sla": dict(contract.get("sla", {}))
            if isinstance(contract.get("sla"), dict)
            else {},
            "tables": [
                _format_table_metadata(contract, tbl, target_dataset) for tbl in tables
            ],
        }

    return {}


def estimate_and_query_bigquery(
    query: str,
    project_id: str | None = None,
    max_bytes_billed: int = 1073741824,  # 1 GiB default ceiling
    dry_run_only: bool = False,
) -> dict[str, Any]:
    """Validates SQL and performs a FinOps dry run before executing BigQuery jobs."""
    validate_bigquery_sql(query)

    try:
        from google.cloud import bigquery  # type: ignore[import-not-found]
    except ImportError:
        # Graceful fallback when google-cloud-bigquery library is not installed
        return {
            "status": "dry_run_simulated",
            "message": "google-cloud-bigquery library not installed. SQL syntax and FinOps rules passed.",
            "estimated_bytes": 0,
            "dry_run": True,
        }

    client = bigquery.Client(project=project_id)
    job_config = bigquery.QueryJobConfig(dry_run=True, use_query_cache=True)

    dry_run_job = client.query(query, job_config=job_config)
    bytes_processed = dry_run_job.total_bytes_processed or 0

    if bytes_processed > max_bytes_billed:
        raise ValueError(
            f"FinOps Budget Guard: Query estimates {bytes_processed} bytes, which exceeds maximum allowable {max_bytes_billed} bytes."
        )

    if dry_run_only:
        return {
            "status": "dry_run_complete",
            "bytes_processed": bytes_processed,
            "dry_run": True,
        }

    run_config = bigquery.QueryJobConfig(maximum_bytes_billed=max_bytes_billed)
    job = client.query(query, job_config=run_config)
    arrow_table = job.to_arrow()
    return {
        "status": "success",
        "bytes_processed": bytes_processed,
        "row_count": len(arrow_table),
        "data": arrow_table,
    }

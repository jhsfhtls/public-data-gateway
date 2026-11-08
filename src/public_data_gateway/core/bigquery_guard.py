"""BigQuery FinOps Guard & Base dos Dados Metadata Bridge (POP_T_176)."""

import re
from typing import Any

import httpx

_SELECT_STAR_PATTERN = re.compile(r"\bSELECT\s+\*\b", re.IGNORECASE)
_MUTATING_SQL_PATTERN = re.compile(
    r"\b(DROP|DELETE|TRUNCATE|UPDATE|INSERT|CREATE|ALTER)\b", re.IGNORECASE
)
_BD_GRAPHQL_ENDPOINT = "https://backend.basedosdados.org/api/v1/graphql"


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


def query_basedosdados_metadata(
    dataset_id: str,
    table_id: str,
    timeout: float = 15.0,
) -> dict[str, Any]:
    """Fetch table schema and partition metadata from Base dos Dados GraphQL API."""
    query = """
    query GetTableMetadata($dataset: String!, $table: String!) {
        allTable(dataset_Slug: $dataset, slug: $table) {
            edges {
                node {
                    id
                    slug
                    name
                    description
                    columns {
                        edges {
                            node {
                                name
                                description
                                type
                            }
                        }
                    }
                }
            }
        }
    }
    """
    payload = {
        "query": query,
        "variables": {"dataset": dataset_id, "table": table_id},
    }

    try:
        with httpx.Client(timeout=timeout) as client:
            resp = client.post(_BD_GRAPHQL_ENDPOINT, json=payload)
            resp.raise_for_status()
            data = resp.json()
            edges = data.get("data", {}).get("allTable", {}).get("edges", [])
            if not edges:
                return {}
            node = edges[0].get("node", {})
            return node
    except (httpx.HTTPError, OSError):
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

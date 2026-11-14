"""Serverless Public Data Warehouse Adapters (DuckDB, MotherDuck, and BigQuery)."""

from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

import duckdb

from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.lakehouse.cdc import CdcDeltaTracker

_BANNED_SQL_PATTERN = re.compile(
    r"\b(DROP|DELETE|TRUNCATE|ALTER|UPDATE|INSERT|CREATE|ATTACH|COPY|DETACH)\b",
    re.IGNORECASE,
)


class PublicWarehouseAdapter:
    """Publishes mirrored Parquet datasets as views/external tables across serverless warehouses."""

    VALID_PROVIDERS = {"duckdb", "motherduck", "bigquery"}

    def __init__(
        self,
        settings: GatewaySettings | None = None,
        cdc_tracker: CdcDeltaTracker | None = None,
        duckdb_path: Path | None = None,
    ) -> None:
        self.settings = settings or GatewaySettings()
        self.cdc = cdc_tracker or CdcDeltaTracker(self.settings)
        self.duckdb_path = duckdb_path or (self.settings.data_dir / "brasil.duckdb")

    @staticmethod
    def _sanitize_view_name(source_id: str, layer: str, stem: str) -> str:
        raw = f"{layer}_{source_id}_{stem}".lower()
        cleaned = re.sub(r"[^a-z0-9_]", "_", raw)
        return re.sub(r"_+", "_", cleaned).strip("_")

    def _artifact_uri(self, art: dict[str, Any], provider: str) -> str:
        if provider == "duckdb":
            return Path(art["local_path"]).resolve().as_posix()
        mirror_provider = self.settings.mirror_provider
        prefix = self.settings.mirror_prefix.strip("/")
        obj_key = f"{prefix}/{art['relative_key']}" if prefix else art["relative_key"]
        if mirror_provider in ("s3", "r2") and self.settings.mirror_bucket:
            scheme = "gs" if provider == "bigquery" else mirror_provider
            return f"{scheme}://{self.settings.mirror_bucket}/{obj_key}"
        return Path(art["local_path"]).resolve().as_posix()

    def generate_ddl(
        self,
        provider: str | None = None,
        layers: tuple[str, ...] = ("silver", "gold"),
        sources: list[str] | None = None,
    ) -> dict[str, Any]:
        """Generate serverless SQL view / external-table DDL statements for discovered artifacts."""
        selected = (provider or self.settings.warehouse_provider or "duckdb").lower()
        if selected not in self.VALID_PROVIDERS:
            raise ValueError(
                f"Unsupported warehouse_provider '{selected}'. Expected one of {sorted(self.VALID_PROVIDERS)}."
            )

        artifacts = self.cdc.discover_local_artifacts(layers=layers, sources=sources)
        statements: list[dict[str, str]] = []

        for art in artifacts:
            stem = Path(art["local_path"]).stem
            view_name = self._sanitize_view_name(art["source_id"], art["layer"], stem)
            uri = self._artifact_uri(art, selected)
            escaped_uri = uri.replace("'", "''")

            if selected == "bigquery":
                project = os.environ.get("GCP_BILLING_PROJECT_ID", "").strip() or "your-gcp-project"
                dataset = self.settings.bq_dataset or "public_data_gateway"
                sql = (
                    f"CREATE OR REPLACE EXTERNAL TABLE `{project}.{dataset}.{view_name}`\n"
                    f"OPTIONS (\n"
                    f"  format = 'PARQUET',\n"
                    f"  uris = ['{escaped_uri}']\n"
                    f");"
                )
            elif selected == "motherduck":
                db_name = self.settings.warehouse_database or "public_data_gateway"
                sql = (
                    f"CREATE OR REPLACE VIEW {db_name}.main.{view_name} AS\n"
                    f"SELECT * FROM read_parquet('{escaped_uri}');"
                )
            else:
                sql = (
                    f"CREATE OR REPLACE VIEW {view_name} AS\n"
                    f"SELECT * FROM read_parquet('{escaped_uri}');"
                )

            statements.append({
                "view_name": view_name,
                "source_id": art["source_id"],
                "layer": art["layer"],
                "uri": uri,
                "ddl": sql,
            })

        return {
            "provider": selected,
            "total_views": len(statements),
            "statements": statements,
        }

    def publish_views(
        self,
        provider: str | None = None,
        layers: tuple[str, ...] = ("silver", "gold"),
        sources: list[str] | None = None,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Register Parquet views in DuckDB (or MotherDuck/BigQuery when configured)."""
        plan = self.generate_ddl(provider=provider, layers=layers, sources=sources)
        selected = plan["provider"]
        published: list[str] = []

        if not dry_run and plan["statements"]:
            if selected == "duckdb":
                self.duckdb_path.parent.mkdir(parents=True, exist_ok=True)
                with duckdb.connect(str(self.duckdb_path)) as conn:
                    conn.execute(f"SET memory_limit='{self.settings.duckdb_memory_limit}';")
                    for stmt in plan["statements"]:
                        conn.execute(stmt["ddl"])
                        published.append(stmt["view_name"])
            elif selected == "motherduck":
                token = os.environ.get("BDG_MOTHERDUCK_TOKEN", "").strip()
                if not token:
                    raise ValueError(
                        "BDG_MOTHERDUCK_TOKEN must be configured for live MotherDuck publishing."
                    )
                db_name = self.settings.warehouse_database or "public_data_gateway"
                with duckdb.connect(f"md:{db_name}?motherduck_token={token}") as conn:
                    for stmt in plan["statements"]:
                        conn.execute(stmt["ddl"])
                        published.append(stmt["view_name"])
            elif selected == "bigquery":
                project = os.environ.get("GCP_BILLING_PROJECT_ID", "").strip()
                if not project:
                    raise ValueError(
                        "GCP_BILLING_PROJECT_ID must be configured for live BigQuery publishing."
                    )
                try:
                    from google.cloud import bigquery  # type: ignore[import-untyped]
                except ImportError as exc:
                    raise RuntimeError(
                        "Optional dependency 'google-cloud-bigquery' is not installed."
                    ) from exc
                client = bigquery.Client(project=project)
                for stmt in plan["statements"]:
                    job_config = bigquery.QueryJobConfig(
                        maximum_bytes_billed=self.settings.bq_max_bytes_billed
                    )
                    client.query(stmt["ddl"], job_config=job_config).result()
                    published.append(stmt["view_name"])

        return {
            "provider": selected,
            "dry_run": dry_run,
            "total_views": plan["total_views"],
            "published_views": published,
            "statements": plan["statements"],
        }

    def query_sql(self, sql: str, limit: int = 100) -> list[dict[str, Any]]:
        """Execute a strictly read-only SQL query against the local DuckDB warehouse."""
        stripped = sql.strip()
        if not stripped:
            raise ValueError("SQL query must not be empty.")
        if _BANNED_SQL_PATTERN.search(stripped):
            raise ValueError("Only read-only SELECT/WITH SQL queries are permitted.")
        if not stripped.lower().startswith(("select", "with", "show", "describe", "pragma")):
            raise ValueError("SQL query must begin with SELECT, WITH, SHOW, or DESCRIBE.")

        safe_limit = max(1, min(int(limit), 1000))
        if self.duckdb_path.is_file():
            conn = duckdb.connect(str(self.duckdb_path), read_only=True)
        else:
            conn = duckdb.connect(":memory:")
        try:
            conn.execute(f"SET memory_limit='{self.settings.duckdb_memory_limit}';")
            wrapped = f"SELECT * FROM ({stripped.rstrip(';')}) AS _q LIMIT {safe_limit}"
            cursor = conn.execute(wrapped)
            cols = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            return [dict(zip(cols, row, strict=False)) for row in rows]
        finally:
            conn.close()

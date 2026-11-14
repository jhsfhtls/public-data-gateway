"""Hybrid Medallion Lakehouse & SQLite Caching Engine (ODCS v3.1 & Point-in-Time ASOF)."""

import hashlib
import logging
import os
import re
import sqlite3
import threading
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.core.contracts import evaluate_five_pillars
from public_data_gateway.core.models import DataContract, ObservabilityReport, SnapshotManifest

logger = logging.getLogger(__name__)

_BANNED_SQL_PATTERN = re.compile(
    r"\b(DROP|DELETE|TRUNCATE|ALTER|UPDATE|INSERT|CREATE)\b",
    re.IGNORECASE,
)


class HybridStorageEngine:
    """Manages SQLite TTL cache for REST queries and DuckDB + Parquet for Medallion Lakehouse layers."""

    def __init__(self, settings: GatewaySettings | None = None) -> None:
        self.settings = settings or GatewaySettings()
        self.settings.ensure_directories()
        self._lock = threading.Lock()
        self._cache_db_path = self.settings.cache_dir / "http_cache.db"
        self._init_sqlite_cache()

    def _init_sqlite_cache(self) -> None:
        """Initialize SQLite table with WAL mode and busy timeout."""
        with sqlite3.connect(self._cache_db_path, timeout=5.0) as conn:
            conn.execute("PRAGMA journal_mode=WAL;")
            conn.execute("PRAGMA synchronous=NORMAL;")
            conn.execute("PRAGMA busy_timeout=5000;")
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS cache_entries (
                    key TEXT PRIMARY KEY,
                    value BLOB,
                    content_type TEXT,
                    status_code INTEGER,
                    created_at REAL,
                    expires_at REAL
                );
                """
            )
            conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_cache_expires ON cache_entries(expires_at);"
            )
            conn.commit()

    # -------------------------------------------------------------------------
    # SQLite TTL Cache Operations
    # -------------------------------------------------------------------------

    def get_cached(self, key: str) -> dict[str, Any] | None:
        """Fetch unexpired cached response payload."""
        now = time.time()
        with sqlite3.connect(self._cache_db_path, timeout=5.0) as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.execute(
                "SELECT value, content_type, status_code, expires_at FROM cache_entries WHERE key = ?",
                (key,),
            )
            row = cur.fetchone()
            if row and row["expires_at"] > now:
                return {
                    "value": row["value"],
                    "content_type": row["content_type"],
                    "status_code": row["status_code"],
                }
        return None

    def get_stale(self, key: str) -> dict[str, Any] | None:
        """Retrieve stale cache entry regardless of expiration (Stale-While-Error BCP fallback)."""
        with sqlite3.connect(self._cache_db_path, timeout=5.0) as conn:
            conn.row_factory = sqlite3.Row
            cur = conn.execute(
                "SELECT value, content_type, status_code, expires_at FROM cache_entries WHERE key = ?",
                (key,),
            )
            row = cur.fetchone()
            if row:
                return {
                    "value": row["value"],
                    "content_type": row["content_type"],
                    "status_code": row["status_code"],
                    "stale": True,
                }
        return None

    def set_cached(
        self,
        key: str,
        value: bytes | str,
        ttl_seconds: int = 86400,
        content_type: str = "text/plain",
        status_code: int = 200,
    ) -> None:
        """Store payload in SQLite with explicit TTL."""
        raw_val = value.encode("utf-8") if isinstance(value, str) else value
        now = time.time()
        expires = now + ttl_seconds
        with sqlite3.connect(self._cache_db_path, timeout=5.0) as conn:
            conn.execute(
                """
                INSERT OR REPLACE INTO cache_entries
                (key, value, content_type, status_code, created_at, expires_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (key, raw_val, content_type, status_code, now, expires),
            )
            conn.commit()

    def purge_expired(self) -> int:
        """Delete all expired cache records."""
        now = time.time()
        with sqlite3.connect(self._cache_db_path, timeout=5.0) as conn:
            cur = conn.execute(
                "DELETE FROM cache_entries WHERE expires_at <= ?", (now,)
            )
            deleted = cur.rowcount
            conn.commit()
            return deleted

    # -------------------------------------------------------------------------
    # Medallion Lakehouse Storage & Observability
    # -------------------------------------------------------------------------

    def write_medallion_table(
        self,
        table: pa.Table,
        source_id: str,
        layer: Literal["bronze", "silver", "gold", "quarantine"],
        table_name: str | None = None,
        contract: DataContract | None = None,
        use_source_subfolder: bool | None = None,
    ) -> tuple[Path, SnapshotManifest, ObservabilityReport | None]:
        """Atomically write a Medallion table. Automatically routes failed Silver writes to Quarantine."""
        with self._lock:
            report: ObservabilityReport | None = None
            tbl_identifier = table_name or source_id

            # If use_source_subfolder is explicitly provided, respect it; otherwise use subfolder only when table_name is set
            isolated = use_source_subfolder if use_source_subfolder is not None else (table_name is not None)

            if isolated:
                target_dir = self.settings.get_source_layer_dir(source_id, layer)
            else:
                target_dir = self.settings.get_layer_dir(layer)

            # 5-Pillar evaluation for Silver layer
            if layer == "silver" and contract is not None:
                silver_target = target_dir / f"{tbl_identifier}.parquet"
                prev_rows: int | None = None
                if silver_target.is_file():
                    try:
                        prev_rows = pq.read_metadata(silver_target).num_rows
                    except Exception:
                        prev_rows = None

                report = evaluate_five_pillars(
                    table, contract, previous_row_count=prev_rows
                )

                if not report.passed:
                    # Route to quarantine WITHOUT touching healthy silver
                    ts = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
                    if isolated:
                        q_dir = self.settings.get_source_layer_dir(source_id, "quarantine")
                    else:
                        q_dir = self.settings.quarantine_dir

                    q_path = q_dir / f"{tbl_identifier}_{ts}.parquet"
                    tmp_q_path = q_dir / f"{tbl_identifier}_{ts}.tmp.parquet"

                    pq.write_table(table, tmp_q_path, compression="ZSTD")
                    os.replace(tmp_q_path, q_path)

                    sha = self._compute_file_sha256(q_path)
                    manifest = SnapshotManifest(
                        source_id=f"{source_id}.{table_name}" if table_name else source_id,
                        layer="quarantine",
                        row_count=table.num_rows,
                        sha256=sha,
                    )
                    manifest_path = q_dir / f"{tbl_identifier}_{ts}.manifest.json"
                    with open(manifest_path, "w", encoding="utf-8") as f:
                        f.write(manifest.model_dump_json(indent=2))

                    report.quarantine_path = str(q_path)
                    return q_path, manifest, report

            # Standard atomic write to destination layer
            target_file = target_dir / f"{tbl_identifier}.parquet"
            tmp_file = target_dir / f"{tbl_identifier}.tmp.parquet"

            pq.write_table(table, tmp_file, compression="ZSTD")
            os.replace(tmp_file, target_file)

            sha = self._compute_file_sha256(target_file)
            manifest = SnapshotManifest(
                source_id=f"{source_id}.{table_name}" if table_name else source_id,
                layer=layer,
                row_count=table.num_rows,
                sha256=sha,
            )
            manifest_path = target_dir / f"{tbl_identifier}.manifest.json"
            with open(manifest_path, "w", encoding="utf-8") as f:
                f.write(manifest.model_dump_json(indent=2))

            # Maintain backward compatibility alias in flat layer when writing primary table
            import shutil
            if isolated and (not table_name or table_name == "municipios"):
                legacy_dir = self.settings.get_layer_dir(layer)
                legacy_file = legacy_dir / f"{source_id}.parquet"
                try:
                    shutil.copy2(target_file, legacy_file)
                except Exception as exc:
                    logger.debug("Failed copying legacy flat table alias: %s", exc)

            return target_file, manifest, report

    def read_table(
        self,
        source_id: str,
        layer: str = "silver",
        table_name: str | None = None,
    ) -> pa.Table:
        """Read a table from a specific medallion layer."""
        tbl_identifier = table_name or source_id
        # Check source-first path first
        source_target = (
            self.settings.data_dir / source_id / layer / f"{tbl_identifier}.parquet"
        )
        if source_target.is_file():
            return pq.read_table(source_target)

        # Fallback to legacy flat layout
        target_dir = self.settings.get_layer_dir(layer)
        target_file = target_dir / f"{tbl_identifier}.parquet"
        if not target_file.is_file():
            target_file = target_dir / f"{source_id}.parquet"

        if not target_file.is_file():
            raise FileNotFoundError(
                f"Medallion table '{tbl_identifier}' not found in layer '{layer}' (checked {source_target} and {target_file})"
            )
        return pq.read_table(target_file)

    def query_sql(self, sql: str, params: list[Any] | None = None) -> pa.Table:
        """Run analytical SQL inside DuckDB, blocking mutating DDL/DML statements."""
        if _BANNED_SQL_PATTERN.search(sql):
            raise ValueError(
                "Destructive or mutating SQL statements (DROP, DELETE, TRUNCATE, UPDATE, ALTER, INSERT, CREATE) are strictly banned."
            )

        con = duckdb.connect()
        try:
            con.execute(f"PRAGMA memory_limit='{self.settings.duckdb_memory_limit}';")
            if params:
                return con.execute(sql, params).to_arrow_table()
            return con.sql(sql).to_arrow_table()
        finally:
            con.close()

    def asof_join(
        self,
        left_table: pa.Table,
        right_table: pa.Table,
        entity_key: str,
        left_time_col: str,
        right_time_col: str = "reference_date",
    ) -> pa.Table:
        """Point-in-Time ASOF LEFT JOIN via DuckDB preventing future data leakage."""
        con = duckdb.connect()
        try:
            con.register("_left_table", left_table)
            con.register("_right_table", right_table)

            query = f"""
                SELECT
                    l.*,
                    r.* EXCLUDE ({entity_key}, {right_time_col})
                FROM _left_table AS l
                ASOF LEFT JOIN _right_table AS r
                   ON l.{entity_key} = r.{entity_key}
                  AND l.{left_time_col} >= r.{right_time_col}
                ORDER BY l.{left_time_col}
            """
            return con.sql(query).to_arrow_table()
        finally:
            con.close()

    def profile_parquet(self, parquet_path: Path) -> dict[str, Any]:
        """Generate a token-safe summary profile of a Parquet dataset."""
        if not parquet_path.is_file():
            raise FileNotFoundError(f"Parquet file not found: {parquet_path}")

        table = pq.read_table(parquet_path)
        sha256_hash = self._compute_file_sha256(parquet_path)
        size_bytes = parquet_path.stat().st_size

        null_counts = {name: table[name].null_count for name in table.column_names}
        numeric_summary: dict[str, dict[str, float | None]] = {}

        for name in table.column_names:
            field_type = table.schema.field(name).type
            if pa.types.is_floating(field_type) or pa.types.is_integer(field_type):
                col = table[name]
                if col.null_count < len(col):
                    numeric_summary[name] = {
                        "min": float(duckdb.compute(f"min({name})", col)[0]) if hasattr(duckdb, "compute") else None,
                        "max": float(duckdb.compute(f"max({name})", col)[0]) if hasattr(duckdb, "compute") else None,
                    }

        # If duckdb compute wasn't used, compute min/max via pyarrow compute
        if any(v["min"] is None for v in numeric_summary.values()):
            import pyarrow.compute as pc
            for name in numeric_summary:
                col = table[name]
                if col.null_count < len(col):
                    numeric_summary[name] = {
                        "min": float(pc.min(col).as_py()),
                        "max": float(pc.max(col).as_py()),
                    }

        return {
            "row_count": table.num_rows,
            "columns": table.column_names,
            "null_counts": null_counts,
            "numeric_summary": numeric_summary,
            "sha256": sha256_hash,
            "size_bytes": size_bytes,
        }

    @staticmethod
    def _compute_file_sha256(path: Path) -> str:
        """Compute SHA-256 hash of a file on disk."""
        hasher = hashlib.sha256()
        with open(path, "rb") as f:
            while chunk := f.read(64 * 1024):
                hasher.update(chunk)
        return hasher.hexdigest()

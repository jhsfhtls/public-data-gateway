"""Shared Medallion base class for Wave 10.5A Public APIs connectors."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.core.connector import BaseConnector
from public_data_gateway.core.contracts import DataContract, load_contract
from public_data_gateway.core.http_client import ResilientHttpClient
from public_data_gateway.core.storage import HybridStorageEngine

_SENTINEL_STRINGS = {"", "N/A", "NA", "NULL", "NONE", "NAN", "-"}
_SENTINEL_NUMBERS = {-1, -1.0, -999, -999.0, 999999}
_DEFAULT_USER_AGENT = "public-data-gateway/1.0 (+https://github.com/jhsfhtls/public-data-gateway)"


class PublicApiMedallionConnector(BaseConnector):
    """Reusable 11-Gate Medallion connector base for Public APIs Hub connectors."""

    default_table: str = ""

    def __init__(
        self,
        source_id: str,
        settings: GatewaySettings | None = None,
        storage: HybridStorageEngine | None = None,
        http_client: ResilientHttpClient | None = None,
        contract_path: Path | None = None,
    ) -> None:
        super().__init__(source_id=source_id, settings=settings)
        self.storage = storage or HybridStorageEngine(settings=self.settings)
        self.http_client = http_client or ResilientHttpClient(
            storage_engine=self.storage, settings=self.settings
        )
        self.contract_path = contract_path
        self._contract: DataContract | None = None

    @property
    def contract(self) -> DataContract:
        """Lazy-load ODCS v3.1 contract from contract.yaml."""
        if self._contract is None:
            if self.contract_path is None:
                raise ValueError(f"No contract_path configured for '{self.source_id}'.")
            self._contract = load_contract(self.contract_path)
        return self._contract

    def _layer_dir(self, layer: str) -> Path:
        path = self.settings.data_dir / self.source_id / layer
        path.mkdir(parents=True, exist_ok=True)
        return path

    @staticmethod
    def _write_sha256(target: Path) -> Path:
        digest = hashlib.sha256(target.read_bytes()).hexdigest()
        sidecar = Path(f"{target}.sha256")
        sidecar.write_text(f"{digest}  {target.name}\n", encoding="utf-8", newline="\n")
        return sidecar

    @staticmethod
    def _is_sentinel(val: Any) -> bool:
        if val is None:
            return True
        if isinstance(val, bool):
            return False
        if isinstance(val, (int, float)):
            return float(val) in _SENTINEL_NUMBERS
        return str(val).strip().upper() in _SENTINEL_STRINGS

    @staticmethod
    def _to_float_or_none(val: Any) -> float | None:
        if val is None or isinstance(val, bool):
            return None
        if isinstance(val, (int, float)):
            fval = float(val)
            return None if fval in _SENTINEL_NUMBERS else fval
        text = str(val).strip().replace(",", ".")
        if not text or text.upper() in _SENTINEL_STRINGS:
            return None
        try:
            fval = float(text)
            return None if fval in _SENTINEL_NUMBERS else fval
        except ValueError:
            return None

    def _get_json(
        self,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        cache_ttl: int = 300,
    ) -> Any:
        merged_headers = {"User-Agent": _DEFAULT_USER_AGENT, "Accept": "application/json"}
        if headers:
            merged_headers.update(headers)
        resp = self.http_client.get(
            url,
            params=params,
            headers=merged_headers,
            cache_ttl=cache_ttl,
        )
        resp.raise_for_status()
        return resp.json()

    def _sanitize_and_partition(
        self,
        records: list[dict[str, Any]],
        pk_columns: list[str],
        numeric_columns: list[str] | None = None,
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        """Validate PKs, sanitize sentinel values, deduplicate, and route rejects to quarantine."""
        deduped: dict[tuple[str, ...], dict[str, Any]] = {}
        quarantined: list[dict[str, Any]] = []
        num_cols = set(numeric_columns or [])

        for row in records:
            clean = dict(row)
            pk_tuple: list[str] = []
            missing_pk = False
            for pk in pk_columns:
                raw_pk = clean.get(pk)
                if raw_pk is None or str(raw_pk).strip().upper() in _SENTINEL_STRINGS:
                    missing_pk = True
                    break
                pk_str = str(raw_pk).strip()
                clean[pk] = pk_str
                pk_tuple.append(pk_str)

            if missing_pk:
                bad = dict(clean)
                bad["quarantine_reason"] = "missing_or_sentinel_primary_key"
                quarantined.append(bad)
                continue

            sentinel_hit = False
            for col in num_cols:
                if col in clean and clean[col] is not None:
                    parsed = self._to_float_or_none(clean[col])
                    if parsed is None:
                        sentinel_hit = True
                        break
                    clean[col] = parsed

            if sentinel_hit:
                bad = dict(clean)
                bad["quarantine_reason"] = "sentinel_numeric_value"
                quarantined.append(bad)
                continue

            deduped[tuple(pk_tuple)] = clean

        return list(deduped.values()), quarantined

    def persist_medallion(
        self,
        table_name: str,
        records: list[dict[str, Any]],
        pk_columns: list[str],
        numeric_columns: list[str] | None = None,
        gold_table_name: str | None = None,
    ) -> tuple[pa.Table, Path]:
        """Persist Bronze JSON, Quarantine Parquet, Silver Parquet ZSTD, and Gold Parquet ZSTD."""
        now_iso = datetime.now(UTC).isoformat()

        # 1. Bronze raw snapshot + SHA-256
        bronze_path = self._layer_dir("bronze") / f"{table_name}.json"
        bronze_payload = {
            "source_id": self.source_id,
            "table_name": table_name,
            "extracted_at": now_iso,
            "row_count": len(records),
            "records": records,
        }
        bronze_path.write_text(
            json.dumps(bronze_payload, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
            newline="\n",
        )
        self._write_sha256(bronze_path)

        # 2. Sanitize & Quarantine
        valid_rows, quarantined_rows = self._sanitize_and_partition(
            records, pk_columns=pk_columns, numeric_columns=numeric_columns
        )
        quarantine_dir = self._layer_dir("quarantine")
        if quarantined_rows:
            q_table = pa.Table.from_pylist(
                [{k: str(v) if v is not None else "" for k, v in r.items()} for r in quarantined_rows]
            )
            q_path = quarantine_dir / f"{table_name}_quarantine.parquet"
            pq.write_table(q_table, q_path, compression="zstd")
            self._write_sha256(q_path)

        if not valid_rows:
            raise ValueError(
                f"[{self.source_id}:{table_name}] All {len(records)} rows failed Medallion quality gate."
            )

        # 3. Silver Parquet ZSTD + SHA-256
        silver_table = pa.Table.from_pylist(valid_rows)
        silver_path = self._layer_dir("silver") / f"{table_name}.parquet"
        pq.write_table(silver_table, silver_path, compression="zstd")
        self._write_sha256(silver_path)

        # 4. Gold Parquet ZSTD + flat compatibility export
        gold_name = gold_table_name or table_name
        gold_path = self._layer_dir("gold") / f"{gold_name}.parquet"
        pq.write_table(silver_table, gold_path, compression="zstd")
        self._write_sha256(gold_path)

        flat_gold = self.settings.data_dir / "gold"
        flat_gold.mkdir(parents=True, exist_ok=True)
        pq.write_table(silver_table, flat_gold / f"{self.source_id}.parquet", compression="zstd")

        return silver_table, silver_path

    def read_silver(self, table_name: str, limit: int = 100) -> list[dict[str, Any]]:
        """Read Silver Parquet table as Python records."""
        silver_path = self._layer_dir("silver") / f"{table_name}.parquet"
        if not silver_path.exists():
            return []
        table = pq.read_table(silver_path)
        return table.slice(0, max(1, limit)).to_pylist()

    def extract_table(self, table_name: str, **kwargs: Any) -> pa.Table:
        """Extract and materialize a specific table for this connector."""
        raise NotImplementedError

    def extract(self) -> pa.Table:
        """Execute default ingestion and return standardized Silver PyArrow table."""
        return self.extract_table(self.default_table)

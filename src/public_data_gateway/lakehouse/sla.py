"""Freshness SLA & Cron Schedule Auditor for all cataloged sources and public APIs."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import yaml

from public_data_gateway.core.config import GatewaySettings


class FreshnessSlaAuditor:
    """Audits update_frequency, freshness_sla_hours, cron schedules, and local snapshot age."""

    def __init__(self, settings: GatewaySettings | None = None) -> None:
        self.settings = settings or GatewaySettings()

    def _load_yaml(self, path: Path) -> dict[str, Any]:
        if not path.is_file():
            return {}
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}

    def _find_latest_parquet_mtime(self, source_id: str) -> datetime | None:
        """Find newest Silver or Gold Parquet modification time for a source_id."""
        candidates: list[Path] = []
        for layer in ("gold", "silver"):
            layer_dir = self.settings.data_dir / layer
            if layer_dir.is_dir():
                candidates.extend(layer_dir.glob(f"{source_id}*.parquet"))
                src_sub = layer_dir / source_id
                if src_sub.is_dir():
                    candidates.extend(src_sub.rglob("*.parquet"))
            iso_dir = self.settings.data_dir / source_id / layer
            if iso_dir.is_dir():
                candidates.extend(iso_dir.rglob("*.parquet"))

        if not candidates:
            return None
        newest_ts = max(p.stat().st_mtime for p in candidates if p.is_file())
        return datetime.fromtimestamp(newest_ts, tz=UTC)

    def audit_sources(
        self,
        source_id: str | None = None,
        include_public_apis: bool = True,
        now: datetime | None = None,
    ) -> dict[str, Any]:
        """Audit SLA metadata and local freshness status across cataloged sources."""
        ref_now = now or datetime.now(UTC)
        sources_data = self._load_yaml(self.settings.catalog_path)
        entries: list[dict[str, Any]] = []

        for src in sources_data.get("sources", []):
            sid = src.get("source_id", "")
            origin = src.get("source_origin", "GOV_BR")
            if not include_public_apis and origin == "PUBLIC_GLOBAL":
                continue
            if source_id and sid != source_id:
                continue
            sla_hours = int(src.get("freshness_sla_hours", 24))
            cron_expr = str(src.get("cron_schedule_expression", "0 6 * * *"))
            latest_dt = self._find_latest_parquet_mtime(sid)
            age_hours: float | None = None
            if latest_dt is None:
                freshness_status = "NEVER_SYNCED"
            else:
                age_hours = round((ref_now - latest_dt).total_seconds() / 3600.0, 2)
                freshness_status = "HEALTHY" if age_hours <= sla_hours else "STALE"

            entries.append({
                "id": sid,
                "registry": "public_apis_registry" if origin == "PUBLIC_GLOBAL" else "sources_registry",
                "source_origin": origin,
                "name": src.get("name", sid),
                "access_mode": src.get("api_block") or src.get("access_mode", ""),
                "update_frequency": src.get("update_frequency", ""),
                "freshness_sla_hours": sla_hours,
                "cron_schedule_expression": cron_expr,
                "last_local_sync_utc": latest_dt.isoformat() if latest_dt else None,
                "age_hours": age_hours,
                "freshness_status": freshness_status,
            })

        healthy = sum(1 for e in entries if e["freshness_status"] == "HEALTHY")
        stale = sum(1 for e in entries if e["freshness_status"] == "STALE")
        never_synced = sum(1 for e in entries if e["freshness_status"] == "NEVER_SYNCED")

        return {
            "checked_at_utc": ref_now.isoformat(),
            "total_audited": len(entries),
            "summary": {
                "healthy": healthy,
                "stale": stale,
                "never_synced": never_synced,
            },
            "entries": entries,
        }

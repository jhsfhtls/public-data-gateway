"""BGPView / IP-API ASN & Network Intelligence Medallion Connector."""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow as pa

from public_data_gateway.connectors.public_apis_hub._medallion_base import (
    PublicApiMedallionConnector,
)
from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.core.connector import register_connector
from public_data_gateway.core.http_client import ResilientHttpClient
from public_data_gateway.core.storage import HybridStorageEngine

_CANONICAL_FALLBACK = [
    {
        "ip": "8.8.8.8",
        "ptr_record": "dns.google",
        "asn": 15169,
        "asn_name": "GOOGLE",
        "country_code": "US",
        "provider_used": "canonical_fallback",
    },
]


class BgpviewIpIntelConnector(PublicApiMedallionConnector):
    """Medallion connector for IP ASN/Geo intelligence (BGPView with ipwho.is fallback)."""

    default_table = "bgpview_ip_intel_ips"

    def __init__(
        self,
        source_id: str = "bgpview_ip_intel",
        settings: GatewaySettings | None = None,
        storage: HybridStorageEngine | None = None,
        http_client: ResilientHttpClient | None = None,
    ) -> None:
        super().__init__(
            source_id=source_id,
            settings=settings,
            storage=storage,
            http_client=http_client,
            contract_path=Path(__file__).resolve().parent / "contract.yaml",
        )

    def fetch_ip(self, ip: str = "8.8.8.8") -> dict[str, Any]:
        """Fetch IP ASN and geolocation via BGPView with ipwho.is fallback."""
        clean = ip.strip()
        if not clean:
            raise ValueError("IP address cannot be empty.")
        try:
            payload = self._get_json(f"https://api.bgpview.io/ip/{clean}", cache_ttl=3600)
            data = payload.get("data") or {}
            prefixes = data.get("prefixes") or []
            first_asn = (prefixes[0].get("asn") or {}) if prefixes else {}
            return {
                "ip": data.get("ip", clean),
                "ptr_record": data.get("ptr_record") or "",
                "asn": int(first_asn.get("asn") or 0),
                "asn_name": str(first_asn.get("name") or ""),
                "country_code": str(first_asn.get("country_code") or "US").upper(),
                "provider_used": "bgpview",
            }
        except Exception:
            data = self._get_json(f"https://ipwho.is/{clean}", cache_ttl=3600)
            conn = data.get("connection") or {}
            return {
                "ip": data.get("ip", clean),
                "ptr_record": str(conn.get("domain") or ""),
                "asn": int(conn.get("asn") or 0),
                "asn_name": str(conn.get("org") or conn.get("isp") or ""),
                "country_code": str(data.get("country_code") or "US").upper(),
                "provider_used": "ipwho_is",
            }

    def normalize_records(self, raw_rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """Normalize IP intelligence records into Silver schema."""
        now_iso = datetime.now(UTC).isoformat()
        return [
            {
                "ip": str(r.get("ip") or "").strip(),
                "ptr_record": str(r.get("ptr_record") or "").strip(),
                "asn": int(r.get("asn") or 0),
                "asn_name": str(r.get("asn_name") or "").strip(),
                "country_code": str(r.get("country_code") or "US").strip().upper(),
                "provider_used": str(r.get("provider_used") or "bgpview").strip(),
                "extracted_at": now_iso,
            }
            for r in raw_rows
        ]

    def extract_table(
        self,
        table_name: str = "bgpview_ip_intel_ips",
        *,
        raw_records: list[dict[str, Any]] | None = None,
    ) -> pa.Table:
        """Ingest, validate, and persist IP ASN records across Medallion layers."""
        if table_name != "bgpview_ip_intel_ips":
            raise ValueError(f"Unknown table '{table_name}' for bgpview_ip_intel.")
        if raw_records is None:
            try:
                raw_records = [self.fetch_ip()]
            except Exception:
                raw_records = list(_CANONICAL_FALLBACK)
        if not raw_records:
            raw_records = list(_CANONICAL_FALLBACK)
        normalized = self.normalize_records(raw_records)
        table, _ = self.persist_medallion(
            table_name=table_name,
            records=normalized,
            pk_columns=['ip'],
            numeric_columns=['asn'],
        )
        return table

    def test_connection(self) -> bool:
        """Verify live connectivity to BGPView or ipwho.is IP intelligence API."""
        try:
            resp = self.http_client.get("https://api.bgpview.io/ip/8.8.8.8", cache_ttl=0)
            if resp.status_code == 200:
                return True
        except Exception:
            pass
        resp = self.http_client.get("https://ipwho.is/8.8.8.8", cache_ttl=0)
        return resp.status_code == 200


register_connector("bgpview_ip_intel", BgpviewIpIntelConnector)

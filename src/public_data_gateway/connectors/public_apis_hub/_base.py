"""Shared resilient HTTP base for the Phase 10 Public APIs Hub."""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Any

import httpx
import yaml

from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.core.http_client import ResilientHttpClient

logger = logging.getLogger(__name__)

_REPO_ROOT = Path(__file__).resolve().parents[4]
_DEFAULT_REGISTRY_PATH = _REPO_ROOT / "catalog" / "sources_registry.yaml"
_DEFAULT_USER_AGENT = "public-data-gateway/1.0 (+https://github.com/jhsfhtls/public-data-gateway)"


class PublicApiHubBase:
    """Base helper providing resilient GET/POST JSON calls and catalog lookup."""

    def __init__(
        self,
        settings: GatewaySettings | None = None,
        http_client: ResilientHttpClient | None = None,
        registry_path: Path | None = None,
    ) -> None:
        self.settings = settings or GatewaySettings()
        self.http_client = http_client or ResilientHttpClient(settings=self.settings)
        self.registry_path = registry_path or _DEFAULT_REGISTRY_PATH

    @staticmethod
    def _digits_only(value: str) -> str:
        return re.sub(r"\D", "", str(value))

    def _get_json(
        self,
        url: str,
        *,
        params: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
        cache_ttl: int = 3600,
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

    def _post_json(
        self,
        url: str,
        *,
        data: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
        headers: dict[str, str] | None = None,
    ) -> Any:
        merged_headers = {"User-Agent": _DEFAULT_USER_AGENT, "Accept": "application/json"}
        if headers:
            merged_headers.update(headers)
        self.http_client.rate_limiter.acquire(tokens=1.0)
        resp = self.http_client.client.post(
            url,
            data=data,
            json=json_body,
            headers=merged_headers,
        )
        resp.raise_for_status()
        return resp.json()

    def load_catalog(
        self,
        *,
        block: str | None = None,
        pack_id: str | None = None,
    ) -> dict[str, Any]:
        """Load curated Public APIs catalog from unified sources_registry.yaml."""
        if not self.registry_path.is_file():
            raise FileNotFoundError(f"Public APIs registry not found at '{self.registry_path}'.")
        payload = yaml.safe_load(self.registry_path.read_text(encoding="utf-8")) or {}
        if "apis" in payload:
            apis = list(payload.get("apis", []))
        else:
            apis = [
                {
                    "api_id": s["source_id"],
                    "name": s["name"],
                    "block": s.get("api_block", ""),
                    "base_url": s.get("official_url", ""),
                    "auth_type": s.get("auth_type", "NONE"),
                    "rate_limit_rpm": s.get("rate_limit_rpm", 60),
                    "finops_tier": s.get("finops_tier", "FREE_UNLIMITED"),
                    "target_packs": s.get("solution_packs", []),
                    "gateway_method": s.get("gateway_method", ""),
                    "status": s.get("status", "HOMOLOGATED"),
                    "notes": s.get("notes", ""),
                    "medallion_parity": True,
                    "connector_id": s["source_id"],
                    "update_frequency": s.get("update_frequency", ""),
                    "freshness_sla_hours": s.get("freshness_sla_hours", 24),
                    "cron_schedule_expression": s.get("cron_schedule_expression", "0 6 * * *"),
                }
                for s in payload.get("sources", [])
                if s.get("source_origin") == "PUBLIC_GLOBAL"
            ]
        if block:
            clean_block = block.strip().upper()
            apis = [a for a in apis if str(a.get("block", "")).upper() == clean_block]
        if pack_id:
            clean_pack = pack_id.strip().upper()
            apis = [a for a in apis if clean_pack in a.get("target_packs", [])]
        return {
            "total": len(apis),
            "blocks": payload.get("public_api_blocks", payload.get("blocks", [])),
            "already_homologated_gov_br": payload.get("already_homologated_gov_br", []),
            "apis": apis,
        }

    @staticmethod
    def _first_success(
        providers: list[tuple[str, Any]],
        operation: str,
    ) -> tuple[str, Any]:
        errors: list[str] = []
        for provider_name, fn in providers:
            try:
                result = fn()
                if result is not None:
                    return provider_name, result
            except (httpx.HTTPError, ValueError, KeyError, RuntimeError) as exc:
                logger.warning("%s fallback '%s' failed: %s", operation, provider_name, exc)
                errors.append(f"{provider_name}: {exc}")
        raise RuntimeError(f"All cascade providers failed for {operation}: {'; '.join(errors)}")

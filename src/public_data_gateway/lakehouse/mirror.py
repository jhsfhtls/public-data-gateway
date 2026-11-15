"""Cloud Lakehouse Mirror Engine for S3, Cloudflare R2, and Local Staging Backends."""

from __future__ import annotations

import hashlib
import hmac
import json
import os
import shutil
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import quote, urlparse

import httpx

from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.lakehouse.cdc import CdcDeltaTracker


def _sign_sigv4_headers(
    method: str,
    url: str,
    payload_sha256: str,
    access_key: str,
    secret_key: str,
    region: str,
    service: str = "s3",
    content_type: str = "application/octet-stream",
    now: datetime | None = None,
) -> dict[str, str]:
    """Generate AWS Signature Version 4 headers using pure Python stdlib."""
    dt = now or datetime.now(UTC)
    amz_date = dt.strftime("%Y%m%dT%H%M%SZ")
    date_stamp = dt.strftime("%Y%m%d")

    parsed = urlparse(url)
    host = parsed.netloc
    canonical_uri = quote(parsed.path or "/", safe="/-_.~")

    canonical_headers = (
        f"content-type:{content_type}\n"
        f"host:{host}\n"
        f"x-amz-content-sha256:{payload_sha256}\n"
        f"x-amz-date:{amz_date}\n"
    )
    signed_headers = "content-type;host;x-amz-content-sha256;x-amz-date"
    canonical_request = "\n".join([
        method.upper(),
        canonical_uri,
        "",
        canonical_headers,
        signed_headers,
        payload_sha256,
    ])

    algorithm = "AWS4-HMAC-SHA256"
    credential_scope = f"{date_stamp}/{region}/{service}/aws4_request"
    string_to_sign = "\n".join([
        algorithm,
        amz_date,
        credential_scope,
        hashlib.sha256(canonical_request.encode("utf-8")).hexdigest(),
    ])

    def _hmac(key: bytes, msg: str) -> bytes:
        return hmac.new(key, msg.encode("utf-8"), hashlib.sha256).digest()

    k_date = _hmac(f"AWS4{secret_key}".encode(), date_stamp)
    k_region = _hmac(k_date, region)
    k_service = _hmac(k_region, service)
    k_signing = _hmac(k_service, "aws4_request")
    signature = hmac.new(k_signing, string_to_sign.encode("utf-8"), hashlib.sha256).hexdigest()

    authorization = (
        f"{algorithm} Credential={access_key}/{credential_scope}, "
        f"SignedHeaders={signed_headers}, Signature={signature}"
    )
    return {
        "Content-Type": content_type,
        "x-amz-content-sha256": payload_sha256,
        "x-amz-date": amz_date,
        "Authorization": authorization,
    }


class CloudLakehouseMirror:
    """Mirrors Silver/Gold Parquet ZSTD snapshots and manifests to S3, R2, or local mirror."""

    VALID_PROVIDERS = {"local", "s3", "r2"}

    def __init__(
        self,
        settings: GatewaySettings | None = None,
        cdc_tracker: CdcDeltaTracker | None = None,
        http_client: httpx.Client | None = None,
    ) -> None:
        self.settings = settings or GatewaySettings()
        self.cdc = cdc_tracker or CdcDeltaTracker(self.settings)
        self._http_client = http_client

    def _resolve_object_url(self, provider: str, object_key: str) -> str:
        bucket = self.settings.mirror_bucket.strip()
        if not bucket:
            raise ValueError(
                f"BDG_MIRROR_BUCKET must be configured when mirror_provider='{provider}'."
            )
        endpoint = self.settings.mirror_endpoint_url.strip().rstrip("/")
        if provider == "r2":
            if not endpoint:
                raise ValueError(
                    "BDG_MIRROR_ENDPOINT_URL must be configured for Cloudflare R2 mirroring."
                )
            return f"{endpoint}/{bucket}/{object_key}"
        if endpoint:
            return f"{endpoint}/{bucket}/{object_key}"
        region = self.settings.mirror_region or "us-east-1"
        return f"https://{bucket}.s3.{region}.amazonaws.com/{object_key}"

    def _resolve_target_uri(self, provider: str, object_key: str) -> str:
        if provider == "local":
            base = self.settings.mirror_local_dir or (self.settings.data_dir / "mirror")
            return str((base / object_key).resolve())
        bucket = self.settings.mirror_bucket.strip() or "<unconfigured-bucket>"
        scheme = "r2" if provider == "r2" else "s3"
        return f"{scheme}://{bucket}/{object_key}"

    def _upload_s3_compatible(
        self,
        provider: str,
        object_key: str,
        data: bytes,
        content_type: str = "application/octet-stream",
    ) -> str:
        access_key = os.environ.get("BDG_MIRROR_ACCESS_KEY_ID", "").strip()
        secret_key = os.environ.get("BDG_MIRROR_SECRET_ACCESS_KEY", "").strip()
        if not access_key or not secret_key:
            raise ValueError(
                "BDG_MIRROR_ACCESS_KEY_ID and BDG_MIRROR_SECRET_ACCESS_KEY must be set in environment."
            )
        url = self._resolve_object_url(provider, object_key)
        region = self.settings.mirror_region or ("auto" if provider == "r2" else "us-east-1")
        payload_sha256 = hashlib.sha256(data).hexdigest()
        headers = _sign_sigv4_headers(
            method="PUT",
            url=url,
            payload_sha256=payload_sha256,
            access_key=access_key,
            secret_key=secret_key,
            region=region,
            content_type=content_type,
        )

        if self._http_client is not None:
            resp = self._http_client.put(url, content=data, headers=headers)
        else:
            with httpx.Client(timeout=60.0) as client:
                resp = client.put(url, content=data, headers=headers)

        if resp.status_code not in (200, 201, 204):
            raise RuntimeError(
                f"Mirror upload failed for '{object_key}' (HTTP {resp.status_code}): {resp.text[:300]}"
            )
        return self._resolve_target_uri(provider, object_key)

    def plan(
        self,
        layers: tuple[str, ...] = ("silver", "gold"),
        sources: list[str] | None = None,
        provider: str | None = None,
    ) -> dict[str, Any]:
        """Compute an incremental CDC mirror plan without transferring bytes."""
        selected_provider = (provider or self.settings.mirror_provider or "local").lower()
        if selected_provider not in self.VALID_PROVIDERS:
            raise ValueError(
                f"Unsupported mirror_provider '{selected_provider}'. Expected one of {sorted(self.VALID_PROVIDERS)}."
            )
        delta = self.cdc.build_delta_plan(layers=layers, sources=sources)
        prefix = self.settings.mirror_prefix.strip("/")
        for item in delta["to_upload"]:
            obj_key = f"{prefix}/{item['relative_key']}" if prefix else item["relative_key"]
            item["object_key"] = obj_key
            item["target_uri"] = self._resolve_target_uri(selected_provider, obj_key)
        delta["provider"] = selected_provider
        delta["prefix"] = prefix
        return delta

    def sync(
        self,
        layers: tuple[str, ...] = ("silver", "gold"),
        sources: list[str] | None = None,
        provider: str | None = None,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        """Execute incremental Silver/Gold Parquet mirror sync and publish manifest."""
        plan = self.plan(layers=layers, sources=sources, provider=provider)
        selected_provider = plan["provider"]
        prefix = plan["prefix"]

        uploaded: list[dict[str, Any]] = []
        if not dry_run:
            for item in plan["to_upload"]:
                src_path = Path(item["local_path"])
                obj_key = item["object_key"]
                if selected_provider == "local":
                    base = self.settings.mirror_local_dir or (self.settings.data_dir / "mirror")
                    dest_path = base / obj_key
                    dest_path.parent.mkdir(parents=True, exist_ok=True)
                    shutil.copy2(src_path, dest_path)
                    target_uri = str(dest_path.resolve())
                else:
                    data = src_path.read_bytes()
                    target_uri = self._upload_s3_compatible(
                        provider=selected_provider,
                        object_key=obj_key,
                        data=data,
                        content_type="application/vnd.apache.parquet",
                    )
                uploaded.append({**item, "target_uri": target_uri})

            manifest_key = f"{prefix}/manifest.json" if prefix else "manifest.json"
            manifest_payload = {
                "schema_version": "1.0.0",
                "generated_at_utc": datetime.now(UTC).isoformat(),
                "provider": selected_provider,
                "uploaded_count": len(uploaded),
                "unchanged_count": plan["unchanged_count"],
                "artifacts": [
                    {
                        "relative_key": u["relative_key"],
                        "target_uri": u["target_uri"],
                        "sha256": u["sha256"],
                        "size_bytes": u["size_bytes"],
                        "row_count": u["row_count"],
                    }
                    for u in uploaded
                ],
            }
            manifest_bytes = (
                json.dumps(manifest_payload, indent=2, ensure_ascii=False) + "\n"
            ).encode("utf-8")

            if selected_provider == "local":
                base = self.settings.mirror_local_dir or (self.settings.data_dir / "mirror")
                manifest_path = base / manifest_key
                manifest_path.parent.mkdir(parents=True, exist_ok=True)
                manifest_path.write_bytes(manifest_bytes)
                manifest_uri = str(manifest_path.resolve())
            else:
                manifest_uri = self._upload_s3_compatible(
                    provider=selected_provider,
                    object_key=manifest_key,
                    data=manifest_bytes,
                    content_type="application/json",
                )

            if uploaded:
                self.cdc.commit_synced_entries(uploaded)
        else:
            manifest_key = f"{prefix}/manifest.json" if prefix else "manifest.json"
            manifest_uri = self._resolve_target_uri(selected_provider, manifest_key)

        return {
            "provider": selected_provider,
            "dry_run": dry_run,
            "total_discovered": plan["total_discovered"],
            "uploaded_count": len(uploaded) if not dry_run else 0,
            "planned_upload_count": plan["to_upload_count"],
            "skipped_unchanged": plan["unchanged_count"],
            "bytes_transferred": sum(u["size_bytes"] for u in uploaded) if not dry_run else 0,
            "bytes_saved_by_cdc": plan["bytes_saved_by_cdc"],
            "manifest_uri": manifest_uri,
            "uploaded_files": uploaded,
        }

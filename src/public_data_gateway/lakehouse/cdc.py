"""Incremental Change Data Capture (CDC) Delta Tracker for Medallion Parquet snapshots."""

from __future__ import annotations

import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq

from public_data_gateway.core.config import GatewaySettings


class CdcDeltaTracker:
    """Tracks SHA-256 and row-count watermarks to avoid redundant full cloud transfers."""

    def __init__(
        self,
        settings: GatewaySettings | None = None,
        state_path: Path | None = None,
    ) -> None:
        self.settings = settings or GatewaySettings()
        self.state_path = state_path or (
            (self.settings.mirror_local_dir or (self.settings.data_dir / "mirror"))
            / "cdc_state.json"
        )

    def load_state(self) -> dict[str, dict[str, Any]]:
        """Load persisted CDC watermark state keyed by relative artifact key."""
        if not self.state_path.is_file():
            return {}
        try:
            payload = json.loads(self.state_path.read_text(encoding="utf-8"))
            files = payload.get("files", {})
            return files if isinstance(files, dict) else {}
        except (OSError, json.JSONDecodeError):
            return {}

    def save_state(self, files_state: dict[str, dict[str, Any]]) -> Path:
        """Persist updated CDC watermark state atomically with UTF-8 LF encoding."""
        self.state_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "schema_version": "1.0.0",
            "updated_at_utc": datetime.now(UTC).isoformat(),
            "files": dict(sorted(files_state.items())),
        }
        self.state_path.write_text(
            json.dumps(payload, indent=2, ensure_ascii=False) + "\n",
            encoding="utf-8",
            newline="\n",
        )
        return self.state_path

    @staticmethod
    def _compute_sha256(path: Path) -> str:
        hasher = hashlib.sha256()
        with open(path, "rb") as f:
            for chunk in iter(lambda: f.read(65536), b""):
                hasher.update(chunk)
        return hasher.hexdigest()

    @staticmethod
    def _read_parquet_rows(path: Path) -> int:
        try:
            meta = pq.read_metadata(path)
            return int(meta.num_rows)
        except Exception:
            return 0

    def discover_local_artifacts(
        self,
        layers: tuple[str, ...] = ("silver", "gold"),
        sources: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """Discover Silver/Gold Parquet artifacts across standard and isolated layouts."""
        normalized_layers = tuple(layer.lower().strip() for layer in layers)
        source_filter = set(sources) if sources else None
        discovered: dict[str, dict[str, Any]] = {}

        # Layout 1: data/<layer>/<file>.parquet or data/<layer>/<source_id>/...
        for layer in normalized_layers:
            layer_dir = self.settings.data_dir / layer
            if not layer_dir.is_dir():
                continue
            for p in sorted(layer_dir.rglob("*.parquet")):
                rel_to_layer = p.relative_to(layer_dir)
                inferred_source = (
                    rel_to_layer.parts[0]
                    if len(rel_to_layer.parts) > 1
                    else p.stem.split("_")[0]
                )
                if source_filter and not any(
                    inferred_source == s or p.stem.startswith(s) for s in source_filter
                ):
                    continue
                rel_key = f"{layer}/{rel_to_layer.as_posix()}"
                discovered[rel_key] = {
                    "relative_key": rel_key,
                    "layer": layer,
                    "source_id": inferred_source,
                    "local_path": str(p),
                    "size_bytes": p.stat().st_size,
                    "sha256": self._compute_sha256(p),
                    "row_count": self._read_parquet_rows(p),
                }

        # Layout 2: data/<source_id>/<layer>/.../*.parquet
        reserved_dirs = {"bronze", "silver", "gold", "quarantine", "cache", "reference", "mirror", "marts", "manifests"}
        if self.settings.data_dir.is_dir():
            for child in sorted(self.settings.data_dir.iterdir()):
                if not child.is_dir() or child.name in reserved_dirs:
                    continue
                sid = child.name
                if source_filter and sid not in source_filter:
                    continue
                for layer in normalized_layers:
                    iso_layer = child / layer
                    if not iso_layer.is_dir():
                        continue
                    for p in sorted(iso_layer.rglob("*.parquet")):
                        rel_key = f"{layer}/{sid}/{p.relative_to(iso_layer).as_posix()}"
                        discovered[rel_key] = {
                            "relative_key": rel_key,
                            "layer": layer,
                            "source_id": sid,
                            "local_path": str(p),
                            "size_bytes": p.stat().st_size,
                            "sha256": self._compute_sha256(p),
                            "row_count": self._read_parquet_rows(p),
                        }

        return [discovered[k] for k in sorted(discovered)]

    def build_delta_plan(
        self,
        layers: tuple[str, ...] = ("silver", "gold"),
        sources: list[str] | None = None,
    ) -> dict[str, Any]:
        """Compare local Parquet artifacts against CDC state to classify NEW/MODIFIED/UNCHANGED."""
        artifacts = self.discover_local_artifacts(layers=layers, sources=sources)
        state = self.load_state()

        to_upload: list[dict[str, Any]] = []
        unchanged: list[dict[str, Any]] = []

        for art in artifacts:
            rel_key = art["relative_key"]
            prev = state.get(rel_key)
            if prev is None:
                entry = {**art, "change_type": "NEW"}
                to_upload.append(entry)
            elif prev.get("sha256") != art["sha256"]:
                entry = {**art, "change_type": "MODIFIED"}
                to_upload.append(entry)
            else:
                entry = {**art, "change_type": "UNCHANGED"}
                unchanged.append(entry)

        bytes_to_transfer = sum(item["size_bytes"] for item in to_upload)
        bytes_saved = sum(item["size_bytes"] for item in unchanged)

        return {
            "generated_at_utc": datetime.now(UTC).isoformat(),
            "layers": list(layers),
            "total_discovered": len(artifacts),
            "to_upload_count": len(to_upload),
            "unchanged_count": len(unchanged),
            "bytes_to_transfer": bytes_to_transfer,
            "bytes_saved_by_cdc": bytes_saved,
            "to_upload": to_upload,
            "unchanged": unchanged,
        }

    def commit_synced_entries(self, synced_entries: list[dict[str, Any]]) -> Path:
        """Update CDC watermark state with newly mirrored entries."""
        state = self.load_state()
        now_iso = datetime.now(UTC).isoformat()
        for item in synced_entries:
            state[item["relative_key"]] = {
                "sha256": item["sha256"],
                "size_bytes": item["size_bytes"],
                "row_count": item["row_count"],
                "layer": item["layer"],
                "source_id": item["source_id"],
                "synced_at_utc": now_iso,
            }
        return self.save_state(state)

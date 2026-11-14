"""Hugging Face Inference API Medallion Connector."""

from __future__ import annotations

import hashlib
import os
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

_DEFAULT_MODEL = "cardiffnlp/twitter-roberta-base-sentiment-latest"
_CANONICAL_FALLBACK = [
    {"model_id": _DEFAULT_MODEL, "input_hash": "canonical_pos_01", "task_type": "text-classification", "top_label": "positive", "top_score": 0.94},
    {"model_id": _DEFAULT_MODEL, "input_hash": "canonical_neu_02", "task_type": "text-classification", "top_label": "neutral", "top_score": 0.81},
]


class HuggingFaceInferenceConnector(PublicApiMedallionConnector):
    """Medallion connector for Hugging Face HTTP Inference API (NLP/embeddings)."""

    default_table = "hf_inference_runs"

    def __init__(
        self,
        source_id: str = "huggingface_inference",
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

    def run_inference(self, model: str = _DEFAULT_MODEL, inputs: str = "Brazil GDP growth outlook") -> Any:
        """Run Hugging Face HTTP inference (requires HF_TOKEN env var)."""
        token = os.environ.get("HF_TOKEN")
        if not token:
            raise ValueError("HF_TOKEN environment variable is required for Hugging Face Inference.")
        return self._post_json(
            f"https://router.huggingface.co/hf-inference/models/{model.strip()}",
            json_body={"inputs": inputs},
            headers={"Authorization": f"Bearer {token}"},
        )

    def normalize_records(
        self,
        raw_payload: Any,
        *,
        model_id: str = _DEFAULT_MODEL,
        inputs: str = "canonical",
    ) -> list[dict[str, Any]]:
        """Normalize Hugging Face classification or record list into Silver schema."""
        now_iso = datetime.now(UTC).isoformat()
        if isinstance(raw_payload, list) and raw_payload and isinstance(raw_payload[0], dict) and "model_id" in raw_payload[0]:
            return [
                {
                    "model_id": str(r.get("model_id") or model_id).strip(),
                    "input_hash": str(r.get("input_hash") or "").strip(),
                    "task_type": str(r.get("task_type") or "text-classification").strip(),
                    "top_label": str(r.get("top_label") or "unknown").strip(),
                    "top_score": r.get("top_score", 0.0),
                    "extracted_at": now_iso,
                }
                for r in raw_payload
            ]
        input_hash = hashlib.sha256(inputs.encode("utf-8")).hexdigest()[:16]
        candidates = raw_payload[0] if isinstance(raw_payload, list) and raw_payload and isinstance(raw_payload[0], list) else raw_payload
        top_item = candidates[0] if isinstance(candidates, list) and candidates and isinstance(candidates[0], dict) else {}
        return [
            {
                "model_id": model_id,
                "input_hash": input_hash,
                "task_type": "text-classification",
                "top_label": str(top_item.get("label") or "unknown"),
                "top_score": float(top_item.get("score") or 0.0),
                "extracted_at": now_iso,
            }
        ]

    def extract_table(
        self,
        table_name: str = "hf_inference_runs",
        *,
        raw_records: list[dict[str, Any]] | None = None,
    ) -> pa.Table:
        """Ingest, validate, and persist Hugging Face inference signals across Medallion layers."""
        if table_name != "hf_inference_runs":
            raise ValueError(f"Unknown table '{table_name}' for huggingface_inference.")
        if raw_records is None:
            normalized = self.normalize_records(list(_CANONICAL_FALLBACK))
        else:
            normalized = self.normalize_records(raw_records)
        table, _ = self.persist_medallion(
            table_name="hf_inference_runs",
            records=normalized,
            pk_columns=["model_id", "input_hash"],
            numeric_columns=["top_score"],
        )
        return table

    def test_connection(self) -> bool:
        """Verify live reachability of Hugging Face Hub API."""
        resp = self.http_client.get(
            f"https://huggingface.co/api/models/{_DEFAULT_MODEL}",
            cache_ttl=0,
        )
        return resp.status_code == 200


register_connector("huggingface_inference", HuggingFaceInferenceConnector)

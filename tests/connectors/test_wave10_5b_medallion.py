"""Offline Medallion & 4-interface parity tests for Wave 10.5B Block 3 connectors."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

from public_data_gateway.api.app import create_app
from public_data_gateway.cli.main import main as cli_main
from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.core.connector import get_connector, list_registered_connectors
from public_data_gateway.mcp.server import MCPDispatcher
from public_data_gateway.sdk import BrazilData

WAVE10_5B_IDS = [
    "overpass_osm",
    "osrm_routing",
    "open_meteo",
    "nasa_power",
    "open_elevation",
    "huggingface_inference",
    "alpha_vantage",
    "nasa_youtube_open_ml",
    "upgini_adapter",
]


def test_wave10_5b_connectors_registered() -> None:
    """All 9 Wave 10.5B connectors must be registered in the global plugin registry."""
    registered = set(list_registered_connectors())
    for cid in WAVE10_5B_IDS:
        assert cid in registered, f"Missing connector registration: {cid}"


@pytest.mark.parametrize("source_id", WAVE10_5B_IDS)
def test_wave10_5b_medallion_pipeline_isolated(tmp_path: Path, source_id: str) -> None:
    """Each Wave 10.5B connector must load ODCS contract and materialize Bronze, Silver, Gold & SHA-256."""
    settings = GatewaySettings(data_dir=tmp_path)
    conn = get_connector(source_id, settings=settings)

    assert conn.contract.source_id == source_id
    assert len(conn.contract.primary_keys) >= 1
    assert conn.contract.lgpd_legal_basis

    with patch.object(conn, "_get_json", side_effect=RuntimeError("offline_test")), patch.object(
        conn, "_post_json", side_effect=RuntimeError("offline_test")
    ):
        table = conn.extract_table(conn.default_table)
    assert table.num_rows >= 1

    bronze_files = list((tmp_path / source_id / "bronze").glob("*.json"))
    silver_files = list((tmp_path / source_id / "silver").glob("*.parquet"))
    gold_files = list((tmp_path / source_id / "gold").glob("*.parquet"))
    sha_files = list((tmp_path / source_id / "silver").glob("*.sha256"))

    assert len(bronze_files) == 1
    assert len(silver_files) == 1
    assert len(gold_files) == 1
    assert len(sha_files) == 1
    assert pq.read_table(silver_files[0]).num_rows == table.num_rows


def test_wave10_5b_four_interface_parity(tmp_path: Path) -> None:
    """Verify SDK, FastAPI, CLI, and MCP parity for Wave 10.5B connectors."""
    settings = GatewaySettings(data_dir=tmp_path)
    sdk = BrazilData(settings=settings)

    with patch(
        "public_data_gateway.connectors.ambiental.open_meteo.connector.OpenMeteoConnector.fetch_forecast",
        return_value={
            "daily": {
                "time": ["2026-11-14"],
                "temperature_2m_max": [27.4],
                "temperature_2m_min": [17.8],
                "precipitation_sum": [4.2],
            }
        },
    ):
        sdk_rows = sdk.public_apis.fetch_open_meteo_forecast()
        assert len(sdk_rows) == 1
        assert sdk_rows[0]["temp_max_c"] == 27.4

        app = create_app(settings=settings)
        client = TestClient(app)
        resp = client.get("/api/v1/public-apis/climate/open-meteo")
        assert resp.status_code == 200
        assert resp.json()["count"] == 1

        rc = cli_main(["public-apis", "open-meteo"])
        assert rc == 0

        mcp = MCPDispatcher(settings=settings)
        tool_resp = mcp.handle_request(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {"name": "bdg_fetch_open_meteo_forecast", "arguments": {}},
            }
        )
        text = tool_resp["result"]["content"][0]["text"]
        assert "<untrusted_gov_data" in text
        payload = json.loads(text.split("\n", 1)[1].rsplit("\n", 1)[0])
        assert payload[0]["temp_max_c"] == 27.4

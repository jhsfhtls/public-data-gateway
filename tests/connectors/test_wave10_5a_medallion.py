"""Offline Medallion & 4-interface parity tests for Wave 10.5A Block 2 connectors."""

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

WAVE10_5A_IDS = [
    "ipeadata_odata",
    "brapi_b3",
    "awesomeapi_fx",
    "aneel_anp_ckan",
    "camara_senado_dados_abertos",
    "world_bank_v2",
    "who_gho_odata",
]


def test_wave10_5a_connectors_registered() -> None:
    """All 7 Wave 10.5A connectors must be registered in the global plugin registry."""
    registered = set(list_registered_connectors())
    for cid in WAVE10_5A_IDS:
        assert cid in registered, f"Missing connector registration: {cid}"


@pytest.mark.parametrize("source_id", WAVE10_5A_IDS)
def test_wave10_5a_medallion_pipeline_isolated(tmp_path: Path, source_id: str) -> None:
    """Each connector must load ODCS contract and materialize Bronze, Silver, Gold & SHA-256."""
    settings = GatewaySettings(data_dir=tmp_path)
    conn = get_connector(source_id, settings=settings)

    assert conn.contract.source_id == source_id
    assert len(conn.contract.primary_keys) >= 1
    assert conn.contract.lgpd_legal_basis

    with patch.object(conn, "_get_json", side_effect=RuntimeError("offline_test")):
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

    # Verify ZSTD readable Parquet
    pq_table = pq.read_table(silver_files[0])
    assert pq_table.num_rows == table.num_rows


def test_wave10_5a_quarantine_sentinel_and_missing_pk(tmp_path: Path) -> None:
    """Sentinel values and missing PKs must be routed to quarantine/ instead of Silver."""
    settings = GatewaySettings(data_dir=tmp_path)
    conn = get_connector("ipeadata_odata", settings=settings)
    table = conn.extract_table(
        raw_records=[
            {"SERCODIGO": "BM12_TJOVER12", "VALDATA": "2026-10-01", "VALVALOR": 11.25},
            {"SERCODIGO": "BM12_TJOVER12", "VALDATA": "2026-09-01", "VALVALOR": -999},
            {"SERCODIGO": "", "VALDATA": "2026-08-01", "VALVALOR": 10.5},
        ]
    )
    assert table.num_rows == 1
    q_path = tmp_path / "ipeadata_odata" / "quarantine" / "ipeadata_series_quarantine.parquet"
    assert q_path.exists()
    q_table = pq.read_table(q_path)
    assert q_table.num_rows == 2


def test_wave10_5a_four_interface_parity(tmp_path: Path) -> None:
    """Verify SDK, FastAPI, CLI, and MCP parity for Wave 10.5A connectors (mocked HTTP)."""
    settings = GatewaySettings(data_dir=tmp_path)
    sdk = BrazilData(settings=settings)

    with patch(
        "public_data_gateway.connectors.macrofinanceiro.ipeadata_odata.connector.IpeadataOdataConnector.fetch_series",
        return_value=[{"SERCODIGO": "BM12_TJOVER12", "VALDATA": "2026-10-01", "VALVALOR": 11.25}],
    ):
        sdk_rows = sdk.public_apis.fetch_ipeadata_series("BM12_TJOVER12", limit=1)
        assert len(sdk_rows) == 1
        assert sdk_rows[0]["sercodigo"] == "BM12_TJOVER12"

        app = create_app(settings=settings)
        client = TestClient(app)
        resp = client.get("/api/v1/public-apis/ipeadata/BM12_TJOVER12?limit=1")
        assert resp.status_code == 200
        assert resp.json()["count"] == 1

        rc = cli_main(["public-apis", "ipeadata", "BM12_TJOVER12", "--limit", "1"])
        assert rc == 0

        mcp = MCPDispatcher(settings=settings)
        tool_resp = mcp.handle_request(
            {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "tools/call",
                "params": {"name": "bdg_fetch_ipeadata_series", "arguments": {"sercodigo": "BM12_TJOVER12", "limit": 1}},
            }
        )
        text = tool_resp["result"]["content"][0]["text"]
        assert "<untrusted_gov_data" in text
        payload = json.loads(text.split("\n", 1)[1].rsplit("\n", 1)[0])
        assert payload[0]["sercodigo"] == "BM12_TJOVER12"

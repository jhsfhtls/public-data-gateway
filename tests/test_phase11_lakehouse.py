"""Offline unit & 4-interface parity tests for Phase 11 Cloud Lakehouse Mirror."""

from __future__ import annotations

import json
from pathlib import Path

import httpx
import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

from public_data_gateway.api.app import create_app
from public_data_gateway.cli.main import main as cli_main
from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.lakehouse import (
    CdcDeltaTracker,
    CloudLakehouseMirror,
    FreshnessSlaAuditor,
    PublicWarehouseAdapter,
)
from public_data_gateway.mcp.server import MCPDispatcher
from public_data_gateway.sdk import BrazilData


def _seed_sample_parquet(data_dir: Path, source_id: str = "bcb_sgs", layer: str = "gold") -> Path:
    target_dir = data_dir / source_id / layer
    target_dir.mkdir(parents=True, exist_ok=True)
    pq_path = target_dir / f"{source_id}_sample.parquet"
    table = pa.table({"id_municipio": ["3550308", "3304557"], "valor": [10.5, 20.25]})
    pq.write_table(table, pq_path, compression="zstd")
    return pq_path


def test_freshness_sla_audit_covers_all_49_sources_and_29_apis(tmp_path: Path) -> None:
    settings = GatewaySettings(data_dir=tmp_path / "data")
    _seed_sample_parquet(settings.data_dir, source_id="bcb_sgs", layer="gold")

    auditor = FreshnessSlaAuditor(settings=settings)
    report = auditor.audit_sources(include_public_apis=True)

    assert report["total_audited"] >= 78  # 49+ sources + 29 public APIs
    assert report["summary"]["healthy"] >= 1
    bcb = next(e for e in report["entries"] if e["id"] == "bcb_sgs")
    assert bcb["freshness_status"] == "HEALTHY"
    assert bcb["freshness_sla_hours"] > 0
    assert len(bcb["cron_schedule_expression"].split()) == 5


def test_cdc_delta_tracker_detects_new_unchanged_and_modified(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    settings = GatewaySettings(data_dir=data_dir)
    pq_path = _seed_sample_parquet(data_dir, source_id="ibge_localidades", layer="silver")

    cdc = CdcDeltaTracker(settings=settings)
    plan1 = cdc.build_delta_plan()
    assert plan1["to_upload_count"] == 1
    assert plan1["to_upload"][0]["change_type"] == "NEW"
    assert plan1["to_upload"][0]["row_count"] == 2

    cdc.commit_synced_entries(plan1["to_upload"])

    plan2 = cdc.build_delta_plan()
    assert plan2["to_upload_count"] == 0
    assert plan2["unchanged_count"] == 1
    assert plan2["bytes_saved_by_cdc"] > 0

    # Modify parquet file
    pq.write_table(
        pa.table({"id_municipio": ["3550308", "3304557", "5300108"], "valor": [1.0, 2.0, 3.0]}),
        pq_path,
        compression="zstd",
    )
    plan3 = cdc.build_delta_plan()
    assert plan3["to_upload_count"] == 1
    assert plan3["to_upload"][0]["change_type"] == "MODIFIED"
    assert plan3["to_upload"][0]["row_count"] == 3


def test_cloud_mirror_local_and_r2_sigv4_mock(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    data_dir = tmp_path / "data"
    settings = GatewaySettings(data_dir=data_dir)
    _seed_sample_parquet(data_dir, source_id="cvm_fundos", layer="gold")

    # 1. Local mirror sync + idempotent second sync
    mirror = CloudLakehouseMirror(settings=settings)
    res1 = mirror.sync(provider="local", dry_run=False)
    assert res1["provider"] == "local"
    assert res1["uploaded_count"] == 1
    assert Path(res1["manifest_uri"]).is_file()

    res2 = mirror.sync(provider="local", dry_run=False)
    assert res2["uploaded_count"] == 0
    assert res2["skipped_unchanged"] == 1

    # 2. Cloudflare R2 SigV4 mocked HTTP PUT
    seen_requests: list[tuple[str, dict[str, str]]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen_requests.append((str(request.url), dict(request.headers)))
        return httpx.Response(200)

    monkeypatch.setenv("BDG_MIRROR_PROVIDER", "r2")
    monkeypatch.setenv("BDG_MIRROR_BUCKET", "test-public-bucket")
    monkeypatch.setenv("BDG_MIRROR_ENDPOINT_URL", "https://acct.r2.cloudflarestorage.com")
    monkeypatch.setenv("BDG_MIRROR_ACCESS_KEY_ID", "test-ak")
    monkeypatch.setenv("BDG_MIRROR_SECRET_ACCESS_KEY", "test-sk")

    r2_settings = GatewaySettings(
        data_dir=data_dir,
        mirror_local_dir=tmp_path / "r2_state",
    )
    r2_client = httpx.Client(transport=httpx.MockTransport(handler))
    r2_mirror = CloudLakehouseMirror(settings=r2_settings, http_client=r2_client)
    r2_res = r2_mirror.sync(provider="r2", dry_run=False)

    assert r2_res["provider"] == "r2"
    assert r2_res["uploaded_count"] == 1
    assert r2_res["manifest_uri"] == "r2://test-public-bucket/public-data-gateway/manifest.json"
    assert len(seen_requests) == 2  # parquet + manifest.json
    assert any("AWS4-HMAC-SHA256" in hdrs.get("authorization", "") for _, hdrs in seen_requests)


def test_warehouse_adapter_duckdb_motherduck_bigquery_and_sql_guard(tmp_path: Path) -> None:
    data_dir = tmp_path / "data"
    settings = GatewaySettings(data_dir=data_dir)
    _seed_sample_parquet(data_dir, source_id="bcb_sgs", layer="gold")

    wh = PublicWarehouseAdapter(settings=settings, duckdb_path=tmp_path / "test.duckdb")

    # DuckDB live publish + read-only SQL query
    pub = wh.publish_views(provider="duckdb", dry_run=False)
    assert pub["total_views"] == 1
    view_name = pub["published_views"][0]
    rows = wh.query_sql(f"SELECT id_municipio, valor FROM {view_name} ORDER BY id_municipio")
    assert len(rows) == 2
    assert rows[0]["id_municipio"] == "3304557"

    with pytest.raises(ValueError, match="read-only"):
        wh.query_sql(f"DROP VIEW {view_name}")

    # MotherDuck & BigQuery DDL generation
    md_ddl = wh.generate_ddl(provider="motherduck")
    assert "CREATE OR REPLACE VIEW" in md_ddl["statements"][0]["ddl"]

    bq_ddl = wh.generate_ddl(provider="bigquery")
    assert "CREATE OR REPLACE EXTERNAL TABLE" in bq_ddl["statements"][0]["ddl"]
    assert "format = 'PARQUET'" in bq_ddl["statements"][0]["ddl"]


def test_phase11_four_interface_parity(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    data_dir = tmp_path / "data"
    _seed_sample_parquet(data_dir, source_id="bcb_sgs", layer="gold")
    monkeypatch.setenv("PDG_DATA_DIR", str(data_dir))
    settings = GatewaySettings(data_dir=data_dir)

    # 1. SDK
    sdk = BrazilData(settings=settings)
    assert sdk.lakehouse.audit_sla(source_id="bcb_sgs")["total_audited"] == 1
    assert sdk.lakehouse.cdc_plan()["to_upload_count"] == 1
    assert sdk.lakehouse.mirror_sync(provider="local", dry_run=True)["planned_upload_count"] == 1
    assert sdk.lakehouse.warehouse_ddl(provider="bigquery")["total_views"] == 1

    # 2. FastAPI
    client = TestClient(create_app(settings=settings))
    assert client.get("/api/v1/lakehouse/sla?source_id=bcb_sgs").status_code == 200
    assert client.get("/api/v1/lakehouse/cdc/plan").status_code == 200
    assert client.post("/api/v1/lakehouse/mirror/sync?provider=local&dry_run=true").status_code == 200
    assert client.post("/api/v1/lakehouse/warehouse/publish?provider=duckdb&dry_run=true").status_code == 200
    assert client.get("/api/v1/lakehouse/warehouse/query?sql=SELECT%201%20AS%20ok").status_code == 200

    # 3. CLI
    rc = cli_main(["lakehouse", "sla", "--source-id", "bcb_sgs", "--json"])
    assert rc == 0
    cli_out = json.loads(capsys.readouterr().out)
    assert cli_out["entries"][0]["id"] == "bcb_sgs"

    # 4. MCP
    mcp = MCPDispatcher(settings=settings)
    tools_resp = mcp.handle_request({"jsonrpc": "2.0", "id": 1, "method": "tools/list"})
    tool_names = {t["name"] for t in tools_resp["result"]["tools"]}
    assert {
        "bdg_lakehouse_sla_audit",
        "bdg_lakehouse_cdc_plan",
        "bdg_lakehouse_mirror_sync",
        "bdg_lakehouse_warehouse_ddl",
    }.issubset(tool_names)

    call_resp = mcp.handle_request({
        "jsonrpc": "2.0",
        "id": 2,
        "method": "tools/call",
        "params": {"name": "bdg_lakehouse_sla_audit", "arguments": {"source_id": "bcb_sgs"}},
    })
    assert call_resp["result"]["isError"] is False

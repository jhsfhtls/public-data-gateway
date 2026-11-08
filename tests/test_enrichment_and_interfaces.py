"""Tests for EnrichmentEngine, POP_T_154 shield, REST API, CLI, and MCP Server."""

import csv
from pathlib import Path

import pyarrow as pa
import pytest
from fastapi.testclient import TestClient

from brazil_data_gateway.api.app import create_app
from brazil_data_gateway.cli.main import main as cli_main
from brazil_data_gateway.core.config import GatewaySettings
from brazil_data_gateway.enrichment.engine import EnrichmentEngine
from brazil_data_gateway.mcp.server import MCPDispatcher


def test_enrichment_engine_and_client_path_shield(tmp_settings: GatewaySettings, tmp_path: Path) -> None:
    """Test territorial enrichment, CWE-1236 CSV safety, and POP_T_154 Client Path Shield."""
    enricher = EnrichmentEngine(settings=tmp_settings)

    table = pa.table(
        {
            "cidade": ["Boa Esperança do Norte", "São Paulo", "CidadeInexistenteXYZ"],
            "uf": ["MT", "SP", "XX"],
            "nota": ["=cmd|'/C calc'!A0", "-15.5", "Normal"],
        }
    )

    # 1. Enforce POP_T_154: Output path inside data_dir MUST raise ValueError
    forbidden_out = tmp_settings.data_dir / "forbidden_client_output.csv"
    with pytest.raises(ValueError, match="Client Path Shield"):
        enricher.enrich(
            entity_type="municipio",
            key_column="cidade",
            input_table=table,
            uf_column="uf",
            output_path=forbidden_out,
        )

    # 2. Valid external destination with CSV output sanitization
    safe_out = tmp_path / "client_exports" / "enriched.csv"
    enriched_table, report = enricher.enrich(
        entity_type="municipio",
        key_column="cidade",
        input_table=table,
        uf_column="uf",
        output_path=safe_out,
    )

    assert report.total_rows == 3
    assert report.matched_rows == 2
    assert report.invalid_keys_count == 1
    assert safe_out.is_file()

    # Read back generated CSV and verify CWE-1236 prefixing
    with open(safe_out, encoding="utf-8") as f:
        reader = list(csv.DictReader(f))
        assert len(reader) == 3
        # "=cmd" must be neutralized with a leading single-quote
        assert reader[0]["nota"].startswith("'=")
        # Legitimate negative float "-15.5" must NOT be prefixed
        assert reader[1]["nota"] == "-15.5"
        # Canonical IBGE columns enriched
        assert reader[0]["id_municipio"] == "5101837"
        assert reader[0]["sigla_uf"] == "MT"
        assert reader[1]["id_municipio"] == "3550308"


def test_fastapi_rest_endpoints(tmp_settings: GatewaySettings) -> None:
    """Test FastAPI application endpoints, X-Request-ID, and RFC 7807 ProblemDetails."""
    app = create_app(tmp_settings)
    client = TestClient(app)

    # 1. Health check & X-Request-ID propagation
    resp = client.get("/api/v1/health", headers={"X-Request-ID": "custom-req-id-123"})
    assert resp.status_code == 200
    assert resp.json() == {"status": "healthy", "version": "0.1.0"}
    assert resp.headers.get("X-Request-ID") == "custom-req-id-123"

    # 2. Municipality normalization endpoint
    norm_resp = client.get("/api/v1/normalize/municipality", params={"query": "Boa Esperança do Norte", "uf": "MT"})
    assert norm_resp.status_code == 200
    assert norm_resp.json()["id_municipio"] == "5101837"

    # 3. RFC 7807 ProblemDetails on 404 resolution error
    err_resp = client.get("/api/v1/normalize/municipality", params={"query": "CidadeInexistenteZZZ"})
    assert err_resp.status_code == 404
    assert err_resp.headers["content-type"] == "application/problem+json"
    err_body = err_resp.json()
    assert err_body["status"] == 404
    assert "CidadeInexistenteZZZ" in err_body["detail"]

    # 4. Real-time enrichment preview POST
    preview_payload = {
        "rows": [
            {"city": "Boa Esperança do Norte", "state": "MT"},
            {"city": "São Paulo", "state": "SP"},
        ],
        "entity_type": "municipio",
        "key_column": "city",
        "uf_column": "state",
    }
    prev_resp = client.post("/api/v1/enrich/preview", json=preview_payload)
    assert prev_resp.status_code == 200
    data = prev_resp.json()
    assert len(data["rows"]) == 2
    assert data["rows"][0]["id_municipio"] == "5101837"
    assert data["report"]["matched_rows"] == 2


def test_cli_interface(tmp_settings: GatewaySettings, tmp_path: Path) -> None:
    """Test bdg CLI subcommands: status, catalog, profile, enrich."""
    # Subcommand: status
    assert cli_main(["status"]) == 0

    # Subcommand: catalog --json
    assert cli_main(["catalog", "--json"]) == 0

    # Subcommand: profile
    sample_table = pa.table({"col_a": [1, 2, 3], "col_b": [10.0, 20.0, 30.0]})
    parquet_path = tmp_path / "cli_sample.parquet"
    pa.parquet.write_table(sample_table, parquet_path)

    assert cli_main(["profile", str(parquet_path), "--json"]) == 0

    # Subcommand: enrich
    csv_input = tmp_path / "cli_input.csv"
    with open(csv_input, "w", encoding="utf-8", newline="\n") as f:
        f.write("municipio,uf\nBoa Esperança do Norte,MT\n")

    csv_output = tmp_path / "cli_output.csv"
    ret = cli_main(
        [
            "enrich",
            "--input",
            str(csv_input),
            "--out-file",
            str(csv_output),
            "--entity",
            "municipio",
            "--key-col",
            "municipio",
            "--uf-col",
            "uf",
            "--json",
        ]
    )
    assert ret == 0
    assert csv_output.is_file()


def test_mcp_dispatcher_and_hitl_guard(tmp_settings: GatewaySettings, tmp_path: Path) -> None:
    """Test MCP JSON-RPC protocol dispatch, untrusted XML wrapping, and HITL Tier 3 guard."""
    dispatcher = MCPDispatcher(tmp_settings)

    # 1. initialize
    init_res = dispatcher.handle_request({"id": 1, "method": "initialize", "params": {}})
    assert init_res["result"]["serverInfo"]["name"] in ["public-data-gateway-mcp", "brazil-data-gateway-mcp"]

    # 2. tools/list
    tools_res = dispatcher.handle_request({"id": 2, "method": "tools/list", "params": {}})
    tool_names = [t["name"] for t in tools_res["result"]["tools"]]
    assert "bdg_list_sources" in tool_names
    assert "bdg_resolve_municipality" in tool_names
    assert "bdg_enrich_dataset" in tool_names
    assert "bdg_sync_dataset" in tool_names

    # 3. tools/call bdg_resolve_municipality with XML wrapping
    call_res = dispatcher.handle_request(
        {
            "id": 3,
            "method": "tools/call",
            "params": {
                "name": "bdg_resolve_municipality",
                "arguments": {"code_or_name": "Boa Esperança do Norte", "uf": "MT"},
            },
        }
    )
    content_text = call_res["result"]["content"][0]["text"]
    assert '<untrusted_gov_data source="ibge_municipios">' in content_text
    assert "5101837" in content_text

    # 4. tools/call bdg_sync_dataset without human approval MUST be rejected (HITL Tier 3 Guard)
    sync_blocked = dispatcher.handle_request(
        {
            "id": 4,
            "method": "tools/call",
            "params": {
                "name": "bdg_sync_dataset",
                "arguments": {"source_id": "rfb_cnpj", "human_approved": False},
            },
        }
    )
    assert sync_blocked["result"]["isError"] is True
    assert "HITL Tier 3 Guard" in sync_blocked["result"]["content"][0]["text"]

    # 5. tools/call bdg_sync_dataset WITH human approval passes
    sync_ok = dispatcher.handle_request(
        {
            "id": 5,
            "method": "tools/call",
            "params": {
                "name": "bdg_sync_dataset",
                "arguments": {"source_id": "rfb_cnpj", "human_approved": True},
            },
        }
    )
    assert sync_ok["result"]["isError"] is False
    assert "approved and queued" in sync_ok["result"]["content"][0]["text"]

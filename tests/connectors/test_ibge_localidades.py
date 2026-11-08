"""Comprehensive integration & contract test suite for IBGE Localidades connector.

Tests the full Bronze -> Silver -> Gold pipeline and interfaces across diverse cities
from all 5 macro-regions and multiple references:
  1. Manaus (AM) - Norte, Capital, Amazônia / Polo Industrial
  2. Ouro Preto (MG) - Sudeste, Histórica, Interior
  3. Chuí (RS) - Sul, Extremo Sul, Fronteira
  4. Boa Esperança do Norte (MT) - Centro-Oeste, Emancipada 2024 (microrregião nula)
  5. Olinda (PE) - Nordeste, Litoral, Histórica
  6. Brasília (DF) - Centro-Oeste, Capital Federal
"""

import json
from pathlib import Path
from typing import Any

import pytest
from fastapi.testclient import TestClient

from brazil_data_gateway.api.app import create_app
from brazil_data_gateway.cli.main import main as cli_main
from brazil_data_gateway.connectors.demografico.ibge_localidades import (
    IBGELocalidadesConnector,
)
from brazil_data_gateway.core.config import GatewaySettings
from brazil_data_gateway.core.normalizer import CheckDigitValidator
from brazil_data_gateway.core.storage import HybridStorageEngine
from brazil_data_gateway.mcp.server import MCPDispatcher
from brazil_data_gateway.sdk import BrazilData


@pytest.fixture
def temp_settings(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> GatewaySettings:
    """Fixture providing isolated temporary directories for tests."""
    monkeypatch.setenv("BDG_OFFLINE_MODE", "1")
    repo_root = Path(__file__).resolve().parents[2]
    settings = GatewaySettings(
        repo_root=repo_root,
        data_dir=tmp_path / "data",
    )
    settings.ensure_directories()
    return settings


@pytest.fixture
def connector(temp_settings: GatewaySettings) -> IBGELocalidadesConnector:
    """Instantiate IBGE Localidades connector in isolated workspace."""
    storage = HybridStorageEngine(settings=temp_settings)
    return IBGELocalidadesConnector(settings=temp_settings, storage=storage)


# ---------------------------------------------------------------------------
# Test Case 1: Bronze Layer Ingestion & Fallback
# ---------------------------------------------------------------------------
def test_bronze_extraction_and_snapshot(connector: IBGELocalidadesConnector, temp_settings: GatewaySettings) -> None:
    """Bronze layer must fetch raw data and persist raw JSON snapshot."""
    raw_data = connector.fetch_raw_bronze(offline_fallback=True)
    assert isinstance(raw_data, list)
    assert len(raw_data) >= 5500, "Reference seed must contain >= 5,500 municipalities"

    bronze_snapshot = temp_settings.data_dir / "bronze" / "ibge_localidades" / "raw_municipios.json"
    assert bronze_snapshot.is_file(), "Bronze JSON snapshot must be written to disk"

    # Verify key cities are present in raw payload
    raw_ids = {str(item.get("id") or item.get("id_municipio")) for item in raw_data}
    assert "1302603" in raw_ids, "Manaus must be present in Bronze payload"
    assert "3146107" in raw_ids, "Ouro Preto must be present in Bronze payload"
    assert "4305439" in raw_ids, "Chuí must be present in Bronze payload"
    assert "5101837" in raw_ids, "Boa Esperança do Norte must be present in Bronze payload"
    assert "2609600" in raw_ids, "Olinda must be present in Bronze payload"
    assert "5300108" in raw_ids, "Brasília must be present in Bronze payload"


# ---------------------------------------------------------------------------
# Test Case 2: Silver Normalization & Diverse Cities Resolution
# ---------------------------------------------------------------------------
def test_silver_normalization_diverse_cities(connector: IBGELocalidadesConnector) -> None:
    """Silver layer normalizes varied cities with valid check digits and hierarchy resolution."""
    raw_data = connector.fetch_raw_bronze()
    silver_rows = connector.transform_silver_rows(raw_data)
    assert len(silver_rows) >= 5500

    by_id = {r["id_municipio"]: r for r in silver_rows}

    # City 1: Manaus (AM - Norte, Capital)
    manaus = by_id.get("1302603")
    assert manaus is not None
    assert manaus["nome_municipio"] == "Manaus"
    assert manaus["sigla_uf"] == "AM"
    assert manaus["id_uf"] == "13"
    assert manaus["regiao"] == "Norte"
    assert manaus["sigla_regiao"] == "N"
    assert manaus["is_capital"] is True
    assert CheckDigitValidator.is_valid_ibge_7("1302603")

    # City 2: Ouro Preto (MG - Sudeste, Histórica, Interior)
    ouro_preto = by_id.get("3146107")
    assert ouro_preto is not None
    assert ouro_preto["nome_municipio"] == "Ouro Preto"
    assert ouro_preto["sigla_uf"] == "MG"
    assert ouro_preto["id_uf"] == "31"
    assert ouro_preto["regiao"] == "Sudeste"
    assert ouro_preto["is_capital"] is False
    assert CheckDigitValidator.is_valid_ibge_7("3146107")

    # City 3: Chuí (RS - Sul, Extremo)
    chui = by_id.get("4305439")
    assert chui is not None
    assert chui["nome_municipio"] == "Chuí"
    assert chui["sigla_uf"] == "RS"
    assert chui["id_uf"] == "43"
    assert chui["regiao"] == "Sul"
    assert chui["is_capital"] is False
    assert CheckDigitValidator.is_valid_ibge_7("4305439")

    # City 4: Boa Esperança do Norte (MT - Centro-Oeste, Emancipado 2024, Microrregião nula)
    boa_esp = by_id.get("5101837")
    assert boa_esp is not None
    assert boa_esp["nome_municipio"] == "Boa Esperança do Norte"
    assert boa_esp["sigla_uf"] == "MT"
    assert boa_esp["id_uf"] == "51"
    assert boa_esp["regiao"] == "Centro-Oeste"
    assert boa_esp["is_capital"] is False
    assert CheckDigitValidator.is_valid_ibge_7("5101837")

    # City 5: Olinda (PE - Nordeste, Litoral)
    olinda = by_id.get("2609600")
    assert olinda is not None
    assert olinda["nome_municipio"] == "Olinda"
    assert olinda["sigla_uf"] == "PE"
    assert olinda["id_uf"] == "26"
    assert olinda["regiao"] == "Nordeste"
    assert olinda["is_capital"] is False
    assert CheckDigitValidator.is_valid_ibge_7("2609600")

    # City 6: Brasília (DF - Centro-Oeste, Capital Federal)
    brasilia = by_id.get("5300108")
    assert brasilia is not None
    assert brasilia["nome_municipio"] == "Brasília"
    assert brasilia["sigla_uf"] == "DF"
    assert brasilia["id_uf"] == "53"
    assert brasilia["regiao"] == "Centro-Oeste"
    assert brasilia["is_capital"] is True
    assert CheckDigitValidator.is_valid_ibge_7("5300108")


# ---------------------------------------------------------------------------
# Test Case 3: Full End-to-End Extract (Bronze -> Silver -> Gold)
# ---------------------------------------------------------------------------
def test_full_pipeline_extract_and_gold_materialization(
    connector: IBGELocalidadesConnector, temp_settings: GatewaySettings
) -> None:
    """Full extract must materialize Silver and Gold Parquet tables with ML features."""
    silver_table = connector.extract()
    assert silver_table.num_rows >= 5500

    # Silver file check
    silver_file = temp_settings.silver_dir / "ibge_localidades.parquet"
    assert silver_file.is_file(), "Silver Parquet must exist on disk"

    # Gold file check
    gold_file = temp_settings.gold_dir / "ibge_localidades.parquet"
    assert gold_file.is_file(), "Gold Parquet must exist on disk"

    # Read Gold feature table
    gold_table = connector.storage.read_table("ibge_localidades", layer="gold")
    assert gold_table.num_rows == silver_table.num_rows

    gold_rows = {r["id_municipio"]: r for r in gold_table.to_pylist()}

    # Check Gold features for Manaus (Capital, Norte = 1)
    g_manaus = gold_rows["1302603"]
    assert g_manaus["is_capital"] == 1
    assert g_manaus["codigo_macro_regiao"] == 1  # Norte
    assert g_manaus["qtd_municipios_mesma_uf"] == 62  # AM has 62 municipalities

    # Check Gold features for Ouro Preto (Non-capital, Sudeste = 3)
    g_ouro_preto = gold_rows["3146107"]
    assert g_ouro_preto["is_capital"] == 0
    assert g_ouro_preto["codigo_macro_regiao"] == 3  # Sudeste
    assert g_ouro_preto["qtd_municipios_mesma_uf"] == 853  # MG has 853 municipalities

    # Check Gold features for Olinda (Non-capital, Nordeste = 2)
    g_olinda = gold_rows["2609600"]
    assert g_olinda["is_capital"] == 0
    assert g_olinda["codigo_macro_regiao"] == 2  # Nordeste
    assert g_olinda["qtd_municipios_mesma_uf"] == 185  # PE has 185 municipalities

    # Check Gold features for Chuí (Non-capital, Sul = 4)
    g_chui = gold_rows["4305439"]
    assert g_chui["is_capital"] == 0
    assert g_chui["codigo_macro_regiao"] == 4  # Sul
    assert g_chui["qtd_municipios_mesma_uf"] == 497  # RS has 497 municipalities


# ---------------------------------------------------------------------------
# Test Case 4: Query Methods (Single, List, UFs)
# ---------------------------------------------------------------------------
def test_connector_query_methods(connector: IBGELocalidadesConnector) -> None:
    """Test get_municipio, list_municipios, and list_ufs across references."""
    # Ensure tables exist
    connector.extract()

    # 1. Query by 7-digit code
    rec_manaus = connector.get_municipio("1302603")
    assert rec_manaus is not None
    assert rec_manaus["nome_municipio"] == "Manaus"

    # 2. Query by 6-digit legacy code
    rec_ouro = connector.get_municipio("314610")
    assert rec_ouro is not None
    assert rec_ouro["nome_municipio"] == "Ouro Preto"

    # 3. Query by Name + UF disambiguation
    rec_chui = connector.get_municipio("chui", uf="RS")
    assert rec_chui is not None
    assert rec_chui["id_municipio"] == "4305439"

    rec_olinda = connector.get_municipio("olinda", uf="PE")
    assert rec_olinda is not None
    assert rec_olinda["id_municipio"] == "2609600"

    # 4. List UFs: exactly 27 federative units
    ufs = connector.list_ufs()
    assert len(ufs) == 27
    uf_siglas = {u["sigla_uf"] for u in ufs}
    assert {"AM", "MG", "RS", "MT", "PE", "DF"}.issubset(uf_siglas)

    # 5. List municipalities filtered by UF
    am_munis = connector.list_municipios(uf="AM")
    assert len(am_munis) == 62
    assert any(m["nome_municipio"] == "Manaus" for m in am_munis)

    # 6. List only capitals
    capitais = connector.list_municipios(capitais_only=True)
    assert len(capitais) == 27
    cap_names = {c["nome_municipio"] for c in capitais}
    assert "Manaus" in cap_names
    assert "Brasília" in cap_names
    assert "Ouro Preto" not in cap_names


# ---------------------------------------------------------------------------
# Test Case 5: Python SDK Integration (bd.geo)
# ---------------------------------------------------------------------------
def test_sdk_geo_facade_integration(temp_settings: GatewaySettings) -> None:
    """Verify high-level Python SDK bd.geo facade."""
    bd = BrazilData(settings=temp_settings)
    # Trigger sync
    bd.geo.sync_localidades()

    # Query varied cities via SDK
    manaus = bd.geo.municipio("1302603")
    assert manaus is not None
    assert manaus["nome_municipio"] == "Manaus"

    ouro = bd.geo.municipio("ouro preto", uf="MG")
    assert ouro is not None
    assert ouro["id_municipio"] == "3146107"

    boa_esp = bd.geo.municipio("5101837")
    assert boa_esp is not None
    assert boa_esp["nome_municipio"] == "Boa Esperança do Norte"

    ufs = bd.geo.ufs()
    assert len(ufs) == 27


# ---------------------------------------------------------------------------
# Test Case 6: FastAPI Endpoints Integration
# ---------------------------------------------------------------------------
def test_fastapi_geo_endpoints(temp_settings: GatewaySettings) -> None:
    """Verify REST API endpoints for IBGE localidades."""
    app = create_app(settings=temp_settings)
    client = TestClient(app)

    # 1. Sync via API
    res_sync = client.post("/api/v1/geo/ibge/sync")
    assert res_sync.status_code == 200
    sync_data = res_sync.json()
    assert sync_data["status"] == "success"
    assert sync_data["rows_materialized"] >= 5500

    # 2. Get UFs
    res_ufs = client.get("/api/v1/geo/ibge/ufs")
    assert res_ufs.status_code == 200
    assert len(res_ufs.json()) == 27

    # 3. Get single municipality by ID
    res_manaus = client.get("/api/v1/geo/ibge/municipio/1302603")
    assert res_manaus.status_code == 200
    assert res_manaus.json()["nome_municipio"] == "Manaus"

    # 4. Get single municipality by Name + UF
    res_olinda = client.get("/api/v1/geo/ibge/municipio/olinda?uf=PE")
    assert res_olinda.status_code == 200
    assert res_olinda.json()["id_municipio"] == "2609600"

    # 5. Non-existent city -> 404 RFC 7807 ProblemDetails
    res_404 = client.get("/api/v1/geo/ibge/municipio/cidade_inexistente_xyz")
    assert res_404.status_code == 404
    assert res_404.headers["content-type"] == "application/problem+json"


# ---------------------------------------------------------------------------
# Test Case 7: CLI Integration (bdg sync & bdg geo)
# ---------------------------------------------------------------------------
def test_cli_geo_commands(temp_settings: GatewaySettings, monkeypatch: Any, capsys: Any) -> None:
    """Verify CLI subcommands for sync and geo queries."""
    monkeypatch.setenv("BDG_DATA_DIR", str(temp_settings.data_dir))
    # Run sync via CLI
    exit_code = cli_main(["sync", "ibge_localidades"])
    assert exit_code == 0
    out = capsys.readouterr().out
    assert "Sync complete!" in out

    # Run geo ufs via CLI
    exit_code = cli_main(["geo", "ufs"])
    assert exit_code == 0
    out = capsys.readouterr().out
    assert "[AM]" in out
    assert "[MG]" in out

    # Run geo municipio via CLI
    exit_code = cli_main(["geo", "municipio", "1302603", "--json"])
    assert exit_code == 0
    out = capsys.readouterr().out
    data = json.loads(out)
    assert data["nome_municipio"] == "Manaus"


# ---------------------------------------------------------------------------
# Test Case 8: MCP Tool Handlers & HITL Tier 3 Guard
# ---------------------------------------------------------------------------
def test_mcp_geo_tools_and_hitl_guard(temp_settings: GatewaySettings) -> None:
    """Verify MCP tools for UFs, municipalities, and HITL Tier 3 sync protection."""
    dispatcher = MCPDispatcher(settings=temp_settings)

    # 1. bdg_list_ufs
    req_ufs = {"jsonrpc": "2.0", "id": 1, "method": "tools/call", "params": {"name": "bdg_list_ufs"}}
    resp_ufs = dispatcher.handle_request(req_ufs)
    assert not resp_ufs.get("result", {}).get("isError")
    assert "Amazonas" in resp_ufs["result"]["content"][0]["text"]

    # 2. bdg_sync_dataset without human_approved -> Tier 3 Guard Rejection
    req_sync_unauth = {
        "jsonrpc": "2.0",
        "id": 2,
        "method": "tools/call",
        "params": {"name": "bdg_sync_dataset", "arguments": {"source_id": "ibge_localidades", "human_approved": False}},
    }
    resp_sync_unauth = dispatcher.handle_request(req_sync_unauth)
    assert resp_sync_unauth["result"]["isError"] is True
    assert "HITL Tier 3 Guard" in resp_sync_unauth["result"]["content"][0]["text"]

    # 3. bdg_sync_dataset with human_approved -> Successful execution
    req_sync_auth = {
        "jsonrpc": "2.0",
        "id": 3,
        "method": "tools/call",
        "params": {"name": "bdg_sync_dataset", "arguments": {"source_id": "ibge_localidades", "human_approved": True}},
    }
    resp_sync_auth = dispatcher.handle_request(req_sync_auth)
    assert resp_sync_auth["result"]["isError"] is False
    assert "completed successfully" in resp_sync_auth["result"]["content"][0]["text"]


# ---------------------------------------------------------------------------
# Test Case 9: Live Endpoint Connectivity Smoke Test (Optional Live Gate)
# ---------------------------------------------------------------------------
@pytest.mark.live
def test_live_ibge_connectivity(connector: IBGELocalidadesConnector) -> None:
    """Live smoke test verifying actual HTTP connectivity to servicodados.ibge.gov.br."""
    connected = connector.test_connection()
    assert connected is True, "Must successfully connect to official IBGE API in live mode"

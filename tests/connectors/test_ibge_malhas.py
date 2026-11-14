"""Tests for IBGE Malhas Territoriais Vetoriais Connector (11-Gate Protocol)."""

from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import patch

import pyarrow.parquet as pq
import pytest
import yaml
from fastapi.testclient import TestClient

from public_data_gateway.api.app import create_app
from public_data_gateway.cli.main import main
from public_data_gateway.connectors.geografico.ibge_malhas.connector import (
    IBGEMalhasConnector,
    compute_geometry_metrics,
    haversine_km,
)
from public_data_gateway.mcp.server import MCPDispatcher
from public_data_gateway.sdk import BrazilData


@pytest.fixture(scope="module", autouse=True)
def ensure_malhas_baseline() -> None:
    """Garante que os Parquets de baseline existam em ambiente CI sem rede externa."""
    conn = IBGEMalhasConnector()
    if not (conn.silver_dir / "malhas_municipios.parquet").exists():
        with patch(
            "public_data_gateway.connectors.geografico.ibge_malhas.connector.requests.get",
            side_effect=RuntimeError("Offline CI baseline synthesis"),
        ):
            conn.extract_all()


def test_contract_validity() -> None:
    """Verifica se o contrato ODCS v3.1 está em conformidade estrutural."""
    contract_path = (
        Path(__file__).resolve().parents[2]
        / "src"
        / "public_data_gateway"
        / "connectors"
        / "geografico"
        / "ibge_malhas"
        / "contract.yaml"
    )
    assert contract_path.exists()
    with contract_path.open(encoding="utf-8") as f:
        data = yaml.safe_load(f)

    assert data["schema_version"] == "3.1.0"
    assert data["contract_id"] == "ODCS-BR-IBGE-MALHAS-001"
    assert data["source_id"] == "ibge_malhas"
    tables = data["schema"]["tables"]
    assert "malhas_municipios" in tables
    assert "malhas_ufs" in tables
    assert "malhas_regioes" in tables
    assert "malhas_brasil" in tables
    assert "features_malhas_territoriais_municipios_brasil" in tables


def test_geometry_metrics_computation() -> None:
    """Verifica a precisão matemática das métricas de geometria esférica e compacidade."""
    # Quadrado 0.1 x 0.1 graus
    square_geom = {
        "type": "Polygon",
        "coordinates": [
            [
                [-46.6, -23.5],
                [-46.5, -23.5],
                [-46.5, -23.6],
                [-46.6, -23.6],
                [-46.6, -23.5],
            ]
        ],
    }
    metrics = compute_geometry_metrics(square_geom)
    assert metrics["tipo_geometria"] == "Polygon"
    assert metrics["qtd_vertices"] == 5
    assert metrics["qtd_poligonos"] == 1
    assert metrics["area_km2"] > 100.0
    assert metrics["perimeter_km"] > 40.0
    assert 50.0 <= metrics["compacidade"] <= 100.0
    assert metrics["bbox"] == [-46.6, -23.6, -46.5, -23.5]
    assert -46.6 <= metrics["centroide_lon"] <= -46.5
    assert -23.6 <= metrics["centroide_lat"] <= -23.5


def test_haversine_accuracy() -> None:
    """Valida a precisão da distância geodésica entre São Paulo e Rio de Janeiro (~357 km)."""
    # SP: -46.6333, -23.5505 | RJ: -43.1729, -22.9068
    dist = haversine_km(-46.6333, -23.5505, -43.1729, -22.9068)
    assert 350.0 < dist < 370.0


def test_baseline_parquets_exist() -> None:
    """Valida a materialização física dos arquivos Parquet de baseline em data/ibge_malhas/."""
    conn = IBGEMalhasConnector()
    assert conn.silver_dir.exists()
    assert conn.gold_dir.exists()

    mun_p = conn.silver_dir / "malhas_municipios.parquet"
    uf_p = conn.silver_dir / "malhas_ufs.parquet"
    reg_p = conn.silver_dir / "malhas_regioes.parquet"
    br_p = conn.silver_dir / "malhas_brasil.parquet"
    gold_p = conn.gold_dir / "features_malhas_territoriais_municipios_brasil.parquet"

    assert mun_p.exists()
    assert uf_p.exists()
    assert reg_p.exists()
    assert br_p.exists()
    assert gold_p.exists()

    t_mun = pq.read_table(mun_p)
    assert len(t_mun) >= 5
    assert "id_municipio" in t_mun.column_names
    assert "geojson_geometry" in t_mun.column_names

    t_uf = pq.read_table(uf_p)
    assert len(t_uf) >= 20

    t_gold = pq.read_table(gold_p)
    assert "score_compacidade_territorial" in t_gold.column_names
    assert "score_isolamento_geografico" in t_gold.column_names


def test_connector_methods() -> None:
    """Testa os métodos de consulta de limites, features e rankings do conector."""
    conn = IBGEMalhasConnector()

    mun = conn.get_municipio_boundary("3550308")
    assert mun["id_municipio"] == "3550308"
    assert "geometry" in mun
    assert mun["area_aproximada_km2"] > 0.0

    uf = conn.get_uf_boundary("SP")
    assert uf["sigla_uf"] == "SP"
    assert "geometry" in uf

    feat = conn.get_territorial_features("3550308")
    assert feat["id_municipio"] == "3550308"
    assert 0.0 <= feat["score_compacidade_territorial"] <= 100.0
    assert 0.0 <= feat["score_isolamento_geografico"] <= 100.0

    ranks = conn.list_top_compact_municipalities(limit=5)
    assert len(ranks) > 0
    assert ranks[0]["score_compacidade_territorial"] >= ranks[-1]["score_compacidade_territorial"]


def test_sdk_parity() -> None:
    """Testa a paridade das chamadas de malha territorial no Python SDK BrazilData."""
    sdk = BrazilData()

    mun = sdk.get_municipio_boundary("3550308")
    assert mun["id_municipio"] == "3550308"

    uf = sdk.get_uf_boundary("SP")
    assert uf["sigla_uf"] == "SP"

    feat = sdk.get_territorial_features("3550308")
    assert feat["id_municipio"] == "3550308"

    ranks = sdk.list_top_compact_municipalities(limit=3)
    assert len(ranks) <= 3


def test_cli_parity(capsys: pytest.CaptureFixture[str]) -> None:
    """Testa os comandos de CLI correspondentes para malhas e features espaciais."""
    # 1. Features em JSON
    rc = main(["malhas", "features", "3550308", "--json"])
    assert rc == 0
    out = capsys.readouterr().out
    data = json.loads(out)
    assert data["id_municipio"] == "3550308"

    # 2. Município boundary
    rc = main(["malhas", "municipio", "3550308"])
    assert rc == 0
    out_mun = capsys.readouterr().out
    assert "IBGE Malha Territorial Municipal" in out_mun

    # 3. UF boundary
    rc = main(["malhas", "uf", "SP"])
    assert rc == 0
    out_uf = capsys.readouterr().out
    assert "IBGE Malha Territorial Estadual" in out_uf

    # 4. Ranking
    rc = main(["malhas", "ranking", "--limit", "3"])
    assert rc == 0
    out_rank = capsys.readouterr().out
    assert "Top" in out_rank


def test_rest_api_parity() -> None:
    """Testa a paridade dos endpoints REST FastAPI para malhas territoriais."""
    app = create_app()
    client = TestClient(app)

    # 1. Top compact (rota estática)
    r_top = client.get("/api/v1/geo/malhas/top-compact?limit=3")
    assert r_top.status_code == 200
    assert len(r_top.json()) <= 3

    # 2. Features
    r_feat = client.get("/api/v1/geo/malhas/features/3550308")
    assert r_feat.status_code == 200
    assert r_feat.json()["id_municipio"] == "3550308"

    # 3. UF
    r_uf = client.get("/api/v1/geo/malhas/uf/SP")
    assert r_uf.status_code == 200
    assert r_uf.json()["sigla_uf"] == "SP"

    # 4. Município
    r_mun = client.get("/api/v1/geo/malhas/municipio/3550308")
    assert r_mun.status_code == 200
    assert r_mun.json()["id_municipio"] == "3550308"


def test_mcp_parity() -> None:
    """Testa a paridade das ferramentas MCP com a blindagem de segurança untrusted_gov_data."""
    dispatcher = MCPDispatcher()

    # 1. tools/list contém as 4 ferramentas de malha
    req_list = {"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}}
    res_list = dispatcher.handle_request(req_list)
    tool_names = [t["name"] for t in res_list["result"]["tools"]]
    assert "bdg_get_municipio_boundary" in tool_names
    assert "bdg_get_uf_boundary" in tool_names
    assert "bdg_get_territorial_features" in tool_names
    assert "bdg_list_top_compact_municipalities" in tool_names

    # 2. tools/call bdg_get_territorial_features
    req_call = {
        "jsonrpc": "2.0",
        "id": 2,
        "method": "tools/call",
        "params": {
            "name": "bdg_get_territorial_features",
            "arguments": {"id_municipio": "3550308"},
        },
    }
    res_call = dispatcher.handle_request(req_call)
    assert not res_call.get("isError")
    content = res_call["result"]["content"][0]["text"]
    assert "untrusted_gov_data" in content
    assert "3550308" in content

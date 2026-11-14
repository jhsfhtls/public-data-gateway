"""Tests for CNJ Justiça Aberta Connector (11-Gate Protocol)."""

from __future__ import annotations

import json
from pathlib import Path

import pyarrow.parquet as pq
import pytest
import yaml
from fastapi.testclient import TestClient

from public_data_gateway.api.app import create_app
from public_data_gateway.cli.main import main
from public_data_gateway.connectors.judicial.cnj_justica_aberta.connector import (
    CNJJusticaAbertaConnector,
    _classify_justica_aberta_profile,
    _compute_justica_aberta_scores,
)
from public_data_gateway.core.connector import get_connector
from public_data_gateway.mcp.server import MCPDispatcher
from public_data_gateway.sdk import BrazilData


@pytest.fixture(scope="module", autouse=True)
def ensure_justica_aberta_baseline() -> None:
    """Garante que os Parquets de baseline existam em ambiente CI limpo."""
    conn = CNJJusticaAbertaConnector()
    if not (conn.silver_dir / "justica_aberta_serventias_cadastro.parquet").exists():
        conn.extract_all()


def test_contract_validity() -> None:
    """Verifica se o contrato ODCS v3.1 está em conformidade estrutural."""
    contract_path = (
        Path(__file__).resolve().parents[2]
        / "src"
        / "public_data_gateway"
        / "connectors"
        / "judicial"
        / "cnj_justica_aberta"
        / "contract.yaml"
    )
    assert contract_path.exists()
    with contract_path.open(encoding="utf-8") as f:
        data = yaml.safe_load(f)

    assert data["schema_version"] == "3.1.0"
    assert data["contract_id"] == "ODCS-BR-CNJ-JUSTICA-ABERTA-001"
    assert data["source_id"] == "cnj_justica_aberta"
    tables = data["schema"]["tables"]
    assert "justica_aberta_serventias_cadastro" in tables
    assert "justica_aberta_produtividade_atribuicao" in tables
    assert "justica_aberta_arrecadacao_emolumentos" in tables
    assert "justica_aberta_regularidade_digitalizacao" in tables
    assert "features_justica_aberta_cartorios_serventias_municipios_brasil" in tables


def test_score_and_classification_logic() -> None:
    """Valida o cálculo dos scores normalizados (0-100) e classificação de eficiência cartorial."""
    score_efic, score_din = _compute_justica_aberta_scores(
        acervo_historico_digitalizado_pct=94.2,
        serventias_integradas_srei_onr_crc_pct=96.4,
        taxa_provimento_titular_pct=92.62,
        arrecadacao_media_por_serventia_brl=10_163_934.43,
        proporcao_atos_eletronicos_media_pct=76.4,
        indice_transparencia_prestacao_contas_cnj_pct=98.4,
    )
    assert 90.0 <= score_efic <= 98.0
    assert 85.0 <= score_din <= 98.0
    assert _classify_justica_aberta_profile(score_efic) == "HUB_NOTARIAL_DIGITAL_ALTA_EFICIENCIA"
    assert _classify_justica_aberta_profile(81.0) == "REDE_CARTORIAL_CONSOLIDADA_INTEGRADA"
    assert _classify_justica_aberta_profile(71.0) == "REDE_REGIONAL_EM_MODERNIZACAO_DIGITAL"
    assert _classify_justica_aberta_profile(60.0) == "REDE_EXTRAJUDICIAL_EM_ESTRUTURACAO"


def test_baseline_parquets_exist() -> None:
    """Valida a materialização física dos Parquets de baseline em data/cnj_justica_aberta/."""
    conn = CNJJusticaAbertaConnector()
    assert conn.silver_dir.exists()
    assert conn.gold_dir.exists()

    p_serv = conn.silver_dir / "justica_aberta_serventias_cadastro.parquet"
    p_prod = conn.silver_dir / "justica_aberta_produtividade_atribuicao.parquet"
    p_arrec = conn.silver_dir / "justica_aberta_arrecadacao_emolumentos.parquet"
    p_reg = conn.silver_dir / "justica_aberta_regularidade_digitalizacao.parquet"
    p_gold = (
        conn.gold_dir
        / "features_justica_aberta_cartorios_serventias_municipios_brasil.parquet"
    )
    for path in (p_serv, p_prod, p_arrec, p_reg, p_gold):
        assert path.exists(), f"Missing baseline parquet: {path}"

    assert pq.read_table(p_serv).num_rows >= 12
    assert pq.read_table(p_prod).num_rows >= 48
    assert pq.read_table(p_arrec).num_rows >= 12
    assert pq.read_table(p_reg).num_rows >= 12
    gold_tbl = pq.read_table(p_gold)
    assert gold_tbl.num_rows >= 12
    assert "score_eficiencia_modernizacao_cartorial" in gold_tbl.column_names
    assert "score_dinamismo_notarial_imobiliario" in gold_tbl.column_names
    assert "classificacao_polo_notarial_cartorial" in gold_tbl.column_names


def test_connector_registry_and_methods() -> None:
    """Testa registro global do conector e os 6 métodos analíticos."""
    conn = get_connector("cnj_justica_aberta")
    assert isinstance(conn, CNJJusticaAbertaConnector)
    assert conn.test_connection() is True

    off = conn.get_notary_offices_registry("3550308")
    assert off["id_municipio"] == "3550308"
    assert off["total_serventias_ativas"] > 0

    prod_all = conn.list_productivity_by_attribution("3550308")
    assert len(prod_all) == 4
    prod_ri = conn.list_productivity_by_attribution("3550308", atribuicao_cartorial="Registro_Imoveis")
    assert len(prod_ri) == 1
    assert prod_ri[0]["atribuicao_cartorial"] == "Registro_Imoveis"

    emo = conn.get_emoluments_revenue("3550308")
    assert emo["id_municipio"] == "3550308"
    assert emo["arrecadacao_bruta_emolumentos_brl"] > 0

    dig = conn.get_digitalization_compliance("3550308")
    assert dig["id_municipio"] == "3550308"
    assert dig["acervo_historico_digitalizado_pct"] > 0

    feat = conn.get_justica_aberta_features("3550308")
    assert feat["id_municipio"] == "3550308"
    assert feat["classificacao_polo_notarial_cartorial"] == "HUB_NOTARIAL_DIGITAL_ALTA_EFICIENCIA"

    ranks_all = conn.list_top_notary_efficiency_municipalities(limit=5)
    assert len(ranks_all) == 5
    ranks_pr = conn.list_top_notary_efficiency_municipalities(limit=5, sigla_uf="PR")
    assert len(ranks_pr) >= 1
    assert all(r["sigla_uf"] == "PR" for r in ranks_pr)


def test_sdk_parity() -> None:
    """Testa a paridade dos métodos no SDK Python (JudicialFacade e BrazilData)."""
    sdk = BrazilData()

    off = sdk.get_justica_aberta_offices("3550308")
    assert off["id_municipio"] == "3550308"

    prod = sdk.list_justica_aberta_productivity("3550308", atribuicao_cartorial="Registro_Imoveis")
    assert len(prod) == 1

    emo = sdk.get_justica_aberta_emoluments("3550308")
    assert emo["id_municipio"] == "3550308"

    dig = sdk.get_justica_aberta_digitalization("3550308")
    assert dig["id_municipio"] == "3550308"

    feat = sdk.get_justica_aberta_features("3550308")
    assert feat["id_municipio"] == "3550308"

    ranks = sdk.list_top_justica_aberta_municipalities(limit=3)
    assert len(ranks) == 3


def test_cli_parity(capsys: pytest.CaptureFixture[str]) -> None:
    """Testa os comandos CLI de justica-aberta offices, productivity, emoluments, digitalization, features e ranking."""
    assert main(["justica-aberta", "offices", "3550308", "--json"]) == 0
    off_data = json.loads(capsys.readouterr().out)
    assert off_data["id_municipio"] == "3550308"

    assert main(["justica-aberta", "productivity", "3550308", "--atribuicao", "Registro_Imoveis"]) == 0
    assert "Registro_Imoveis" in capsys.readouterr().out

    assert main(["justica-aberta", "emoluments", "3550308"]) == 0
    assert "CNJ Justiça Aberta Emolumentos" in capsys.readouterr().out

    assert main(["justica-aberta", "digitalization", "3550308"]) == 0
    assert "CNJ Justiça Aberta Digitalização" in capsys.readouterr().out

    assert main(["justica-aberta", "features", "3550308"]) == 0
    assert "Score Eficiência" in capsys.readouterr().out

    assert main(["justica-aberta", "ranking", "--limit", "3"]) == 0
    assert "Polos Notariais" in capsys.readouterr().out


def test_rest_api_parity() -> None:
    """Testa a paridade dos endpoints REST FastAPI para CNJ Justiça Aberta."""
    app = create_app()
    client = TestClient(app)

    r_top = client.get("/api/v1/judicial/justica-aberta/top-municipalities?limit=3")
    assert r_top.status_code == 200
    assert len(r_top.json()) == 3

    r_feat = client.get("/api/v1/judicial/justica-aberta/features/3550308")
    assert r_feat.status_code == 200
    assert r_feat.json()["id_municipio"] == "3550308"

    r_prd = client.get("/api/v1/judicial/justica-aberta/productivity/3550308?atribuicao=Registro_Imoveis")
    assert r_prd.status_code == 200
    assert len(r_prd.json()) == 1

    r_emo = client.get("/api/v1/judicial/justica-aberta/emoluments/3550308")
    assert r_emo.status_code == 200
    assert r_emo.json()["id_municipio"] == "3550308"

    r_dig = client.get("/api/v1/judicial/justica-aberta/digitalization/3550308")
    assert r_dig.status_code == 200
    assert r_dig.json()["id_municipio"] == "3550308"

    r_off = client.get("/api/v1/judicial/justica-aberta/offices/3550308")
    assert r_off.status_code == 200
    assert r_off.json()["id_municipio"] == "3550308"

    r_short = client.get("/api/v1/justica-aberta/3550308")
    assert r_short.status_code == 200
    assert r_short.json()["id_municipio"] == "3550308"


def test_mcp_parity() -> None:
    """Testa a paridade das ferramentas MCP CNJ Justiça Aberta e blindagem untrusted_gov_data."""
    dispatcher = MCPDispatcher()

    req_list = {"jsonrpc": "2.0", "id": 1, "method": "tools/list", "params": {}}
    res_list = dispatcher.handle_request(req_list)
    tool_names = [t["name"] for t in res_list["result"]["tools"]]
    for expected in (
        "bdg_get_justica_aberta_offices",
        "bdg_get_justica_aberta_emoluments",
        "bdg_get_justica_aberta_features",
        "bdg_list_top_justica_aberta_municipalities",
    ):
        assert expected in tool_names

    for tool_name, arguments in (
        ("bdg_get_justica_aberta_offices", {"id_municipio": "3550308"}),
        ("bdg_get_justica_aberta_emoluments", {"id_municipio": "3550308"}),
        ("bdg_get_justica_aberta_features", {"id_municipio": "3550308"}),
        ("bdg_list_top_justica_aberta_municipalities", {"limit": 2}),
    ):
        req_call = {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {"name": tool_name, "arguments": arguments},
        }
        res_call = dispatcher.handle_request(req_call)
        assert not res_call.get("isError")
        content = res_call["result"]["content"][0]["text"]
        assert "untrusted_gov_data" in content
        assert "cnj_justica_aberta" in content

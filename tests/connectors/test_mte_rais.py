"""Offline contract, ingestion, and multi-interface tests for MTE RAIS."""

from __future__ import annotations

import csv
import importlib
import json
import shutil
from pathlib import Path

import pyarrow.parquet as pq
import pytest
from fastapi.testclient import TestClient

from public_data_gateway.api.app import create_app
from public_data_gateway.cli.main import main
from public_data_gateway.connectors.trabalho.mte_rais.connector import MTERaisConnector
from public_data_gateway.core.config import GatewaySettings
from public_data_gateway.mcp.server import MCPDispatcher
from public_data_gateway.sdk import BrazilData


@pytest.fixture
def gateway_settings(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> GatewaySettings:
    monkeypatch.setenv("BDG_OFFLINE_MODE", "1")
    settings = GatewaySettings(
        repo_root=Path(__file__).resolve().parents[2], data_dir=tmp_path / "data"
    )
    settings.ensure_directories()
    reference = settings.data_dir / "reference"
    reference.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(
        settings.repo_root / "data/reference/municipios_brasil.json",
        reference / "municipios_brasil.json",
    )
    return settings


@pytest.fixture
def extracted_rais(gateway_settings: GatewaySettings, tmp_path: Path) -> MTERaisConnector:
    """Aggregate tiny synthetic rows through the real parser; never label as national data."""
    fields = [
        "Município Trab - Código",
        "Ind Vínculo Ativo 31/12 - Código",
        "CNAE 2.0 Subclasse - Codigo",
        "Escolaridade Após 2005 - Código",
        "Sexo - Código",
        "Vl Rem Média Nom",
        "Vl Rem Média (SM)",
        "Idade",
        "Tempo Emprego",
        *[f"Vl Rem {month} SC" for month in MTERaisConnector.MONTHS],
        "Vl Rem Dezembro Nom",
    ]
    vinculos = tmp_path / "fixture_vinculos.csv"
    with vinculos.open("w", encoding="latin-1", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        for city, sector, active in (
            ("355030", "6201501", "1"),
            ("355030", "4711301", "1"),
            ("130260", "1011201", "1"),
            ("355030", "6201501", "0"),
        ):
            writer.writerow(
                {
                    "Município Trab - Código": city,
                    "Ind Vínculo Ativo 31/12 - Código": active,
                    "CNAE 2.0 Subclasse - Codigo": sector,
                    "Escolaridade Após 2005 - Código": "9",
                    "Sexo - Código": "1",
                    "Vl Rem Média Nom": "100",
                    "Vl Rem Média (SM)": "0,08",
                    "Idade": "35",
                    "Tempo Emprego": "24",
                    **{f"Vl Rem {month} SC": "100" for month in MTERaisConnector.MONTHS},
                    "Vl Rem Dezembro Nom": "100",
                }
            )
    establishments = tmp_path / "fixture_estabelecimentos.csv"
    with establishments.open("w", encoding="latin-1", newline="") as stream:
        writer = csv.DictWriter(
            stream,
            fieldnames=[
                "Município - Código",
                "Tamanho Estabelecimento - Código",
                "Qtd Vínculos Ativos",
                "Ind Atividade Ano - Código",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "Município - Código": "355030",
                "Tamanho Estabelecimento - Código": "2",
                "Qtd Vínculos Ativos": "2",
                "Ind Atividade Ano - Código": "1",
            }
        )
        writer.writerow(
            {
                "Município - Código": "130260",
                "Tamanho Estabelecimento - Código": "1",
                "Qtd Vínculos Ativos": "1",
                "Ind Atividade Ano - Código": "1",
            }
        )
    connector = MTERaisConnector(settings=gateway_settings)
    connector.extract_all(
        ano=2025,
        vinculos_csv_paths=[vinculos],
        estabelecimentos_csv_path=establishments,
        allow_partial=True,
    )
    return connector


def test_rais_odcs_contract(gateway_settings: GatewaySettings) -> None:
    connector = MTERaisConnector(settings=gateway_settings)
    assert connector.contract.contract_id == "ODCS-BR-MTE-RAIS-001"
    assert connector.contract.source_id == "mte_rais"
    assert connector.contract.version == "3.1.0"
    for name in connector.SILVER_TABLES:
        table = connector.contract_raw["schema"]["tables"][name]
        assert table["primary_keys"]
        assert table["grain"]


def test_rais_requires_both_fixture_inputs(
    gateway_settings: GatewaySettings, tmp_path: Path
) -> None:
    connector = MTERaisConnector(settings=gateway_settings)
    with pytest.raises(ValueError, match="Both vínculo and establishment"):
        connector.extract_all(vinculos_csv_paths=[tmp_path / "v.csv"], allow_partial=True)


def test_rais_five_silver_tables(extracted_rais: MTERaisConnector) -> None:
    silver_dir = extracted_rais.settings.data_dir / "mte_rais" / "silver"
    for name in extracted_rais.SILVER_TABLES:
        table = pq.read_table(silver_dir / f"{name}.parquet")
        assert table.num_rows > 0, name
        assert {"ano", "id_municipio", "source_url", "reference_date"} <= set(table.column_names)


def test_rais_gold_score_and_coverage(extracted_rais: MTERaisConnector) -> None:
    root = extracted_rais.settings.data_dir / "mte_rais"
    gold = pq.read_table(root / "gold" / extracted_rais.GOLD_FILE).to_pylist()
    assert len(gold) == 2
    assert all(0 <= row["score_formalizacao_trabalho_municipio"] <= 100 for row in gold)
    assert {row["classificacao_formalizacao_trabalho_municipio"] for row in gold} <= {
        "ALTA",
        "MEDIA",
        "BAIXA",
    }
    assert (
        json.loads((root / "coverage.json").read_text(encoding="utf-8"))["coverage"]
        == "PARTIAL_FIXTURE"
    )


def test_rais_aggregates_active_rows_only(extracted_rais: MTERaisConnector) -> None:
    summary = extracted_rais.get_formal_employment_summary("3550308", ano=2025)
    assert summary is not None
    assert summary["total_vinculos_ativos"] == 2
    assert summary["massa_salarial_anual_reais"] == 2400.0
    assert extracted_rais.get_formal_employment_summary("3550308", ano=2024) is None
    assert len(extracted_rais.get_sectoral_employment("3550308", ano=2025)) == 2


def test_rais_invalid_workplace_uses_establishment_municipality(
    gateway_settings: GatewaySettings, tmp_path: Path
) -> None:
    """A suppressed workplace code must use the valid establishment code and record fallback."""
    vinculos = tmp_path / "fallback_vinculos.csv"
    vinculo = {
        "Município Trab - Código": "999999",
        "Município - Código": "355030",
        "Ind Vínculo Ativo 31/12 - Código": "1",
        "CNAE 2.0 Subclasse - Codigo": "6201501",
        "Escolaridade Após 2005 - Código": "9",
        "Sexo - Código": "1",
        "Vl Rem Média Nom": "100",
        "Vl Rem Média (SM)": "0,08",
        "Idade": "35",
        "Tempo Emprego": "24",
        **{f"Vl Rem {month} SC": "100" for month in MTERaisConnector.MONTHS},
        "Vl Rem Dezembro Nom": "100",
    }
    with vinculos.open("w", encoding="latin-1", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(vinculo))
        writer.writeheader()
        writer.writerow(vinculo)
    establishments = tmp_path / "fallback_estabelecimentos.csv"
    with establishments.open("w", encoding="latin-1", newline="") as stream:
        writer = csv.DictWriter(
            stream,
            fieldnames=[
                "Município - Código",
                "Tamanho Estabelecimento - Código",
                "Qtd Vínculos Ativos",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "Município - Código": "355030",
                "Tamanho Estabelecimento - Código": "2",
                "Qtd Vínculos Ativos": "1",
            }
        )
    connector = MTERaisConnector(settings=gateway_settings)
    tables = connector.extract_all(
        ano=2025,
        vinculos_csv_paths=[vinculos],
        estabelecimentos_csv_path=establishments,
        allow_partial=True,
    )
    row = tables["rais_vinculos_ativos"].to_pylist()[0]
    assert row["id_municipio"] == "3550308"
    assert row["total_vinculos_ativos"] == 1
    coverage = json.loads(
        (gateway_settings.data_dir / "mte_rais" / "coverage.json").read_text(encoding="utf-8")
    )
    assert coverage["workplace_fallback_rows"] == 1
    assert coverage["unmapped_rows"] == 0


def test_rais_query_gold_and_ranking(extracted_rais: MTERaisConnector) -> None:
    profile = extracted_rais.get_rais_market_profile("3550308")
    assert profile is not None
    assert profile["total_estabelecimentos"] == 1
    assert extracted_rais.get_rais_market_profile("9999999") is None
    ranking = extracted_rais.list_top_formalized_municipalities(limit=2)
    assert len(ranking) == 2
    assert (
        ranking[0]["score_formalizacao_trabalho_municipio"]
        >= ranking[1]["score_formalizacao_trabalho_municipio"]
    )


@pytest.fixture
def stub_rais(monkeypatch: pytest.MonkeyPatch) -> None:
    """Stub only the connector's query boundary to test all public interfaces."""
    from public_data_gateway.connectors.trabalho.mte_rais.connector import MTERaisConnector

    row = {
        "id_municipio": "3550308",
        "nome_municipio": "São Paulo",
        "ano": 2024,
        "total_vinculos_ativos": 100,
    }
    profile = {**row, "score_formalizacao_trabalho_municipio": 82.5}
    monkeypatch.setattr(
        MTERaisConnector,
        "get_formal_employment_summary",
        lambda self, id_municipio, ano=None: row if id_municipio == "3550308" else None,
    )
    monkeypatch.setattr(
        MTERaisConnector,
        "get_rais_market_profile",
        lambda self, id_municipio: profile if id_municipio == "3550308" else None,
    )
    monkeypatch.setattr(
        MTERaisConnector,
        "get_sectoral_employment",
        lambda self, id_municipio, ano=None: (
            [{**row, "cnae_7": "6201501"}] if id_municipio == "3550308" else []
        ),
    )
    monkeypatch.setattr(
        MTERaisConnector,
        "list_top_formalized_municipalities",
        lambda self, limit=10: [profile][:limit],
    )


def test_sdk_rais_summary(gateway_settings: GatewaySettings, stub_rais: None) -> None:
    sdk = BrazilData(settings=gateway_settings)
    assert sdk.get_rais_employment_summary("3550308", ano=2024)["total_vinculos_ativos"] == 100
    assert sdk.get_rais_employment_summary("9999999") is None


def test_sdk_rais_profile(gateway_settings: GatewaySettings, stub_rais: None) -> None:
    sdk = BrazilData(settings=gateway_settings)
    assert sdk.labor.rais_profile("3550308")["score_formalizacao_trabalho_municipio"] == 82.5


def test_sdk_rais_sectors_and_ranking(gateway_settings: GatewaySettings, stub_rais: None) -> None:
    sdk = BrazilData(settings=gateway_settings)
    assert sdk.get_rais_sectoral_employment("3550308", ano=2024)[0]["cnae_7"] == "6201501"
    assert len(sdk.get_top_formalized_municipalities(limit=1)) == 1


def test_api_rais_summary(gateway_settings: GatewaySettings, stub_rais: None) -> None:
    client = TestClient(create_app(settings=gateway_settings))
    assert (
        client.get("/api/v1/labor/rais/municipality/3550308/summary?ano=2024").json()[
            "total_vinculos_ativos"
        ]
        == 100
    )
    assert client.get("/api/v1/labor/rais/municipality/9999999/summary").status_code == 404


def test_api_rais_profile_and_shortcut(gateway_settings: GatewaySettings, stub_rais: None) -> None:
    client = TestClient(create_app(settings=gateway_settings))
    for route in ("/api/v1/labor/rais/municipality/3550308/profile", "/api/v1/rais/3550308"):
        response = client.get(route)
        assert response.status_code == 200
        assert response.json()["score_formalizacao_trabalho_municipio"] == 82.5


def test_api_rais_sectors_and_ranking(gateway_settings: GatewaySettings, stub_rais: None) -> None:
    client = TestClient(create_app(settings=gateway_settings))
    assert len(client.get("/api/v1/labor/rais/municipality/3550308/sectors").json()) == 1
    assert len(client.get("/api/v1/labor/rais/ranking?limit=1").json()) == 1
    assert client.get("/api/v1/labor/rais/ranking?limit=0").status_code == 422


def test_mcp_rais_tools_listed(gateway_settings: GatewaySettings, stub_rais: None) -> None:
    response = MCPDispatcher(settings=gateway_settings).handle_request(
        {"jsonrpc": "2.0", "id": 1, "method": "tools/list"}
    )
    names = {tool["name"] for tool in response["result"]["tools"]}
    assert {
        "bdg_get_rais_employment_summary",
        "bdg_get_rais_market_profile",
        "bdg_list_top_formalized_municipalities",
    } <= names


def test_mcp_rais_untrusted_wrapper(gateway_settings: GatewaySettings, stub_rais: None) -> None:
    response = MCPDispatcher(settings=gateway_settings).handle_request(
        {
            "jsonrpc": "2.0",
            "id": 2,
            "method": "tools/call",
            "params": {
                "name": "bdg_get_rais_market_profile",
                "arguments": {"id_municipio": "3550308"},
            },
        }
    )
    content = response["result"]["content"][0]["text"]
    assert response["result"]["isError"] is False
    assert "untrusted_gov_data" in content
    assert "mte_rais" in content


def test_mcp_rais_missing_municipality(gateway_settings: GatewaySettings, stub_rais: None) -> None:
    response = MCPDispatcher(settings=gateway_settings).handle_request(
        {
            "jsonrpc": "2.0",
            "id": 3,
            "method": "tools/call",
            "params": {
                "name": "bdg_get_rais_employment_summary",
                "arguments": {"id_municipio": "9999999"},
            },
        }
    )
    assert response["result"]["isError"] is True
    assert "No RAIS data" in response["result"]["content"][0]["text"]


def test_cli_rais_json(
    gateway_settings: GatewaySettings,
    stub_rais: None,
    capsys: pytest.CaptureFixture[str],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cli_module = importlib.import_module("public_data_gateway.cli.main")
    monkeypatch.setattr(cli_module, "GatewaySettings", lambda: gateway_settings)
    assert main(["rais", "profile", "3550308", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["score_formalizacao_trabalho_municipio"] == 82.5

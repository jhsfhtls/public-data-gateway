"""Live fail-hard tests against official Banco Central do Brasil IF.data Olinda OData endpoints."""

from __future__ import annotations

import pytest
import requests
import urllib3

from public_data_gateway.connectors.macrofinanceiro.bcb_ifdata.connector import (
    BCB_IFDATA_ODATA_ROOT as BCB_IFDATA_ODATA_URL,
)
from public_data_gateway.connectors.macrofinanceiro.bcb_ifdata.connector import (
    BcbIfdataConnector,
)

urllib3.disable_warnings()


@pytest.mark.live
def test_bcb_ifdata_live_smoke_connection() -> None:
    """Verify live connectivity against BACEN IF.data Olinda API and fail-hard on HTTP 5xx."""
    conn = BcbIfdataConnector()
    report = conn.verify_live()

    assert report["status"] == "LIVE_SUCCESS"
    assert report["portal_status"] in (200, 301, 302)
    assert report["odata_status"] == 200


@pytest.mark.live
def test_bcb_ifdata_live_odata_relatorios() -> None:
    """Verify official BACEN Olinda endpoint for ListaDeRelatorio returns valid reports catalog."""
    headers = {
        "User-Agent": "PublicDataGateway/1.0 (Government Integration; contact@publicdata.gateway)"
    }
    url = f"{BCB_IFDATA_ODATA_URL}ListaDeRelatorio()?$format=json"
    resp = requests.get(url, headers=headers, timeout=30, verify=False)
    assert resp.status_code == 200, f"BACEN IF.data Relatorios returned HTTP {resp.status_code}"

    data = resp.json()
    assert "value" in data, "OData response must contain 'value' collection"
    reports = data["value"]
    assert len(reports) > 0, "BACEN must return at least one available report"

    # Verify standard report types exist
    report_names = [r.get("NomeRelatorio", "") for r in reports]
    has_resumo = any("Resumo" in name for name in report_names)
    has_ativo = any("Ativo" in name for name in report_names)
    assert has_resumo or has_ativo, f"Expected Ativo or Resumo report in BACEN IF.data, got: {report_names[:5]}"


@pytest.mark.live
def test_bcb_ifdata_live_institutions_cadastro() -> None:
    """Verify official BACEN Olinda endpoint for IfDataCadastro returns active banking entities."""
    headers = {
        "User-Agent": "PublicDataGateway/1.0 (Government Integration; contact@publicdata.gateway)"
    }
    # Query reference period 202406
    url = f"{BCB_IFDATA_ODATA_URL}IfDataCadastro(AnoMes=@AnoMes)?@AnoMes=202406&$format=json"
    resp = requests.get(url, headers=headers, timeout=30, verify=False)
    assert resp.status_code == 200, f"BACEN IF.data Cadastro returned HTTP {resp.status_code}"

    data = resp.json()
    assert "value" in data, "OData response must contain 'value' collection"
    entities = data["value"]
    assert len(entities) > 10, f"Expected > 10 financial institutions in BACEN registry, got {len(entities)}"

    # Verify mandatory regulatory columns in returned records
    sample = entities[0]
    assert "CodInst" in sample, "CodInst must exist in BACEN IF.data record"
    assert "NomeInstituicao" in sample, "NomeInstituicao must exist in BACEN IF.data record"

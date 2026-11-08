"""Live fail-hard network probe tests against official IBGE Censo Demográfico 2022 endpoints.

Tests real-world HTTPS connectivity and response integrity against:
1. IBGE SIDRA Tabela 4712 (Censo 2022: Domicílios particulares permanentes e moradores)
2. IBGE SIDRA Tabela 9515 (Censo 2022: Idade mediana e Índice de Envelhecimento)
3. IBGE SIDRA Tabela 9543 (Censo 2022: Taxa de Alfabetização)
4. Comprehensive verify_live() status report
"""

from __future__ import annotations

import pytest

from public_data_gateway.connectors.demografico.ibge_censo2022 import (
    IBGECenso2022Connector,
)


@pytest.mark.live
def test_ibge_censo2022_verify_live_full_report() -> None:
    """Validate that official IBGE Censo 2022 open data endpoints are reachable and healthy."""
    connector = IBGECenso2022Connector()
    report = connector.verify_live()

    assert report["source_id"] == "ibge_censo2022"
    assert "endpoints" in report

    # 1. Domicílios & População probe
    dom_ep = report["endpoints"].get("censo_domicilios")
    assert dom_ep is not None, "Missing censo_domicilios endpoint in report"
    assert dom_ep["passed"] is True, f"Domicílios endpoint failed: {dom_ep}"
    assert dom_ep["status_code"] == 200
    assert dom_ep["domicilios_amostra"] is not None

    # 2. Estrutura Etária & Envelhecimento probe
    age_ep = report["endpoints"].get("censo_estrutura_etaria")
    assert age_ep is not None, "Missing censo_estrutura_etaria endpoint in report"
    assert age_ep["passed"] is True, f"Estrutura etária endpoint failed: {age_ep}"
    assert age_ep["status_code"] == 200
    assert age_ep["indice_envelhecimento_amostra"] is not None

    # 3. Alfabetização probe
    lit_ep = report["endpoints"].get("censo_alfabetizacao")
    assert lit_ep is not None, "Missing censo_alfabetizacao endpoint in report"
    assert lit_ep["passed"] is True, f"Alfabetização endpoint failed: {lit_ep}"
    assert lit_ep["status_code"] == 200
    assert lit_ep["taxa_alfabetizacao_amostra"] is not None

    # Overall health status
    assert report["status"] == "PASS", f"IBGE Censo 2022 verify_live failed: {report}"

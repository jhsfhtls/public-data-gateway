"""Live fail-hard network probe tests against official IBGE SIDRA endpoints.

Tests real-world HTTPS connectivity and response integrity against:
1. IBGE SIDRA Tabela 5938 (PIB Municipal)
2. IBGE SIDRA Tabela 4709 (Censo Demográfico 2022)
3. IBGE SIDRA Tabela 7060 (IPCA Mensal)
4. Comprehensive verify_live() status report
"""

from __future__ import annotations

import pytest

from public_data_gateway.connectors.demografico.ibge_sidra import (
    IBGESidraConnector,
)


@pytest.mark.live
def test_ibge_sidra_verify_live_full_report() -> None:
    """Validate that official IBGE SIDRA open data endpoints are reachable and healthy."""
    connector = IBGESidraConnector()
    report = connector.verify_live()

    assert report["source_id"] == "ibge_sidra"
    assert "endpoints" in report

    # 1. PIB Municipal probe
    pib_ep = report["endpoints"].get("sidra_pib_municipal")
    assert pib_ep is not None, "Missing sidra_pib_municipal endpoint in report"
    assert pib_ep["passed"] is True, f"PIB Municipal endpoint failed: {pib_ep}"
    assert pib_ep["status_code"] == 200
    assert pib_ep["valor_amostra"] is not None

    # 2. Censo Demográfico probe
    pop_ep = report["endpoints"].get("sidra_populacao_censo")
    assert pop_ep is not None, "Missing sidra_populacao_censo endpoint in report"
    assert pop_ep["passed"] is True, f"Censo endpoint failed: {pop_ep}"
    assert pop_ep["status_code"] == 200
    assert pop_ep["populacao_amostra"] is not None

    # 3. IPCA probe
    ipca_ep = report["endpoints"].get("sidra_ipca_mensal")
    assert ipca_ep is not None, "Missing sidra_ipca_mensal endpoint in report"
    assert ipca_ep["passed"] is True, f"IPCA endpoint failed: {ipca_ep}"
    assert ipca_ep["status_code"] == 200

    # Overall health status
    assert report["status"] == "PASS", f"IBGE SIDRA verify_live failed: {report}"

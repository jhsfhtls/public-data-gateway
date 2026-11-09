"""Live fail-hard network probe tests against official IBGE SIDRA PNAD Contínua endpoints.

Tests real-world HTTPS connectivity and response integrity against:
1. IBGE SIDRA Tabela 4099 (Taxa de desocupação das pessoas de 14 anos ou mais de idade)
2. IBGE SIDRA Tabela 6388 (Rendimento médio mensal real habitual e efetivo)
3. Comprehensive verify_live() status report
"""

from __future__ import annotations

import pytest

from public_data_gateway.connectors.trabalho.ibge_pnad_continua import (
    IBGEPnadContinuaConnector,
)


@pytest.mark.live
def test_ibge_pnad_continua_verify_live_full_report() -> None:
    """Validate that official IBGE SIDRA PNAD Contínua open data endpoints are reachable and healthy."""
    connector = IBGEPnadContinuaConnector()
    report = connector.verify_live()

    assert report["source_id"] == "ibge_pnad_continua"
    assert "endpoints" in report

    # 1. Taxa de desocupação probe (Tabela 4099)
    desoc_ep = report["endpoints"].get("pnad_desocupacao_sidra")
    assert desoc_ep is not None, "Missing pnad_desocupacao_sidra endpoint in report"
    assert desoc_ep["passed"] is True, f"PNAD desocupação probe failed: {desoc_ep}"
    assert desoc_ep["status_code"] == 200
    assert desoc_ep["taxa_desocupacao_amostra"] is not None

    # 2. Rendimento médio probe (Tabela 6388)
    renda_ep = report["endpoints"].get("pnad_rendimento_sidra")
    assert renda_ep is not None, "Missing pnad_rendimento_sidra endpoint in report"
    assert renda_ep["passed"] is True, f"PNAD rendimento probe failed: {renda_ep}"
    assert renda_ep["status_code"] == 200
    assert renda_ep["rendimento_medio_amostra"] is not None

    # Overall health status
    assert report["status"] == "PASS", f"IBGE PNAD Contínua verify_live failed: {report}"

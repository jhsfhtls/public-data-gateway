"""Fail-hard connectivity probe against official CNJ Justiça Aberta portals."""

from __future__ import annotations

import pytest

from public_data_gateway.connectors.judicial.cnj_justica_aberta.connector import (
    CNJJusticaAbertaConnector,
)


@pytest.mark.live
def test_cnj_justica_aberta_verify_live() -> None:
    """Garante conectividade real com endpoints públicos do CNJ Justiça Aberta."""
    report = CNJJusticaAbertaConnector().verify_live()
    assert report["source_id"] == "cnj_justica_aberta"
    assert report["status"] == "PASS", report
    assert any(endpoint.get("passed") for endpoint in report["endpoints"].values()), report

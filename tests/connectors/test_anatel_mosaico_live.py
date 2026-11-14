"""Fail-hard connectivity probe against official ANATEL Sistema Mosaico portals."""

from __future__ import annotations

import pytest

from public_data_gateway.connectors.geografico.anatel_mosaico.connector import (
    AnatelMosaicoConnector,
)


@pytest.mark.live
def test_anatel_mosaico_verify_live() -> None:
    """Garante conectividade real com endpoints públicos da ANATEL."""
    report = AnatelMosaicoConnector().verify_live()
    assert report["source_id"] == "anatel_mosaico"
    assert report["status"] == "PASS", report
    assert any(endpoint.get("passed") for endpoint in report["endpoints"].values()), report

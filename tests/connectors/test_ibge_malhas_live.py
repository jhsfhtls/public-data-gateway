"""Fail-hard connectivity probe against official IBGE Malhas API service."""

from __future__ import annotations

import pytest

from public_data_gateway.connectors.geografico.ibge_malhas.connector import IBGEMalhasConnector


@pytest.mark.live
def test_ibge_malhas_verify_live() -> None:
    report = IBGEMalhasConnector().verify_live()
    assert report["source_id"] == "ibge_malhas"
    assert report["status"] == "PASS", report
    assert any(endpoint.get("passed") for endpoint in report["endpoints"].values()), report

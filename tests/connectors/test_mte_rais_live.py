"""Fail-hard connectivity probe against official MTE RAIS publishing endpoint."""

from __future__ import annotations

import pytest

from public_data_gateway.connectors.trabalho.mte_rais.connector import MTERaisConnector


@pytest.mark.live
def test_mte_rais_verify_live() -> None:
    report = MTERaisConnector().verify_live()
    assert report["source_id"] == "mte_rais"
    assert report["status"] == "PASS", report
    assert any(endpoint.get("passed") for endpoint in report["endpoints"].values()), report

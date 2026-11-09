"""Live fail-hard network probe tests against official MTE Novo CAGED FTP repository.

Tests real-world FTP connectivity and response integrity against:
1. MTE FTP Server (ftp.mtps.gov.br)
2. Novo CAGED microdata directory hierarchy (pdet/microdados/NOVO CAGED/)
3. Comprehensive verify_live() status report
"""

from __future__ import annotations

import pytest

from public_data_gateway.connectors.trabalho.mte_novo_caged import (
    MTENovoCagedConnector,
)


@pytest.mark.live
def test_mte_novo_caged_verify_live_full_report() -> None:
    """Validate that official MTE Novo CAGED open data repository is reachable and healthy."""
    connector = MTENovoCagedConnector()
    report = connector.verify_live()

    assert report["source_id"] == "mte_novo_caged"
    assert "endpoints" in report

    # FTP repository probe
    ftp_ep = report["endpoints"].get("ftp_novo_caged")
    assert ftp_ep is not None, "Missing ftp_novo_caged endpoint in report"
    assert ftp_ep["passed"] is True, f"MTE CAGED FTP probe failed: {ftp_ep}"
    assert "welcome" in ftp_ep
    assert "available_dirs_sample" in ftp_ep
    assert len(ftp_ep["available_dirs_sample"]) > 0

    # Overall health status
    assert report["status"] == "PASS", f"MTE Novo CAGED verify_live failed: {report}"

"""Live fail-hard tests against official PGFN Dívida Ativa Dados Abertos endpoints."""

from __future__ import annotations

import httpx
import pytest

from public_data_gateway.connectors.compliance.pgfn_divida_ativa import (
    PGFN_OPEN_DATA_URL,
    PgfnDividaAtivaConnector,
)


@pytest.mark.live
def test_pgfn_live_smoke_connection() -> None:
    """Verify live connectivity against PGFN Dados Abertos index and fail hard on HTTP 5xx."""
    conn = PgfnDividaAtivaConnector()
    report = conn.verify_live()

    assert report["status"] == "LIVE_SUCCESS"
    assert report["http_status"] == 200
    assert report["quarters_found"] > 0
    assert "_trimestre_" in report["latest_quarter_dir"]


@pytest.mark.live
def test_pgfn_live_open_data_index_quarters() -> None:
    """Verify official PGFN open data repository exposes quarterly CSV ZIP directories."""
    resp = httpx.get(PGFN_OPEN_DATA_URL, timeout=20.0, follow_redirects=True, verify=False)
    assert resp.status_code == 200, f"PGFN index returned HTTP {resp.status_code}"
    assert "trimestre" in resp.text.lower()

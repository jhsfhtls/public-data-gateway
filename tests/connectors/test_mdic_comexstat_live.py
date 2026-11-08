"""Live smoke test for the MDIC Comex Stat open data portals (Fail-Hard)."""

from __future__ import annotations

import pytest

from public_data_gateway.connectors.comercio_exterior.mdic_comexstat.connector import (
    MDICComexStatConnector,
)


@pytest.mark.live
def test_mdic_live_endpoint_smoke() -> None:
    """Perform live 1-line stream reads from official MDIC open data portals.

    Fails hard if upstream returns HTTP 5xx or server is unavailable.
    """
    conn = MDICComexStatConnector()
    res = conn.verify_live()

    assert res["ncm_status"] == 200
    assert len(res["ncm_header_preview"]) > 0
    assert res["mun_status"] == 200
    assert len(res["mun_header_preview"]) > 0
    assert "verified_at" in res


@pytest.mark.live
def test_mdic_live_ncm_url_accessibility() -> None:
    """Verify official MDIC NCM catalog URL responds with HTTP 200."""
    import requests
    import urllib3

    urllib3.disable_warnings()

    url = "https://balanca.economia.gov.br/balanca/bd/tabelas/NCM.csv"
    resp = requests.head(
        url,
        timeout=15,
        headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"},
        verify=False,
    )
    assert resp.status_code == 200


@pytest.mark.live
def test_mdic_live_municipality_export_accessibility() -> None:
    """Verify official MDIC municipal exports URL responds with HTTP 200."""
    import requests
    import urllib3

    urllib3.disable_warnings()

    url = "https://balanca.economia.gov.br/balanca/bd/comexstat-bd/mun/EXP_2024_MUN.csv"
    resp = requests.head(
        url,
        timeout=15,
        headers={"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)"},
        verify=False,
    )
    assert resp.status_code == 200
